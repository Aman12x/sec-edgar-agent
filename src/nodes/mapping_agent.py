"""
mapping_agent.py — Phase 3: LLM-powered extraction of debt instruments.

Uses Llama 3.2 (via Groq or local Ollama) to convert parsed 10-K Markdown
into a structured FinancialProfile JSON. On retry runs, judge_feedback is
injected into the prompt so the model can correct specific errors.
"""

from __future__ import annotations

import json
import re
from typing import Optional

from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import ValidationError

from config.settings import config
from src.schemas import FinancialProfile, PipelineState
from src.utils.logger import get_logger
from src.utils.prompt_builder import SYSTEM_PROMPT, build_user_prompt

logger = get_logger("mapping_agent")


def _get_llm():
    """
    Return a LangChain chat model based on the configured LLM_PROVIDER.
    Factory pattern keeps provider-specific imports isolated.
    """
    if config.LLM_PROVIDER == "groq":
        from langchain_groq import ChatGroq

        return ChatGroq(
            model=config.LLM_MODEL,
            api_key=config.GROQ_API_KEY,
            temperature=config.LLM_TEMPERATURE,
            max_tokens=config.LLM_MAX_TOKENS,
        )
    elif config.LLM_PROVIDER == "ollama":
        from langchain_community.chat_models import ChatOllama

        return ChatOllama(
            model=config.LLM_MODEL,
            base_url=config.OLLAMA_BASE_URL,
            temperature=config.LLM_TEMPERATURE,
            num_ctx=32768,
        )
    else:
        raise ValueError(
            f"Unknown LLM_PROVIDER: '{config.LLM_PROVIDER}'. "
            "Set LLM_PROVIDER=groq or LLM_PROVIDER=ollama in .env"
        )


# JSON extraction: handles clean JSON, fenced JSON, JSON with preamble
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(text: str) -> Optional[dict]:
    """Robustly extract a JSON object from LLM output."""
    # 1. Try stripping markdown fences
    fence_match = _JSON_FENCE_RE.search(text)
    candidate = fence_match.group(1).strip() if fence_match else text

    # 2. Try to find a JSON object in the candidate text
    obj_match = _JSON_OBJECT_RE.search(candidate)
    if obj_match:
        try:
            return json.loads(obj_match.group(0))
        except json.JSONDecodeError:
            pass

    # 3. Try parsing the full text as-is
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        return None


def mapping_agent_node(state: PipelineState) -> PipelineState:
    """
    LangGraph node: Call Llama 3.2 to extract FinancialProfile from Markdown.

    Reads from state:  ticker, item_8_markdown, item_1a_markdown,
                       xbrl_snippet, judge_feedback, retry_count
    Writes to state:   extracted_profile, llm_raw_response, confidence_score
    """
    if not state.item_8_markdown:
        raise ValueError("mapping_agent_node requires item_8_markdown in state")

    attempt = state.retry_count + 1
    logger.info(f"Calling {config.LLM_MODEL} for {state.ticker} (attempt {attempt})")

    llm = _get_llm()

    user_content = build_user_prompt(
        ticker=state.ticker,
        xbrl_snippet=state.xbrl_snippet or "",
        item_8_markdown=state.item_8_markdown,
        item_1a_markdown=state.item_1a_markdown or "",
        judge_feedback=state.judge_feedback,
        max_item8_chars=config.MAX_ITEM8_CHARS - 2000,  # leave room for prompt overhead
        max_item1a_chars=config.MAX_ITEM1A_CHARS - 1000,
    )

    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_content),
    ]

    # LLM call
    try:
        response = llm.invoke(messages)
        raw_text = response.content
        state.llm_raw_response = raw_text
        logger.debug(f"LLM response ({len(raw_text)} chars): {raw_text[:300]}...")
    except Exception as e:
        logger.error(f"LLM API call failed: {e}")
        state.judge_feedback = f"LLM API error on attempt {attempt}: {e}"
        return state

    # Optionally save raw response for debugging
    if config.SAVE_LLM_RAW_RESPONSE:
        _save_raw_response(state.ticker, attempt, raw_text)

    # Parse JSON from response
    parsed_dict = _extract_json(raw_text)
    if not parsed_dict:
        logger.error("Could not extract valid JSON from LLM response")
        state.judge_feedback = (
            f"Attempt {attempt}: Failed to parse JSON from LLM output.\n"
            f"Response was:\n{raw_text[:2000]}\n\n"
            "Please return ONLY a valid JSON object with no surrounding text."
        )
        return state

    # Inject ticker (model shouldn't need to know it)
    parsed_dict["ticker"] = state.ticker

    # Post-process: fix amounts stored as billions instead of millions.
    # Known large-balance XBRL tags where sub-100 values are always a units error.
    import re as _re

    LARGE_BALANCE_TAGS = {
        "us-gaap:LongTermDebt",
        "us-gaap:LongTermDebtCurrent",
        "us-gaap:LongTermDebtNoncurrent",
        "us-gaap:DebtCurrent",
        "us-gaap:LongTermDebtAndCapitalLeaseObligations",
        "us-gaap:SeniorNotes",
        "us-gaap:NotesPayable",
    }
    item8_text = (state.item_8_markdown or "") + (state.xbrl_snippet or "")
    for inst in parsed_dict.get("debt_instruments", []):
        amt = inst.get("amount")
        if amt is None or amt >= 500:
            continue
        tag = inst.get("xbrl_tag", "")
        name = inst.get("name", "")

        # Strategy 1: value followed by "billion" anywhere in source text
        billion_pat = _re.compile(
            rf"{_re.escape(str(amt))}\s*(?:billion|B)", _re.IGNORECASE
        )
        if billion_pat.search(item8_text):
            logger.debug(f"Corrected {name}: {amt} -> {amt*1000} (billion in text)")
            inst["amount"] = amt * 1000
            continue

        # Strategy 2: known large-balance tag with suspiciously small value
        if tag in LARGE_BALANCE_TAGS and amt < 100:
            logger.debug(
                f"Corrected {name}: {amt} -> {amt*1000} (large-balance tag {tag})"
            )
            inst["amount"] = amt * 1000

    # Validate against Pydantic schema
    try:
        profile = FinancialProfile.model_validate(parsed_dict)
        state.extracted_profile = profile
        state.confidence_score = profile.confidence_score
        logger.info(
            f"Extracted {len(profile.debt_instruments)} instruments "
            f"with confidence {profile.confidence_score:.2f}"
        )
    except ValidationError as ve:
        logger.error(f"Pydantic validation failed: {ve}")
        state.judge_feedback = (
            f"Attempt {attempt}: Schema validation errors:\n{ve}\n\n"
            f"Partial JSON received:\n{json.dumps(parsed_dict, indent=2)[:1500]}\n\n"
            "Fix the errors above and return a corrected JSON object."
        )

    return state


def _save_raw_response(ticker: str, attempt: int, raw_text: str) -> None:
    """Save raw LLM response to disk for debugging (only when SAVE_LLM_RAW_RESPONSE=true)."""
    from pathlib import Path

    debug_dir = Path(config.OUTPUT_DIR) / "debug"
    debug_dir.mkdir(exist_ok=True)
    path = debug_dir / f"{ticker}_attempt_{attempt}_raw.txt"
    path.write_text(raw_text, encoding="utf-8")
    logger.debug(f"Saved raw LLM response: {path}")
