"""
judge_node.py — Verification layer that gates pipeline output quality.

Applies 5 independent rule-based checks to the LLM-extracted FinancialProfile.
Failures are collected (not short-circuited) and fed back to the Mapping Agent
as a structured critique for the retry loop.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Optional

from config.settings import config
from src.schemas import FinancialProfile, PipelineState
from src.utils.logger import get_logger
from src.utils.xbrl_utils import is_valid_xbrl_tag

logger = get_logger("judge_node")


# ── Individual verification checks ────────────────────────────────────────────


def _check_confidence(profile: FinancialProfile) -> tuple[bool, Optional[str]]:
    """LLM self-reported confidence must meet threshold."""
    if profile.confidence_score >= config.CONFIDENCE_THRESHOLD:
        return True, None
    return False, (
        f"confidence_score {profile.confidence_score:.2f} is below threshold "
        f"{config.CONFIDENCE_THRESHOLD}. Re-extract with higher certainty or "
        "set confidence to reflect actual uncertainty and add extraction_notes."
    )


def _check_xbrl_format(profile: FinancialProfile) -> tuple[bool, Optional[str]]:
    """XBRL tags present must be in valid namespace:CamelCase format."""
    bad = [
        inst.xbrl_tag
        for inst in profile.debt_instruments
        if inst.xbrl_tag and not is_valid_xbrl_tag(inst.xbrl_tag)
    ]
    if bad:
        return False, (
            f"Invalid XBRL tag format (expected 'namespace:CamelCase'): {bad}. "
            "Use tags like 'us-gaap:LongTermDebt' or set xbrl_tag to null "
            "if no matching tag is available."
        )
    return True, None


def _check_date_sanity(profile: FinancialProfile) -> tuple[bool, Optional[str]]:
    """period_ending must be a plausible recent date."""
    try:
        period = date.fromisoformat(profile.period_ending)
    except ValueError:
        return False, (
            f"period_ending '{profile.period_ending}' is not a valid ISO date. "
            "Use YYYY-MM-DD format (e.g., '2023-12-31')."
        )

    today = date.today()
    if period > today:
        return False, (
            f"period_ending '{profile.period_ending}' is in the future. "
            "Extract the fiscal year-end date from the filing text."
        )
    if period.year < today.year - 10:
        return False, (
            f"period_ending '{profile.period_ending}' is more than 10 years ago. "
            "Verify you extracted the correct fiscal period."
        )
    return True, None


def _check_completeness(
    profile: FinancialProfile, item_8_markdown: str
) -> tuple[bool, Optional[str]]:
    """
    If financial text contains dollar amounts, expect at least one instrument.
    This catches cases where the model returned an empty instruments list
    despite clear debt amounts being present.
    """
    has_dollar_amounts = bool(
        re.search(
            r"\$[\d,]+\.?\d*\s*(?:billion|million|B\b|M\b)",
            item_8_markdown,
            re.IGNORECASE,
        )
    )
    if has_dollar_amounts and not profile.debt_instruments:
        return False, (
            "The financial statements contain dollar amounts with billion/million "
            "qualifiers, but debt_instruments is empty. Look for long-term debt, "
            "notes payable, senior notes, credit facilities, and similar items in "
            "the balance sheet or debt schedule sections."
        )
    return True, None


def _parse_xbrl_values(xbrl_snippet: str) -> dict:
    """
    Parse the XBRL snippet from state into a tag->value_millions dict.
    Format: "us-gaap:LongTermDebt | 12300.0M | ctx:..."
    """
    xbrl_values = {}
    if not xbrl_snippet:
        return xbrl_values
    for line in xbrl_snippet.splitlines():
        parts = line.split("|")
        if len(parts) >= 2:
            tag = parts[0].strip()
            val_str = parts[1].strip().rstrip("M").replace(",", "")
            try:
                xbrl_values[tag] = float(val_str)
            except ValueError:
                pass
    return xbrl_values


def _check_amount_plausibility(
    profile: FinancialProfile, item_8_markdown: str, xbrl_snippet: str = ""
) -> tuple[bool, Optional[str]]:
    """
    Cross-reference extracted amounts against XBRL values (primary) and
    source text (fallback). XBRL values are ground truth — if the tag
    matches and the value is within 5%, it passes. Text search catches
    instruments without XBRL tags.
    """
    xbrl_values = _parse_xbrl_values(xbrl_snippet)
    issues = []

    for inst in profile.debt_instruments:
        if inst.amount < 100:
            continue  # skip small amounts — text representation too variable

        # Strategy 1: XBRL cross-reference (most reliable)
        if inst.xbrl_tag and inst.xbrl_tag in xbrl_values:
            xbrl_val = xbrl_values[inst.xbrl_tag]
            if xbrl_val > 0:
                pct_diff = abs(inst.amount - xbrl_val) / xbrl_val
                if pct_diff <= 0.05:  # within 5%
                    continue  # XBRL confirms this value
                else:
                    issues.append(
                        f"'{inst.name}': extracted ${inst.amount:,.0f}M but XBRL tag "
                        f"{inst.xbrl_tag} shows ${xbrl_val:,.0f}M — correct the amount"
                    )
                    continue

        # Strategy 2: Text search fallback (for instruments without XBRL match)
        amt = inst.amount
        amt_b = amt / 1000
        text_patterns = [
            re.compile(rf"\${amt:,.0f}(?:\.\d)?\s*million", re.IGNORECASE),
            re.compile(rf"\${amt:,.1f}\s*million", re.IGNORECASE),
            re.compile(rf"\${amt_b:,.1f}\s*billion", re.IGNORECASE),
            re.compile(rf"\${amt_b:,.0f}\s*billion", re.IGNORECASE),
            re.compile(rf"{amt:,.0f}"),
        ]
        if not any(p.search(item_8_markdown) for p in text_patterns):
            # If no XBRL tag was provided, flag it
            if not inst.xbrl_tag:
                issues.append(
                    f"'{inst.name}': ${inst.amount:,.0f}M not confirmed in source text "
                    f"and no XBRL tag provided"
                )
            # If XBRL tag exists but wasn't in our extract, skip — likely a custom tag
            # (e.g. aapl:FixedRateNotesMember) — we can't validate, so we trust it

    if issues:
        return False, (
            "Amount verification failed:\n"
            + "\n".join(f"  - {i}" for i in issues)
            + "\nCorrect the amounts to match XBRL-tagged values in the source filing."
        )
    return True, None


# ── Main node function ─────────────────────────────────────────────────────────


def judge_node(state: PipelineState) -> PipelineState:
    """
    LangGraph node: Verify extracted FinancialProfile quality.

    Reads from state:  extracted_profile, item_8_markdown, retry_count
    Writes to state:   is_verified, judge_feedback, confidence_score
    """
    if not state.extracted_profile:
        logger.error("No extracted_profile to verify")
        state.judge_feedback = (
            "The mapping agent produced no output. "
            "Ensure the LLM response is valid JSON matching the FinancialProfile schema."
        )
        state.is_verified = False
        state.retry_count += 1  # must increment to prevent infinite retry loop
        return state

    profile = state.extracted_profile

    # Run all 5 checks and collect all failures (no short-circuit)
    check_results = [
        _check_confidence(profile),
        _check_xbrl_format(profile),
        _check_date_sanity(profile),
        _check_completeness(profile, state.item_8_markdown or ""),
        _check_amount_plausibility(
            profile, state.item_8_markdown or "", state.xbrl_snippet or ""
        ),
    ]

    failures = [msg for passed, msg in check_results if not passed and msg]

    if not failures:
        state.is_verified = True
        state.judge_feedback = None
        state.confidence_score = profile.confidence_score
        logger.info(
            f"{state.ticker} VERIFIED — confidence {profile.confidence_score:.2f}, "
            f"{len(profile.debt_instruments)} instruments"
        )
    else:
        state.is_verified = False
        state.judge_feedback = (
            f"Verification failed on attempt {state.retry_count + 1} "
            f"({len(failures)} issue(s)):\n\n"
            + "\n\n".join(f"{i+1}. {f}" for i, f in enumerate(failures))
            + "\n\nFix all issues above and return a corrected JSON object."
        )
        logger.warning(
            f"{state.ticker} FAILED verification "
            f"(attempt {state.retry_count + 1}/{config.MAX_RETRY_LOOPS}): "
            f"{len(failures)} failure(s)"
        )

    state.retry_count += 1
    return state


def should_retry(state: PipelineState) -> str:
    """
    LangGraph conditional edge routing function.

    Returns:
        "retry"   → back to mapping_agent_node
        "proceed" → forward to storage_node
    """
    if state.is_verified:
        return "proceed"

    if state.retry_count < config.MAX_RETRY_LOOPS:
        logger.info(
            f"Routing to retry ({state.retry_count}/{config.MAX_RETRY_LOOPS} used)"
        )
        return "retry"

    logger.warning(
        f"Max retries ({config.MAX_RETRY_LOOPS}) reached for {state.ticker}. "
        "Proceeding with unverified output."
    )
    return "proceed"
