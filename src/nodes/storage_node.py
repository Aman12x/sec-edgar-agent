"""
storage_node.py — Persist the final FinancialProfile to disk.

Outputs per run:
  {ticker}_{period}_{verified|unverified}.json   — Full FinancialProfile
  {ticker}_{period}_{verified|unverified}_risks.md — Risk factors Markdown

Both files are written atomically via safe_write_json to prevent partial writes.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from config.settings import config
from src.schemas import PipelineState
from src.utils.file_utils import ensure_dirs, safe_write_json, archive_filing
from src.utils.logger import get_logger

logger = get_logger("storage_node")


def storage_node(state: PipelineState) -> PipelineState:
    """
    LangGraph node: Persist FinancialProfile JSON and Markdown sidecar.

    Reads from state:  extracted_profile, is_verified, filing_path, retry_count
    Writes to state:   output_path
    """
    ensure_dirs(config.OUTPUT_DIR)

    if not state.extracted_profile:
        logger.error(f"No profile to save for {state.ticker}")
        return state

    profile = state.extracted_profile
    verified_tag = "verified" if state.is_verified else "unverified"
    base_name = f"{state.ticker}_{profile.period_ending}_{verified_tag}"

    # ── JSON output ──────────────────────────────────────────────────────────
    profile_dict = profile.model_dump()
    profile_dict["_pipeline_metadata"] = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "retry_count": state.retry_count,
        "is_verified": state.is_verified,
        "filing_source": state.filing_path,
        "llm_provider": config.LLM_PROVIDER,
        "llm_model": config.LLM_MODEL,
    }

    json_path = safe_write_json(
        profile_dict,
        config.OUTPUT_DIR / f"{base_name}.json",
    )
    logger.info(f"Saved JSON: {json_path}")

    # ── Markdown sidecar ─────────────────────────────────────────────────────
    md_path = config.OUTPUT_DIR / f"{base_name}_risks.md"
    md_content = [
        f"# {state.ticker} — Risk Summary",
        f"**Period:** {profile.period_ending}  ",
        f"**Filing:** {state.filing_type}  ",
        f"**Confidence:** {profile.confidence_score:.2f}  ",
        f"**Verified:** {'Yes' if state.is_verified else 'No'}  ",
        "",
        "## Risk Factors",
        "",
        profile.risks_summary or "_Risk factors not extracted._",
    ]
    if profile.extraction_notes:
        md_content += ["", "## Extraction Notes", "", f"_{profile.extraction_notes}_"]

    md_path.write_text("\n".join(md_content), encoding="utf-8")
    logger.info(f"Saved Markdown: {md_path}")

    state.output_path = str(json_path)

    # ── Optional: archive the raw filing ─────────────────────────────────────
    if config.ARCHIVE_AFTER_PROCESSING and state.filing_path:
        try:
            archive_filing(Path(state.filing_path), config.ARCHIVE_DIR)
        except Exception as e:
            logger.warning(f"Archiving failed (non-fatal): {e}")

    return state
