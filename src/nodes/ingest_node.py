"""
ingest_node.py — Phase 1: Download 10-K filings from SEC EDGAR.

Key behaviors:
  - Token-bucket rate limiter (RateLimiter) enforces SEC's 10 req/s policy
  - tenacity retries with exponential backoff for transient network errors
  - DRY_RUN mode skips actual download (uses existing cached filing if present)
  - Validates SEC_USER_AGENT at node entry, not just at startup
"""

from __future__ import annotations

from pathlib import Path

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging

from config.settings import config
from src.schemas import PipelineState
from src.utils.logger import get_logger
from src.utils.rate_limiter import sec_rate_limited

logger = get_logger("ingest_node")


class IngestError(Exception):
    """Raised when a filing cannot be downloaded after all retries."""

    pass


@sec_rate_limited
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    reraise=True,
)
def _download_filing(ticker: str, filing_type: str, save_path: str) -> None:
    """Download filing with rate limiting and retry logic."""
    from sec_edgar_downloader import Downloader

    # Extract email from SEC_USER_AGENT for Downloader constructor
    user_agent_parts = config.SEC_USER_AGENT.strip().split()
    email = next((p for p in user_agent_parts if "@" in p), "user@example.com")
    company = " ".join(p for p in user_agent_parts if "@" not in p) or "AFIP"

    # API changed in sec-edgar-downloader v5+: positional args only
    dl = Downloader(company, email, save_path)
    dl.get(filing_type, ticker, limit=1)


def _find_latest_filing(ticker: str, filing_type: str) -> Path:
    """Locate the most recently downloaded filing HTML file."""
    filing_dir = config.STORAGE_DIR / "sec-edgar-filings" / ticker.upper() / filing_type

    if not filing_dir.exists():
        raise IngestError(f"No filing directory found at {filing_dir}")

    # Find most recent download directory (by modification time)
    subdirs = sorted(
        [d for d in filing_dir.iterdir() if d.is_dir()],
        key=lambda d: d.stat().st_mtime,
    )
    if not subdirs:
        raise IngestError(f"No filing subdirectories under {filing_dir}")

    latest = subdirs[-1]

    # Try .htm/.html files first (older downloader versions)
    htm_files = sorted(
        list(latest.glob("*.htm")) + list(latest.glob("*.html")),
        key=lambda f: f.stat().st_size,
    )
    if htm_files:
        return htm_files[-1]

    # Newer sec-edgar-downloader saves full-submission.txt
    txt_file = latest / "full-submission.txt"
    if txt_file.exists():
        return _extract_html_from_submission(txt_file, latest)

    raise IngestError(f"No usable filing files in {latest}")


def _extract_html_from_submission(txt_path: Path, output_dir: Path) -> Path:
    """
    Extract the primary HTML document from a full-submission.txt SGML file.
    EDGAR multipart files contain all documents; we want the largest non-exhibit block.
    """
    import re

    content = txt_path.read_text(encoding="utf-8", errors="replace")

    doc_pattern = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.DOTALL)
    type_pattern = re.compile(r"<TYPE>([^\n]+)")
    text_pattern = re.compile(r"<TEXT>(.*?)</TEXT>", re.DOTALL)

    best_doc = None
    best_size = 0

    for match in doc_pattern.finditer(content):
        doc_block = match.group(1)
        type_match = type_pattern.search(doc_block)
        doc_type = type_match.group(1).strip() if type_match else ""

        # Skip exhibits and binary attachments
        if doc_type.startswith("EX-") or doc_type in ("GRAPHIC", "ZIP", "PDF", "XML"):
            continue

        text_match = text_pattern.search(doc_block)
        if not text_match:
            continue

        text_content = text_match.group(1).strip()
        if len(text_content) > best_size:
            best_size = len(text_content)
            best_doc = text_content

    if not best_doc:
        # Fallback: just use the whole file as-is
        logger.warning(
            "Could not parse DOCUMENT blocks — using full submission text as fallback"
        )
        best_doc = content

    out_path = output_dir / "primary-document.htm"
    out_path.write_text(best_doc, encoding="utf-8", errors="replace")
    logger.info(
        f"Extracted primary document ({len(best_doc):,} chars) → {out_path.name}"
    )
    return out_path


def ingest_node(state: PipelineState) -> PipelineState:
    """
    LangGraph node: Download the most recent filing for state.ticker.

    Reads from state:  ticker, filing_type
    Writes to state:   filing_path, raw_html
    """
    config.ensure_dirs()

    # Validate SEC User-Agent before making any network calls
    if not config.SEC_USER_AGENT or "example.com" in config.SEC_USER_AGENT:
        raise EnvironmentError(
            "SEC_USER_AGENT must be set to 'Your Name your@email.com'. "
            "SEC will block requests without a valid contact header. "
            "Set this in your .env file."
        )

    logger.info(f"Fetching {state.filing_type} for {state.ticker}")

    # DRY_RUN: skip download, use existing cached filing
    if config.DRY_RUN:
        logger.warning(
            "DRY_RUN=true — skipping SEC download, looking for cached filing"
        )
        try:
            filing_path = _find_latest_filing(state.ticker, state.filing_type)
            logger.info(f"DRY_RUN: Using cached filing: {filing_path}")
        except IngestError as e:
            raise IngestError(f"DRY_RUN enabled but no cached filing found: {e}")
    else:
        try:
            _download_filing(state.ticker, state.filing_type, str(config.STORAGE_DIR))
        except Exception as e:
            raise IngestError(
                f"Failed to download {state.filing_type} for {state.ticker} "
                f"after 3 retries: {e}"
            ) from e

        filing_path = _find_latest_filing(state.ticker, state.filing_type)

    raw_html = filing_path.read_text(encoding="utf-8", errors="replace")

    logger.info(
        f"Downloaded {state.ticker}: {filing_path.name} ({len(raw_html):,} chars)"
    )

    state.filing_path = str(filing_path)
    state.raw_html = raw_html
    return state
