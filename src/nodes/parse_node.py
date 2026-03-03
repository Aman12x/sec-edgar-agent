"""
parse_node.py — Phase 2: Convert HTML filing to structured Markdown.

Uses Docling (IBM Research) for layout-aware HTML → Markdown conversion,
then extracts Item 8 (Financial Statements) and Item 1A (Risk Factors)
using cascading regex patterns to handle the wide variation in 10-K formats.
XBRL tag extraction is delegated to src/utils/xbrl_utils.py.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from config.settings import config
from src.schemas import PipelineState
from src.utils.logger import get_logger
from src.utils.xbrl_utils import extract_debt_xbrl_tags, format_xbrl_snippet

logger = get_logger("parse_node")


# ── Section extraction patterns ────────────────────────────────────────────────
# Two sets of patterns: markdown-heading format (post-Docling) and plain-text
# uppercase format (fallback for filings where Docling doesn't detect headings).

_MD_PATTERNS = {
    "item_1a": re.compile(
        r"(?:^|\n)#{1,4}\s*Item\s+1A[.\s:–—]*Risk\s+Factors"
        r"(.*?)"
        r"(?=\n#{1,4}\s*Item\s+1B|\n#{1,4}\s*Item\s+2|\Z)",
        re.DOTALL | re.IGNORECASE,
    ),
    "item_8": re.compile(
        r"(?:^|\n)#{1,4}\s*Item\s+8[.\s:–—]*(?:Financial\s+Statements?[^\n]*)"
        r"(.*?)"
        r"(?=\n#{1,4}\s*Item\s+9|\Z)",
        re.DOTALL | re.IGNORECASE,
    ),
}

_PLAIN_PATTERNS = {
    "item_1a": re.compile(
        r"ITEM\s+1A[.\s:–—]*RISK\s+FACTORS(.*?)(?=ITEM\s+1B|ITEM\s+2|\Z)",
        re.DOTALL | re.IGNORECASE,
    ),
    "item_8": re.compile(
        r"ITEM\s+8[.\s:–—]*FINANCIAL\s+STATEMENTS?(.*?)(?=ITEM\s+9|\Z)",
        re.DOTALL | re.IGNORECASE,
    ),
}

# Additional flexible patterns for companies with non-standard headers (e.g. MSFT)
_FLEXIBLE_PATTERNS = {
    "item_8": re.compile(
        r"(?:^|\n)#{1,4}\s*Item\s+8\b(.*?)" r"(?=\n#{1,4}\s*Item\s+9|\Z)",
        re.DOTALL | re.IGNORECASE,
    ),
    "item_1a": re.compile(
        r"(?:^|\n)#{1,4}\s*Item\s+1A\b(.*?)"
        r"(?=\n#{1,4}\s*Item\s+1B|\n#{1,4}\s*Item\s+2|\Z)",
        re.DOTALL | re.IGNORECASE,
    ),
}

# Minimum characters for a real section (filters out TOC entries)
_MIN_SECTION_CHARS = 300


def _html_to_markdown(html_content: str, file_path: Optional[str] = None) -> str:
    """
    Convert HTML filing to Markdown via Docling.
    Falls back to plain HTML stripping if Docling is unavailable or fails.
    """
    try:
        from docling.document_converter import DocumentConverter
        from docling.datamodel.base_models import InputFormat
        from docling.document_converter import HTMLFormatOption

        converter = DocumentConverter(
            format_options={InputFormat.HTML: HTMLFormatOption()}
        )

        if file_path and Path(file_path).exists():
            result = converter.convert(file_path)
        else:
            import tempfile

            with tempfile.NamedTemporaryFile(
                suffix=".html", mode="w", encoding="utf-8", delete=False
            ) as tmp:
                tmp.write(html_content)
                tmp_path = tmp.name
            result = converter.convert(tmp_path)

        md = result.document.export_to_markdown()
        logger.debug(f"Docling conversion successful: {len(md):,} chars")
        return md

    except ImportError:
        logger.warning("Docling not installed — using HTML fallback parser")
        return _strip_html(html_content)
    except Exception as e:
        logger.warning(f"Docling failed ({e}) — using HTML fallback parser")
        return _strip_html(html_content)


def _strip_html(html: str) -> str:
    """Minimal HTML → text fallback: strip tags, unescape entities."""
    import html as html_lib

    text = re.sub(
        r"<style[^>]*>.*?</style>", " ", html, flags=re.DOTALL | re.IGNORECASE
    )
    text = re.sub(
        r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE
    )
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_section(markdown: str, key: str) -> Optional[str]:
    """
    Extract a named section from Markdown using cascading patterns.
    Returns None if the section cannot be found with sufficient content.
    """
    for pattern_set in (_MD_PATTERNS, _PLAIN_PATTERNS, _FLEXIBLE_PATTERNS):
        if key not in pattern_set:
            continue
        match = pattern_set[key].search(markdown)
        if match:
            content = match.group(1).strip()
            if len(content) >= _MIN_SECTION_CHARS:
                return content

    logger.warning(f"Could not extract {key} — section not found or too short")
    return None


def parse_node(state: PipelineState) -> PipelineState:
    """
    LangGraph node: Convert raw HTML to Markdown, extract Item 8 and Item 1A.

    Reads from state:  raw_html, filing_path
    Writes to state:   item_8_markdown, item_1a_markdown, xbrl_snippet
    """
    if not state.raw_html:
        raise ValueError(
            "parse_node requires raw_html in state (run ingest_node first)"
        )

    logger.info(f"Converting {state.ticker} filing to Markdown via Docling")

    # Step 1: HTML → Markdown
    markdown = _html_to_markdown(state.raw_html, state.filing_path)

    # Step 2: Extract target sections
    item_8 = _extract_section(markdown, "item_8")
    item_1a = _extract_section(markdown, "item_1a")

    if item_8:
        item_8 = item_8[: config.MAX_ITEM8_CHARS]
        logger.info(f"Item 8 extracted: {len(item_8):,} chars")
    else:
        logger.warning(
            f"Item 8 not found for {state.ticker} — using full document fallback"
        )
        item_8 = markdown[: config.MAX_ITEM8_CHARS]

    if item_1a:
        item_1a = item_1a[: config.MAX_ITEM1A_CHARS]
        logger.info(f"Item 1A extracted: {len(item_1a):,} chars")
    else:
        item_1a = ""

    # Step 3: Extract XBRL tags (from raw HTML, not Markdown)
    xbrl_tags = extract_debt_xbrl_tags(state.raw_html)
    xbrl_snippet = format_xbrl_snippet(
        xbrl_tags, max_tags=20
    )  # cap to stay under TPM limit
    logger.info(f"XBRL: found {len(xbrl_tags)} debt tags")

    state.item_8_markdown = item_8
    state.item_1a_markdown = item_1a
    state.xbrl_snippet = xbrl_snippet
    return state
