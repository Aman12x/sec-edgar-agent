"""
file_utils.py — Filesystem helpers used across pipeline nodes.

Centralising file I/O here means node code stays focused on business logic,
and we have one place to change serialization (e.g., switch to orjson for speed).
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from src.utils.logger import get_logger

logger = get_logger("file_utils")


def ensure_dirs(*paths: Path) -> None:
    """Create directories (including parents) if they don't exist."""
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def safe_write_json(data: Any, path: Path, indent: int = 2) -> Path:
    """
    Atomically write JSON to a file.

    Uses a temp file + rename to prevent partially-written files from being
    read by concurrent consumers if the process crashes mid-write.
    """
    path = Path(path)
    ensure_dirs(path.parent)

    tmp_path = path.with_suffix(".tmp")
    try:
        tmp_path.write_text(json.dumps(data, indent=indent, default=str), encoding="utf-8")
        tmp_path.rename(path)
        logger.debug(f"Wrote JSON: {path}")
        return path
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def load_json(path: Path) -> Any:
    """Load and parse a JSON file. Raises FileNotFoundError if missing."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def archive_filing(filing_path: Path, archive_dir: Path) -> Path:
    """
    Move a processed filing to an archive directory.
    Preserves directory structure under archive_dir.
    Used by storage_node to keep the data/ directory tidy.
    """
    archive_dir = Path(archive_dir)
    dest = archive_dir / filing_path.name
    ensure_dirs(dest.parent)
    shutil.move(str(filing_path), str(dest))
    logger.debug(f"Archived filing: {filing_path} → {dest}")
    return dest


def list_output_files(output_dir: Path, ticker: Optional[str] = None) -> list[Path]:
    """
    List all FinancialProfile JSON files in the output directory.
    Optionally filter by ticker symbol.
    """
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return []

    pattern = f"{ticker.upper()}_*.json" if ticker else "*.json"
    files = sorted(output_dir.glob(pattern), key=lambda f: f.stat().st_mtime, reverse=True)
    return [f for f in files if not f.name.endswith("_risks.md")]


def get_latest_output(output_dir: Path, ticker: str) -> Optional[Path]:
    """Return the most recent FinancialProfile JSON for a given ticker."""
    files = list_output_files(output_dir, ticker)
    return files[0] if files else None
