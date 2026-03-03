"""
logger.py — Centralised logging configuration using loguru.

All pipeline components call get_logger(__name__) to get a named logger.
Log output goes to:
  - stderr (console) with color formatting for development
  - logs/afip_{date}.log for production runs (auto-rotated daily, 7-day retention)

Format includes the node name so log lines are immediately traceable
to the pipeline step that produced them.
"""

import sys
from pathlib import Path
from loguru import logger as _loguru_logger


_configured = False


def get_logger(name: str = "afip"):
    """
    Return a loguru logger bound with the given name as a context field.
    Configures sinks on first call (idempotent).
    """
    global _configured

    if not _configured:
        _loguru_logger.remove()  # Remove default sink

        # Console sink — colored, human-readable
        _loguru_logger.add(
            sys.stderr,
            format=(
                "<green>{time:HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{extra[module]}</cyan> | "
                "{message}"
            ),
            level="DEBUG",
            colorize=True,
        )

        # File sink — full detail, rotated daily
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        _loguru_logger.add(
            log_dir / "afip_{time:YYYY-MM-DD}.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[module]} | {message}",
            level="DEBUG",
            rotation="00:00",       # new file each day
            retention="7 days",     # keep 7 days of logs
            compression="zip",
        )

        _configured = True

    return _loguru_logger.bind(module=name)
