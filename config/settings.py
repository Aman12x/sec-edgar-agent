"""
settings.py — Centralised configuration loaded from environment variables.

All pipeline configuration flows through this single Config class.
Environment variables can be set in .env (local dev) or injected by
Prefect's secret management at deploy time.

Design principle: fail loudly at startup if required config is missing,
not silently mid-run when a config value is first used.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── LLM Provider ─────────────────────────────────────────────────────────
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "groq")          # "groq" or "ollama"
    GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
    OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "llama-3.2-90b-text-preview")
    LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0.1"))
    LLM_MAX_TOKENS: int = int(os.getenv("LLM_MAX_TOKENS", "4096"))

    # ── SEC EDGAR ─────────────────────────────────────────────────────────────
    SEC_USER_AGENT: str = os.getenv("SEC_USER_AGENT", "AFIP Pipeline afip@example.com")
    SEC_RATE_LIMIT: int = int(os.getenv("SEC_RATE_LIMIT_PER_SECOND", "10"))
    SEC_MAX_CONCURRENT: int = int(os.getenv("SEC_MAX_CONCURRENT", "10"))

    # ── Storage ───────────────────────────────────────────────────────────────
    BASE_DIR: Path = Path(os.getenv("BASE_DIR", "."))
    DATA_DIR: Path = Path(os.getenv("DATA_DIR", "./data"))
    STORAGE_DIR: Path = Path(os.getenv("STORAGE_DIR", "./data/filings"))
    OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", "./data/output"))
    ARCHIVE_DIR: Path = Path(os.getenv("ARCHIVE_DIR", "./data/archive"))
    CHECKPOINT_DB: str = os.getenv("CHECKPOINT_DB", "./data/checkpoints/pipeline.db")
    LOG_DIR: Path = Path(os.getenv("LOG_DIR", "./logs"))

    # ── Pipeline Behavior ─────────────────────────────────────────────────────
    CONFIDENCE_THRESHOLD: float = float(os.getenv("CONFIDENCE_THRESHOLD", "0.9"))
    MAX_RETRY_LOOPS: int = int(os.getenv("MAX_RETRY_LOOPS", "3"))
    MAX_ITEM8_CHARS: int = int(os.getenv("MAX_ITEM8_CHARS", "40000"))
    MAX_ITEM1A_CHARS: int = int(os.getenv("MAX_ITEM1A_CHARS", "15000"))

    # ── Prefect Scheduling ────────────────────────────────────────────────────
    PREFECT_SCHEDULE: str = os.getenv("PREFECT_SCHEDULE", "0 17 * * 1-5")
    PREFECT_TIMEZONE: str = os.getenv("PREFECT_TIMEZONE", "America/New_York")
    PREFECT_MAX_CONCURRENT_TICKERS: int = int(os.getenv("PREFECT_MAX_CONCURRENT_TICKERS", "5"))

    # ── LangSmith Tracing ─────────────────────────────────────────────────────
    LANGCHAIN_TRACING_V2: str = os.getenv("LANGCHAIN_TRACING_V2", "false")
    LANGCHAIN_API_KEY: str = os.getenv("LANGCHAIN_API_KEY", "")
    LANGCHAIN_PROJECT: str = os.getenv("LANGCHAIN_PROJECT", "afip")
    LANGCHAIN_ENDPOINT: str = os.getenv("LANGCHAIN_ENDPOINT", "https://api.smith.langchain.com")

    # ── Feature Flags ─────────────────────────────────────────────────────────
    ARCHIVE_AFTER_PROCESSING: bool = os.getenv("ARCHIVE_AFTER_PROCESSING", "false").lower() == "true"
    SAVE_LLM_RAW_RESPONSE: bool = os.getenv("SAVE_LLM_RAW_RESPONSE", "false").lower() == "true"
    DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"

    @classmethod
    def ensure_dirs(cls) -> None:
        for d in [cls.STORAGE_DIR, cls.OUTPUT_DIR, cls.ARCHIVE_DIR, cls.LOG_DIR]:
            Path(d).mkdir(parents=True, exist_ok=True)
        Path(cls.CHECKPOINT_DB).parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate(cls) -> None:
        errors = []
        if cls.LLM_PROVIDER == "groq" and not cls.GROQ_API_KEY:
            errors.append(
                "GROQ_API_KEY is required when LLM_PROVIDER=groq. "
                "Get a key at https://console.groq.com"
            )
        if cls.LLM_PROVIDER not in ("groq", "ollama"):
            errors.append(f"LLM_PROVIDER must be 'groq' or 'ollama', got: {cls.LLM_PROVIDER}")
        if "example.com" in cls.SEC_USER_AGENT or not cls.SEC_USER_AGENT:
            errors.append(
                "SEC_USER_AGENT must contain your real name and email, e.g.: "
                "'John Smith jsmith@company.com'. SEC blocks requests without this."
            )
        if not (0.0 < cls.CONFIDENCE_THRESHOLD <= 1.0):
            errors.append(
                f"CONFIDENCE_THRESHOLD must be between 0.0 and 1.0, got: {cls.CONFIDENCE_THRESHOLD}"
            )
        if errors:
            raise EnvironmentError(
                "AFIP configuration errors:\n" + "\n".join(f"  - {e}" for e in errors)
            )

    @classmethod
    def summary(cls) -> str:
        return (
            f"LLM: {cls.LLM_PROVIDER}/{cls.LLM_MODEL} | "
            f"Confidence threshold: {cls.CONFIDENCE_THRESHOLD} | "
            f"Max retries: {cls.MAX_RETRY_LOOPS} | "
            f"SEC rate limit: {cls.SEC_RATE_LIMIT}/s | "
            f"Output: {cls.OUTPUT_DIR}"
        )


config = Config()
