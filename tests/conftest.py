"""
conftest.py — Shared pytest fixtures and test configuration.
"""
import os
import pytest

# Set test environment variables before any imports
os.environ.setdefault("LLM_PROVIDER", "groq")
os.environ.setdefault("GROQ_API_KEY", "test_key_not_real")
os.environ.setdefault("SEC_USER_AGENT", "Test User test@test.com")
os.environ.setdefault("DRY_RUN", "true")
os.environ.setdefault("CONFIDENCE_THRESHOLD", "0.9")
os.environ.setdefault("MAX_RETRY_LOOPS", "3")
os.environ.setdefault("STORAGE_DIR", "/tmp/afip_test/filings")
os.environ.setdefault("OUTPUT_DIR", "/tmp/afip_test/output")
os.environ.setdefault("CHECKPOINT_DB", "/tmp/afip_test/checkpoints/test.db")
os.environ.setdefault("LOG_DIR", "/tmp/afip_test/logs")


@pytest.fixture(autouse=True)
def clean_test_dirs(tmp_path):
    """Ensure test output directories exist and are clean."""
    for subdir in ["filings", "output", "checkpoints", "logs"]:
        (tmp_path / subdir).mkdir(parents=True, exist_ok=True)
    yield
