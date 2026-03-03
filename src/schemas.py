"""
schemas.py — Pydantic models for the FinancialProfile target schema and
internal pipeline state objects.

Why Pydantic v2?
  - Strict type enforcement at runtime catches LLM hallucinations early
  - .model_dump() / .model_validate() provide clean JSON serialization
  - Field-level validators let us normalize amounts (e.g. "1.2B" → 1200.0)
"""

from __future__ import annotations

import re
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Target output schema (Section 3 of spec)
# ---------------------------------------------------------------------------

class DebtInstrument(BaseModel):
    name: str = Field(..., description="Name or label of the debt instrument")
    amount: float = Field(..., description="Principal amount in millions USD")
    currency: str = Field(default="USD", description="ISO 4217 currency code")
    maturity_year: Optional[int] = Field(None, description="Year of maturity")
    xbrl_tag: Optional[str] = Field(
        None,
        description="XBRL concept tag, e.g. us-gaap:LongTermDebt"
    )

    @field_validator("amount", mode="before")
    @classmethod
    def normalize_amount(cls, v: object) -> float:
        """
        Accept numeric strings with scale suffixes.
        '1.2B' → 1200.0, '500M' → 500.0, '$2,400' → 2400.0
        """
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            v = v.replace(",", "").replace("$", "").strip()
            multipliers = {"B": 1000.0, "M": 1.0, "K": 0.001}
            for suffix, mult in multipliers.items():
                if v.upper().endswith(suffix):
                    return float(v[:-1]) * mult
            return float(v)
        raise ValueError(f"Cannot parse amount: {v}")

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, v: str) -> str:
        return v.upper()


class FinancialProfile(BaseModel):
    """
    The canonical output schema produced by the pipeline for each 10-K filing.
    Every downstream consumer (database writer, alerting system) works from
    this model.
    """
    ticker: str = Field(..., description="Stock ticker symbol, e.g. AAPL")
    filing_type: str = Field(default="10-K")
    period_ending: str = Field(
        ...,
        description="Fiscal period end date in YYYY-MM-DD format"
    )
    debt_instruments: list[DebtInstrument] = Field(default_factory=list)
    risks_summary: str = Field(
        default="",
        description="Markdown-formatted summary of Item 1A Risk Factors"
    )

    # Metadata added by the pipeline (not in LLM output)
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0)
    extraction_notes: Optional[str] = None

    @field_validator("ticker")
    @classmethod
    def uppercase_ticker(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("period_ending")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        try:
            date.fromisoformat(v)
        except ValueError:
            raise ValueError(f"period_ending must be YYYY-MM-DD, got: {v}")
        return v

    @model_validator(mode="after")
    def ensure_debt_instruments_populated(self) -> "FinancialProfile":
        # Warn (not error) if no instruments found — judge node handles this
        if not self.debt_instruments:
            if not self.extraction_notes:
                self.extraction_notes = "No debt instruments extracted."
        return self


# ---------------------------------------------------------------------------
# Internal pipeline state (passed between LangGraph nodes)
# ---------------------------------------------------------------------------

class PipelineState(BaseModel):
    """
    The mutable state object threaded through LangGraph nodes.
    Each node reads what it needs and writes its outputs back to this object.
    LangGraph's Checkpointer serializes this to SQLite for resume support.
    """
    # --- Inputs ---
    ticker: str
    filing_type: str = "10-K"

    # --- Ingest Node outputs ---
    filing_path: Optional[str] = None          # local path to downloaded filing
    raw_html: Optional[str] = None             # raw HTML content

    # --- Parse Node outputs ---
    item_8_markdown: Optional[str] = None      # Financial Statements section
    item_1a_markdown: Optional[str] = None     # Risk Factors section
    xbrl_snippet: Optional[str] = None         # Extracted XBRL tags

    # --- Mapping Agent outputs ---
    extracted_profile: Optional[FinancialProfile] = None
    llm_raw_response: Optional[str] = None    # for debugging

    # --- Judge Node outputs ---
    confidence_score: float = 0.0
    judge_feedback: Optional[str] = None      # error log passed back to agent
    retry_count: int = 0

    # --- Final state ---
    is_verified: bool = False
    output_path: Optional[str] = None         # path to saved JSON

    class Config:
        arbitrary_types_allowed = True
