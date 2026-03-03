"""
tests/test_pipeline.py — Unit tests for all pipeline components.

All tests mock the LLM so the suite runs without API keys or network access.
Run with: pytest tests/ -v
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch, call
from pathlib import Path

import pytest

from src.schemas import DebtInstrument, FinancialProfile, PipelineState


# ─── Schema tests ───────────────────────────────────────────────────────────

class TestDebtInstrumentSchema:
    def test_amount_billions(self):
        assert DebtInstrument(name="X", amount="1.2B").amount == 1200.0

    def test_amount_millions(self):
        assert DebtInstrument(name="X", amount="500M").amount == 500.0

    def test_amount_thousands(self):
        assert DebtInstrument(name="X", amount="200K").amount == pytest.approx(0.2, rel=1e-3)

    def test_amount_with_commas_and_dollar(self):
        assert DebtInstrument(name="X", amount="$1,500").amount == 1500.0

    def test_amount_plain_float(self):
        assert DebtInstrument(name="X", amount=750.0).amount == 750.0

    def test_currency_uppercased(self):
        assert DebtInstrument(name="X", amount=100.0, currency="usd").currency == "USD"

    def test_xbrl_tag_optional(self):
        inst = DebtInstrument(name="X", amount=100.0)
        assert inst.xbrl_tag is None

    def test_maturity_year_optional(self):
        inst = DebtInstrument(name="X", amount=100.0)
        assert inst.maturity_year is None

    def test_invalid_amount_raises(self):
        with pytest.raises(Exception):
            DebtInstrument(name="X", amount="not_a_number")


class TestFinancialProfileSchema:
    def test_ticker_uppercased(self):
        p = FinancialProfile(ticker="aapl", period_ending="2023-09-30")
        assert p.ticker == "AAPL"

    def test_valid_date_accepted(self):
        p = FinancialProfile(ticker="AAPL", period_ending="2023-09-30")
        assert p.period_ending == "2023-09-30"

    def test_invalid_date_raises(self):
        with pytest.raises(Exception):
            FinancialProfile(ticker="AAPL", period_ending="30-09-2023")

    def test_empty_instruments_adds_note(self):
        p = FinancialProfile(ticker="AAPL", period_ending="2023-09-30")
        assert "No debt instruments" in (p.extraction_notes or "")

    def test_confidence_score_bounds(self):
        with pytest.raises(Exception):
            FinancialProfile(ticker="AAPL", period_ending="2023-09-30", confidence_score=1.5)

    def test_full_valid_profile(self):
        p = FinancialProfile(
            ticker="MSFT",
            period_ending="2023-06-30",
            debt_instruments=[
                DebtInstrument(name="Senior Notes", amount=5000.0,
                               maturity_year=2027, xbrl_tag="us-gaap:SeniorNotes")
            ],
            risks_summary="- Competition risk",
            confidence_score=0.95,
        )
        assert p.ticker == "MSFT"
        assert len(p.debt_instruments) == 1
        assert p.debt_instruments[0].xbrl_tag == "us-gaap:SeniorNotes"


# ─── XBRL utils tests ────────────────────────────────────────────────────────

class TestXBRLUtils:
    def test_valid_tag_parsed(self):
        from src.utils.xbrl_utils import parse_xbrl_tag
        result = parse_xbrl_tag("us-gaap:LongTermDebt")
        assert result is not None
        assert result.namespace == "us-gaap"
        assert result.concept == "LongTermDebt"
        assert result.is_debt_related

    def test_invalid_tag_returns_none(self):
        from src.utils.xbrl_utils import parse_xbrl_tag
        assert parse_xbrl_tag("bad_tag") is None
        assert parse_xbrl_tag("lowercase:concept") is None
        assert parse_xbrl_tag("") is None
        assert parse_xbrl_tag(None) is None

    def test_is_valid_xbrl_tag(self):
        from src.utils.xbrl_utils import is_valid_xbrl_tag
        assert is_valid_xbrl_tag("us-gaap:LongTermDebt") is True
        assert is_valid_xbrl_tag("dei:EntityCommonStockSharesOutstanding") is True
        assert is_valid_xbrl_tag("badformat") is False
        assert is_valid_xbrl_tag("us-gaap:lowercase") is False

    def test_debt_xbrl_extraction_filters_non_debt(self):
        from src.utils.xbrl_utils import extract_debt_xbrl_tags
        html = """
        <ix:nonFraction name="us-gaap:LongTermDebt" contextRef="FY2023">15000000000</ix:nonFraction>
        <ix:nonFraction name="us-gaap:Revenues" contextRef="FY2023">100000000000</ix:nonFraction>
        <ix:nonFraction name="us-gaap:SeniorNotes" contextRef="FY2023">5000000000</ix:nonFraction>
        """
        tags = extract_debt_xbrl_tags(html)
        names = [t["name"] for t in tags]
        assert "us-gaap:LongTermDebt" in names
        assert "us-gaap:SeniorNotes" in names
        assert "us-gaap:Revenues" not in names

    def test_amount_normalization_billions(self):
        from src.utils.xbrl_utils import normalize_xbrl_amount
        # 15 billion dollars in raw form → 15000 millions
        result = normalize_xbrl_amount("15000000000", scale=None, decimals=None)
        assert result == pytest.approx(15000.0, rel=0.01)

    def test_format_snippet_empty(self):
        from src.utils.xbrl_utils import format_xbrl_snippet
        result = format_xbrl_snippet([])
        assert "No inline XBRL" in result


# ─── Parse node tests ────────────────────────────────────────────────────────

class TestParseNode:
    SAMPLE_MARKDOWN = """
# Item 1A. Risk Factors

We face significant competition from larger companies with greater resources.
Market conditions may adversely affect our performance and financial results.
Regulatory changes could increase our costs and limit our ability to operate.
Our business depends on consumer spending which may fluctuate with economic conditions.
Supply chain disruptions could delay product availability and increase our operating costs significantly.

# Item 1B. Unresolved Staff Comments

None.

# Item 8. Financial Statements and Supplementary Data

Our total long-term debt as of December 31, 2023 was $15.0 billion.
This consisted of the following instruments:
- $5.0 billion of 3.875% Senior Notes due 2027, issued in March 2020
- $10.0 billion of 4.250% Senior Notes due 2032, issued in August 2021
Interest is payable semi-annually. The notes are unsecured obligations of the Company.
Fair value of long-term debt was approximately $14.2 billion as of December 31, 2023.

# Item 9. Changes in and Disagreements
"""

    def test_item_8_extracted(self):
        from src.nodes.parse_node import _extract_section
        result = _extract_section(self.SAMPLE_MARKDOWN, "item_8")
        assert result is not None
        assert "Senior Notes" in result
        assert "Item 1A" not in result

    def test_item_1a_extracted(self):
        from src.nodes.parse_node import _extract_section
        result = _extract_section(self.SAMPLE_MARKDOWN, "item_1a")
        assert result is not None
        assert "competition" in result
        assert "Financial Statements" not in result

    def test_section_not_found_returns_none(self):
        from src.nodes.parse_node import _extract_section
        result = _extract_section("No sections here.", "item_8")
        assert result is None

    def test_strip_html_fallback(self):
        from src.nodes.parse_node import _strip_html
        html = "<html><body><p>Hello <b>World</b></p></body></html>"
        result = _strip_html(html)
        assert "Hello" in result
        assert "World" in result
        assert "<" not in result


# ─── Mapping agent tests ─────────────────────────────────────────────────────

class TestMappingAgent:
    VALID_LLM_RESPONSE = json.dumps({
        "ticker": "AAPL",
        "filing_type": "10-K",
        "period_ending": "2023-09-30",
        "debt_instruments": [
            {
                "name": "3.45% Notes due 2024",
                "amount": 1000.0,
                "currency": "USD",
                "maturity_year": 2024,
                "xbrl_tag": "us-gaap:LongTermDebt",
            }
        ],
        "risks_summary": "- Competition: intense market competition\n- Regulatory risks",
        "confidence_score": 0.95,
        "extraction_notes": None,
    })

    def _make_state(self) -> PipelineState:
        return PipelineState(
            ticker="AAPL",
            item_8_markdown="Long-term debt: $1.0 billion 3.45% Notes due 2024",
            item_1a_markdown="We face competition risks.",
            xbrl_snippet="us-gaap:LongTermDebt | 1000.0M | ctx:FY2023",
        )

    @patch("src.nodes.mapping_agent._get_llm")
    def test_successful_extraction(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = MagicMock(content=self.VALID_LLM_RESPONSE)
        mock_get_llm.return_value = mock_llm

        from src.nodes.mapping_agent import mapping_agent_node
        result = mapping_agent_node(self._make_state())

        assert result.extracted_profile is not None
        assert len(result.extracted_profile.debt_instruments) == 1
        assert result.extracted_profile.debt_instruments[0].name == "3.45% Notes due 2024"
        assert result.confidence_score == 0.95

    @patch("src.nodes.mapping_agent._get_llm")
    def test_fenced_json_extracted(self, mock_get_llm):
        """Model wraps JSON in markdown fences — should still parse."""
        fenced = f"```json\n{self.VALID_LLM_RESPONSE}\n```"
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = MagicMock(content=fenced)
        mock_get_llm.return_value = mock_llm

        from src.nodes.mapping_agent import mapping_agent_node
        result = mapping_agent_node(self._make_state())
        assert result.extracted_profile is not None

    @patch("src.nodes.mapping_agent._get_llm")
    def test_invalid_json_sets_feedback(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = MagicMock(
            content="I cannot extract this filing because the text is unclear."
        )
        mock_get_llm.return_value = mock_llm

        from src.nodes.mapping_agent import mapping_agent_node
        result = mapping_agent_node(self._make_state())
        assert result.extracted_profile is None
        assert result.judge_feedback is not None
        assert "JSON" in result.judge_feedback

    @patch("src.nodes.mapping_agent._get_llm")
    def test_api_error_sets_feedback(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_llm.invoke.side_effect = Exception("API rate limit exceeded")
        mock_get_llm.return_value = mock_llm

        from src.nodes.mapping_agent import mapping_agent_node
        result = mapping_agent_node(self._make_state())
        assert result.extracted_profile is None
        assert "API error" in result.judge_feedback

    @patch("src.nodes.mapping_agent._get_llm")
    def test_ticker_always_injected(self, mock_get_llm):
        """Ticker in JSON output should be overridden by state.ticker."""
        response_wrong_ticker = json.loads(self.VALID_LLM_RESPONSE)
        response_wrong_ticker["ticker"] = "WRONG"
        mock_llm = MagicMock()
        mock_llm.invoke.return_value = MagicMock(
            content=json.dumps(response_wrong_ticker)
        )
        mock_get_llm.return_value = mock_llm

        from src.nodes.mapping_agent import mapping_agent_node
        state = self._make_state()
        state.ticker = "AAPL"
        result = mapping_agent_node(state)
        assert result.extracted_profile.ticker == "AAPL"


# ─── Judge node tests ────────────────────────────────────────────────────────

class TestJudgeNode:
    def _make_verified_state(self) -> PipelineState:
        profile = FinancialProfile(
            ticker="AAPL",
            period_ending="2023-09-30",
            debt_instruments=[
                DebtInstrument(
                    name="3.45% Notes",
                    amount=1000.0,
                    xbrl_tag="us-gaap:LongTermDebt",
                    maturity_year=2024,
                )
            ],
            confidence_score=0.95,
        )
        return PipelineState(
            ticker="AAPL",
            extracted_profile=profile,
            item_8_markdown=(
                "Long-term debt: $1.0 billion 3.45% Notes due 2024. "
                "Total debt: $1,000 million."
            ),
        )

    def test_valid_profile_passes(self):
        from src.nodes.judge_node import judge_node
        state = self._make_verified_state()
        result = judge_node(state)
        assert result.is_verified is True
        assert result.judge_feedback is None

    def test_low_confidence_fails(self):
        from src.nodes.judge_node import judge_node
        state = self._make_verified_state()
        state.extracted_profile.confidence_score = 0.5
        result = judge_node(state)
        assert result.is_verified is False
        assert "confidence" in result.judge_feedback.lower()

    def test_invalid_xbrl_tag_fails(self):
        from src.nodes.judge_node import judge_node
        state = self._make_verified_state()
        state.extracted_profile.debt_instruments[0].xbrl_tag = "invalid_tag_format"
        result = judge_node(state)
        assert result.is_verified is False
        assert "XBRL" in result.judge_feedback

    def test_future_date_fails(self):
        from src.nodes.judge_node import judge_node
        state = self._make_verified_state()
        state.extracted_profile.period_ending = "2035-12-31"
        result = judge_node(state)
        assert result.is_verified is False
        assert "future" in result.judge_feedback.lower()

    def test_empty_instruments_with_dollar_amounts_fails(self):
        from src.nodes.judge_node import judge_node
        profile = FinancialProfile(
            ticker="AAPL",
            period_ending="2023-09-30",
            debt_instruments=[],
            confidence_score=0.95,
        )
        # Need to manually remove extraction_notes to avoid triggering different issue
        profile.extraction_notes = None
        state = PipelineState(
            ticker="AAPL",
            extracted_profile=profile,
            item_8_markdown="Total long-term debt: $15.0 billion",
        )
        result = judge_node(state)
        assert result.is_verified is False

    def test_retry_count_incremented(self):
        from src.nodes.judge_node import judge_node
        state = self._make_verified_state()
        assert state.retry_count == 0
        result = judge_node(state)
        assert result.retry_count == 1

    def test_no_profile_sets_feedback(self):
        from src.nodes.judge_node import judge_node
        state = PipelineState(ticker="AAPL", extracted_profile=None)
        result = judge_node(state)
        assert result.is_verified is False
        assert result.judge_feedback is not None

    def test_should_retry_under_limit(self):
        from src.nodes.judge_node import should_retry
        state = PipelineState(ticker="AAPL", is_verified=False, retry_count=1)
        assert should_retry(state) == "retry"

    def test_should_proceed_when_verified(self):
        from src.nodes.judge_node import should_retry
        state = PipelineState(ticker="AAPL", is_verified=True, retry_count=1)
        assert should_retry(state) == "proceed"

    def test_should_proceed_at_max_retries(self):
        from src.nodes.judge_node import should_retry
        from config.settings import config
        state = PipelineState(
            ticker="AAPL",
            is_verified=False,
            retry_count=config.MAX_RETRY_LOOPS,
        )
        assert should_retry(state) == "proceed"

    def test_all_failures_collected(self):
        """Judge should report ALL failures, not just the first."""
        from src.nodes.judge_node import judge_node
        state = self._make_verified_state()
        # Trigger multiple failures
        state.extracted_profile.confidence_score = 0.3
        state.extracted_profile.debt_instruments[0].xbrl_tag = "bad:format:wrong"
        result = judge_node(state)
        assert result.is_verified is False
        # Both failures should appear in feedback
        assert "confidence" in result.judge_feedback.lower()
        assert "XBRL" in result.judge_feedback


# ─── Prompt builder tests ────────────────────────────────────────────────────

class TestPromptBuilder:
    def test_prompt_contains_ticker(self):
        from src.utils.prompt_builder import build_user_prompt
        prompt = build_user_prompt(
            ticker="AAPL",
            xbrl_snippet="us-gaap:LongTermDebt | 5000M",
            item_8_markdown="Long-term debt: $5.0 billion",
            item_1a_markdown="We face competition.",
        )
        assert "AAPL" in prompt

    def test_feedback_included_on_retry(self):
        from src.utils.prompt_builder import build_user_prompt
        prompt = build_user_prompt(
            ticker="AAPL",
            xbrl_snippet="",
            item_8_markdown="text",
            item_1a_markdown="",
            judge_feedback="confidence_score too low",
        )
        assert "confidence_score too low" in prompt
        assert "PREVIOUS ATTEMPT FEEDBACK" in prompt

    def test_no_feedback_on_first_attempt(self):
        from src.utils.prompt_builder import build_user_prompt
        prompt = build_user_prompt(
            ticker="AAPL",
            xbrl_snippet="",
            item_8_markdown="text",
            item_1a_markdown="",
            judge_feedback=None,
        )
        assert "first extraction attempt" in prompt.lower()

    def test_text_truncation(self):
        from src.utils.prompt_builder import build_user_prompt
        long_text = "x" * 100_000
        prompt = build_user_prompt(
            ticker="AAPL",
            xbrl_snippet="",
            item_8_markdown=long_text,
            item_1a_markdown=long_text,
            max_item8_chars=1000,
            max_item1a_chars=500,
        )
        # Prompt should be much shorter than the combined input
        assert len(prompt) < 5000


# ─── Integration smoke test (no LLM, no network) ────────────────────────────

class TestGraphStructure:
    def test_graph_compiles_without_checkpointer(self):
        from src.graph import build_graph
        graph = build_graph(use_checkpointer=False)
        assert graph is not None

    def test_graph_has_expected_nodes(self):
        from src.graph import build_graph
        graph = build_graph(use_checkpointer=False)
        node_names = set(graph.nodes.keys())
        for expected in ["ingest", "parse", "mapping_agent", "judge", "storage"]:
            assert expected in node_names, f"Missing node: {expected}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
