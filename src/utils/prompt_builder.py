"""
prompt_builder.py — Prompt construction utilities for the Mapping Agent.

Separating prompt logic from the node code makes prompts easier to:
  - Version and diff in git
  - A/B test different formulations
  - Override in tests without mocking the LLM
"""

from __future__ import annotations

from typing import Optional

SYSTEM_PROMPT = """You are a financial data extraction specialist with deep expertise in SEC filings and XBRL taxonomy. Your task is to extract debt instrument information from 10-K filing text and return it as structured JSON.

## Output Contract
Return ONLY a valid JSON object. No markdown fences, no explanatory text before or after the JSON. The JSON must match this exact schema:

{
  "ticker": "string",
  "filing_type": "10-K",
  "period_ending": "YYYY-MM-DD",
  "debt_instruments": [
    {
      "name": "string",
      "amount": float (in millions USD),
      "currency": "USD",
      "maturity_year": int or null,
      "xbrl_tag": "string or null"
    }
  ],
  "risks_summary": "string (markdown bullet list of top 5 risks using - prefix)",
  "confidence_score": float between 0.0 and 1.0,
  "extraction_notes": "string or null"
}

## Extraction Rules
1. **Amounts**: Always convert to millions USD. This is critical.
   - "$1.2 billion" or "1.2B" → 1200.0  (NEVER output 1.2)
   - "$12.3 billion" → 12300.0           (NEVER output 12.3)
   - "$78.3 billion" → 78300.0           (NEVER output 78.3)
   - "$500 million" or "500M" → 500.0
   - "$538 million" → 538.0
   - Raw XBRL values > 10^8 are in actual dollars → divide by 1,000,000
   - ALWAYS ask yourself: is this value already in millions, or do I need to multiply by 1000?
   - A value like "12.3" is WRONG if the source says "$12.3 billion" — correct value is 12300.0

2. **XBRL tags**: If the XBRL snippet contains a matching concept for an instrument,
   use the full tag verbatim (e.g., "us-gaap:LongTermDebt"). If no clear match exists,
   set xbrl_tag to null. NEVER invent tags.

3. **Maturity year**: Extract from phrases like "due 2028", "maturing in 2031",
   "2029 Notes", "Notes due January 15, 2027". Set null if not specified.

4. **Confidence scoring rubric**:
   - 0.95–1.00: Values explicitly stated with exact figures AND matching XBRL tags
   - 0.85–0.94: Values clearly stated in text but XBRL confirmation absent or partial
   - 0.70–0.84: Values found but required inference or conversion
   - Below 0.70: Values are ambiguous, estimated, or text was unclear/truncated

5. **extraction_notes**: REQUIRED if confidence < 0.9. List each uncertain field
   and explain WHY it was uncertain (e.g., "maturity_year for Senior Notes not found
   in provided text excerpt; may be in a footnote not included in this chunk").

6. **risks_summary**: Extract the top 5 most material risks from Item 1A.
   Format as markdown: "- [Risk title]: [1-sentence description]"
   If Item 1A is empty, write "- Risk factors not available in provided excerpt."

## Few-Shot Example

### XBRL Snippet:
us-gaap:LongTermDebt | 15000.0M | ctx:FY2023_Annual
us-gaap:SeniorNotes | 5000.0M | ctx:FY2023_Annual

### Item 8 Text (excerpt):
"As of December 31, 2023, our long-term debt consisted of:
3.875% Senior Notes due 2027: $5,000 million
4.250% Senior Notes due 2032: $10,000 million
Total long-term debt: $15,000 million"

### Item 1A Text (excerpt):
"We face intense competition. Our business depends on consumer spending.
Regulatory changes may increase our costs. Exchange rate fluctuations affect revenue.
Our supply chain is subject to disruption."

### Correct JSON output:
{
  "ticker": "EXAMPLE",
  "filing_type": "10-K",
  "period_ending": "2023-12-31",
  "debt_instruments": [
    {
      "name": "3.875% Senior Notes",
      "amount": 5000.0,
      "currency": "USD",
      "maturity_year": 2027,
      "xbrl_tag": "us-gaap:SeniorNotes"
    },
    {
      "name": "4.250% Senior Notes",
      "amount": 10000.0,
      "currency": "USD",
      "maturity_year": 2032,
      "xbrl_tag": "us-gaap:LongTermDebt"
    }
  ],
  "risks_summary": "- Competition: Faces intense competitive pressure in all markets\\n- Consumer spending: Revenue depends on discretionary consumer spending levels\\n- Regulatory: Changes in regulations may materially increase operating costs\\n- Currency: Foreign exchange rate fluctuations impact international revenue\\n- Supply chain: Disruptions could delay product availability and increase costs",
  "confidence_score": 0.97,
  "extraction_notes": null
}"""


USER_PROMPT_TEMPLATE = """Extract all debt instruments from the following 10-K filing data for {ticker}.

Determine period_ending from dates mentioned in the text (e.g., "As of December 31, 2023" → "2023-12-31").

---
## XBRL DEBT TAGS (use for concept tag matching):
{xbrl_snippet}

---
## ITEM 8 — FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA:
{item_8_markdown}

---
## ITEM 1A — RISK FACTORS:
{item_1a_markdown}

---
{feedback_section}

Return the JSON object now:"""


def build_user_prompt(
    ticker: str,
    xbrl_snippet: str,
    item_8_markdown: str,
    item_1a_markdown: str,
    judge_feedback: Optional[str] = None,
    max_item8_chars: int = 38_000,
    max_item1a_chars: int = 14_000,
) -> str:
    """
    Build the user-turn prompt for the Mapping Agent.

    Args:
        ticker:           Company ticker symbol
        xbrl_snippet:     Formatted XBRL tags string from xbrl_utils
        item_8_markdown:  Financial statements section text
        item_1a_markdown: Risk factors section text
        judge_feedback:   Error feedback from previous judge run (retry only)
        max_item8_chars:  Truncation limit for Item 8 (keeps within LLM context)
        max_item1a_chars: Truncation limit for Item 1A

    Returns:
        Formatted prompt string ready to send to the LLM.
    """
    feedback_section = (
        f"## PREVIOUS ATTEMPT FEEDBACK (fix these issues):\n{judge_feedback}"
        if judge_feedback
        else "## NOTE: This is the first extraction attempt."
    )

    return USER_PROMPT_TEMPLATE.format(
        ticker=ticker.upper(),
        xbrl_snippet=xbrl_snippet or "(No XBRL data available)",
        item_8_markdown=item_8_markdown[:max_item8_chars]
        if item_8_markdown
        else "(Not available)",
        item_1a_markdown=item_1a_markdown[:max_item1a_chars]
        if item_1a_markdown
        else "(Not available)",
        feedback_section=feedback_section,
    )
