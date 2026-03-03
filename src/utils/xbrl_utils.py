"""
xbrl_utils.py — XBRL tag parsing, validation, and normalization utilities.

XBRL (eXtensible Business Reporting Language) is an XML-based standard used
in SEC filings to tag financial data with machine-readable identifiers.
These utilities help the pipeline work with inline XBRL (iXBRL) embedded
directly in HTML 10-K filings.

Key XBRL concepts:
  - Namespace: "us-gaap" (US GAAP), "dei" (Document and Entity Information),
               "ifrs-full" (IFRS), company-specific extension namespaces
  - Concept:   CamelCase identifier, e.g. "LongTermDebt", "SeniorNotes"
  - Full tag:  "us-gaap:LongTermDebt"
  - Context:   Reference to the reporting period and entity (e.g. "FY2023")
"""

from __future__ import annotations

import re
from typing import Optional, NamedTuple

# Standard XBRL tag format: namespace:CamelCaseConcept
XBRL_TAG_RE = re.compile(r"^([a-z][a-z0-9\-]*):([A-Z][a-zA-Z0-9]+)$")

# Inline XBRL numeric element pattern
IXBRL_NUMERIC_RE = re.compile(
    r"<ix:nonFraction[^>]+name=['\"]([^'\"]+)['\"][^>]*"
    r"(?:contextRef=['\"]([^'\"]*)['\"])?[^>]*"
    r"(?:decimals=['\"]([^'\"]*)['\"])?[^>]*"
    r"(?:scale=['\"]([^'\"]*)['\"])?[^>]*"
    r">([\d,.\-]+)</ix:nonFraction>",
    re.IGNORECASE | re.DOTALL,
)

# Inline XBRL text element pattern (for dates, strings)
IXBRL_TEXT_RE = re.compile(
    r"<ix:nonNumeric[^>]+name=['\"]([^'\"]+)['\"][^>]*" r">(.*?)</ix:nonNumeric>",
    re.IGNORECASE | re.DOTALL,
)

# Debt-related XBRL concepts — used to filter the XBRL snippet
DEBT_CONCEPTS = frozenset(
    {
        "LongTermDebt",
        "LongTermDebtCurrent",
        "LongTermDebtNoncurrent",
        "LongTermDebtAndCapitalLeaseObligations",
        "ShortTermBorrowings",
        "NotesPayable",
        "NotesPayableCurrent",
        "NotesPayableRelatedPartiesCurrentAndNoncurrent",
        "DebtInstrumentCarryingAmount",
        "DebtInstrumentFaceAmount",
        "LongTermNotesPayable",
        "SeniorNotes",
        "SeniorLongTermNotes",
        "UnsecuredDebt",
        "SecuredDebt",
        "ConvertibleNotesPayable",
        "CommercialPaper",
        "LineOfCreditFacilityMaximumBorrowingCapacity",
        "FinanceLeaseLiability",
        "FinanceLeaseLiabilityCurrent",
        "FinanceLeaseLiabilityNoncurrent",
        "OperatingLeaseLiability",
        "LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths",
        "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo",
        "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree",
        "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour",
        "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFive",
        "DebtCurrent",
        "LongTermLineOfCredit",
        "CapitalLeaseObligations",
        "MediumTermNotes",
        "SubordinatedDebt",
        "TrustPreferredSecurities",
    }
)

# Known XBRL namespaces
KNOWN_NAMESPACES = {
    "us-gaap": "US GAAP taxonomy",
    "dei": "Document and Entity Information",
    "ifrs-full": "IFRS full taxonomy",
    "srt": "SEC Reporting Taxonomy",
    "us-roles": "US GAAP Roles taxonomy",
}


class XBRLTag(NamedTuple):
    """Parsed XBRL tag components."""

    namespace: str
    concept: str
    full_tag: str

    @property
    def is_us_gaap(self) -> bool:
        return self.namespace == "us-gaap"

    @property
    def is_debt_related(self) -> bool:
        return self.concept in DEBT_CONCEPTS


def parse_xbrl_tag(tag: str) -> Optional[XBRLTag]:
    """
    Parse a full XBRL tag string into its components.

    Returns None if the tag does not match the expected format.

    Examples:
        parse_xbrl_tag("us-gaap:LongTermDebt")
        → XBRLTag(namespace="us-gaap", concept="LongTermDebt", full_tag="us-gaap:LongTermDebt")
    """
    if not tag:
        return None
    match = XBRL_TAG_RE.match(tag.strip())
    if not match:
        return None
    ns, concept = match.group(1), match.group(2)
    return XBRLTag(namespace=ns, concept=concept, full_tag=tag.strip())


def is_valid_xbrl_tag(tag: str) -> bool:
    """Return True if the tag matches namespace:CamelCase format."""
    return parse_xbrl_tag(tag) is not None


def normalize_xbrl_amount(
    raw_value: str, scale: Optional[str] = None, decimals: Optional[str] = None
) -> Optional[float]:
    """
    Convert a raw XBRL numeric value to a float in millions USD.

    XBRL filings use two mechanisms to indicate scale:
      - 'scale' attribute: powers of 10 (e.g. scale="6" means millions, scale="9" means billions)
      - 'decimals' attribute: significant decimal places (negative = rounded, e.g. decimals="-6")

    Args:
        raw_value: The numeric string from the XBRL element (e.g., "15000000000")
        scale:     XBRL scale attribute value (e.g., "6", "9")
        decimals:  XBRL decimals attribute value (e.g., "-6", "2")

    Returns:
        Float in millions USD, or None if parsing fails.
    """
    try:
        value = float(raw_value.replace(",", "").strip())
    except (ValueError, AttributeError):
        return None

    # Apply scale (XBRL scale is a power of 10)
    if scale:
        try:
            scale_int = int(scale)
            # Convert to millions: divide by 10^6 then multiply by 10^scale
            # Raw unit values: scale=0, Thousands: scale=3, Millions: scale=6, Billions: scale=9
            value = value * (10**scale_int) / 1_000_000
            return value
        except (ValueError, OverflowError):
            pass

    # Heuristic: raw values > 10^8 are likely in actual dollars → convert to millions
    if abs(value) > 1e8:
        return value / 1_000_000
    # Values between 1000 and 10^8 could be thousands → convert to millions
    elif abs(value) > 1000:
        return value / 1_000  # assume thousands
    else:
        return value  # assume already in millions


# Attribute-order-independent patterns for individual XBRL attributes
_ATTR_NAME_RE = re.compile(
    r"name=[\"\']((?:[^\"\'\\\\]|\\\\.)+)[\"\'\"\'\\s]", re.IGNORECASE
)
_ATTR_CTX_RE = re.compile(
    r"contextRef=[\"\']((?:[^\"\'\\\\]|\\\\.)*)[\"\'\"\'\\s]", re.IGNORECASE
)
_ATTR_SCALE_RE = re.compile(
    r"scale=[\"\']((?:[^\"\'\\\\]|\\\\.)*)[\"\'\"\'\\s]", re.IGNORECASE
)
_ATTR_DECIMALS_RE = re.compile(
    r"decimals=[\"\']((?:[^\"\'\\\\]|\\\\.)*)[\"\'\"\'\\s]", re.IGNORECASE
)
_IXBRL_TAG_RE = re.compile(
    r"<ix:nonFraction\b([^>]*)>([\d,.\-]+)</ix:nonFraction>",
    re.IGNORECASE | re.DOTALL,
)


def extract_debt_xbrl_tags(html: str) -> list[dict]:
    """
    Extract all debt-related XBRL tags from raw HTML filing content.

    Uses attribute-order-independent parsing — EDGAR filings vary attribute
    order across companies and years. Extracts each attribute separately.

    Returns a list of dicts with keys: name, value_raw, value_millions,
    context_ref, scale, decimals.
    """
    results = []
    seen = set()

    for match in _IXBRL_TAG_RE.finditer(html):
        attrs = match.group(1)
        raw_value = match.group(2)

        # Extract each attribute independently (order-insensitive)
        name_m = _ATTR_NAME_RE.search(attrs)
        if not name_m:
            continue
        name = name_m.group(1)

        ctx_m = _ATTR_CTX_RE.search(attrs)
        context_ref = ctx_m.group(1) if ctx_m else ""

        scale_m = _ATTR_SCALE_RE.search(attrs)
        scale = scale_m.group(1) if scale_m else ""

        dec_m = _ATTR_DECIMALS_RE.search(attrs)
        decimals = dec_m.group(1) if dec_m else ""

        # Parse and filter to debt-related concepts only
        parsed = parse_xbrl_tag(name)
        if not parsed or not parsed.is_debt_related:
            continue

        # Deduplicate by name + context
        key = f"{name}|{context_ref}"
        if key in seen:
            continue
        seen.add(key)

        value_millions = normalize_xbrl_amount(raw_value, scale, decimals)

        results.append(
            {
                "name": name,
                "value_raw": raw_value,
                "value_millions": value_millions,
                "context_ref": context_ref,
                "scale": scale,
                "decimals": decimals,
            }
        )

    return results


def format_xbrl_snippet(tags: list[dict], max_tags: int = 100) -> str:
    """
    Format extracted XBRL tags as a compact text snippet for the Mapping Agent prompt.

    Format per line: {name} | {value_millions}M | ctx:{context_ref}
    """
    if not tags:
        return "(No inline XBRL debt tags found — rely on text extraction)"

    lines = []
    for tag in tags[:max_tags]:
        val_str = (
            f"{tag['value_millions']:.1f}M"
            if tag["value_millions"] is not None
            else tag["value_raw"]
        )
        lines.append(f"{tag['name']} | {val_str} | ctx:{tag['context_ref']}")

    return "\n".join(lines)
