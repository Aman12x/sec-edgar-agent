"""
dashboard.py — AFIP Streamlit Dashboard

Run with: streamlit run dashboard.py

Provides:
  - Live pipeline execution (trigger a run for any ticker)
  - Browse and inspect all processed outputs
  - Debt instrument tables with maturity timeline
  - Risk factor viewer
  - Cross-ticker comparison
  - Run history and status
"""

import json
import os
import sys
import subprocess
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import streamlit as st

# Inject Streamlit Cloud secrets into os.environ so all os.getenv() calls work
try:
    for _k, _v in st.secrets.items():
        os.environ.setdefault(_k, str(_v))
except Exception:
    pass

# ── Page config (must be first Streamlit call) ──────────────────────────────
st.set_page_config(
    page_title="AFIP — Financial Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Add project root to path ─────────────────────────────────────────────────
ROOT = Path(__file__).parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Styling ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

  html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
  }

  /* Sidebar */
  [data-testid="stSidebar"] {
    background: #0f1117;
    border-right: 1px solid #1e2130;
  }
  [data-testid="stSidebar"] * {
    color: #c9d1d9 !important;
  }
  [data-testid="stSidebar"] .stSelectbox label,
  [data-testid="stSidebar"] .stTextInput label {
    color: #8b949e !important;
    font-size: 0.75rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  /* Main background */
  .main { background: #0d1117; }
  .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

  /* Metric cards */
  .metric-card {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 8px;
    padding: 1.25rem 1.5rem;
    margin-bottom: 0.5rem;
  }
  .metric-label {
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #8b949e;
    margin-bottom: 0.35rem;
  }
  .metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem;
    font-weight: 500;
    color: #e6edf3;
    line-height: 1;
  }
  .metric-sub {
    font-size: 0.75rem;
    color: #8b949e;
    margin-top: 0.3rem;
  }

  /* Status badges */
  .badge-verified {
    display: inline-block;
    background: #1a3a2a;
    color: #3fb950;
    border: 1px solid #2ea043;
    border-radius: 4px;
    padding: 2px 10px;
    font-size: 0.72rem;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }
  .badge-unverified {
    display: inline-block;
    background: #2d1f00;
    color: #d29922;
    border: 1px solid #9e6a03;
    border-radius: 4px;
    padding: 2px 10px;
    font-size: 0.72rem;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }
  .badge-error {
    display: inline-block;
    background: #3a1a1a;
    color: #f85149;
    border: 1px solid #da3633;
    border-radius: 4px;
    padding: 2px 10px;
    font-size: 0.72rem;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }

  /* Section headers */
  .section-header {
    font-size: 0.7rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #8b949e;
    border-bottom: 1px solid #21262d;
    padding-bottom: 0.5rem;
    margin-bottom: 1rem;
    margin-top: 1.5rem;
  }

  /* Ticker display */
  .ticker-hero {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2.8rem;
    font-weight: 500;
    color: #e6edf3;
    letter-spacing: -0.02em;
    line-height: 1;
  }
  .period-sub {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.85rem;
    color: #8b949e;
    margin-top: 0.3rem;
  }

  /* Confidence bar */
  .conf-bar-bg {
    background: #21262d;
    border-radius: 3px;
    height: 6px;
    margin-top: 0.4rem;
    overflow: hidden;
  }
  .conf-bar-fill-high {
    background: #3fb950;
    height: 6px;
    border-radius: 3px;
    transition: width 0.6s ease;
  }
  .conf-bar-fill-mid {
    background: #d29922;
    height: 6px;
    border-radius: 3px;
  }
  .conf-bar-fill-low {
    background: #f85149;
    height: 6px;
    border-radius: 3px;
  }

  /* Debt instrument rows */
  .debt-row {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 6px;
    padding: 0.9rem 1.2rem;
    margin-bottom: 0.5rem;
    display: flex;
    align-items: center;
    gap: 1rem;
  }
  .debt-name {
    font-size: 0.9rem;
    color: #e6edf3;
    font-weight: 500;
    flex: 1;
  }
  .debt-amount {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.9rem;
    color: #79c0ff;
    font-weight: 500;
    min-width: 100px;
    text-align: right;
  }
  .debt-maturity {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    color: #8b949e;
    min-width: 60px;
    text-align: right;
  }
  .debt-tag {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    color: #8b949e;
    background: #0d1117;
    border: 1px solid #21262d;
    border-radius: 3px;
    padding: 2px 6px;
  }

  /* Risk summary */
  .risk-item {
    border-left: 3px solid #21262d;
    padding: 0.5rem 0.8rem;
    margin-bottom: 0.5rem;
    color: #c9d1d9;
    font-size: 0.875rem;
    line-height: 1.5;
  }

  /* Run trigger */
  .run-box {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 8px;
    padding: 1.5rem;
  }

  /* Streamlit overrides */
  div[data-testid="stButton"] button {
    background: #238636;
    color: white;
    border: 1px solid #2ea043;
    border-radius: 6px;
    font-family: 'IBM Plex Sans', sans-serif;
    font-weight: 500;
    padding: 0.4rem 1.2rem;
    transition: all 0.15s ease;
  }
  div[data-testid="stButton"] button:hover {
    background: #2ea043;
    border-color: #3fb950;
  }
  div[data-testid="stButton"] button[kind="secondary"] {
    background: transparent;
    border-color: #30363d;
    color: #c9d1d9;
  }
  .stTextInput input, .stSelectbox select {
    background: #0d1117 !important;
    border: 1px solid #30363d !important;
    color: #e6edf3 !important;
    font-family: 'IBM Plex Mono', monospace !important;
  }
  .stDataFrame { border-radius: 8px; overflow: hidden; }
  h1, h2, h3 { color: #e6edf3 !important; }
  p, li { color: #c9d1d9; }
  .stAlert { border-radius: 6px; }
  .stSpinner > div { border-top-color: #3fb950 !important; }
  [data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace !important;
    color: #e6edf3 !important;
  }
</style>
""", unsafe_allow_html=True)


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_output_dir() -> Path:
    try:
        from config.settings import config
        return Path(config.OUTPUT_DIR)
    except Exception:
        return Path("./data/output")


def load_all_profiles() -> list[dict]:
    """Load all FinancialProfile JSON files from the output directory."""
    output_dir = get_output_dir()
    if not output_dir.exists():
        return []
    profiles = []
    for f in sorted(output_dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            data = json.loads(f.read_text())
            data["_filename"] = f.name
            data["_filepath"] = str(f)
            profiles.append(data)
        except Exception:
            continue
    return profiles


def load_profile(filepath: str) -> Optional[dict]:
    try:
        return json.loads(Path(filepath).read_text())
    except Exception:
        return None


def confidence_bar_html(score: float) -> str:
    pct = int(score * 100)
    cls = "conf-bar-fill-high" if score >= 0.9 else ("conf-bar-fill-mid" if score >= 0.7 else "conf-bar-fill-low")
    return (
        f'<div style="display:flex; align-items:center; gap:0.6rem;">'
        f'<div class="conf-bar-bg" style="flex:1;">'
        f'<div class="{cls}" style="width:{pct}%"></div>'
        f'</div>'
        f'<span style="font-family:\'IBM Plex Mono\',monospace; font-size:0.8rem; color:#8b949e; min-width:36px;">{pct}%</span>'
        f'</div>'
    )


def status_badge(is_verified: bool) -> str:
    if is_verified:
        return '<span class="badge-verified">✓ Verified</span>'
    return '<span class="badge-unverified">~ Unverified</span>'


def maturity_color(year: Optional[int]) -> str:
    if not year:
        return "#8b949e"
    current_year = date.today().year
    years_out = year - current_year
    if years_out <= 2:
        return "#f85149"   # red — near-term pressure
    elif years_out <= 5:
        return "#d29922"   # yellow — medium term
    return "#3fb950"       # green — long dated


def format_amount(amount: float) -> str:
    if amount >= 1000:
        return f"${amount/1000:,.2f}B"
    return f"${amount:,.0f}M"


def run_pipeline_subprocess(ticker: str) -> tuple[bool, str]:
    """Run pipeline in subprocess so Streamlit doesn't block."""
    script = ROOT / "scripts" / "run_single.py"
    try:
        result = subprocess.run(
            [sys.executable, str(script), ticker.upper()],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            cwd=str(ROOT),
        )
        output = result.stdout + result.stderr
        success = result.returncode == 0 and "[OK]" in result.stdout
        return success, output
    except subprocess.TimeoutExpired:
        return False, "Pipeline timed out after 5 minutes."
    except Exception as e:
        return False, str(e)


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style="padding: 0.5rem 0 1.5rem;">
      <div style="font-family:'IBM Plex Mono',monospace; font-size:1.1rem; color:#e6edf3; font-weight:500;">AFIP</div>
      <div style="font-size:0.7rem; color:#8b949e; letter-spacing:0.08em; text-transform:uppercase; margin-top:2px;">Financial Intelligence Pipeline</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["Overview", "Run Pipeline", "Inspect Filing", "Compare Tickers"],
        label_visibility="collapsed",
    )

    st.markdown("---")

    # Quick stats
    profiles = load_all_profiles()
    verified = sum(1 for p in profiles if p.get("_pipeline_metadata", {}).get("is_verified"))
    st.markdown(f"""
    <div style="font-size:0.7rem; color:#8b949e; letter-spacing:0.08em; text-transform:uppercase; margin-bottom:0.75rem;">Database</div>
    <div style="display:flex; gap:1.5rem; margin-bottom:1rem;">
      <div>
        <div style="font-family:'IBM Plex Mono',monospace; font-size:1.4rem; color:#e6edf3;">{len(profiles)}</div>
        <div style="font-size:0.7rem; color:#8b949e;">Total filings</div>
      </div>
      <div>
        <div style="font-family:'IBM Plex Mono',monospace; font-size:1.4rem; color:#3fb950;">{verified}</div>
        <div style="font-size:0.7rem; color:#8b949e;">Verified</div>
      </div>
      <div>
        <div style="font-family:'IBM Plex Mono',monospace; font-size:1.4rem; color:#d29922;">{len(profiles)-verified}</div>
        <div style="font-size:0.7rem; color:#8b949e;">Unverified</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    if profiles:
        latest = profiles[0]
        meta = latest.get("_pipeline_metadata", {})
        ts = meta.get("ingested_at", "")
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                ts_display = dt.strftime("%b %d, %H:%M UTC")
            except Exception:
                ts_display = ts[:16]
        else:
            ts_display = "—"
        st.markdown(f"""
        <div style="font-size:0.7rem; color:#8b949e; margin-top:0.5rem;">
          Last run: <span style="font-family:'IBM Plex Mono',monospace; color:#c9d1d9;">{ts_display}</span>
        </div>
        """, unsafe_allow_html=True)


# ── Page: Overview ───────────────────────────────────────────────────────────

if page == "Overview":
    st.markdown('<div class="ticker-hero">Overview</div>', unsafe_allow_html=True)
    st.markdown('<div class="period-sub">All processed filings</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    profiles = load_all_profiles()

    if not profiles:
        st.info("No filings processed yet. Go to **Run Pipeline** to extract your first filing.")
    else:
        # Summary metrics row
        total_debt = sum(
            sum(inst.get("amount", 0) for inst in p.get("debt_instruments", []))
            for p in profiles
        )
        avg_confidence = sum(p.get("confidence_score", 0) for p in profiles) / len(profiles)
        total_instruments = sum(len(p.get("debt_instruments", [])) for p in profiles)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Filings Processed</div>
              <div class="metric-value">{len(profiles)}</div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Total Debt Tracked</div>
              <div class="metric-value">{format_amount(total_debt)}</div>
              <div class="metric-sub">across all companies</div>
            </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Debt Instruments</div>
              <div class="metric-value">{total_instruments}</div>
            </div>""", unsafe_allow_html=True)
        with c4:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Avg Confidence</div>
              <div class="metric-value">{avg_confidence:.0%}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown('<div class="section-header">All Filings</div>', unsafe_allow_html=True)

        for profile in profiles:
            meta = profile.get("_pipeline_metadata", {})
            is_verified = meta.get("is_verified", False)
            instruments = profile.get("debt_instruments", [])
            total = sum(i.get("amount", 0) for i in instruments)
            conf = profile.get("confidence_score", 0)

            with st.expander(
                f"{'✓' if is_verified else '~'}  {profile.get('ticker','?')}  ·  "
                f"{profile.get('period_ending','?')}  ·  "
                f"{format_amount(total)} debt  ·  "
                f"{len(instruments)} instruments",
                expanded=False
            ):
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.markdown(f"""
                    <div style="margin-bottom:0.5rem;">{status_badge(is_verified)}</div>
                    <div style="font-size:0.8rem; color:#8b949e; margin-bottom:0.3rem;">Confidence</div>
                    {confidence_bar_html(conf)}
                    """, unsafe_allow_html=True)
                with col2:
                    retries = meta.get("retry_count", 0)
                    model = meta.get("llm_model", "—")
                    st.markdown(f"""
                    <div style="font-size:0.75rem; color:#8b949e; line-height:2;">
                      Filing type: <span style="color:#c9d1d9">{profile.get('filing_type','10-K')}</span><br>
                      Retries used: <span style="color:#c9d1d9">{retries}</span><br>
                      Model: <span style="font-family:'IBM Plex Mono',monospace; color:#c9d1d9; font-size:0.7rem;">{model}</span>
                    </div>
                    """, unsafe_allow_html=True)

                if instruments:
                    st.markdown("**Debt instruments:**")
                    for inst in instruments:
                        yr = inst.get("maturity_year")
                        yr_color = maturity_color(yr)
                        st.markdown(f"""
                        <div class="debt-row">
                          <span class="debt-name">{inst.get('name','—')}</span>
                          <span class="debt-amount">{format_amount(inst.get('amount',0))}</span>
                          <span class="debt-maturity" style="color:{yr_color};">{yr or '—'}</span>
                          {f'<span class="debt-tag">{inst.get("xbrl_tag","")}</span>' if inst.get("xbrl_tag") else ''}
                        </div>
                        """, unsafe_allow_html=True)


# ── Page: Run Pipeline ───────────────────────────────────────────────────────

elif page == "Run Pipeline":
    st.markdown('<div class="ticker-hero">Run Pipeline</div>', unsafe_allow_html=True)
    st.markdown('<div class="period-sub">Trigger a new extraction from SEC EDGAR</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # Check config
    groq_key = os.getenv("GROQ_API_KEY", "")
    sec_agent = os.getenv("SEC_USER_AGENT", "")
    config_ok = bool(groq_key) and "example.com" not in sec_agent and bool(sec_agent)

    if not config_ok:
        st.warning("""
        **Configuration required before running.**

        Set these environment variables before launching the dashboard:
        ```
        GROQ_API_KEY=gsk_your_key_here
        SEC_USER_AGENT=Your Name your@email.com
        ```
        Then restart with: `GROQ_API_KEY=... SEC_USER_AGENT=... streamlit run dashboard.py`
        """)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown('<div class="run-box">', unsafe_allow_html=True)

        ticker_input = st.text_input(
            "Ticker Symbol",
            placeholder="e.g. AAPL, MSFT, JPM",
            max_chars=10,
        ).strip().upper()

        filing_type = st.selectbox("Filing Type", ["10-K", "10-Q"], index=0)

        st.markdown("""
        <div style="font-size:0.75rem; color:#8b949e; margin: 0.75rem 0; line-height:1.6;">
          The pipeline will:<br>
          1. Download the most recent filing from SEC EDGAR<br>
          2. Parse with Docling (HTML → Markdown)<br>
          3. Extract debt instruments via Llama 3.2<br>
          4. Verify with the judge (up to 3 retries)<br>
          5. Save verified JSON to <code style="color:#79c0ff;">data/output/</code>
        </div>
        """, unsafe_allow_html=True)

        run_btn = st.button(
            "▶  Run Pipeline",
            disabled=not (ticker_input and config_ok),
            use_container_width=True,
        )
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div style="background:#161b22; border:1px solid #21262d; border-radius:8px; padding:1.2rem;">
          <div style="font-size:0.7rem; color:#8b949e; letter-spacing:0.1em; text-transform:uppercase; margin-bottom:0.75rem;">Typical run time</div>
          <div style="font-size:0.85rem; color:#c9d1d9; line-height:2;">
            Download: <span style="font-family:'IBM Plex Mono',monospace; color:#79c0ff;">~30s</span><br>
            Docling parse: <span style="font-family:'IBM Plex Mono',monospace; color:#79c0ff;">30–90s</span><br>
            LLM extraction: <span style="font-family:'IBM Plex Mono',monospace; color:#79c0ff;">~15s</span><br>
            Verification: <span style="font-family:'IBM Plex Mono',monospace; color:#79c0ff;">&lt;1s</span><br>
            <span style="color:#8b949e;">Total: ~2–3 min</span>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Common tickers quick-launch
        st.markdown('<div style="font-size:0.7rem; color:#8b949e; letter-spacing:0.1em; text-transform:uppercase; margin-bottom:0.5rem;">Quick launch</div>', unsafe_allow_html=True)
        quick_tickers = ["AAPL", "MSFT", "GOOGL", "JPM", "NVDA"]
        for qt in quick_tickers:
            if st.button(qt, key=f"quick_{qt}", use_container_width=True):
                ticker_input = qt
                st.rerun()

    if run_btn and ticker_input:
        st.markdown("---")
        with st.spinner(f"Running pipeline for **{ticker_input}**… this takes 2–3 minutes"):
            log_placeholder = st.empty()
            log_placeholder.markdown(f"""
            <div style="background:#0d1117; border:1px solid #21262d; border-radius:6px;
                        padding:1rem; font-family:'IBM Plex Mono',monospace; font-size:0.75rem;
                        color:#8b949e; line-height:1.8;">
              [ingest]  Downloading {ticker_input} 10-K from SEC EDGAR...<br>
              [parse]   Converting HTML → Markdown via Docling...<br>
              [agent]   Extracting debt instruments with Llama 3.2...<br>
              [judge]   Verifying extraction quality...<br>
            </div>
            """, unsafe_allow_html=True)

            success, output = run_pipeline_subprocess(ticker_input)

        if success:
            st.success(f"✓ Pipeline completed successfully for **{ticker_input}**")
            # Show the output log
            with st.expander("Run log", expanded=False):
                st.code(output, language="text")
            # Reload and show the result
            st.markdown("**Switching to Inspect Filing to show results...**")
            time.sleep(1)
            st.session_state["inspect_ticker"] = ticker_input
            st.rerun()
        else:
            st.error(f"Pipeline failed for **{ticker_input}**")
            with st.expander("Error log", expanded=True):
                st.code(output, language="text")
            st.markdown("""
            **Common causes:**
            - `GROQ_API_KEY` not set or invalid
            - `SEC_USER_AGENT` not set correctly (`"Firstname Lastname email@domain.com"`)
            - No internet connection
            - SEC EDGAR temporarily unavailable
            """)


# ── Page: Inspect Filing ─────────────────────────────────────────────────────

elif page == "Inspect Filing":
    profiles = load_all_profiles()

    if not profiles:
        st.info("No filings processed yet. Go to **Run Pipeline** to extract your first filing.")
    else:
        # Group by ticker, pick latest per ticker
        by_ticker: dict[str, dict] = {}
        for p in profiles:
            t = p.get("ticker", "?")
            if t not in by_ticker:
                by_ticker[t] = p

        # Pre-select from session state (set after a run)
        default_ticker = st.session_state.get("inspect_ticker", list(by_ticker.keys())[0])
        if default_ticker not in by_ticker:
            default_ticker = list(by_ticker.keys())[0]

        selected_ticker = st.selectbox(
            "Select ticker",
            options=list(by_ticker.keys()),
            index=list(by_ticker.keys()).index(default_ticker),
        )

        if selected_ticker in st.session_state.get("inspect_ticker", ""):
            st.session_state.pop("inspect_ticker", None)

        profile = by_ticker[selected_ticker]
        meta = profile.get("_pipeline_metadata", {})
        is_verified = meta.get("is_verified", False)
        instruments = profile.get("debt_instruments", [])
        conf = profile.get("confidence_score", 0)
        total_debt = sum(i.get("amount", 0) for i in instruments)

        # Header
        st.markdown(f"""
        <div style="display:flex; align-items:flex-end; gap:1rem; margin-bottom:1rem;">
          <div class="ticker-hero">{selected_ticker}</div>
          <div>
            {status_badge(is_verified)}
            <div class="period-sub">{profile.get('filing_type','10-K')} · Period ending {profile.get('period_ending','—')}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Metrics
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Total Debt</div>
              <div class="metric-value">{format_amount(total_debt)}</div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Instruments</div>
              <div class="metric-value">{len(instruments)}</div>
            </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Confidence</div>
              <div class="metric-value">{conf:.0%}</div>
              {confidence_bar_html(conf)}
            </div>""", unsafe_allow_html=True)
        with c4:
            near_term = [i for i in instruments
                         if i.get("maturity_year") and i["maturity_year"] <= date.today().year + 3]
            near_debt = sum(i.get("amount", 0) for i in near_term)
            st.markdown(f"""<div class="metric-card">
              <div class="metric-label">Matures ≤ 3yr</div>
              <div class="metric-value" style="color:{'#f85149' if near_debt > 0 else '#e6edf3'};">{format_amount(near_debt) if near_debt else "None"}</div>
              <div class="metric-sub">{len(near_term)} instrument(s)</div>
            </div>""", unsafe_allow_html=True)

        # Tabs
        tab1, tab2, tab3 = st.tabs(["Debt Instruments", "Risk Factors", "Raw JSON"])

        with tab1:
            st.markdown('<div class="section-header">Debt Schedule</div>', unsafe_allow_html=True)

            if not instruments:
                st.info("No debt instruments extracted for this filing.")
            else:
                # Sort by maturity year
                sorted_instruments = sorted(
                    instruments,
                    key=lambda i: (i.get("maturity_year") or 9999)
                )

                for inst in sorted_instruments:
                    yr = inst.get("maturity_year")
                    yr_color = maturity_color(yr)
                    years_out = (yr - date.today().year) if yr else None
                    urgency = ""
                    if years_out is not None:
                        if years_out <= 0:
                            urgency = " · <span style='color:#f85149;'>MATURED</span>"
                        elif years_out <= 2:
                            urgency = f" · <span style='color:#f85149;'>due in {years_out}yr</span>"
                        elif years_out <= 5:
                            urgency = f" · <span style='color:#d29922;'>due in {years_out}yr</span>"

                    st.markdown(f"""
                    <div class="debt-row">
                      <span class="debt-name">{inst.get('name','—')}</span>
                      <span class="debt-amount">{format_amount(inst.get('amount', 0))}</span>
                      <span class="debt-maturity" style="color:{yr_color};">{yr or '—'}{urgency}</span>
                      {f'<span class="debt-tag">{inst.get("xbrl_tag","")}</span>' if inst.get("xbrl_tag") else ''}
                    </div>
                    """, unsafe_allow_html=True)

                # Maturity bar chart
                st.markdown('<div class="section-header">Maturity Timeline</div>', unsafe_allow_html=True)
                import pandas as pd
                chart_data = [
                    {"Year": str(i.get("maturity_year", "Unknown")), "Amount ($M)": i.get("amount", 0)}
                    for i in sorted_instruments if i.get("maturity_year")
                ]
                if chart_data:
                    df = pd.DataFrame(chart_data).groupby("Year", as_index=False).sum()
                    st.bar_chart(df.set_index("Year"), color="#79c0ff")

        with tab2:
            st.markdown('<div class="section-header">Risk Factors (Item 1A)</div>', unsafe_allow_html=True)
            risks_raw = profile.get("risks_summary", "")
            if not risks_raw:
                st.info("No risk summary extracted.")
            else:
                lines = [l.strip() for l in risks_raw.split("\n") if l.strip()]
                for line in lines:
                    text = line.lstrip("- ").strip()
                    if text:
                        st.markdown(f'<div class="risk-item">{text}</div>', unsafe_allow_html=True)

            if profile.get("extraction_notes"):
                st.markdown('<div class="section-header">Extraction Notes</div>', unsafe_allow_html=True)
                st.warning(profile["extraction_notes"])

        with tab3:
            st.markdown('<div class="section-header">Full JSON Output</div>', unsafe_allow_html=True)
            clean = {k: v for k, v in profile.items() if not k.startswith("_filename")}
            st.json(clean)
            st.download_button(
                "Download JSON",
                data=json.dumps(clean, indent=2),
                file_name=f"{selected_ticker}_{profile.get('period_ending','')}.json",
                mime="application/json",
            )


# ── Page: Compare Tickers ────────────────────────────────────────────────────

elif page == "Compare Tickers":
    st.markdown('<div class="ticker-hero">Compare</div>', unsafe_allow_html=True)
    st.markdown('<div class="period-sub">Side-by-side debt structure comparison</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    profiles = load_all_profiles()

    if len(profiles) < 2:
        st.info("Process at least 2 companies to enable comparison. Go to **Run Pipeline**.")
    else:
        by_ticker = {}
        for p in profiles:
            t = p.get("ticker", "?")
            if t not in by_ticker:
                by_ticker[t] = p

        tickers = list(by_ticker.keys())

        col1, col2 = st.columns(2)
        with col1:
            t1 = st.selectbox("Company A", tickers, index=0, key="cmp_t1")
        with col2:
            default_t2 = tickers[1] if len(tickers) > 1 else tickers[0]
            t2 = st.selectbox("Company B", tickers, index=tickers.index(default_t2), key="cmp_t2")

        p1, p2 = by_ticker[t1], by_ticker[t2]
        inst1 = p1.get("debt_instruments", [])
        inst2 = p2.get("debt_instruments", [])
        total1 = sum(i.get("amount", 0) for i in inst1)
        total2 = sum(i.get("amount", 0) for i in inst2)

        st.markdown("---")

        # Side-by-side summary
        c1, c2 = st.columns(2)

        def render_company_summary(profile, instruments, total, col):
            with col:
                meta = profile.get("_pipeline_metadata", {})
                is_verified = meta.get("is_verified", False)
                conf = profile.get("confidence_score", 0)
                st.markdown(f"""
                <div style="margin-bottom:1rem;">
                  <div style="font-family:'IBM Plex Mono',monospace; font-size:1.8rem; color:#e6edf3; font-weight:500;">{profile.get('ticker','')}</div>
                  <div style="font-size:0.8rem; color:#8b949e; margin-top:2px;">{profile.get('period_ending','')}</div>
                  <div style="margin-top:0.5rem;">{status_badge(is_verified)}</div>
                </div>
                <div class="metric-card" style="margin-bottom:0.5rem;">
                  <div class="metric-label">Total Debt</div>
                  <div class="metric-value">{format_amount(total)}</div>
                </div>
                <div class="metric-card" style="margin-bottom:0.5rem;">
                  <div class="metric-label">Instruments</div>
                  <div class="metric-value">{len(instruments)}</div>
                </div>
                <div class="metric-card">
                  <div class="metric-label">Confidence</div>
                  <div class="metric-value">{conf:.0%}</div>
                  {confidence_bar_html(conf)}
                </div>
                """, unsafe_allow_html=True)

                if instruments:
                    st.markdown('<div class="section-header" style="margin-top:1rem;">Debt Schedule</div>', unsafe_allow_html=True)
                    for inst in sorted(instruments, key=lambda i: i.get("maturity_year") or 9999):
                        yr = inst.get("maturity_year")
                        st.markdown(f"""
                        <div class="debt-row">
                          <span class="debt-name" style="font-size:0.8rem;">{inst.get('name','—')}</span>
                          <span class="debt-amount" style="font-size:0.8rem;">{format_amount(inst.get('amount',0))}</span>
                          <span class="debt-maturity" style="color:{maturity_color(yr)};">{yr or '—'}</span>
                        </div>
                        """, unsafe_allow_html=True)

        render_company_summary(p1, inst1, total1, c1)
        render_company_summary(p2, inst2, total2, c2)

        # Combined maturity chart
        st.markdown('<div class="section-header">Combined Maturity Timeline</div>', unsafe_allow_html=True)

        import pandas as pd
        rows = []
        for inst in inst1:
            if inst.get("maturity_year"):
                rows.append({"Year": str(inst["maturity_year"]), t1: inst.get("amount", 0), t2: 0})
        for inst in inst2:
            if inst.get("maturity_year"):
                rows.append({"Year": str(inst["maturity_year"]), t1: 0, t2: inst.get("amount", 0)})

        if rows:
            df = pd.DataFrame(rows).groupby("Year", as_index=False).sum()
            st.bar_chart(df.set_index("Year")[[t1, t2]])
        else:
            st.info("No maturity data available for timeline chart.")
