"""
prefect_flow.py — Prefect 3.0 orchestration for scheduled pipeline execution.

Schedule: cron "0 17 * * 1-5" = Mon-Fri at 17:00 ET (post-NYSE close).
Tickers processed concurrently up to PREFECT_MAX_CONCURRENT_TICKERS.

Usage:
    python prefect_flow.py               # local run with default tickers
    python prefect_flow.py AAPL MSFT     # local run with specific tickers
    python prefect_flow.py deploy        # register deployment with Prefect Cloud
"""

from __future__ import annotations

import sys
from datetime import timedelta
from typing import Optional

from prefect import flow, task
from prefect.tasks import task_input_hash

from config.settings import config
from src.graph import run_pipeline
from src.utils.logger import get_logger

logger = get_logger("prefect_flow")

DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "JPM", "BAC", "WMT", "UNH",
]


@task(
    name="process-ticker",
    retries=3,
    retry_delay_seconds=[30, 90, 270],   # 30s, 1.5m, 4.5m exponential backoff
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24), # skip re-processing same ticker same day
    log_prints=True,
)
def process_ticker_task(ticker: str, filing_type: str = "10-K") -> dict:
    """
    Prefect task wrapping the full LangGraph pipeline for one ticker.

    Prefect retries handle inter-run failures (network outages, API downtime).
    LangGraph retries inside run_pipeline() handle intra-run LLM quality issues.
    The 24-hour cache prevents duplicate processing within a single day's run.
    """
    logger.info(f"Task starting: {ticker} {filing_type}")
    try:
        result = run_pipeline(ticker=ticker, filing_type=filing_type)
        profile = result.extracted_profile
        summary = {
            "ticker": ticker,
            "status": "verified" if result.is_verified else "unverified",
            "output_path": result.output_path,
            "debt_count": len(profile.debt_instruments) if profile else 0,
            "confidence": result.confidence_score or 0.0,
            "retries_used": result.retry_count or 0,
            "period_ending": profile.period_ending if profile else None,
        }
        logger.info(
            f"Task complete: {ticker} → {summary['status']} "
            f"(conf: {summary['confidence']:.2f}, instruments: {summary['debt_count']})"
        )
        return summary

    except Exception as e:
        logger.error(f"Task failed: {ticker} — {e}")
        return {"ticker": ticker, "status": "error", "error": str(e)}


@flow(
    name="AFIP-Financial-Intelligence-Pipeline",
    description=(
        "Autonomous Financial Intelligence Pipeline — fetches SEC 10-K filings, "
        "extracts debt instruments and risk factors via Llama 3.2, and stores "
        "verified FinancialProfile JSON. Runs Mon-Fri post-market close."
    ),
    log_prints=True,
)
def afip_flow(
    tickers: Optional[list[str]] = None,
    filing_type: str = "10-K",
) -> list[dict]:
    """
    Main Prefect flow: process all tickers, report results.

    Args:
        tickers:      Ticker list. Defaults to DEFAULT_TICKERS.
        filing_type:  SEC form type. Default "10-K".

    Returns:
        List of result dicts, one per ticker, with status/confidence/path.
    """
    target = tickers or DEFAULT_TICKERS
    logger.info(f"AFIP flow starting: {len(target)} tickers — {', '.join(target)}")

    # Submit all tasks — Prefect ConcurrentTaskRunner handles parallelism
    futures = [
        process_ticker_task.submit(ticker=t, filing_type=filing_type)
        for t in target
    ]

    results = [f.result(raise_on_failure=False) for f in futures]

    # Summary
    verified = sum(1 for r in results if r.get("status") == "verified")
    unverified = sum(1 for r in results if r.get("status") == "unverified")
    errors = sum(1 for r in results if r.get("status") == "error")

    logger.info(
        f"Flow complete — verified: {verified}, unverified: {unverified}, errors: {errors}"
    )
    return results


def deploy():
    """
    Register the flow as a Prefect Cloud deployment with cron schedule.
    Run once: `python prefect_flow.py deploy`
    """
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule

    deployment = Deployment.build_from_flow(
        flow=afip_flow,
        name="daily-post-market",
        schedule=CronSchedule(
            cron=config.PREFECT_SCHEDULE,    # "0 17 * * 1-5"
            timezone=config.PREFECT_TIMEZONE, # "America/New_York"
        ),
        parameters={"tickers": DEFAULT_TICKERS, "filing_type": "10-K"},
        tags=["finance", "sec", "production"],
        description="Daily post-market 10-K extraction. Runs Mon-Fri at 17:00 ET.",
    )
    deployment.apply()
    logger.info(
        f"Deployment registered: '{deployment.name}' "
        f"— schedule: {config.PREFECT_SCHEDULE} {config.PREFECT_TIMEZONE}"
    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        deploy()
    elif len(sys.argv) > 1:
        # Local run with tickers from command line
        tickers = [t.upper() for t in sys.argv[1:]]
        afip_flow(tickers=tickers)
    else:
        # Local run with default watchlist
        afip_flow()
