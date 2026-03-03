#!/usr/bin/env python3
"""
scripts/batch_run.py — Run the pipeline for multiple tickers in sequence.

Usage:
    python scripts/batch_run.py AAPL MSFT GOOGL AMZN
    python scripts/batch_run.py --from-file tickers.txt
    python scripts/batch_run.py --watchlist
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "JPM", "BAC"]


def main():
    parser = argparse.ArgumentParser(description="Batch-run AFIP pipeline for multiple tickers")
    parser.add_argument("tickers", nargs="*", help="Ticker symbols to process")
    parser.add_argument("--from-file", help="Read tickers from a text file (one per line)")
    parser.add_argument("--watchlist", action="store_true", help="Use default ticker watchlist")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Seconds between tickers (default: 2.0)")
    parser.add_argument("--output-summary", help="Save JSON summary to this file")
    args = parser.parse_args()

    if args.from_file:
        tickers = [t.strip().upper() for t in Path(args.from_file).read_text().splitlines()
                   if t.strip() and not t.startswith("#")]
    elif args.watchlist:
        tickers = DEFAULT_TICKERS
    elif args.tickers:
        tickers = [t.upper() for t in args.tickers]
    else:
        parser.print_help()
        sys.exit(1)

    from src.graph import run_pipeline
    from src.utils.logger import get_logger

    logger = get_logger("batch_run")
    logger.info(f"Batch run: {len(tickers)} tickers")

    results = []
    for i, ticker in enumerate(tickers, 1):
        print(f"\n[{i}/{len(tickers)}] Processing {ticker}...")
        try:
            result = run_pipeline(ticker=ticker)
            profile = result.extracted_profile
            summary = {
                "ticker": ticker,
                "status": "verified" if result.is_verified else "unverified",
                "output_path": result.output_path,
                "confidence": result.confidence_score or 0,
                "retries": result.retry_count or 0,
                "instruments": len(profile.debt_instruments) if profile else 0,
            }
            status_icon = "✓" if summary["status"] == "verified" else "~"
            print(f"  {status_icon} {summary['status']} | "
                  f"confidence: {summary['confidence']:.2f} | "
                  f"instruments: {summary['instruments']}")
        except Exception as e:
            summary = {"ticker": ticker, "status": "error", "error": str(e)}
            print(f"  ✗ Error: {e}")

        results.append(summary)

        if i < len(tickers):
            time.sleep(args.delay)

    # Summary
    verified = sum(1 for r in results if r["status"] == "verified")
    unverified = sum(1 for r in results if r["status"] == "unverified")
    errors = sum(1 for r in results if r["status"] == "error")
    print(f"\n{'='*50}")
    print(f"Batch complete: {verified} verified, {unverified} unverified, {errors} errors")

    if args.output_summary:
        Path(args.output_summary).write_text(json.dumps(results, indent=2))
        print(f"Summary saved: {args.output_summary}")


if __name__ == "__main__":
    main()
