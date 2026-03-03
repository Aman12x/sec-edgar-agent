#!/usr/bin/env python3
"""
scripts/run_single.py — Run the pipeline for a single ticker from the command line.

Usage:
    python scripts/run_single.py AAPL
    python scripts/run_single.py MSFT --filing-type 10-K
    python scripts/run_single.py GOOGL --dry-run
"""

import argparse
import os
import sys
from pathlib import Path

# Ensure project root is on PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Run AFIP pipeline for a single ticker")
    parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL)")
    parser.add_argument("--filing-type", default="10-K", help="SEC filing type (default: 10-K)")
    parser.add_argument("--dry-run", action="store_true", help="Skip SEC download, use cached filing")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.dry_run:
        os.environ["DRY_RUN"] = "true"

    from src.graph import run_pipeline
    from src.utils.logger import get_logger

    logger = get_logger("run_single")
    logger.info(f"Starting pipeline: {args.ticker} ({args.filing_type})")

    try:
        result = run_pipeline(ticker=args.ticker, filing_type=args.filing_type)

        if result.output_path:
            profile = result.extracted_profile
            instrument_count = len(profile.debt_instruments) if profile else 0
            print(f"\n[OK] Output: {result.output_path}")
            print(f"     Instruments: {instrument_count}")
            print(f"     Confidence:  {result.confidence_score or 0:.2f}")
            print(f"     Verified:    {result.is_verified or False}")
            print(f"     Retries:     {result.retry_count or 0}")
        else:
            print(f"\n[FAIL] Pipeline completed but no output was saved for {args.ticker}")
            sys.exit(1)

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
