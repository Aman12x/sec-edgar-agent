#!/usr/bin/env python3
"""
scripts/inspect_output.py — View and query pipeline output files.

Usage:
    python scripts/inspect_output.py AAPL             # show latest output
    python scripts/inspect_output.py AAPL --list      # list all outputs
    python scripts/inspect_output.py AAPL --debt      # debt instruments only
    python scripts/inspect_output.py AAPL --risks     # risk summary only
    python scripts/inspect_output.py --all            # summary of all tickers
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Inspect AFIP pipeline outputs")
    parser.add_argument("ticker", nargs="?", help="Ticker symbol")
    parser.add_argument("--list", action="store_true", help="List all output files for ticker")
    parser.add_argument("--debt", action="store_true", help="Show only debt instruments")
    parser.add_argument("--risks", action="store_true", help="Show only risk summary")
    parser.add_argument("--all", dest="all_tickers", action="store_true",
                        help="Summary of all processed tickers")
    args = parser.parse_args()

    from config.settings import config
    from src.utils.file_utils import list_output_files, get_latest_output

    if args.all_tickers:
        files = list_output_files(config.OUTPUT_DIR)
        if not files:
            print("No output files found.")
            return
        print(f"\nAll processed tickers ({len(files)} files):\n")
        print(f"{'TICKER':<8} {'PERIOD':<12} {'V':<3} {'CONF':<6} {'INSTRUMENTS'}")
        print("-" * 45)
        for f in files:
            data = json.loads(f.read_text())
            meta = data.get("_pipeline_metadata", {})
            verified = "✓" if meta.get("is_verified") else "✗"
            print(f"{data.get('ticker','?'):<8} "
                  f"{data.get('period_ending','?'):<12} "
                  f"{verified:<3} "
                  f"{data.get('confidence_score',0):.2f}  "
                  f"{len(data.get('debt_instruments',[]))}")
        return

    if not args.ticker:
        parser.print_help()
        return

    ticker = args.ticker.upper()

    if args.list:
        files = list_output_files(config.OUTPUT_DIR, ticker)
        if not files:
            print(f"No output files for {ticker}")
            return
        for f in files:
            print(f)
        return

    path = get_latest_output(config.OUTPUT_DIR, ticker)
    if not path:
        print(f"No output found for {ticker} in {config.OUTPUT_DIR}")
        return

    data = json.loads(path.read_text())

    if args.debt:
        instruments = data.get("debt_instruments", [])
        print(f"\n{ticker} — Debt Instruments ({data.get('period_ending')}):\n")
        if not instruments:
            print("  (none extracted)")
            return
        for inst in instruments:
            print(f"  {inst['name']}")
            print(f"    Amount:   ${inst['amount']:,.1f}M {inst.get('currency','USD')}")
            if inst.get("maturity_year"):
                print(f"    Maturity: {inst['maturity_year']}")
            if inst.get("xbrl_tag"):
                print(f"    XBRL:     {inst['xbrl_tag']}")
            print()
        return

    if args.risks:
        print(f"\n{ticker} — Risk Summary ({data.get('period_ending')}):\n")
        print(data.get("risks_summary", "No risk summary available."))
        return

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
