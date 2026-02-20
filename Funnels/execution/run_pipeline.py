#!/usr/bin/env python3
from __future__ import annotations
"""
Run Pipeline — Full Scrape → Filter → Export
==============================================
Orchestrates the complete funnel pipeline.

Usage:
    python execution/run_pipeline.py                  # Full pipeline
    python execution/run_pipeline.py --skip-scrape    # Filter + export only
    python execution/run_pipeline.py --csv            # Export to CSV instead of Sheets
    python execution/run_pipeline.py --csv            # Export to CSV instead of Sheets
    python execution/run_pipeline.py --safe           # Safe mode (no login)
    python execution/run_pipeline.py --region santiago # Single region
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXECUTION_DIR = PROJECT_ROOT / "execution"


def run_step(name: str, cmd: list[str]) -> bool:
    """Run a pipeline step and return success status."""
    print(f"\n{'='*60}")
    print(f"  STEP: {name}")
    print(f"  CMD:  {' '.join(cmd)}")
    print(f"{'='*60}\n")

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))

    if result.returncode != 0:
        print(f"\n  ✗ STEP FAILED: {name} (exit code {result.returncode})")
        return False

    print(f"\n  ✓ STEP COMPLETE: {name}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run the full scraping pipeline")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip scraping, run filter+export on existing data")
    parser.add_argument("--csv", action="store_true",
                        help="Export to CSV instead of Google Sheets")
    parser.add_argument("--region", type=str, default=None,
                        choices=["santiago", "valparaiso", "coquimbo"],
                        help="Scrape a specific region only")
    parser.add_argument("--min-year", type=int, default=2015,
                        help="Minimum car year (default: 2015)")
    parser.add_argument("--min-days", type=int, default=14,
                        help="Minimum days active (default: 14)")
    parser.add_argument("--url", type=str, default=None,
                        help="Custom URL to scrape (overrides region)")
    parser.add_argument("--details", action="store_true",
                        help="Also scrape individual listing details")
    parser.add_argument("--safe", action="store_true",
                        help="Run in Safe Mode (no login required)")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  FB MARKETPLACE CAR FUNNEL — PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    python = sys.executable

    # ── Step 1: Scrape (Local Playwright) ──────────────────────────────
    if not args.skip_scrape:
        scrape_cmd = [python, str(EXECUTION_DIR / "scrape_fb_marketplace.py")]
        
        if args.url:
            scrape_cmd += ["--url", args.url]
        elif args.region:
            scrape_cmd += ["--region", args.region]
            
        if args.details:
            scrape_cmd += ["--details"]
        if args.safe:
            scrape_cmd += ["--no-login"]

        if not run_step("Scrape Facebook Marketplace (Local)", scrape_cmd):
            print("\n  Pipeline aborted at scraping step.")
            sys.exit(1)
    else:
        print("\n  → Skipping scrape step (using existing data)")

    # ── Step 2: Filter ─────────────────────────────────────────────────
    filter_cmd = [
        python, str(EXECUTION_DIR / "filter_listings.py"),
        "--min-year", str(args.min_year),
        "--min-days", str(args.min_days),
    ]

    if not run_step("Filter Listings", filter_cmd):
        print("\n  Pipeline aborted at filter step.")
        sys.exit(1)

    # ── Step 3: Export ─────────────────────────────────────────────────
    export_cmd = [python, str(EXECUTION_DIR / "export_to_sheets.py")]
    if args.csv:
        export_cmd += ["--csv"]

    if not run_step("Export Results", export_cmd):
        print("\n  Pipeline aborted at export step.")
        sys.exit(1)

    # ── Done ───────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  ✓ PIPELINE COMPLETE")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
