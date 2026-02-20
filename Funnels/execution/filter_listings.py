#!/usr/bin/env python3
from __future__ import annotations
"""
Filter Facebook Marketplace Car Listings
==========================================
Reads raw scraped data and filters by:
  - Year ≥ 2015
  - Days active ≥ 14 (not sold in last 2 weeks)
  - Not marked as sold
  - Deduplicates by URL

Usage:
    python execution/filter_listings.py
    python execution/filter_listings.py --min-year 2018 --min-days 7
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# ─── Config ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
INPUT_FILE = TMP_DIR / "scraped_cars.json"
OUTPUT_FILE = TMP_DIR / "filtered_cars.json"
STATS_FILE = TMP_DIR / "filter_stats.json"


# ─── Filter Logic ─────────────────────────────────────────────────────────────

def filter_listings(
    listings: list[dict],
    min_year: int = 2015,
    min_days_active: int = 14,
    exclude_sold: bool = True,
) -> tuple[list[dict], dict]:
    """
    Apply all filters and return (filtered_listings, stats).

    Filters:
      1. Year >= min_year (skip if year is None — flag for manual review)
      2. Days active >= min_days_active (skip if days_active is None)
      3. Not marked as sold
      4. Deduplicate by URL
    """
    stats = {
        "total_input": len(listings),
        "removed_sold": 0,
        "removed_too_new_listing": 0,
        "removed_year_too_old": 0,
        "removed_no_year": 0,
        "removed_no_date": 0,
        "removed_duplicate": 0,
        "total_output": 0,
    }

    filtered = []
    seen_urls = set()

    for listing in listings:
        url = listing.get("url", "")

        # ── Deduplicate ────────────────────────────────────────────────
        if url in seen_urls:
            stats["removed_duplicate"] += 1
            continue
        seen_urls.add(url)

        # ── Sold filter ────────────────────────────────────────────────
        if exclude_sold and listing.get("is_sold", False):
            stats["removed_sold"] += 1
            continue

        # ── Year filter ────────────────────────────────────────────────
        year = listing.get("year")
        if year is None:
            # Keep but flag — we can't be sure about the year
            listing["_flag"] = "no_year_detected"
            stats["removed_no_year"] += 1
            # Still include — user can manually check
            # If you want to skip: uncomment below and comment out the append
            # continue
        elif year < min_year:
            stats["removed_year_too_old"] += 1
            continue

        # ── Listing age filter ─────────────────────────────────────────
        days_active = listing.get("days_active")
        if days_active is None:
            listing["_flag"] = listing.get("_flag", "") + "|no_date"
            stats["removed_no_date"] += 1
            # Still include with flag
        elif days_active < min_days_active:
            stats["removed_too_new_listing"] += 1
            continue

        filtered.append(listing)

    stats["total_output"] = len(filtered)

    return filtered, stats


def sort_listings(listings: list[dict], sort_by: str = "days_active") -> list[dict]:
    """Sort filtered listings. Default: oldest listings first (most days active)."""
    def sort_key(l):
        val = l.get(sort_by)
        if val is None:
            return -1  # Put unknowns at the end
        return val

    return sorted(listings, key=sort_key, reverse=True)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Filter scraped FB Marketplace listings")
    parser.add_argument("--min-year", type=int, default=2015,
                        help="Minimum car year (default: 2015)")
    parser.add_argument("--min-days", type=int, default=14,
                        help="Minimum days active on marketplace (default: 14)")
    parser.add_argument("--include-sold", action="store_true",
                        help="Include listings marked as sold")
    parser.add_argument("--input", type=str, default=None,
                        help="Path to input JSON (default: .tmp/scraped_cars.json)")
    parser.add_argument("--output", type=str, default=None,
                        help="Path to output JSON (default: .tmp/filtered_cars.json)")
    args = parser.parse_args()

    input_file = Path(args.input) if args.input else INPUT_FILE
    output_file = Path(args.output) if args.output else OUTPUT_FILE

    # ── Load ───────────────────────────────────────────────────────────────
    if not input_file.exists():
        print(f"✗ Input file not found: {input_file}")
        print(f"  Run the scraper first: python execution/scrape_fb_marketplace.py")
        sys.exit(1)

    data = json.loads(input_file.read_text())
    listings = data.get("listings", [])
    print(f"{'='*60}")
    print(f"  FILTER LISTINGS")
    print(f"{'='*60}")
    print(f"  Input:      {input_file}")
    print(f"  Loaded:     {len(listings)} listings")
    print(f"  Min year:   {args.min_year}")
    print(f"  Min days:   {args.min_days}")
    print(f"  Excl sold:  {not args.include_sold}")

    # ── Filter ─────────────────────────────────────────────────────────────
    filtered, stats = filter_listings(
        listings,
        min_year=args.min_year,
        min_days_active=args.min_days,
        exclude_sold=not args.include_sold,
    )

    # ── Sort ───────────────────────────────────────────────────────────────
    filtered = sort_listings(filtered, sort_by="days_active")

    # ── Stats ──────────────────────────────────────────────────────────────
    print(f"\n  FILTER RESULTS:")
    print(f"  {'─'*40}")
    print(f"  Total input:            {stats['total_input']}")
    print(f"  Removed (sold):         {stats['removed_sold']}")
    print(f"  Removed (too new):      {stats['removed_too_new_listing']}")
    print(f"  Removed (year < {args.min_year}):  {stats['removed_year_too_old']}")
    print(f"  Removed (duplicates):   {stats['removed_duplicate']}")
    print(f"  Flagged (no year):      {stats['removed_no_year']}")
    print(f"  Flagged (no date):      {stats['removed_no_date']}")
    print(f"  {'─'*40}")
    print(f"  ✓ Output:               {stats['total_output']} listings")

    # ── Region breakdown ───────────────────────────────────────────────────
    region_counts = {}
    for l in filtered:
        r = l.get("region", "Unknown")
        region_counts[r] = region_counts.get(r, 0) + 1
    if region_counts:
        print(f"\n  BY REGION:")
        for region, count in sorted(region_counts.items()):
            print(f"    • {region}: {count}")

    # ── Save ───────────────────────────────────────────────────────────────
    output_data = {
        "filtered_at": datetime.now().isoformat(),
        "filters": {
            "min_year": args.min_year,
            "min_days_active": args.min_days,
            "exclude_sold": not args.include_sold,
        },
        "stats": stats,
        "total_listings": len(filtered),
        "listings": filtered,
    }

    output_file.write_text(json.dumps(output_data, indent=2, ensure_ascii=False))
    STATS_FILE.write_text(json.dumps(stats, indent=2))

    print(f"\n  → Saved to {output_file}")
    print(f"  → Stats saved to {STATS_FILE}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
