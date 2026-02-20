#!/usr/bin/env python3
from __future__ import annotations
"""
Apify Facebook Marketplace Scraper
=========================================
Runs Apify actor `moJkRxrc85HZ6` (facebook-marketplace-scraper) 
and saves results in the format expected by the pipeline.

Usage:
    python execution/scrape_apify.py --url "..."
    python execution/scrape_apify.py --region santiago
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from apify_client import ApifyClient

# ─── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

# User provided API Token
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# Default Actor (U5DUNxhH3qKt5PnCf - likely official one)
ACTOR_ID = "U5DUNxhH3qKt5PnCf" 

# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
OUTPUT_FILE = TMP_DIR / "scraped_cars.json"

# Santiago URL provided by user (Original complex one)
# This one seemed to work in their manual run (at least didn't block immediately)
SANTIAGO_URL = "https://www.facebook.com/marketplace/106647439372422/search?minPrice=3500&query=Vehicles&category_id=546583916084032&exact=false&referral_ui_component=category_menu_item"

REGIONS = {
    "santiago": {
        "name": "Región Metropolitana (Santiago)",
        "url": SANTIAGO_URL
    },
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def map_apify_result(item: dict) -> dict:
    """Map Apify result format (menew/facebook-marketplace-scraper) to our pipeline format."""
    
    # Extract price (often formatted string or raw number)
    price = item.get("listingPrice") or item.get("price")
    if price and isinstance(price, (int, float)):
         price = f"${price}"

    # Extract location
    location = item.get("location") or item.get("marketplace_listing_location_name")

    # Extract Year from title (simple heuristic if not provided)
    title = item.get("title") or item.get("marketplace_listing_title") or ""
    
    return {
        "url": item.get("url") or item.get("link"),
        "title": title,
        "price": str(price) if price else None,
        "location": location,
        "region": REGIONS["santiago"]["name"], # Default for now
        "region_key": "santiago",
        "date_text": "Scraped via Apify", 
        "listed_date": None, 
        "days_active": None,
        "is_sold": item.get("isSold", False),
        "seller_name": item.get("sellerName"),
        "scraped_at": datetime.now().isoformat(),
        # Store raw if debugging needed
        # "raw": item 
    }

def run_actor(start_url: str):
    """Run the Apify actor with the given start URL."""
    print(f"Initializing ApifyClient with token ending in ...{APIFY_TOKEN[-4:]}")
    client = ApifyClient(APIFY_TOKEN)

    # Input structure for U5DUNxhH3qKt5PnCf
    # Matching user's successful config: No proxy config, logic handling elsewhere
    run_input = {
        "startUrls": [{"url": start_url}],
        "maxItems": 60, 
    }

    print(f"Starting Actor {ACTOR_ID}...")
    print(f"Target URL: {start_url}")
    
    # Run the actor and wait for it to finish
    # This might take time.
    run = client.actor(ACTOR_ID).call(run_input=run_input)
    
    if not run:
        raise Exception("Actor run failed to start or return info.")

    print(f"Actor finished. Run ID: {run['id']}")
    print(f"Status: {run['status']}")
    print("Fetching results from default dataset...")

    # Fetch results from the run's dataset
    listings = []
    dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items
    
    for item in dataset_items:
        try:
            mapped = map_apify_result(item)
            listings.append(mapped)
        except Exception as e:
            print(f"Error mapping item: {e}")

    return listings

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape FB Marketplace via Apify")
    parser.add_argument("--region", type=str, default="santiago",
                        choices=list(REGIONS.keys()) + ["custom"],
                        help="Predefined region or custom URL")
    parser.add_argument("--url", type=str, help="Custom URL to scrape")
    
    args = parser.parse_args()

    # Determine URL
    target_url = None
    if args.url:
        target_url = args.url
    elif args.region in REGIONS:
        target_url = REGIONS[args.region]["url"]
    
    if not target_url:
        print("Error: Must provide --region or --url")
        sys.exit(1)

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        results = run_actor(target_url)
    except Exception as e:
        print(f"Error running Apify actor: {e}")
        # Print detailed traceback if possible
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Save Results
    output = {
        "scraped_at": datetime.now().isoformat(),
        "total_listings": len(results),
        "listings": results,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\n{'='*60}")
    print(f"  ✓ DONE! Scraped {len(results)} listings via Apify")
    print(f"  → Saved to {OUTPUT_FILE}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
