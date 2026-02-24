"""
parse_har.py — Extract Facebook Marketplace listings from a HAR file.

Usage:
    python parse_har.py --input /path/to/www.facebook.com.har
    python parse_har.py --input /path/to/file.har --output /path/to/Funnels

What it does:
1. Reads all GraphQL responses in the HAR file
2. Extracts marketplace_search listings from each response
3. Merges with any existing Apify dataset files (deduplicates by listing id)
4. Outputs a combined dataset_facebook-marketplace-scraper_<timestamp>_har.json
   in the Funnels folder — the dashboard auto-picks the largest file
"""

import json
import glob
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

# ── Defaults ────────────────────────────────────────────────────────────────
FUNNELS_DIR = Path(__file__).resolve().parent.parent


def parse_har(har_path: Path) -> list[dict]:
    """Parse a HAR file and return all marketplace_search listing nodes."""
    with open(har_path, "r", encoding="utf-8") as f:
        har = json.load(f)

    entries = har["log"]["entries"]
    listing_map: dict[str, dict] = {}  # id → normalized record

    for entry in entries:
        text = entry["response"]["content"].get("text", "")
        if not text or "marketplace_search" not in text:
            continue

        # Facebook returns newline-delimited JSON (multiple objects per response)
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            try:
                edges = obj["data"]["marketplace_search"]["feed_units"]["edges"]
            except (KeyError, TypeError):
                continue

            for edge in edges:
                node = edge.get("node", {})
                listing = node.get("listing")
                if not listing:
                    continue

                record = normalize_har_listing(listing)
                if record and record.get("id"):
                    listing_map[record["id"]] = record

    listings = list(listing_map.values())
    print(f"[har] Extracted {len(listings)} unique listings from HAR")
    return listings


def normalize_har_listing(listing: dict) -> Optional[dict]:
    """Convert a raw HAR GraphQL listing node to Apify-compatible format."""
    listing_id = listing.get("id")
    if not listing_id:
        return None

    # Title
    title = (
        listing.get("marketplace_listing_title")
        or listing.get("custom_title")
        or ""
    )

    # Price
    price_info = listing.get("listing_price") or {}
    listing_price = {
        "amount": price_info.get("amount", "0"),
        "formatted_amount": price_info.get("formatted_amount", ""),
        "currency": "CLP",
    }

    # Location — build locationText from reverse_geocode
    location_text = ""
    loc = listing.get("location") or {}
    rev = loc.get("reverse_geocode") or {}
    city = rev.get("city", "")
    state = rev.get("state", "")
    city_page = rev.get("city_page") or {}
    if city_page.get("display_name"):
        location_text = city_page["display_name"]
    elif city or state:
        parts = [p for p in [city, state] if p]
        location_text = ", ".join(parts)

    # Mileage subtitles
    subtitles = listing.get("custom_sub_titles_with_rendering_flags") or []
    # Normalize to camelCase for consistency with Apify format
    custom_sub_titles = [{"subtitle": s.get("subtitle", "")} for s in subtitles]

    # Photo — HAR uses .image.uri, map to photo_image_url for normalize_apify_item
    photo_url = ""
    primary_photo = listing.get("primary_listing_photo") or {}
    img = primary_photo.get("image") or {}
    photo_url = img.get("uri", "")

    # Seller name — THIS is the key new field from HAR
    seller = listing.get("marketplace_listing_seller") or {}
    seller_name = seller.get("name", "")
    seller_id = seller.get("id", "")

    return {
        "id": listing_id,
        "itemUrl": f"https://www.facebook.com/marketplace/item/{listing_id}/",
        "listingTitle": title,
        "listingPrice": listing_price,
        "locationText": {"text": location_text},
        "customSubTitlesWithRenderingFlags": custom_sub_titles,
        "primaryListingPhoto": {
            "photo_image_url": photo_url,
        },
        "isSold": listing.get("is_sold", False),
        "isLive": listing.get("is_live", True),
        "sellerName": seller_name,
        "sellerId": seller_id,
        "source": "har",
    }


def load_existing_apify(funnels_dir: Path) -> dict[str, dict]:
    """Load all existing Apify dataset JSON files and return id → record map."""
    pattern = str(funnels_dir / "dataset_facebook-marketplace-scraper_*.json")
    files = glob.glob(pattern)

    # Also check Downloads
    downloads = Path.home() / "Downloads"
    files += glob.glob(str(downloads / "dataset_facebook-marketplace-scraper_*.json"))

    existing: dict[str, dict] = {}
    for fpath in files:
        # Skip our own output files to avoid double-loading on re-runs
        if "_har.json" in fpath:
            continue
        try:
            raw = json.loads(Path(fpath).read_text(encoding="utf-8"))
            if not isinstance(raw, list):
                continue
            for item in raw:
                item_id = item.get("id")
                if item_id and (item.get("listingTitle") or item.get("listingAttributes")):
                    # Keep HAR record if already present (has seller name)
                    if item_id not in existing:
                        item.setdefault("source", "apify")
                        existing[item_id] = item
            print(f"[apify] Loaded {len(raw)} records from {Path(fpath).name}")
        except Exception as e:
            print(f"[apify] Error loading {fpath}: {e}")

    return existing


def merge(har_listings: list[dict], existing: dict[str, dict]) -> list[dict]:
    """Merge HAR listings with existing Apify data. HAR wins for duplicates (has seller name)."""
    merged = dict(existing)  # start with all existing
    new_count = 0
    updated_count = 0

    for item in har_listings:
        item_id = item["id"]
        if item_id in merged:
            # Update existing with seller name if we now have it
            if item.get("sellerName") and not merged[item_id].get("sellerName"):
                merged[item_id]["sellerName"] = item["sellerName"]
                merged[item_id]["sellerId"] = item.get("sellerId", "")
                updated_count += 1
        else:
            merged[item_id] = item
            new_count += 1

    print(f"[merge] {new_count} new listings added, {updated_count} existing enriched with seller name")
    print(f"[merge] Total unique listings: {len(merged)}")
    return list(merged.values())


def main():
    parser = argparse.ArgumentParser(description="Parse Facebook HAR file into Funnel dataset")
    parser.add_argument("--input", required=True, help="Path to .har file")
    parser.add_argument("--output", default=str(FUNNELS_DIR), help="Output directory (default: Funnels/)")
    parser.add_argument("--no-merge", action="store_true", help="Don't merge with existing Apify data")
    args = parser.parse_args()

    har_path = Path(args.input)
    output_dir = Path(args.output)

    if not har_path.exists():
        print(f"ERROR: HAR file not found: {har_path}")
        sys.exit(1)

    # 1. Extract from HAR
    har_listings = parse_har(har_path)
    if not har_listings:
        print("ERROR: No marketplace listings found in HAR file.")
        print("Make sure you recorded network traffic while browsing Facebook Marketplace search results.")
        sys.exit(1)

    # 2. Load & merge with existing data
    if not args.no_merge:
        existing = load_existing_apify(FUNNELS_DIR)
        final = merge(har_listings, existing)
    else:
        final = har_listings
        print(f"[merge] Skipped — using {len(final)} HAR listings only")

    # 3. Save output
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = output_dir / f"dataset_facebook-marketplace-scraper_{timestamp}_har.json"
    output_path.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n✅ Saved {len(final)} listings → {output_path.name}")
    print(f"   Full path: {output_path}")
    sellers_with_name = sum(1 for r in final if r.get("sellerName"))
    print(f"   Listings with seller name: {sellers_with_name}/{len(final)}")
    print(f"\nThe dashboard will auto-load this file on next reload.")
    print(f"Run: curl -X POST http://localhost:5001/api/reload   (if dashboard is running)")


if __name__ == "__main__":
    main()
