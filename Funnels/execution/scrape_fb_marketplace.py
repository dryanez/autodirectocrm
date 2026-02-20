#!/usr/bin/env python3
from __future__ import annotations
"""
Facebook Marketplace Car Scraper — Chile
=========================================
Scrapes vehicle listings from FB Marketplace for regions IV, V, and RM.
Uses Playwright with stealth to avoid detection.

Usage:
    python execution/scrape_fb_marketplace.py
    python execution/scrape_fb_marketplace.py --dry-run
    python execution/scrape_fb_marketplace.py --region santiago
    python execution/scrape_fb_marketplace.py --no-login
"""

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

# ─── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

FB_EMAIL = os.getenv("FB_EMAIL")
FB_PASSWORD = os.getenv("FB_PASSWORD")
DELAY_MIN = int(os.getenv("SCRAPE_DELAY_MIN", 3))
DELAY_MAX = int(os.getenv("SCRAPE_DELAY_MAX", 7))
MAX_SCROLL_PAGES = int(os.getenv("MAX_SCROLL_PAGES", 20))

# Project root (one level up from execution/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
COOKIES_FILE = PROJECT_ROOT / "fb_cookies.json"
OUTPUT_FILE = TMP_DIR / "scraped_cars.json"

# ─── Target Regions ───────────────────────────────────────────────────────────

REGIONS = {
    "santiago": {
        "name": "Región Metropolitana (Santiago)",
        "url": "https://www.facebook.com/marketplace/santiago/vehicles"
               "?minYear=2015&sortBy=date_listed_newest"
               "&exact=false&latitude=-33.4489&longitude=-70.6693&radius=20",
        "lat": -33.4489,
        "lon": -70.6693,
    },
    "valparaiso": {
        "name": "V Región (Valparaíso / Viña del Mar)",
        "url": "https://www.facebook.com/marketplace/valparaiso/vehicles"
               "?minYear=2015&sortBy=date_listed_newest"
               "&exact=false&latitude=-33.0472&longitude=-71.6127&radius=20",
        "lat": -33.0472,
        "lon": -71.6127,
    },
    "coquimbo": {
        "name": "IV Región (Coquimbo / La Serena)",
        "url": "https://www.facebook.com/marketplace/la-serena/vehicles"
               "?minYear=2015&sortBy=date_listed_newest"
               "&exact=false&latitude=-29.9027&longitude=-71.2520&radius=20",
        "lat": -29.9027,
        "lon": -71.2520,
    },
}

# User-Agent pool for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def human_delay(min_s=None, max_s=None):
    """Sleep for a random human-like duration."""
    lo = min_s or DELAY_MIN
    hi = max_s or DELAY_MAX
    time.sleep(random.uniform(lo, hi))


def parse_relative_date(text: str) -> str | None:
    """
    Convert Facebook's relative date strings to ISO date.
    E.g. "Listed 2 weeks ago", "hace 3 semanas", "Listed yesterday"
    Returns ISO date string or None if unparseable.
    """
    text = text.lower().strip()
    now = datetime.now()

    # English patterns
    if "yesterday" in text or "ayer" in text:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    if "today" in text or "hoy" in text:
        return now.strftime("%Y-%m-%d")

    # "X days ago" / "hace X días"
    m = re.search(r"(\d+)\s*(?:day|día|dias|days)", text)
    if m:
        return (now - timedelta(days=int(m.group(1)))).strftime("%Y-%m-%d")

    # "X weeks ago" / "hace X semanas"
    m = re.search(r"(\d+)\s*(?:week|semana|semanas|weeks)", text)
    if m:
        return (now - timedelta(weeks=int(m.group(1)))).strftime("%Y-%m-%d")

    # "X months ago" / "hace X meses"
    m = re.search(r"(\d+)\s*(?:month|mes|meses|months)", text)
    if m:
        return (now - timedelta(days=int(m.group(1)) * 30)).strftime("%Y-%m-%d")

    # "X hours ago" / "hace X horas"
    m = re.search(r"(\d+)\s*(?:hour|hora|horas|hours)", text)
    if m:
        return now.strftime("%Y-%m-%d")

    # "X minutes ago" / "hace X minutos"
    m = re.search(r"(\d+)\s*(?:minute|minuto|minutos|minutes|min)", text)
    if m:
        return now.strftime("%Y-%m-%d")

    return None


def extract_year_from_title(title: str) -> int | None:
    """Extract a 4-digit year (2015–2030) from listing title."""
    m = re.search(r"\b(20[12]\d)\b", title)
    if m:
        year = int(m.group(1))
        if 2015 <= year <= 2030:
            return year
    return None


def calculate_days_active(listed_date_iso: str | None) -> int | None:
    """Calculate days between listed date and today."""
    if not listed_date_iso:
        return None
    try:
        listed = datetime.strptime(listed_date_iso, "%Y-%m-%d")
        return (datetime.now() - listed).days
    except ValueError:
        return None


# ─── Core Scraper ─────────────────────────────────────────────────────────────

def login_to_facebook(page):
    """Log into Facebook and save cookies for reuse."""
    print("  → Navigating to Facebook login...")
    page.goto("https://www.facebook.com/login", wait_until="networkidle")
    human_delay(2, 4)

    # Check if already logged in (cookies worked)
    if "login" not in page.url.lower():
        print("  → Already logged in via cookies!")
        return True

    if not FB_EMAIL or not FB_PASSWORD:
        print("  ✗ ERROR: FB_EMAIL and FB_PASSWORD must be set in .env")
        return False

    print(f"  → Logging in as {FB_EMAIL}...")
    try:
        page.fill('input[name="email"]', FB_EMAIL)
        human_delay(0.5, 1.5)
        page.fill('input[name="pass"]', FB_PASSWORD)
        human_delay(0.5, 1.0)
        page.click('button[name="login"]')
    except Exception as e:
        print(f"  ⚠ Login failed (element not found or timeout): {e}")
        return False

    # Wait for redirect
    page.wait_for_load_state("networkidle", timeout=30000)
    human_delay(3, 5)

    # Check for login success or checkpoint
    current_url = page.url.lower()
    if "login" in current_url or "checkpoint" in current_url:
        print("\n" + "!" * 60)
        print("  ⚠ 2FA / SECURITY CHECKPOINT DETECTED (or login failed)")
        print("!" * 60)
        print("  → The script will now PAUSE for up to 120 seconds.")
        print("  → Please manually complete the login (enter code/approve on phone) in the browser window.")
        
        start_wait = time.time()
        while time.time() - start_wait < 120:
            if "login" not in page.url.lower() and "checkpoint" not in page.url.lower():
                print("\n  ✓ Login Successful! Continuing scrape...")
                return True
            time.sleep(2)
            sys.stdout.write(".")
            sys.stdout.flush()
        
        print("\n  ✗ Timed out waiting for manual login.")
        return False

    print("  ✓ Logged in successfully.")
    return True


def save_cookies(context):
    """Save browser cookies for session persistence."""
    cookies = context.cookies()
    COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    print(f"  → Cookies saved to {COOKIES_FILE}")


def load_cookies(context):
    """Load saved cookies if they exist."""
    if COOKIES_FILE.exists():
        cookies = json.loads(COOKIES_FILE.read_text())
        context.add_cookies(cookies)
        print(f"  → Loaded cookies from {COOKIES_FILE}")
        return True
    return False


def dismiss_popups(page):
    """Dismiss common FB popups/modals that block scraping."""
    popup_selectors = [
        '[aria-label="Close"]',
        '[aria-label="Cerrar"]',
        'div[role="dialog"] [aria-label="Close"]',
        'div[role="dialog"] [aria-label="Cerrar"]',
    ]
    for sel in popup_selectors:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click()
                human_delay(0.5, 1.0)
        except Exception:
            pass



    # Check for login modal (blocks content without login)
    login_modal = page.query_selector('div[role="dialog"] a[href*="/login/"]')
    if login_modal:
        # If we see a login modal, we might want to close it if possible, 
        # or just acknowledge it blocks further scrolling
        try:
            close_btn = page.query_selector('div[role="dialog"] [aria-label="Close"]')
            if close_btn:
                close_btn.click()
        except:
            pass

def scroll_and_collect(page, region_key: str, region_name: str) -> list[dict]:
    """
    Scroll through marketplace listings and collect data.
    Returns list of raw listing dicts.
    """
    listings = []
    seen_urls = set()

    print(f"\n  → Scrolling and collecting listings for {region_name}...")

    for scroll_round in range(MAX_SCROLL_PAGES):
        # Dismiss any popups
        dismiss_popups(page)

        # Find listing cards — FB uses <a> tags with href containing /marketplace/item/
        links = page.query_selector_all('a[href*="/marketplace/item/"]')

        new_count = 0
        for link in links:
            try:
                href = link.get_attribute("href") or ""
                # Normalize URL
                if href.startswith("/"):
                    href = "https://www.facebook.com" + href
                # Remove query params for dedup
                clean_url = href.split("?")[0]

                if clean_url in seen_urls:
                    continue
                seen_urls.add(clean_url)
                new_count += 1

                # Extract text content from listing card
                card_text = link.inner_text() or ""
                lines = [l.strip() for l in card_text.split("\n") if l.strip()]

                # Typical FB card structure: Price, Title, Location, Date
                price = lines[0] if len(lines) > 0 else ""
                title = lines[1] if len(lines) > 1 else ""
                location = lines[2] if len(lines) > 2 else ""
                date_text = lines[3] if len(lines) > 3 else ""

                # Parse year from title
                year = extract_year_from_title(title)

                # Parse listed date
                listed_date = parse_relative_date(date_text)
                days_active = calculate_days_active(listed_date)

                # Check sold status from card text
                full_text_lower = card_text.lower()
                is_sold = any(w in full_text_lower for w in [
                    "sold", "vendido", "vendida", "no disponible", "unavailable"
                ])

                listing = {
                    "url": clean_url,
                    "title": title,
                    "price": price,
                    "year": year,
                    "location": location,
                    "region": region_name,
                    "region_key": region_key,
                    "date_text": date_text,
                    "listed_date": listed_date,
                    "days_active": days_active,
                    "is_sold": is_sold,
                    "seller_name": None,  # Populated in detail scrape if needed
                    "messenger_link": None,
                    "scraped_at": datetime.now().isoformat(),
                }
                listings.append(listing)

            except Exception as e:
                print(f"    ⚠ Error parsing listing card: {e}")
                continue

        print(f"    Scroll {scroll_round + 1}/{MAX_SCROLL_PAGES}: "
              f"{new_count} new listings (total: {len(listings)})")

        if new_count == 0 and scroll_round > 2:
            print("    → No new listings found, stopping scroll.")
            break

        # Check for login barrier
        if page.query_selector('div[role="dialog"] a[href*="/login/"]'):
            print("    ⚠ Login modal detected (stopping scroll to avoid ban/block)")
            break

        # Scroll down
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        human_delay()

    return listings


def scrape_listing_details(page, listing: dict) -> dict:
    """
    Navigate to individual listing page to get seller info and messenger link.
    Only called for listings that pass the filter.
    """
    try:
        page.goto(listing["url"], wait_until="domcontentloaded", timeout=15000)
        human_delay(2, 4)
        dismiss_popups(page)

        # Try to find seller name
        # FB typically shows seller in a link below the listing details
        seller_els = page.query_selector_all('a[href*="/marketplace/profile/"]')
        if seller_els:
            listing["seller_name"] = seller_els[0].inner_text().strip()

        # Try to get messenger link
        msg_btn = page.query_selector('a[href*="messenger.com"], '
                                       'a[href*="/messages/"], '
                                       '[aria-label*="Message"], '
                                       '[aria-label*="Mensaje"], '
                                       '[aria-label*="Send message"]')
        if msg_btn:
            msg_href = msg_btn.get_attribute("href")
            if msg_href:
                listing["messenger_link"] = msg_href

        # Check for detailed date info
        # Look for "Listed X ago" text on the detail page
        page_text = page.inner_text("body") or ""
        for line in page_text.split("\n"):
            line_stripped = line.strip()
            if any(k in line_stripped.lower() for k in
                   ["listed", "publicado", "publicada", "hace"]):
                parsed = parse_relative_date(line_stripped)
                if parsed:
                    listing["listed_date"] = parsed
                    listing["days_active"] = calculate_days_active(parsed)
                    listing["date_text"] = line_stripped
                    break

        # Check sold status on detail page
        if any(k in page_text.lower() for k in
               ["sold", "vendido", "vendida", "no disponible", "unavailable",
                "this listing is no longer available"]):
            listing["is_sold"] = True

    except Exception as e:
        print(f"    ⚠ Could not scrape details for {listing['url']}: {e}")

    return listing


def scrape_region(page, region_key: str) -> list[dict]:
    """Scrape all listings for a given region."""
    region = REGIONS[region_key]
    print(f"\n{'='*60}")
    print(f"  SCRAPING: {region['name']}")
    print(f"  URL: {region['url']}")
    print(f"  Location: {region.get('lat')}, {region.get('lon')}")
    print(f"{'='*60}")

    # Set geolocation if available
    if "lat" in region and "lon" in region:
        page.context.set_geolocation({"latitude": region["lat"], "longitude": region["lon"]})

    page.goto(region["url"], wait_until="domcontentloaded", timeout=30000)
    human_delay(3, 5)
    dismiss_popups(page)

    # Collect listings from search results
    listings = scroll_and_collect(page, region_key, region["name"])
    print(f"\n  → Collected {len(listings)} total listings from {region['name']}")

    return listings


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape FB Marketplace cars in Chile")
    parser.add_argument("--dry-run", action="store_true",
                        help="Test mode: verify config without scraping")
    parser.add_argument("--region", type=str, default=None,
                        choices=list(REGIONS.keys()),
                        help="Scrape a specific region only")
    parser.add_argument("--url", type=str, default=None,
                        help="Custom URL to scrape (overrides region)")
    parser.add_argument("--details", action="store_true",
                        help="Also scrape individual listing pages for seller info")
    parser.add_argument("--no-login", action="store_true",
                        help="Skip login (Safe Mode). Scrapes fewer listings but zero risk.")
    args = parser.parse_args()

    # ── Dry Run ────────────────────────────────────────────────────────────
    if args.dry_run:
        print("=" * 60)
        print("  DRY RUN — Config Verification")
        print("=" * 60)
        print(f"\n  FB_EMAIL:       {'✓ set' if FB_EMAIL else '✗ MISSING'}")
        print(f"  FB_PASSWORD:    {'✓ set' if FB_PASSWORD else '✗ MISSING'}")
        print(f"  Delay range:    {DELAY_MIN}–{DELAY_MAX}s")
        print(f"  Max scrolls:    {MAX_SCROLL_PAGES}")
        print(f"  Output:         {OUTPUT_FILE}")
        print(f"  Cookies:        {COOKIES_FILE}")
        print(f"\n  Target Regions:")
        regions_to_scrape = [args.region] if args.region else list(REGIONS.keys())
        for key in regions_to_scrape:
            r = REGIONS[key]
            print(f"    • {r['name']}")
            print(f"      {r['url']}")
        print(f"\n  ✓ Dry run complete. Everything looks good!")
        return

    # ── Real Scrape ────────────────────────────────────────────────────────
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError as e:
        print(f"ERROR: Import failed: {e}")
        print("Install deps first: pip install -r requirements.txt && playwright install")
        sys.exit(1)

    all_listings = []
    
    if args.url:
        print(f"  → Scraping Custom URL: {args.url}")
        REGIONS["custom"] = {"name": "Custom URL", "url": args.url}
        regions_to_scrape = ["custom"]
    else:
        regions_to_scrape = [args.region] if args.region else list(REGIONS.keys())

    print("\n" + "=" * 60)
    print("  FB MARKETPLACE CAR SCRAPER — CHILE")
    print(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Regions: {', '.join(regions_to_scrape)}")
    print("=" * 60)

    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(
            headless=False,  # Set to True for production
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768},
            locale="es-CL",
            timezone_id="America/Santiago",
            permissions=["geolocation"],
        )

        # Load saved cookies
        load_cookies(context)

        page = context.new_page()

        # Apply stealth
        stealth = Stealth(
            navigator_languages_override=("es-CL", "es"),
            navigator_vendor_override="Google Inc.",
            webgl_vendor_override="Intel Inc.",
            webgl_renderer_override="Intel Iris OpenGL Engine",
        )
        stealth.apply_stealth_sync(page)

        # Login
        if not args.no_login:
            if not login_to_facebook(page):
                print("\n  ✗ Could not log in. Aborting.")
                browser.close()
                sys.exit(1)
            # Save cookies for next run
            save_cookies(context)
        else:
            print("\n  ⚠ Running in SAFE MODE (No Login). Results will be limited.")

        # Scrape each region
        for region_key in regions_to_scrape:
            try:
                region_listings = scrape_region(page, region_key)
                all_listings.extend(region_listings)
                human_delay(5, 10)  # Pause between regions
            except Exception as e:
                print(f"\n  ✗ Error scraping {region_key}: {e}")
                continue

        # Optionally scrape individual listing details
        if args.details:
            print(f"\n{'='*60}")
            print(f"  SCRAPING LISTING DETAILS ({len(all_listings)} listings)")
            print(f"{'='*60}")
            for i, listing in enumerate(all_listings):
                print(f"  [{i+1}/{len(all_listings)}] {listing['title'][:50]}...")
                listing = scrape_listing_details(page, listing)
                human_delay(2, 5)

        browser.close()

    # ── Save Results ───────────────────────────────────────────────────────
    output = {
        "scraped_at": datetime.now().isoformat(),
        "regions": regions_to_scrape,
        "total_listings": len(all_listings),
        "listings": all_listings,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\n{'='*60}")
    print(f"  ✓ DONE! Scraped {len(all_listings)} listings")
    print(f"  → Saved to {OUTPUT_FILE}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
