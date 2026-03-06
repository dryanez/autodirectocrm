"""
scrape_fb_live.py — Facebook Marketplace GraphQL scraper for Funnels dashboard.

Uses the REAL Google Chrome (with your logged-in FB session) via Playwright.
Copies your Chrome profile to a temp dir so the original is never locked.
Outputs Apify-compatible JSON that the Funnels dashboard ingests directly.

Usage:
  python scrape_fb_live.py [--output /path/to/output.json] [--scrolls 30]

Requirements:
  pip install playwright && playwright install  (or use fb app/venv)
"""

import asyncio
import json
import sys
import shutil
import tempfile
import argparse
from pathlib import Path
from datetime import datetime

# ─── Config ─────────────────────────────────────────────────────────────────────
DEFAULT_TARGET_URL = (
    "https://www.facebook.com/marketplace/106647439372422/search/"
    "?minPrice=8000000&query=Vehicles&exact=false&radius=20"
)
DEFAULT_SCROLL_STEPS = 30
SCROLL_PX    = 1200
SCROLL_DELAY = 2.5

# Real Chrome — must be installed
CHROME_EXECUTABLE = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
CHROME_USER_DATA  = Path.home() / "Library/Application Support/Google/Chrome"

# ─── Global store ───────────────────────────────────────────────────────────────
vehicles: dict[str, dict] = {}
graphql_count = 0


def parse_feed_units(data: dict):
    """Extract listing nodes from a marketplace_search GraphQL response."""
    try:
        edges = data["data"]["marketplace_search"]["feed_units"]["edges"]
    except (KeyError, TypeError):
        return

    for edge in edges:
        try:
            listing = edge["node"]["listing"]
            lid = str(listing.get("id", ""))
            if not lid or lid in vehicles:
                continue

            title = listing.get("marketplace_listing_title") or listing.get("custom_title", "")
            if not title:
                continue

            # Price
            price_info = listing.get("listing_price") or {}

            # Location
            loc = listing.get("location") or {}
            rev = loc.get("reverse_geocode") or {}
            city_page = rev.get("city_page") or {}
            location_text = (
                city_page.get("display_name")
                or ", ".join(p for p in [rev.get("city", ""), rev.get("state", "")] if p)
                or ""
            )

            # Mileage / subtitle
            subtitles = listing.get("custom_sub_titles_with_rendering_flags") or []
            custom_sub_titles = [{"subtitle": s.get("subtitle", "")} for s in subtitles]

            # Photo
            primary_photo = listing.get("primary_listing_photo") or {}
            photo_img = primary_photo.get("image") or {}
            photo_url = photo_img.get("uri", "")

            # Seller
            seller = listing.get("marketplace_listing_seller") or {}

            vehicles[lid] = {
                "id": lid,
                "itemUrl": f"https://www.facebook.com/marketplace/item/{lid}/",
                "listingTitle": title,
                "listingPrice": {
                    "amount": price_info.get("amount", "0"),
                    "formatted_amount": price_info.get("formatted_amount", ""),
                    "currency": "CLP",
                },
                "locationText": {"text": location_text},
                "customSubTitlesWithRenderingFlags": custom_sub_titles,
                "primaryListingPhoto": {"photo_image_url": photo_url},
                "isSold": listing.get("is_sold", False),
                "isLive": listing.get("is_live", True),
                "sellerName": seller.get("name", ""),
                "sellerId": seller.get("id", ""),
                "source": "scraper",
                "scraped_at": datetime.now().isoformat(),
            }

            print(
                f"  ✅ [{len(vehicles):>3}] {title[:50]:<50} | "
                f"{price_info.get('formatted_amount', 'N/A'):<18} | {location_text}",
                file=sys.stderr,
            )
        except Exception:
            continue


async def handle_response(response):
    global graphql_count
    if "/api/graphql" not in response.url:
        return
    graphql_count += 1
    try:
        text = await response.text()
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                parse_feed_units(data)
            except json.JSONDecodeError:
                pass
    except Exception:
        pass


async def run_scrape(target_url: str, scroll_steps: int) -> list[dict]:
    """Run the scraper using the real Chrome with your FB session."""
    global vehicles, graphql_count
    vehicles = {}
    graphql_count = 0

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("ERROR: playwright not installed.", file=sys.stderr)
        return []

    if not Path(CHROME_EXECUTABLE).exists():
        print(f"ERROR: Chrome not found at {CHROME_EXECUTABLE}", file=sys.stderr)
        return []

    if not CHROME_USER_DATA.exists():
        print("ERROR: Chrome user data directory not found!", file=sys.stderr)
        return []

    print("📂 Copying Chrome profile to temp directory…", file=sys.stderr)
    tmp_dir = Path(tempfile.mkdtemp(prefix="fb_scrape_"))
    default_src = CHROME_USER_DATA / "Default"
    default_dst = tmp_dir / "Default"

    try:
        shutil.copytree(
            default_src, default_dst,
            ignore=shutil.ignore_patterns(
                'Cache', 'Code Cache', 'GPUCache', 'Service Worker',
                'blob_storage', 'IndexedDB', 'File System',
                'GCM Store', 'BudgetDatabase', 'optimization_guide*',
                'heavy_ad*', 'AutofillStrikeDatabase',
                'databases', 'Platform Notifications', 'shared_proto_db',
            ),
            dirs_exist_ok=True,
        )
    except Exception as e:
        print(f"⚠️  Profile copy warning: {e}", file=sys.stderr)

    local_state = CHROME_USER_DATA / "Local State"
    if local_state.exists():
        try:
            shutil.copy2(local_state, tmp_dir / "Local State")
        except Exception:
            pass

    print("✅ Profile ready", file=sys.stderr)

    try:
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(tmp_dir),
                headless=False,
                executable_path=CHROME_EXECUTABLE,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-extensions",
                ],
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            )
            page = context.pages[0] if context.pages else await context.new_page()
            page.on("response", handle_response)

            print("🌐 Opening Facebook…", file=sys.stderr)
            await page.goto("https://www.facebook.com", wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(3)

            current_url = page.url
            print(f"📍 URL: {current_url}", file=sys.stderr)

            logged_in = (
                "login" not in current_url
                and "checkpoint" not in current_url
                and "signup" not in current_url
            )

            if not logged_in:
                print("⚠️  Not logged in — waiting up to 120s for manual login…", file=sys.stderr)
                for _ in range(120):
                    await asyncio.sleep(1)
                    u = page.url
                    if "login" not in u and "checkpoint" not in u and "signup" not in u:
                        print("✅ Logged in!", file=sys.stderr)
                        logged_in = True
                        break
                if not logged_in:
                    print("❌ Login timeout — aborting.", file=sys.stderr)
                    await context.close()
                    return []
            else:
                print("✅ Already logged in!", file=sys.stderr)

            print("\n🌐 Navigating to Marketplace…", file=sys.stderr)
            await page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
            await asyncio.sleep(5)

            print(f"\n🔄 Scrolling {scroll_steps} times…\n", file=sys.stderr)
            for i in range(1, scroll_steps + 1):
                await page.evaluate(f"window.scrollBy(0, {SCROLL_PX})")
                print(
                    f"  Scroll {i:>2}/{scroll_steps} — {len(vehicles)} vehicles | {graphql_count} GraphQL",
                    file=sys.stderr,
                )
                await asyncio.sleep(SCROLL_DELAY)

            await asyncio.sleep(3)
            await context.close()
            print(f"\n✅ Done! {len(vehicles)} vehicles captured.", file=sys.stderr)

    except Exception as e:
        print(f"❌ Scrape error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print("🗑️  Temp profile cleaned up.", file=sys.stderr)

    return list(vehicles.values())


def main():
    parser = argparse.ArgumentParser(description="Scrape Facebook Marketplace (real Chrome)")
    parser.add_argument("--output", "-o", help="Output JSON file path (default: stdout)")
    parser.add_argument("--scrolls", "-s", type=int, default=DEFAULT_SCROLL_STEPS)
    parser.add_argument("--url", default=DEFAULT_TARGET_URL)
    parser.add_argument("--headless", action="store_true", help="(ignored — always visible)")
    args = parser.parse_args()

    listings = asyncio.run(run_scrape(args.url, args.scrolls))

    output = json.dumps(listings, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"💾 Saved {len(listings)} listings → {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
