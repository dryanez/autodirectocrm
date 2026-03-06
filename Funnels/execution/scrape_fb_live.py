"""
scrape_fb_live.py — Facebook Marketplace GraphQL scraper for Funnels dashboard.

Logs into Facebook with email/password credentials, scrapes Marketplace listings.
Saves session cookies after first login so subsequent runs skip the login step.
Outputs Apify-compatible JSON that the Funnels dashboard ingests directly.

Usage:
  python scrape_fb_live.py [--output /path/to/output.json] [--scrolls 30]
"""

import asyncio
import json
import sys
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

FB_EMAIL    = "felipe@autodirecto.cl"
FB_PASSWORD = "Todaysiagoodday01@"

# Session cookies saved here after first login — reused on next runs
COOKIES_FILE = Path(__file__).parent.parent / "fb_cookies.json"

# ─── Global store ───────────────────────────────────────────────────────────────
vehicles: dict[str, dict] = {}
graphql_count = 0


def parse_feed_units(data: dict):
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
            price_info = listing.get("listing_price") or {}
            loc = listing.get("location") or {}
            rev = loc.get("reverse_geocode") or {}
            city_page = rev.get("city_page") or {}
            location_text = (
                city_page.get("display_name")
                or ", ".join(p for p in [rev.get("city", ""), rev.get("state", "")] if p)
                or ""
            )
            subtitles = listing.get("custom_sub_titles_with_rendering_flags") or []
            custom_sub_titles = [{"subtitle": s.get("subtitle", "")} for s in subtitles]
            primary_photo = listing.get("primary_listing_photo") or {}
            photo_url = (primary_photo.get("image") or {}).get("uri", "")
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
                parse_feed_units(json.loads(line))
            except json.JSONDecodeError:
                pass
    except Exception:
        pass


async def do_login(page) -> bool:
    print("🔐 Logging in with credentials…", file=sys.stderr)
    try:
        await page.goto("https://www.facebook.com/login", wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(2)

        # Dismiss cookie banner if present
        try:
            for selector in ['[data-testid="cookie-policy-manage-dialog-accept-button"]',
                             'button[title="Allow all cookies"]',
                             'button:has-text("Allow all cookies")',
                             'button:has-text("Accept all")']:
                btn = page.locator(selector)
                if await btn.count() > 0:
                    await btn.first.click()
                    await asyncio.sleep(1)
                    break
        except Exception:
            pass

        await page.locator('#email').wait_for(timeout=10_000)
        await page.locator('#email').fill(FB_EMAIL)
        await asyncio.sleep(0.4)
        await page.locator('#pass').fill(FB_PASSWORD)
        await asyncio.sleep(0.4)
        await page.locator('#loginbutton').click()

        print("⏳ Waiting for login redirect…", file=sys.stderr)
        for i in range(30):
            await asyncio.sleep(2)
            url = page.url
            print(f"   [{i*2}s] {url[:80]}", file=sys.stderr)
            if "checkpoint" in url:
                print("⚠️  Security checkpoint — waiting 90s for manual approval…", file=sys.stderr)
                await asyncio.sleep(90)
                if "checkpoint" not in page.url:
                    return True
                print("❌ Checkpoint not cleared.", file=sys.stderr)
                return False
            if not any(x in url for x in ("login", "signup")):
                print("✅ Login successful!", file=sys.stderr)
                return True

        print("❌ Login timeout.", file=sys.stderr)
        return False
    except Exception as e:
        print(f"❌ Login error: {e}", file=sys.stderr)
        import traceback; traceback.print_exc(file=sys.stderr)
        return False


async def run_scrape(target_url: str, scroll_steps: int) -> list[dict]:
    global vehicles, graphql_count
    vehicles = {}
    graphql_count = 0

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium", file=sys.stderr)
        return []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-extensions",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="es-CL",
        )

        # ── Try saved cookies first ──────────────────────────────────────
        session_ok = False
        if COOKIES_FILE.exists():
            try:
                saved = json.loads(COOKIES_FILE.read_text())
                if saved:
                    await context.add_cookies(saved)
                    print(f"🍪 Restored {len(saved)} saved cookies — checking session…", file=sys.stderr)
                    page = await context.new_page()
                    page.on("response", handle_response)
                    await page.goto("https://www.facebook.com", wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(3)
                    url = page.url
                    print(f"📍 URL: {url}", file=sys.stderr)
                    if not any(x in url for x in ("login", "checkpoint", "signup")):
                        print("✅ Session restored!", file=sys.stderr)
                        session_ok = True
                    else:
                        print("🍪 Cookies expired — logging in fresh…", file=sys.stderr)
                        await page.close()
            except Exception as e:
                print(f"⚠️  Cookie restore error: {e}", file=sys.stderr)

        # ── Fresh login if needed ────────────────────────────────────────
        if not session_ok:
            page = await context.new_page()
            page.on("response", handle_response)
            if not await do_login(page):
                await browser.close()
                return []
            # Save cookies for next run
            try:
                cookies = await context.cookies()
                COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
                print(f"💾 Saved {len(cookies)} session cookies → {COOKIES_FILE.name}", file=sys.stderr)
            except Exception as e:
                print(f"⚠️  Could not save cookies: {e}", file=sys.stderr)

        # ── Scrape Marketplace ───────────────────────────────────────────
        print(f"\n🌐 Navigating to Marketplace…", file=sys.stderr)
        await page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
        await asyncio.sleep(5)

        print(f"\n🔄 Scrolling {scroll_steps} times…\n", file=sys.stderr)
        for i in range(1, scroll_steps + 1):
            await page.evaluate(f"window.scrollBy(0, {SCROLL_PX})")
            print(
                f"  Scroll {i:>2}/{scroll_steps} — {len(vehicles)} vehicles | {graphql_count} GraphQL hits",
                file=sys.stderr,
            )
            await asyncio.sleep(SCROLL_DELAY)

        await asyncio.sleep(3)
        await browser.close()
        print(f"\n✅ Done! {len(vehicles)} vehicles captured.", file=sys.stderr)

    return list(vehicles.values())


def main():
    parser = argparse.ArgumentParser(description="Scrape Facebook Marketplace")
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--scrolls", "-s", type=int, default=DEFAULT_SCROLL_STEPS)
    parser.add_argument("--url", default=DEFAULT_TARGET_URL)
    parser.add_argument("--headless", action="store_true", help="(ignored — needs visible browser)")
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
