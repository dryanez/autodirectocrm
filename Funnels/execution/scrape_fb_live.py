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
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

# ─── Load .env if present ────────────────────────────────────────────────────────
_env_file = Path(__file__).parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ─── Config ─────────────────────────────────────────────────────────────────────
DEFAULT_TARGET_URL = (
    "https://www.facebook.com/marketplace/106647439372422/search/"
    "?minPrice=4000000&query=Vehicles&exact=false&radius=20"
)
MAX_SCROLLS          = 2000      # safety cap — never scroll more than this
TARGET_LEADS         = 500       # stop early when we reach this many qualifying V-Region leads
MIN_PRICE_CLP        = 4_000_000 # 4 million CLP minimum
DEFAULT_SCROLL_STEPS = MAX_SCROLLS
SCROLL_PX    = 1200
SCROLL_DELAY = 2.5

FB_EMAIL    = os.environ.get("FB_EMAIL", "REDACTED_EMAIL")
FB_PASSWORD = os.environ.get("FB_PASSWORD", "REDACTED_PASSWORD")

# Session cookies saved here after first login — reused on next runs
COOKIES_FILE = Path(__file__).parent.parent / "fb_cookies.json"

# ─── V Region communes (lowercase) ─────────────────────────────────────────────
V_REGION_COMMUNES = {
    "viña del mar", "vina del mar", "concón", "concon",
    "valparaíso", "valparaiso", "quilpué", "quilpue",
    "villa alemana", "quintero", "limache", "olmué", "olmue",
    "casablanca", "quillota", "la cruz", "puchuncaví", "puchuncavi",
    "calera", "nogales", "hijuelas", "algarrobo",
    "el quisco", "el tabo", "san antonio", "cartagena", "santo domingo",
}

# ─── Global store ───────────────────────────────────────────────────────────────
vehicles: dict[str, dict] = {}
qualifying_count = 0
graphql_count = 0


def _is_v_region(text: str) -> bool:
    """Check if location text contains a V Region commune."""
    if not text:
        return False
    loc = text.lower().strip()
    return any(commune in loc for commune in V_REGION_COMMUNES)


def _parse_price_clp(amount_str: str, formatted: str) -> int:
    """Extract numeric CLP value."""
    if amount_str and amount_str != "0":
        try:
            return int(amount_str)
        except (ValueError, TypeError):
            pass
    if not formatted:
        return 0
    digits = "".join(ch for ch in formatted if ch.isdigit())
    return int(digits) if digits else 0


def parse_feed_units(data: dict):
    global qualifying_count
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
            # V Region + price qualification
            price_clp = _parse_price_clp(price_info.get("amount", "0"), price_info.get("formatted_amount", ""))
            is_v = _is_v_region(location_text)
            qualifies = is_v and price_clp >= MIN_PRICE_CLP

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
                "v_region": is_v,
                "qualifies": qualifies,
                "source": "scraper",
                "scraped_at": datetime.now().isoformat(),
            }
            if qualifies:
                qualifying_count += 1
                tag = f"🟢 Q{qualifying_count:>3}/{TARGET_LEADS}"
            else:
                tag = "⚪ skip"
            print(
                f"  {tag} [{len(vehicles):>4}] {title[:45]:<45} | "
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
    print(f"🔐 Logging in as {FB_EMAIL}…", file=sys.stderr)
    try:
        # Go to /login directly — most reliable across all locales
        await page.goto("https://www.facebook.com/login", wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(4)

        print(f"   Page URL: {page.url}", file=sys.stderr)
        print(f"   Page title: {await page.title()}", file=sys.stderr)

        # Dismiss any cookie / consent banner FIRST (multi-language)
        for selector in [
            'button:has-text("Permitir todas las cookies")',
            'button:has-text("Permitir")',
            'button:has-text("Allow all cookies")',
            'button:has-text("Accept all")',
            'button:has-text("Alle Cookies erlauben")',
            'button:has-text("Alle akzeptieren")',
            'button:has-text("Decline optional cookies")',
            'button:has-text("Rechazar cookies opcionales")',
            'button:has-text("Optionale Cookies ablehnen")',
            '[data-testid="cookie-policy-manage-dialog-accept-button"]',
            'button[data-cookiebanner="accept_button"]',
            'button[data-cookiebanner="accept_only_essential_button"]',
        ]:
            try:
                btn = page.locator(selector)
                if await btn.count() > 0:
                    await btn.first.click()
                    print(f"   ✅ Dismissed banner: {selector}", file=sys.stderr)
                    await asyncio.sleep(2)
                    break
            except Exception:
                pass

        # If already logged in (no login form visible), skip
        url_now = page.url
        if not any(x in url_now for x in ("login", "checkpoint", "signup", "recover")):
            # Check if there's actually a login form on the page
            email_count = await page.locator('#email').count()
            if email_count == 0:
                print("✅ Already logged in — no login form found.", file=sys.stderr)
                return True

        # ── Fill EMAIL ───────────────────────────────────────────────────
        email_filled = False
        for sel in ['#email', 'input[name="email"]', 'input[type="email"]', 'input[id="email"]']:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click()
                    await asyncio.sleep(0.3)
                    await loc.first.fill("")          # clear first
                    await asyncio.sleep(0.2)
                    await page.keyboard.type(FB_EMAIL, delay=50)   # type char-by-char
                    email_filled = True
                    print(f"   ✅ Email typed via {sel}", file=sys.stderr)
                    break
            except Exception:
                pass

        if not email_filled:
            await page.screenshot(path="/tmp/fb_login_debug.png")
            print(f"❌ Could not find email field. URL: {page.url}", file=sys.stderr)
            print("   Screenshot: /tmp/fb_login_debug.png", file=sys.stderr)
            return False

        await asyncio.sleep(0.5)

        # ── Fill PASSWORD ────────────────────────────────────────────────
        pass_filled = False
        for sel in ['#pass', 'input[name="pass"]', 'input[type="password"]']:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click()
                    await asyncio.sleep(0.3)
                    await loc.first.fill("")          # clear first
                    await asyncio.sleep(0.2)
                    await page.keyboard.type(FB_PASSWORD, delay=50)  # type char-by-char
                    pass_filled = True
                    print(f"   ✅ Password typed via {sel}", file=sys.stderr)
                    break
            except Exception:
                pass

        if not pass_filled:
            print("❌ Could not find password field.", file=sys.stderr)
            return False

        await asyncio.sleep(0.5)

        # ── CLICK LOGIN BUTTON ───────────────────────────────────────────
        # Multi-language: Spanish, English, German
        login_clicked = False
        for sel in [
            'button[name="login"]',
            '#loginbutton',
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Iniciar sesión")',
            'button:has-text("Log In")',
            'button:has-text("Log in")',
            'button:has-text("Anmelden")',
            '[data-testid="royal_login_button"]',
        ]:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click()
                    login_clicked = True
                    print(f"   ✅ Clicked login via {sel}", file=sys.stderr)
                    break
            except Exception:
                pass

        if not login_clicked:
            # Last resort: press Enter in the password field
            print("   ⚠️ No login button found — pressing Enter…", file=sys.stderr)
            await page.keyboard.press("Enter")
            login_clicked = True

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
            if not any(x in url for x in ("login", "signup", "recover")):
                print("✅ Login successful!", file=sys.stderr)
                return True

        print("❌ Login timeout.", file=sys.stderr)
        return False
    except Exception as e:
        print(f"❌ Login error: {e}", file=sys.stderr)
        import traceback; traceback.print_exc(file=sys.stderr)
        return False


async def _handle_marketplace_login_wall(page, context, target_url: str):
    """
    Handle the Facebook login wall / modal that appears on marketplace pages.
    Facebook shows an overlay with email + password fields even if cookies are set.
    This fills them with credentials and submits — exactly like the Marketing version did.
    """
    await asyncio.sleep(2)

    # Look for ANY visible email field on the page (modal, overlay, or inline)
    email_selectors = [
        'input[name="email"]',
        '#email',
        'input[type="email"]',
        'input[placeholder*="E-Mail"]',
        'input[placeholder*="email"]',
        'input[placeholder*="correo"]',
        'input[placeholder*="Telefon"]',
        'input[placeholder*="phone"]',
    ]

    email_field = None
    for sel in email_selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                email_field = loc.first
                print(f"⚠️  Login wall detected — found email field via: {sel}", file=sys.stderr)
                break
        except Exception:
            pass

    if not email_field:
        print("✅ No login wall — marketplace loaded fine.", file=sys.stderr)
        return

    # ── TYPE email (character by character for reliability) ──────────────
    try:
        await email_field.click()
        await asyncio.sleep(0.3)
        # Clear any existing text first
        await email_field.fill("")
        await asyncio.sleep(0.2)
        # Type character by character like a human
        await page.keyboard.type(FB_EMAIL, delay=50)
        print(f"   ✅ Typed email: {FB_EMAIL}", file=sys.stderr)
    except Exception as e:
        print(f"   ❌ Failed to type email: {e}", file=sys.stderr)
        return

    await asyncio.sleep(0.5)

    # ── TYPE password ───────────────────────────────────────────────────
    pass_selectors = [
        'input[name="pass"]',
        '#pass',
        'input[type="password"]',
        'input[placeholder*="Passwort"]',
        'input[placeholder*="password"]',
        'input[placeholder*="contraseña"]',
    ]

    pass_field = None
    for sel in pass_selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                pass_field = loc.first
                break
        except Exception:
            pass

    if not pass_field:
        print("   ❌ No password field found on login wall.", file=sys.stderr)
        return

    try:
        await pass_field.click()
        await asyncio.sleep(0.3)
        await pass_field.fill("")
        await asyncio.sleep(0.2)
        await page.keyboard.type(FB_PASSWORD, delay=50)
        print(f"   ✅ Typed password", file=sys.stderr)
    except Exception as e:
        print(f"   ❌ Failed to type password: {e}", file=sys.stderr)
        return

    await asyncio.sleep(0.5)

    # ── CLICK submit button ─────────────────────────────────────────────
    login_btn_selectors = [
        'button[name="login"]',
        '#loginbutton',
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Anmelden")',
        'button:has-text("Iniciar sesión")',
        'button:has-text("Log In")',
        'button:has-text("Log in")',
        '[data-testid="royal_login_button"]',
        'button[data-testid="login_button"]',
    ]

    btn_clicked = False
    for sel in login_btn_selectors:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click()
                btn_clicked = True
                print(f"   ✅ Clicked login button: {sel}", file=sys.stderr)
                break
        except Exception:
            pass

    if not btn_clicked:
        # Fallback: press Enter
        print("   ⚠️ No button found — pressing Enter…", file=sys.stderr)
        await page.keyboard.press("Enter")

    # ── Wait for login to complete ──────────────────────────────────────
    print("   ⏳ Waiting for login wall to clear…", file=sys.stderr)
    for i in range(20):
        await asyncio.sleep(2)
        url = page.url
        print(f"     [{i*2}s] {url[:80]}", file=sys.stderr)

        if "checkpoint" in url:
            print("   ⚠️ Security checkpoint — waiting 90s for manual action…", file=sys.stderr)
            await asyncio.sleep(90)
            break

        # Check if the email field is gone (modal dismissed)
        still_visible = False
        for sel in email_selectors[:3]:
            try:
                if await page.locator(sel).count() > 0 and await page.locator(sel).first.is_visible():
                    still_visible = True
                    break
            except Exception:
                pass

        if not still_visible:
            print("   ✅ Login wall cleared!", file=sys.stderr)
            break

    # Save cookies after successful wall login
    try:
        cookies = await context.cookies()
        COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
        print(f"   💾 Saved {len(cookies)} cookies after login wall", file=sys.stderr)
    except Exception:
        pass

    # If we got redirected away from marketplace, navigate back
    await asyncio.sleep(2)
    if "marketplace" not in page.url:
        print(f"   🌐 Navigating back to marketplace…", file=sys.stderr)
        await page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
        await asyncio.sleep(5)


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

        # ── Handle login modal/popup that may appear on marketplace ──────
        # Facebook sometimes shows "See more on Facebook" overlay even after login
        url_now = page.url
        if any(x in url_now for x in ("login", "checkpoint")):
            print("⚠️  Redirected to login — attempting login…", file=sys.stderr)
            if not await do_login(page):
                await browser.close()
                return []
            # Save cookies after successful login
            try:
                cookies = await context.cookies()
                COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
                print(f"💾 Saved cookies after marketplace login redirect", file=sys.stderr)
            except Exception:
                pass
            # Navigate back to marketplace
            await page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
            await asyncio.sleep(5)

        # Check for login modal / overlay that Facebook shows on marketplace
        # This catches the "Mehr auf Facebook ansehen" overlay or any login form
        await _handle_marketplace_login_wall(page, context, target_url)

        print(f"\n🔄 Smart scrape: target {TARGET_LEADS} qualifying V-Region leads (max {scroll_steps} scrolls)…\n", file=sys.stderr)
        for i in range(1, scroll_steps + 1):
            await page.evaluate(f"window.scrollBy(0, {SCROLL_PX})")
            print(
                f"  Scroll {i:>4}/{scroll_steps} — {qualifying_count}/{TARGET_LEADS} qualifying | {len(vehicles)} total | {graphql_count} GraphQL",
                file=sys.stderr,
            )
            await asyncio.sleep(SCROLL_DELAY)

            # ── Smart stop: enough qualifying leads ──
            if qualifying_count >= TARGET_LEADS:
                print(f"\n🎯 Reached {qualifying_count} qualifying V-Region leads! Stopping early.", file=sys.stderr)
                break

        await asyncio.sleep(3)
        await browser.close()
        print(f"\n✅ Done! {len(vehicles)} total vehicles | {qualifying_count} qualifying V-Region leads.", file=sys.stderr)

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
