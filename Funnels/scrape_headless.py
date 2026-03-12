"""
Facebook Marketplace — Headless Playwright Scraper (Railway / server mode)
Loads saved FB cookies instead of a Chrome profile.
Usage: python scrape_headless.py --cookies /path/to/cookies.json
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from playwright.async_api import async_playwright

# ─── Config ─────────────────────────────────────────────────────────────────────
TARGET_URL = (
    "https://www.facebook.com/marketplace/106647439372422/search/"
    "?minPrice=4000000&query=Vehicles&exact=false&radius=20"
)
MAX_SCROLLS  = 300
TARGET_LEADS = 200
MIN_PRICE    = 4_000_000
SCROLL_PX    = 1200
SCROLL_DELAY = 3.0

# ─── V Region communes ──────────────────────────────────────────────────────────
V_REGION_COMMUNES = {
    "viña del mar", "vina del mar", "concón", "concon",
    "valparaíso", "valparaiso", "quilpué", "quilpue",
    "villa alemana", "quintero", "limache", "olmué", "olmue",
    "casablanca", "quillota", "la cruz", "puchuncaví", "puchuncavi",
    "calera", "nogales", "hijuelas", "algarrobo",
    "el quisco", "el tabo", "san antonio", "cartagena", "santo domingo",
}

vehicles: dict[str, dict] = {}
qualifying_count = 0
graphql_count = 0

def _is_v_region(city: str) -> bool:
    if not city:
        return False
    loc = city.lower().strip()
    return any(c in loc for c in V_REGION_COMMUNES)

def _parse_price_clp(formatted: str) -> int:
    if not formatted:
        return 0
    digits = "".join(ch for ch in formatted if ch.isdigit())
    return int(digits) if digits else 0

def parse_feed_units(data: dict):
    global qualifying_count
    # FB often wraps multi-query responses; try both root and nested paths
    for blob in _extract_graphql_blobs(data):
        try:
            edges = blob["data"]["marketplace_search"]["feed_units"]["edges"]
        except (KeyError, TypeError):
            continue
        for edge in edges:
            try:
                listing = edge["node"]["listing"]
                lid = str(listing.get("id", ""))
                title = listing.get("marketplace_listing_title") or listing.get("custom_title", "")
                price_fmt = (listing.get("listing_price") or {}).get("formatted_amount", "")
                price_raw = int((listing.get("listing_price") or {}).get("amount", "0") or "0")
                city = ((listing.get("location") or {}).get("reverse_geocode", {}).get("city", ""))
                km_list = listing.get("custom_sub_titles_with_rendering_flags") or []
                km = km_list[0].get("subtitle", "") if km_list else ""
                seller = (listing.get("marketplace_listing_seller") or {}).get("name", "")
                listing_url = f"https://www.facebook.com/marketplace/item/{lid}/"

                if lid and title and lid not in vehicles:
                    price_num = price_raw if price_raw > 0 else _parse_price_clp(price_fmt)
                    is_v = _is_v_region(city)
                    qualifies = is_v and price_num >= MIN_PRICE
                    vehicles[lid] = {
                        "id": lid, "title": title, "price": price_fmt,
                        "price_clp": price_num, "city": city, "km": km,
                        "seller": seller, "url": listing_url,
                        "v_region": is_v, "qualifies": qualifies,
                    }
                    if qualifies:
                        qualifying_count += 1
                        tag = f"🟢 Q{qualifying_count:>3}/{TARGET_LEADS}"
                    else:
                        tag = "⚪ skip"
                    print(f"  {tag} [{len(vehicles):>4}] {title[:45]:<45} | {price_fmt:<18} | {city}", flush=True)
            except Exception:
                continue


def _extract_graphql_blobs(data):
    """Yield all possible GraphQL result blobs from FB's response format.
    FB responses can be: a single JSON object, a JSON object with nested 'data',
    or multiple newline-delimited JSON objects concatenated."""
    if isinstance(data, dict):
        yield data
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item


async def handle_response(response):
    global graphql_count
    url = response.url
    if "/api/graphql" not in url:
        return
    graphql_count += 1
    try:
        text = await response.text()
        # FB sometimes returns multiple JSON objects separated by newlines
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                parse_feed_units(data)
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f"  ⚠️ GraphQL parse error: {e}", flush=True)


async def main(cookies_path: str):
    cookies = json.loads(Path(cookies_path).read_text())

    print("🚀 Launching headless Chromium...", flush=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="es-CL",
            timezone_id="America/Santiago",
        )

        # ── Load saved FB cookies ──
        # Only load c_user and xs (the auth cookies). Skip placeholder/fake cookies.
        # Let Facebook set datr and other tracking cookies naturally.
        VALID_SAME_SITE = {"Strict", "Lax", "None"}
        AUTH_COOKIES = {"c_user", "xs"}   # only these are required for auth
        pw_cookies = []
        skipped = 0
        for c in cookies:
            name = c.get("name", "")
            val = c.get("value", "")

            # Skip placeholder values
            if val in ("placeholder", ""):
                print(f"  ⏭️  Skipping cookie '{name}' (placeholder/empty)", flush=True)
                continue

            try:
                val.encode("ascii")          # Playwright requires ASCII-safe values
            except UnicodeEncodeError:
                skipped += 1
                continue

            pw_cookies.append({
                "name":     name,
                "value":    val,
                "domain":   c.get("domain", ".facebook.com"),
                "path":     c.get("path", "/"),
                "httpOnly": bool(c.get("httpOnly", False)),
                "secure":   bool(c.get("secure", False)),
                "sameSite": c.get("sameSite") if c.get("sameSite") in VALID_SAME_SITE else "Lax",
            })
        if skipped:
            print(f"⚠️  Skipped {skipped} cookies with non-ASCII values", flush=True)

        # Check we have the essential auth cookies
        loaded_names = {c["name"] for c in pw_cookies}
        missing_auth = AUTH_COOKIES - loaded_names
        if missing_auth:
            print(f"❌ Faltan cookies de autenticación: {missing_auth}. Pega c_user y xs desde Chrome DevTools.", flush=True)
            await browser.close()
            sys.exit(1)

        await context.add_cookies(pw_cookies)
        print(f"🍪 Loaded {len(pw_cookies)} FB cookies: {[c['name'] for c in pw_cookies]}", flush=True)

        page = await context.new_page()
        page.on("response", handle_response)

        # ── Step 1: Navigate to facebook.com to validate session ──
        print("🌐 Opening Facebook...", flush=True)
        await page.goto("https://www.facebook.com", wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(4)

        current_url = page.url
        page_title = await page.title()
        print(f"📍 URL after load: {current_url}", flush=True)
        print(f"📄 Page title: {page_title}", flush=True)

        # Check for login/checkpoint redirects
        if "login" in current_url or "checkpoint" in current_url:
            print("❌ Sesión de Facebook inválida o expirada.", flush=True)
            print(f"   URL: {current_url}", flush=True)
            await browser.close()
            sys.exit(1)

        # Try to dismiss cookie consent banner if present
        try:
            consent_btn = page.locator('[data-cookiebanner="accept_button"], [data-testid="cookie-policy-manage-dialog-accept-button"]')
            if await consent_btn.count() > 0:
                print("🍪 Dismissing cookie consent banner...", flush=True)
                await consent_btn.first.click()
                await asyncio.sleep(2)
        except Exception:
            pass

        print("✅ Sesión de Facebook activa!", flush=True)

        # ── Step 2: Navigate to Marketplace ──
        print(f"🌐 Navegando al marketplace...", flush=True)
        await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60_000)
        await asyncio.sleep(5)

        mktpl_url = page.url
        mktpl_title = await page.title()
        print(f"📍 Marketplace URL: {mktpl_url}", flush=True)
        print(f"📄 Marketplace title: {mktpl_title}", flush=True)

        # Check if we got redirected away from marketplace
        if "login" in mktpl_url or "checkpoint" in mktpl_url:
            print("❌ Redirigido fuera del Marketplace (sesión inválida).", flush=True)
            await browser.close()
            sys.exit(1)

        # Log if marketplace didn't load properly
        if "marketplace" not in mktpl_url.lower():
            print(f"⚠️ URL no parece ser Marketplace: {mktpl_url}", flush=True)

        # Check page content for common issues
        try:
            body_text = await page.inner_text("body", timeout=5000)
            snippet = body_text[:500].replace("\n", " ").strip()
            print(f"📝 Page content preview: {snippet[:200]}...", flush=True)
            if "you must log in" in body_text.lower() or "inicia sesión" in body_text.lower():
                print("❌ Facebook pide iniciar sesión — cookies inválidas.", flush=True)
                await browser.close()
                sys.exit(1)
        except Exception:
            pass

        # ── Step 3: Try clicking "See all" or dismissing popups ──
        try:
            # Dismiss any modal/overlay that might block scrolling
            close_btns = page.locator('[aria-label="Close"], [aria-label="Cerrar"]')
            if await close_btns.count() > 0:
                print("🔲 Cerrando modal/popup...", flush=True)
                await close_btns.first.click()
                await asyncio.sleep(1)
        except Exception:
            pass

        print(f"\n🔄 Scraping: meta {TARGET_LEADS} leads V-Región (máx {MAX_SCROLLS} scrolls)...\n", flush=True)
        print(f"   (GraphQL responses interceptados hasta ahora: {graphql_count})\n", flush=True)

        for i in range(1, MAX_SCROLLS + 1):
            await page.evaluate(f"window.scrollBy(0, {SCROLL_PX})")
            print(f"  Scroll {i:>4}/{MAX_SCROLLS} — {qualifying_count}/{TARGET_LEADS} qualifying | {len(vehicles)} total | {graphql_count} GraphQL", flush=True)
            await asyncio.sleep(SCROLL_DELAY)
            if qualifying_count >= TARGET_LEADS:
                print(f"\n🎯 ¡Meta alcanzada: {qualifying_count} leads V-Región!", flush=True)
                break

            # Early warning if after 10 scrolls still no GraphQL
            if i == 10 and graphql_count == 0:
                print("\n⚠️ 10 scrolls y 0 GraphQL interceptados. Facebook puede estar bloqueando.", flush=True)
                curr = page.url
                print(f"📍 Current URL: {curr}", flush=True)
                try:
                    txt = await page.inner_text("body", timeout=3000)
                    print(f"📝 Body: {txt[:300]}", flush=True)
                except Exception:
                    pass

        await asyncio.sleep(3)

        # ── Save to Supabase via API ──
        print(f"\n💾 Guardando {len(vehicles)} vehículos en Supabase...", flush=True)
        import os as _os
        import requests as _req
        supa_url = _os.environ.get("SUPABASE_URL", "").strip()
        supa_key = (_os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                    or _os.environ.get("SUPABASE_SERVICE_KEY")
                    or _os.environ.get("SUPABASE_KEY", "")).strip()
        if supa_url and supa_key:
            headers = {
                "apikey": supa_key, "Authorization": f"Bearer {supa_key}",
                "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates",
            }
            saved = 0
            for v in vehicles.values():
                row = {
                    "listing_id": v["id"], "title": v["title"],
                    "price": v["price"], "price_clp": v["price_clp"],
                    "location": v["city"], "mileage": v["km"],
                    "seller": v["seller"], "url": v["url"],
                    "is_v_region": v["v_region"], "status": "new",
                }
                r = _req.post(f"{supa_url}/rest/v1/funnel_listings",
                              json=row, headers=headers, timeout=10)
                if r.status_code in (200, 201):
                    saved += 1
            print(f"✅ Guardados {saved}/{len(vehicles)} en Supabase", flush=True)
        else:
            print("⚠️ SUPABASE_URL/KEY no configurados — guardando CSV local.", flush=True)
            import csv
            out = Path(__file__).parent / "facebook_graphql_vehicles.csv"
            with open(out, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["id","title","price","price_clp","city","km","seller","url","v_region","qualifies"])
                writer.writeheader()
                writer.writerows(vehicles.values())
            print(f"📄 CSV guardado: {out}", flush=True)

        print(f"\n✅ Scrape completo!", flush=True)
        print(f"   Total vehículos: {len(vehicles)}", flush=True)
        print(f"   Leads V-Región:  {qualifying_count}", flush=True)
        print(f"   GraphQL calls:   {graphql_count}", flush=True)

        await browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cookies", required=True, help="Path to FB cookies JSON file")
    args = parser.parse_args()
    asyncio.run(main(args.cookies))
