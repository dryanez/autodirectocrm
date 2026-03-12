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
    try:
        edges = data["data"]["marketplace_search"]["feed_units"]["edges"]
    except (KeyError, TypeError):
        return
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

async def handle_response(response):
    global graphql_count
    if "/api/graphql" not in response.url:
        return
    graphql_count += 1
    try:
        text = await response.text()
        data = json.loads(text)
        parse_feed_units(data)
    except Exception:
        pass

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
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )

        # ── Load saved FB cookies ──
        # Only pass the 7 fields Playwright accepts; skip cookies with non-ASCII
        # values (decryption artifacts that would cause Protocol errors).
        VALID_SAME_SITE = {"Strict", "Lax", "None"}
        pw_cookies = []
        skipped = 0
        for c in cookies:
            val = c.get("value", "")
            try:
                val.encode("ascii")          # Playwright requires ASCII-safe values
            except UnicodeEncodeError:
                skipped += 1
                continue
            pw_cookies.append({
                "name":     c["name"],
                "value":    val,
                "domain":   c.get("domain", ".facebook.com"),
                "path":     c.get("path", "/"),
                "httpOnly": bool(c.get("httpOnly", False)),
                "secure":   bool(c.get("secure", False)),
                "sameSite": c.get("sameSite") if c.get("sameSite") in VALID_SAME_SITE else "Lax",
            })
        if skipped:
            print(f"⚠️  Skipped {skipped} cookies with non-ASCII values (re-save session from Mac)", flush=True)
        if not pw_cookies:
            print("❌ Todas las cookies son inválidas. Guarda de nuevo tu sesión desde el botón '💾 Guardar Sesión FB' en tu Mac.", flush=True)
            await browser.close()
            sys.exit(1)
        await context.add_cookies(pw_cookies)
        print(f"🍪 Loaded {len(pw_cookies)} FB cookies", flush=True)

        page = await context.new_page()
        page.on("response", handle_response)

        print("🌐 Opening Facebook...", flush=True)
        await page.goto("https://www.facebook.com", wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(3)

        if "login" in page.url or "checkpoint" in page.url:
            print("❌ Sesión de Facebook inválida o expirada. Guarda de nuevo tu sesión desde el botón '💾 Guardar Sesión FB' en tu Mac.", flush=True)
            await browser.close()
            sys.exit(1)

        print("✅ Sesión de Facebook activa!", flush=True)
        print(f"🌐 Navegando al marketplace...", flush=True)
        await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60_000)
        await asyncio.sleep(5)

        print(f"🔄 Scraping: meta {TARGET_LEADS} leads V-Región (máx {MAX_SCROLLS} scrolls)...\n", flush=True)
        for i in range(1, MAX_SCROLLS + 1):
            await page.evaluate(f"window.scrollBy(0, {SCROLL_PX})")
            print(f"  Scroll {i:>4}/{MAX_SCROLLS} — {qualifying_count}/{TARGET_LEADS} qualifying | {len(vehicles)} total | {graphql_count} GraphQL", flush=True)
            await asyncio.sleep(SCROLL_DELAY)
            if qualifying_count >= TARGET_LEADS:
                print(f"\n🎯 ¡Meta alcanzada: {qualifying_count} leads V-Región!", flush=True)
                break

        await asyncio.sleep(3)

        # ── Save to Supabase via API ──
        print(f"\n💾 Guardando {len(vehicles)} vehículos en Supabase...", flush=True)
        import os as _os
        import requests as _req
        supa_url = _os.environ.get("SUPABASE_URL", "")
        supa_key = _os.environ.get("SUPABASE_SERVICE_KEY") or _os.environ.get("SUPABASE_KEY", "")
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
