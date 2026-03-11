from flask import Flask, render_template, jsonify, request, Response, stream_with_context
try:
    import pandas as pd
except ImportError:
    pd = None
import json
import os
import sys
import subprocess
import glob
from pathlib import Path
import requests

from datetime import datetime

# Ensure this directory is on sys.path so `utils` resolves when loaded
# via importlib from the root app.py (e.g. on Vercel where cwd=/var/task).
_THIS_DIR = str(Path(__file__).resolve().parent)
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
TMP_DIR = BASE_DIR / ".tmp"
FB_APP_DIR = BASE_DIR.parent / "fb app"
EXECUTION_DIR = BASE_DIR / "execution"
SCRAPE_LIVE_SCRIPT = EXECUTION_DIR / "scrape_fb_live.py"
LEADS_CSV = FB_APP_DIR / "facebook_graphql_vehicles.csv"
LEADS_JSON = TMP_DIR / "filtered_cars.json"
STATUS_FILE = TMP_DIR / "lead_status.json"

# ── Writable directory (Vercel /var/task is read-only -> use /tmp) ────────
def _resolve_data_write_dir():
    if str(BASE_DIR).startswith("/var/task"):
        fallback = Path("/tmp/funnels_data")
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback
    try:
        test_file = BASE_DIR / ".write_test"
        test_file.write_text("ok")
        test_file.unlink()
        return BASE_DIR
    except Exception:
        fallback = Path("/tmp/funnels_data")
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

DATA_WRITE_DIR = _resolve_data_write_dir()

# In-memory cache
_cached_listings = []


from utils import calculate_liquidity_score, get_region_data


def normalize_apify_item(item):
    """Convert a raw Apify Facebook Marketplace scraper record to dashboard format."""
    title = (
        item.get("listingTitle")
        or item.get("marketplace_listing_title")
        or item.get("customTitle")
        or item.get("custom_title")
        or "Unknown"
    )

    price_info = item.get("listingPrice") or item.get("listing_price") or {}
    try:
        price_num = int(float(price_info.get("amount", 0)))
        price = f"CLP {price_num:,}" if price_num else "N/A"
    except Exception:
        price = str(price_info.get("formatted_amount") or price_info.get("amount") or "N/A")

    location = ""
    loc_text = item.get("locationText") or {}
    if loc_text.get("text"):
        location = loc_text["text"]
    else:
        loc = item.get("location") or {}
        rev = loc.get("reverse_geocode") or {}
        city_page = rev.get("city_page") or {}
        location = (
            city_page.get("display_name")
            or f"{rev.get('city', '')}, {rev.get('state', '')}".strip(", ")
            or "Unknown"
        )

    year = None
    parts = title.split()
    if parts and parts[0].isdigit() and len(parts[0]) == 4:
        year = int(parts[0])

    mileage = ""
    subtitles = (
        item.get("customSubTitlesWithRenderingFlags")
        or item.get("custom_sub_titles_with_rendering_flags")
        or []
    )
    for s in subtitles:
        sub = s.get("subtitle", "")
        if "km" in sub.lower():
            mileage = sub
            break

    photo = item.get("primaryListingPhoto") or item.get("primary_listing_photo") or {}
    photo_url = photo.get("photo_image_url") or ""
    if not photo_url:
        photos = item.get("listingPhotos") or item.get("listing_photos") or []
        if photos:
            photo_url = (photos[0].get("image") or {}).get("uri", "")

    url = (
        item.get("itemUrl")
        or item.get("listingUrl")
        or item.get("url")
        or ""
    )

    seller = item.get("sellerName") or item.get("seller_name") or item.get("seller") or ""

    normalized = {
        "id": item.get("id", ""),
        "url": url,
        "title": title,
        "price": price,
        "location": location,
        "year": year,
        "mileage": mileage,
        "photo_url": photo_url,
        "is_sold": item.get("isSold") or item.get("is_sold", False),
        "status": "new",
        "seller": seller,
    }

    normalized["score"] = calculate_liquidity_score(normalized)
    region_data = get_region_data(location)
    normalized.update(region_data)

    return normalized


def find_latest_apify_json():
    """Find the largest Apify dataset JSON."""
    pattern = str(BASE_DIR / "dataset_facebook-marketplace-scraper_*.json")
    files = glob.glob(pattern)

    if DATA_WRITE_DIR != BASE_DIR:
        files += glob.glob(str(DATA_WRITE_DIR / "dataset_facebook-marketplace-scraper_*.json"))

    downloads = Path.home() / "Downloads"
    dl_pattern = str(downloads / "dataset_facebook-marketplace-scraper_*.json")
    files += glob.glob(dl_pattern)

    if not files:
        return None
    return max(files, key=os.path.getsize)


def find_root_har_file():
    """Look for www.facebook.com.har in the project root."""
    root = BASE_DIR.parent.parent
    har = root / "www.facebook.com.har"
    if har.exists():
        return har
    return None


def normalize_csv_row(row):
    """Map CSV column names from the Playwright scraper to dashboard field names."""
    url = row.get("url", "")
    title = row.get("title", "Unknown")
    price_raw = row.get("price", "N/A")
    location = row.get("city", "Unknown")

    year = None
    title_str = str(title) if title else ""
    parts = title_str.split()
    if parts and parts[0].isdigit() and len(parts[0]) == 4:
        year = int(parts[0])

    region_data = get_region_data(location)

    lead = {
        "id": url or row.get("id", ""),
        "url": url,
        "title": title,
        "price": str(price_raw),
        "location": str(location),
        "year": year,
        "mileage": str(row.get("km", "")),
        "photo_url": str(row.get("photo_url", "")),
        "is_sold": False,
        "status": "new",
        "first_seen": row.get("first_seen", ""),
        "last_scraped": row.get("last_scraped", ""),
        "seller": row.get("seller", "")
    }
    lead["score"] = calculate_liquidity_score(lead)
    lead.update(region_data)
    return lead


def load_all_listings():
    """Load and normalize listings from the best available source."""

    # 1. Raw Apify dataset JSON (highest priority)
    apify_file = find_latest_apify_json()
    if apify_file:
        try:
            raw = json.loads(Path(apify_file).read_text(encoding="utf-8"))
            valid = [item for item in raw if item.get("id") or item.get("listingTitle")]
            listings = [normalize_apify_item(item) for item in valid]
            print(f"[data] Loaded {len(listings)} listings from Apify JSON: {Path(apify_file).name}")
            print(f"[data]   (skipped {len(raw) - len(valid)} empty records)")
            return listings
        except Exception as e:
            print(f"[data] Error loading Apify JSON: {e}")

    # 2. HAR file from project root
    har_file = find_root_har_file()
    if har_file:
        try:
            sys.path.insert(0, str(BASE_DIR / "execution"))
            from parse_har import parse_har as _parse_har
            raw_listings = _parse_har(har_file)
            listings = [normalize_apify_item(item) for item in raw_listings]
            print(f"[data] Loaded {len(listings)} listings from HAR: {har_file.name}")
            return listings
        except Exception as e:
            print(f"[data] Error loading HAR: {e}")

    # 3. Filtered JSON
    if LEADS_JSON.exists():
        try:
            data = json.loads(LEADS_JSON.read_text())
            listings = data.get("listings", [])
            if listings:
                print(f"[data] Loaded {len(listings)} listings from filtered JSON")
                return listings
        except Exception as e:
            print(f"[data] Error loading filtered JSON: {e}")

    # 4. CSV fallback (requires pandas)
    if LEADS_CSV.exists() and pd is not None:
        try:
            df = pd.read_csv(LEADS_CSV)
            df = df.fillna("")
            rows = df.to_dict(orient="records")
            listings = [normalize_csv_row(r) for r in rows]
            print(f"[data] Loaded {len(listings)} listings from CSV")
            return listings
        except Exception as e:
            print(f"[data] Error loading CSV: {e}")

    print("[data] No data source found!")
    return []


def get_leads():
    """Return listings merged with current status map."""
    status_map = {}
    if STATUS_FILE.exists():
        try:
            status_map = json.loads(STATUS_FILE.read_text())
        except Exception:
            pass

    results = []
    for item in _cached_listings:
        url = item.get("url") or item.get("id")
        if not url:
            continue
        item_copy = dict(item)

        val = status_map.get(item.get("url", ""), "new")
        if isinstance(val, dict):
            item_copy["status"] = val.get("status", "new")
            item_copy["contacted_at"] = val.get("contacted_at")
            item_copy["valuation"] = val.get("valuation")
        else:
            item_copy["status"] = val
            item_copy["contacted_at"] = None
            item_copy["valuation"] = None

        results.append(item_copy)

    # Sort: V Region first -> closest to Vina -> highest score
    results.sort(key=lambda x: (
        not x.get("is_v_region", False),
        x.get("distance_to_vina", 9999),
        -x.get("score", 0)
    ))
    return results


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/leads", methods=["GET"])
def api_leads():
    return jsonify(get_leads())


@app.route("/api/reload", methods=["POST"])
def api_reload():
    """Reload listings from disk."""
    global _cached_listings
    _cached_listings = load_all_listings()
    return jsonify({"success": True, "count": len(_cached_listings)})


@app.route("/api/auto_message", methods=["POST", "GET"])
def trigger_auto_message():
    """Stream auto-messenger output via SSE."""
    limit = request.args.get("limit", 50, type=int)

    def generate():
        yield "data: " + json.dumps({"log": "Starting auto-messenger (limit: %d)..." % limit, "pct": 0}) + "\n\n"

        messenger_script = Path(__file__).resolve().parent.parent.parent / "auto_messenger.py"
        cmd = [sys.executable, str(messenger_script), "--limit", str(limit)]
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        proc = subprocess.Popen(
            cmd, cwd=str(messenger_script.parent),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )

        sent = 0
        total = limit

        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            if "Marked" in line or "Message sent" in line:
                sent += 1
            pct = min(95, int((sent / max(total, 1)) * 90))
            yield "data: " + json.dumps({"log": line, "pct": pct, "sent": sent}) + "\n\n"

        proc.wait()
        success = proc.returncode == 0
        done_msg = "Done! Sent %d message(s)." % sent if success else "Messenger exited with errors."
        yield "data: " + json.dumps({"log": done_msg, "pct": 100, "sent": sent, "done": True, "success": success}) + "\n\n"

        global _cached_listings
        _cached_listings = load_all_listings()

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/scrape", methods=["POST", "GET"])
def trigger_scrape():
    """Stream scraper output via SSE using scrape_fb_live.py (proper login + cookies)."""
    WHATSAPP_NUMBER = "4917632407062"
    CALLMEBOT_API_KEY = "4106204"

    def _wa_encode(text):
        out = str(text)
        out = out.replace(' ', '%20')
        out = out.replace(':', '%3A')
        out = out.replace('/', '%2F')
        out = out.replace('\n', '%0A')
        return out

    def generate():
        yield "data: " + json.dumps({"log": "Starting Facebook Marketplace scraper (with login)...", "pct": 0}) + "\n\n"

        # ── Build output path (match Apify naming so load_all_listings picks it up) ──
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_file = DATA_WRITE_DIR / ("dataset_facebook-marketplace-scraper_%s_live.json" % ts)

        cmd = [
            sys.executable,
            str(SCRAPE_LIVE_SCRIPT),
            "--output", str(out_file),
            "--scrolls", "2000",
        ]
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        print("[scrape] Running: %s" % " ".join(cmd))

        proc = subprocess.Popen(
            cmd, cwd=str(EXECUTION_DIR),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env, bufsize=1
        )

        # scrape_fb_live.py sends progress to stderr, JSON to stdout
        total_scrolls = 2000
        scroll_count = 0
        vehicle_count = 0

        for line in proc.stderr:
            line = line.rstrip()
            if not line:
                continue

            # Parse scroll progress: "  Scroll  42/2000 — 123 vehicles | 56 GraphQL hits"
            if "Scroll " in line and "/" in line:
                try:
                    part = line.split("Scroll")[1].strip()
                    nums = part.split("—")[0].strip() if "—" in part else part.split("-")[0].strip()
                    cur, tot = nums.split("/")
                    scroll_count = int(cur.strip())
                    total_scrolls = int(tot.strip())
                    if "vehicles" in line:
                        v_part = line.split("vehicles")[0]
                        v_part = v_part.split("—")[-1] if "—" in v_part else v_part.split("-")[-1]
                        vehicle_count = int(v_part.strip())
                except Exception:
                    pass

            # Parse vehicle captures: "  ✅ [  3] ..."
            if "✅" in line and "[" in line:
                try:
                    count_str = line.split("[")[1].split("]")[0].strip()
                    vehicle_count = max(vehicle_count, int(count_str))
                except Exception:
                    pass

            pct = min(95, int((scroll_count / max(total_scrolls, 1)) * 90))
            yield "data: " + json.dumps({"log": line, "pct": pct, "vehicles": vehicle_count}) + "\n\n"

        proc.wait()
        success = proc.returncode == 0

        # ── Reload leads from all sources (including the new JSON) ───────
        global _cached_listings
        _cached_listings = load_all_listings()

        done_msg = "Scrape complete! %d vehicles saved." % vehicle_count if success else "Scraper exited with errors."
        yield "data: " + json.dumps({"log": done_msg, "pct": 100, "vehicles": vehicle_count, "done": True, "success": success}) + "\n\n"

        try:
            msg = "Autodirecto Scraper\n%s (%s)" % (done_msg, datetime.now().strftime('%H:%M'))
            wa_url = (
                "https://api.callmebot.com/whatsapp.php"
                "?phone=%s"
                "&text=%s"
                "&apikey=%s"
            ) % (WHATSAPP_NUMBER, _wa_encode(msg), CALLMEBOT_API_KEY)
            resp = requests.get(wa_url, timeout=8)
            print("[whatsapp] Response: %s %s" % (resp.status_code, resp.text[:100]))
        except Exception as e:
            print("[whatsapp] Notification failed: %s" % e)

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/leads/status", methods=["POST"])
def api_update_status():
    data = request.json
    url = data.get("url")
    status = data.get("status")
    valuation = data.get("valuation")

    if not url:
        return jsonify({"error": "Missing url"}), 400

    status_map = {}
    if STATUS_FILE.exists():
        try:
            status_map = json.loads(STATUS_FILE.read_text())
        except Exception:
            pass

    entry = status_map.get(url, {})
    if not isinstance(entry, dict):
        entry = {"status": entry if entry else "new"}

    import time
    entry["updated_at"] = int(time.time())

    if status:
        entry["status"] = status
        if status == "contacted":
            entry["contacted_at"] = int(time.time())

    if valuation:
        entry["valuation"] = valuation

    status_map[url] = entry
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.write_text(json.dumps(status_map, indent=2))
    return jsonify({"success": True, "status": entry.get("status"), "valuation": entry.get("valuation")})


@app.route("/api/valuation", methods=["POST"])
def api_valuation():
    """Proxy to MrcarCotizacion API to get real market valuation."""
    data = request.json
    make = data.get("make")
    model = data.get("model")
    year = data.get("year")
    mileage = data.get("mileage")

    if not all([make, model, year]):
        return jsonify({"error": "Missing make, model, or year"}), 400

    if mileage:
        mileage = str(mileage).lower().replace("km", "").replace("miles", "").replace(",", "").strip()
        import re
        digits = re.findall(r'\d+', mileage)
        if digits:
            mileage = digits[0]
        else:
            mileage = "0"

    print("[valuation] Requesting for %s %s %s (%s km)" % (make, model, year, mileage))

    try:
        url = "https://mrcar-cotizacion.vercel.app/api/market-price"
        params = {
            "make": make,
            "model": model,
            "year": year,
            "mileage": mileage or "0"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp_data = resp.json()

        if not resp_data.get("success"):
            return jsonify({"error": "Valuation failed", "details": resp_data}), 400

        return jsonify(resp_data)

    except Exception as e:
        print("[valuation] Error: %s" % e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("[startup] Loading listings...")
    _cached_listings = load_all_listings()
    print("[startup] Ready -- %d listings cached" % len(_cached_listings))
    print("[startup] Dashboard at http://localhost:5001")
    app.run(debug=False, port=5001, use_reloader=False)
