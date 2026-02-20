from flask import Flask, render_template, jsonify, request
import csv
import json
import os
import sys
import subprocess
import glob
from pathlib import Path
import requests

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
TMP_DIR = BASE_DIR / ".tmp"
LEADS_CSV = TMP_DIR / "leads.csv"
LEADS_JSON = TMP_DIR / "filtered_cars.json"
STATUS_FILE = TMP_DIR / "lead_status.json"

# In-memory cache — loaded once at startup
_cached_listings = []


def normalize_apify_item(item):
    """Convert a raw Apify Facebook Marketplace scraper record to dashboard format.
    Handles both the new camelCase format and the old snake_case format."""

    # ── Title ──────────────────────────────────────────────────────────────
    title = (
        item.get("listingTitle")
        or item.get("marketplace_listing_title")
        or item.get("customTitle")
        or item.get("custom_title")
        or "Unknown"
    )

    # ── Price ──────────────────────────────────────────────────────────────
    # New format: listingPrice.amount  (e.g. "11500000")
    # Old format: listing_price.amount
    price_info = item.get("listingPrice") or item.get("listing_price") or {}
    try:
        price_num = int(float(price_info.get("amount", 0)))
        price = f"CLP {price_num:,}" if price_num else "N/A"
    except Exception:
        price = str(price_info.get("formatted_amount") or price_info.get("amount") or "N/A")

    # ── Location ───────────────────────────────────────────────────────────
    # New format: locationText.text  (e.g. "Viña del Mar, VS")
    # Old format: location.reverse_geocode.city_page.display_name
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

    # ── Year (parse from title) ────────────────────────────────────────────
    year = None
    parts = title.split()
    if parts and parts[0].isdigit() and len(parts[0]) == 4:
        year = int(parts[0])

    # ── Mileage from subtitles ─────────────────────────────────────────────
    # New format: customSubTitlesWithRenderingFlags
    # Old format: custom_sub_titles_with_rendering_flags
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

    # ── Photo ──────────────────────────────────────────────────────────────
    # New format: primaryListingPhoto.photo_image_url
    # Old format: primary_listing_photo.photo_image_url
    photo = item.get("primaryListingPhoto") or item.get("primary_listing_photo") or {}
    photo_url = photo.get("photo_image_url") or ""

    # Fallback: first photo in listingPhotos array
    if not photo_url:
        photos = item.get("listingPhotos") or item.get("listing_photos") or []
        if photos:
            photo_url = (photos[0].get("image") or {}).get("uri", "")

    # ── URL ────────────────────────────────────────────────────────────────
    url = (
        item.get("itemUrl")
        or item.get("listingUrl")
        or item.get("url")
        or ""
    )

    return {
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
    }


def find_latest_apify_json():
    """Find the largest Apify dataset JSON in BASE_DIR or Downloads.
    We pick by size (largest = most complete dataset) rather than modification time."""
    # Search in the Funnels folder first
    pattern = str(BASE_DIR / "dataset_facebook-marketplace-scraper_*.json")
    files = glob.glob(pattern)

    # Also check Downloads as a fallback
    downloads = Path.home() / "Downloads"
    dl_pattern = str(downloads / "dataset_facebook-marketplace-scraper_*.json")
    files += glob.glob(dl_pattern)

    if not files:
        return None
    # Pick the largest file — it contains the most listings
    return max(files, key=os.path.getsize)


def load_all_listings():
    """Load and normalize listings from the best available source. Called once at startup."""

    # 1. Raw Apify dataset JSON — full dataset (highest priority for viewing)
    apify_file = find_latest_apify_json()
    if apify_file:
        try:
            raw = json.loads(Path(apify_file).read_text(encoding="utf-8"))
            # Filter out empty/partial records (only have facebookUrl, no actual listing)
            valid = [item for item in raw if item.get("id") or item.get("listingTitle")]
            listings = [normalize_apify_item(item) for item in valid]
            print(f"[data] Loaded {len(listings)} listings from Apify JSON: {Path(apify_file).name}")
            print(f"[data]   (skipped {len(raw) - len(valid)} empty records)")
            return listings
        except Exception as e:
            print(f"[data] Error loading Apify JSON: {e}")

    # 2. Filtered JSON
    if LEADS_JSON.exists():
        try:
            data = json.loads(LEADS_JSON.read_text())
            listings = data.get("listings", [])
            if listings:
                print(f"[data] Loaded {len(listings)} listings from filtered JSON")
                return listings
        except Exception as e:
            print(f"[data] Error loading filtered JSON: {e}")

    # 3. CSV fallback
    if LEADS_CSV.exists():
        try:
            with open(LEADS_CSV, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = [{k: (v if v != '' else None) for k, v in row.items()} for row in reader]
            listings = [normalize_csv_row(r) for r in rows]
            print(f"[data] Loaded {len(listings)} listings from CSV")
            return listings
        except Exception as e:
            print(f"[data] Error loading CSV: {e}")

    print("[data] No data source found!")
    return []


def normalize_csv_row(row):
    """Map CSV column names (from filter_listings.py output) to dashboard field names."""
    url = row.get("Listing URL") or row.get("url") or row.get("listing_url") or ""
    title = row.get("Title") or row.get("title") or "Unknown"
    price_raw = row.get("Price") or row.get("price") or "N/A"
    location = row.get("Location") or row.get("location") or row.get("Region") or "Unknown"
    year_raw = row.get("Year") or row.get("year")
    try:
        year = int(float(year_raw)) if year_raw else None
    except Exception:
        year = None
    return {
        "id": url,
        "url": url,
        "title": title,
        "price": str(price_raw),
        "location": str(location),
        "year": year,
        "mileage": str(row.get("Days Active") or row.get("Date Text (Raw)") or ""),
        "photo_url": "",
        "is_sold": str(row.get("Sold?", "")).lower() == "yes",
        "status": "new",
    }


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
        
        # Handle both legacy string status and new dict status
        val = status_map.get(item.get("url", ""), "new")
        if isinstance(val, dict):
            item_copy["status"] = val.get("status", "new")
            item_copy["contacted_at"] = val.get("contacted_at")
            item_copy["valuation"] = val.get("valuation")
        else:
            item_copy["status"] = val # assume string
            item_copy["contacted_at"] = None
            item_copy["valuation"] = None
            
        results.append(item_copy)
    return results


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/leads", methods=["GET"])
def api_leads():
    return jsonify(get_leads())


@app.route("/api/reload", methods=["POST"])
def api_reload():
    """Reload listings from disk (e.g. after a new scrape)."""
    global _cached_listings
    _cached_listings = load_all_listings()
    return jsonify({"success": True, "count": len(_cached_listings)})


@app.route("/api/scrape", methods=["POST"])
def trigger_scrape():
    region = request.json.get("region", "santiago")
    try:
        use_safe_mode = request.json.get("safe_mode", True)
        cmd = [sys.executable, str(BASE_DIR / "execution/run_pipeline.py"), "--region", region]
        if use_safe_mode:
            cmd.append("--safe")
        subprocess.Popen(cmd, cwd=BASE_DIR)
        mode_msg = "Safe Mode" if use_safe_mode else "Login Mode"
        return jsonify({"success": True, "message": f"Scraper started in {mode_msg}!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

    # Get existing entry or create new
    entry = status_map.get(url, {})
    if not isinstance(entry, dict):
        entry = {"status": entry if entry else "new"}
    
    import time
    entry["updated_at"] = int(time.time())

    # Update status if provided
    if status:
        entry["status"] = status
        if status == "contacted":
            entry["contacted_at"] = int(time.time())
    
    # Update valuation if provided
    if valuation:
        entry["valuation"] = valuation

    status_map[url] = entry
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

    # Clean mileage (remove 'km', 'miles', etc)
    if mileage:
        mileage = str(mileage).lower().replace("km", "").replace("miles", "").replace(",", "").strip()
        # extract digits only if mixed
        import re
        digits = re.findall(r'\d+', mileage)
        if digits:
            mileage = digits[0]
        else:
            mileage = "0"

    print(f"[valuation] Requesting for {make} {model} {year} ({mileage} km)")

    try:
        # Call external API
        url = "https://mrcar-cotizacion.vercel.app/api/market-price"
        params = {
            "make": make,
            "model": model,
            "year": year,
            "mileage": mileage or "0"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp_data = resp.json()

        if not resp_data.get("success"):
            return jsonify({"error": "Valuation failed", "details": resp_data}), 400

        return jsonify(resp_data)

    except Exception as e:
        print(f"[valuation] Error: {e}")
        return jsonify({"error": str(e)}), 500



# ── Bridge: Autodirecto Match ─────────────────────────────────────────────────
# Connects FB Marketplace leads with Autodirecto appointments
AUTODIRECTO_BRIDGE_URL = os.environ.get(
    "AUTODIRECTO_BRIDGE_URL",
    "https://autodirecto.cl/api/bridge/match"
)


@app.route("/api/bridge/match", methods=["POST"])
def api_bridge_match():
    """
    El Match Mágico — sends lead data to Autodirecto to find 
    a matching appointment by car_make + car_model + car_year + name + mileage.
    Called from the dashboard when a lead is contacted or manually.
    """
    data = request.json
    title = data.get("title", "")
    
    # Parse brand/model/year from the FB listing title (e.g. "2020 Toyota Corolla")
    parts = title.strip().split()
    year = None
    brand = ""
    model = ""
    
    if parts and parts[0].isdigit() and len(parts[0]) == 4:
        year = int(parts[0])
        brand = parts[1] if len(parts) > 1 else ""
        model = " ".join(parts[2:]) if len(parts) > 2 else ""
    elif parts:
        brand = parts[0]
        model = " ".join(parts[1:]) if len(parts) > 1 else ""
    
    # Parse mileage (remove "km", commas, etc)
    import re
    mileage_raw = data.get("mileage", "")
    mileage_digits = re.findall(r"\d+", str(mileage_raw).replace(",", "").replace(".", ""))
    mileage = int(mileage_digits[0]) if mileage_digits else None
    
    # Build the match payload
    payload = {
        "name": data.get("seller_name", ""),
        "car_make": brand,
        "car_model": model,
        "car_year": year,
        "mileage": mileage,
        "phone": data.get("phone", ""),
        "funnel_lead_id": data.get("url", data.get("id", ""))
    }
    
    print(f"[bridge] Matching: {brand} {model} {year} | mileage={mileage}")
    
    try:
        resp = requests.post(
            AUTODIRECTO_BRIDGE_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15
        )
        result = resp.json()
        matched = result.get("matched")
        confidence = result.get("confidence")
        score = result.get("score")
        print(f"[bridge] Result: matched={matched}, confidence={confidence}, score={score}")
        return jsonify(result)
    except Exception as e:
        print(f"[bridge] Error: {e}")
        return jsonify({"success": False, "error": str(e), "matched": False}), 500


if __name__ == "__main__":
    print(f"[startup] Loading listings...")
    _cached_listings = load_all_listings()
    print(f"[startup] Ready — {len(_cached_listings)} listings cached")
    print(f"[startup] Dashboard at http://localhost:5001")
    # use_reloader=False prevents double-startup in background mode
    app.run(debug=False, port=5001, use_reloader=False)
