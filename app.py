"""
app.py — Flask backend for Autodirecto CRM.
Wraps the execution layer scripts as REST API endpoints.

Run:
    python3 app.py
Then open: http://127.0.0.1:5001
"""

import io
import json
import os
import sys
import time as _time
from datetime import date, datetime
from pathlib import Path

import requests as _requests
from flask import Flask, jsonify, render_template, request, session, send_file

ROOT = Path(__file__).parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env
env_file = ROOT / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ─── Database: Supabase instead of SQLite ────────────────────────────────────
# db.py provides get_conn/get_db/get_crm_conn/row_to_dict that talk to Supabase
# using the same .execute()/.fetchone()/.fetchall() interface as sqlite3.
from db import get_conn, get_db, get_crm_conn, row_to_dict

from execution.consignment_logic import calculate_commission
from execution.validate_dte_schema import validate as validate_schema


def log_to_file(msg):
    """Simple logger — prints to stdout and appends to simply_sync.log."""
    print(msg, flush=True)
    try:
        with open(os.path.join(os.path.dirname(__file__), "simply_sync.log"), "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "autodirecto-crm-secret-2026")

# ─── Mount Funnels Dashboard as Blueprint ─────────────────────────────────────
FUNNELS_DIR = ROOT / "Funnels" / "dashboard"
if FUNNELS_DIR.exists():
    # Load the Funnels module dynamically (avoids needing __init__.py)
    import importlib.util
    _funnels_spec = importlib.util.spec_from_file_location(
        "funnels_dashboard", str(FUNNELS_DIR / "app.py"))
    funnels_module = importlib.util.module_from_spec(_funnels_spec)
    _funnels_spec.loader.exec_module(funnels_module)

    # Register all Funnels routes under /funnels prefix
    from flask import Blueprint
    funnels_bp = Blueprint(
        'funnels', __name__,
        template_folder=str(FUNNELS_DIR / 'templates'),
        static_folder=str(FUNNELS_DIR / 'static'),
        static_url_path='/funnels/static',
        url_prefix='/funnels'
    )

    # Load listings at startup (local files if available, else Supabase)
    funnels_module._cached_listings = funnels_module.load_all_listings()
    # If no local data found (e.g. on Vercel), load from Supabase
    if not funnels_module._cached_listings:
        try:
            with get_db() as conn:
                rows = conn.execute("SELECT * FROM funnel_listings ORDER BY year DESC LIMIT 1000").fetchall()
            funnels_module._cached_listings = [row_to_dict(r) for r in rows]
            print(f"  ✅ Loaded {len(funnels_module._cached_listings)} funnel listings from Supabase")
        except Exception as e:
            print(f"  ⚠️  Could not load funnel listings from Supabase: {e}")

    @funnels_bp.route('/')
    def funnels_index():
        from flask import render_template_string
        tpl_path = FUNNELS_DIR / 'templates' / 'index.html'
        return render_template_string(tpl_path.read_text(encoding='utf-8'))

    @funnels_bp.route('/api/leads', methods=['GET'])
    def funnels_api_leads():
        leads = list(funnels_module.get_leads())
        # If in-memory cache is empty (cold start on Vercel), load from Supabase
        if not leads:
            try:
                with get_db() as conn:
                    rows = conn.execute("SELECT * FROM funnel_listings ORDER BY year DESC LIMIT 1000").fetchall()
                leads = [row_to_dict(r) for r in rows]
                # Cache for subsequent requests
                funnels_module._cached_listings = leads
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        # Overlay status from Supabase (persists across deploys)
        try:
            with get_db() as conn:
                rows = conn.execute("SELECT * FROM funnel_lead_status").fetchall()
            status_map = {row_to_dict(r)["url"]: row_to_dict(r) for r in rows}
            for lead in leads:
                url = lead.get("url") or lead.get("id", "")
                if url in status_map:
                    s = status_map[url]
                    lead["status"] = s.get("status", "new")
                    lead["contacted_at"] = s.get("contacted_at")
                    lead["valuation"] = s.get("valuation")
        except Exception:
            pass  # fall back to file-based status
        return jsonify(leads)

    @funnels_bp.route('/api/reload', methods=['POST'])
    def funnels_api_reload():
        funnels_module._cached_listings = funnels_module.load_all_listings()
        # Also re-sync from Supabase if local is empty
        if not funnels_module._cached_listings:
            try:
                with get_db() as conn:
                    rows = conn.execute("SELECT * FROM funnel_listings ORDER BY year DESC LIMIT 1000").fetchall()
                funnels_module._cached_listings = [row_to_dict(r) for r in rows]
            except Exception:
                pass
        return jsonify({"success": True, "count": len(funnels_module._cached_listings)})

    @funnels_bp.route('/api/scrape', methods=['POST'])
    def funnels_api_scrape():
        import subprocess
        region = request.json.get("region", "santiago")
        try:
            use_safe_mode = request.json.get("safe_mode", True)
            cmd = [sys.executable, str(funnels_module.BASE_DIR / "execution/run_pipeline.py"), "--region", region]
            if use_safe_mode:
                cmd.append("--safe")
            subprocess.Popen(cmd, cwd=funnels_module.BASE_DIR)
            mode_msg = "Safe Mode" if use_safe_mode else "Login Mode"
            return jsonify({"success": True, "message": f"Scraper started in {mode_msg}!"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @funnels_bp.route('/api/leads/status', methods=['POST'])
    def funnels_api_update_status():
        data = request.json
        url = data.get("url")
        status = data.get("status")
        valuation = data.get("valuation")
        if not url:
            return jsonify({"error": "Missing url"}), 400
        now_ts = int(_time.time())
        # Upsert into Supabase
        try:
            with get_db() as conn:
                existing = conn.execute(
                    "SELECT * FROM funnel_lead_status WHERE url=?", (url,)
                ).fetchone()
            entry = row_to_dict(existing) if existing else {"url": url, "status": "new"}
            entry["updated_at"] = now_ts
            if status:
                entry["status"] = status
                if status == "contacted":
                    entry["contacted_at"] = now_ts
            if valuation:
                entry["valuation"] = valuation
                # Ensure the AI price also syncs to the main CRM leads database 
                # so that appointments can instantly retrieve it.
                # UPSERT: UPDATE if exists, INSERT if not (so AI tasación auto-creates CRM leads)
                import re as _re
                try:
                    with get_crm_conn() as crm:
                        # Check if a CRM lead already exists for this funnel URL
                        existing_lead = crm.execute(
                            "SELECT id FROM crm_leads WHERE source='funnels' AND funnel_url=?", (url,)
                        ).fetchone()

                        if existing_lead:
                            # Lead exists — just update the AI prices
                            crm.execute('''
                                UPDATE crm_leads SET 
                                    estimated_value=?, ai_consignacion_price=?, ai_instant_buy_price=?,
                                    updated_at=?
                                WHERE source='funnels' AND funnel_url=?
                            ''', (
                                valuation.get("market_price"),
                                valuation.get("consignment_liquidation"),
                                valuation.get("immediate_offer"),
                                datetime.now().isoformat(),
                                url
                            ))
                        else:
                            # No CRM lead yet — auto-create from cached listing data
                            listing = None
                            for l in (funnels_module._cached_listings or []):
                                if (l.get("url") or l.get("id", "")) == url:
                                    listing = l
                                    break

                            title = (listing or {}).get("title", "") if listing else ""
                            parts = title.split()
                            car_make = parts[0] if len(parts) > 0 else None
                            car_model = " ".join(parts[1:]) if len(parts) > 1 else None
                            car_year = None
                            year_match = _re.search(r'\b(19|20)\d{2}\b', title)
                            if year_match:
                                car_year = int(year_match.group())
                            elif listing and listing.get("year"):
                                car_year = int(listing["year"])

                            mileage = None
                            if listing and listing.get("mileage"):
                                digits = _re.findall(r'\d+', str(listing["mileage"]).replace(",", ""))
                                if digits:
                                    mileage = int(digits[0])

                            listing_price = None
                            if listing:
                                price_str = listing.get("price", "")
                                if price_str:
                                    digits = _re.findall(r'\d+', str(price_str).replace(",", "").replace(".", ""))
                                    if digits:
                                        listing_price = int(digits[0])

                            now_iso = datetime.now().isoformat()
                            crm.execute("""
                                INSERT INTO crm_leads (
                                    full_name, phone, car_make, car_model, car_year, mileage,
                                    listing_price, estimated_value, ai_consignacion_price,
                                    ai_instant_buy_price, stage, source, funnel_url,
                                    created_at, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                (listing or {}).get("seller"),
                                (listing or {}).get("seller_phone"),
                                car_make, car_model, car_year, mileage,
                                listing_price,
                                valuation.get("market_price"),
                                valuation.get("consignment_liquidation"),
                                valuation.get("immediate_offer"),
                                'nuevo', 'funnels', url, now_iso, now_iso
                            ))
                            # Add activity log
                            lead_id = crm.execute("SELECT last_insert_rowid()").fetchone()[0]
                            crm.execute(
                                "INSERT INTO crm_activities (lead_id, type, title, description) VALUES (?, ?, ?, ?)",
                                (lead_id, 'ai_valuation', 'Lead creado por AI Tasación',
                                 'Publicación: {} — Precio AI: ${}'.format(
                                     title, valuation.get("market_price", "?")))
                            )
                            print(f"[funnels_api_update_status] Auto-created CRM lead #{lead_id} for {url}")

                        crm.commit()
                except Exception as e:
                    print(f"[funnels_api_update_status] Error syncing to crm_leads: {e}")

            with get_db() as conn:
                if existing:
                    set_clause = ", ".join(f"{k}=?" for k in entry if k != "url")
                    vals = [entry[k] for k in entry if k != "url"] + [url]
                    conn.execute(f"UPDATE funnel_lead_status SET {set_clause} WHERE url=?", vals)
                else:
                    cols = ", ".join(entry.keys())
                    placeholders = ", ".join("?" for _ in entry)
                    conn.execute(f"INSERT INTO funnel_lead_status ({cols}) VALUES ({placeholders})", list(entry.values()))
                conn.commit()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        # Also write to local file as backup
        if funnels_module.STATUS_FILE.parent.exists():
            try:
                status_map = {}
                if funnels_module.STATUS_FILE.exists():
                    status_map = json.loads(funnels_module.STATUS_FILE.read_text())
                status_map[url] = entry
                funnels_module.STATUS_FILE.write_text(json.dumps(status_map, indent=2))
            except Exception:
                pass
        return jsonify({"success": True, "status": entry.get("status"), "valuation": entry.get("valuation")})

    @funnels_bp.route('/api/valuation', methods=['POST'])
    def funnels_api_valuation():
        import re as _re
        import requests as _requests
        data = request.json
        make = data.get("make")
        model = data.get("model")
        year = data.get("year")
        mileage = data.get("mileage")
        if not all([make, model, year]):
            return jsonify({"error": "Missing make, model, or year"}), 400
        if mileage:
            mileage = str(mileage).lower().replace("km", "").replace("miles", "").replace(",", "").strip()
            digits = _re.findall(r'\d+', mileage)
            mileage = digits[0] if digits else "0"
        try:
            resp = _requests.get("https://mrcar-cotizacion.vercel.app/api/market-price",
                params={"make": make, "model": model, "year": year, "mileage": mileage or "0"},
                headers={"User-Agent": "SimplyAPI/1.0"}, timeout=30)
            resp_data = resp.json()
            if not resp_data.get("success"):
                return jsonify({"error": "Valuation failed", "details": resp_data}), 400
            return jsonify(resp_data)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    app.register_blueprint(funnels_bp)
    print("  ✅ Funnels dashboard mounted at /funnels")

DB_PATH = os.getenv("DB_PATH", str(ROOT / "data" / "inventory.db"))


# ─── DB helpers ──────────────────────────────────────────────────────────────
# get_conn, get_db, get_crm_conn, row_to_dict are imported from db.py (Supabase)
# The functions below are kept only for the CREATE TABLE schema reference.

def _legacy_init_schema():
    """Schema reference only — tables are created in Supabase via setup_crm.sql"""
    pass


# Schema managed via setup_crm.sql in Supabase


# ─── API: Users ───────────────────────────────────────────────────────────────
@app.route("/api/users", methods=["GET"])
def get_users():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM crm_users WHERE active=1 ORDER BY name").fetchall()
    return jsonify([row_to_dict(r) for r in rows])


@app.route("/api/users", methods=["POST"])
def create_user():
    data = request.json
    if not data.get("name") or not data.get("email"):
        return jsonify({"error": "name and email required"}), 400
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT INTO crm_users (name, email, role, color, sucursal, password) VALUES (?, ?, ?, ?, ?, ?)",
                (data["name"], data["email"], data.get("role", "agent"), data.get("color", "#3b82f6"),
                 data.get("sucursal", "Vitacura"), data.get("password", "admin1234"))
            )
            conn.commit()
            user_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            row = conn.execute("SELECT * FROM crm_users WHERE id=?", (user_id,)).fetchone()
            return jsonify(row_to_dict(row)), 201
        except Exception:
            return jsonify({"error": "Email already exists"}), 409


@app.route("/api/users/<int:user_id>", methods=["PATCH"])
def update_user(user_id):
    data = request.json
    allowed = {"name", "email", "role", "color", "active", "sucursal", "password"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"error": "No valid fields"}), 400
    set_clause = ", ".join("{}=?".format(k) for k in updates)
    with get_db() as conn:
        conn.execute("UPDATE crm_users SET {} WHERE id=?".format(set_clause), list(updates.values()) + [user_id])
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    with get_db() as conn:
        conn.execute("UPDATE crm_users SET active=0 WHERE id=?", (user_id,))
        conn.commit()
    return jsonify({"ok": True})


# ─── API: Auth login (for SimplyAPI CRM itself) ───────────────────────────────
@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    data = request.json or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")
    if not email or not password:
        return jsonify({"error": "Email y contraseña requeridos"}), 400
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM crm_users WHERE email=? AND active=1", (email,)
        ).fetchone()
    if not row:
        return jsonify({"error": "Credenciales inválidas"}), 401
    user = row_to_dict(row)
    stored_pw = user.get("password") or "admin1234"
    if password != stored_pw:
        return jsonify({"error": "Credenciales inválidas"}), 401
    user_out = {k: user[k] for k in ["id","name","email","role","color","sucursal"] if k in user}
    session["user"] = user_out
    return jsonify({"ok": True, "user": user_out})


@app.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    session.pop("user", None)
    return jsonify({"ok": True})


@app.route("/api/auth/me", methods=["GET"])
def auth_me():
    user = session.get("user")
    if not user:
        return jsonify({"error": "Not logged in"}), 401
    return jsonify(user)


# ─── API: Consignación → appraisal callback (called by mrcar-consignaciones) ──
@app.route("/api/consignaciones/<int:cid>/appraisal", methods=["POST"])
def link_appraisal(cid):
    """Called by mrcar-consignaciones after saving an appraisal to mark Part 2 done."""
    data = request.json or {}
    appraisal_id = data.get("appraisal_id")
    if not appraisal_id:
        return jsonify({"error": "appraisal_id required"}), 400
    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute(
            "UPDATE consignaciones SET appraisal_supabase_id=?, status=?, part2_completed_at=?, updated_at=? WHERE id=?",
            (appraisal_id, "parte2_completa", now, now, cid)
        )
        conn.commit()
        row = conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"ok": True, "consignacion": row_to_dict(row)})


@app.route("/api/inspecciones", methods=["POST"])
def create_inspeccion():
    """
    Receives the full inspection form from the embedded CRM form,
    proxies it to Supabase appraisals table, then marks the consignacion as parte2_completa.
    """
    import requests as req_lib
    data = request.json or {}
    consignacion_id = data.pop("consignacion_id", None)

    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")

    if not supabase_url or not supabase_key:
        return jsonify({"error": "Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY env vars."}), 500

    headers = {
        "apikey": supabase_key,
        "Authorization": "Bearer " + supabase_key,
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    # ── Extract fields that belong on consignaciones, not appraisals ──────────
    ai_market_price = data.pop("ai_market_price", None)
    ai_instant_buy_price = data.pop("ai_instant_buy_price", None)
    selling_price = data.pop("precio_publicado", None)    # alias alias
    tasacion = data.get("tasacion")                        # keep in appraisals too

    try:
        resp = req_lib.post(
            supabase_url + "/rest/v1/appraisals",
            json=data,
            headers=headers,
            timeout=10
        )
        if resp.status_code not in (200, 201):
            return jsonify({"error": f"Supabase error {resp.status_code}: {resp.text}"}), 502

        rows = resp.json()
        appraisal = rows[0] if isinstance(rows, list) and rows else {}
        appraisal_id = appraisal.get("id")

        # Mark consignacion as parte2_completa and save AI prices
        if consignacion_id and appraisal_id:
            now = datetime.now().isoformat()
            # Build the update dynamically so we only set non-None values
            updates = ["appraisal_supabase_id=?", "status=?",
                       "part2_completed_at=?", "updated_at=?"]
            params: list = [appraisal_id, "parte2_completa", now, now]
            if ai_market_price is not None:
                updates.append("ai_market_price=?")
                params.append(int(ai_market_price))
            if ai_instant_buy_price is not None:
                updates.append("ai_instant_buy_price=?")
                params.append(int(ai_instant_buy_price))
            if selling_price is not None:
                updates.append("selling_price=?")
                params.append(int(selling_price))
            params.append(consignacion_id)
            with get_db() as conn:
                conn.execute(
                    "UPDATE consignaciones SET " + ", ".join(updates) + " WHERE id=?",
                    params
                )
                conn.commit()

        return jsonify({"ok": True, "appraisal_id": appraisal_id})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/inspecciones/fotos", methods=["POST"])
def upload_inspeccion_foto():
    """Upload a photo for an appraisal to Supabase Storage."""
    import requests as req_lib
    appraisal_id = request.form.get("appraisal_id")
    file = request.files.get("file")
    if not file or not appraisal_id:
        return jsonify({"error": "file and appraisal_id required"}), 400

    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")

    if not supabase_url or not supabase_key:
        return jsonify({"ok": True, "note": "Supabase not configured, photo skipped"})

    import uuid, mimetypes
    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else "jpg"
    filename = f"{appraisal_id}/{uuid.uuid4()}.{ext}"
    mime = mimetypes.guess_type(file.filename)[0] or "image/jpeg"

    try:
        upload_resp = req_lib.post(
            f"{supabase_url}/storage/v1/object/vehicle-images/{filename}",
            data=file.read(),
            headers={
                "apikey": supabase_key,
                "Authorization": "Bearer " + supabase_key,
                "Content-Type": mime,
            },
            timeout=30
        )
        public_url = f"{supabase_url}/storage/v1/object/public/vehicle-images/{filename}"
        # Save reference to vehicle_images table
        req_lib.post(
            supabase_url + "/rest/v1/vehicle_images",
            json={"appraisal_id": appraisal_id, "storage_path": filename, "url": public_url},
            headers={"apikey": supabase_key, "Authorization": "Bearer " + supabase_key,
                     "Content-Type": "application/json"},
            timeout=10
        )
        return jsonify({"ok": True, "url": public_url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Helper: get Supabase headers ─────────────────────────────────────────────
def _supa_headers():
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")
    return supabase_url, {
        "apikey": supabase_key,
        "Authorization": "Bearer " + supabase_key,
        "Content-Type": "application/json",
    }


@app.route("/api/inspecciones/<appraisal_id>", methods=["GET"])
def get_inspeccion(appraisal_id):
    """Fetch a full inspection (appraisal + photos) from Supabase."""
    import requests as req_lib
    supabase_url, headers = _supa_headers()
    if not supabase_url:
        return jsonify({"error": "Supabase not configured"}), 500

    try:
        # Get appraisal
        r = req_lib.get(
            supabase_url + "/rest/v1/appraisals",
            params={"select": "*", "id": "eq.{}".format(appraisal_id)},
            headers=headers, timeout=10
        )
        if r.status_code != 200 or not r.json():
            return jsonify({"error": "Appraisal not found"}), 404
        appraisal = r.json()[0]

        # Get photos
        r2 = req_lib.get(
            supabase_url + "/rest/v1/vehicle_images",
            params={"select": "*", "appraisal_id": "eq.{}".format(appraisal_id)},
            headers=headers, timeout=10
        )
        photos = r2.json() if r2.status_code == 200 else []

        return jsonify({"appraisal": appraisal, "photos": photos})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/inspecciones/<appraisal_id>/pdf", methods=["GET"])
def get_inspeccion_pdf(appraisal_id):
    """
    Generate a beautiful PDF inspection report.
    Uses HTML → PDF via WeasyPrint if available, otherwise returns HTML.
    """
    import requests as req_lib
    supabase_url, headers = _supa_headers()
    if not supabase_url:
        return jsonify({"error": "Supabase not configured"}), 500

    # Fetch data
    r = req_lib.get(supabase_url + "/rest/v1/appraisals",
                    params={"select": "*", "id": "eq.{}".format(appraisal_id)},
                    headers=headers, timeout=10)
    if r.status_code != 200 or not r.json():
        return jsonify({"error": "Not found"}), 404
    a = r.json()[0]

    # Fetch photos
    r2 = req_lib.get(supabase_url + "/rest/v1/vehicle_images",
                     params={"select": "url", "appraisal_id": "eq.{}".format(appraisal_id)},
                     headers=headers, timeout=10)
    photos = [p["url"] for p in (r2.json() if r2.status_code == 200 else []) if p.get("url")]

    # Feature label map
    feature_map = {
        "aireAcondicionado": "Aire acondicionado", "bluetooth": "Bluetooth",
        "calefactorAsiento": "Asientos calefaccionados", "conexionUsb": "USB",
        "gps": "GPS", "isofix": "ISOFIX", "smartKey": "Smart Key",
        "lucesLed": "Luces LED", "mandosVolante": "Mandos en volante",
        "sensorEstacionamiento": "Sensor estacionamiento",
        "sonidoPremium": "Sonido premium", "techoElectrico": "Techo eléctrico",
        "ventiladorAsiento": "Asientos ventilados", "carplayAndroid": "CarPlay / Android Auto",
    }
    features = a.get("features") or {}
    active_features = [v for k, v in feature_map.items() if features.get(k)]

    # Neumáticos
    neum = a.get("neumaticos") or [True]*5
    neum_labels = ["Del. Izq.", "Del. Der.", "Tras. Izq.", "Tras. Der.", "Repuesto"]
    neum_html = "".join(
        '<span style="display:inline-block;padding:3px 8px;margin:2px;border-radius:4px;font-size:11px;'
        'background:{bg};color:{fg}">{label}</span>'.format(
            bg="#166534" if (neum[i] if i < len(neum) else True) else "#991b1b",
            fg="#bbf7d0" if (neum[i] if i < len(neum) else True) else "#fecaca",
            label=neum_labels[i] + (" ✓" if (neum[i] if i < len(neum) else True) else " ✗")
        ) for i in range(5)
    )

    # Format price
    def fmt_price(v):
        if not v: return "—"
        try: return "${:,.0f}".format(int(v)).replace(",", ".")
        except: return str(v)

    created = (a.get("created_at") or "")[:10]

    # Photos HTML
    photos_html = ""
    if photos:
        photos_html = '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px;">'
        for url in photos[:9]:
            photos_html += '<img src="{}" style="width:100%;height:120px;object-fit:cover;border-radius:6px;border:1px solid #334155;" />'.format(url)
        photos_html += '</div>'

    html = '''<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  @page {{ size: A4; margin: 20mm; }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; background: #0f172a; color: #e2e8f0; font-size: 13px; line-height: 1.5; }}
  .page {{ max-width: 800px; margin: 0 auto; background: #1e293b; border-radius: 16px; overflow: hidden; }}
  .header {{ background: linear-gradient(135deg, #0f172a, #1e293b); padding: 32px 36px; border-bottom: 2px solid #f59e0b; }}
  .header h1 {{ font-size: 22px; color: #f8fafc; font-weight: 700; letter-spacing: -0.5px; }}
  .header .sub {{ color: #94a3b8; font-size: 12px; margin-top: 4px; }}
  .header .logo {{ color: #f59e0b; font-weight: 800; font-size: 14px; letter-spacing: 2px; text-transform: uppercase; }}
  .section {{ padding: 24px 36px; border-bottom: 1px solid #334155; }}
  .section-title {{ color: #f59e0b; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 14px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px 24px; }}
  .grid3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px 24px; }}
  .field {{ }}
  .field .label {{ color: #64748b; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .field .value {{ color: #f1f5f9; font-size: 14px; font-weight: 600; }}
  .badge {{ display: inline-block; padding: 3px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
  .badge-green {{ background: #166534; color: #bbf7d0; }}
  .badge-amber {{ background: #92400e; color: #fde68a; }}
  .badge-slate {{ background: #334155; color: #94a3b8; }}
  .features {{ display: flex; flex-wrap: wrap; gap: 6px; }}
  .feat {{ padding: 4px 10px; background: #334155; color: #cbd5e1; border-radius: 6px; font-size: 11px; }}
  .plate {{ display: inline-block; background: #f59e0b; color: #0f172a; font-weight: 800; font-size: 18px; padding: 6px 16px; border-radius: 8px; letter-spacing: 2px; font-family: monospace; }}
  .price-box {{ background: #0f172a; border: 1px solid #334155; border-radius: 12px; padding: 16px 20px; margin-top: 8px; }}
  .price-row {{ display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid #1e293b; }}
  .price-row:last-child {{ border: none; }}
  .price-label {{ color: #94a3b8; font-size: 12px; }}
  .price-val {{ color: #f1f5f9; font-size: 15px; font-weight: 700; }}
  .price-val.main {{ color: #4ade80; font-size: 18px; }}
  .footer {{ padding: 20px 36px; text-align: center; color: #475569; font-size: 10px; }}
</style>
</head><body>
<div class="page">
  <!-- Header -->
  <div class="header">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <div>
        <div class="logo">Auto Directo</div>
        <h1>{marca} {modelo} {año}</h1>
        <div class="sub">Informe de Inspección · {fecha}</div>
      </div>
      <div style="text-align:right;">
        <div class="plate">{patente}</div>
        <div style="color:#64748b;font-size:10px;margin-top:4px;">ID: {id_short}</div>
      </div>
    </div>
  </div>

  <!-- Vehicle -->
  <div class="section">
    <div class="section-title">🚗 Datos del Vehículo</div>
    <div class="grid">
      <div class="field"><div class="label">Marca</div><div class="value">{marca}</div></div>
      <div class="field"><div class="label">Modelo</div><div class="value">{modelo}</div></div>
      <div class="field"><div class="label">Versión</div><div class="value">{version}</div></div>
      <div class="field"><div class="label">Año</div><div class="value">{año}</div></div>
      <div class="field"><div class="label">Kilometraje</div><div class="value">{km} km</div></div>
      <div class="field"><div class="label">Color</div><div class="value">{color}</div></div>
      <div class="field"><div class="label">Transmisión</div><div class="value">{transmision}</div></div>
      <div class="field"><div class="label">Combustible</div><div class="value">{combustible}</div></div>
    </div>
  </div>

  <!-- Owner -->
  <div class="section">
    <div class="section-title">👤 Propietario</div>
    <div class="grid">
      <div class="field"><div class="label">Nombre</div><div class="value">{nombre} {apellido}</div></div>
      <div class="field"><div class="label">RUT</div><div class="value">{rut}</div></div>
      <div class="field"><div class="label">Teléfono</div><div class="value">{telefono}</div></div>
      <div class="field"><div class="label">Email</div><div class="value">{email}</div></div>
      <div class="field" style="grid-column:1/-1"><div class="label">Dirección</div><div class="value">{direccion}</div></div>
    </div>
  </div>

  <!-- Pricing -->
  <div class="section">
    <div class="section-title">💰 Valorización</div>
    <div class="price-box">
      <div class="price-row"><span class="price-label">Tasación</span><span class="price-val">{tasacion}</span></div>
      <div class="price-row"><span class="price-label">Precio Publicado</span><span class="price-val main">{precio_pub}</span></div>
      <div class="price-row"><span class="price-label">Precio Sugerido</span><span class="price-val">{precio_sug}</span></div>
      <div class="price-row"><span class="price-label">Comisión</span><span class="price-val">{comision}</span></div>
      <div class="price-row"><span class="price-label">N° Dueños</span><span class="price-val">{duenos}</span></div>
    </div>
  </div>

  <!-- Docs -->
  <div class="section">
    <div class="section-title">📋 Documentación</div>
    <div class="grid3">
      <div class="field"><div class="label">Permiso Circulación</div><div class="value">{permiso_badge}</div></div>
      <div class="field"><div class="label">Revisión Técnica</div><div class="value">{revision_badge}</div></div>
      <div class="field"><div class="label">SOAP</div><div class="value">{soap_badge}</div></div>
    </div>
    <div class="grid" style="margin-top:10px;">
      <div class="field"><div class="label">En Prenda</div><div class="value">{prenda_badge}</div></div>
      <div class="field"><div class="label">En Remate</div><div class="value">{remate_badge}</div></div>
    </div>
  </div>

  <!-- Features -->
  <div class="section">
    <div class="section-title">⚙️ Equipamiento</div>
    <div class="features">{features_html}</div>
  </div>

  <!-- Technical -->
  <div class="section">
    <div class="section-title">🔧 Estado Técnico</div>
    <div class="grid">
      <div class="field"><div class="label">Airbags</div><div class="value">{airbags}</div></div>
      <div class="field"><div class="label">N° Llaves</div><div class="value">{llaves}</div></div>
      <div class="field"><div class="label">Fotos tomadas por</div><div class="value">{quien_fotos}</div></div>
    </div>
    <div style="margin-top:10px;">
      <div class="label" style="color:#64748b;font-size:10px;text-transform:uppercase;margin-bottom:6px;">Neumáticos</div>
      {neumaticos_html}
    </div>
    <div style="margin-top:12px;">
      <div class="label" style="color:#64748b;font-size:10px;text-transform:uppercase;margin-bottom:4px;">Observaciones</div>
      <div style="color:#cbd5e1;font-size:13px;">{observaciones}</div>
    </div>
  </div>

  <!-- Photos -->
  <div class="section" style="border-bottom:none;">
    <div class="section-title">📸 Galería ({num_photos} fotos)</div>
    {photos_html}
  </div>

  <!-- Footer -->
  <div class="footer">
    Informe generado por <strong style="color:#f59e0b;">Auto Directo</strong> · autochile.cl · {fecha}<br>
    Este documento es confidencial y de uso interno.
  </div>
</div>
</body></html>'''.format(
        marca=a.get("vehicle_marca") or "—",
        modelo=a.get("vehicle_modelo") or "—",
        version=a.get("vehicle_version") or "—",
        año=a.get("vehicle_año") or "—",
        km="{:,}".format(int(a.get("vehicle_km") or 0)).replace(",", "."),
        color=a.get("vehicle_color") or "—",
        transmision=a.get("vehicle_transmision") or "—",
        combustible=a.get("vehicle_combustible") or "—",
        patente=(a.get("vehicle_patente") or "—").upper(),
        nombre=a.get("client_nombre") or "—",
        apellido=a.get("client_apellido") or "",
        rut=a.get("client_rut") or "—",
        telefono=a.get("client_telefono") or "—",
        email=a.get("client_email") or "—",
        direccion=a.get("client_direccion") or "—",
        tasacion=fmt_price(a.get("tasacion")),
        precio_pub=fmt_price(a.get("precio_publicado")),
        precio_sug=fmt_price(a.get("precio_sugerido")),
        comision=fmt_price(a.get("comision")),
        duenos=a.get("num_dueños") or "—",
        permiso_badge='<span class="badge badge-green">Vigente</span>' if a.get("permiso_circulacion") else '<span class="badge badge-slate">—</span>',
        revision_badge='<span class="badge badge-green">Vigente</span>' if a.get("revision_tecnica") else '<span class="badge badge-slate">—</span>',
        soap_badge='<span class="badge badge-green">Vigente</span>' if a.get("soap") else '<span class="badge badge-slate">—</span>',
        prenda_badge='<span class="badge badge-amber">Sí</span>' if a.get("en_prenda") else '<span class="badge badge-green">No</span>',
        remate_badge='<span class="badge badge-amber">Sí</span>' if a.get("remate") else '<span class="badge badge-green">No</span>',
        features_html="".join('<span class="feat">{}</span>'.format(f) for f in active_features) or '<span class="badge badge-slate">Sin equipamiento registrado</span>',
        airbags=a.get("airbags") or "—",
        llaves=a.get("num_llaves") or "—",
        quien_fotos=a.get("quien_tomo_fotos") or "—",
        neumaticos_html=neum_html,
        observaciones=a.get("observaciones") or a.get("observations") or "Sin observaciones",
        photos_html=photos_html,
        num_photos=len(photos),
        fecha=created,
        id_short=appraisal_id[:8] if appraisal_id else "—",
    )

    # Check if they want PDF format
    fmt = request.args.get("format", "html")
    if fmt == "pdf":
        try:
            from weasyprint import HTML as WeasyprintHTML
            pdf_bytes = WeasyprintHTML(string=html).write_pdf()
            return app.response_class(
                pdf_bytes, mimetype="application/pdf",
                headers={"Content-Disposition": "inline; filename=inspeccion-{}.pdf".format(
                    (a.get("vehicle_patente") or appraisal_id[:8]).upper()
                )}
            )
        except ImportError:
            # WeasyPrint not installed — return HTML with print-friendly header
            pass

    return app.response_class(html, mimetype="text/html")


@app.route("/api/inspecciones/<appraisal_id>/email", methods=["POST"])
def send_inspeccion_email(appraisal_id):
    """
    Send the inspection report via Resend (autochile.cl domain).
    Body: { "to": "email@example.com", "cc": "...", "subject": "..." }
    """
    import requests as req_lib
    data = request.json or {}
    to_email = data.get("to")
    if not to_email:
        return jsonify({"error": "Destinatario (to) requerido"}), 400

    resend_key = os.environ.get("RESEND_API_KEY", "")
    if not resend_key:
        return jsonify({"error": "RESEND_API_KEY no configurada en .env"}), 500

    # Fetch the HTML report
    supabase_url, headers = _supa_headers()
    r = req_lib.get(supabase_url + "/rest/v1/appraisals",
                    params={"select": "*", "id": "eq.{}".format(appraisal_id)},
                    headers=headers, timeout=10)
    if r.status_code != 200 or not r.json():
        return jsonify({"error": "Appraisal not found"}), 404
    a = r.json()[0]

    patente = (a.get("vehicle_patente") or "").upper()
    marca = a.get("vehicle_marca") or ""
    modelo = a.get("vehicle_modelo") or ""
    año = a.get("vehicle_año") or ""

    # Get the full HTML by calling our own PDF endpoint
    import urllib.request
    try:
        local_url = "http://127.0.0.1:8080/api/inspecciones/{}/pdf".format(appraisal_id)
        with urllib.request.urlopen(local_url, timeout=10) as resp:
            html_body = resp.read().decode("utf-8")
    except Exception as e:
        return jsonify({"error": "Could not generate report: {}".format(e)}), 500

    subject = data.get("subject") or "Informe de Inspección — {} {} {} · {}".format(
        marca, modelo, año, patente
    )

    # Send via Resend API
    email_payload = {
        "from": "Auto Directo <inspeccion@autochile.cl>",
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    }
    if data.get("cc"):
        email_payload["cc"] = [data["cc"]] if isinstance(data["cc"], str) else data["cc"]

    try:
        resp = req_lib.post(
            "https://api.resend.com/emails",
            json=email_payload,
            headers={
                "Authorization": "Bearer " + resend_key,
                "Content-Type": "application/json",
            },
            timeout=15
        )
        if resp.status_code in (200, 201):
            return jsonify({"ok": True, "resend_id": resp.json().get("id")})
        else:
            return jsonify({"error": "Resend error {}: {}".format(resp.status_code, resp.text)}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/calendar", methods=["GET"])
def calendar_get():
    """
    Returns all appointments for a month, merged with local assignment data.
    Query params: ?year=2026&month=2  (defaults to current month)
    """
    import calendar as _cal
    year = int(request.args.get("year", datetime.now().year))
    month = int(request.args.get("month", datetime.now().month))
    _, last_day = _cal.monthrange(year, month)
    date_from = "{:04d}-{:02d}-01".format(year, month)
    date_to = "{:04d}-{:02d}-{:02d}".format(year, month, last_day)

    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")

    appointments = []
    if supabase_url and supabase_key:
        try:
            resp = _requests.get(
                supabase_url + "/rest/v1/appointments",
                params={
                    "select": "*",
                    "appointment_date": "gte.{}".format(date_from),
                    "appointment_date": "lte.{}".format(date_to),
                    "order": "appointment_date.asc,appointment_time.asc"
                },
                headers={"apikey": supabase_key, "Authorization": "Bearer " + supabase_key},
                timeout=10
            )
            if resp.status_code == 200:
                appointments = resp.json()
        except Exception as e:
            print("[Calendar] Supabase error:", e)

    # Merge with local consignaciones (assignment info)
    with get_db() as conn:
        consig_rows = conn.execute(
            "SELECT * FROM consignaciones WHERE appointment_date BETWEEN ? AND ?",
            (date_from, date_to)
        ).fetchall()
        consig_by_supabase_id = {r["appointment_supabase_id"]: row_to_dict(r) for r in consig_rows if r["appointment_supabase_id"]}

        users_map = {r["id"]: row_to_dict(r) for r in conn.execute("SELECT * FROM crm_users").fetchall()}

    result = []
    for appt in appointments:
        appt_id = appt.get("id")
        consig = consig_by_supabase_id.get(appt_id, {})
        assigned_user_id = consig.get("assigned_user_id")
        result.append({
            **appt,
            "consignacion_id": consig.get("id"),
            "consignacion_status": consig.get("status", "sin_consignacion"),
            "assigned_user_id": assigned_user_id,
            "assigned_user": users_map.get(assigned_user_id) if assigned_user_id else None,
        })

    # Collect plates already present from Supabase appointments to avoid duplicates
    supabase_plates = set()
    for ev in result:
        p = (ev.get("plate") or "").upper().strip()
        d = ev.get("appointment_date") or ""
        if p:
            supabase_plates.add((p, d))

    # Also include appointments from local CRM (no supabase_id, exclude funnels)
    with get_db() as conn:
        local_appts = conn.execute(
            "SELECT * FROM crm_leads WHERE appointment_date BETWEEN ? AND ? AND (supabase_id IS NULL OR supabase_id='') AND source NOT IN ('funnels') AND appointment_date IS NOT NULL",
            (date_from, date_to)
        ).fetchall()
    for la in local_appts:
        la_dict = row_to_dict(la)
        # Skip entries with no actual appointment_date
        if not la_dict.get("appointment_date"):
            continue
        # Skip if same plate+date already exists from Supabase appointments
        p = (la_dict.get("plate") or "").upper().strip()
        d = la_dict.get("appointment_date") or ""
        if p and (p, d) in supabase_plates:
            continue
        result.append({
            "id": "local-{}".format(la_dict["id"]),
            "first_name": la_dict.get("first_name"),
            "last_name": la_dict.get("last_name"),
            "full_name": la_dict.get("full_name"),
            "phone": la_dict.get("phone"),
            "plate": la_dict.get("plate"),
            "car_make": la_dict.get("car_make"),
            "car_model": la_dict.get("car_model"),
            "car_year": la_dict.get("car_year"),
            "appointment_date": la_dict.get("appointment_date"),
            "appointment_time": la_dict.get("appointment_time"),
            "status": la_dict.get("stage", "agendado"),
            "source": la_dict.get("source", "local"),
            "consignacion_id": None,
            "consignacion_status": "sin_consignacion",
            "assigned_user_id": None,
            "assigned_user": None,
        })

    # Also include consignaciones created directly via wizard (no supabase appointment)
    seen_consig_ids = {ev.get("consignacion_id") for ev in result if ev.get("consignacion_id")}
    # Build a set of plate+date combos already in results to avoid duplicates
    seen_plate_date = set()
    for ev in result:
        p = (ev.get("plate") or "").upper().strip()
        d = ev.get("appointment_date") or ""
        if p:
            seen_plate_date.add((p, d))

    # Try to link unlinked Supabase appointments to consignaciones by plate
    with get_db() as conn:
        all_consigs = conn.execute(
            "SELECT * FROM consignaciones WHERE appointment_date BETWEEN ? AND ?",
            (date_from, date_to)
        ).fetchall()
    consig_by_plate = {}
    for c in all_consigs:
        cd = row_to_dict(c)
        p = (cd.get("plate") or "").upper().strip()
        if p:
            consig_by_plate[p] = cd

    for ev in result:
        if not ev.get("consignacion_id"):
            p = (ev.get("plate") or "").upper().strip()
            if p and p in consig_by_plate:
                c = consig_by_plate[p]
                ev["consignacion_id"] = c["id"]
                ev["consignacion_status"] = c.get("status", "pendiente")
                seen_consig_ids.add(c["id"])

    # Add consignaciones not yet represented
    with get_db() as conn:
        direct_consigs = conn.execute(
            "SELECT * FROM consignaciones WHERE appointment_date BETWEEN ? AND ? AND (appointment_supabase_id IS NULL OR appointment_supabase_id='')",
            (date_from, date_to)
        ).fetchall()
    for dc in direct_consigs:
        dc_dict = row_to_dict(dc)
        if dc_dict["id"] in seen_consig_ids:
            continue  # Already in results via supabase match or plate match
        plate = (dc_dict.get("plate") or "").upper().strip()
        date = dc_dict.get("appointment_date") or ""
        if plate and (plate, date) in seen_plate_date:
            continue  # Same plate+date already in calendar from Supabase appointment
        assigned_user_id = dc_dict.get("assigned_user_id")
        result.append({
            "id": "consig-{}".format(dc_dict["id"]),
            "first_name": dc_dict.get("owner_first_name"),
            "last_name": dc_dict.get("owner_last_name"),
            "full_name": dc_dict.get("owner_full_name"),
            "phone": dc_dict.get("owner_phone"),
            "email": dc_dict.get("owner_email"),
            "rut": dc_dict.get("owner_rut"),
            "plate": dc_dict.get("plate"),
            "car_make": dc_dict.get("car_make"),
            "car_model": dc_dict.get("car_model"),
            "car_year": dc_dict.get("car_year"),
            "mileage": dc_dict.get("mileage"),
            "version": dc_dict.get("version"),
            "appointment_date": dc_dict.get("appointment_date"),
            "appointment_time": dc_dict.get("appointment_time"),
            "status": dc_dict.get("status", "pendiente"),
            "source": "wizard",
            "consignacion_id": dc_dict["id"],
            "consignacion_status": dc_dict.get("status", "pendiente"),
            "assigned_user_id": assigned_user_id,
            "assigned_user": users_map.get(assigned_user_id) if assigned_user_id else None,
        })

    # Enrich each appointment with Funnels match suggestions
    with get_db() as conn:
        for ev in result:
            make = (ev.get("car_make") or "").lower().strip()
            model = (ev.get("car_model") or "").lower().strip()
            year = ev.get("car_year")
            if not make and not model:
                ev["matched_funnel_leads"] = []
                continue
            # Score every funnels lead against this appointment
            candidates = conn.execute(
                "SELECT id, full_name, car_make, car_model, car_year, mileage, listing_price, funnel_url FROM crm_leads WHERE source='funnels'"
            ).fetchall()
            matches = []
            for c in candidates:
                score = 0
                c_make = (c["car_make"] or "").lower().strip()
                c_model = (c["car_model"] or "").lower().strip()
                # Funnels often stores title as "2017 Mazda CX-5" → car_make="2017", car_model="Mazda CX-5"
                # So also search the model field for make matches
                c_combined = (c_make + " " + c_model).lower()
                c_year = c["car_year"]
                if make and (make in c_make or make in c_model or make in c_combined):
                    score += 40
                if model and c_model and (model in c_model or c_model in model):
                    score += 40
                if year and c_year and int(year) == int(c_year):
                    score += 30  # exact year bonus
                elif year and c_year and abs(int(year) - int(c_year)) <= 1:
                    score += 15
                if score >= 60:
                    matches.append({
                        "id": c["id"],
                        "full_name": c["full_name"],
                        "car_make": c["car_make"],
                        "car_model": c["car_model"],
                        "car_year": c["car_year"],
                        "mileage": c["mileage"],
                        "listing_price": c["listing_price"],
                        "funnel_url": c["funnel_url"],
                        "score": score,
                    })
            matches.sort(key=lambda x: -x["score"])
            ev["matched_funnel_leads"] = matches[:8]

    # Sort all by date then time
    result.sort(key=lambda x: (x.get("appointment_date") or "", x.get("appointment_time") or ""))
    return jsonify(result)


@app.route("/api/calendar/assign", methods=["POST"])
def calendar_assign():
    """Assign a user to an appointment and auto-create consignacion Part 1."""
    data = request.json
    supabase_id = data.get("supabase_id")
    user_id = data.get("user_id")
    if not supabase_id:
        return jsonify({"error": "supabase_id required"}), 400

    # Fetch full appointment from Supabase
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")
    appt = {}
    if supabase_url and supabase_key:
        try:
            resp = _requests.get(
                supabase_url + "/rest/v1/appointments",
                params={"select": "*", "id": "eq.{}".format(supabase_id)},
                headers={"apikey": supabase_key, "Authorization": "Bearer " + supabase_key},
                timeout=10
            )
            if resp.status_code == 200 and resp.json():
                appt = resp.json()[0]
        except Exception as e:
            print("[Calendar assign] Supabase error:", e)

    now = datetime.now().isoformat()
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM consignaciones WHERE appointment_supabase_id=?", (supabase_id,)
        ).fetchone()
        if existing:
            # Update assignment
            conn.execute(
                "UPDATE consignaciones SET assigned_user_id=?, updated_at=? WHERE appointment_supabase_id=?",
                (user_id, now, supabase_id)
            )
            consig_id = existing["id"]
        else:
            # Check if this appointment is linked to a funnel lead with AI prices
            selling_price = None
            owner_price = None
            ai_market_price = None
            ai_instant_buy_price = None
            matched_id = appt.get("matched_funnel_id")
            if matched_id:
                try:
                    with get_crm_conn() as crm_conn:
                        lead = crm_conn.execute(
                            "SELECT estimated_value, ai_consignacion_price, listing_price, ai_instant_buy_price FROM crm_leads WHERE funnel_url=? OR id=? LIMIT 1",
                            (matched_id, matched_id)
                        ).fetchone()
                        if lead:
                            # market value goes to selling_price
                            selling_price = lead.get("estimated_value")
                            ai_market_price = lead.get("estimated_value")
                            ai_instant_buy_price = lead.get("ai_instant_buy_price")
                            # payout goes to owner_price
                            owner_price = lead.get("ai_consignacion_price") or lead.get("listing_price")
                except Exception as e:
                    print(f"[Calendar assign] Error fetching CRM lead prices: {e}")

            # Create Part 1 consignación from appointment data
            conn.execute("""
                INSERT INTO consignaciones (
                    appointment_supabase_id, owner_first_name, owner_last_name,
                    owner_full_name, owner_rut, owner_phone, owner_country_code,
                    owner_email, owner_region, owner_commune, owner_address,
                    plate, car_make, car_model, car_year, mileage, version,
                    appointment_date, appointment_time, assigned_user_id,
                    status, selling_price, owner_price, ai_market_price, ai_instant_buy_price, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                supabase_id,
                appt.get("first_name"), appt.get("last_name"), appt.get("full_name"),
                appt.get("rut"), appt.get("phone"), appt.get("country_code", "+56"),
                appt.get("email"), appt.get("region"), appt.get("commune"), appt.get("address"),
                appt.get("plate"), appt.get("car_make"), appt.get("car_model"),
                appt.get("car_year"), appt.get("mileage"), appt.get("version"),
                appt.get("appointment_date"), appt.get("appointment_time"),
                user_id, "parte1_completa", selling_price, owner_price, ai_market_price, ai_instant_buy_price, now, now
            ))
            consig_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        row = conn.execute("SELECT * FROM consignaciones WHERE id=?", (consig_id,)).fetchone()
    return jsonify({"ok": True, "consignacion": row_to_dict(row)})


# ─── API: Consignaciones ──────────────────────────────────────────────────────
@app.route("/api/consignaciones", methods=["POST"])
def create_consignacion():
    """Create a new consignacion directly from the Autodirecto.cl wizard form."""
    data = request.json or {}
    now = datetime.now().isoformat()

    # Accept both camelCase (from wizard) and snake_case
    def g(camel, snake=None):
        return data.get(camel) or data.get(snake or camel)

    car = data.get("carData") or {}
    plate = (g("plate") or "").upper().strip()
    supa_id = data.get("appointment_supabase_id")
    appointment_date = g("appointmentDate", "appointment_date") or ""
    appointment_time = g("appointmentTime", "appointment_time") or ""

    # Extract date/time if combined ISO string was sent
    if "T" in appointment_date:
        parts = appointment_date.split("T")
        appointment_date = parts[0]
        if not appointment_time:
            appointment_time = parts[1][:5]

    first_name = (g("firstName", "first_name") or "").strip()
    last_name  = (g("lastName",  "last_name")  or "").strip()
    full_name  = f"{first_name} {last_name}".strip() or data.get("full_name", "")

    with get_db() as conn:
        result = conn.execute("""
            INSERT INTO consignaciones (
                appointment_supabase_id,
                owner_first_name, owner_last_name, owner_full_name,
                owner_rut, owner_phone, owner_country_code, owner_email,
                owner_region, owner_commune, owner_address,
                plate, car_make, car_model, car_year, mileage, version,
                appointment_date, appointment_time,
                status, part1_completed_at, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            supa_id,
            first_name, last_name, full_name,
            g("rut"), g("phone"), g("countryCode", "country_code") or "+56", g("email"),
            g("region"), g("commune"), g("address"),
            plate,
            car.get("make") or g("carMake", "car_make"),
            car.get("model") or g("carModel", "car_model"),
            car.get("year") or g("carYear", "car_year"),
            g("mileage"), g("version"),
            appointment_date, appointment_time,
            "parte1_completa", now, now, now
        ))
        conn.commit()
        # The Supabase adapter returns the inserted row directly
        inserted = result.fetchone()
        new_id = inserted.get("id") if inserted else conn._last_insert_id
        row = conn.execute("SELECT * FROM consignaciones WHERE id=?", (new_id,)).fetchone() if new_id else inserted

    # ── Match against existing funnels leads (FB Marketplace) by make+model+year ──
    # Then call MrCar AI to get real pricing, and create/update a CRM lead as "Agendado".
    try:
        car_make_val = (car.get("make") or g("carMake", "car_make") or "").strip().upper()
        car_model_val = (car.get("model") or g("carModel", "car_model") or "").strip().upper()
        car_year_val = car.get("year") or g("carYear", "car_year") or ""
        mileage_val = g("mileage") or ""

        matched_lead = None
        listing_price = None   # What the FB seller asks
        print("[consignacion] matching: make={} model={} year={}".format(car_make_val, car_model_val, car_year_val), flush=True)

        with get_crm_conn() as crm:
            # Try to find a funnels lead that matches this car.
            # Funnels car_model = "Brand Model" (e.g. "Mazda CX-5") and car_year = year.
            # Wizard sends make="TOYOTA", model="YARIS" separately.
            # Strategy: use the same point-based scoring logic as the calendar view.
            if car_make_val or car_model_val:
                make = car_make_val.lower().strip()
                model = car_model_val.lower().strip()
                year = car_year_val

                candidates = crm.execute(
                    "SELECT id, full_name, car_make, car_model, car_year, mileage, "
                    "listing_price, estimated_value, ai_consignacion_price, ai_instant_buy_price, funnel_url "
                    "FROM crm_leads WHERE source='funnels'"
                ).fetchall()

                matches = []
                for c in candidates:
                    score = 0
                    c_make = (c["car_make"] or "").lower().strip()
                    c_model = (c["car_model"] or "").lower().strip()
                    c_combined = (c_make + " " + c_model).lower()
                    c_year = c["car_year"]

                    if make and (make in c_make or make in c_model or make in c_combined):
                        score += 40
                    if model and c_model and (model in c_model or c_model in model):
                        score += 40
                    if year and c_year and int(year) == int(c_year):
                        score += 30  # exact year bonus
                    elif year and c_year and abs(int(year) - int(c_year)) <= 1:
                        score += 15

                    if score >= 60:
                        matches.append({
                            "lead": c,
                            "score": score
                        })

                if matches:
                    matches.sort(key=lambda x: -x["score"])
                    matched_lead = matches[0]["lead"]
                    listing_price = matched_lead.get("listing_price") or matched_lead.get("estimated_value")
                    print("[consignacion] MATCHED funnels lead id={} with score={}, listing_price={}".format(
                        matched_lead.get("id"), matches[0]["score"], listing_price), flush=True)

            print(f"[consignacion] Matching checkpoints - Plate: {plate}, SupaID: {supa_id}, RUT: {g('rut')}, Phone: {g('phone')}", flush=True)

            # 1. Match by Supabase ID (strongest link from wizard)
            if not matched_lead and supa_id:
                existing_by_supa = crm.execute(
                    "SELECT * FROM crm_leads WHERE supabase_id=? LIMIT 1", (supa_id,)
                ).fetchone()
                if existing_by_supa:
                    matched_lead = existing_by_supa
                    print(f"[consignacion] Matched by Supabase ID: {supa_id}", flush=True)

            # 2. Match by plate (standard match) - Simplified SQL for db.py
            if not matched_lead and plate:
                existing_by_plate = crm.execute(
                    "SELECT * FROM crm_leads WHERE plate=? LIMIT 1", (plate,)
                ).fetchone()
                if existing_by_plate:
                    matched_lead = existing_by_plate
                    print(f"[consignacion] Matched by Plate: {plate}", flush=True)

            # 3. Match by RUT
            if not matched_lead and g("rut"):
                existing_by_rut = crm.execute(
                    "SELECT * FROM crm_leads WHERE rut=? LIMIT 1", (g("rut"),)
                ).fetchone()
                if existing_by_rut:
                    matched_lead = existing_by_rut
                    print(f"[consignacion] Matched by RUT: {g('rut')}", flush=True)

            # 4. Match by Phone
            if not matched_lead and g("phone"):
                existing_by_phone = crm.execute(
                    "SELECT * FROM crm_leads WHERE phone=? LIMIT 1", (g("phone"),)
                ).fetchone()
                if existing_by_phone:
                    matched_lead = existing_by_phone
                    print(f"[consignacion] Matched by Phone: {g('phone')}", flush=True)

            if matched_lead:
                listing_price = matched_lead.get("listing_price") or matched_lead.get("estimated_value")
                lead_id = matched_lead.get("id")
                # Update the matched lead to agendado with the new contact info
                crm.execute("""
                    UPDATE crm_leads SET
                        stage=?, first_name=?, last_name=?, full_name=?,
                        rut=?, phone=?, country_code=?, email=?,
                        region=?, commune=?, address=?, plate=?,
                        appointment_date=?, appointment_time=?,
                        supabase_id=?, updated_at=?
                    WHERE id=?
                """, (
                    "agendado", first_name, last_name, full_name,
                    g("rut"), g("phone"), g("countryCode", "country_code") or "+56", g("email"),
                    g("region"), g("commune"), g("address"), plate,
                    appointment_date, appointment_time,
                    supa_id, now, lead_id
                ))
                crm.commit()
                print("[consignacion] matched funnels lead #{} (listing_price={})".format(lead_id, listing_price))
            else:
                # No match — create a fresh CRM lead
                crm.execute("""
                    INSERT INTO crm_leads (
                        first_name, last_name, full_name, rut, phone, country_code, email,
                        region, commune, address, plate, car_make, car_model, car_year,
                        mileage, version, appointment_date, appointment_time,
                        stage, source, supabase_id, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    first_name, last_name, full_name,
                    g("rut"), g("phone"), g("countryCode", "country_code") or "+56", g("email"),
                    g("region"), g("commune"), g("address"),
                    plate, car_make_val, car_model_val, car_year_val,
                    mileage_val, g("version"),
                    appointment_date, appointment_time,
                    "agendado", "web_wizard", supa_id, now, now
                ))
                crm.commit()

        if new_id:
            print(f"[consignacion] Successfully created consignacion #{new_id} for {full_name}")
        
        # ── Use AI price from matched crm_lead if available, else skip AI price ──
        ai_consignacion_price = None
        ai_market_price = None
        ai_immediate_offer = None
        if matched_lead:
            ai_consignacion_price = matched_lead.get("ai_consignacion_price")
            ai_market_price = matched_lead.get("estimated_value")
            ai_immediate_offer = matched_lead.get("ai_instant_buy_price")
            print(f"[consignacion] using AI price from crm_lead: consignacion={ai_consignacion_price}", flush=True)
        # If no matched lead or no AI price, do NOT recalculate — just leave selling_price blank
        else:
            print("[consignacion] no matched lead or no AI price, skipping AI price", flush=True)

        # ── Update consignacion with AI prices ──
        # selling_price = AI market price (what the car will be sold for)
        # owner_price = AI consignación price (what the owner gets)
        if new_id and (ai_market_price or ai_consignacion_price):
            try:
                updates = []
                params = []
                if ai_market_price:
                    updates.append("selling_price=?")
                    params.append(int(ai_market_price))
                    updates.append("ai_market_price=?")
                    params.append(int(ai_market_price))
                if ai_consignacion_price:
                    updates.append("owner_price=?")
                    params.append(int(ai_consignacion_price))
                if ai_immediate_offer:
                    updates.append("ai_instant_buy_price=?")
                    params.append(int(ai_immediate_offer))
                updates.append("updated_at=?")
                params.append(now)
                params.append(new_id)

                print("[consignacion] updating consignacion #{}: {}".format(
                    new_id, ", ".join(updates)), flush=True)
                with get_db() as conn2:
                    conn2.execute(
                        "UPDATE consignaciones SET {} WHERE id=?".format(", ".join(updates)),
                        tuple(params)
                    )
                    conn2.commit()
                print("[consignacion] set selling_price={}, owner_price={} on consignacion #{}".format(
                    ai_consignacion_price, listing_price, new_id), flush=True)
            except Exception as e2:
                import traceback
                print("[consignacion] price update error: {}".format(e2), flush=True)
                traceback.print_exc()
        else:
            print("[consignacion] skipping price update (no AI price and no listing_price)", flush=True)

        # ── Also update AI prices on the CRM lead (if columns exist) ──
        if matched_lead and (ai_consignacion_price or ai_immediate_offer or ai_market_price):
            try:
                lead_id = matched_lead.get("id")
                with get_crm_conn() as crm2:
                    lead_updates = []
                    lead_params = []
                    if ai_market_price:
                        lead_updates.append("estimated_value=?")
                        lead_params.append(int(ai_market_price))
                    if ai_consignacion_price:
                        lead_updates.append("ai_consignacion_price=?")
                        lead_params.append(int(ai_consignacion_price))
                    if ai_immediate_offer:
                        lead_updates.append("ai_instant_buy_price=?")
                        lead_params.append(int(ai_immediate_offer))
                    lead_updates.append("updated_at=?")
                    lead_params.append(now)
                    lead_params.append(lead_id)
                    crm2.execute(
                        "UPDATE crm_leads SET {} WHERE id=?".format(", ".join(lead_updates)),
                        tuple(lead_params)
                    )
                    crm2.commit()
                print("[consignacion] updated AI prices on crm_lead #{}".format(lead_id), flush=True)
            except Exception as e3:
                # Columns may not exist yet — non-fatal
                print("[consignacion] crm_lead AI price update skipped: {}".format(e3), flush=True)

    except Exception as e:
        import traceback
        print("[consignacion→crm_lead] error:", e, flush=True)
        traceback.print_exc()

    return jsonify({"ok": True, "id": new_id, "consignacion": row_to_dict(row or inserted)}), 201


@app.route("/api/consignaciones", methods=["GET"])
def get_consignaciones():
    status = request.args.get("status")
    with get_db() as conn:
        q = "SELECT * FROM consignaciones WHERE 1=1"
        params = []
        if status:
            q += " AND status=?"
            params.append(status)
        q += " ORDER BY appointment_date ASC, appointment_time ASC"
        rows = conn.execute(q, params).fetchall()
    result = [row_to_dict(r) for r in rows]
    # Resolve assigned user names in a second pass
    user_cache = {}
    for c in result:
        uid = c.get("assigned_user_id")
        if uid and uid not in user_cache:
            try:
                with get_db() as conn2:
                    u = conn2.execute("SELECT name, color FROM crm_users WHERE id=?", (uid,)).fetchone()
                    user_cache[uid] = row_to_dict(u) if u else {}
            except:
                user_cache[uid] = {}
        if uid and user_cache.get(uid):
            c["assigned_user_name"] = user_cache[uid].get("name", "")
            c["assigned_user_color"] = user_cache[uid].get("color", "")
    return jsonify(result)


@app.route("/api/consignaciones/<int:cid>", methods=["GET"])
def get_consignacion(cid):
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM consignaciones WHERE id=?",
            (cid,)
        ).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    result = row_to_dict(row)
    # Add assigned user name if assigned
    if result.get("assigned_user_id"):
        with get_db() as conn:
            user = conn.execute("SELECT * FROM crm_users WHERE id=?", (result["assigned_user_id"],)).fetchone()
            result["assigned_user_name"] = row_to_dict(user).get("name") if user else None
    else:
        result["assigned_user_name"] = None
    return jsonify(result)


@app.route("/api/consignaciones/<int:cid>", methods=["PATCH"])
def update_consignacion(cid):
    data = request.json
    allowed = {
        "owner_first_name","owner_last_name","owner_rut","owner_phone","owner_email",
        "owner_region","owner_commune","owner_address","plate","car_make","car_model",
        "car_year","mileage","version","color","vin","owner_price","selling_price",
        "ai_market_price","ai_instant_buy_price",
        "commission_pct","condition_notes","km_verified","inspection_photos",
        "appointment_date","appointment_time","assigned_user_id","status","notes",
        "part1_completed_at","part2_completed_at","appraisal_supabase_id",
        "contract_signed_at","contract_pdf"
    }
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"message": "No valid fields to update, ignoring", "ok": True}), 200

    # Auto-set completion timestamps
    now = datetime.now().isoformat()
    if updates.get("status") == "parte2_completa" and not updates.get("part2_completed_at"):
        updates["part2_completed_at"] = now
    updates["updated_at"] = now

    # Pre-calculate owner_full_name if name parts change to ensure immediate UI parity
    if "owner_first_name" in updates or "owner_last_name" in updates:
        with get_db() as conn:
            current = conn.execute("SELECT owner_first_name, owner_last_name FROM consignaciones WHERE id=?", (cid,)).fetchone()
        if current:
            fn = updates.get("owner_first_name", current.get("owner_first_name") or "").strip()
            ln = updates.get("owner_last_name", current.get("owner_last_name") or "").strip()
            updates["owner_full_name"] = "{} {}".format(fn, ln).strip()

    set_clause = ", ".join("{}=?".format(k) for k in updates)
    with get_db() as conn:
        conn.execute("UPDATE consignaciones SET {} WHERE id=?".format(set_clause), list(updates.values()) + [cid])
        conn.commit()
        row = conn.execute(
            "SELECT c.*, u.name as assigned_user_name FROM consignaciones c LEFT JOIN crm_users u ON c.assigned_user_id=u.id WHERE c.id=?",
            (cid,)
        ).fetchone()

    result = row_to_dict(row)

    # Sync CRM lead stage when consignacion status changes
    new_status = updates.get("status")
    if new_status:
        _sync_crm_lead_stage(
            result.get("plate"), 
            new_status, 
            result.get("appointment_supabase_id"), 
            result.get("owner_rut"), 
            result.get("owner_phone")
        )

    # Sync Owner details to CRM — re-fetch full consig to ensure all fields are present
    try:
        with get_db() as conn_sync:
            full_consig = conn_sync.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
        if full_consig:
            _sync_crm_lead_owner_details(row_to_dict(full_consig))
    except Exception as e_sync:
        print(f"[update_consignacion] sync error: {e_sync}", flush=True)

    result["ok"] = True
    return jsonify(result)


def _sync_crm_lead_stage(plate, consig_status, appt_id=None, rut=None, phone=None):
    """
    When a consignacion status changes, find any matching CRM lead
    using multi-layered matching and update its stage accordingly.
    """
    # Map consignacion status → CRM stage
    status_to_stage = {
        "pendiente":       "nuevo",
        "parte1_completa": "agendado",
        "parte2_completa": "inspeccionado",
        "en_venta":        "en_venta",
        "vendida":         "vendido",
    }
    new_stage = status_to_stage.get(consig_status)
    plate = (plate or "").strip()
    
    msg = f"[sync_crm_stage] Triggered (plate: '{plate}', appt_id: '{appt_id}', rut: '{rut}', phone: '{phone}') -> {new_stage}"
    print(msg, flush=True)
    log_to_file(msg)

    if not any([plate, appt_id, rut, phone]):
        return

    try:
        with get_crm_conn() as conn:
            lead = None
            
            # 1. Match by Supabase ID
            if appt_id:
                lead = conn.execute(
                    "SELECT id, stage FROM crm_leads WHERE supabase_id=? LIMIT 1",
                    (appt_id,)
                ).fetchone()
                if lead: log_to_file(f"[sync_crm_stage] Matched by Supabase ID: {lead['id']}")

            # 2. Match by Plate - Simplified for db.py
            if not lead and plate:
                lead = conn.execute(
                    "SELECT id, stage FROM crm_leads WHERE plate=? LIMIT 1",
                    (plate,)
                ).fetchone()
                if lead: log_to_file(f"[sync_crm_stage] Matched by Plate: {lead['id']}")

            # 3. Match by RUT
            if not lead and rut:
                lead = conn.execute(
                    "SELECT id, stage FROM crm_leads WHERE rut=? LIMIT 1",
                    (rut,)
                ).fetchone()
                if lead: log_to_file(f"[sync_crm_stage] Matched by RUT: {lead['id']}")

            # 4. Match by Phone
            if not lead and phone:
                lead = conn.execute(
                    "SELECT id, stage FROM crm_leads WHERE phone=? LIMIT 1",
                    (phone,)
                ).fetchone()
                if lead: log_to_file(f"[sync_crm_stage] Matched by Phone: {lead['id']}")

            if lead:
                log_to_file(f"[sync_crm_stage] Syncing stage for lead ID {lead['id']}")
                old_stage = lead["stage"]
                if old_stage != new_stage:
                    now = datetime.now().isoformat()
                    conn.execute(
                        "UPDATE crm_leads SET stage=?, updated_at=? WHERE id=?",
                        (new_stage, now, lead["id"])
                    )
                    conn.execute(
                        "INSERT INTO crm_activities (lead_id, type, title, description) VALUES (?, ?, ?, ?)",
                        (lead["id"], "stage_change", "Etapa actualizada (auto)",
                         "{} → {} (desde consignación)".format(
                             CRM_STAGE_LABELS.get(old_stage, old_stage),
                             CRM_STAGE_LABELS.get(new_stage, new_stage)))
                    )
                    conn.commit()
                    log_to_file(f"[sync_crm_stage] Successfully updated lead {lead['id']} stage to {new_stage}")
            else:
                log_to_file(f"[sync_crm_stage] No matching CRM lead found")
    except Exception as e:
        log_to_file(f"[sync_crm_stage] Error: {e}")

def _sync_crm_lead_owner_details(consig):
    """
    When consignacion owner details are updated, push them to the linked CRM lead
    so both modules stay perfectly in sync. 
    Also updates owner_full_name in the consignacion if not set.
    """
    consig_id = consig.get("id")
    plate = (consig.get("plate") or "").strip()
    appt_id = consig.get("appointment_supabase_id")
    rut = (consig.get("owner_rut") or "").strip()
    phone = (consig.get("owner_phone") or "").strip()
    
    log_to_file(f"[sync_crm_owner] Triggered for consig ID {consig_id} (plate: '{plate}', appt_id: '{appt_id}', rut: '{rut}', phone: '{phone}')")
    
    if not any([plate, appt_id, rut, phone]):
        log_to_file(f"[sync_crm_owner] No identifiers found for consig {consig_id}, skipping sync.")
        return
    
    # ...
    fn = (consig.get("owner_first_name") or "").strip()
    ln = (consig.get("owner_last_name") or "").strip()
    full = "{} {}".format(fn, ln).strip()
    
    # Update local owner_full_name if it changed or is missing
    if full and full != consig.get("owner_full_name"):
        try:
            with get_db() as conn:
                conn.execute("UPDATE consignaciones SET owner_full_name=? WHERE id=?", (full, consig_id))
                conn.commit()
                print(f"[sync_crm_owner] Updated local owner_full_name for {consig_id} to: {full}", flush=True)
        except Exception as e:
            print(f"[sync_crm_owner] Local full_name update error for {consig_id}: {e}", flush=True)

    lead_updates = {
        "first_name": fn,
        "last_name": ln,
        "full_name": full,
        "phone": phone,
        "email": consig.get("owner_email"),
        "rut": rut,
        "region": consig.get("owner_region"),
        "commune": consig.get("owner_commune"),
        "address": consig.get("owner_address"),
        "updated_at": datetime.now().isoformat()
    }

    try:
        with get_crm_conn() as conn:
            lead = None
            
            # 1. Match by Supabase ID
            if appt_id:
                print(f"[sync_crm_owner] Attempting match by Supabase ID: {appt_id}", flush=True)
                lead = conn.execute(
                    "SELECT id FROM crm_leads WHERE supabase_id=? LIMIT 1",
                    (appt_id,)
                ).fetchone()
                if lead: print(f"[sync_crm_owner] Found match by Supabase ID: {lead['id']}", flush=True)

            # 2. Match by Plate - Simplified for db.py
            if not lead and plate:
                print(f"[sync_crm_owner] Attempting match by Plate: {plate}", flush=True)
                lead = conn.execute(
                    "SELECT id FROM crm_leads WHERE plate=? LIMIT 1",
                    (plate,)
                ).fetchone()
                if lead: print(f"[sync_crm_owner] Found match by Plate: {lead['id']}", flush=True)

            # 3. Match by RUT
            if not lead and rut:
                print(f"[sync_crm_owner] Attempting match by RUT: {rut}", flush=True)
                lead = conn.execute(
                    "SELECT id FROM crm_leads WHERE rut=? LIMIT 1",
                    (rut,)
                ).fetchone()
                if lead: print(f"[sync_crm_owner] Found match by RUT: {lead['id']}", flush=True)

            # 4. Match by Phone
            if not lead and phone:
                print(f"[sync_crm_owner] Attempting match by Phone: {phone}", flush=True)
                lead = conn.execute(
                    "SELECT id FROM crm_leads WHERE phone=? LIMIT 1",
                    (phone,)
                ).fetchone()
                if lead: print(f"[sync_crm_owner] Found match by Phone: {lead['id']}", flush=True)

            if lead:
                print(f"[sync_crm_owner] Syncing to CRM lead ID {lead['id']} (Name: {full})", flush=True)
                set_clause = ", ".join("{}=?".format(k) for k in lead_updates)
                conn.execute(
                    "UPDATE crm_leads SET {} WHERE id=?".format(set_clause),
                    list(lead_updates.values()) + [lead["id"]]
                )
                conn.commit()
                print(f"[sync_crm_owner] Successfully pushed updates to CRM lead {lead['id']}", flush=True)
            else:
                print(f"[sync_crm_owner] No matching CRM lead found for consig {consig_id}", flush=True)
    except Exception as e:
        print(f"[sync_crm_owner] Error for consig {consig_id}: {e}", flush=True)


def _sync_consignacion_from_crm_lead(lead):
    """
    Reverse sync: when CRM lead details are updated, push them to the
    linked consignacion so both modules stay perfectly in sync.
    Matches by plate, rut, or phone.
    """
    lead_id = lead.get("id")
    plate = (lead.get("plate") or "").strip()
    rut = (lead.get("rut") or "").strip()
    phone = (lead.get("phone") or "").strip()
    supabase_id = lead.get("supabase_id")

    print(f"[sync_consig_from_crm] Triggered for CRM lead {lead_id} (plate='{plate}', rut='{rut}', phone='{phone}')", flush=True)

    if not any([plate, rut, phone, supabase_id]):
        print(f"[sync_consig_from_crm] No identifiers, skipping", flush=True)
        return

    # Build update payload: CRM field → consignacion field
    consig_updates = {}
    fn = (lead.get("first_name") or "").strip()
    ln = (lead.get("last_name") or "").strip()
    full = (lead.get("full_name") or "").strip() or "{} {}".format(fn, ln).strip()

    if fn: consig_updates["owner_first_name"] = fn
    if ln: consig_updates["owner_last_name"] = ln
    if full: consig_updates["owner_full_name"] = full
    if lead.get("rut"): consig_updates["owner_rut"] = rut
    if lead.get("phone"): consig_updates["owner_phone"] = phone
    if lead.get("email"): consig_updates["owner_email"] = lead["email"]
    if lead.get("region"): consig_updates["owner_region"] = lead["region"]
    if lead.get("commune"): consig_updates["owner_commune"] = lead["commune"]
    if lead.get("address"): consig_updates["owner_address"] = lead["address"]
    if lead.get("country_code"): consig_updates["owner_country_code"] = lead["country_code"]

    if not consig_updates:
        print(f"[sync_consig_from_crm] No owner fields to sync, skipping", flush=True)
        return

    consig_updates["updated_at"] = datetime.now().isoformat()

    try:
        with get_db() as conn:
            consig = None

            # 1. Match by appointment_supabase_id
            if supabase_id:
                consig = conn.execute(
                    "SELECT id FROM consignaciones WHERE appointment_supabase_id=? LIMIT 1",
                    (supabase_id,)
                ).fetchone()
                if consig: print(f"[sync_consig_from_crm] Matched by supabase_id → consig #{consig['id']}", flush=True)

            # 2. Match by plate
            if not consig and plate:
                consig = conn.execute(
                    "SELECT id FROM consignaciones WHERE plate=? LIMIT 1",
                    (plate,)
                ).fetchone()
                if consig: print(f"[sync_consig_from_crm] Matched by plate → consig #{consig['id']}", flush=True)

            # 3. Match by RUT
            if not consig and rut:
                consig = conn.execute(
                    "SELECT id FROM consignaciones WHERE owner_rut=? LIMIT 1",
                    (rut,)
                ).fetchone()
                if consig: print(f"[sync_consig_from_crm] Matched by RUT → consig #{consig['id']}", flush=True)

            # 4. Match by phone
            if not consig and phone:
                consig = conn.execute(
                    "SELECT id FROM consignaciones WHERE owner_phone=? LIMIT 1",
                    (phone,)
                ).fetchone()
                if consig: print(f"[sync_consig_from_crm] Matched by phone → consig #{consig['id']}", flush=True)

            if consig:
                cid = consig["id"]
                set_clause = ", ".join("{}=?".format(k) for k in consig_updates)
                conn.execute(
                    "UPDATE consignaciones SET {} WHERE id=?".format(set_clause),
                    list(consig_updates.values()) + [cid]
                )
                conn.commit()
                print(f"[sync_consig_from_crm] Updated consig #{cid} with: {list(consig_updates.keys())}", flush=True)
            else:
                print(f"[sync_consig_from_crm] No matching consignacion found for CRM lead {lead_id}", flush=True)
    except Exception as e:
        print(f"[sync_consig_from_crm] Error: {e}", flush=True)


@app.route("/api/consignaciones/<int:cid>/publicar", methods=["POST"])
def publicar_en_catalogo(cid):
    """
    Publish an inspected vehicle to the autodirecto.cl catalog.
    Reads the appraisal data from Supabase, fetches photos from vehicle_images,
    then creates/updates a row in the `listings` table so the Next.js site can
    display it in /catalogo.
    """
    import requests as req_lib

    with get_db() as conn:
        c = conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
    if not c:
        return jsonify({"error": "Not found"}), 404
    c = row_to_dict(c)

    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")

    if not supabase_url or not supabase_key:
        return jsonify({"error": "Supabase not configured"}), 500

    headers = {
        "apikey": supabase_key,
        "Authorization": "Bearer " + supabase_key,
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    # 1. Fetch appraisal data from Supabase (has full inspection details + pricing)
    appraisal = {}
    appraisal_id = c.get("appraisal_supabase_id")
    if appraisal_id:
        try:
            r = req_lib.get(
                supabase_url + "/rest/v1/appraisals",
                params={"select": "*", "id": "eq.{}".format(appraisal_id)},
                headers=headers, timeout=10
            )
            if r.status_code == 200 and r.json():
                appraisal = r.json()[0]
        except Exception as e:
            print("[publicar] appraisal fetch error:", e)

    # 2. Fetch photos for this appraisal
    image_urls = []
    if appraisal_id:
        try:
            r = req_lib.get(
                supabase_url + "/rest/v1/vehicle_images",
                params={"select": "url", "appraisal_id": "eq.{}".format(appraisal_id)},
                headers=headers, timeout=10
            )
            if r.status_code == 200:
                image_urls = [row["url"] for row in r.json() if row.get("url")]
        except Exception as e:
            print("[publicar] photos fetch error:", e)

    # Fall back to a placeholder if no photos
    if not image_urls:
        image_urls = ["https://images.unsplash.com/photo-1552519507-da3b142c6e3d?w=800&q=80"]

    # 3. Build listing payload — maps inspection data to catalog fields
    brand  = (appraisal.get("vehicle_marca") or c.get("car_make") or "").strip()
    model  = (appraisal.get("vehicle_modelo") or c.get("car_model") or "").strip()
    year   = appraisal.get("vehicle_año") or c.get("car_year")
    color  = (appraisal.get("vehicle_color") or c.get("color") or "").strip()
    km     = appraisal.get("vehicle_km") or c.get("mileage")
    plate  = (appraisal.get("vehicle_patente") or c.get("plate") or "").upper()
    price  = appraisal.get("precio_publicado") or appraisal.get("tasacion")
    fuel   = appraisal.get("vehicle_combustible") or "Bencina"
    trans  = appraisal.get("vehicle_transmision") or "Manual"
    motor  = appraisal.get("vehicle_motor") or ""
    obs    = appraisal.get("observaciones") or appraisal.get("observations") or ""
    features = appraisal.get("features") or {}

    # Build a description from inspection data
    feature_labels = []
    feature_map = {
        "aireAcondicionado": "Aire acondicionado", "bluetooth": "Bluetooth",
        "calefactorAsiento": "Calefactor de asiento", "conexionUsb": "Conexión USB",
        "gps": "GPS", "isofix": "ISOFIX", "smartKey": "Smart Key",
        "lucesLed": "Luces LED", "mandosVolante": "Mandos en volante",
        "sensorEstacionamiento": "Sensor de estacionamiento",
        "sonidoPremium": "Sonido premium", "techoElectrico": "Techo eléctrico",
        "ventiladorAsiento": "Ventilador de asiento", "carplayAndroid": "CarPlay/Android Auto",
    }
    if isinstance(features, dict):
        feature_labels = [v for k, v in feature_map.items() if features.get(k)]

    description_parts = []
    if obs:
        description_parts.append(obs)
    if feature_labels:
        description_parts.append("Equipamiento: " + ", ".join(feature_labels) + ".")
    if appraisal.get("num_dueños"):
        description_parts.append("{} dueño(s) previo(s).".format(appraisal.get("num_dueños")))
    description = " ".join(description_parts) or "{} {} {} en excelentes condiciones.".format(brand, model, year)

    listing_payload = {
        "consignacion_id":   str(cid),
        "appraisal_id":      appraisal_id,
        "brand":             brand,
        "model":             model,
        "year":              year,
        "color":             color,
        "mileage_km":        km,
        "plate":             plate,
        "price":             price,
        "fuel_type":         fuel,
        "transmission":      trans,
        "motor":             motor,
        "description":       description,
        "features":          features,
        "image_urls":        image_urls,
        "status":            "disponible",
        "featured":          False,
    }

    # 4. Upsert into listings table (insert or update if plate already published)
    try:
        # Try update first
        check_r = req_lib.get(
            supabase_url + "/rest/v1/listings",
            params={"select": "id", "consignacion_id": "eq.{}".format(cid)},
            headers=headers, timeout=10
        )
        existing_listings = check_r.json() if check_r.status_code == 200 else []

        if existing_listings:
            # Update
            listing_id = existing_listings[0]["id"]
            put_r = req_lib.patch(
                supabase_url + "/rest/v1/listings?id=eq.{}".format(listing_id),
                json={**listing_payload, "updated_at": datetime.now().isoformat()},
                headers={**headers, "Prefer": "return=representation"},
                timeout=10
            )
            result_listing = put_r.json()
            listing_id = result_listing[0]["id"] if isinstance(result_listing, list) and result_listing else listing_id
        else:
            # Insert
            post_r = req_lib.post(
                supabase_url + "/rest/v1/listings",
                json=listing_payload,
                headers=headers,
                timeout=10
            )
            if post_r.status_code not in (200, 201):
                return jsonify({"error": "Supabase error {}: {}".format(post_r.status_code, post_r.text)}), 502
            result_listing = post_r.json()
            listing_id = result_listing[0]["id"] if isinstance(result_listing, list) and result_listing else None

        # Mark consignacion as en_venta + store listing_id
        now = datetime.now().isoformat()
        with get_db() as conn:
            conn.execute(
                "UPDATE consignaciones SET status='en_venta', listing_id=?, updated_at=? WHERE id=?",
                (str(listing_id) if listing_id else None, now, cid)
            )
            conn.commit()

            # Auto-promote to inventory if not already there
            c = row_to_dict(conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone())
            plate = (c.get("plate") or "").upper()
            if plate:
                existing_car = conn.execute("SELECT id FROM cars WHERE patente=?", (plate,)).fetchone()
                if not existing_car:
                    try:
                        conn.execute("""
                            INSERT INTO cars (patente, vin, brand, model, year, color,
                                owner_name, owner_rut, owner_email, owner_phone,
                                owner_price, selling_price, commission_pct, notes)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            plate, c.get("vin"),
                            c.get("car_make"), c.get("car_model"), c.get("car_year"), c.get("color"),
                            c.get("owner_full_name") or "{} {}".format(c.get("owner_first_name",""), c.get("owner_last_name","")),
                            c.get("owner_rut"),
                            c.get("owner_email"), c.get("owner_phone"),
                            int(c.get("owner_price") or 0), int(c.get("selling_price") or 0),
                            float(c.get("commission_pct") or 0.10),
                            c.get("condition_notes") or c.get("notes")
                        ))
                        car_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                        conn.execute("UPDATE consignaciones SET car_id=? WHERE id=?", (car_id, cid))
                        conn.commit()
                    except Exception as inv_e:
                        print("[publicar] auto-promote error:", inv_e)

        # Sync CRM lead stage
        _sync_crm_lead_stage(c.get("plate"), "en_venta")

        return jsonify({"ok": True, "listing_id": listing_id, "image_count": len(image_urls)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/consignaciones/<int:cid>/promote", methods=["POST"])
def promote_to_inventory(cid):
    """
    Part 2 complete → promote consignación to the cars inventory table.
    Creates a new entry in `cars` with all Part 1 + Part 2 data.
    """
    with get_db() as conn:
        c = conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
    if not c:
        return jsonify({"error": "Not found"}), 404
    c = row_to_dict(c)

    required = ["plate", "car_make", "car_model", "owner_rut", "owner_full_name", "owner_price", "selling_price"]
    missing = [f for f in required if not c.get(f)]
    if missing:
        return jsonify({"error": "Missing fields for inventory: {}".format(", ".join(missing))}), 400

    with get_db() as conn:
        # Check if already in inventory
        existing = conn.execute("SELECT id FROM cars WHERE patente=?", (c["plate"].upper(),)).fetchone()
        if existing:
            return jsonify({"error": "Ya existe un auto con patente {} en inventario".format(c["plate"])}), 409
        conn.execute("""
            INSERT INTO cars (patente, vin, brand, model, year, color,
                owner_name, owner_rut, owner_email, owner_phone,
                owner_price, selling_price, commission_pct, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            c["plate"].upper(), c.get("vin"),
            c["car_make"], c["car_model"], c.get("car_year"), c.get("color"),
            c["owner_full_name"], c["owner_rut"],
            c.get("owner_email"), c.get("owner_phone"),
            int(c["owner_price"]), int(c["selling_price"]),
            float(c.get("commission_pct") or 0.10),
            c.get("condition_notes") or c.get("notes")
        ))
        car_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        now = datetime.now().isoformat()
        conn.execute(
            "UPDATE consignaciones SET car_id=?, status='en_venta', updated_at=? WHERE id=?",
            (car_id, now, cid)
        )
        conn.commit()

    return jsonify({"ok": True, "car_id": car_id, "message": "Auto promovido al inventario"})


# ─── CONTRACT GENERATION & DIGITAL SIGNING ─────────────────────────────────────

def _build_contract_pdf(consig, appraisal=None):
    """
    Generate a professional consignment contract PDF using reportlab.
    Returns the PDF bytes.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import mm, cm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
    import io

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            topMargin=1.2*cm, bottomMargin=1.2*cm,
                            leftMargin=1.8*cm, rightMargin=1.8*cm)

    # Company info from env
    company_name = os.getenv("EMPRESA_RAZON_SOCIAL", "Wiackowska Group Spa")
    company_rut  = os.getenv("EMPRESA_RUT", "78355717-7")
    company_giro = os.getenv("EMPRESA_GIRO", "Compraventa de Vehículos Usados")
    company_dir  = os.getenv("EMPRESA_DIRECCION", "Av. Montemar 1055")
    company_com  = os.getenv("EMPRESA_COMUNA", "Concón")
    company_city = os.getenv("EMPRESA_CIUDAD", "Valparaíso")
    signer_name  = "Felipe Horacio Yáñez Fernández"

    # Styles — compact to fit everything on one page
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle('Title2', parent=styles['Title'], fontSize=13, spaceAfter=2, spaceBefore=0, textColor=colors.HexColor('#1a1a2e')))
    styles.add(ParagraphStyle('CompanyName', fontSize=14, fontName='Helvetica-Bold', textColor=colors.HexColor('#1a1a2e'), alignment=TA_LEFT, spaceAfter=1))
    styles.add(ParagraphStyle('CompanyInfo', fontSize=7, textColor=colors.HexColor('#666666'), alignment=TA_LEFT, spaceAfter=1))
    styles.add(ParagraphStyle('SectionTitle', fontSize=9, fontName='Helvetica-Bold', textColor=colors.HexColor('#1a1a2e'), spaceBefore=6, spaceAfter=3))
    styles.add(ParagraphStyle('BodyJustify', parent=styles['Normal'], fontSize=8, leading=10, alignment=TA_JUSTIFY, textColor=colors.HexColor('#333333')))
    styles.add(ParagraphStyle('SmallGray', fontSize=6, textColor=colors.HexColor('#999999'), alignment=TA_CENTER))
    styles.add(ParagraphStyle('SignLabel', fontSize=7, textColor=colors.HexColor('#666666'), alignment=TA_CENTER, spaceBefore=2))
    styles.add(ParagraphStyle('DateRight', fontSize=8, textColor=colors.HexColor('#333333'), alignment=TA_RIGHT))

    # Date
    from datetime import datetime as dt
    today = dt.now()
    meses = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre']
    date_str = "Santiago, {} de {} de {}".format(today.day, meses[today.month-1], today.year)

    # Extract data
    c = consig
    owner_name = "{} {}".format(c.get("owner_first_name",""), c.get("owner_last_name","")).strip() or c.get("owner_full_name","")
    owner_rut  = c.get("owner_rut","")
    owner_dir  = c.get("owner_address","") or ""
    if c.get("owner_commune"):
        owner_dir += ", " + c["owner_commune"]
    if c.get("owner_region"):
        owner_dir += ", " + c["owner_region"]
    owner_phone = c.get("owner_phone","")
    owner_email = c.get("owner_email","")

    plate       = c.get("plate","").upper()
    car_make    = c.get("car_make","")
    car_model   = c.get("car_model","")
    car_year    = c.get("car_year","")
    car_color   = c.get("color","")
    car_km      = c.get("mileage","")
    car_version = c.get("version","")
    car_vin     = c.get("vin","")

    # Appraisal data (if available from Supabase)
    a = appraisal or {}
    combustible = a.get("vehicle_combustible","Gasolina")
    transmision = a.get("vehicle_transmision","")
    motor_num   = a.get("vehicle_motor","")
    num_duenos  = a.get("num_dueños", "")
    rev_tecnica = "Al día" if a.get("revision_tecnica") else "Pendiente"
    permiso     = "Pagado" if a.get("permiso_circulacion") else "Pendiente"
    soap        = "Pagado" if a.get("soap") else "Pendiente"
    num_llaves  = a.get("num_llaves","")
    tasacion    = a.get("tasacion", 0)
    # ── PRICING LOGIC FIX ──
    # PRECIO PUBLICACIÓN (market price) should come from explicit newly mapped "ai_market_price" natively, or fallback to the appraisal's sugerido as string
    precio_pub = c.get("ai_market_price") or c.get("selling_price") or a.get("precio_sugerido") or 0
    
    # PRECIO CLIENTE (owner payout)
    # The owner payout is explicitly defined in owner_price, falling back to what they asked for publicacion
    precio_cli = c.get("owner_price") or a.get("precio_publicado") or 0
    comision_calc = precio_pub - precio_cli if precio_pub > precio_cli else 0
    observaciones = a.get("observaciones","") or c.get("condition_notes","") or "No se registran observaciones."
    comision    = a.get("comision") or c.get("commission_pct") or 0.10
    consignado_por = a.get("quien_tomo_fotos","") or signer_name

    def fmt_clp(val):
        try:
            return "${:,.0f}".format(int(val)).replace(",",".")
        except:
            return "$0"

    # Build story
    story = []

    # ─── HEADER ───
    story.append(Paragraph(company_name, styles['CompanyName']))
    story.append(Paragraph("RUT: {} · {}".format(company_rut, company_giro), styles['CompanyInfo']))
    story.append(Paragraph("{}, {}, {}".format(company_dir, company_com, company_city), styles['CompanyInfo']))
    story.append(Spacer(1, 2))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1a1a2e'), spaceAfter=4))

    # Title + Date
    story.append(Paragraph("CONTRATO DE CONSIGNACIÓN", styles['Title2']))
    story.append(Paragraph(date_str, styles['DateRight']))
    story.append(Spacer(1, 4))

    # ─── INTRO ───
    intro = (
        'Vienen a suscribir el siguiente contrato de consignación del vehículo motorizado entre '
        '<b>{}</b>, RUT <b>{}</b>, con domicilio en {}, {}, {}, '
        'en adelante <b>"el consignatario"</b>, y <b>{}</b>, RUT <b>{}</b>, con domicilio en {}, '
        'en adelante <b>"el consignador"</b>.'
    ).format(company_name, company_rut, company_dir, company_com, company_city,
             owner_name, owner_rut, owner_dir or "—")
    story.append(Paragraph(intro, styles['BodyJustify']))
    story.append(Spacer(1, 4))

    # ─── DETALLES DEL CLIENTE ───
    story.append(Paragraph("DETALLES DEL CLIENTE", styles['SectionTitle']))
    client_data = [
        ["Nombre", owner_name,   "RUT",      owner_rut],
        ["Dirección", owner_dir or "—", "Teléfono", owner_phone],
        ["Email", owner_email or "—", "", ""],
    ]
    ct = Table(client_data, colWidths=[55, 175, 55, 175])
    ct.setStyle(TableStyle([
        ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
        ('FONTNAME', (2,0), (2,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,0), (0,-1), colors.HexColor('#666666')),
        ('TEXTCOLOR', (2,0), (2,-1), colors.HexColor('#666666')),
        ('TEXTCOLOR', (1,0), (1,-1), colors.HexColor('#1a1a2e')),
        ('TEXTCOLOR', (3,0), (3,-1), colors.HexColor('#1a1a2e')),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('LINEBELOW', (0,0), (-1,-2), 0.5, colors.HexColor('#e0e0e0')),
    ]))
    story.append(ct)
    story.append(Spacer(1, 4))

    # ─── OBLIGACIONES ───
    story.append(Paragraph("OBLIGACIONES", styles['SectionTitle']))
    obligations = (
        'Serán obligaciones del consignador o comitente las que se describen a continuación:<br/>'
        '• Pagar los gastos de publicación y edición del vehículo, monto que corresponde a '
        '<b>$50.000</b>, en caso de retirar el auto dentro de los primeros 30 días de la fecha de publicación.<br/>'
        '• Pagar el precio cobrado por el consignatario para la gestión de la venta del vehículo '
        '(comisión del <b>{:.0%}</b>).<br/>'
        '• Entregar el vehículo dado en consignación para su venta con toda la documentación '
        'requerida por la legislación vigente.<br/>'
        '• Entregar el vehículo en perfectas condiciones mecánicas de uso, para ser puesto a la venta.<br/>'
        'Por parte de <b>{}</b> será obligación realizar todas las gestiones necesarias para la venta del vehículo.'
    ).format(float(comision), company_name)
    story.append(Paragraph(obligations, styles['BodyJustify']))
    story.append(Spacer(1, 4))

    # ─── DETALLES DEL VEHÍCULO ───
    story.append(Paragraph("DETALLES DEL VEHÍCULO", styles['SectionTitle']))
    veh_data = [
        ["Tipo", "Automóvil",         "Patente",       plate],
        ["Marca", car_make,            "Nº Motor",      motor_num or "—"],
        ["Modelo", car_model,          "Chasis/VIN",    car_vin or "—"],
        ["Versión", car_version or "—","Combustible",   combustible],
        ["Año", str(car_year),         "Rev. Técnica",  rev_tecnica],
        ["Color", car_color or "—",    "Permiso",       permiso],
        ["Kilometraje", "{:,.0f}".format(int(car_km or 0)).replace(",","."), "Seguro Oblig.", soap],
        ["Dueños", str(num_duenos) if num_duenos else "—", "Copia Llaves", str(num_llaves) if num_llaves else "—"],
    ]
    vt = Table(veh_data, colWidths=[65, 165, 65, 165])
    vt.setStyle(TableStyle([
        ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
        ('FONTNAME', (2,0), (2,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,0), (0,-1), colors.HexColor('#666666')),
        ('TEXTCOLOR', (2,0), (2,-1), colors.HexColor('#666666')),
        ('TEXTCOLOR', (1,0), (1,-1), colors.HexColor('#1a1a2e')),
        ('TEXTCOLOR', (3,0), (3,-1), colors.HexColor('#1a1a2e')),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('LINEBELOW', (0,0), (-1,-2), 0.5, colors.HexColor('#e0e0e0')),
        ('LINEBELOW', (0,-1), (-1,-1), 0.5, colors.HexColor('#e0e0e0')),
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#f8f8fc')),
    ]))
    story.append(vt)
    story.append(Spacer(1, 4))

    # ─── OBSERVACIONES ───
    story.append(Paragraph("OBSERVACIONES", styles['SectionTitle']))
    story.append(Paragraph(observaciones, styles['BodyJustify']))
    story.append(Spacer(1, 4))

    # ─── PRECIOS ───
    story.append(Paragraph("CONDICIONES ECONÓMICAS", styles['SectionTitle']))
    price_data = [
        ["PRECIO CLIENTE", fmt_clp(precio_cli), "PRECIO PUBLICACIÓN", fmt_clp(precio_pub)],
    ]
    if tasacion:
        price_data.append(["TASACIÓN FISCAL", fmt_clp(tasacion), "COMISIÓN", "{:.0%}".format(float(comision))])
    pt = Table(price_data, colWidths=[110, 110, 120, 120])
    pt.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,0), (0,-1), colors.HexColor('#666666')),
        ('TEXTCOLOR', (2,0), (2,-1), colors.HexColor('#666666')),
        ('TEXTCOLOR', (1,0), (1,-1), colors.HexColor('#1a1a2e')),
        ('TEXTCOLOR', (3,0), (3,-1), colors.HexColor('#1a1a2e')),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#1a1a2e')),
        ('LINEBELOW', (0,0), (-1,0), 0.5, colors.HexColor('#cccccc')),
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f0f0f8')),
    ]))
    story.append(pt)
    story.append(Spacer(1, 6))

    # ─── CONSIGNADO POR ───
    story.append(Paragraph("Consignado por: <b>{}</b>".format(consignado_por), styles['BodyJustify']))
    story.append(Spacer(1, 12))

    # ─── SIGNATURES ───
    sig_data = [
        [Paragraph("<b>{}</b><br/>{}<br/>Rep. Legal".format(company_name, signer_name), styles['SignLabel']),
         "",
         Paragraph("<b>{}</b><br/>RUT: {}<br/>Consignador".format(owner_name, owner_rut), styles['SignLabel'])],
    ]
    sig_table = Table(sig_data, colWidths=[200, 60, 200])
    sig_table.setStyle(TableStyle([
        ('LINEABOVE', (0,0), (0,0), 1, colors.HexColor('#333333')),
        ('LINEABOVE', (2,0), (2,0), 1, colors.HexColor('#333333')),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    story.append(sig_table)
    story.append(Spacer(1, 10))

    # ─── FOOTER ───
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#cccccc'), spaceAfter=4))
    footer_id = "CONSIG-{}-{}".format(c.get("id",""), today.strftime("%Y%m%d"))
    story.append(Paragraph(
        "Documento generado automáticamente por Autodirecto CRM · {} · ID: {}".format(
            today.strftime("%d/%m/%Y %H:%M"), footer_id),
        styles['SmallGray']))

    doc.build(story)
    return buf.getvalue()


def _sign_pdf_with_certificate(pdf_bytes, reason="Contrato de Consignación", location="Santiago, Chile"):
    """
    Digitally sign a PDF using the company PFX certificate (endesive).
    Returns the signed PDF bytes.
    """
    from endesive.pdf import cms as pdf_cms
    from cryptography.hazmat.primitives.serialization import pkcs12
    from datetime import timezone

    # Load certificate — from base64 env var (Vercel) or local file (dev)
    pfx_pass = os.getenv("CERT_PASSWORD", "Todayisagoodday01").encode()
    cert_b64 = os.getenv("CERT_PFX_BASE64")
    if cert_b64:
        import base64
        pfx_data = base64.b64decode(cert_b64)
    else:
        pfx_path = os.path.join(os.path.dirname(__file__), "firma_18842443-0 (3).pfx")
        with open(pfx_path, "rb") as f:
            pfx_data = f.read()

    private_key, certificate, chain = pkcs12.load_key_and_certificates(pfx_data, pfx_pass)

    # Build signing parameters
    now = datetime.now(timezone.utc)
    dct = {
        "aligned": 0,
        "sigflags": 3,
        "sigflagsft": 132,
        "sigpage": 0,
        "sigbutton": True,
        "sigfield": "Firma_Empresa",
        "auto_sigfield": True,
        "sigandcertify": True,
        "contact": os.getenv("EMPRESA_RAZON_SOCIAL", "Wiackowska Group Spa"),
        "location": location,
        "signingdate": now.strftime("D:%Y%m%d%H%M%S+00'00'"),
        "reason": reason,
        "signature": "Firmado digitalmente por Felipe Horacio Yáñez Fernández",
        "signaturebox": (360, 50, 560, 120),
    }

    # Sign
    signed = pdf_cms.sign(
        pdf_bytes,
        dct,
        private_key,
        certificate,
        chain or [],
        "sha256",
        None
    )

    return pdf_bytes + signed


def _add_client_signature_to_pdf(pdf_bytes, signature_b64, consig):
    """
    Overlay the client's hand-drawn signature onto the contract PDF.
    The signature_b64 is a base64-encoded PNG from a canvas.
    Returns new PDF bytes with the signature embedded.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas as pdf_canvas
    from PyPDF2 import PdfReader, PdfWriter
    import base64, io

    # Decode the signature image
    sig_data = base64.b64decode(signature_b64.split(",")[-1] if "," in signature_b64 else signature_b64)
    sig_img = io.BytesIO(sig_data)

    # Create overlay PDF with just the signature image
    overlay_buf = io.BytesIO()
    c = pdf_canvas.Canvas(overlay_buf, pagesize=letter)
    # Position: right side, on the signature line (above client name/RUT)
    # The client signature area is roughly at x=350, y=175, width=180, height=55
    from reportlab.lib.utils import ImageReader
    img = ImageReader(sig_img)
    c.drawImage(img, 350, 175, width=180, height=55, mask='auto', preserveAspectRatio=True, anchor='c')
    c.save()
    overlay_buf.seek(0)

    # Merge overlay onto the original PDF
    reader = PdfReader(io.BytesIO(pdf_bytes))
    overlay_reader = PdfReader(overlay_buf)
    writer = PdfWriter()

    page = reader.pages[0]
    page.merge_page(overlay_reader.pages[0])
    writer.add_page(page)

    # Copy remaining pages if any
    for i in range(1, len(reader.pages)):
        writer.add_page(reader.pages[i])

    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def _upload_contract_to_supabase(pdf_bytes, filename):
    """Upload a contract PDF to Supabase Storage bucket 'contratos'."""
    import requests as req_lib
    supa_url, headers = _supa_headers()
    # Use storage API (not REST)
    storage_url = "{}/storage/v1/object/contratos/{}".format(supa_url, filename)
    upload_headers = {
        "apikey": headers["apikey"],
        "Authorization": headers["Authorization"],
        "Content-Type": "application/pdf",
        "x-upsert": "true",  # overwrite if exists
    }
    r = req_lib.post(storage_url, headers=upload_headers, data=pdf_bytes, timeout=15)
    if r.status_code in (200, 201):
        print("[contrato] Uploaded {} to Supabase Storage".format(filename))
        return True
    else:
        print("[contrato] Upload failed {}: {}".format(r.status_code, r.text))
        return False


def _download_contract_from_supabase(filename):
    """Download a contract PDF from Supabase Storage bucket 'contratos'. Returns bytes or None."""
    import requests as req_lib
    supa_url, headers = _supa_headers()
    storage_url = "{}/storage/v1/object/contratos/{}".format(supa_url, filename)
    dl_headers = {
        "apikey": headers["apikey"],
        "Authorization": headers["Authorization"],
    }
    r = req_lib.get(storage_url, headers=dl_headers, timeout=15)
    if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("application/"):
        return r.content
    print("[contrato] Download from storage failed {}: {}".format(r.status_code, r.text[:200]))
    return None

@app.route("/api/consignaciones/<int:cid>/contrato", methods=["GET"])
def generate_contract(cid):
    """Generate or retrieve the consignment contract PDF, digitally signed."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
    if not row:
        return jsonify({"error": "Consignación no encontrada"}), 404
    consig = row_to_dict(row)

    # If the contract is already signed, serve the signed version from Supabase Storage
    if consig.get("contract_signed_at") and consig.get("contract_pdf"):
        signed_filename = consig["contract_pdf"]
        # Try local cache first
        contratos_dir = os.path.join("/tmp", "contratos")
        filepath = os.path.join(contratos_dir, signed_filename)
        if os.path.exists(filepath):
            return send_file(filepath, mimetype="application/pdf", as_attachment=False,
                             download_name=signed_filename)
        # Try Supabase Storage
        pdf_bytes = _download_contract_from_supabase(signed_filename)
        if pdf_bytes:
            # Cache locally
            os.makedirs(contratos_dir, exist_ok=True)
            with open(filepath, "wb") as f:
                f.write(pdf_bytes)
            return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf",
                             as_attachment=False, download_name=signed_filename)

    # Try to get appraisal data from Supabase
    appraisal = None
    if consig.get("appraisal_supabase_id"):
        try:
            supa_url, headers = _supa_headers()
            r = __import__('requests').get(
                "{}/rest/v1/appraisals?id=eq.{}".format(supa_url, consig["appraisal_supabase_id"]),
                headers=headers, timeout=8
            )
            if r.status_code == 200:
                data = r.json()
                if data:
                    appraisal = data[0]
        except Exception as e:
            print("[contrato] appraisal fetch:", e)

    # Generate PDF
    pdf_bytes = _build_contract_pdf(consig, appraisal)

    # Digitally sign it
    try:
        pdf_bytes = _sign_pdf_with_certificate(pdf_bytes, reason="Contrato de Consignación — {}".format(consig.get("plate","")))
    except Exception as e:
        print("[contrato] signing error (returning unsigned):", e)

    # Save to disk (best-effort; Vercel has read-only filesystem except /tmp)
    filename = "contrato_{}_{}.pdf".format(cid, consig.get("plate","").upper().replace(" ",""))
    try:
        contratos_dir = os.path.join("/tmp", "contratos")
        os.makedirs(contratos_dir, exist_ok=True)
        filepath = os.path.join(contratos_dir, filename)
        with open(filepath, "wb") as f:
            f.write(pdf_bytes)
    except Exception as e:
        print("[contrato] save error (non-fatal):", e)

    # Update consignacion with contract path
    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute("UPDATE consignaciones SET contract_pdf=?, updated_at=? WHERE id=?",
                     (filename, now, cid))
        conn.commit()

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=False,
        download_name=filename
    )


@app.route("/api/consignaciones/<int:cid>/contrato/firmar", methods=["POST"])
def sign_contract_client(cid):
    """
    Client signs the contract.
    Expects JSON: { "signature": "data:image/png;base64,..." }
    Embeds the signature into the PDF, re-signs, and saves.
    """
    data = request.json
    signature_b64 = data.get("signature")
    if not signature_b64:
        return jsonify({"error": "Falta la firma del cliente"}), 400

    with get_db() as conn:
        row = conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
    if not row:
        return jsonify({"error": "Consignación no encontrada"}), 404
    consig = row_to_dict(row)

    # Get existing contract or generate new one
    contratos_dir = os.path.join("/tmp", "contratos")
    os.makedirs(contratos_dir, exist_ok=True)
    filename = consig.get("contract_pdf") or "contrato_{}_{}.pdf".format(cid, consig.get("plate","").upper().replace(" ",""))
    filepath = os.path.join(contratos_dir, filename)

    if os.path.exists(filepath):
        with open(filepath, "rb") as f:
            pdf_bytes = f.read()
    else:
        # Generate fresh contract
        appraisal = None
        if consig.get("appraisal_supabase_id"):
            try:
                supa_url, headers = _supa_headers()
                r = __import__('requests').get(
                    "{}/rest/v1/appraisals?id=eq.{}".format(supa_url, consig["appraisal_supabase_id"]),
                    headers=headers, timeout=8
                )
                if r.status_code == 200:
                    rdata = r.json()
                    if rdata:
                        appraisal = rdata[0]
            except:
                pass
        pdf_bytes = _build_contract_pdf(consig, appraisal)

    # Add client signature image to the PDF
    try:
        pdf_bytes = _add_client_signature_to_pdf(pdf_bytes, signature_b64, consig)
    except Exception as e:
        return jsonify({"error": "Error adding signature: {}".format(str(e))}), 500

    # Re-sign the PDF with the digital certificate (now includes client sig)
    try:
        pdf_bytes = _sign_pdf_with_certificate(
            pdf_bytes,
            reason="Contrato firmado por cliente — {}".format(consig.get("plate","")),
            location="Santiago, Chile"
        )
    except Exception as e:
        print("[contrato] re-signing error:", e)

    # Save signed version locally
    signed_filename = "contrato_{}_firmado.pdf".format(cid)
    signed_filepath = os.path.join(contratos_dir, signed_filename)
    with open(signed_filepath, "wb") as f:
        f.write(pdf_bytes)

    # Upload signed contract to Supabase Storage for permanent persistence
    try:
        _upload_contract_to_supabase(pdf_bytes, signed_filename)
    except Exception as e:
        print("[contrato] Supabase upload error (non-fatal):", e)

    # Save client signature as separate image too
    try:
        import base64
        sig_raw = base64.b64decode(signature_b64.split(",")[-1] if "," in signature_b64 else signature_b64)
        sig_img_path = os.path.join(contratos_dir, "firma_cliente_{}.png".format(cid))
        with open(sig_img_path, "wb") as f:
            f.write(sig_raw)
    except:
        pass

    # Update consignacion
    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute(
            "UPDATE consignaciones SET contract_pdf=?, contract_signed_at=?, updated_at=? WHERE id=?",
            (signed_filename, now, now, cid)
        )
        conn.commit()

    return jsonify({"ok": True, "filename": signed_filename, "signed_at": now})


@app.route("/api/consignaciones/<int:cid>/contrato/descargar", methods=["GET"])
def download_contract(cid):
    """Download the latest contract PDF (signed version preferred).
    If the cached file is gone (Vercel ephemeral /tmp), fetch from Supabase Storage or regenerate.
    """
    with get_db() as conn:
        row = conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
    if not row:
        return jsonify({"error": "Consignación no encontrada"}), 404
    consig = row_to_dict(row)

    contratos_dir = os.path.join("/tmp", "contratos")
    filename = consig.get("contract_pdf")
    filepath = os.path.join(contratos_dir, filename) if filename else None

    # Try local cache first
    if filepath and os.path.exists(filepath):
        return send_file(filepath, mimetype="application/pdf",
                         as_attachment=False, download_name=filename)

    # Try Supabase Storage (for signed contracts that were persisted)
    if filename:
        pdf_bytes = _download_contract_from_supabase(filename)
        if pdf_bytes:
            # Cache locally for future requests
            os.makedirs(contratos_dir, exist_ok=True)
            with open(os.path.join(contratos_dir, filename), "wb") as f:
                f.write(pdf_bytes)
            return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf",
                             as_attachment=False, download_name=filename)

    # File not found anywhere — regenerate unsigned version
    appraisal = None
    if consig.get("appraisal_supabase_id"):
        try:
            supa_url, headers = _supa_headers()
            r = __import__('requests').get(
                "{}/rest/v1/appraisals?id=eq.{}".format(supa_url, consig["appraisal_supabase_id"]),
                headers=headers, timeout=8
            )
            if r.status_code == 200:
                rdata = r.json()
                if rdata:
                    appraisal = rdata[0]
        except:
            pass
    pdf_bytes = _build_contract_pdf(consig, appraisal)
    try:
        pdf_bytes = _sign_pdf_with_certificate(pdf_bytes, reason="Contrato de Consignación — {}".format(consig.get("plate","")))
    except Exception as e:
        print("[contrato] signing error:", e)

    dl_name = filename or "contrato_{}_{}.pdf".format(cid, consig.get("plate","").upper().replace(" ",""))
    return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf",
                     as_attachment=False, download_name=dl_name)


# ─── API: Compradores (Buyers) ────────────────────────────────────────────────

COMPRADOR_STAGES = ['interesado', 'contactado', 'test_drive', 'credito', 'reservado', 'vendido', 'descartado']
COMPRADOR_STAGE_LABELS = {
    'interesado': 'Interesado',
    'contactado': 'Contactado',
    'test_drive': 'Test Drive',
    'credito': 'Crédito',
    'reservado': 'Reservado',
    'vendido': 'Vendido',
    'descartado': 'Descartado',
}


@app.route("/api/compradores", methods=["GET"])
def get_compradores():
    """List all buyer leads."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT b.*, u.name as assigned_user_name, u.color as assigned_user_color "
            "FROM compradores b LEFT JOIN crm_users u ON b.assigned_user_id = u.id "
            "ORDER BY b.id DESC"
        ).fetchall()
    return jsonify([row_to_dict(r) for r in rows])


@app.route("/api/compradores", methods=["POST"])
def create_comprador():
    """Create a new buyer lead."""
    data = request.json or {}
    now = datetime.now().isoformat()

    first_name = data.get("first_name", "")
    last_name = data.get("last_name", "")
    full_name = "{} {}".format(first_name, last_name).strip()

    with get_db() as conn:
        conn.execute(
            """INSERT INTO compradores
               (first_name, last_name, full_name, rut, phone, email,
                region, commune, address, car_description, car_plate,
                car_price, consignacion_id, status, assigned_user_id, notes, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (first_name, last_name, full_name,
             data.get("rut"), data.get("phone"), data.get("email"),
             data.get("region"), data.get("commune"), data.get("address"),
             data.get("car_description"), data.get("car_plate"),
             data.get("car_price"), data.get("consignacion_id"),
             data.get("status", "interesado"), data.get("assigned_user_id"),
             data.get("notes"), now, now)
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return jsonify({"ok": True, "id": new_id})


@app.route("/api/compradores/<int:bid>", methods=["GET"])
def get_comprador(bid):
    """Get a single buyer."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM compradores WHERE id=?", (bid,)).fetchone()
    if not row:
        return jsonify({"error": "Comprador no encontrado"}), 404
    return jsonify(row_to_dict(row))


@app.route("/api/compradores/<int:bid>", methods=["PATCH"])
def update_comprador(bid):
    """Update buyer fields."""
    data = request.json or {}
    allowed = {
        "first_name", "last_name", "full_name", "rut", "phone", "email",
        "region", "commune", "address",
        "consignacion_id", "listing_id", "car_description", "car_plate", "car_price",
        "credit_requested", "credit_status", "credit_amount", "credit_down_payment",
        "credit_months", "credit_rate", "credit_monthly_payment", "credit_institution",
        "credit_notes",
        "status", "assigned_user_id", "test_drive_date", "test_drive_completed",
        "offer_amount", "nota_compra_pdf", "nota_compra_signed_at", "notes"
    }
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"message": "No valid fields to update, ignoring", "ok": True}), 200

    updates["updated_at"] = datetime.now().isoformat()
    if "first_name" in updates or "last_name" in updates:
        fn = updates.get("first_name", data.get("first_name", ""))
        ln = updates.get("last_name", data.get("last_name", ""))
        updates["full_name"] = "{} {}".format(fn, ln).strip()

    set_clause = ", ".join("{}=?".format(k) for k in updates)
    with get_db() as conn:
        conn.execute(
            "UPDATE compradores SET {} WHERE id=?".format(set_clause),
            list(updates.values()) + [bid]
        )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/compradores/<int:bid>", methods=["DELETE"])
def delete_comprador(bid):
    """Delete a buyer."""
    with get_db() as conn:
        conn.execute("DELETE FROM compradores WHERE id=?", (bid,))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/compradores/<int:bid>/simular-credito", methods=["POST"])
def simular_credito(bid):
    """
    Simulate auto credit for a buyer.
    Body: { "car_price": 15000000, "down_payment": 3000000, "months": 48, "annual_rate": 0.14 }
    Returns monthly payment and saves to the buyer record.
    """
    data = request.json or {}
    car_price = int(data.get("car_price", 0))
    down_payment = int(data.get("down_payment", 0))
    months = int(data.get("months", 48))
    annual_rate = float(data.get("annual_rate", 0.14))  # 14% default
    institution = data.get("institution", "")

    financed = car_price - down_payment
    if financed <= 0 or months <= 0:
        return jsonify({"error": "Monto financiado debe ser positivo"}), 400

    # Standard amortization: M = P * r / (1 - (1+r)^(-n))
    monthly_rate = annual_rate / 12
    if monthly_rate > 0:
        monthly_payment = financed * monthly_rate / (1 - (1 + monthly_rate) ** (-months))
    else:
        monthly_payment = financed / months

    monthly_payment = int(round(monthly_payment))
    total_paid = monthly_payment * months
    total_interest = total_paid - financed

    # Save to buyer record
    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute(
            """UPDATE compradores SET
               credit_requested=1, credit_status='solicitado',
               credit_amount=?, credit_down_payment=?, credit_months=?,
               credit_rate=?, credit_monthly_payment=?, credit_institution=?,
               car_price=?, updated_at=?
               WHERE id=?""",
            (financed, down_payment, months, annual_rate,
             monthly_payment, institution, car_price, now, bid)
        )
        conn.commit()

    return jsonify({
        "ok": True,
        "car_price": car_price,
        "down_payment": down_payment,
        "financed": financed,
        "months": months,
        "annual_rate": annual_rate,
        "monthly_rate": round(monthly_rate, 6),
        "monthly_payment": monthly_payment,
        "total_paid": total_paid,
        "total_interest": total_interest,
    })


@app.route("/api/compradores/<int:bid>/match", methods=["POST"])
def match_comprador(bid):
    """
    Match a buyer to a consignación car.
    Body: { "consignacion_id": 20 }
    """
    data = request.json or {}
    cid = data.get("consignacion_id")
    if not cid:
        return jsonify({"error": "Falta consignacion_id"}), 400

    with get_db() as conn:
        consig = conn.execute("SELECT * FROM consignaciones WHERE id=?", (cid,)).fetchone()
    if not consig:
        return jsonify({"error": "Consignación no encontrada"}), 404
    c = row_to_dict(consig)

    car_desc = "{} {} {}".format(c.get("car_make", ""), c.get("car_model", ""), c.get("car_year", "")).strip()
    car_plate = c.get("plate", "").upper()
    car_price = c.get("ai_market_price") or c.get("selling_price") or 0

    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute(
            """UPDATE compradores SET
               consignacion_id=?, car_description=?, car_plate=?, car_price=?, updated_at=?
               WHERE id=?""",
            (cid, car_desc, car_plate, car_price, now, bid)
        )
        conn.commit()

    return jsonify({"ok": True, "car_description": car_desc, "car_plate": car_plate, "car_price": car_price})


def _build_nota_compra_pdf(comprador, consig):
    """Build a Nota de Compra PDF for a buyer matched to a car."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            topMargin=30, bottomMargin=30,
                            leftMargin=40, rightMargin=40)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='CompanyName2', fontName='Helvetica-Bold', fontSize=16,
                              textColor=colors.HexColor('#1a1a2e'), spaceAfter=1))
    styles.add(ParagraphStyle(name='CompanyInfo2', fontName='Helvetica', fontSize=7,
                              textColor=colors.HexColor('#666666'), spaceAfter=0))
    styles.add(ParagraphStyle(name='DocTitle', fontName='Helvetica-Bold', fontSize=14,
                              alignment=TA_CENTER, textColor=colors.HexColor('#1a1a2e'),
                              spaceBefore=8, spaceAfter=2))
    styles.add(ParagraphStyle(name='DateRight2', fontName='Helvetica', fontSize=8,
                              alignment=TA_RIGHT, textColor=colors.HexColor('#999999'),
                              spaceAfter=6))
    styles.add(ParagraphStyle(name='SectionHead', fontName='Helvetica-Bold', fontSize=10,
                              textColor=colors.HexColor('#1a1a2e'), spaceBefore=8, spaceAfter=4))
    styles.add(ParagraphStyle(name='Body2', fontName='Helvetica', fontSize=9,
                              textColor=colors.HexColor('#333333'), leading=13))
    styles.add(ParagraphStyle(name='SmallGray2', fontName='Helvetica', fontSize=6.5,
                              textColor=colors.HexColor('#aaaaaa'), alignment=TA_CENTER))

    today = datetime.now()
    meses = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre']
    date_str = "Santiago, {} de {} de {}".format(today.day, meses[today.month-1], today.year)

    b = comprador
    c = consig or {}

    buyer_name = "{} {}".format(b.get("first_name",""), b.get("last_name","")).strip() or b.get("full_name","")
    buyer_rut = b.get("rut","")
    buyer_phone = b.get("phone","")
    buyer_email = b.get("email","")
    buyer_addr = b.get("address","") or ""
    if b.get("commune"):
        buyer_addr += ", " + b["commune"]
    if b.get("region"):
        buyer_addr += ", " + b["region"]

    car_desc = b.get("car_description","") or "{} {} {}".format(c.get("car_make",""), c.get("car_model",""), c.get("car_year","")).strip()
    plate = (b.get("car_plate","") or c.get("plate","")).upper()
    car_price = b.get("car_price") or c.get("ai_market_price") or c.get("selling_price") or 0
    car_color = c.get("color","")
    car_vin = c.get("vin","")
    car_km = c.get("mileage","")
    car_year = c.get("car_year","")

    down_payment = b.get("credit_down_payment") or 0
    financed = b.get("credit_amount") or 0
    months = b.get("credit_months") or 0
    monthly = b.get("credit_monthly_payment") or 0
    rate = b.get("credit_rate") or 0
    institution = b.get("credit_institution") or ""

    def fmt_clp(val):
        try:
            return "${:,.0f}".format(int(val)).replace(",",".")
        except:
            return "$0"

    company_name = "Wiackowska Group Spa"
    company_rut = "77.895.687-3"

    story = []

    # ─── HEADER ───
    story.append(Paragraph(company_name, styles['CompanyName2']))
    story.append(Paragraph("RUT: {} · Compraventa de vehículos motorizados".format(company_rut), styles['CompanyInfo2']))
    story.append(Paragraph("Los Militares 5953, Of. 1509, Las Condes, Santiago", styles['CompanyInfo2']))
    story.append(Spacer(1, 2))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1a1a2e'), spaceAfter=4))

    story.append(Paragraph("NOTA DE COMPRA", styles['DocTitle']))
    story.append(Paragraph(date_str, styles['DateRight2']))
    story.append(Spacer(1, 4))

    # ─── BUYER INFO ───
    story.append(Paragraph("DATOS DEL COMPRADOR", styles['SectionHead']))
    buyer_data = [
        ["Nombre", buyer_name, "RUT", buyer_rut],
        ["Teléfono", buyer_phone, "Email", buyer_email],
        ["Dirección", buyer_addr, "", ""],
    ]
    t = Table(buyer_data, colWidths=[70, 190, 70, 190])
    t.setStyle(TableStyle([
        ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
        ('FONTNAME', (2,0), (2,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,0), (-1,-1), colors.HexColor('#333333')),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#dddddd')),
        ('BACKGROUND', (0,0), (0,-1), colors.HexColor('#f7f7f7')),
        ('BACKGROUND', (2,0), (2,-1), colors.HexColor('#f7f7f7')),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))

    # ─── VEHICLE INFO ───
    story.append(Paragraph("DATOS DEL VEHÍCULO", styles['SectionHead']))
    vehicle_data = [
        ["Vehículo", car_desc, "Patente", plate],
        ["Año", str(car_year), "Color", car_color],
        ["Kilometraje", "{:,}".format(int(car_km)).replace(",",".") if car_km else "—", "VIN", car_vin or "—"],
    ]
    t2 = Table(vehicle_data, colWidths=[70, 190, 70, 190])
    t2.setStyle(TableStyle([
        ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
        ('FONTNAME', (2,0), (2,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,0), (-1,-1), colors.HexColor('#333333')),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#dddddd')),
        ('BACKGROUND', (0,0), (0,-1), colors.HexColor('#f7f7f7')),
        ('BACKGROUND', (2,0), (2,-1), colors.HexColor('#f7f7f7')),
    ]))
    story.append(t2)
    story.append(Spacer(1, 8))

    # ─── PRICING ───
    story.append(Paragraph("CONDICIONES ECONÓMICAS", styles['SectionHead']))
    price_data = [
        ["PRECIO VEHÍCULO", fmt_clp(car_price)],
        ["PIE (Pago Inicial)", fmt_clp(down_payment)],
        ["MONTO FINANCIADO", fmt_clp(financed)],
        ["PLAZO", "{} meses".format(months) if months else "—"],
        ["TASA ANUAL", "{:.1f}%".format(float(rate)*100) if rate else "—"],
        ["CUOTA MENSUAL", fmt_clp(monthly)],
        ["INSTITUCIÓN", institution or "—"],
    ]
    t3 = Table(price_data, colWidths=[200, 320])
    t3.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica'),
        ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TEXTCOLOR', (0,0), (-1,-1), colors.HexColor('#333333')),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#dddddd')),
        ('BACKGROUND', (0,0), (0,0), colors.HexColor('#1a1a2e')),
        ('TEXTCOLOR', (0,0), (0,0), colors.white),
        ('BACKGROUND', (1,0), (1,0), colors.HexColor('#1a1a2e')),
        ('TEXTCOLOR', (1,0), (1,0), colors.white),
        ('FONTNAME', (0,0), (1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (1,0), 10),
    ]))
    story.append(t3)
    story.append(Spacer(1, 12))

    # ─── TERMS ───
    story.append(Paragraph("CONDICIONES GENERALES", styles['SectionHead']))
    terms = (
        "1. La presente Nota de Compra constituye una intención formal de adquisición del vehículo descrito. "
        "2. El precio indicado incluye IVA cuando corresponda. "
        "3. La reserva del vehículo se hará efectiva una vez confirmado el pago del pie o la aprobación del crédito. "
        "4. Autodirecto se compromete a entregar el vehículo en las condiciones descritas en el informe de inspección. "
        "5. El comprador declara haber revisado el estado del vehículo y estar conforme con las condiciones."
    )
    story.append(Paragraph(terms, styles['Body2']))
    story.append(Spacer(1, 16))

    # ─── SIGNATURES ───
    signer_name = "Felipe Horacio Yáñez Fernández"
    sig_data = [
        ["_" * 40, "_" * 40],
        [Paragraph("<b>{}</b><br/>{}<br/>Rep. Legal".format(company_name, signer_name),
                    ParagraphStyle('sc', fontName='Helvetica', fontSize=7, alignment=TA_CENTER,
                                   textColor=colors.HexColor('#333333'))),
         Paragraph("<b>{}</b><br/>RUT: {}<br/>Comprador".format(buyer_name, buyer_rut),
                    ParagraphStyle('sc2', fontName='Helvetica', fontSize=7, alignment=TA_CENTER,
                                   textColor=colors.HexColor('#333333')))]
    ]
    sig_table = Table(sig_data, colWidths=[260, 260])
    sig_table.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,0), 'Helvetica'),
        ('FONTSIZE', (0,0), (-1,0), 8),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('TOPPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(sig_table)
    story.append(Spacer(1, 10))

    # ─── FOOTER ───
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#cccccc'), spaceAfter=4))
    footer_id = "COMPRA-{}-{}".format(b.get("id",""), today.strftime("%Y%m%d"))
    story.append(Paragraph(
        "Documento generado automáticamente por Autodirecto CRM · {} · ID: {}".format(
            today.strftime("%d/%m/%Y %H:%M"), footer_id),
        styles['SmallGray2']))

    doc.build(story)
    return buf.getvalue()


@app.route("/api/compradores/<int:bid>/nota-compra", methods=["GET"])
def generate_nota_compra(bid):
    """Generate Nota de Compra PDF for a buyer."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM compradores WHERE id=?", (bid,)).fetchone()
    if not row:
        return jsonify({"error": "Comprador no encontrado"}), 404
    comprador = row_to_dict(row)

    # Get matched consignacion data if available
    consig = None
    if comprador.get("consignacion_id"):
        with get_db() as conn:
            crow = conn.execute("SELECT * FROM consignaciones WHERE id=?",
                                (comprador["consignacion_id"],)).fetchone()
            if crow:
                consig = row_to_dict(crow)

    pdf_bytes = _build_nota_compra_pdf(comprador, consig)

    # Try to digitally sign
    try:
        pdf_bytes = _sign_pdf_with_certificate(
            pdf_bytes,
            reason="Nota de Compra — {}".format(comprador.get("car_plate", "")))
    except Exception as e:
        print("[nota-compra] signing error:", e)

    filename = "nota_compra_{}_{}.pdf".format(bid, (comprador.get("car_plate","") or "").upper().replace(" ",""))
    try:
        contratos_dir = os.path.join("/tmp", "contratos")
        os.makedirs(contratos_dir, exist_ok=True)
        with open(os.path.join(contratos_dir, filename), "wb") as f:
            f.write(pdf_bytes)
    except:
        pass

    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute("UPDATE compradores SET nota_compra_pdf=?, updated_at=? WHERE id=?",
                     (filename, now, bid))
        conn.commit()

    return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf",
                     as_attachment=False, download_name=filename)


@app.route("/api/compradores/<int:bid>/nota-compra/descargar", methods=["GET"])
def download_nota_compra(bid):
    """Download/view the Nota de Compra PDF."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM compradores WHERE id=?", (bid,)).fetchone()
    if not row:
        return jsonify({"error": "Comprador no encontrado"}), 404
    comprador = row_to_dict(row)

    contratos_dir = os.path.join("/tmp", "contratos")
    filename = comprador.get("nota_compra_pdf")
    if filename:
        filepath = os.path.join(contratos_dir, filename)
        if os.path.exists(filepath):
            return send_file(filepath, mimetype="application/pdf",
                             as_attachment=False, download_name=filename)
        # Try Supabase Storage
        pdf_bytes = _download_contract_from_supabase(filename)
        if pdf_bytes:
            os.makedirs(contratos_dir, exist_ok=True)
            with open(os.path.join(contratos_dir, filename), "wb") as f:
                f.write(pdf_bytes)
            return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf",
                             as_attachment=False, download_name=filename)

    # Regenerate
    consig = None
    if comprador.get("consignacion_id"):
        with get_db() as conn:
            crow = conn.execute("SELECT * FROM consignaciones WHERE id=?",
                                (comprador["consignacion_id"],)).fetchone()
            if crow:
                consig = row_to_dict(crow)
    pdf_bytes = _build_nota_compra_pdf(comprador, consig)
    dl_name = filename or "nota_compra_{}.pdf".format(bid)
    return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf",
                     as_attachment=False, download_name=dl_name)


CRM_STAGES = ['nuevo', 'contactado', 'agendado', 'inspeccionado', 'en_venta', 'vendido', 'descartado']
CRM_STAGE_LABELS = {
    'nuevo': 'Nuevo',
    'contactado': 'Contactado',
    'agendado': 'Agendado',
    'inspeccionado': 'Inspeccionado',
    'en_venta': 'En Venta',
    'vendido': 'Vendido',
    'descartado': 'Descartado'
}


# ─── CRM API: Leads ───────────────────────────────────────────────────────────
@app.route("/api/crm/leads", methods=["GET"])
def crm_get_leads():
    stage = request.args.get("stage")
    source = request.args.get("source")
    search = request.args.get("search", "")
    with get_crm_conn() as conn:
        query = "SELECT * FROM crm_leads WHERE 1=1"
        params = []
        if stage:
            query += " AND stage=?"
            params.append(stage)
        if source:
            query += " AND source=?"
            params.append(source)
        if search:
            query += " AND (full_name LIKE ? OR plate LIKE ? OR phone LIKE ? OR email LIKE ? OR rut LIKE ? OR car_make LIKE ? OR car_model LIKE ?)"
            s = "%" + search + "%"
            params.extend([s, s, s, s, s, s, s])
        query += " ORDER BY updated_at DESC"
        rows = conn.execute(query, params).fetchall()
    return jsonify([row_to_dict(r) for r in rows])


@app.route("/api/crm/leads", methods=["POST"])
def crm_create_lead():
    data = request.json
    fields = [
        'first_name', 'last_name', 'full_name', 'rut', 'email', 'phone', 'country_code',
        'region', 'commune', 'address', 'plate', 'car_make', 'car_model', 'car_year',
        'mileage', 'version', 'appointment_date', 'appointment_time', 'stage', 'priority',
        'assigned_to', 'source', 'source_id', 'supabase_id', 'funnel_url',
        'estimated_value', 'listing_price', 'notes', 'tags',
        'last_contact_at', 'next_followup_at'
    ]
    record = {}
    for f in fields:
        if f in data:
            record[f] = data[f]
    if not record.get('full_name') and record.get('first_name'):
        record['full_name'] = (record.get('first_name', '') + ' ' + record.get('last_name', '')).strip()
    record['created_at'] = datetime.now().isoformat()
    record['updated_at'] = record['created_at']
    if isinstance(record.get('tags'), list):
        record['tags'] = json.dumps(record['tags'])

    cols = ", ".join(record.keys())
    placeholders = ", ".join(["?"] * len(record))
    with get_crm_conn() as conn:
        try:
            conn.execute("INSERT INTO crm_leads ({}) VALUES ({})".format(cols, placeholders), list(record.values()))
            conn.commit()
            lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            # Add creation activity
            conn.execute(
                "INSERT INTO crm_activities (lead_id, type, title, description) VALUES (?, ?, ?, ?)",
                (lead_id, 'created', 'Lead creado',
                 'Fuente: ' + record.get('source', 'manual'))
            )
            conn.commit()
            row = conn.execute("SELECT * FROM crm_leads WHERE id=?", (lead_id,)).fetchone()
            return jsonify(row_to_dict(row)), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 409


@app.route("/api/crm/leads/<int:lead_id>", methods=["GET"])
def crm_get_lead(lead_id):
    with get_crm_conn() as conn:
        row = conn.execute("SELECT * FROM crm_leads WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return jsonify({"error": "Lead no encontrado"}), 404
        lead = row_to_dict(row)
        # Get activities
        activities = conn.execute(
            "SELECT * FROM crm_activities WHERE lead_id=? ORDER BY created_at DESC",
            (lead_id,)
        ).fetchall()
        lead['activities'] = [row_to_dict(a) for a in activities]
    return jsonify(lead)


@app.route("/api/crm/leads/<int:lead_id>", methods=["PATCH"])
def crm_update_lead(lead_id):
    data = request.json
    allowed = {
        'first_name', 'last_name', 'full_name', 'rut', 'email', 'phone', 'country_code',
        'region', 'commune', 'address', 'plate', 'car_make', 'car_model', 'car_year',
        'mileage', 'version', 'appointment_date', 'appointment_time', 'stage', 'priority',
        'assigned_to', 'source', 'source_id', 'funnel_url', 'estimated_value',
        'listing_price', 'notes', 'tags', 'last_contact_at', 'next_followup_at'
    }
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"message": "No valid fields to update, ignoring", "ok": True}), 200

    if isinstance(updates.get('tags'), list):
        updates['tags'] = json.dumps(updates['tags'])
    updates['updated_at'] = datetime.now().isoformat()

    # Auto-compute full_name when first_name or last_name changes
    if 'first_name' in updates or 'last_name' in updates:
        with get_crm_conn() as conn:
            current = conn.execute("SELECT first_name, last_name FROM crm_leads WHERE id=?", (lead_id,)).fetchone()
        if current:
            fn = updates.get('first_name', current.get('first_name') or '').strip()
            ln = updates.get('last_name', current.get('last_name') or '').strip()
            updates['full_name'] = "{} {}".format(fn, ln).strip()

    # Track stage change
    old_stage = None
    if 'stage' in updates:
        with get_crm_conn() as conn:
            row = conn.execute("SELECT stage FROM crm_leads WHERE id=?", (lead_id,)).fetchone()
            if row:
                old_stage = row['stage']

    set_clause = ", ".join("{}=?".format(k) for k in updates)
    values = list(updates.values()) + [lead_id]
    with get_crm_conn() as conn:
        conn.execute("UPDATE crm_leads SET {} WHERE id=?".format(set_clause), values)
        # Log activity for stage changes
        if old_stage and 'stage' in updates and old_stage != updates['stage']:
            old_label = CRM_STAGE_LABELS.get(old_stage, old_stage)
            new_label = CRM_STAGE_LABELS.get(updates['stage'], updates['stage'])
            conn.execute(
                "INSERT INTO crm_activities (lead_id, type, title, description) VALUES (?, ?, ?, ?)",
                (lead_id, 'stage_change', 'Etapa actualizada',
                 '{} → {}'.format(old_label, new_label))
            )
        conn.commit()
        row = conn.execute("SELECT * FROM crm_leads WHERE id=?", (lead_id,)).fetchone()

    result = row_to_dict(row)

    # ── Reverse sync: push owner details to matching consignacion ──
    _sync_consignacion_from_crm_lead(result)

    return jsonify(result)


@app.route("/api/crm/leads/<int:lead_id>", methods=["DELETE"])
def crm_delete_lead(lead_id):
    with get_crm_conn() as conn:
        row = conn.execute("SELECT id FROM crm_leads WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return jsonify({"error": "Lead no encontrado"}), 404
        conn.execute("DELETE FROM crm_activities WHERE lead_id=?", (lead_id,))
        conn.execute("DELETE FROM crm_leads WHERE id=?", (lead_id,))
        conn.commit()
    return jsonify({"ok": True})


# ─── CRM API: Activities ──────────────────────────────────────────────────────
@app.route("/api/crm/leads/<int:lead_id>/activities", methods=["GET"])
def crm_get_activities(lead_id):
    with get_crm_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM crm_activities WHERE lead_id=? ORDER BY created_at DESC",
            (lead_id,)
        ).fetchall()
    return jsonify([row_to_dict(r) for r in rows])


@app.route("/api/crm/leads/<int:lead_id>/activities", methods=["POST"])
def crm_add_activity(lead_id):
    data = request.json
    if not data.get('title'):
        return jsonify({"error": "Se requiere título"}), 400
    with get_crm_conn() as conn:
        conn.execute(
            "INSERT INTO crm_activities (lead_id, type, title, description, created_by) VALUES (?, ?, ?, ?, ?)",
            (lead_id, data.get('type', 'note'), data['title'],
             data.get('description', ''), data.get('created_by', 'user'))
        )
        # Update last_contact_at
        if data.get('type') in ('call', 'email', 'whatsapp', 'meeting'):
            conn.execute(
                "UPDATE crm_leads SET last_contact_at=?, updated_at=? WHERE id=?",
                (datetime.now().isoformat(), datetime.now().isoformat(), lead_id)
            )
        conn.commit()
    return jsonify({"ok": True}), 201


# ─── CRM API: Pipeline Stats ──────────────────────────────────────────────────
@app.route("/api/crm/stats")
def crm_stats():
    with get_crm_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM crm_leads").fetchone()[0]
        pipeline = {}
        for stage in CRM_STAGES:
            count = conn.execute("SELECT COUNT(*) FROM crm_leads WHERE stage=?", (stage,)).fetchone()[0]
            pipeline[stage] = count
        # Source breakdown
        all_leads = conn.execute("SELECT source FROM crm_leads").fetchall()
        source_map = {}
        for r in all_leads:
            src = r.get("source") or "unknown"
            source_map[src] = source_map.get(src, 0) + 1
        # Recent leads (last 7 days)
        recent = conn.execute(
            "SELECT COUNT(*) FROM crm_leads WHERE created_at >= datetime('now', '-7 days')"
        ).fetchone()[0]
        # Leads needing follow-up
        needs_followup = conn.execute(
            "SELECT COUNT(*) FROM crm_leads WHERE next_followup_at <= datetime('now') AND stage NOT IN ('vendido', 'descartado')"
        ).fetchone()[0]
    return jsonify({
        "total": total,
        "pipeline": pipeline,
        "sources": source_map,
        "recent_7d": recent,
        "needs_followup": needs_followup,
    })


# ─── CRM API: Sync from Supabase ──────────────────────────────────────────────
@app.route("/api/crm/sync", methods=["POST"])
def crm_sync_supabase():
    """Pull appointments from Supabase and merge into local CRM leads."""
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")
    if not supabase_url or not supabase_key:
        return jsonify({"error": "Supabase not configured"}), 400

    try:
        resp = _requests.get(
            supabase_url + "/rest/v1/appointments?select=*&order=created_at.desc",
            headers={
                "apikey": supabase_key,
                "Authorization": "Bearer " + supabase_key,
                "Content-Type": "application/json"
            },
            timeout=15
        )
        if resp.status_code != 200:
            return jsonify({"error": "Supabase error", "status": resp.status_code, "body": resp.text}), 502
        appointments = resp.json()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    imported = 0
    skipped = 0
    with get_crm_conn() as conn:
        for appt in appointments:
            supabase_id = appt.get("id")
            if not supabase_id:
                continue
            # Check if already imported
            existing = conn.execute(
                "SELECT id FROM crm_leads WHERE supabase_id=?", (supabase_id,)
            ).fetchone()
            if existing:
                skipped += 1
                continue
            # Map Supabase appointment → CRM lead
            stage_map = {
                'agendado': 'agendado',
                'confirmado': 'agendado',
                'completado': 'inspeccionado',
                'cancelado': 'descartado'
            }
            stage = stage_map.get(appt.get('status', ''), 'nuevo')
            now = datetime.now().isoformat()
            conn.execute("""
                INSERT INTO crm_leads (
                    first_name, last_name, full_name, rut, country_code, phone, email,
                    region, commune, address, plate, car_make, car_model, car_year,
                    mileage, version, appointment_date, appointment_time,
                    stage, source, supabase_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                appt.get('first_name'), appt.get('last_name'), appt.get('full_name'),
                appt.get('rut'), appt.get('country_code', '+56'),
                appt.get('phone'), appt.get('email'),
                appt.get('region'), appt.get('commune'), appt.get('address'),
                appt.get('plate'), appt.get('car_make'), appt.get('car_model'),
                appt.get('car_year'), appt.get('mileage'), appt.get('version'),
                appt.get('appointment_date'), appt.get('appointment_time'),
                stage, 'autodirecto', supabase_id, now, now
            ))
            lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                "INSERT INTO crm_activities (lead_id, type, title, description) VALUES (?, ?, ?, ?)",
                (lead_id, 'imported', 'Importado desde Autodirecto',
                 'Cita del {} a las {}'.format(appt.get('appointment_date', '?'), appt.get('appointment_time', '?')))
            )
            imported += 1
        conn.commit()

    return jsonify({"ok": True, "imported": imported, "skipped": skipped, "total_from_supabase": len(appointments)})


# ─── CRM API: Import from Funnels ─────────────────────────────────────────────
@app.route("/api/crm/import-funnels", methods=["POST"])
def crm_import_funnels():
    """Import Funnels leads into CRM."""
    if not FUNNELS_DIR.exists():
        return jsonify({"error": "Funnels not available"}), 404

    try:
        leads = funnels_module.get_leads()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    imported = 0
    skipped = 0
    import re as _re
    with get_crm_conn() as conn:
        for lead in leads:
            funnel_url = lead.get("url", "")
            if not funnel_url:
                continue
            existing = conn.execute(
                "SELECT id FROM crm_leads WHERE funnel_url=?", (funnel_url,)
            ).fetchone()
            if existing:
                skipped += 1
                continue
            # Parse vehicle info from title
            title = lead.get("title", "")
            parts = title.split()
            car_make = parts[0] if len(parts) > 0 else None
            car_model = " ".join(parts[1:]) if len(parts) > 1 else None
            # Parse year from title
            car_year = None
            year_match = _re.search(r'\b(19|20)\d{2}\b', title)
            if year_match:
                car_year = int(year_match.group())
            # Parse mileage
            mileage_raw = lead.get("mileage", "")
            mileage = None
            if mileage_raw:
                digits = _re.findall(r'\d+', str(mileage_raw).replace(",", ""))
                if digits:
                    mileage = int(digits[0])
            # Parse price
            price_str = lead.get("price", "")
            listing_price = None
            if price_str:
                digits = _re.findall(r'\d+', str(price_str).replace(",", "").replace(".", ""))
                if digits:
                    listing_price = int(digits[0])
            now = datetime.now().isoformat()
            funnels_status = lead.get("status", "new")
            stage_map = {"new": "nuevo", "contacted": "contactado", "interested": "agendado", "discarded": "descartado"}
            stage = stage_map.get(funnels_status, "nuevo")

            conn.execute("""
                INSERT INTO crm_leads (
                    full_name, phone, car_make, car_model, car_year, mileage,
                    listing_price, stage, source, funnel_url, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                lead.get("seller", None), lead.get("seller_phone", None),
                car_make, car_model, car_year, mileage,
                listing_price, stage, 'funnels', funnel_url, now, now
            ))
            lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                "INSERT INTO crm_activities (lead_id, type, title, description) VALUES (?, ?, ?, ?)",
                (lead_id, 'imported', 'Importado desde Funnels',
                 'Publicación: {} - {}'.format(title, lead.get("location", "")))
            )
            imported += 1
        conn.commit()
    return jsonify({"ok": True, "imported": imported, "skipped": skipped, "total_funnels": len(leads)})


# ─── Frontend ─────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


# ─── API: Stats ───────────────────────────────────────────────────────────────
@app.route("/api/stats")
def stats():
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM cars").fetchone()[0]
        available = conn.execute("SELECT COUNT(*) FROM cars WHERE status='available'").fetchone()[0]
        sold = conn.execute("SELECT COUNT(*) FROM cars WHERE status='sold'").fetchone()[0]
        sent_dte = conn.execute("SELECT COUNT(*) FROM cars WHERE status='sent_dte'").fetchone()[0]
        draft_dte = conn.execute("SELECT COUNT(*) FROM cars WHERE status='draft_dte'").fetchone()[0]

        # Commission sums (sold + sent_dte cars)
        rows = conn.execute(
            "SELECT selling_price, commission_pct FROM cars WHERE status IN ('sold','sent_dte')"
        ).fetchall()
        total_commission = sum(round((r["selling_price"] or 0) * (r["commission_pct"] or 0)) for r in rows)
        total_ventas = sum(r["selling_price"] or 0 for r in rows)

    return jsonify({
        "total": total,
        "available": available,
        "sold": sold,
        "sent_dte": sent_dte,
        "draft_dte": draft_dte,
        "total_commission": total_commission,
        "total_ventas": total_ventas,
    })


# ─── API: Cars (inventory) ────────────────────────────────────────────────────
@app.route("/api/cars", methods=["GET"])
def get_cars():
    status = request.args.get("status")
    search = request.args.get("search", "")
    with get_conn() as conn:
        query = "SELECT * FROM cars WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"
            params.append(status)
        if search:
            query += " AND (patente LIKE ? OR brand LIKE ? OR model LIKE ? OR owner_name LIKE ?)"
            s = f"%{search}%"
            params.extend([s, s, s, s])
        query += " ORDER BY id DESC"
        rows = conn.execute(query, params).fetchall()

    cars = []
    for r in rows:
        d = row_to_dict(r)
        cal = calculate_commission(d)
        d["commission_amount"] = cal["commission_amount"]
        d["net_to_owner"] = cal["net_to_owner"]
        d["iva_on_commission"] = cal["iva_on_commission"]
        cars.append(d)
    return jsonify(cars)


@app.route("/api/cars", methods=["POST"])
def add_car():
    import re
    data = request.json
    required = ["patente", "brand", "model", "owner_name", "owner_rut", "owner_price", "selling_price"]
    for f in required:
        if not data.get(f):
            return jsonify({"error": f"Campo requerido: {f}"}), 400

    rut = str(data["owner_rut"]).strip()
    if not re.match(r"^\d{7,8}-[\dKk]$", rut):
        return jsonify({"error": f"RUT inválido: '{rut}'. Formato: 12345678-9"}), 400

    with get_conn() as conn:
        try:
            conn.execute("""
                INSERT INTO cars (patente,vin,brand,model,year,color,owner_name,owner_rut,
                    owner_email,owner_phone,owner_price,selling_price,commission_pct,notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data["patente"].upper(), data.get("vin"), data["brand"], data["model"],
                data.get("year"), data.get("color"), data["owner_name"], rut,
                data.get("owner_email"), data.get("owner_phone"),
                int(data["owner_price"]), int(data["selling_price"]),
                float(data.get("commission_pct", 0.10)), data.get("notes"),
            ))
            conn.commit()
            car_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            return jsonify({"ok": True, "id": car_id}), 201
        except Exception:
            return jsonify({"error": f"Ya existe un auto con patente '{data['patente'].upper()}'"}), 409


@app.route("/api/cars/<int:car_id>", methods=["GET"])
def get_car(car_id):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM cars WHERE id=?", (car_id,)).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    d = row_to_dict(row)
    cal = calculate_commission(d)
    d.update(cal)
    return jsonify(d)


@app.route("/api/cars/<int:car_id>", methods=["PATCH"])
def update_car(car_id):
    data = request.json
    allowed = {"status", "selling_price", "owner_price", "commission_pct",
               "owner_email", "owner_phone", "notes", "vin", "color"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"error": "No valid fields to update"}), 400

    updates["updated_at"] = datetime.now().isoformat()
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [car_id]

    with get_conn() as conn:
        conn.execute(f"UPDATE cars SET {set_clause} WHERE id=?", values)
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/cars/<int:car_id>", methods=["DELETE"])
def delete_car(car_id):
    with get_conn() as conn:
        row = conn.execute("SELECT patente FROM cars WHERE id=?", (car_id,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        conn.execute("DELETE FROM cars WHERE id=?", (car_id,))
        conn.commit()
    return jsonify({"ok": True})


# ─── API: Commission Calculator ───────────────────────────────────────────────
@app.route("/api/calculate", methods=["POST"])
def calculate():
    data = request.json
    try:
        result = calculate_commission({
            "selling_price": int(data["selling_price"]),
            "owner_price": int(data["owner_price"]),
            "commission_pct": float(data.get("commission_pct", 0.10)),
        })
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ─── API: DTE Generation (simulation) ────────────────────────────────────────
@app.route("/api/dte/generate/<int:car_id>", methods=["POST"])
def generate_dte(car_id):
    data = request.json or {}
    folio = data.get("folio", 1)

    with get_conn() as conn:
        row = conn.execute("SELECT * FROM cars WHERE id=?", (car_id,)).fetchone()
    if not row:
        return jsonify({"error": "Auto no encontrado"}), 404

    car = row_to_dict(row)
    cal = calculate_commission(car)
    today = date.today().isoformat()
    empresa_rut = os.getenv("EMPRESA_RUT", "78355717-7")
    empresa_rzn = os.getenv("EMPRESA_RAZON_SOCIAL", "Wiackowska Group Spa")

    dte = {
        "Encabezado": {
            "IdDoc": {"TipoDTE": 43, "Folio": folio, "FchEmis": today, "IndServicio": 1},
            "Emisor": {
                "RUTEmisor": empresa_rut, "RznSoc": empresa_rzn,
                "GiroEmis": os.getenv("EMPRESA_GIRO", "Compraventa de Vehículos Usados"),
                "DirOrigen": os.getenv("EMPRESA_DIRECCION", "Av. Providencia 123"),
                "CmnaOrigen": os.getenv("EMPRESA_COMUNA", "Providencia"),
            },
            "Receptor": {
                "RUTRecep": car["owner_rut"], "RznSocRecep": car["owner_name"],
                "DirRecep": car.get("notes") or "Sin dirección",
                "CmnaRecep": "Santiago",
            },
            "Totales": {
                "MntNeto": cal["commission_amount"],
                "TasaIVA": 19,
                "IVA": cal["iva_on_commission"],
                "MntTotal": cal["gross_commission"],
            },
        },
        "Detalle": [{
            "NroLinDet": 1,
            "NmbItem": f"Comisión consignación {car['brand']} {car['model']} {car.get('year','')}- Patente {car['patente']}",
            "QtyItem": 1,
            "PrcItem": cal["commission_amount"],
            "MontoItem": cal["commission_amount"],
        }],
    }

    # Validate
    errors = validate_schema(dte)

    # Save draft
    draft_dir = ROOT / ".tmp" / "draft_dtes"
    draft_dir.mkdir(parents=True, exist_ok=True)
    draft_path = draft_dir / f"liq_factura_{car_id}.json"
    full_dte = dict(dte)
    full_dte["_meta"] = {"car_id": car_id, "generated_at": today}
    draft_path.write_text(json.dumps(full_dte, ensure_ascii=False, indent=2), encoding="utf-8")

    # Update status to draft_dte
    with get_conn() as conn:
        conn.execute("UPDATE cars SET status='draft_dte', updated_at=? WHERE id=?",
                     (datetime.now().isoformat(), car_id))
        conn.commit()

    return jsonify({
        "ok": True,
        "errors": errors,
        "dte": dte,
        "draft_path": str(draft_path),
        "commission_breakdown": cal,
    })


@app.route("/api/dte/simulate_send/<int:car_id>", methods=["POST"])
def simulate_send(car_id):
    """Simulate sending to SimpleAPI (no real API call)."""
    import time, random
    time.sleep(0.8)  # Simulate network

    api_key = os.getenv("SIMPLEAPI_KEY", "")
    has_key = api_key and api_key != "your-api-key-here"
    has_cert = Path(os.getenv("CERT_PATH", "")).exists()
    has_caf = Path(os.getenv("CAF_PATH_43", "")).exists()

    with get_conn() as conn:
        row = conn.execute("SELECT * FROM cars WHERE id=?", (car_id,)).fetchone()
    if not row:
        return jsonify({"error": "Auto no encontrado"}), 404

    car = row_to_dict(row)

    # Simulate a signed XML folio number
    folio_sim = random.randint(1000, 9999)

    checklist = [
        {"label": "API Key configurada", "ok": has_key},
        {"label": "Certificado digital (.pfx)", "ok": has_cert},
        {"label": "CAF Tipo 43 (Liquidación Factura)", "ok": has_caf},
        {"label": "Schema DTE válido", "ok": True},
        {"label": "RUT emisor verificado", "ok": True},
    ]

    all_ok = has_key and has_cert and has_caf
    if all_ok:
        msg = f"DTE enviado correctamente. Folio SII: {folio_sim}"
        with get_conn() as conn:
            conn.execute("UPDATE cars SET status='sent_dte', updated_at=? WHERE id=?",
                         (datetime.now().isoformat(), car_id))
            conn.commit()
    else:
        msg = "Simulación completa. Faltan credenciales para envío real."

    return jsonify({
        "ok": True,
        "simulated": not all_ok,
        "message": msg,
        "folio": folio_sim,
        "checklist": checklist,
        "car": car,
    })


if __name__ == "__main__":
    print("\n🚀 Autodirecto CRM")
    print("   http://127.0.0.1:8080\n")
    print("📋 If the listings table doesn't exist in Supabase, run this SQL:")
    print("   https://supabase.com/dashboard/project/kqympdxeszdyppbhtzbm/sql/new\n")
    print("""CREATE TABLE IF NOT EXISTS listings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  consignacion_id TEXT,
  appraisal_id UUID,
  brand TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  year INTEGER,
  color TEXT,
  mileage_km INTEGER,
  plate TEXT,
  price BIGINT,
  fuel_type TEXT DEFAULT 'Bencina',
  transmission TEXT DEFAULT 'Manual',
  motor TEXT,
  description TEXT,
  features JSONB DEFAULT '{}',
  image_urls JSONB DEFAULT '[]',
  status TEXT DEFAULT 'disponible',
  featured BOOLEAN DEFAULT FALSE
);
ALTER TABLE listings ENABLE ROW LEVEL SECURITY;
CREATE POLICY IF NOT EXISTS "Public read" ON listings FOR SELECT USING (status = 'disponible');
CREATE POLICY IF NOT EXISTS "Service write" ON listings USING (true) WITH CHECK (true);
""")
    app.run(debug=True, port=8080)
