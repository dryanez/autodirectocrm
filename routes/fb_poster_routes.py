"""
fb_poster_routes.py — FB Marketplace Auto-Poster Blueprint for Autodirecto CRM.

Handles:
  - Fetching cars available for posting (en_venta consignaciones with images)
  - Managing posting jobs (create, status, logs)
  - Group configuration (enable/disable, add/remove)
  - Job history

The actual Playwright browser automation runs locally on the user's Mac.
This CRM module provides the API + triggers a local worker.
"""

import json
import os
import uuid
from datetime import datetime

import requests
from flask import Blueprint, jsonify, request

# ─── Blueprint ───────────────────────────────────────────────────────────────
fb_poster_bp = Blueprint('fb_poster', __name__, url_prefix='/api/fb-poster')

# ─── Supabase config ────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://kqympdxeszdyppbhtzbm.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY", "")


def _supa_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _supa_get(table, params=None):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=_supa_headers(), params=params or {}, timeout=15)
    r.raise_for_status()
    return r.json()


def _supa_post(table, data):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=_supa_headers(), json=data, timeout=15)
    r.raise_for_status()
    return r.json()


def _supa_patch(table, data, match):
    params = {k: f"eq.{v}" for k, v in match.items()}
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}", headers=_supa_headers(), json=data, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _supa_delete(table, match):
    params = {k: f"eq.{v}" for k, v in match.items()}
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}", headers=_supa_headers(), params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _company_id():
    """Get company_id from request header (multi-tenant)."""
    return request.headers.get('X-Company-Id', 'a0000000-0000-0000-0000-000000000001')


# ─── Default Groups ─────────────────────────────────────────────────────────
DEFAULT_GROUPS = [
    {"name": "CHILE AUTOS - Sin Telefono Se Borra", "url": "https://www.facebook.com/groups/repuestosdeautos.cl/", "enabled": True},
    {"name": "MUNDO TUERCA VALPO, VIÑA", "url": "https://www.facebook.com/groups/1721514511403756/", "enabled": True},
    {"name": "Autos Viña del mar - Valparaiso", "url": "https://www.facebook.com/groups/chileautosvinadelmar/", "enabled": True},
    {"name": "venta de autos y repuestos V región", "url": "https://www.facebook.com/groups/367129786996327/", "enabled": True},
    {"name": "Autos Valparaiso - Viña del mar", "url": "https://www.facebook.com/groups/chileautosvalparaiso/", "enabled": True},
    {"name": "Todo Tuercas Quilpue - Villa Alemana", "url": "https://www.facebook.com/groups/todotuercas/", "enabled": True},
    {"name": "venta de autos y motos V region", "url": "https://www.facebook.com/groups/651487044978775/", "enabled": True},
    {"name": "Vende tu auto VRegion", "url": "https://www.facebook.com/groups/617218648426911/", "enabled": True},
    {"name": "Autos en Venta Quinta Región Chile", "url": "https://www.facebook.com/groups/autos.en.venta.quinta.region.chile/", "enabled": True},
    {"name": "VENTA DE VEHÍCULOS DE OCASIÓN 🤝", "url": "https://www.facebook.com/groups/701541853761323/", "enabled": True},
    {"name": "compra y permutas de autos 5ta region", "url": "https://www.facebook.com/groups/819445958235019/", "enabled": True},
    {"name": "Venta de Autos Usado en Chile", "url": "https://www.facebook.com/groups/467592933360081/", "enabled": True},
    {"name": "Gran feria del auto usado V Region", "url": "https://www.facebook.com/groups/236187406570255/", "enabled": True},
    {"name": "Venta de Autos 5ta Region (1)", "url": "https://www.facebook.com/groups/3089228341220930/", "enabled": True},
    {"name": "Venta de Autos 5ta Region (2)", "url": "https://www.facebook.com/groups/1537403170317369/", "enabled": True},
    {"name": "Compraventa autos v región", "url": "https://www.facebook.com/groups/639650642724240/", "enabled": True},
    {"name": "COMPROVEHICULOS.CL", "url": "https://www.facebook.com/groups/506547946136436/", "enabled": True},
    {"name": "Compra venta permutas Suv chile", "url": "https://www.facebook.com/groups/671270960818458/", "enabled": True},
    {"name": "AUTOS USADOS CHILE . CL", "url": "https://www.facebook.com/groups/669775896446268/", "enabled": True},
    {"name": "VEHICULOS USADOS DE TODO CHILE ✅", "url": "https://www.facebook.com/groups/1094829247361002/", "enabled": True},
    {"name": "Yapo concon", "url": "https://www.facebook.com/groups/231458073867654/", "enabled": True},
    {"name": "COMPRA Y VENTA AUTOS NUEVOS USADOS", "url": "https://www.facebook.com/groups/330969950788171/", "enabled": True},
    {"name": "Compra Venta - Autos CHILE", "url": "https://www.facebook.com/groups/1401391943432509/", "enabled": True},
    {"name": "Autos 0KM Seminuevos Usados Chile", "url": "https://www.facebook.com/groups/autos0kmseminuevosyusados/", "enabled": True},
    {"name": "Autos y motos V REGIÓN", "url": "https://www.facebook.com/groups/481371528705498/", "enabled": True},
    {"name": "Autos Usados Chile", "url": "https://www.facebook.com/groups/autosbaratostemucoyalrededores/", "enabled": True},
    {"name": "Autos Usados Viña del Mar", "url": "https://www.facebook.com/groups/548654176561779/", "enabled": True},
    {"name": "Vendo Mi Auto Viña Del Mar", "url": "https://www.facebook.com/groups/1156515334359895/", "enabled": True},
    {"name": "Compra-Venta AUTOS USADOS Chile", "url": "https://www.facebook.com/groups/754676712758575/", "enabled": True},
    {"name": "autos V region chile 🔰", "url": "https://www.facebook.com/groups/750188968678503/", "enabled": True},
    {"name": "Feria tuerca quinta region", "url": "https://www.facebook.com/groups/470520613089491/", "enabled": True},
    {"name": "Venta de Autos V region", "url": "https://www.facebook.com/groups/375455562659988/", "enabled": True},
    {"name": "MULTI AUTOS V REGIÓN", "url": "https://www.facebook.com/groups/590614651103274/", "enabled": True},
    {"name": "COMPRA VENTA AUTOS QUILPUE", "url": "https://www.facebook.com/groups/1237843139611306/", "enabled": True},
    {"name": "BUSCO AUTO V REGION", "url": "https://www.facebook.com/groups/AUTOSQUINTAREGION/", "enabled": True},
    {"name": "COMPRA VENDE O PERMUTA UN AUTO", "url": "https://www.facebook.com/groups/229097777240972/", "enabled": True},
    {"name": "Todo autos quinta region", "url": "https://www.facebook.com/groups/809445329202354/", "enabled": True},
    {"name": "COMPRA VENTA AUTOS MOTOS 5TA REGION", "url": "https://www.facebook.com/groups/340277276445153/", "enabled": True},
    {"name": "venta vehiculo quillota y alrededores", "url": "https://www.facebook.com/groups/2872527959681039/", "enabled": True},
    {"name": "Autos Clasificados Quinta Región", "url": "https://www.facebook.com/groups/192003684626534/", "enabled": True},
    {"name": "Venta de autos quinta región", "url": "https://www.facebook.com/groups/938149021504442/", "enabled": True},
    {"name": "Marketplace Valpo-Viña", "url": "https://www.facebook.com/groups/1246293805815497/", "enabled": True},
]


# ═══════════════════════════════════════════════════════════════════════════════
# 1. CONFIG — Groups + Location
# ═══════════════════════════════════════════════════════════════════════════════

@fb_poster_bp.route('/config', methods=['GET'])
def get_config():
    """Get FB Poster config (groups, location) for current company."""
    cid = _company_id()
    rows = _supa_get("fb_poster_config", {"company_id": f"eq.{cid}"})
    if rows:
        cfg = rows[0]
        # Parse groups if string
        groups = cfg.get("groups", [])
        if isinstance(groups, str):
            groups = json.loads(groups)
        cfg["groups"] = groups
        return jsonify({"ok": True, "config": cfg})
    else:
        # Return defaults (and auto-create config row)
        cfg = {
            "company_id": cid,
            "location_name": "Bosques de Miramar, Viña del Mar",
            "latitude": -33.0245,
            "longitude": -71.5518,
            "groups": DEFAULT_GROUPS,
            "delay_min": 15,
            "delay_max": 45,
        }
        try:
            _supa_post("fb_poster_config", {
                **cfg,
                "groups": json.dumps(DEFAULT_GROUPS),
            })
        except Exception:
            pass  # Config table might not exist yet
        return jsonify({"ok": True, "config": cfg})


@fb_poster_bp.route('/config', methods=['PATCH'])
def update_config():
    """Update FB Poster config."""
    cid = _company_id()
    data = request.json or {}
    patch = {}
    if "location_name" in data:
        patch["location_name"] = data["location_name"]
    if "latitude" in data:
        patch["latitude"] = data["latitude"]
    if "longitude" in data:
        patch["longitude"] = data["longitude"]
    if "groups" in data:
        patch["groups"] = json.dumps(data["groups"]) if isinstance(data["groups"], list) else data["groups"]
    if "delay_min" in data:
        patch["delay_min"] = data["delay_min"]
    if "delay_max" in data:
        patch["delay_max"] = data["delay_max"]

    if patch:
        patch["updated_at"] = datetime.utcnow().isoformat()
        _supa_patch("fb_poster_config", patch, {"company_id": cid})

    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# 2. CARS — Fetch available vehicles with images
# ═══════════════════════════════════════════════════════════════════════════════

@fb_poster_bp.route('/cars', methods=['GET'])
def get_cars():
    """Get consignaciones en_venta with their images for posting."""
    cid = _company_id()
    try:
        cars = _supa_get("consignaciones", {
            "select": "id,car_make,car_model,car_year,plate,color,mileage,km_verified,"
                      "version,selling_price,owner_price,fuel_type,transmission,"
                      "appraisal_supabase_id,listing_id,status,en_venta",
            "company_id": f"eq.{cid}",
            "en_venta": "eq.true",
            "order": "updated_at.desc",
        })

        # Attach images to each car
        for car in cars:
            car["_images"] = []
            appraisal_id = car.get("appraisal_supabase_id")
            if appraisal_id:
                try:
                    images = _supa_get("vehicle_images", {
                        "select": "url,label,photo_type",
                        "appraisal_id": f"eq.{appraisal_id}",
                        "order": "created_at.asc",
                    })
                    car["_images"] = [img["url"] for img in images if img.get("url")]
                except Exception:
                    pass

            # Fallback to listing image_urls
            if not car["_images"] and car.get("listing_id"):
                try:
                    listings = _supa_get("listings", {
                        "select": "image_urls",
                        "id": f"eq.{car['listing_id']}",
                    })
                    if listings and listings[0].get("image_urls"):
                        urls = listings[0]["image_urls"]
                        if isinstance(urls, str):
                            urls = json.loads(urls)
                        car["_images"] = [u["url"] if isinstance(u, dict) else u for u in urls]
                except Exception:
                    pass

        return jsonify({"ok": True, "cars": cars})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# 3. JOBS — Create, list, get status
# ═══════════════════════════════════════════════════════════════════════════════

@fb_poster_bp.route('/jobs', methods=['GET'])
def list_jobs():
    """List recent posting jobs."""
    cid = _company_id()
    try:
        jobs = _supa_get("fb_poster_jobs", {
            "company_id": f"eq.{cid}",
            "order": "created_at.desc",
            "limit": "50",
        })
        for j in jobs:
            if isinstance(j.get("log"), str):
                j["log"] = json.loads(j["log"])
            if isinstance(j.get("groups_detail"), str):
                j["groups_detail"] = json.loads(j["groups_detail"])
        return jsonify({"ok": True, "jobs": jobs})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@fb_poster_bp.route('/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    """Get a single job status + logs."""
    try:
        rows = _supa_get("fb_poster_jobs", {"id": f"eq.{job_id}"})
        if not rows:
            return jsonify({"ok": False, "error": "Job not found"}), 404
        job = rows[0]
        if isinstance(job.get("log"), str):
            job["log"] = json.loads(job["log"])
        if isinstance(job.get("groups_detail"), str):
            job["groups_detail"] = json.loads(job["groups_detail"])
        return jsonify({"ok": True, "job": job})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@fb_poster_bp.route('/jobs', methods=['POST'])
def create_job():
    """
    Create a new posting job.
    The job is created with status='queued'. A local Playwright worker
    picks it up and executes the browser automation.
    Body: { consignacion_id, caption, groups: [{name, url}] }
    """
    cid = _company_id()
    data = request.json or {}
    consig_id = data.get("consignacion_id")
    caption = data.get("caption", "")
    car_title = data.get("car_title", "")
    groups = data.get("groups", [])

    if not consig_id:
        return jsonify({"ok": False, "error": "consignacion_id required"}), 400

    try:
        job = _supa_post("fb_poster_jobs", {
            "company_id": cid,
            "consignacion_id": consig_id,
            "car_title": car_title,
            "caption": caption,
            "status": "queued",
            "groups_total": len(groups),
            "groups_detail": json.dumps([
                {"name": g["name"], "url": g["url"], "status": "pending"}
                for g in groups
            ]),
            "log": json.dumps([f"[{datetime.utcnow().strftime('%H:%M:%S')}] Job created, waiting for worker..."]),
        })
        return jsonify({"ok": True, "job": job[0] if job else {}})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@fb_poster_bp.route('/jobs/<job_id>', methods=['PATCH'])
def update_job(job_id):
    """Update job status/logs (called by the Playwright worker)."""
    data = request.json or {}
    patch = {}
    for key in ["status", "marketplace_url", "groups_posted", "groups_failed",
                 "groups_detail", "log", "error", "started_at", "finished_at"]:
        if key in data:
            val = data[key]
            if key in ("groups_detail", "log") and isinstance(val, list):
                val = json.dumps(val)
            patch[key] = val

    if patch:
        try:
            _supa_patch("fb_poster_jobs", patch, {"id": job_id})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True})


@fb_poster_bp.route('/jobs/<job_id>', methods=['DELETE'])
def delete_job(job_id):
    """Delete a job."""
    try:
        _supa_delete("fb_poster_jobs", {"id": job_id})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CAPTION BUILDER (server-side)
# ═══════════════════════════════════════════════════════════════════════════════

@fb_poster_bp.route('/build-caption', methods=['POST'])
def build_caption():
    """Build a marketplace caption for a car. Returns { caption, title, price }."""
    data = request.json or {}

    brand = (data.get("car_make") or "").strip()
    model = (data.get("car_model") or "").strip()
    year = data.get("car_year") or ""
    version = (data.get("version") or "").strip()
    mileage = data.get("mileage") or data.get("km_verified")
    color = (data.get("color") or "").strip()
    price = data.get("selling_price") or data.get("owner_price") or 0

    # Get location from config
    cid = _company_id()
    location = "Bosques de Miramar, Viña del Mar"
    try:
        cfgs = _supa_get("fb_poster_config", {"company_id": f"eq.{cid}"})
        if cfgs:
            location = cfgs[0].get("location_name", location)
    except Exception:
        pass

    title = f"{brand} {model} {year}".strip()
    if version:
        title += f" {version}"

    lines = [f"🚗 {title}", ""]
    if year:
        lines.append(f"📅 Año: {year}")
    if mileage:
        lines.append(f"📏 Kilómetros: {mileage:,} km".replace(",", "."))
    if color:
        lines.append(f"🎨 Color: {color}")
    if version:
        lines.append(f"⚙️ Versión: {version}")
    lines.append("")
    if price:
        lines.append(f"💰 Precio: ${price:,} CLP".replace(",", "."))
    else:
        lines.append("💰 Precio: Consultar")
    lines.extend([
        "",
        f"📍 {location}",
        "",
        "✅ Revisión mecánica disponible",
        "✅ Documentación al día",
        "✅ Financiamiento disponible",
        "",
        "📲 Escríbenos por Messenger o WhatsApp",
        "🌐 autodirecto.cl",
        "",
        "#AutoDirecto #AutosUsados #ViñaDelMar #QuintaRegión #AutosEnVenta",
    ])

    return jsonify({
        "ok": True,
        "caption": "\n".join(lines),
        "title": title,
        "price": price,
    })
