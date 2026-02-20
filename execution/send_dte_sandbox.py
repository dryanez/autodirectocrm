"""
send_dte_sandbox.py — Sends a validated DTE JSON to SimpleAPI for signing and submission.

Reads a draft DTE JSON (produced by build_liquidacion_factura.py or build_guia_despacho.py),
strips internal _meta fields, and POSTs it to SimpleAPI's DTE generation endpoint.

The signed XML response is saved to .tmp/responses/.

Prerequisites:
    - SIMPLEAPI_KEY in .env
    - CERT_PATH + CERT_PASSWORD in .env (valid .pfx file)
    - CAF_PATH_43 or CAF_PATH_52 in .env (matching the DTE type)

Usage:
    python execution/send_dte_sandbox.py --draft .tmp/draft_dtes/liq_factura_1.json
    python execution/send_dte_sandbox.py --draft .tmp/draft_dtes/guia_despacho_1.json --dry-run

Flags:
    --dry-run   Build and validate the payload, but don't actually send it.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SIMPLEAPI_BASE = "https://api.simpleapi.cl"
ENDPOINT = "/api/v1/dte/documento"
MAX_RETRIES = 3
RATE_LIMIT_SLEEP = 2  # seconds to wait on 429


def load_env():
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def check_prerequisites():
    """Verify required config exists before attempting API call."""
    errors = []

    api_key = os.getenv("SIMPLEAPI_KEY", "")
    if not api_key or api_key == "your-api-key-here":
        errors.append("SIMPLEAPI_KEY not set. Get a free key at https://panel.simpleapi.cl")

    cert_path = os.getenv("CERT_PATH", "")
    if cert_path and not Path(cert_path).exists():
        errors.append(f"Certificate not found at CERT_PATH={cert_path}")

    if errors:
        print("\n❌ Prerequisites missing:")
        for e in errors:
            print(f"   • {e}")
        print("\n   Set these in your .env file (copy from .env.example).")
        sys.exit(1)

    return api_key


def load_caf(tipo_dte: int) -> Optional[str]:
    """Load CAF XML content for the given DTE type (base64-encoded for API)."""
    caf_key = f"CAF_PATH_{tipo_dte}"
    caf_path = os.getenv(caf_key, "")
    if not caf_path:
        print(f"⚠️  {caf_key} not set in .env — proceeding without CAF (may fail in API).")
        return None
    if not Path(caf_path).exists():
        print(f"⚠️  CAF file not found at {caf_path}")
        return None
    content = Path(caf_path).read_text(encoding="utf-8")
    return content


def load_cert_b64() -> Optional[str]:
    """Load certificate .pfx as base64 string."""
    cert_path = os.getenv("CERT_PATH", "")
    if not cert_path or not Path(cert_path).exists():
        return None
    raw = Path(cert_path).read_bytes()
    return base64.b64encode(raw).decode("utf-8")


def build_request_body(dte: dict, tipo_dte: int) -> dict:
    """Build the JSON body for the SimpleAPI DTE endpoint."""
    body = {
        "documento": dte,
        "certificado_password": os.getenv("CERT_PASSWORD", ""),
    }

    cert_b64 = load_cert_b64()
    if cert_b64:
        body["certificado"] = cert_b64

    caf = load_caf(tipo_dte)
    if caf:
        body["caf"] = caf

    return body


def send_to_api(body: dict, api_key: str) -> Tuple[int, str]:
    """
    POST to SimpleAPI. Returns (status_code, response_text).
    Retries on 429 (rate limit) up to MAX_RETRIES times.
    """
    try:
        import requests
    except ImportError:
        print("❌ 'requests' library not installed. Run: pip install requests")
        sys.exit(1)

    url = SIMPLEAPI_BASE + ENDPOINT
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"   → Sending to SimpleAPI (attempt {attempt}/{MAX_RETRIES})...")
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=30)
        except requests.exceptions.ConnectionError:
            print("❌ Connection error. Check your internet connection or SimpleAPI status.")
            print(f"   Status: https://status.chilesystems.com/")
            sys.exit(1)
        except requests.exceptions.Timeout:
            print("❌ Request timed out after 30 seconds.")
            sys.exit(1)

        if resp.status_code == 429:
            print(f"   ⚠️  Rate limited (429). Waiting {RATE_LIMIT_SLEEP}s before retry...")
            time.sleep(RATE_LIMIT_SLEEP)
            continue

        return resp.status_code, resp.text

    print(f"❌ Failed after {MAX_RETRIES} retries (rate limit).")
    sys.exit(1)


def save_response(xml_text: str, draft_path: Path) -> Path:
    """Save the signed XML response to .tmp/responses/."""
    out_dir = ROOT / ".tmp" / "responses"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_name = draft_path.stem + "_signed.xml"
    out_path = out_dir / out_name
    out_path.write_text(xml_text, encoding="utf-8")
    return out_path


def main():
    load_env()
    parser = argparse.ArgumentParser(
        description="Send a DTE JSON draft to SimpleAPI for signing and SII submission."
    )
    parser.add_argument("--draft", required=True,
                        help="Path to the DTE JSON draft (from build_*.py)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate and build the payload, but don't send it.")
    args = parser.parse_args()

    # ── 1. Load draft DTE ────────────────────────────────────────────────────
    draft_path = Path(args.draft)
    if not draft_path.exists():
        print(f"❌ Draft file not found: {draft_path}")
        sys.exit(1)

    full_dte = json.loads(draft_path.read_text(encoding="utf-8"))
    tipo_dte = full_dte.get("Encabezado", {}).get("IdDoc", {}).get("TipoDTE")

    if not tipo_dte:
        print("❌ Could not determine TipoDTE from draft. Is this a valid DTE JSON?")
        sys.exit(1)

    names = {43: "Liquidación Factura", 52: "Guía de Despacho"}
    print(f"\nDraft cargado: {draft_path.name}")
    print(f"  TipoDTE: {tipo_dte} ({names.get(tipo_dte, 'Unknown')})")
    print(f"  Folio  : {full_dte['Encabezado']['IdDoc'].get('Folio')}")

    # ── 2. Validate schema ───────────────────────────────────────────────────
    print("\nValidando schema DTE...")
    sys.path.insert(0, str(ROOT))
    from execution.validate_dte_schema import validate  # noqa

    # Strip _meta before validation and sending
    dte_clean = {k: v for k, v in full_dte.items() if not k.startswith("_")}

    errors = validate(dte_clean)
    if errors:
        print(f"❌ Schema inválido — {len(errors)} error(s):")
        for e in errors:
            print(f"   • {e}")
        sys.exit(1)
    print("✅ Schema válido")

    # ── 3. Dry run ───────────────────────────────────────────────────────────
    if args.dry_run:
        api_key = os.getenv("SIMPLEAPI_KEY", "DRY-RUN-KEY")
        body = build_request_body(dte_clean, tipo_dte)
        print(f"\n🔵 DRY RUN — no se envía nada.")
        print(f"   URL: POST {SIMPLEAPI_BASE}{ENDPOINT}")
        print(f"   Authorization: {api_key[:8]}...")
        print(f"   Payload keys: {list(body.keys())}")
        print(f"   DTE MntTotal: ${dte_clean['Encabezado']['Totales']['MntTotal']:,}")
        return

    # ── 4. Check prerequisites & send ────────────────────────────────────────
    api_key = check_prerequisites()
    body = build_request_body(dte_clean, tipo_dte)

    print(f"\nEnviando a SimpleAPI...")
    status_code, response_text = send_to_api(body, api_key)

    # ── 5. Handle response ───────────────────────────────────────────────────
    if status_code == 200:
        out_path = save_response(response_text, draft_path)
        print(f"\n✅ DTE firmado recibido → {out_path}")
        print(f"   El XML firmado está listo para enviar al SII o al receptor.")
        print(f"\n── Primeras 500 chars del XML ──────────────────────────")
        print(response_text[:500])
        print("────────────────────────────────────────────────────────")
    elif status_code == 401:
        print(f"\n❌ HTTP 401 — API Key inválida.")
        print(f"   Verifica SIMPLEAPI_KEY en .env")
        sys.exit(1)
    elif status_code == 400:
        print(f"\n❌ HTTP 400 — DTE rechazado por SimpleAPI:")
        print(f"   {response_text[:1000]}")
        print(f"\n   Consejo: Revisa los campos en el DTE JSON con validate_dte_schema.py")
        sys.exit(1)
    else:
        print(f"\n❌ HTTP {status_code} — Respuesta inesperada:")
        print(f"   {response_text[:500]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
