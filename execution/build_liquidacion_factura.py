"""
build_liquidacion_factura.py — Builds a Liquidación Factura (TipoDTE=43) JSON.

This is the primary DTE for car consignment in Chile.
The document is issued by YOU (the consignment agent) TO the car owner,
documenting the sale and your commission deduction.

Usage:
    python execution/build_liquidacion_factura.py --car_id 1
    python execution/build_liquidacion_factura.py --car_id 1 --folio 5 --output .tmp/draft_dtes/custom.json

Output:
    Saves JSON to .tmp/draft_dtes/liq_factura_{car_id}.json
    Also prints the JSON to stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ─── Load .env ────────────────────────────────────────────────────────────────
def load_env():
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


# ─── Config from env ──────────────────────────────────────────────────────────
def get_emisor_config():
    return {
        "RUTEmisor":  os.getenv("EMPRESA_RUT", "76000000-0"),
        "RznSoc":     os.getenv("EMPRESA_RAZON_SOCIAL", "AutoDirecto SpA"),
        "GiroEmis":   os.getenv("EMPRESA_GIRO", "Compraventa de Vehículos Usados"),
        "DirOrigen":  os.getenv("EMPRESA_DIRECCION", "Av. Providencia 123"),
        "CmnaOrigen": os.getenv("EMPRESA_COMUNA", "Providencia"),
        "CiudadOrigen": os.getenv("EMPRESA_CIUDAD", "Santiago"),
    }


# ─── Fetch car from DB ────────────────────────────────────────────────────────
def get_car(car_id: int) -> dict:
    db_path = os.getenv("DB_PATH", str(ROOT / "data" / "inventory.db"))
    if not Path(db_path).exists():
        print(f"❌ No existe la base de datos en: {db_path}")
        print("   Ejecuta primero: python execution/inventory_manager.py add ...")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM cars WHERE id=?", (car_id,)).fetchone()
    conn.close()

    if not row:
        print(f"❌ No existe auto con ID={car_id}")
        sys.exit(1)

    return dict(row)


# ─── DTE Builder ─────────────────────────────────────────────────────────────
def build_dte(car: dict, folio: int, fecha: Optional[str] = None) -> dict:
    """
    Build the Liquidación Factura JSON for a given car consignment.

    Structure follows SII's DTE schema for TipoDTE=43.
    """
    from execution.consignment_logic import calculate_commission  # noqa

    cal = calculate_commission(car)
    emisor = get_emisor_config()
    fecha = fecha or date.today().isoformat()

    # Vehicle description for the line item
    car_desc = f"Comisión consignación {car['brand']} {car['model']}"
    if car.get("year"):
        car_desc += f" {car['year']}"
    if car.get("patente"):
        car_desc += f" — Patente {car['patente']}"

    dte = {
        "Encabezado": {
            "IdDoc": {
                "TipoDTE": 43,
                "Folio": folio,
                "FchEmis": fecha,
                "IndServicio": 1,   # 1 = Afecta a IVA (standard for liquidaciones)
            },
            "Emisor": {
                "RUTEmisor":    emisor["RUTEmisor"],
                "RznSoc":       emisor["RznSoc"],
                "GiroEmis":     emisor["GiroEmis"],
                "DirOrigen":    emisor["DirOrigen"],
                "CmnaOrigen":   emisor["CmnaOrigen"],
                "CiudadOrigen": emisor["CiudadOrigen"],
            },
            "Receptor": {
                # Receptor = the car owner (consignor)
                "RUTRecep":     car["owner_rut"],
                "RznSocRecep":  car["owner_name"],
                "DirRecep":     car.get("notes") or "Sin dirección registrada",
                "CmnaRecep":    "Santiago",   # Update if you capture owner location
                "CiudadRecep":  "Santiago",
            },
            "Totales": {
                "MntNeto":   cal["commission_amount"],  # Commission is the taxable base
                "TasaIVA":   19,
                "IVA":       cal["iva_on_commission"],
                "MntTotal":  cal["gross_commission"],
            },
        },
        "Detalle": [
            {
                "NroLinDet": 1,
                "NmbItem":   car_desc,
                "DscItem":   (
                    f"Precio de venta al público: ${cal['selling_price']:,} | "
                    f"Neto al consignante: ${cal['net_to_owner']:,}"
                ),
                "QtyItem":   1,
                "PrcItem":   cal["commission_amount"],
                "MontoItem": cal["commission_amount"],
            }
        ],
        # ─── Metadata (not sent to SII, used internally) ──────────────────
        "_meta": {
            "car_id":        car["id"],
            "patente":       car["patente"],
            "selling_price": cal["selling_price"],
            "net_to_owner":  cal["net_to_owner"],
            "commission_pct": cal["commission_pct"],
            "generated_at":  fecha,
        },
    }

    return dte


# ─── CLI ─────────────────────────────────────────────────────────────────────
def main():
    load_env()
    parser = argparse.ArgumentParser(
        description="Build a Liquidación Factura (TipoDTE=43) JSON for a consignment car."
    )
    parser.add_argument("--car_id", type=int, required=True, help="ID del auto en el inventario")
    parser.add_argument("--folio", type=int, default=1,
                        help="Número de folio (del CAF). Default=1 para pruebas.")
    parser.add_argument("--fecha", help="Fecha emisión YYYY-MM-DD (default: hoy)")
    parser.add_argument("--output", help="Ruta de salida del JSON (opcional)")

    args = parser.parse_args()
    car = get_car(args.car_id)
    dte = build_dte(car, folio=args.folio, fecha=args.fecha)

    # Determine output path
    if args.output:
        out_path = Path(args.output)
    else:
        out_dir = ROOT / ".tmp" / "draft_dtes"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"liq_factura_{args.car_id}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dte, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n✅ DTE generado → {out_path}")
    print(f"   Folio: {args.folio}  |  Fecha: {dte['Encabezado']['IdDoc']['FchEmis']}")
    print(f"   Comisión: ${dte['_meta']['commission_pct']*100:.0f}% = "
          f"${dte['Encabezado']['Totales']['MntNeto']:,}")
    print(f"   MntTotal (con IVA): ${dte['Encabezado']['Totales']['MntTotal']:,}")
    print(f"\n   Siguiente paso: python execution/validate_dte_schema.py --file {out_path}")
    print(json.dumps(dte, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
