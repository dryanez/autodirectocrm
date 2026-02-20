"""
build_guia_despacho.py — Builds a Guía de Despacho (TipoDTE=52) JSON.

Used when physically transferring a vehicle between locations. Lighter than a
Liquidación Factura — no IVA math required, simpler structure.

Usage:
    python execution/build_guia_despacho.py --car_id 1
    python execution/build_guia_despacho.py --car_id 1 --folio 3 --indtraslado 1
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


def load_env():
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def get_car(car_id: int) -> dict:
    db_path = os.getenv("DB_PATH", str(ROOT / "data" / "inventory.db"))
    if not Path(db_path).exists():
        print(f"❌ No existe la base de datos en: {db_path}")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM cars WHERE id=?", (car_id,)).fetchone()
    conn.close()
    if not row:
        print(f"❌ No existe auto con ID={car_id}")
        sys.exit(1)
    return dict(row)


# IndTraslado codes (SII):
# 1 = Operación constituye venta
# 2 = Ventas por efectuar
# 3 = Consignaciones
# 4 = Entrega gratuita
# 5 = Traslados internos
# 6 = Otros traslados no venta
# 7 = Guía de devolución
INDTRASLADO_LABELS = {
    1: "Operación constituye venta",
    2: "Ventas por efectuar",
    3: "Consignaciones",
    5: "Traslados internos",
    6: "Otros traslados no venta",
}


def build_guia_despacho(car: dict, folio: int, fecha: Optional[str] = None,
                         indtraslado: int = 3) -> dict:
    """
    Build Guía de Despacho JSON (TipoDTE=52) for a vehicle transfer.
    """
    fecha = fecha or date.today().isoformat()
    emisor_rut = os.getenv("EMPRESA_RUT", "76000000-0")
    emisor_rzn = os.getenv("EMPRESA_RAZON_SOCIAL", "AutoDirecto SpA")
    emisor_giro = os.getenv("EMPRESA_GIRO", "Compraventa de Vehículos Usados")
    emisor_dir = os.getenv("EMPRESA_DIRECCION", "Av. Providencia 123")
    emisor_cmna = os.getenv("EMPRESA_COMUNA", "Providencia")

    vehicle_label = f"{car['brand']} {car['model']}"
    if car.get("year"):
        vehicle_label += f" {car['year']}"
    if car.get("color"):
        vehicle_label += f" color {car['color']}"

    dte = {
        "Encabezado": {
            "IdDoc": {
                "TipoDTE": 52,
                "Folio": folio,
                "FchEmis": fecha,
                "IndTraslado": indtraslado,
            },
            "Emisor": {
                "RUTEmisor":    emisor_rut,
                "RznSoc":       emisor_rzn,
                "GiroEmis":     emisor_giro,
                "DirOrigen":    emisor_dir,
                "CmnaOrigen":   emisor_cmna,
            },
            "Receptor": {
                "RUTRecep":    car["owner_rut"],
                "RznSocRecep": car["owner_name"],
                "DirRecep":    "Sin dirección registrada",
                "CmnaRecep":   "Santiago",
            },
            "Totales": {
                "MntTotal": car["selling_price"],  # Reference value (no IVA in Guía)
            },
        },
        "Detalle": [
            {
                "NroLinDet": 1,
                "NmbItem":   f"Traslado vehículo: {vehicle_label}",
                "DscItem":   (
                    f"Patente: {car['patente']} | "
                    f"VIN: {car.get('vin') or 'N/A'} | "
                    f"Motivo: {INDTRASLADO_LABELS.get(indtraslado, str(indtraslado))}"
                ),
                "QtyItem":   1,
                "PrcItem":   car["selling_price"],
                "MontoItem": car["selling_price"],
            }
        ],
        "_meta": {
            "car_id":        car["id"],
            "patente":       car["patente"],
            "indtraslado":   indtraslado,
            "generated_at":  fecha,
        },
    }

    return dte


def main():
    load_env()
    parser = argparse.ArgumentParser(
        description="Build a Guía de Despacho (TipoDTE=52) JSON for vehicle transfer."
    )
    parser.add_argument("--car_id", type=int, required=True)
    parser.add_argument("--folio", type=int, default=1)
    parser.add_argument("--fecha", help="Fecha emisión YYYY-MM-DD (default: hoy)")
    parser.add_argument("--indtraslado", type=int, default=3,
                        help="Código de tipo de traslado (3=Consignaciones, default)")
    parser.add_argument("--output", help="Ruta de salida del JSON (opcional)")

    args = parser.parse_args()
    car = get_car(args.car_id)
    dte = build_guia_despacho(car, folio=args.folio, fecha=args.fecha,
                               indtraslado=args.indtraslado)

    if args.output:
        out_path = Path(args.output)
    else:
        out_dir = ROOT / ".tmp" / "draft_dtes"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"guia_despacho_{args.car_id}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dte, ensure_ascii=False, indent=2), encoding="utf-8")

    tipo_str = INDTRASLADO_LABELS.get(args.indtraslado, str(args.indtraslado))
    print(f"\n✅ Guía de Despacho generada → {out_path}")
    print(f"   Folio: {args.folio}  |  Fecha: {dte['Encabezado']['IdDoc']['FchEmis']}")
    print(f"   Motivo traslado: {tipo_str}")
    print(f"   Vehículo: {car['brand']} {car['model']} — Patente {car['patente']}")
    print(f"\n   Siguiente paso: python execution/validate_dte_schema.py --file {out_path}")
    print(json.dumps(dte, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
