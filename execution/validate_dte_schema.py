"""
validate_dte_schema.py — Validates a DTE JSON dict before sending to SimpleAPI.

Runs structural checks against SII-required fields for TipoDTE 43 and 52.
Always run this before send_dte_sandbox.py to catch errors early (free, instant).

Usage:
    python execution/validate_dte_schema.py --file .tmp/draft_dtes/liq_factura_1.json

Self-test (no args):
    python execution/validate_dte_schema.py
"""

import argparse
import json
import sys
from pathlib import Path


# ─── Required field definitions ──────────────────────────────────────────────
# Format: dot-notation path to required field
COMMON_REQUIRED = [
    "Encabezado.IdDoc.TipoDTE",
    "Encabezado.IdDoc.Folio",
    "Encabezado.IdDoc.FchEmis",
    "Encabezado.Emisor.RUTEmisor",
    "Encabezado.Emisor.RznSoc",
    "Encabezado.Emisor.GiroEmis",
    "Encabezado.Emisor.DirOrigen",
    "Encabezado.Emisor.CmnaOrigen",
    "Encabezado.Receptor.RUTRecep",
    "Encabezado.Receptor.RznSocRecep",
    "Encabezado.Receptor.DirRecep",
    "Encabezado.Receptor.CmnaRecep",
    "Encabezado.Totales.MntTotal",
]

# Additional required for Liquidación Factura (43)
LIQUIDACION_REQUIRED = [
    "Encabezado.Totales.MntNeto",
    "Encabezado.Totales.TasaIVA",
    "Encabezado.Totales.IVA",
]

TIPO_DTE_NAMES = {
    33: "Factura Electrónica",
    34: "Factura No Afecta",
    43: "Liquidación Factura Electrónica",
    52: "Guía de Despacho Electrónica",
    56: "Nota de Débito Electrónica",
    61: "Nota de Crédito Electrónica",
    110: "Factura de Exportación Electrónica",
}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def get_nested(d: dict, dotpath: str):
    """Navigate dot-notation path in nested dict. Returns None if missing."""
    keys = dotpath.split(".")
    current = d
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def validate(dte: dict) -> list[str]:
    """
    Validate a DTE dict. Returns a list of error strings.
    Empty list = valid.
    """
    errors = []

    tipo = get_nested(dte, "Encabezado.IdDoc.TipoDTE")
    if tipo is None:
        errors.append("MISSING: Encabezado.IdDoc.TipoDTE")
        return errors  # Can't validate further without knowing the type

    if tipo not in TIPO_DTE_NAMES:
        errors.append(f"INVALID TipoDTE: {tipo} — not a recognized SII document type")

    # Check common required fields
    for field in COMMON_REQUIRED:
        if get_nested(dte, field) is None:
            errors.append(f"MISSING: {field}")

    # Additional checks for Liquidación Factura
    if tipo == 43:
        for field in LIQUIDACION_REQUIRED:
            if get_nested(dte, field) is None:
                errors.append(f"MISSING (TipoDTE=43): {field}")

        # Validate that Detalle exists and has at least one item
        if not dte.get("Detalle") or not isinstance(dte["Detalle"], list):
            errors.append("MISSING: Detalle (must be a list with at least 1 item)")
        else:
            for i, item in enumerate(dte["Detalle"]):
                for f in ["NroLinDet", "NmbItem", "QtyItem", "PrcItem", "MontoItem"]:
                    if f not in item:
                        errors.append(f"MISSING in Detalle[{i}]: {f}")

    # Validate RUT format (basic)
    for rut_field in ["Encabezado.Emisor.RUTEmisor", "Encabezado.Receptor.RUTRecep"]:
        rut = get_nested(dte, rut_field)
        if rut:
            import re
            if not re.match(r"^\d{7,8}-[\dKk]$", str(rut).strip()):
                errors.append(f"INVALID RUT format at {rut_field}: '{rut}' — expected format: 12345678-9")

    # Validate date format FchEmis
    fecha = get_nested(dte, "Encabezado.IdDoc.FchEmis")
    if fecha:
        import re
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(fecha)):
            errors.append(f"INVALID FchEmis format: '{fecha}' — expected YYYY-MM-DD")

    # Math cross-check for Liquidación
    if tipo == 43:
        neto = get_nested(dte, "Encabezado.Totales.MntNeto")
        tasa = get_nested(dte, "Encabezado.Totales.TasaIVA")
        iva = get_nested(dte, "Encabezado.Totales.IVA")
        total = get_nested(dte, "Encabezado.Totales.MntTotal")
        if all(v is not None for v in [neto, tasa, iva, total]):
            expected_iva = round(neto * tasa / 100)
            if abs(expected_iva - iva) > 1:  # Allow 1 CLP rounding diff
                errors.append(
                    f"MATH ERROR: IVA={iva:,} but expected round({neto:,} × {tasa}%) = {expected_iva:,}"
                )
            expected_total = neto + iva
            if abs(expected_total - total) > 1:
                errors.append(
                    f"MATH ERROR: MntTotal={total:,} but expected MntNeto+IVA = {expected_total:,}"
                )

    return errors


# ─── CLI ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Validate a DTE JSON before sending to SimpleAPI.")
    parser.add_argument("--file", help="Path to a DTE JSON file")
    parser.add_argument("--self-test", action="store_true", help="Run built-in self-tests")
    args = parser.parse_args()

    if args.file:
        path = Path(args.file)
        if not path.exists():
            print(f"❌ File not found: {path}")
            sys.exit(1)
        dte = json.loads(path.read_text(encoding="utf-8"))
        tipo = get_nested(dte, "Encabezado.IdDoc.TipoDTE")
        label = TIPO_DTE_NAMES.get(tipo, f"TipoDTE={tipo}")
        print(f"\nValidando: {path.name} ({label})")
        errors = validate(dte)
        if errors:
            print(f"\n❌ {len(errors)} error(s) encontrados:")
            for e in errors:
                print(f"   • {e}")
            sys.exit(1)
        else:
            print("✅ Schema válido. Listo para enviar a SimpleAPI.")
    else:
        # Self-test
        _self_test()


def _self_test():
    print("\n=== Self-test: validate_dte_schema.py ===\n")

    # Test 1: valid Liquidación Factura
    valid_dte = {
        "Encabezado": {
            "IdDoc": {
                "TipoDTE": 43,
                "Folio": 1,
                "FchEmis": "2026-02-18",
            },
            "Emisor": {
                "RUTEmisor": "76123456-7",
                "RznSoc": "AutoDirecto SpA",
                "GiroEmis": "Compraventa de Vehículos Usados",
                "DirOrigen": "Av. Providencia 123",
                "CmnaOrigen": "Providencia",
            },
            "Receptor": {
                "RUTRecep": "12345678-9",
                "RznSocRecep": "Juan Pérez",
                "DirRecep": "Los Alerces 456",
                "CmnaRecep": "Las Condes",
            },
            "Totales": {
                "MntNeto": 1_000_000,
                "TasaIVA": 19,
                "IVA": 190_000,
                "MntTotal": 1_190_000,
            },
        },
        "Detalle": [
            {
                "NroLinDet": 1,
                "NmbItem": "Comisión venta Toyota Corolla 2020 (AB1234)",
                "QtyItem": 1,
                "PrcItem": 1_000_000,
                "MontoItem": 1_000_000,
            }
        ],
    }
    errors = validate(valid_dte)
    assert errors == [], f"Expected no errors, got: {errors}"
    print("✅ Test 1 passed: DTE válido sin errores")

    # Test 2: missing fields
    bad_dte = {"Encabezado": {"IdDoc": {"TipoDTE": 43, "Folio": 1}}}
    errors = validate(bad_dte)
    assert len(errors) > 3, f"Expected multiple errors, got: {errors}"
    print(f"✅ Test 2 passed: {len(errors)} errores detectados correctamente en DTE incompleto")

    # Test 3: math error
    math_error_dte = dict(valid_dte)
    import copy
    math_error_dte = copy.deepcopy(valid_dte)
    math_error_dte["Encabezado"]["Totales"]["IVA"] = 999  # wrong IVA
    errors = validate(math_error_dte)
    assert any("MATH ERROR" in e for e in errors), f"Expected math error, got: {errors}"
    print("✅ Test 3 passed: Error de cálculo IVA detectado correctamente")

    print("\n✅ Todos los self-tests pasaron.")


if __name__ == "__main__":
    main()
