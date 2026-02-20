"""
consignment_logic.py — Commission and tax calculation for car consignment.

In Chile, a car consignment uses a Liquidación Factura (TipoDTE=43).
The IVA (19%) applies ONLY on your commission, not on the full sale price.

Usage (standalone test):
    python execution/consignment_logic.py

Usage (as module):
    from execution.consignment_logic import calculate_commission
    result = calculate_commission(car_dict)
"""

from __future__ import annotations
from typing import TypedDict


class CommissionResult(TypedDict):
    selling_price: int       # Precio de venta al público
    owner_price: int         # Precio que quiere recibir el dueño
    commission_pct: float    # % de comisión (e.g. 0.10)
    commission_amount: int   # Monto comisión (redondeado a entero CLP)
    iva_on_commission: int   # IVA 19% sobre la comisión
    gross_commission: int    # commission_amount + iva_on_commission
    net_to_owner: int        # Lo que recibe el dueño
    margin: int              # Tu ingreso neto = commission_amount (sin IVA)


def calculate_commission(car: dict, iva_rate: float = 0.19) -> CommissionResult:
    """
    Calculate consignment financials from a car record dict.

    Args:
        car: Dict with at least: selling_price, owner_price, commission_pct
        iva_rate: Current Chilean IVA rate (default 0.19 = 19%)

    Returns:
        CommissionResult dict with full financial breakdown.
    """
    selling_price = int(car["selling_price"])
    owner_price = int(car["owner_price"])
    commission_pct = float(car.get("commission_pct", 0.10))

    # Commission is calculated on the total sale price
    commission_amount = round(selling_price * commission_pct)

    # IVA only on commission (Liquidación Factura treatment)
    iva_on_commission = round(commission_amount * iva_rate)

    # Total cost to owner = commission + IVA on commission
    gross_commission = commission_amount + iva_on_commission

    # What the owner actually receives
    net_to_owner = selling_price - gross_commission

    # Your margin (before your own expenses) = commission before IVA
    margin = commission_amount

    return CommissionResult(
        selling_price=selling_price,
        owner_price=owner_price,
        commission_pct=commission_pct,
        commission_amount=commission_amount,
        iva_on_commission=iva_on_commission,
        gross_commission=gross_commission,
        net_to_owner=net_to_owner,
        margin=margin,
    )


def print_breakdown(result: CommissionResult, car_label: str = ""):
    """Pretty-print a commission breakdown."""
    label = f"  {car_label}" if car_label else ""
    print(f"\n{'─'*52}")
    if label:
        print(label)
    print(f"{'─'*52}")
    print(f"  Precio de venta al público : ${result['selling_price']:>12,}")
    print(f"  Precio acordado con dueño  : ${result['owner_price']:>12,}")
    print(f"{'─'*52}")
    print(f"  Comisión ({result['commission_pct']*100:.0f}%)             : ${result['commission_amount']:>12,}")
    print(f"  IVA sobre comisión (19%)   : ${result['iva_on_commission']:>12,}")
    print(f"  Comisión total (con IVA)   : ${result['gross_commission']:>12,}")
    print(f"{'─'*52}")
    print(f"  ► Neto al dueño            : ${result['net_to_owner']:>12,}")
    print(f"  ► Tu ingreso neto          : ${result['margin']:>12,}")

    if result["net_to_owner"] < result["owner_price"]:
        deficit = result["owner_price"] - result["net_to_owner"]
        print(f"\n  ⚠️  ATENCIÓN: El dueño recibe ${deficit:,} MENOS de lo que espera.")
        print(f"     Ajusta el precio de venta o la comisión.")
    print(f"{'─'*52}\n")


# ─── Self-test ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n=== Test 1: Comisión 10% — Toyota Corolla ===")
    car1 = {
        "selling_price": 10_000_000,
        "owner_price":    8_500_000,
        "commission_pct": 0.10,
    }
    result1 = calculate_commission(car1)
    print_breakdown(result1, "Toyota Corolla 2020")

    # Assertions
    assert result1["commission_amount"] == 1_000_000, "Commission amount mismatch"
    assert result1["iva_on_commission"] == 190_000,   "IVA mismatch"
    assert result1["net_to_owner"] == 8_810_000,      "Net to owner mismatch"
    print("  ✅ Test 1 passed")

    print("\n=== Test 2: Comisión 8% — BMW X5  ===")
    car2 = {
        "selling_price": 25_000_000,
        "owner_price":   23_000_000,
        "commission_pct": 0.08,
    }
    result2 = calculate_commission(car2)
    print_breakdown(result2, "BMW X5 2022")
    assert result2["commission_amount"] == 2_000_000
    assert result2["net_to_owner"] < car2["owner_price"], "Should warn: net < owner_price"
    print("  ✅ Test 2 passed (net < owner_price warning expected above)")

    print("\n✅ Todos los tests pasaron.")
