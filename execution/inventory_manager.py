"""
inventory_manager.py — CRUD for the car inventory (SQLite-backed).

Usage:
    python execution/inventory_manager.py add --patente="AB1234" --brand="Toyota" \
        --model="Corolla" --year=2020 --color="Blanco" \
        --owner_name="Juan Pérez" --owner_rut="12345678-9" \
        --owner_price=8500000 --selling_price=10000000

    python execution/inventory_manager.py list
    python execution/inventory_manager.py show --id=1
    python execution/inventory_manager.py update --id=1 --status=sold
    python execution/inventory_manager.py delete --id=1
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ─── Config ─────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", str(ROOT / "data" / "inventory.db"))


# ─── Database Setup ──────────────────────────────────────────────────────────
def get_connection():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create the cars table if it doesn't exist."""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cars (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                patente          TEXT UNIQUE NOT NULL,
                vin              TEXT,
                brand            TEXT NOT NULL,
                model            TEXT NOT NULL,
                year             INTEGER,
                color            TEXT,
                owner_name       TEXT NOT NULL,
                owner_rut        TEXT NOT NULL,
                owner_email      TEXT,
                owner_phone      TEXT,
                owner_price      INTEGER NOT NULL,
                selling_price    INTEGER NOT NULL,
                commission_pct   REAL DEFAULT 0.10,
                status           TEXT DEFAULT 'available',
                notes            TEXT,
                created_at       TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at       TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


# ─── RUT Validation ──────────────────────────────────────────────────────────
def validate_rut(rut: str) -> bool:
    """Basic Chilean RUT format check (e.g. 12345678-9 or 76543210-K)."""
    import re
    return bool(re.match(r"^\d{7,8}-[\dKk]$", rut.strip()))


# ─── CRUD Operations ─────────────────────────────────────────────────────────
def add_car(args):
    if not validate_rut(args.owner_rut):
        print(f"❌ RUT inválido: '{args.owner_rut}'. Formato esperado: 12345678-9")
        sys.exit(1)

    if args.selling_price < args.owner_price:
        print(f"⚠️  Advertencia: selling_price ({args.selling_price:,}) < owner_price ({args.owner_price:,}). Perderías dinero.")

    with get_connection() as conn:
        try:
            conn.execute("""
                INSERT INTO cars
                    (patente, vin, brand, model, year, color,
                     owner_name, owner_rut, owner_email, owner_phone,
                     owner_price, selling_price, commission_pct, notes)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                args.patente.upper(),
                getattr(args, "vin", None),
                args.brand,
                args.model,
                getattr(args, "year", None),
                getattr(args, "color", None),
                args.owner_name,
                args.owner_rut,
                getattr(args, "owner_email", None),
                getattr(args, "owner_phone", None),
                args.owner_price,
                args.selling_price,
                getattr(args, "commission_pct", 0.10),
                getattr(args, "notes", None),
            ))
            conn.commit()
            car_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            print(f"✅ Auto agregado: ID={car_id}, Patente={args.patente.upper()}")
        except sqlite3.IntegrityError:
            print(f"❌ Ya existe un auto con patente '{args.patente.upper()}'.")
            sys.exit(1)


def list_cars(args):
    with get_connection() as conn:
        status_filter = getattr(args, "status", None)
        if status_filter:
            rows = conn.execute("SELECT * FROM cars WHERE status=? ORDER BY id", (status_filter,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM cars ORDER BY id").fetchall()

    if not rows:
        print("No hay autos en el inventario.")
        return

    # Print table
    print(f"\n{'ID':<4} {'Patente':<8} {'Marca':<12} {'Modelo':<14} {'Año':<5} "
          f"{'Dueño':<20} {'P.Dueño':>12} {'P.Venta':>12} {'Com%':>6} {'Estado':<12}")
    print("─" * 110)
    for r in rows:
        print(f"{r['id']:<4} {r['patente']:<8} {r['brand']:<12} {r['model']:<14} "
              f"{str(r['year'] or ''):<5} {r['owner_name']:<20} "
              f"${r['owner_price']:>10,} ${r['selling_price']:>10,} "
              f"{r['commission_pct']*100:>5.0f}% {r['status']:<12}")
    print(f"\nTotal: {len(rows)} autos")


def show_car(args):
    from execution.consignment_logic import calculate_commission
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM cars WHERE id=?", (args.id,)).fetchone()
    if not row:
        print(f"❌ No existe auto con ID={args.id}")
        sys.exit(1)

    cal = calculate_commission(dict(row))

    print(f"\n{'─'*50}")
    print(f"  Auto ID: {row['id']}  |  Patente: {row['patente']}")
    print(f"{'─'*50}")
    print(f"  Marca / Modelo : {row['brand']} {row['model']} {row['year'] or ''}")
    print(f"  Color          : {row['color'] or '—'}")
    print(f"  VIN            : {row['vin'] or '—'}")
    print(f"  Estado         : {row['status']}")
    print(f"  Notas          : {row['notes'] or '—'}")
    print(f"\n  Propietario")
    print(f"  ├─ Nombre   : {row['owner_name']}")
    print(f"  ├─ RUT      : {row['owner_rut']}")
    print(f"  ├─ Email    : {row['owner_email'] or '—'}")
    print(f"  └─ Teléfono : {row['owner_phone'] or '—'}")
    print(f"\n  Financiero")
    print(f"  ├─ Precio dueño    : ${row['owner_price']:>12,}")
    print(f"  ├─ Precio venta    : ${row['selling_price']:>12,}")
    print(f"  ├─ Comisión ({row['commission_pct']*100:.0f}%) : ${cal['commission_amount']:>12,}")
    print(f"  ├─ IVA comisión    : ${cal['iva_on_commission']:>12,}")
    print(f"  └─ Neto al dueño   : ${cal['net_to_owner']:>12,}")
    print(f"{'─'*50}\n")


def update_car(args):
    allowed = {"status", "selling_price", "owner_price", "commission_pct",
               "owner_email", "owner_phone", "notes", "vin", "color"}
    updates = {k: v for k, v in vars(args).items()
               if k in allowed and v is not None}

    if not updates:
        print("❌ No se especificó ningún campo para actualizar.")
        sys.exit(1)

    with get_connection() as conn:
        row = conn.execute("SELECT id FROM cars WHERE id=?", (args.id,)).fetchone()
        if not row:
            print(f"❌ No existe auto con ID={args.id}")
            sys.exit(1)

        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [datetime.now().isoformat(), args.id]
        conn.execute(
            f"UPDATE cars SET {set_clause}, updated_at=? WHERE id=?", values
        )
        conn.commit()
        print(f"✅ Auto ID={args.id} actualizado: {updates}")


def delete_car(args):
    with get_connection() as conn:
        row = conn.execute("SELECT patente FROM cars WHERE id=?", (args.id,)).fetchone()
        if not row:
            print(f"❌ No existe auto con ID={args.id}")
            sys.exit(1)
        conn.execute("DELETE FROM cars WHERE id=?", (args.id,))
        conn.commit()
        print(f"✅ Auto ID={args.id} (Patente: {row['patente']}) eliminado.")


# ─── CLI ─────────────────────────────────────────────────────────────────────
def build_parser():
    parser = argparse.ArgumentParser(description="Gestión de inventario de autos en consignación.")
    sub = parser.add_subparsers(dest="command", required=True)

    # add
    p_add = sub.add_parser("add", help="Agregar un auto")
    p_add.add_argument("--patente", required=True)
    p_add.add_argument("--brand", required=True)
    p_add.add_argument("--model", required=True)
    p_add.add_argument("--owner_name", required=True)
    p_add.add_argument("--owner_rut", required=True)
    p_add.add_argument("--owner_price", type=int, required=True)
    p_add.add_argument("--selling_price", type=int, required=True)
    p_add.add_argument("--year", type=int)
    p_add.add_argument("--color")
    p_add.add_argument("--vin")
    p_add.add_argument("--owner_email")
    p_add.add_argument("--owner_phone")
    p_add.add_argument("--commission_pct", type=float, default=0.10)
    p_add.add_argument("--notes")

    # list
    p_list = sub.add_parser("list", help="Listar autos")
    p_list.add_argument("--status", help="Filtrar por estado (available, sold, etc.)")

    # show
    p_show = sub.add_parser("show", help="Ver detalle de un auto con desglose financiero")
    p_show.add_argument("--id", type=int, required=True)

    # update
    p_update = sub.add_parser("update", help="Actualizar campos de un auto")
    p_update.add_argument("--id", type=int, required=True)
    p_update.add_argument("--status")
    p_update.add_argument("--selling_price", type=int)
    p_update.add_argument("--owner_price", type=int)
    p_update.add_argument("--commission_pct", type=float)
    p_update.add_argument("--owner_email")
    p_update.add_argument("--owner_phone")
    p_update.add_argument("--notes")
    p_update.add_argument("--vin")
    p_update.add_argument("--color")

    # delete
    p_delete = sub.add_parser("delete", help="Eliminar un auto")
    p_delete.add_argument("--id", type=int, required=True)

    return parser


if __name__ == "__main__":
    # Load .env if available
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    init_db()
    parser = build_parser()
    args = parser.parse_args()

    commands = {
        "add": add_car,
        "list": list_cars,
        "show": show_car,
        "update": update_car,
        "delete": delete_car,
    }
    commands[args.command](args)
