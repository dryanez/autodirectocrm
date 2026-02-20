# Directive: Inventory & Consignment Module

## Purpose
Manage the car inventory for the consignment business.
Track each vehicle, its owner, the agreed price, the listed price, and the commission.
This data is the source of truth that feeds the DTE generator.

## Tools
- `execution/inventory_manager.py` — CRUD for cars (SQLite)
- `execution/consignment_logic.py` — Commission and tax calculation

## Database Schema
Located at `$DB_PATH` (default: `./data/inventory.db`).

```sql
CREATE TABLE cars (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    patente          TEXT UNIQUE NOT NULL,
    vin              TEXT,
    brand            TEXT NOT NULL,
    model            TEXT NOT NULL,
    year             INTEGER,
    color            TEXT,
    owner_name       TEXT NOT NULL,
    owner_rut        TEXT NOT NULL,      -- RUT del consignante
    owner_email      TEXT,
    owner_phone      TEXT,
    owner_price      INTEGER NOT NULL,   -- Precio que el dueño quiere recibir (CLP)
    selling_price    INTEGER NOT NULL,   -- Precio de venta al público (CLP)
    commission_pct   REAL DEFAULT 0.10, -- Tu comisión (ej: 0.10 = 10%)
    status           TEXT DEFAULT 'available', -- available | sold | draft_dte | sent_dte
    notes            TEXT,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at       TEXT DEFAULT CURRENT_TIMESTAMP
);
```

## Commission Logic (Chile — Liquidación Factura)

In a Chilean consignment (liquidación), the legal structure is:

1. You sell the car at `selling_price` to a buyer.
2. You issue a **Liquidación Factura (TipoDTE=43)** to the **owner (consignor)**.
3. The Liquidación shows:
   - Gross amount sold: `selling_price`
   - Your commission: `selling_price × commission_pct`
   - IVA on commission only (19%): `commission × 0.19`
   - Net to owner: `selling_price − commission − IVA_on_commission`
4. The owner receives the net amount.

Run `execution/consignment_logic.py` to compute these values from a car record.

## Common Commands

```bash
# Add a car
python execution/inventory_manager.py add \
  --patente="AB1234" --brand="Toyota" --model="Corolla" \
  --year=2020 --color="Blanco" \
  --owner_name="Juan Pérez" --owner_rut="12345678-9" \
  --owner_price=8500000 --selling_price=10000000

# List all cars
python execution/inventory_manager.py list

# Show one car (with commission breakdown)
python execution/inventory_manager.py show --id=1

# Update status after DTE sent
python execution/inventory_manager.py update --id=1 --status=sent_dte

# Delete a car
python execution/inventory_manager.py delete --id=1
```

## Edge Cases & Learnings
- `owner_rut` must be a valid Chilean RUT (with dígito verificador). The script validates format but NOT existence in SII.
- `selling_price` must be >= `owner_price` or you're losing money — the script warns but doesn't block.
- Commission IVA (19%) is charged on your commission amount ONLY, not on the full sale price. This is the legally correct treatment for Liquidaciones.
- If the owner is IVA-exempt (e.g., persona natural non-commercial), note it in `notes` and set Commission IVA handling manually in the DTE.
