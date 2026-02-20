# SimplyAPI

Chilean DTE integration for car consignment, built on the [SimpleAPI.cl](https://simpleapi.cl) platform.
Follows the 3-layer architecture: Directives → Orchestration (AI) → Execution (Python scripts).

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your keys

# 3. Test all modules (no API key needed)
python execution/consignment_logic.py
python execution/validate_dte_schema.py
python execution/inventory_manager.py list
```

## Project Structure
```
SimplyAPI/
├── directives/                  # SOPs (instructions for the AI orchestrator)
│   ├── inventory.md             # Inventory & consignment logic
│   └── simpleapi_dte.md         # DTE generation & SimpleAPI integration
├── execution/                   # Deterministic Python scripts
│   ├── inventory_manager.py     # CRUD for car inventory (SQLite)
│   ├── consignment_logic.py     # Commission & IVA calculation
│   ├── validate_dte_schema.py   # DTE schema validator
│   ├── build_liquidacion_factura.py  # Build TipoDTE=43 JSON
│   ├── build_guia_despacho.py        # Build TipoDTE=52 JSON
│   └── send_dte_sandbox.py           # Send to SimpleAPI & save signed XML
├── credentials/                 # Gitignored: .pfx cert, CAF XMLs
├── data/                        # SQLite inventory DB
└── .tmp/                        # Intermediate files (gitignored)
    ├── draft_dtes/              # JSON drafts before sending
    └── responses/               # Signed XML responses from SimpleAPI
```

## Workflow

```
1. Add car to inventory
   python execution/inventory_manager.py add --patente="AB1234" \
     --brand="Toyota" --model="Corolla" --year=2020 \
     --owner_name="Juan Pérez" --owner_rut="12345678-9" \
     --owner_price=8500000 --selling_price=10000000

2. View commission breakdown
   python execution/inventory_manager.py show --id=1

3. Generate DTE draft
   python execution/build_liquidacion_factura.py --car_id=1

4. Validate schema
   python execution/validate_dte_schema.py --file .tmp/draft_dtes/liq_factura_1.json

5. Send to SimpleAPI sandbox (requires API key + CAF + cert)
   python execution/send_dte_sandbox.py --draft .tmp/draft_dtes/liq_factura_1.json

6. Update car status
   python execution/inventory_manager.py update --id=1 --status=sent_dte
```

## DTE Types Supported
| TipoDTE | Name | Script |
|---------|------|--------|
| 43 | Liquidación Factura | `build_liquidacion_factura.py` |
| 52 | Guía de Despacho | `build_guia_despacho.py` |

## To Go Live (Production)
1. Buy Digital Certificate `.pfx` from [e-certchile.cl](https://www.e-certchile.cl) or [acepta.com](https://www.acepta.com)
2. Get a free SimpleAPI key at [panel.simpleapi.cl](https://panel.simpleapi.cl)
3. Download CAF files from the SII for the certification environment
4. Pass the SII "Set de Pruebas" (use SimpleAPI's Auto-pass feature)
5. Download production CAFs and go live
