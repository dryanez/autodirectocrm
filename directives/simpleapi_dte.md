# Directive: DTE Generation & SimpleAPI Submission

## Purpose
Generate Chilean Electronic Tax Documents (DTE) for car consignment operations
and submit them to the SII via SimpleAPI.

## DTE Types Used
| TipoDTE | Name | When to use |
|---------|------|-------------|
| 43 | Liquidación Factura Electrónica | Main document for car consignment — issued to the car owner after sale |
| 52 | Guía de Despacho Electrónica | Physical vehicle transfer between locations |

## Prerequisites
Before submitting to the API, you need:
1. **SIMPLEAPI_KEY** — Free at https://panel.simpleapi.cl
2. **Digital Certificate (.pfx)** — Buy from e-certchile.cl or acepta.com (~$20 USD/year)
3. **CAF file (.xml)** — Download from SII once your company is enrolled. One per DTE type.
   - For sandbox/certification: download the CAF for "Ambiente de Certificación" in SII
   - For production: download the CAF for "Ambiente de Producción"

## API Reference
- **Base URL**: `https://api.simpleapi.cl`
- **Auth header**: `Authorization: YOUR_API_KEY`
- **Rate limits**: 3 req/sec, 40 req/min

### Key Endpoint
```
POST /api/v1/dte/documento
Content-Type: application/json
Authorization: {SIMPLEAPI_KEY}
```

**Request body** (multipart or JSON — see script for exact format):
- `documento`: The DTE JSON (Encabezado + Detalle + Totales)
- `certificado`: Base64-encoded .pfx file content
- `certificado_password`: Certificate password
- `caf`: The CAF XML content for the DTE type

**Response**: Signed XML string (the stamped DTE), ready to send to receptor or SII.

## Workflow
```
1. Get car record from inventory (inventory_manager.py show --id=X)
2. Calculate commission (consignment_logic.py)
3. Build DTE JSON (build_liquidacion_factura.py --car_id=X)
   → Saves to .tmp/draft_dtes/liq_factura_{id}.json
4. Validate schema (validate_dte_schema.py --file=.tmp/draft_dtes/...)
5. Send to sandbox (send_dte_sandbox.py --draft=.tmp/draft_dtes/...)
   → Saves signed XML to .tmp/responses/
6. Update car status: inventory_manager.py update --id=X --status=sent_dte
```

## Sandbox vs Production
- **Sandbox**: Use a CAF downloaded from SII's "Ambiente de Certificación"
- **Production**: Use a CAF from "Ambiente de Producción"
- The SimpleAPI endpoint URL is the SAME for both — the CAF file determines the environment.
- SimpleAPI also offers "Auto-pass Set de Pruebas" feature for bypassing manual SII certification.

## The SII Certification Set (Set de Pruebas)
Before going live, SII requires you to pass 5–10 test scenarios:
1. Log in to SII → Facturación Electrónica → Set de Pruebas
2. Download the list of required DTEs to generate
3. Generate each one using this system and submit via `send_dte_sandbox.py`
4. SII reviews and approves → unlocks Production CAFs

SimpleAPI's "Auto-pass" feature handles much of this automatically.

## Error Handling / Learnings
- HTTP 429: Rate limited. The script automatically sleeps 2 seconds and retries (up to 3 times).
- HTTP 401: Invalid API key. Check `.env` SIMPLEAPI_KEY value.
- HTTP 400 with SII error code: Usually a malformed DTE field. Check `validate_dte_schema.py` output first.
- CAF exhausted (no more folios): Download a new CAF from SII for the same DTE type.
- The certificate (.pfx) and CAF must match the same RUT (empresa RUT). Mismatches cause silent 400 errors.
