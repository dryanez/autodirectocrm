# CAV Worker — Registro Civil Robot Agent 🤖

Standalone microservice that automatically fetches **CAV (Certificado de Anotaciones Vigentes)** from [registrocivil.cl](https://www.registrocivil.cl) for any Chilean vehicle plate.

## How it works

1. **Playwright** launches a headless Chromium browser
2. Navigates to registrocivil.cl with the vehicle plate
3. **Claude Vision** (Anthropic API) reads and solves the CAPTCHA image
4. Submits the form and scrapes the result
5. Returns structured data: owner name, RUT, annotations, vehicle info

## Architecture

```
CRM (Vercel) → POST /cav → CAV Worker (Railway) → Playwright → registrocivil.cl
                                     ↓
                              Claude Vision API
                            (CAPTCHA solving)
```

## Deploy on Railway

1. Create a new Railway project
2. Connect your GitHub repo
3. Set the root directory to `cav_worker/` (or use the Dockerfile)
4. Set environment variables:

| Variable | Description |
|---|---|
| `ANTHROPIC_API_KEY` | Anthropic API key for Claude Vision |
| `CAV_SECRET` | Shared secret (set same value in CRM's env) |
| `PORT` | Railway sets this automatically |

5. Deploy!

## API

### `POST /cav`

**Request:**
```json
{
  "plate": "ABCD12",
  "secret": "your-cav-secret"
}
```

**Response (success):**
```json
{
  "ok": true,
  "plate": "ABCD12",
  "status": "clean",
  "owner_name": "JUAN CARLOS PÉREZ SOTO",
  "owner_rut": "12.345.678-9",
  "vehicle_info": {
    "marca": "HYUNDAI",
    "modelo": "CRETA",
    "year": 2023,
    "color": "BLANCO"
  },
  "annotations": [],
  "annotations_text": "",
  "elapsed_seconds": 8.2
}
```

**Response (annotations found):**
```json
{
  "ok": true,
  "status": "annotations",
  "owner_name": "MARÍA LÓPEZ GONZÁLEZ",
  "annotations": [
    "Prenda a favor de Banco Estado",
    "Prohibición de enajenar"
  ],
  "annotations_text": "Prenda a favor de Banco Estado; Prohibición de enajenar"
}
```

## Cost per lookup

| Component | Cost |
|---|---|
| Railway server | ~$0.001 per request |
| Claude Vision (CAPTCHA) | ~$0.003 per image |
| **Total** | **~$0.004 per CAV** (~$4 per 1,000 lookups) |

## Local development

```bash
cd cav_worker
pip install -r requirements.txt
playwright install chromium
export ANTHROPIC_API_KEY="sk-ant-..."
export CAV_SECRET="test"
python app.py
```

Then test:
```bash
curl -X POST http://localhost:8090/cav \
  -H "Content-Type: application/json" \
  -d '{"plate": "ABCD12", "secret": "test"}'
```
