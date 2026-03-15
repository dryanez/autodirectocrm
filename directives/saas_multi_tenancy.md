# SaaS Multi-Tenancy — Full Directive

## Overview
Transform Autodirecto CRM from a single-company app into a true multi-tenant SaaS platform where each automotora (car dealership) gets isolated data, their own API keys (Meta, WhatsApp, etc.), separate branding, and per-company settings.

## Current State Audit

### What Already Exists
- `companies` table — 1 row (Wiackowska Group Spa / autodirecto)
- `modules` + `company_modules` tables — per-company feature toggles
- `COMPANY_ID` env var (hardcoded fallback: `a0000000-...0001`)
- `app_settings` table — flat key-value (global, not per-company)
- `crm_users` table — no `company_id` column

### What's Missing
- No `company_id` column on ANY data table (crm_users, consignaciones, crm_leads, listings, wa_conversations, social_posts, vehicle_images, appraisals, camera_jobs, wa_faq)
- No per-company credentials storage (Meta, WhatsApp, Resend, Gemini, ChileAutos tokens)  
- No company context in auth flow — users aren't linked to companies
- No tenant isolation in API routes — all data is global
- Settings hardcoded to env vars instead of per-company DB config

## Architecture: 4 Phases

### PHASE 1 — Database Schema (company_id everywhere)
**SQL Migration: `setup_saas_tenancy.sql`**

1. Add `company_id UUID REFERENCES companies(id)` to:
   - `crm_users` (users belong to a company)
   - `consignaciones`
   - `crm_leads`  
   - `listings`
   - `vehicle_images` (via appraisal → consignacion chain, OR direct column)
   - `appraisals`
   - `wa_conversations`
   - `wa_messages`
   - `social_posts`
   - `wa_faq`
   - `camera_jobs`

2. Create `company_settings` table:
   ```
   company_id UUID PK REFERENCES companies(id)
   -- Branding
   company_name TEXT
   logo_url TEXT
   -- WhatsApp
   whatsapp_access_token TEXT
   whatsapp_phone_number_id TEXT
   whatsapp_verify_token TEXT
   whatsapp_number TEXT
   -- Meta / Social  
   meta_fb_page_id TEXT
   meta_fb_page_access_token TEXT
   meta_ig_user_id TEXT
   meta_system_user_token TEXT
   meta_app_id TEXT
   meta_app_secret TEXT
   -- Email
   resend_api_key TEXT
   from_email TEXT
   -- AI
   google_api_key TEXT
   ai_system_prompt TEXT
   -- ChileAutos
   chileautos_client_id TEXT
   chileautos_client_secret TEXT
   chileautos_seller_id TEXT
   chileautos_env TEXT DEFAULT 'staging'
   -- General
   contact_phone TEXT
   contact_email TEXT
   timezone TEXT DEFAULT 'America/Santiago'
   ```

3. Back-fill existing data: SET all existing rows to company_id = 'a0000000-...-000000000001'

### PHASE 2 — Backend Tenant Context ✅ DONE
**`app.py` changes**

1. ✅ `_make_token` / `_verify_token` — now carry company_id (4-part token, with legacy 3-part backcompat)
2. ✅ `_get_company_id(user)` — gets company_id from user or env fallback
3. ✅ `_get_company_settings(company_id)` — loads from company_settings table with 60s cache, falls back to env vars
4. ✅ `_tenant_filter(company_id)` — returns Supabase eq. filter string
5. ✅ Login returns `company_id` in user_out + token
6. ✅ `/api/auth/me` returns `company_id`
7. ✅ `/api/company-settings` GET/PATCH — per-tenant CRUD
8. ✅ `/api/settings` GET/POST — merged with company_settings
9. ✅ `/api/modules` + toggle — uses `_get_company_id()` instead of global `COMPANY_ID`
10. ✅ WA manual send — uses per-company WA credentials
11. ✅ WA webhook — uses per-company WA + Gemini credentials
12. ✅ Email send — uses per-company Resend API key
13. ✅ AI description — uses per-company Google API key
14. ✅ `social_routes.py` — `_meta_token()`, `_ig_user_id()`, `_fb_page_id()` now use per-company settings

### PHASE 3 — Frontend Tenant Awareness ✅ DONE
**`templates/index.html` changes**

1. ✅ Login stores `company_id` in `currentUser` state
2. ✅ Sidebar shows company name dynamically (falls back to "Autodirecto CRM")
3. ✅ Ajustes page expanded with tabbed UI: General, WhatsApp API, Meta/Social, Email/AI, ChileAutos
4. ✅ `crmSettings` state expanded with all integration credential fields
5. ✅ Settings load/save uses merged `/api/settings` endpoint (company_settings + crm_settings)
6. ✅ Secrets masked for non-admin users

### PHASE 4 — Onboarding & Super Admin
1. `/api/companies` POST — already exists (creates new company)
2. Super admin panel in Módulos view — manage all tenants
3. Company switcher for super admins
