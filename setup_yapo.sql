-- ============================================================
-- AutoDirecto CRM — Yapo.cl Integration
-- Run in Supabase SQL Editor to enable Yapo.cl integration
-- ============================================================

-- ─── Add Yapo columns to consignaciones ─────────────────────
ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS yapo_id TEXT;

-- ─── Add Yapo columns to listings ───────────────────────────
ALTER TABLE listings ADD COLUMN IF NOT EXISTS yapo_id TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS yapo_status TEXT;

-- ─── Add Yapo credentials to crm_settings ───────────────────
-- cnImport API (publishing ads)
INSERT INTO crm_settings (key, value) VALUES
  ('yapo_cnimport_key', ''),
  ('yapo_email', ''),
  ('yapo_phone', ''),
  ('yapo_region', 'Valparaíso'),
  ('yapo_city', 'Viña del Mar'),
  ('yapo_name', 'Autodirecto'),
  -- CRM/Leads/Stats API
  ('yapo_crm_api_key', ''),
  ('yapo_company_slug', ''),
  ('yapo_user_id', '')
ON CONFLICT (key) DO NOTHING;

-- ─── Clean up old keys if upgrading ─────────────────────────
-- DELETE FROM crm_settings WHERE key IN ('yapo_app_id', 'yapo_api_key');
