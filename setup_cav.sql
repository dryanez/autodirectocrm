-- ============================================================
-- CAV (Certificado de Anotaciones Vigentes) — Run in Supabase SQL Editor
-- Adds columns to consignaciones for storing CAV results
-- ============================================================

ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS cav_obtained_at TIMESTAMPTZ;
ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS cav_status      TEXT;         -- 'clean' | 'annotations'
ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS cav_owner_name  TEXT;
ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS cav_notes       TEXT;
