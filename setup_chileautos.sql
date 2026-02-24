    -- ============================================================
    -- AutoDirecto CRM — ChileAutos Integration & Settings Tables
    -- Run in Supabase SQL Editor to enable ChileAutos integration
    -- ============================================================

    -- ─── CRM Settings (key-value store) ─────────────────────────
    CREATE TABLE IF NOT EXISTS crm_settings (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL DEFAULT ''
    );

    ALTER TABLE crm_settings ENABLE ROW LEVEL SECURITY;
    DROP POLICY IF EXISTS "Service role full access" ON crm_settings;
    CREATE POLICY "Service role full access" ON crm_settings USING (true) WITH CHECK (true);

    -- Insert default settings
    INSERT INTO crm_settings (key, value) VALUES
    ('whatsapp_number', '+56976654569'),
    ('chileautos_client_id', '464f4235-8052-4832-a5ea-6738021263fe'),
    ('chileautos_client_secret', 'Cen/5ic8fYtGbHMD4lU8VYHZ5/sJsU/N4qrl9V2DIzU='),
    ('chileautos_seller_id', '4AA0C7A3-DE66-4F21-91E8-84CA5CD8C6F4'),
    ('chileautos_env', 'staging')
    ON CONFLICT (key) DO NOTHING;

    -- ─── Add new columns to consignaciones ──────────────────────
    -- Body type and doors (for ChileAutos attributes)
    ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS body_type TEXT;
    ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS doors INTEGER;
    ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS fuel_type TEXT DEFAULT 'Bencina';
    ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS transmission TEXT DEFAULT 'Manual';
    ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS motor TEXT;
    ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS chileautos_id TEXT;

    -- ─── Add new columns to listings ────────────────────────────
    ALTER TABLE listings ADD COLUMN IF NOT EXISTS body_type TEXT;
    ALTER TABLE listings ADD COLUMN IF NOT EXISTS doors INTEGER;
    ALTER TABLE listings ADD COLUMN IF NOT EXISTS chileautos_id TEXT;
    ALTER TABLE listings ADD COLUMN IF NOT EXISTS chileautos_status TEXT;

    -- ─── Add source column to compradores ───────────────────────
    ALTER TABLE compradores ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'manual';

    -- ─── Add body_type and doors to appraisals ──────────────────
    ALTER TABLE appraisals ADD COLUMN IF NOT EXISTS vehicle_body_type TEXT;
    ALTER TABLE appraisals ADD COLUMN IF NOT EXISTS vehicle_doors INTEGER;
