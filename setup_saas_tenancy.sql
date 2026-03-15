-- ═══════════════════════════════════════════════════════════
-- SaaS Multi-Tenancy Migration
-- Run this in Supabase SQL Editor
-- Adds company_id to all data tables + creates company_settings
-- ═══════════════════════════════════════════════════════════

-- Default company UUID for existing data backfill
-- (matches the existing Wiackowska Group Spa row)
DO $$ BEGIN RAISE NOTICE 'Starting SaaS multi-tenancy migration...'; END $$;

-- ═══ 1. company_settings table ═══
CREATE TABLE IF NOT EXISTS company_settings (
    company_id UUID PRIMARY KEY REFERENCES companies(id) ON DELETE CASCADE,
    -- Branding
    company_name TEXT,
    logo_url TEXT,
    -- WhatsApp Business API
    whatsapp_access_token TEXT,
    whatsapp_phone_number_id TEXT,
    whatsapp_verify_token TEXT DEFAULT 'autodirecto2026',
    whatsapp_number TEXT,
    -- Meta / Facebook / Instagram
    meta_fb_page_id TEXT,
    meta_fb_page_access_token TEXT,
    meta_ig_user_id TEXT,
    meta_system_user_token TEXT,
    meta_app_id TEXT,
    meta_app_secret TEXT,
    -- Email (Resend)
    resend_api_key TEXT,
    from_email TEXT DEFAULT 'ventas@autodirecto.cl',
    -- AI (Gemini)
    google_api_key TEXT,
    ai_system_prompt TEXT,
    -- ChileAutos integration
    chileautos_client_id TEXT,
    chileautos_client_secret TEXT,
    chileautos_seller_id TEXT,
    chileautos_env TEXT DEFAULT 'staging',
    -- General
    contact_phone TEXT,
    contact_email TEXT,
    timezone TEXT DEFAULT 'America/Santiago',
    commission_pct NUMERIC DEFAULT 3.9,
    commission_min INTEGER DEFAULT 150000,
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE company_settings ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service full access on company_settings" ON company_settings;
CREATE POLICY "Service full access on company_settings"
    ON company_settings USING (true) WITH CHECK (true);

-- Auto-update trigger
CREATE OR REPLACE FUNCTION update_company_settings_ts()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS company_settings_updated ON company_settings;
CREATE TRIGGER company_settings_updated BEFORE UPDATE ON company_settings
    FOR EACH ROW EXECUTE FUNCTION update_company_settings_ts();


-- ═══ 2. Add company_id to ALL data tables ═══

-- crm_users
ALTER TABLE crm_users ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_crm_users_company ON crm_users(company_id);

-- consignaciones
ALTER TABLE consignaciones ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_consignaciones_company ON consignaciones(company_id);

-- crm_leads
ALTER TABLE crm_leads ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_crm_leads_company ON crm_leads(company_id);

-- listings
ALTER TABLE listings ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_listings_company ON listings(company_id);

-- appraisals
ALTER TABLE appraisals ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_appraisals_company ON appraisals(company_id);

-- vehicle_images
ALTER TABLE vehicle_images ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_vehicle_images_company ON vehicle_images(company_id);

-- wa_conversations
ALTER TABLE wa_conversations ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_wa_conversations_company ON wa_conversations(company_id);

-- wa_messages (add company_id for query performance)
ALTER TABLE wa_messages ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_wa_messages_company ON wa_messages(company_id);

-- social_posts
ALTER TABLE social_posts ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_social_posts_company ON social_posts(company_id);

-- camera_jobs
ALTER TABLE camera_jobs ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);
CREATE INDEX IF NOT EXISTS idx_camera_jobs_company ON camera_jobs(company_id);


-- ═══ 3. Back-fill existing data to default company ═══
UPDATE crm_users SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE consignaciones SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE crm_leads SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE listings SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE appraisals SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE vehicle_images SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE wa_conversations SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE wa_messages SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE social_posts SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;
UPDATE camera_jobs SET company_id = 'a0000000-0000-0000-0000-000000000001' WHERE company_id IS NULL;

-- ═══ 4. Seed default company settings (from env vars — user will update in UI) ═══
INSERT INTO company_settings (company_id, company_name, contact_email)
VALUES ('a0000000-0000-0000-0000-000000000001', 'Wiackowska Group Spa', 'admin@autodirecto.cl')
ON CONFLICT (company_id) DO NOTHING;

DO $$ BEGIN RAISE NOTICE 'SaaS multi-tenancy migration complete ✅'; END $$;
