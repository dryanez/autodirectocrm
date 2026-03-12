-- Migration: app_settings table for storing server-side config (FB cookies, etc.)
CREATE TABLE IF NOT EXISTS app_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Allow service role full access
ALTER TABLE app_settings ENABLE ROW LEVEL SECURITY;
CREATE POLICY "service_role_all" ON app_settings
    FOR ALL TO service_role USING (true) WITH CHECK (true);
