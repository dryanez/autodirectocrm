-- ============================================================
-- FB Auto-Poster — Supabase Schema
-- Run this in the Supabase SQL editor:
-- https://supabase.com/dashboard/project/kqympdxeszdyppbhtzbm/sql/new
-- ============================================================

-- Tracks every Marketplace + group posting job
CREATE TABLE IF NOT EXISTS fb_poster_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID DEFAULT 'a0000000-0000-0000-0000-000000000001',
  consignacion_id INTEGER,
  car_title TEXT,
  caption TEXT,
  status TEXT DEFAULT 'queued',       -- queued, running, completed, error
  marketplace_url TEXT,
  groups_total INTEGER DEFAULT 0,
  groups_posted INTEGER DEFAULT 0,
  groups_failed INTEGER DEFAULT 0,
  groups_detail JSONB DEFAULT '[]',   -- [{name, url, status, posted_at}]
  log JSONB DEFAULT '[]',             -- ["[HH:MM:SS] msg", ...]
  error TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_fb_poster_jobs_company ON fb_poster_jobs(company_id);
CREATE INDEX IF NOT EXISTS idx_fb_poster_jobs_status ON fb_poster_jobs(status);
CREATE INDEX IF NOT EXISTS idx_fb_poster_jobs_consig ON fb_poster_jobs(consignacion_id);

-- RLS
ALTER TABLE fb_poster_jobs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON fb_poster_jobs;
CREATE POLICY "Service role full access" ON fb_poster_jobs USING (true) WITH CHECK (true);

-- Config for per-company group lists & location
CREATE TABLE IF NOT EXISTS fb_poster_config (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID UNIQUE DEFAULT 'a0000000-0000-0000-0000-000000000001',
  location_name TEXT DEFAULT 'Bosques de Miramar, Viña del Mar',
  latitude REAL DEFAULT -33.0245,
  longitude REAL DEFAULT -71.5518,
  groups JSONB DEFAULT '[]',           -- [{name, url, enabled}]
  delay_min INTEGER DEFAULT 15,        -- min seconds between group posts
  delay_max INTEGER DEFAULT 45,        -- max seconds between group posts
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE fb_poster_config ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON fb_poster_config;
CREATE POLICY "Service role full access" ON fb_poster_config USING (true) WITH CHECK (true);
