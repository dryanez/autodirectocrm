-- ============================================================
-- Run this SQL in Supabase SQL Editor:
-- https://supabase.com/dashboard/project/kqympdxeszdyppbhtzbm/sql/new
-- ============================================================

CREATE TABLE IF NOT EXISTS listings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  consignacion_id TEXT,
  appraisal_id UUID,
  brand TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  year INTEGER,
  color TEXT,
  mileage_km INTEGER,
  plate TEXT,
  price BIGINT,
  fuel_type TEXT DEFAULT 'Bencina',
  transmission TEXT DEFAULT 'Manual',
  motor TEXT,
  description TEXT,
  features JSONB DEFAULT '{}',
  image_urls JSONB DEFAULT '[]',
  status TEXT DEFAULT 'disponible',
  featured BOOLEAN DEFAULT FALSE
);

-- Enable Row Level Security
ALTER TABLE listings ENABLE ROW LEVEL SECURITY;

-- Public can read available listings (for autodirecto.cl website)
DROP POLICY IF EXISTS "Public read listings" ON listings;
CREATE POLICY "Public read listings" ON listings
  FOR SELECT USING (status = 'disponible');

-- Service role has full access (for CRM publishing)
DROP POLICY IF EXISTS "Service role write listings" ON listings;
CREATE POLICY "Service role write listings" ON listings
  USING (true) WITH CHECK (true);
