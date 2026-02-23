-- ============================================================
-- AutoDirecto CRM — Storage & Photo Tables
-- Run in Supabase SQL Editor BEFORE using the camera app
-- ============================================================

-- ─── appraisals ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS appraisals (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now(),
  car_make      TEXT,
  car_model     TEXT,
  car_year      INTEGER,
  car_plate     TEXT,
  car_color     TEXT,
  car_km        INTEGER,
  tasacion      NUMERIC,
  observaciones TEXT,
  estado        TEXT DEFAULT 'pendiente'
);

ALTER TABLE appraisals ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON appraisals;
CREATE POLICY "Service role full access" ON appraisals USING (true) WITH CHECK (true);

-- ─── vehicle_images ──────────────────────────────────────────
-- DROP and recreate to ensure correct columns (old table may have wrong schema).
DROP TABLE IF EXISTS vehicle_images;
CREATE TABLE vehicle_images (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  appraisal_id  UUID,
  storage_path  TEXT NOT NULL,
  url           TEXT NOT NULL,
  label         TEXT,
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vehicle_images_appraisal ON vehicle_images(appraisal_id);

ALTER TABLE vehicle_images ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON vehicle_images;
CREATE POLICY "Service role full access" ON vehicle_images USING (true) WITH CHECK (true);

-- ─── Storage bucket ──────────────────────────────────────────
-- Creates the "vehicle-images" bucket as PUBLIC.
INSERT INTO storage.buckets (id, name, public)
VALUES ('vehicle-images', 'vehicle-images', true)
ON CONFLICT (id) DO UPDATE SET public = true;

-- Public reads
DROP POLICY IF EXISTS "Public read vehicle-images" ON storage.objects;
CREATE POLICY "Public read vehicle-images"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'vehicle-images');

-- Service role writes (INSERT / UPDATE / DELETE split for compatibility)
DROP POLICY IF EXISTS "Service role insert vehicle-images" ON storage.objects;
CREATE POLICY "Service role insert vehicle-images"
  ON storage.objects FOR INSERT
  WITH CHECK (bucket_id = 'vehicle-images');

DROP POLICY IF EXISTS "Service role update vehicle-images" ON storage.objects;
CREATE POLICY "Service role update vehicle-images"
  ON storage.objects FOR UPDATE
  USING (bucket_id = 'vehicle-images')
  WITH CHECK (bucket_id = 'vehicle-images');

DROP POLICY IF EXISTS "Service role delete vehicle-images" ON storage.objects;
CREATE POLICY "Service role delete vehicle-images"
  ON storage.objects FOR DELETE
  USING (bucket_id = 'vehicle-images');

-- ─── camera_jobs ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS camera_jobs (
  token           TEXT PRIMARY KEY,
  consignacion_id INTEGER,
  appraisal_id    TEXT,
  label           TEXT,
  photos_uploaded INTEGER DEFAULT 0,
  created_at      TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE camera_jobs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON camera_jobs;
CREATE POLICY "Service role full access" ON camera_jobs USING (true) WITH CHECK (true);
