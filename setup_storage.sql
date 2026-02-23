-- ============================================================
-- AutoDirecto CRM — Storage & Photo Tables
-- Run in Supabase SQL Editor BEFORE using the camera app
-- ============================================================

-- ─── appraisals ──────────────────────────────────────────────
-- One row per inspection/consignacion photo session.
-- The CRM creates one automatically when a photo is first uploaded.
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
-- One row per photo. Linked to an appraisal by UUID.
-- The actual file lives in Supabase Storage bucket "vehicle-images".
CREATE TABLE IF NOT EXISTS vehicle_images (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  appraisal_id  UUID REFERENCES appraisals(id) ON DELETE CASCADE,
  storage_path  TEXT NOT NULL,       -- path inside the bucket, e.g. "abc123/frontal.jpg"
  url           TEXT NOT NULL,       -- full public URL
  label         TEXT,                -- e.g. "sedan - Frontal"
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vehicle_images_appraisal ON vehicle_images(appraisal_id);

ALTER TABLE vehicle_images ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON vehicle_images;
CREATE POLICY "Service role full access" ON vehicle_images USING (true) WITH CHECK (true);

-- ─── Storage bucket ──────────────────────────────────────────
-- Creates the "vehicle-images" bucket as PUBLIC (photos are displayed in the CRM).
-- Supabase doesn't support CREATE BUCKET in SQL — do this in the Dashboard UI:
--
--   Storage → New bucket → Name: "vehicle-images" → Public: ON → Save
--
-- Or via the management API (run once):
INSERT INTO storage.buckets (id, name, public)
VALUES ('vehicle-images', 'vehicle-images', true)
ON CONFLICT (id) DO NOTHING;

-- Allow public reads from the bucket
DROP POLICY IF EXISTS "Public read vehicle-images" ON storage.objects;
CREATE POLICY "Public read vehicle-images"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'vehicle-images');

-- Allow service role to insert/update/delete
DROP POLICY IF EXISTS "Service role write vehicle-images" ON storage.objects;
CREATE POLICY "Service role write vehicle-images"
  ON storage.objects FOR ALL
  USING (bucket_id = 'vehicle-images')
  WITH CHECK (bucket_id = 'vehicle-images');

-- ─── camera_jobs ─────────────────────────────────────────────
-- Already in setup_modules.sql — included here for reference.
-- Camera app polls /api/camera-job/latest on every launch.
-- photos_uploaded = 0 means job is pending; > 0 means done.
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
