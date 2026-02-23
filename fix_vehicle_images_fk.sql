-- ============================================================
-- FIX: Drop FK constraint on vehicle_images.appraisal_id
-- ============================================================
-- The upload endpoint auto-generates a UUID for appraisal_id
-- WITHOUT inserting a row into the appraisals table first.
-- This FK constraint was causing all photo inserts to fail silently.
--
-- Run this in Supabase SQL Editor if you already ran setup_storage.sql.
-- ============================================================

-- Step 1: Find and drop the FK constraint
DO $$
DECLARE
  _constraint TEXT;
BEGIN
  SELECT conname INTO _constraint
  FROM pg_constraint
  WHERE conrelid = 'vehicle_images'::regclass
    AND contype = 'f';
  IF _constraint IS NOT NULL THEN
    EXECUTE 'ALTER TABLE vehicle_images DROP CONSTRAINT ' || quote_ident(_constraint);
    RAISE NOTICE 'Dropped FK constraint: %', _constraint;
  ELSE
    RAISE NOTICE 'No FK constraint found on vehicle_images — already clean.';
  END IF;
END $$;

-- Step 2: Verify the column is now just a plain UUID (no FK)
-- After running, you should see no FK row for vehicle_images in pg_constraint.
SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'vehicle_images'::regclass;
