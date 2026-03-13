-- Add image_edits JSONB column to listings table
-- This stores a parallel array of edit objects alongside image_urls
-- Each entry: {zoom, panX, panY, brightness, contrast, saturate, skewV, skewH} or null
ALTER TABLE listings ADD COLUMN IF NOT EXISTS image_edits JSONB DEFAULT '[]';
