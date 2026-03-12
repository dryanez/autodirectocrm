-- ============================================================
-- AutoDirecto CRM — Notifications Table
-- Run in Supabase SQL Editor:
-- https://supabase.com/dashboard/project/kqympdxeszdyppbhtzbm/sql/new
-- ============================================================

CREATE TABLE IF NOT EXISTS notifications (
  id SERIAL PRIMARY KEY,
  type TEXT NOT NULL,              -- 'consignacion', 'inspection', 'contract_signed', 'lead', 'appointment', 'comprador', 'published', 'chileautos'
  title TEXT NOT NULL,
  message TEXT,
  icon TEXT DEFAULT '🔔',
  link_view TEXT,                  -- CRM view to navigate to: 'consignaciones', 'crm', 'compradores', 'calendar'
  link_id INTEGER,                 -- ID of the related record (consignacion_id, lead_id, etc.)
  is_read BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notifications_read ON notifications(is_read);
CREATE INDEX IF NOT EXISTS idx_notifications_created ON notifications(created_at DESC);

ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON notifications;
CREATE POLICY "Service role full access" ON notifications USING (true) WITH CHECK (true);
