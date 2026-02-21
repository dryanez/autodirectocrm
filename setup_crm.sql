-- ============================================================
-- AutoDirecto CRM — Supabase Schema
-- Run this ONCE in the Supabase SQL editor:
-- https://supabase.com/dashboard/project/kqympdxeszdyppbhtzbm/sql/new
-- ============================================================

-- ─── Cars (Inventory) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cars (
  id SERIAL PRIMARY KEY,
  patente TEXT UNIQUE NOT NULL,
  vin TEXT,
  brand TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  year INTEGER,
  color TEXT,
  owner_name TEXT NOT NULL DEFAULT '',
  owner_rut TEXT NOT NULL DEFAULT '',
  owner_email TEXT,
  owner_phone TEXT,
  owner_price INTEGER NOT NULL DEFAULT 0,
  selling_price INTEGER NOT NULL DEFAULT 0,
  commission_pct REAL DEFAULT 0.10,
  status TEXT DEFAULT 'available',
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── CRM Users (agents) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS crm_users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  role TEXT DEFAULT 'agent',
  color TEXT DEFAULT '#3b82f6',
  active INTEGER DEFAULT 1,
  sucursal TEXT DEFAULT 'Vitacura',
  password TEXT DEFAULT 'admin1234',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed default admin
INSERT INTO crm_users (name, email, role, color, password)
VALUES ('Admin', 'admin@autodirecto.cl', 'admin', '#8b5cf6', 'admin')
ON CONFLICT (email) DO NOTHING;

-- ─── Consignaciones ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS consignaciones (
  id SERIAL PRIMARY KEY,
  appointment_supabase_id TEXT UNIQUE,
  car_id INTEGER REFERENCES cars(id),
  -- Part 1: Contact
  owner_first_name TEXT,
  owner_last_name TEXT,
  owner_full_name TEXT,
  owner_rut TEXT,
  owner_phone TEXT,
  owner_country_code TEXT DEFAULT '+56',
  owner_email TEXT,
  owner_region TEXT,
  owner_commune TEXT,
  owner_address TEXT,
  -- Part 1: Vehicle
  plate TEXT,
  car_make TEXT,
  car_model TEXT,
  car_year INTEGER,
  mileage INTEGER,
  version TEXT,
  -- Part 2: Inspection
  color TEXT,
  vin TEXT,
  owner_price INTEGER,
  selling_price INTEGER,
  ai_market_price INTEGER,
  ai_instant_buy_price INTEGER,
  commission_pct REAL DEFAULT 0.10,
  condition_notes TEXT,
  km_verified INTEGER,
  inspection_photos TEXT DEFAULT '[]',
  -- Appointment
  appointment_date TEXT,
  appointment_time TEXT,
  assigned_user_id INTEGER REFERENCES crm_users(id),
  -- Status
  status TEXT DEFAULT 'pendiente',
  part1_completed_at TEXT,
  part2_completed_at TEXT,
  notes TEXT,
  -- Links
  appraisal_supabase_id TEXT,
  listing_id TEXT,
  contract_pdf TEXT,
  contract_signed_at TEXT,
  en_venta BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consig_plate ON consignaciones(plate);
CREATE INDEX IF NOT EXISTS idx_consig_date ON consignaciones(appointment_date);
CREATE INDEX IF NOT EXISTS idx_consig_status ON consignaciones(status);

-- ─── CRM Leads ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crm_leads (
  id SERIAL PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  full_name TEXT,
  rut TEXT,
  email TEXT,
  phone TEXT,
  country_code TEXT DEFAULT '+56',
  region TEXT,
  commune TEXT,
  address TEXT,
  plate TEXT,
  car_make TEXT,
  car_model TEXT,
  car_year INTEGER,
  mileage INTEGER,
  version TEXT,
  appointment_date TEXT,
  appointment_time TEXT,
  stage TEXT DEFAULT 'nuevo',
  priority TEXT DEFAULT 'medium',
  assigned_to TEXT,
  source TEXT DEFAULT 'manual',
  source_id TEXT,
  supabase_id TEXT UNIQUE,
  funnel_url TEXT,
  estimated_value INTEGER,
  listing_price INTEGER,
  ai_consignacion_price INTEGER,
  ai_instant_buy_price INTEGER,
  notes TEXT,
  tags TEXT DEFAULT '[]',
  last_contact_at TEXT,
  next_followup_at TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crm_leads_stage ON crm_leads(stage);
CREATE INDEX IF NOT EXISTS idx_crm_leads_plate ON crm_leads(plate);

-- ─── CRM Activities ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crm_activities (
  id SERIAL PRIMARY KEY,
  lead_id INTEGER NOT NULL REFERENCES crm_leads(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  created_by TEXT DEFAULT 'system',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crm_activities_lead ON crm_activities(lead_id);

-- ─── Funnel Listings ─────────────────────────────────────────
-- FB Marketplace scraped listings (migrated from local Apify JSON)
CREATE TABLE IF NOT EXISTS funnel_listings (
  id TEXT PRIMARY KEY,          -- Apify listing id (e.g. "926783459704701")
  url TEXT UNIQUE NOT NULL,
  title TEXT,
  price TEXT,
  price_num INTEGER,            -- numeric CLP amount for sorting/filtering
  location TEXT,
  year INTEGER,
  mileage TEXT,
  photo_url TEXT,
  is_sold BOOLEAN DEFAULT FALSE,
  scraped_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funnel_listings_year ON funnel_listings(year);
CREATE INDEX IF NOT EXISTS idx_funnel_listings_price ON funnel_listings(price_num);

-- ─── Funnel Lead Status ───────────────────────────────────────
-- Persists FB Marketplace lead status/valuation across deploys
CREATE TABLE IF NOT EXISTS funnel_lead_status (
  id SERIAL PRIMARY KEY,
  url TEXT UNIQUE NOT NULL,
  status TEXT DEFAULT 'new',
  contacted_at BIGINT,
  valuation JSONB,
  updated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_funnel_status_url ON funnel_lead_status(url);

-- ─── Row Level Security ───────────────────────────────────────
-- Service role key bypasses RLS, so these tables are accessible from SimplyAPI.
-- Enable RLS to block public/anon access:
ALTER TABLE cars ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE consignaciones ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_leads ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_activities ENABLE ROW LEVEL SECURITY;
ALTER TABLE funnel_listings ENABLE ROW LEVEL SECURITY;
ALTER TABLE funnel_lead_status ENABLE ROW LEVEL SECURITY;

-- Allow service role full access (SimplyAPI uses service role key)
DROP POLICY IF EXISTS "Service role full access" ON cars;
CREATE POLICY "Service role full access" ON cars USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON crm_users;
CREATE POLICY "Service role full access" ON crm_users USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON consignaciones;
CREATE POLICY "Service role full access" ON consignaciones USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON crm_leads;
CREATE POLICY "Service role full access" ON crm_leads USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON crm_activities;
CREATE POLICY "Service role full access" ON crm_activities USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON funnel_listings;
CREATE POLICY "Service role full access" ON funnel_listings USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON funnel_lead_status;
CREATE POLICY "Service role full access" ON funnel_lead_status USING (true) WITH CHECK (true);

-- ─── Compradores (Buyers) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS compradores (
  id SERIAL PRIMARY KEY,
  -- Contact Info
  first_name TEXT,
  last_name TEXT,
  full_name TEXT,
  rut TEXT,
  phone TEXT,
  country_code TEXT DEFAULT '+56',
  email TEXT,
  region TEXT,
  commune TEXT,
  address TEXT,
  -- Vehicle Interest (linked to consignacion/listing)
  consignacion_id INTEGER REFERENCES consignaciones(id),
  listing_id TEXT,
  car_description TEXT,
  car_plate TEXT,
  car_price INTEGER,
  -- Credit / Financing
  credit_requested BOOLEAN DEFAULT FALSE,
  credit_status TEXT DEFAULT 'none',
  credit_amount INTEGER,
  credit_down_payment INTEGER,
  credit_months INTEGER,
  credit_rate REAL,
  credit_monthly_payment INTEGER,
  credit_institution TEXT,
  credit_notes TEXT,
  -- Pipeline
  status TEXT DEFAULT 'interesado',
  assigned_user_id INTEGER REFERENCES crm_users(id),
  test_drive_date TEXT,
  test_drive_completed BOOLEAN DEFAULT FALSE,
  offer_amount INTEGER,
  -- Documents
  nota_compra_pdf TEXT,
  nota_compra_signed_at TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_compradores_status ON compradores(status);
CREATE INDEX IF NOT EXISTS idx_compradores_rut ON compradores(rut);
CREATE INDEX IF NOT EXISTS idx_compradores_consig ON compradores(consignacion_id);

ALTER TABLE compradores ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Service role full access" ON compradores;
CREATE POLICY "Service role full access" ON compradores USING (true) WITH CHECK (true);
