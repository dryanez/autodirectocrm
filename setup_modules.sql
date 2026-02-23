-- ============================================================
-- AutoDirecto CRM — Modular SaaS System
-- Run in Supabase SQL Editor to enable per-company module control
-- ============================================================

-- ─── Companies (tenants) ─────────────────────────────────────
-- Each automotriz using the platform is a company.
-- The CRM instance reads its company_id from env var COMPANY_ID.
CREATE TABLE IF NOT EXISTS companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  rut TEXT UNIQUE,
  slug TEXT UNIQUE NOT NULL,             -- e.g. 'autodirecto', 'carhouse-stgo'
  logo_url TEXT,
  website TEXT,
  contact_email TEXT,
  contact_phone TEXT,
  address TEXT,
  comuna TEXT,
  ciudad TEXT,
  plan TEXT DEFAULT 'starter',           -- starter | pro | enterprise
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Module Registry ─────────────────────────────────────────
-- Master list of all available modules in the platform.
-- New modules are added here, then enabled per company.
CREATE TABLE IF NOT EXISTS modules (
  id TEXT PRIMARY KEY,                   -- e.g. 'camera_pro', 'ai_description', 'credit_simulator'
  name TEXT NOT NULL,                    -- Human-readable name
  description TEXT,
  icon TEXT DEFAULT 'ph-puzzle-piece',   -- Phosphor icon class
  category TEXT DEFAULT 'general',       -- camera | ai | finance | documents | marketing | general
  is_premium BOOLEAN DEFAULT FALSE,      -- premium modules need pro/enterprise plan
  sort_order INTEGER DEFAULT 100,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Company Modules (permissions) ──────────────────────────
-- Which modules each company has enabled.
CREATE TABLE IF NOT EXISTS company_modules (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  module_id TEXT NOT NULL REFERENCES modules(id) ON DELETE CASCADE,
  enabled BOOLEAN DEFAULT TRUE,
  config JSONB DEFAULT '{}',             -- module-specific config (e.g. camera_pro URL)
  enabled_at TIMESTAMPTZ DEFAULT NOW(),
  enabled_by TEXT,                       -- who activated it
  UNIQUE(company_id, module_id)
);

-- ─── Seed: Module Registry ──────────────────────────────────
INSERT INTO modules (id, name, description, icon, category, is_premium, sort_order) VALUES
  ('camera_pro',        'Cámara Pro',          'Cámara con guías de superposición e IA para fotos profesionales de vehículos', 'ph-aperture',        'camera',    TRUE,  10),
  ('ai_description',    'Descripción IA',      'Generación automática de descripciones de vehículos con Google Gemini',       'ph-sparkle',         'ai',        TRUE,  20),
  ('ai_market_price',   'Precio de Mercado IA','Estimación de precio de mercado usando IA y datos históricos',                'ph-chart-line-up',   'ai',        TRUE,  30),
  ('credit_simulator',  'Simulador Crédito',   'Simulador de crédito automotriz visible en la publicación del sitio web',     'ph-calculator',      'finance',   FALSE, 40),
  ('dte_facturacion',   'Facturación DTE',     'Emisión de boletas y facturas electrónicas vía SII',                          'ph-file-text',       'documents', TRUE,  50),
  ('contrato_digital',  'Contrato Digital',    'Generación y firma de contratos de consignación en PDF',                      'ph-signature',       'documents', FALSE, 60),
  ('email_notifications','Notificaciones Email','Emails automáticos a propietarios y compradores vía Resend',                 'ph-envelope',        'marketing', FALSE, 70),
  ('funnels',           'Funnels',             'Scraping de Facebook Marketplace y gestión de leads de funnels',               'ph-funnel',          'marketing', TRUE,  80),
  ('website_publish',   'Publicar en Web',     'Publicación de vehículos en el sitio web autodirecto.cl',                     'ph-globe',           'marketing', FALSE, 90),
  ('compradores',       'Gestión Compradores', 'Pipeline de compradores interesados, test drives y ofertas',                  'ph-shopping-cart',   'general',   FALSE, 100),
  ('calendar',          'Calendario',          'Gestión de citas e inspecciones con calendario visual',                       'ph-calendar-dots',   'general',   FALSE, 110),
  ('crm',               'CRM Leads',           'Gestión de leads con pipeline Kanban y seguimiento',                          'ph-address-book',    'general',   FALSE, 120),
  ('inventario',        'Inventario',          'Gestión de inventario de vehículos con fichas detalladas',                    'ph-car',             'general',   FALSE, 130)
ON CONFLICT (id) DO NOTHING;

-- ─── Seed: Default Company (Autodirecto / Wiackowska) ───────
INSERT INTO companies (id, name, rut, slug, website, contact_email, plan)
VALUES (
  'a0000000-0000-0000-0000-000000000001',
  'Wiackowska Group Spa',
  '78355717-7',
  'autodirecto',
  'https://autodirecto.cl',
  'admin@autodirecto.cl',
  'enterprise'
)
ON CONFLICT (slug) DO NOTHING;

-- Enable ALL modules for Autodirecto (they own the platform)
INSERT INTO company_modules (company_id, module_id, enabled, enabled_by)
SELECT 'a0000000-0000-0000-0000-000000000001', id, TRUE, 'system'
FROM modules
ON CONFLICT (company_id, module_id) DO NOTHING;

-- ─── RLS ─────────────────────────────────────────────────────
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE modules ENABLE ROW LEVEL SECURITY;
ALTER TABLE company_modules ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Service role full access" ON companies;
CREATE POLICY "Service role full access" ON companies USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON modules;
CREATE POLICY "Service role full access" ON modules USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Service role full access" ON company_modules;
CREATE POLICY "Service role full access" ON company_modules USING (true) WITH CHECK (true);

-- ─── Index ───────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_company_modules_company ON company_modules(company_id);
CREATE INDEX IF NOT EXISTS idx_company_modules_module ON company_modules(module_id);
CREATE INDEX IF NOT EXISTS idx_companies_slug ON companies(slug);

-- ─── Camera Jobs (serverless-safe token relay) ──────────────
-- Short-lived tokens that link a CRM "open camera" action to the vehicle data.
-- The camera PWA fetches these on launch to know which vehicle to photograph.
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
