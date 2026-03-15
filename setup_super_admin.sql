-- ══════════════════════════════════════════════════════════════
-- Phase 4: Super Admin Setup
-- Run this in Supabase SQL Editor
-- ══════════════════════════════════════════════════════════════

-- 1. Promote existing admin user(s) of the default company to super_admin
-- This lets them manage all companies from the CRM UI.
UPDATE crm_users
SET role = 'super_admin'
WHERE company_id = 'a0000000-0000-0000-0000-000000000001'
  AND role = 'admin'
  AND active = 1;

-- 2. Verify the change
SELECT id, name, email, role, company_id
FROM crm_users
WHERE company_id = 'a0000000-0000-0000-0000-000000000001'
  AND active = 1;

-- NOTE: Only super_admin role users can:
--   - View /api/companies (list all companies)
--   - Create new companies via onboarding
--   - Switch between companies (change context)
--   - Access the Empresas panel in the CRM sidebar
