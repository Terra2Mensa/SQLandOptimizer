-- 023_service_zones.sql
-- Processor-centric service zones: per-processor radius + state code.
-- The service area IS the radius around each processor.
-- Growth model: add processor → farms + customers within radius auto-included.

-- ─── 1. Per-processor radius ────────────────────────────────────────────
-- NULL = use global default from optimizer_config
ALTER TABLE processor_costs ADD COLUMN IF NOT EXISTS farmer_radius_miles NUMERIC(6,2);
ALTER TABLE processor_costs ADD COLUMN IF NOT EXISTS customer_radius_miles NUMERIC(6,2);

-- ─── 2. State code on profiles ──────────────────────────────────────────
-- Used for state-line enforcement on custom-exempt processing
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS state_code CHAR(2);

-- Populate state codes for existing processors (from known addresses)
UPDATE profiles SET state_code = 'IN' WHERE company_name = 'Thomas Farm Meats';
UPDATE profiles SET state_code = 'IN' WHERE company_name = 'Showalter''s Custom Meats';
UPDATE profiles SET state_code = 'MI' WHERE company_name = 'Byron Center Meats';
UPDATE profiles SET state_code = 'MI' WHERE company_name = 'Dennison Meat Locker';
UPDATE profiles SET state_code = 'IN' WHERE company_name = 'Malafy''s Meat Processing';

-- Populate for farms (all Michiana area)
UPDATE profiles SET state_code = 'IN' WHERE type = 'farmer' AND state_code IS NULL;

-- ─── 3. Update global default: customer radius 50→30 ───────────────────
UPDATE optimizer_config SET value = '30' WHERE key = 'max_customer_distance_miles';
-- farmer stays at 50

-- ─── Grants ─────────────────────────────────────────────────────────────
-- (columns added to existing tables, no new grants needed)
