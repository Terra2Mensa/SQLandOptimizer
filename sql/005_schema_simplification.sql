-- =============================================================================
-- 005_schema_simplification.sql — Phase 1: Core Tables
-- Drops dead/replaced tables, creates new simplified schema.
-- Run: psql terra_mensa -f sql/005_schema_simplification.sql
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. DROP old tables (dependency order: children first, then parents)
-- ---------------------------------------------------------------------------

-- Dead engine-style tables (in local DB these are in public schema)
DROP TABLE IF EXISTS buyer_cut_preferences CASCADE;
DROP TABLE IF EXISTS order_lines CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS buyers CASCADE;
DROP TABLE IF EXISTS config_regions CASCADE;
DROP TABLE IF EXISTS config_parameters CASCADE;

-- Replaced tables (children first)
DROP TABLE IF EXISTS actual_cuts CASCADE;
DROP TABLE IF EXISTS slaughter_order_lines CASCADE;
DROP TABLE IF EXISTS slaughter_orders CASCADE;
DROP TABLE IF EXISTS invoices CASCADE;
DROP TABLE IF EXISTS po_lines CASCADE;
DROP TABLE IF EXISTS purchase_orders CASCADE;
DROP TABLE IF EXISTS config_processor_capabilities CASCADE;
DROP TABLE IF EXISTS config_processors CASCADE;
DROP TABLE IF EXISTS farmer_inventory CASCADE;
DROP TABLE IF EXISTS processors CASCADE;
DROP TABLE IF EXISTS dtc_customers CASCADE;
DROP TABLE IF EXISTS farmers CASCADE;

-- ---------------------------------------------------------------------------
-- 2. CREATE new tables
-- ---------------------------------------------------------------------------

-- Unified profiles (replaces farmers, dtc_customers, processors)
CREATE TABLE IF NOT EXISTS profiles (
    profile_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(20) NOT NULL,  -- farmer, customer, processor
    first VARCHAR(50),
    last VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(30),
    address TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    company_name VARCHAR(150),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_profiles_type ON profiles(type);
CREATE INDEX idx_profiles_active ON profiles(active) WHERE active = TRUE;

-- Farmer inventory (simplified)
CREATE TABLE IF NOT EXISTS farmer_inventory (
    animal_id VARCHAR(50) PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    live_weight_est NUMERIC(10,1),
    expected_grade VARCHAR(20),
    expected_finish_date DATE,
    active BOOLEAN DEFAULT TRUE,
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_inventory_profile ON farmer_inventory(profile_id);
CREATE INDEX idx_inventory_species_status ON farmer_inventory(species, status);

-- Purchase orders (simplified — share-based)
CREATE TABLE IF NOT EXISTS purchase_orders (
    po_number VARCHAR(50) PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    share VARCHAR(20) NOT NULL,  -- whole, half, quarter, eighth
    note TEXT,
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    deposit NUMERIC(10,2) DEFAULT 0,
    customer_preferences TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_po_profile ON purchase_orders(profile_id);
CREATE INDEX idx_po_status ON purchase_orders(status);
CREATE INDEX idx_po_species_status ON purchase_orders(species, status);

-- PO cut instructions (structured cut preferences per PO)
CREATE TABLE IF NOT EXISTS po_cut_instructions (
    id SERIAL PRIMARY KEY,
    po_number VARCHAR(50) NOT NULL REFERENCES purchase_orders(po_number) ON DELETE CASCADE,
    cut_code VARCHAR(30) NOT NULL,
    instruction VARCHAR(100),  -- "1-inch steaks", "ground", "roast", "bone-in"
    quantity VARCHAR(50)       -- optional, e.g. "2 packs of 4"
);

CREATE INDEX idx_po_cut_instructions_po ON po_cut_instructions(po_number);

-- Slaughter orders (simplified — no optimizer metadata)
CREATE TABLE IF NOT EXISTS slaughter_orders (
    order_number VARCHAR(50) PRIMARY KEY,
    animal_id VARCHAR(50) REFERENCES farmer_inventory(animal_id),
    profile_id VARCHAR(50) REFERENCES profiles(profile_id),  -- processor
    species VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'planned',
    actual_hanging_weight NUMERIC(10,2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX idx_slaughter_status ON slaughter_orders(status);
CREATE INDEX idx_slaughter_animal ON slaughter_orders(animal_id);

-- Slaughter order allocations (per-PO share tracking)
CREATE TABLE IF NOT EXISTS slaughter_order_allocations (
    id SERIAL PRIMARY KEY,
    slaughter_order_number VARCHAR(50) NOT NULL REFERENCES slaughter_orders(order_number) ON DELETE CASCADE,
    po_number VARCHAR(50) NOT NULL REFERENCES purchase_orders(po_number),
    share VARCHAR(20) NOT NULL  -- whole, half, quarter, eighth
);

CREATE INDEX idx_so_alloc_order ON slaughter_order_allocations(slaughter_order_number);
CREATE INDEX idx_so_alloc_po ON slaughter_order_allocations(po_number);

-- Processor costs (replaces config_processors + config_processor_capabilities)
CREATE TABLE IF NOT EXISTS processor_costs (
    id SERIAL PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    kill_fee NUMERIC(10,2) NOT NULL,
    fab_cost_per_lb NUMERIC(10,4) NOT NULL,
    shrink_pct NUMERIC(6,4) NOT NULL,
    daily_capacity_head INTEGER,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (profile_id, species, effective_date)
);

CREATE INDEX idx_processor_costs_lookup ON processor_costs(profile_id, species, effective_date DESC);

COMMIT;
