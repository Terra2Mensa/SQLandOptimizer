-- =============================================================================
-- Terra Mensa — Supabase initialization script
-- Two-schema layout: public (website + REST API) / engine (Python backend only)
-- Run: psql "$SUPA_CONN" -f sql/supabase_init.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schema setup
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS engine;

-- ---------------------------------------------------------------------------
-- PUBLIC schema — Website reads/writes via REST + RLS
-- ---------------------------------------------------------------------------

-- Unified profiles (replaces farmers, dtc_customers, processors)
CREATE TABLE IF NOT EXISTS public.profiles (
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

CREATE INDEX IF NOT EXISTS idx_profiles_type ON public.profiles(type);
CREATE INDEX IF NOT EXISTS idx_profiles_active ON public.profiles(active) WHERE active = TRUE;

-- Farmer inventory (simplified)
CREATE TABLE IF NOT EXISTS public.farmer_inventory (
    animal_id VARCHAR(50) PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES public.profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    live_weight_est NUMERIC(10,1),
    expected_grade VARCHAR(20),
    expected_finish_date DATE,
    active BOOLEAN DEFAULT TRUE,
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inventory_profile ON public.farmer_inventory(profile_id);
CREATE INDEX IF NOT EXISTS idx_inventory_species_status ON public.farmer_inventory(species, status);

-- Purchase orders (share-based)
CREATE TABLE IF NOT EXISTS public.purchase_orders (
    po_number VARCHAR(50) PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES public.profiles(profile_id),
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

CREATE INDEX IF NOT EXISTS idx_po_profile ON public.purchase_orders(profile_id);
CREATE INDEX IF NOT EXISTS idx_po_status ON public.purchase_orders(status);
CREATE INDEX IF NOT EXISTS idx_po_species_status ON public.purchase_orders(species, status);

-- PO cut instructions (structured cut preferences per PO)
CREATE TABLE IF NOT EXISTS public.po_cut_instructions (
    id SERIAL PRIMARY KEY,
    po_number VARCHAR(50) NOT NULL REFERENCES public.purchase_orders(po_number) ON DELETE CASCADE,
    cut_code VARCHAR(30) NOT NULL,
    instruction VARCHAR(100),
    quantity VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_po_cut_instructions_po ON public.po_cut_instructions(po_number);

-- Cut specifications (kept as-is)
CREATE TABLE IF NOT EXISTS public.config_cut_specs (
    id SERIAL PRIMARY KEY,
    species VARCHAR(20) NOT NULL,
    primal_code VARCHAR(30) NOT NULL,
    primal_name VARCHAR(80) NOT NULL,
    cut_code VARCHAR(30) NOT NULL,
    cut_name VARCHAR(100) NOT NULL,
    yield_pct NUMERIC(5,2) NOT NULL,
    min_grade VARCHAR(20),
    is_premium BOOLEAN DEFAULT FALSE,
    notes TEXT,
    UNIQUE (species, cut_code)
);

CREATE INDEX IF NOT EXISTS idx_cut_specs_species ON public.config_cut_specs(species);
CREATE INDEX IF NOT EXISTS idx_cut_specs_cut ON public.config_cut_specs(cut_code);

-- Slaughter orders (simplified)
CREATE TABLE IF NOT EXISTS public.slaughter_orders (
    order_number VARCHAR(50) PRIMARY KEY,
    animal_id VARCHAR(50) REFERENCES public.farmer_inventory(animal_id),
    profile_id VARCHAR(50) REFERENCES public.profiles(profile_id),  -- processor
    species VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'planned',
    actual_hanging_weight NUMERIC(10,2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_slaughter_status ON public.slaughter_orders(status);
CREATE INDEX IF NOT EXISTS idx_slaughter_animal ON public.slaughter_orders(animal_id);

-- Slaughter order allocations (per-PO share tracking)
CREATE TABLE IF NOT EXISTS public.slaughter_order_allocations (
    id SERIAL PRIMARY KEY,
    slaughter_order_number VARCHAR(50) NOT NULL REFERENCES public.slaughter_orders(order_number) ON DELETE CASCADE,
    po_number VARCHAR(50) NOT NULL REFERENCES public.purchase_orders(po_number),
    share VARCHAR(20) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_so_alloc_order ON public.slaughter_order_allocations(slaughter_order_number);
CREATE INDEX IF NOT EXISTS idx_so_alloc_po ON public.slaughter_order_allocations(po_number);

-- Processor costs (replaces config_processors + config_processor_capabilities)
CREATE TABLE IF NOT EXISTS public.processor_costs (
    id SERIAL PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES public.profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    kill_fee NUMERIC(10,2) NOT NULL,
    fab_cost_per_lb NUMERIC(10,4) NOT NULL,
    shrink_pct NUMERIC(6,4) NOT NULL,
    daily_capacity_head INTEGER,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (profile_id, species, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_processor_costs_lookup
    ON public.processor_costs(profile_id, species, effective_date DESC);

-- Weekly pricing (per species + grade, effective-date versioned)
CREATE TABLE IF NOT EXISTS public.weekly_pricing (
    id SERIAL PRIMARY KEY,
    species VARCHAR(20) NOT NULL,
    grade VARCHAR(20) NOT NULL,
    price_per_lb NUMERIC(10,4) NOT NULL,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (species, grade, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_weekly_pricing_lookup
    ON public.weekly_pricing(species, grade, effective_date DESC);

-- Share adjustments (premium/discount by share size)
CREATE TABLE IF NOT EXISTS public.share_adjustments (
    id SERIAL PRIMARY KEY,
    share VARCHAR(20) NOT NULL,
    adjustment_pct NUMERIC(6,4) NOT NULL DEFAULT 0,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (share, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_share_adjustments_lookup
    ON public.share_adjustments(share, effective_date DESC);

-- ---------------------------------------------------------------------------
-- ENGINE schema — Python backend only, invisible to REST API
-- ---------------------------------------------------------------------------

-- USDA cattle prices
CREATE TABLE IF NOT EXISTS engine.usda_subprimal_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    grade VARCHAR(20) NOT NULL,
    imps_code VARCHAR(10) NOT NULL,
    item_description TEXT,
    weighted_avg_cwt NUMERIC(10,2),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    number_trades INTEGER,
    total_pounds BIGINT
);

CREATE INDEX IF NOT EXISTS idx_subprimal_report_date ON engine.usda_subprimal_prices(report_date, grade);
CREATE INDEX IF NOT EXISTS idx_subprimal_imps ON engine.usda_subprimal_prices(imps_code, report_date);

CREATE TABLE IF NOT EXISTS engine.usda_composites (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    primal VARCHAR(30),
    choice_cwt NUMERIC(10,2),
    select_cwt NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS engine.slaughter_cattle_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    source VARCHAR(50),
    class_description VARCHAR(50),
    selling_basis VARCHAR(30),
    grade_description VARCHAR(30),
    head_count INTEGER,
    avg_weight NUMERIC(10,1),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    weighted_avg_price NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_slaughter_report_date ON engine.slaughter_cattle_prices(report_date);

CREATE TABLE IF NOT EXISTS engine.premiums_discounts (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    type VARCHAR(20),
    class_description VARCHAR(40),
    avg_price NUMERIC(10,2),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    price_change NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS engine.indiana_auction (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    commodity VARCHAR(40),
    class VARCHAR(40),
    quality_grade VARCHAR(30),
    frame VARCHAR(20),
    dressing VARCHAR(10),
    yield_grade VARCHAR(10),
    head_count INTEGER,
    avg_weight NUMERIC(10,1),
    avg_price_min NUMERIC(10,2),
    avg_price_max NUMERIC(10,2),
    avg_price NUMERIC(10,2),
    receipts INTEGER
);

CREATE INDEX IF NOT EXISTS idx_auction_report_date ON engine.indiana_auction(report_date);

-- Pork market data
CREATE TABLE IF NOT EXISTS engine.pork_cutout_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    section VARCHAR(30) NOT NULL,
    item_description TEXT NOT NULL,
    weighted_average NUMERIC(10,2),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    total_pounds BIGINT
);

CREATE INDEX IF NOT EXISTS idx_pork_cutout_date ON engine.pork_cutout_prices(report_date);

CREATE TABLE IF NOT EXISTS engine.pork_primal_values (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    pork_carcass NUMERIC(10,2),
    pork_loin NUMERIC(10,2),
    pork_butt NUMERIC(10,2),
    pork_picnic NUMERIC(10,2),
    pork_rib NUMERIC(10,2),
    pork_ham NUMERIC(10,2),
    pork_belly NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_pork_primal_date ON engine.pork_primal_values(report_date);

CREATE TABLE IF NOT EXISTS engine.pork_live_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    purchase_type VARCHAR(50),
    head_count INTEGER,
    avg_weight NUMERIC(10,1),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    weighted_avg_price NUMERIC(10,2),
    carcass_basis NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_pork_live_date ON engine.pork_live_prices(report_date);

-- Lamb market data
CREATE TABLE IF NOT EXISTS engine.lamb_cutout_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    imps_code VARCHAR(10),
    imps_description TEXT,
    fob_price NUMERIC(10,4),
    percentage_carcass NUMERIC(6,2),
    cut_weight NUMERIC(10,4),
    saddle VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_lamb_cutout_date ON engine.lamb_cutout_prices(report_date);

CREATE TABLE IF NOT EXISTS engine.lamb_carcass_summary (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    gross_carcass_price NUMERIC(10,4),
    foresaddle_price NUMERIC(10,4),
    hindsaddle_price NUMERIC(10,4),
    net_carcass_price NUMERIC(10,4),
    processing_cost NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_lamb_summary_date ON engine.lamb_carcass_summary(report_date);

-- Manual species prices (chicken, goat)
CREATE TABLE IF NOT EXISTS engine.manual_species_prices (
    id SERIAL PRIMARY KEY,
    entry_date DATE NOT NULL DEFAULT CURRENT_DATE,
    species VARCHAR(20) NOT NULL,
    cut_code VARCHAR(30) NOT NULL,
    description TEXT,
    price_per_lb NUMERIC(10,4) NOT NULL,
    yield_pct NUMERIC(6,2),
    source VARCHAR(50) DEFAULT 'manual',
    UNIQUE (species, cut_code, entry_date)
);

CREATE INDEX IF NOT EXISTS idx_manual_species ON engine.manual_species_prices(species, entry_date);

-- Grade hierarchy (kept as-is)
CREATE TABLE IF NOT EXISTS engine.config_grade_hierarchy (
    id SERIAL PRIMARY KEY,
    species VARCHAR(20) NOT NULL,
    grade_code VARCHAR(20) NOT NULL,
    grade_name VARCHAR(50) NOT NULL,
    rank_order INTEGER NOT NULL,
    UNIQUE (species, grade_code)
);

CREATE INDEX IF NOT EXISTS idx_grade_hierarchy_species ON engine.config_grade_hierarchy(species);


-- ---------------------------------------------------------------------------
-- Row-Level Security
-- ---------------------------------------------------------------------------

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.farmer_inventory ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.purchase_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.po_cut_instructions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.config_cut_specs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.slaughter_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.slaughter_order_allocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.processor_costs ENABLE ROW LEVEL SECURITY;

-- Profiles: anyone reads, owner writes
CREATE POLICY profiles_select ON public.profiles FOR SELECT USING (true);
CREATE POLICY profiles_insert ON public.profiles FOR INSERT
    WITH CHECK (auth.uid()::text = profile_id);
CREATE POLICY profiles_update ON public.profiles FOR UPDATE
    USING (auth.uid()::text = profile_id);

-- Farmer inventory: anyone reads, farm owner writes
CREATE POLICY inventory_select ON public.farmer_inventory FOR SELECT USING (true);
CREATE POLICY inventory_insert ON public.farmer_inventory FOR INSERT
    WITH CHECK (profile_id IN (SELECT profile_id FROM public.profiles WHERE profile_id = auth.uid()::text AND type = 'farmer'));
CREATE POLICY inventory_update ON public.farmer_inventory FOR UPDATE
    USING (profile_id IN (SELECT profile_id FROM public.profiles WHERE profile_id = auth.uid()::text AND type = 'farmer'));
CREATE POLICY inventory_delete ON public.farmer_inventory FOR DELETE
    USING (profile_id IN (SELECT profile_id FROM public.profiles WHERE profile_id = auth.uid()::text AND type = 'farmer'));

-- Purchase orders: customers create/read own
CREATE POLICY po_select ON public.purchase_orders FOR SELECT
    USING (auth.uid()::text = profile_id);
CREATE POLICY po_insert ON public.purchase_orders FOR INSERT
    WITH CHECK (auth.uid()::text = profile_id);
CREATE POLICY po_update ON public.purchase_orders FOR UPDATE
    USING (auth.uid()::text = profile_id);

-- PO cut instructions: customers read own PO's instructions
CREATE POLICY po_instructions_select ON public.po_cut_instructions FOR SELECT
    USING (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()::text));
CREATE POLICY po_instructions_insert ON public.po_cut_instructions FOR INSERT
    WITH CHECK (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()::text));

-- Config tables: anyone reads, no client writes
CREATE POLICY config_cut_specs_select ON public.config_cut_specs FOR SELECT USING (true);

-- Slaughter orders: read-only for authenticated users
CREATE POLICY slaughter_orders_select ON public.slaughter_orders FOR SELECT
    USING (auth.role() = 'authenticated');
CREATE POLICY so_allocations_select ON public.slaughter_order_allocations FOR SELECT
    USING (slaughter_order_number IN (SELECT order_number FROM public.slaughter_orders));

-- Processor costs: anyone reads
CREATE POLICY processor_costs_select ON public.processor_costs FOR SELECT USING (true);


-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------

-- Anon: read marketplace data
GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON public.profiles, public.farmer_inventory,
    public.config_cut_specs, public.processor_costs TO anon;

-- Authenticated: read/write marketplace + own data (RLS enforces row-level access)
GRANT USAGE ON SCHEMA public TO authenticated;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO authenticated;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO authenticated;

-- Engine: NO grants to anon/authenticated (invisible to REST API)
-- The postgres role used by Python has full access by default.
