-- =============================================================================
-- Terra Mensa — Supabase schema
-- Single source of truth for both website and backend.
-- Run: psql "$SUPA_CONN" -f sql/supabase_init.sql
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─── Profiles (unified: farmer, customer, processor) ─────────────────────

CREATE TABLE IF NOT EXISTS public.profiles (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT NOT NULL CHECK (type IN ('farmer','customer','processor')),
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT UNIQUE,
    phone           TEXT,
    address         TEXT,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    company_name    TEXT,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profiles_type ON public.profiles(type);
CREATE INDEX IF NOT EXISTS idx_profiles_email ON public.profiles(email);

-- ─── Farmer inventory ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.farmer_inventory (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    profile_id              UUID NOT NULL REFERENCES public.profiles(id),
    species                 TEXT NOT NULL CHECK (species IN ('cattle','pork','lamb','goat','chicken')),
    live_weight_est         NUMERIC(10,1),
    expected_grade          TEXT,
    expected_finish_date    DATE,
    description             TEXT,
    active                  BOOLEAN DEFAULT TRUE,
    status                  TEXT NOT NULL DEFAULT 'available'
                            CHECK (status IN ('available','reserved','processing','complete')),
    created_at              TIMESTAMPTZ DEFAULT now(),
    updated_at              TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inventory_profile ON public.farmer_inventory(profile_id);
CREATE INDEX IF NOT EXISTS idx_inventory_species_status ON public.farmer_inventory(species, status);

-- ─── Purchase orders (share-based) ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.purchase_orders (
    po_number       VARCHAR(50) PRIMARY KEY,
    profile_id      UUID NOT NULL REFERENCES public.profiles(id),
    species         TEXT NOT NULL,
    share           TEXT NOT NULL CHECK (share IN ('whole','half','quarter','eighth','uncut')),
    inventory_id    UUID REFERENCES public.farmer_inventory(id),
    note            TEXT,
    order_date      TIMESTAMPTZ DEFAULT now(),
    deposit         NUMERIC(10,2) DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','confirmed','processing','ready','complete','cancelled')),
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_po_profile ON public.purchase_orders(profile_id);
CREATE INDEX IF NOT EXISTS idx_po_status ON public.purchase_orders(status);
CREATE INDEX IF NOT EXISTS idx_po_species_status ON public.purchase_orders(species, status);

-- ─── Processor costs (per species, effective-date versioned) ────────────

CREATE TABLE IF NOT EXISTS public.processor_costs (
    id              SERIAL PRIMARY KEY,
    profile_id      UUID NOT NULL REFERENCES public.profiles(id),
    species         TEXT NOT NULL,
    kill_fee        NUMERIC(10,2) NOT NULL,
    fab_cost_per_lb NUMERIC(10,4) NOT NULL,
    shrink_pct      NUMERIC(6,4) NOT NULL,
    daily_capacity_head INTEGER,
    effective_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (profile_id, species, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_processor_costs_lookup
    ON public.processor_costs(profile_id, species, effective_date DESC);

-- ─── Weekly pricing (per species + grade, effective-date versioned) ─────

CREATE TABLE IF NOT EXISTS public.weekly_pricing (
    id              SERIAL PRIMARY KEY,
    species         TEXT NOT NULL,
    grade           TEXT NOT NULL,
    price_per_lb    NUMERIC(10,4) NOT NULL,
    effective_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (species, grade, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_weekly_pricing_lookup
    ON public.weekly_pricing(species, grade, effective_date DESC);

-- ─── Share adjustments (premium/discount by share size) ─────────────────

CREATE TABLE IF NOT EXISTS public.share_adjustments (
    id              SERIAL PRIMARY KEY,
    share           TEXT NOT NULL,
    adjustment_pct  NUMERIC(6,4) NOT NULL DEFAULT 0,
    effective_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (share, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_share_adjustments_lookup
    ON public.share_adjustments(share, effective_date DESC);

-- ─── Distance matrix (cached driving distances between profiles) ────────

CREATE TABLE IF NOT EXISTS public.distance_matrix (
    id              SERIAL PRIMARY KEY,
    origin_profile_id       UUID NOT NULL REFERENCES public.profiles(id),
    destination_profile_id  UUID NOT NULL REFERENCES public.profiles(id),
    distance_miles          NUMERIC(8,2) NOT NULL,
    duration_minutes        NUMERIC(8,1),
    route_source            TEXT NOT NULL DEFAULT 'google_routes',
    calculated_at           TIMESTAMPTZ DEFAULT now(),
    UNIQUE (origin_profile_id, destination_profile_id),
    CHECK (origin_profile_id < destination_profile_id)
);

CREATE INDEX IF NOT EXISTS idx_distance_matrix_origin ON public.distance_matrix(origin_profile_id);
CREATE INDEX IF NOT EXISTS idx_distance_matrix_dest ON public.distance_matrix(destination_profile_id);

-- ─── Contact requests (website form) ────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.contact_requests (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    role_interest TEXT,
    subject     TEXT,
    message     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'new',
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ─── Cut sheet configs (UI form config, one row per species) ────────────

CREATE TABLE IF NOT EXISTS public.cut_sheet_configs (
    species     TEXT PRIMARY KEY,
    config      JSONB NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- ─── Cut sheet templates (named presets for auto-fill) ──────────────────

CREATE TABLE IF NOT EXISTS public.cut_sheet_templates (
    id          SERIAL PRIMARY KEY,
    species     TEXT NOT NULL REFERENCES public.cut_sheet_configs(species),
    share_size  TEXT NOT NULL,
    name        TEXT NOT NULL,
    label       TEXT NOT NULL,
    description TEXT,
    selections  JSONB NOT NULL,
    sort_order  INT DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (species, share_size, name)
);

-- ─── Cut sheet tables (per-species customer selections) ─────────────────

CREATE TABLE IF NOT EXISTS public.beef_cut_sheets (
    id          SERIAL PRIMARY KEY,
    po_number   VARCHAR(50) REFERENCES public.purchase_orders(po_number),
    half_label  TEXT NOT NULL CHECK (half_label IN ('A','B')),
    selections  JSONB NOT NULL DEFAULT '{}',
    notes       TEXT,
    share_size  TEXT NOT NULL DEFAULT '1/2'
                CHECK (share_size IN ('1/2','1/4','1/8')),
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (po_number, half_label)
);

CREATE INDEX IF NOT EXISTS idx_beef_cut_sheets_po ON public.beef_cut_sheets(po_number);

CREATE TABLE IF NOT EXISTS public.pork_cut_sheets (
    id          SERIAL PRIMARY KEY,
    po_number   VARCHAR(50) REFERENCES public.purchase_orders(po_number),
    half_label  TEXT NOT NULL CHECK (half_label IN ('A','B')),
    selections  JSONB NOT NULL DEFAULT '{}',
    notes       TEXT,
    share_size  TEXT NOT NULL DEFAULT '1/1'
                CHECK (share_size IN ('1/1','1/2','1/4')),
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (po_number, half_label)
);

CREATE INDEX IF NOT EXISTS idx_pork_cut_sheets_po ON public.pork_cut_sheets(po_number);

CREATE TABLE IF NOT EXISTS public.lamb_cut_sheets (
    id          SERIAL PRIMARY KEY,
    po_number   VARCHAR(50) REFERENCES public.purchase_orders(po_number),
    half_label  TEXT NOT NULL CHECK (half_label IN ('A','B')),
    selections  JSONB NOT NULL DEFAULT '{}',
    notes       TEXT,
    share_size  TEXT NOT NULL DEFAULT '1/1'
                CHECK (share_size IN ('1/1','1/2','uncut')),
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (po_number, half_label)
);

CREATE INDEX IF NOT EXISTS idx_lamb_cut_sheets_po ON public.lamb_cut_sheets(po_number);

CREATE TABLE IF NOT EXISTS public.goat_cut_sheets (
    id          SERIAL PRIMARY KEY,
    po_number   VARCHAR(50) REFERENCES public.purchase_orders(po_number),
    half_label  TEXT NOT NULL CHECK (half_label IN ('A','B')),
    selections  JSONB NOT NULL DEFAULT '{}',
    notes       TEXT,
    share_size  TEXT NOT NULL DEFAULT '1/1'
                CHECK (share_size IN ('1/1','1/2','uncut')),
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (po_number, half_label)
);

CREATE INDEX IF NOT EXISTS idx_goat_cut_sheets_po ON public.goat_cut_sheets(po_number);

-- ─── Row-Level Security ─────────────────────────────────────────────────

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.farmer_inventory ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.purchase_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.processor_costs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.weekly_pricing ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.distance_matrix ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.share_adjustments ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.contact_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cut_sheet_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cut_sheet_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.beef_cut_sheets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pork_cut_sheets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.lamb_cut_sheets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.goat_cut_sheets ENABLE ROW LEVEL SECURITY;

-- ─── RLS Policies ───────────────────────────────────────────────────────

-- Helper: get current user's app role from JWT metadata
CREATE OR REPLACE FUNCTION public.current_app_role()
RETURNS TEXT LANGUAGE SQL STABLE AS $$
  SELECT coalesce(auth.jwt() -> 'user_metadata' ->> 'type', 'customer');
$$;

-- Profiles: read all, write own
CREATE POLICYprofiles_read ON public.profiles
    FOR SELECT TO anon, authenticated USING (true);
CREATE POLICYprofiles_insert_own ON public.profiles
    FOR INSERT TO authenticated WITH CHECK (auth.uid() = id);
CREATE POLICYprofiles_update_own ON public.profiles
    FOR UPDATE TO authenticated USING (auth.uid() = id) WITH CHECK (auth.uid() = id);

-- Farmer inventory: public read, farm owner writes
CREATE POLICYinventory_read ON public.farmer_inventory
    FOR SELECT TO anon, authenticated USING (true);
CREATE POLICYinventory_write ON public.farmer_inventory
    FOR INSERT TO authenticated
    WITH CHECK (profile_id = auth.uid() AND EXISTS (
        SELECT 1 FROM public.profiles WHERE id = auth.uid() AND type = 'farmer'
    ));
CREATE POLICYinventory_update ON public.farmer_inventory
    FOR UPDATE TO authenticated
    USING (profile_id = auth.uid());
CREATE POLICYinventory_delete ON public.farmer_inventory
    FOR DELETE TO authenticated
    USING (profile_id = auth.uid());

-- Purchase orders: customer creates/reads own, admin reads all
CREATE POLICYpo_read_own ON public.purchase_orders
    FOR SELECT TO authenticated USING (profile_id = auth.uid());
CREATE POLICYpo_read_admin ON public.purchase_orders
    FOR SELECT TO authenticated USING (public.current_app_role() = 'admin');
CREATE POLICYpo_insert_own ON public.purchase_orders
    FOR INSERT TO authenticated WITH CHECK (profile_id = auth.uid());
CREATE POLICYpo_update_own ON public.purchase_orders
    FOR UPDATE TO authenticated USING (profile_id = auth.uid());

-- Processor costs: public read
CREATE POLICYprocessor_costs_read ON public.processor_costs
    FOR SELECT TO anon, authenticated USING (true);

-- Weekly pricing: public read
CREATE POLICYweekly_pricing_read ON public.weekly_pricing
    FOR SELECT TO anon, authenticated USING (true);

-- Share adjustments: public read
CREATE POLICYshare_adjustments_read ON public.share_adjustments
    FOR SELECT TO anon, authenticated USING (true);

-- Distance matrix: public read, authenticated insert
CREATE POLICYdistance_matrix_read ON public.distance_matrix
    FOR SELECT TO anon, authenticated USING (true);
CREATE POLICYdistance_matrix_insert ON public.distance_matrix
    FOR INSERT TO authenticated WITH CHECK (true);

-- Contact requests: anyone inserts, admin reads
CREATE POLICYcontact_insert ON public.contact_requests
    FOR INSERT TO anon, authenticated WITH CHECK (true);
CREATE POLICYcontact_admin_read ON public.contact_requests
    FOR SELECT TO authenticated USING (public.current_app_role() = 'admin');
CREATE POLICYcontact_admin_update ON public.contact_requests
    FOR UPDATE TO authenticated
    USING (public.current_app_role() = 'admin')
    WITH CHECK (public.current_app_role() = 'admin');

-- Cut sheet configs + templates: public read (reference data)
CREATE POLICYcut_configs_read ON public.cut_sheet_configs
    FOR SELECT TO anon, authenticated USING (true);
CREATE POLICYcut_templates_read ON public.cut_sheet_templates
    FOR SELECT TO anon, authenticated USING (true);

-- Cut sheets: customer reads own PO's sheets, admin reads all
CREATE POLICYbeef_sheets_read_own ON public.beef_cut_sheets
    FOR SELECT TO authenticated
    USING (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));
CREATE POLICYbeef_sheets_insert ON public.beef_cut_sheets
    FOR INSERT TO authenticated
    WITH CHECK (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));

CREATE POLICYpork_sheets_read_own ON public.pork_cut_sheets
    FOR SELECT TO authenticated
    USING (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));
CREATE POLICYpork_sheets_insert ON public.pork_cut_sheets
    FOR INSERT TO authenticated
    WITH CHECK (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));

CREATE POLICYlamb_sheets_read_own ON public.lamb_cut_sheets
    FOR SELECT TO authenticated
    USING (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));
CREATE POLICYlamb_sheets_insert ON public.lamb_cut_sheets
    FOR INSERT TO authenticated
    WITH CHECK (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));

CREATE POLICYgoat_sheets_read_own ON public.goat_cut_sheets
    FOR SELECT TO authenticated
    USING (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));
CREATE POLICYgoat_sheets_insert ON public.goat_cut_sheets
    FOR INSERT TO authenticated
    WITH CHECK (po_number IN (SELECT po_number FROM public.purchase_orders WHERE profile_id = auth.uid()));

-- ─── Auth trigger: auto-create profile on signup ────────────────────────

CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, auth AS $$
BEGIN
    INSERT INTO public.profiles (id, type, first_name, last_name, email, phone, address, latitude, longitude, company_name)
    VALUES (
        new.id,
        coalesce(nullif(new.raw_user_meta_data ->> 'type', ''), 'customer'),
        coalesce(
            nullif(new.raw_user_meta_data ->> 'first_name', ''),
            nullif(split_part(coalesce(new.email, ''), '@', 1), ''),
            'User'
        ),
        nullif(new.raw_user_meta_data ->> 'last_name', ''),
        new.email,
        nullif(new.raw_user_meta_data ->> 'phone', ''),
        nullif(new.raw_user_meta_data ->> 'address', ''),
        (nullif(new.raw_user_meta_data ->> 'latitude', ''))::numeric,
        (nullif(new.raw_user_meta_data ->> 'longitude', ''))::numeric,
        nullif(new.raw_user_meta_data ->> 'company_name', '')
    )
    ON CONFLICT (id) DO UPDATE SET
        email = EXCLUDED.email,
        first_name = coalesce(EXCLUDED.first_name, public.profiles.first_name),
        last_name = coalesce(EXCLUDED.last_name, public.profiles.last_name),
        phone = coalesce(EXCLUDED.phone, public.profiles.phone),
        address = coalesce(EXCLUDED.address, public.profiles.address),
        latitude = coalesce(EXCLUDED.latitude, public.profiles.latitude),
        longitude = coalesce(EXCLUDED.longitude, public.profiles.longitude),
        company_name = coalesce(EXCLUDED.company_name, public.profiles.company_name);

    RETURN new;
END;
$$;

DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE PROCEDURE public.handle_new_user();

-- ─── Grants ─────────────────────────────────────────────────────────────

GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON public.profiles, public.farmer_inventory, public.processor_costs,
    public.weekly_pricing, public.share_adjustments, public.cut_sheet_configs,
    public.cut_sheet_templates TO anon;

GRANT USAGE ON SCHEMA public TO authenticated;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO authenticated;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO authenticated;
