-- 020_phase4_advanced.sql
-- Phase 4: Advanced optimizer features — processor capabilities, quality matching,
-- seasonal blackouts, reliability tracking, geographic zones.

-- ─── 1. Processor capabilities ──────────────────────────────────────────
-- Track what each processor can do (smoking, curing, sausage, etc.)
-- Used by optimizer to match cut sheet requirements to processor services.

ALTER TABLE processor_costs ADD COLUMN IF NOT EXISTS capabilities TEXT[] DEFAULT '{}';
ALTER TABLE processor_costs ADD COLUMN IF NOT EXISTS inspection_type TEXT DEFAULT 'custom_exempt'
    CHECK (inspection_type IN ('custom_exempt', 'usda_inspected', 'both'));

-- Seed capabilities for existing processors
-- (Update with real data — these are realistic defaults)
UPDATE processor_costs SET capabilities = ARRAY['basic_cut', 'grind']
WHERE capabilities = '{}' OR capabilities IS NULL;


-- ─── 2. Processor seasonal blackouts ───────────────────────────────────
-- Many Michiana processors block livestock for 5-6 weeks during deer season.

CREATE TABLE IF NOT EXISTS processor_blackouts (
    id              SERIAL PRIMARY KEY,
    processor_id    UUID NOT NULL,
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    reason          TEXT,
    species         TEXT,  -- NULL = all species blocked
    UNIQUE (processor_id, start_date, end_date)
);

CREATE INDEX IF NOT EXISTS idx_blackouts_dates ON processor_blackouts(start_date, end_date);


-- ─── 3. Processor reliability tracking ─────────────────────────────────
-- Track on-time performance for reliability scoring in the optimizer.

CREATE TABLE IF NOT EXISTS processor_performance (
    id                  SERIAL PRIMARY KEY,
    processor_id        UUID NOT NULL,
    slaughter_order     VARCHAR(50),
    scheduled_date      DATE,
    actual_date         DATE,
    days_variance       INT,  -- negative = early, positive = late
    quality_score       INT CHECK (quality_score BETWEEN 1 AND 5),  -- 1=poor, 5=excellent
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_proc_perf_processor ON processor_performance(processor_id);


-- ─── 4. Farmer inventory quality fields ────────────────────────────────
-- Add quality attributes for matching (breed, finish method, quality tier).

ALTER TABLE farmer_inventory ADD COLUMN IF NOT EXISTS quality_tier TEXT DEFAULT 'standard'
    CHECK (quality_tier IN ('premium', 'standard', 'economy'));
ALTER TABLE farmer_inventory ADD COLUMN IF NOT EXISTS finish_method TEXT DEFAULT 'grain'
    CHECK (finish_method IN ('grass', 'grain', 'mixed'));
ALTER TABLE farmer_inventory ADD COLUMN IF NOT EXISTS breed TEXT;


-- ─── 5. Geographic zones (for clustering) ──────────────────────────────
-- Pre-defined zones for the Michiana area. Optimizer penalizes cross-zone batches.

CREATE TABLE IF NOT EXISTS geographic_zones (
    id              SERIAL PRIMARY KEY,
    zone_name       TEXT NOT NULL UNIQUE,
    center_lat      NUMERIC(9,6) NOT NULL,
    center_lng      NUMERIC(9,6) NOT NULL,
    radius_miles    NUMERIC(6,2) DEFAULT 15.0
);

INSERT INTO geographic_zones (zone_name, center_lat, center_lng, radius_miles) VALUES
('South Bend',    41.6764, -86.2520, 12),
('Elkhart',       41.6820, -85.9767, 12),
('Goshen',        41.5823, -85.8347, 10),
('Mishawaka',     41.6619, -86.1586, 8),
('Niles',         41.8297, -86.2542, 10),
('Plymouth',      41.3437, -86.3089, 12),
('Bremen-Wakarusa', 41.4915, -86.0850, 10)
ON CONFLICT (zone_name) DO UPDATE SET
    center_lat = EXCLUDED.center_lat, center_lng = EXCLUDED.center_lng;


-- ─── 6. Demand history (for forecasting) ───────────────────────────────
-- Aggregated demand snapshots for stochastic optimization.

CREATE TABLE IF NOT EXISTS demand_snapshots (
    id              SERIAL PRIMARY KEY,
    snapshot_date   DATE NOT NULL,
    species         TEXT NOT NULL,
    share           TEXT NOT NULL,
    pending_count   INT NOT NULL DEFAULT 0,
    confirmed_count INT NOT NULL DEFAULT 0,
    total_fraction  NUMERIC(8,3) DEFAULT 0,
    UNIQUE (snapshot_date, species, share)
);

CREATE INDEX IF NOT EXISTS idx_demand_snap_date ON demand_snapshots(snapshot_date DESC);


-- ─── 7. Optimizer run log ──────────────────────────────────────────────
-- Track every optimizer run for analysis and auditing.

CREATE TABLE IF NOT EXISTS optimizer_runs (
    id                  SERIAL PRIMARY KEY,
    run_at              TIMESTAMPTZ DEFAULT now(),
    mode                TEXT NOT NULL,  -- 'phase1', 'unified'
    species             TEXT,
    pos_pending         INT,
    pos_assigned        INT,
    batches_formed      INT,
    batches_assigned    INT,
    total_cost          NUMERIC(12,2),
    solve_time_seconds  NUMERIC(8,3),
    solver_status       TEXT,
    notes               TEXT
);


-- ─── Grants (Supabase) ─────────────────────────────────────────────────
GRANT SELECT ON processor_blackouts TO anon, authenticated;
GRANT SELECT ON processor_performance TO authenticated;
GRANT SELECT ON geographic_zones TO anon, authenticated;
GRANT SELECT ON demand_snapshots TO authenticated;
GRANT SELECT ON optimizer_runs TO authenticated;
