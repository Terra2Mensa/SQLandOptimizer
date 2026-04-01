-- 019_phase3_economics.sql
-- Phase 3: Economics layer — batch-fill pricing, seasonal adjustments,
-- farmer payments, optimizer weights.
-- Run: psql -d terra_mensa -f sql/019_phase3_economics.sql

-- ─── 1. Widen share-size price modifiers ────────────────────────────────
-- Research: Maskin & Riley (1984), Wilson (1993) — optimal nonlinear pricing
-- Current: 0.97–1.10 (too narrow)
-- Target:  0.85–1.25 (incentive-compatible, wider spread)

-- New effective date rows (old rows preserved for history)
INSERT INTO price_modifier (species, share, modifier, effective_date) VALUES
-- Cattle
('cattle', 'whole',   0.9000, '2026-04-02'),
('cattle', 'half',    1.0000, '2026-04-02'),
('cattle', 'quarter', 1.1000, '2026-04-02'),
('cattle', 'eighth',  1.2000, '2026-04-02'),
-- Pork
('pork', 'whole',     0.9000, '2026-04-02'),
('pork', 'half',      1.0000, '2026-04-02'),
('pork', 'quarter',   1.1000, '2026-04-02'),
-- Lamb
('lamb', 'whole',     0.9000, '2026-04-02'),
('lamb', 'half',      1.0000, '2026-04-02'),
('lamb', 'uncut',     0.8500, '2026-04-02'),
-- Goat
('goat', 'whole',     0.9000, '2026-04-02'),
('goat', 'half',      1.0000, '2026-04-02'),
('goat', 'uncut',     0.8500, '2026-04-02')
ON CONFLICT (species, share, effective_date) DO UPDATE SET modifier = EXCLUDED.modifier;


-- ─── 2. Seasonal pricing adjustments ────────────────────────────────────
-- Research: USDA AMS seasonal data, Pork Checkoff indexes
-- Counter-cyclical: discount off-peak, premium in-peak

CREATE TABLE IF NOT EXISTS seasonal_pricing (
    id              SERIAL PRIMARY KEY,
    species         TEXT NOT NULL,
    month_start     INT NOT NULL CHECK (month_start BETWEEN 1 AND 12),
    month_end       INT NOT NULL CHECK (month_end BETWEEN 1 AND 12),
    adjustment      NUMERIC(6,4) NOT NULL DEFAULT 1.0000,
    label           TEXT,
    UNIQUE (species, month_start, month_end)
);

-- Seed seasonal adjustments (multiplier on top of base price)
INSERT INTO seasonal_pricing (species, month_start, month_end, adjustment, label) VALUES
-- Cattle: peak May-Sep (grilling), trough Jan-Mar
('cattle', 1, 3,  0.9700, 'Winter off-peak: 3% discount'),
('cattle', 5, 9,  1.0200, 'Grilling season: 2% premium'),
('cattle', 11, 12, 1.0100, 'Holiday: 1% premium'),
-- Pork: peak Jun-Aug (ribs), trough Jan-Feb
('pork', 1, 2, 0.9700, 'Winter trough: 3% discount'),
('pork', 6, 8, 1.0200, 'Summer grilling: 2% premium'),
-- Lamb: peak Mar-Apr (Easter/Passover)
('lamb', 3, 4, 1.0300, 'Easter/Passover: 3% premium'),
('lamb', 6, 9, 0.9700, 'Summer trough: 3% discount'),
-- Goat: peaks around religious holidays (variable)
('goat', 3, 4, 1.0200, 'Easter: 2% premium'),
('goat', 6, 9, 0.9700, 'Summer trough: 3% discount')
ON CONFLICT (species, month_start, month_end) DO UPDATE SET
    adjustment = EXCLUDED.adjustment, label = EXCLUDED.label;


-- ─── 3. Batch-fill pricing (dynamic) ───────────────────────────────────
-- Research: Belobaba (1987) EMSR, Gallego & van Ryzin (1994)
-- Three phases: early-bird (0-25%), standard (25-75%), close-out (75-100%)

CREATE TABLE IF NOT EXISTS batch_pricing_rules (
    id              SERIAL PRIMARY KEY,
    fill_min        NUMERIC(4,2) NOT NULL,  -- e.g. 0.00
    fill_max        NUMERIC(4,2) NOT NULL,  -- e.g. 0.25
    adjustment      NUMERIC(6,4) NOT NULL,  -- multiplier
    label           TEXT,
    stale_days      INT,                     -- if batch open > N days, apply close-out
    stale_adjustment NUMERIC(6,4),           -- close-out multiplier
    UNIQUE (fill_min, fill_max)
);

INSERT INTO batch_pricing_rules (fill_min, fill_max, adjustment, label, stale_days, stale_adjustment) VALUES
(0.00, 0.25, 0.9600, 'Early-bird: 4% discount',   NULL, NULL),
(0.25, 0.75, 1.0000, 'Standard price',             21,   0.9300),  -- if stale >21 days: 7% discount
(0.75, 1.00, 1.0200, 'Last-share: 2% premium',     NULL, NULL)
ON CONFLICT (fill_min, fill_max) DO UPDATE SET
    adjustment = EXCLUDED.adjustment, label = EXCLUDED.label,
    stale_days = EXCLUDED.stale_days, stale_adjustment = EXCLUDED.stale_adjustment;


-- ─── 4. Open batches tracking ───────────────────────────────────────────
-- Tracks partially-filled batches between optimizer runs

CREATE TABLE IF NOT EXISTS open_batches (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    species         TEXT NOT NULL,
    fill_fraction   NUMERIC(4,3) NOT NULL DEFAULT 0.000,
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'dispatched', 'cancelled')),
    dispatched_at   TIMESTAMPTZ,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_open_batches_species ON open_batches(species, status);


-- ─── 5. Farmer payment model ────────────────────────────────────────────
-- Research: Nash bargaining, Crowd Cow failures, Extension pricing guides
-- Escrow: collect from customer → hold → release to farmer in milestones

CREATE TABLE IF NOT EXISTS farmer_payments (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slaughter_order     VARCHAR(50),
    farmer_profile_id   UUID,
    -- Pricing
    commodity_base_per_lb   NUMERIC(10,2),      -- sale barn equivalent $/lb hanging
    dtc_price_per_lb        NUMERIC(10,2),      -- direct-to-consumer price $/lb hanging
    platform_fee_pct        NUMERIC(5,4) DEFAULT 0.1000,  -- 10% take rate
    -- Computed
    hanging_weight          NUMERIC(10,2),
    gross_revenue           NUMERIC(10,2),       -- dtc_price * hanging_weight
    platform_fee            NUMERIC(10,2),       -- gross * platform_fee_pct
    farmer_gross            NUMERIC(10,2),       -- gross - platform_fee - processor_cost
    -- Milestones
    milestone_1_amount      NUMERIC(10,2),       -- 90% of farmer_gross, paid on delivery to processor
    milestone_1_paid_at     TIMESTAMPTZ,
    milestone_2_amount      NUMERIC(10,2),       -- 10% of farmer_gross, paid on hanging weight confirm
    milestone_2_paid_at     TIMESTAMPTZ,
    -- Status
    status                  TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'milestone_1_paid', 'complete', 'disputed')),
    created_at              TIMESTAMPTZ DEFAULT now(),
    updated_at              TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_farmer_payments_farmer ON farmer_payments(farmer_profile_id);
CREATE INDEX IF NOT EXISTS idx_farmer_payments_status ON farmer_payments(status);
CREATE INDEX IF NOT EXISTS idx_farmer_payments_so ON farmer_payments(slaughter_order);


-- ─── 6. Customer payment tracking ──────────────────────────────────────
-- Tracks customer payment and escrow status

CREATE TABLE IF NOT EXISTS customer_payments (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    po_number           VARCHAR(50),
    profile_id          UUID,
    -- Amounts
    base_price          NUMERIC(10,2),       -- from price_custom
    share_modifier      NUMERIC(6,4),        -- from price_modifier
    seasonal_modifier   NUMERIC(6,4) DEFAULT 1.0000,
    batch_fill_modifier NUMERIC(6,4) DEFAULT 1.0000,
    final_price         NUMERIC(10,2),       -- base * share * seasonal * batch_fill
    -- Payment method
    payment_method      TEXT CHECK (payment_method IN ('card', 'ach', 'check')),
    processing_fee      NUMERIC(10,2),       -- Stripe: 2.9% card, 0.8% ACH
    -- Status
    paid_at             TIMESTAMPTZ,
    refunded_at         TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'unpaid'
                        CHECK (status IN ('unpaid', 'paid', 'escrowed', 'released', 'refunded')),
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_payments_po ON customer_payments(po_number);
CREATE INDEX IF NOT EXISTS idx_customer_payments_status ON customer_payments(status);


-- ─── 7. Optimizer config: add weights and economics params ──────────────

INSERT INTO optimizer_config (key, value) VALUES
-- Multi-objective weights (Phase 2)
('w_cost',          '1.0'),
('w_avg_wait',      '0.3'),
('w_max_wait',      '0.5'),
('w_util_balance',  '0.2'),
('w_geo_penalty',   '0.1'),
-- Economics (Phase 3)
('platform_fee_pct',        '0.10'),   -- 10% take rate
('early_bird_discount',     '0.04'),   -- 4% off for first 25%
('last_share_premium',      '0.02'),   -- 2% premium for last share
('stale_batch_days',        '21'),     -- days before close-out pricing
('stale_batch_discount',    '0.07'),   -- 7% discount on stale batches
('farmer_milestone_1_pct',  '0.90'),   -- 90% paid on delivery to processor
('farmer_milestone_2_pct',  '0.10')    -- 10% paid on hanging weight confirm
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;


-- ─── 8. Commodity base prices (for farmer payment calculation) ──────────
-- Updated periodically from USDA market data / valuation scripts

CREATE TABLE IF NOT EXISTS commodity_base_prices (
    id              SERIAL PRIMARY KEY,
    species         TEXT NOT NULL,
    price_per_lb    NUMERIC(10,2) NOT NULL,  -- $/lb hanging weight equivalent
    source          TEXT,                     -- e.g. 'USDA DataMart 2461'
    effective_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (species, effective_date)
);

INSERT INTO commodity_base_prices (species, price_per_lb, source) VALUES
('cattle', 3.10, 'USDA estimate — sale barn equiv per lb hanging'),
('pork',   1.85, 'USDA estimate — sale barn equiv per lb hanging'),
('lamb',   3.50, 'USDA estimate — sale barn equiv per lb hanging'),
('goat',   3.75, 'USDA estimate — sale barn equiv per lb hanging')
ON CONFLICT (species, effective_date) DO UPDATE SET
    price_per_lb = EXCLUDED.price_per_lb, source = EXCLUDED.source;


-- ─── Grants ─────────────────────────────────────────────────────────────
GRANT SELECT ON seasonal_pricing TO anon, authenticated;
GRANT SELECT ON batch_pricing_rules TO anon, authenticated;
GRANT SELECT ON open_batches TO anon, authenticated;
GRANT SELECT ON commodity_base_prices TO anon, authenticated;
GRANT SELECT ON farmer_payments TO authenticated;
GRANT SELECT ON customer_payments TO authenticated;
