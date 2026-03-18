-- =============================================================================
-- 006_weekly_pricing.sql — Weekly pricing + share adjustments
-- Run: psql terra_mensa -f sql/006_weekly_pricing.sql
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS weekly_pricing (
    id SERIAL PRIMARY KEY,
    species VARCHAR(20) NOT NULL,
    grade VARCHAR(20) NOT NULL,
    price_per_lb NUMERIC(10,4) NOT NULL,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (species, grade, effective_date)
);

CREATE INDEX idx_weekly_pricing_lookup ON weekly_pricing(species, grade, effective_date DESC);

CREATE TABLE IF NOT EXISTS share_adjustments (
    id SERIAL PRIMARY KEY,
    share VARCHAR(20) NOT NULL,
    adjustment_pct NUMERIC(6,4) NOT NULL DEFAULT 0,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (share, effective_date)
);

CREATE INDEX idx_share_adjustments_lookup ON share_adjustments(share, effective_date DESC);

COMMIT;
