-- Migration: Merge valuations + species_valuations into unified valuations table
-- Run: psql cattle_valuation -f migrate_valuations.sql

BEGIN;

-- Create the new unified table
CREATE TABLE IF NOT EXISTS valuations_new (
    id SERIAL PRIMARY KEY,
    valuation_date TIMESTAMP NOT NULL DEFAULT NOW(),
    species VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    live_weight NUMERIC(10,1),
    yield_grade INTEGER,
    quality_grade VARCHAR(20),
    dressing_pct NUMERIC(5,3),
    hot_carcass_weight NUMERIC(10,1),
    total_cut_value NUMERIC(12,2),
    byproduct_value NUMERIC(10,2),
    gross_value NUMERIC(12,2),
    processing_cost NUMERIC(10,2),
    net_value NUMERIC(12,2),
    value_per_lb_carcass NUMERIC(10,4),
    value_per_lb_live NUMERIC(10,4),
    cut_detail_json JSONB
);

-- Migrate existing cattle valuations (convert $/cwt to $/lb)
INSERT INTO valuations_new (valuation_date, species, report_date, live_weight,
    yield_grade, quality_grade, dressing_pct, hot_carcass_weight,
    total_cut_value, byproduct_value, gross_value, processing_cost,
    net_value, value_per_lb_carcass, value_per_lb_live, cut_detail_json)
SELECT valuation_date, 'cattle', report_date, live_weight,
    yield_grade, quality_grade, dressing_pct, hot_carcass_weight,
    total_subprimal_value, byproduct_value, gross_carcass_value, broker_fee,
    net_carcass_value, value_per_cwt_carcass / 100.0, value_per_cwt_live / 100.0,
    cut_detail_json
FROM valuations;

-- Migrate existing species valuations
INSERT INTO valuations_new (valuation_date, species, report_date, live_weight,
    dressing_pct, hot_carcass_weight, total_cut_value, byproduct_value,
    gross_value, processing_cost, net_value, value_per_lb_live, cut_detail_json)
SELECT valuation_date, species, report_date, live_weight,
    dress_pct, hot_carcass_weight, total_cut_value, byproduct_value,
    gross_value, processing_cost, net_value, value_per_lb_live, cut_detail_json
FROM species_valuations;

-- Swap tables
ALTER TABLE valuations RENAME TO valuations_old;
ALTER TABLE species_valuations RENAME TO species_valuations_old;
ALTER TABLE valuations_new RENAME TO valuations;

-- Drop old indexes
DROP INDEX IF EXISTS idx_valuations_date;
DROP INDEX IF EXISTS idx_species_val_date;

-- Create new indexes
CREATE INDEX IF NOT EXISTS idx_valuations_species_date ON valuations(species, valuation_date);
CREATE INDEX IF NOT EXISTS idx_valuations_grade ON valuations(quality_grade, report_date);

COMMIT;

-- Verify migration
SELECT species, COUNT(*) as rows, MIN(report_date) as earliest, MAX(report_date) as latest
FROM valuations GROUP BY species ORDER BY species;

-- After verifying, uncomment to drop old tables:
-- DROP TABLE valuations_old;
-- DROP TABLE species_valuations_old;
