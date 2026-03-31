-- 018_optimizer_config.sql
-- Optimizer configuration: costs, constraints, and parameters.
-- Run: psql -d terra_mensa -f sql/018_optimizer_config.sql

CREATE TABLE IF NOT EXISTS optimizer_config (
    key         TEXT PRIMARY KEY,
    value       NUMERIC NOT NULL,
    unit        TEXT,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

INSERT INTO optimizer_config (key, value, unit) VALUES
('farmer_transport_per_mile',    2.00, '$/mile'),
('customer_transport_per_mile',  1.00, '$/mile'),
('max_farmer_distance_miles',   50.00, 'miles'),
('max_customer_distance_miles', 50.00, 'miles'),
('fill_threshold',               1.00, 'fraction of whole animal'),
('run_interval_hours',           6.00, 'hours'),
('time_distance_penalty',        0.00, '$/mile*day')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit, updated_at = now();

-- Add processing_cost to slaughter_orders
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS processing_cost NUMERIC(10,2);
