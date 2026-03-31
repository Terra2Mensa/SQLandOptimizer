-- 016_pricing_tables.sql
-- Base prices and share modifiers for customer pricing.
-- Customer price = price_custom.price × price_modifier.modifier
-- Run: psql -d terra_mensa -f sql/016_pricing_tables.sql

-- ─── Base price per species + share ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS price_custom (
    id              SERIAL PRIMARY KEY,
    species         TEXT NOT NULL,
    share           TEXT NOT NULL,
    price           NUMERIC(10,2) NOT NULL,
    effective_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (species, share, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_price_custom_lookup
    ON price_custom(species, share, effective_date DESC);

-- ─── Share modifier (multiplier) ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS price_modifier (
    id              SERIAL PRIMARY KEY,
    species         TEXT NOT NULL,
    share           TEXT NOT NULL,
    modifier        NUMERIC(6,4) NOT NULL,
    effective_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (species, share, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_price_modifier_lookup
    ON price_modifier(species, share, effective_date DESC);

-- ─── Seed price_custom (base prices) ─────────────────────────────────────

INSERT INTO price_custom (species, share, price) VALUES
-- Cattle: $4200 whole, divided proportionally
('cattle', 'whole',   4200.00),
('cattle', 'half',    2100.00),
('cattle', 'quarter', 1050.00),
('cattle', 'eighth',   525.00),
-- Pork: $900 whole
('pork', 'whole',  900.00),
('pork', 'half',   450.00),
('pork', 'quarter', 225.00),
-- Lamb: $820 whole
('lamb', 'whole',  820.00),
('lamb', 'half',   410.00),
-- Goat: $820 whole
('goat', 'whole',  820.00),
('goat', 'half',   410.00)
ON CONFLICT (species, share, effective_date) DO UPDATE SET price = EXCLUDED.price;

-- ─── Seed price_modifier (share multipliers) ─────────────────────────────

INSERT INTO price_modifier (species, share, modifier) VALUES
-- Cattle
('cattle', 'whole',   0.9700),
('cattle', 'half',    1.0000),
('cattle', 'quarter', 1.0500),
('cattle', 'eighth',  1.1000),
-- Pork
('pork', 'whole',   0.9700),
('pork', 'half',    1.0000),
('pork', 'quarter', 1.0500),
-- Lamb
('lamb', 'whole',  0.9700),
('lamb', 'half',   1.0000),
-- Goat
('goat', 'whole',  0.9700),
('goat', 'half',   1.0000)
ON CONFLICT (species, share, effective_date) DO UPDATE SET modifier = EXCLUDED.modifier;
