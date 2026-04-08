-- 024_weekly_market_prices.sql
-- Weekly market price data: floor (auction), wholesale (cutout), ceiling (retail).
-- Populated by market_data.py + price_model.py. Feeds into price_custom.

CREATE TABLE IF NOT EXISTS weekly_market_prices (
    id                      SERIAL PRIMARY KEY,
    report_date             DATE NOT NULL,
    species                 TEXT NOT NULL,
    quality_grade           TEXT NOT NULL,
    -- Floor (what farmer gets at auction)
    live_price_cwt          NUMERIC(10,2),      -- $/cwt live weight
    dressed_price_cwt       NUMERIC(10,2),      -- $/cwt carcass weight
    floor_whole_animal      NUMERIC(10,2),      -- total $ at auction for typical animal
    -- Wholesale (USDA boxed cutout)
    cutout_value_cwt        NUMERIC(10,2),      -- $/cwt composite cutout
    cutout_whole_animal     NUMERIC(10,2),      -- total $ wholesale value of all cuts
    -- Ceiling (retail grocery estimate)
    retail_value_estimate   NUMERIC(10,2),      -- total $ if sold by the cut at grocery
    retail_per_lb_avg       NUMERIC(10,2),      -- weighted avg $/lb retail
    -- D2C recommended range
    dtc_price_low           NUMERIC(10,2),      -- floor + processor costs + margin
    dtc_price_high          NUMERIC(10,2),      -- competitive vs retail
    dtc_per_lb_hanging      NUMERIC(10,2),      -- $/lb hanging weight
    -- Reference
    typical_live_weight     NUMERIC(10,2),
    typical_hanging_weight  NUMERIC(10,2),
    processor_cost_est      NUMERIC(10,2),
    data_source             TEXT,
    created_at              TIMESTAMPTZ DEFAULT now(),
    UNIQUE (report_date, species, quality_grade)
);

CREATE INDEX IF NOT EXISTS idx_wmp_lookup
    ON weekly_market_prices(species, quality_grade, report_date DESC);

-- Retail markup multipliers by cut category (used by price_model.py)
CREATE TABLE IF NOT EXISTS retail_markup_factors (
    id              SERIAL PRIMARY KEY,
    species         TEXT NOT NULL,
    cut_category    TEXT NOT NULL,       -- 'ground', 'chuck', 'round', 'rib', 'loin', 'brisket', etc.
    markup_low      NUMERIC(6,3) NOT NULL,  -- e.g. 1.30
    markup_high     NUMERIC(6,3) NOT NULL,  -- e.g. 1.50
    markup_default  NUMERIC(6,3) NOT NULL,  -- e.g. 1.40
    notes           TEXT,
    UNIQUE (species, cut_category)
);

-- Seed retail markup factors (from USDA ERS spread data + industry research)
INSERT INTO retail_markup_factors (species, cut_category, markup_low, markup_high, markup_default, notes) VALUES
-- Cattle
('cattle', 'ground',    1.30, 1.50, 1.40, 'Loss leader, high volume'),
('cattle', 'chuck',     1.40, 1.60, 1.50, 'Value cuts: roasts, stew meat'),
('cattle', 'round',     1.40, 1.60, 1.50, 'Value cuts: roasts, steaks'),
('cattle', 'brisket',   1.40, 1.70, 1.55, 'BBQ demand drives premium'),
('cattle', 'plate',     1.50, 1.80, 1.65, 'Short ribs'),
('cattle', 'rib',       1.60, 2.00, 1.80, 'Ribeye, prime rib'),
('cattle', 'loin',      1.50, 1.80, 1.65, 'NY strip, sirloin, tenderloin'),
('cattle', 'tenderloin',1.70, 2.20, 1.95, 'Filet mignon — highest markup'),
('cattle', 'flank',     1.50, 1.80, 1.65, 'Flank steak'),
-- Pork
('pork', 'ground',      1.25, 1.45, 1.35, 'Ground pork'),
('pork', 'shoulder',    1.30, 1.50, 1.40, 'Boston butt, pulled pork'),
('pork', 'loin',        1.40, 1.70, 1.55, 'Chops, tenderloin'),
('pork', 'belly',       1.50, 1.80, 1.65, 'Bacon demand premium'),
('pork', 'ham',         1.30, 1.50, 1.40, 'Cured/fresh ham'),
('pork', 'ribs',        1.50, 1.80, 1.65, 'Spare ribs, baby back'),
-- Lamb
('lamb', 'ground',      1.30, 1.50, 1.40, 'Ground lamb'),
('lamb', 'shoulder',    1.35, 1.55, 1.45, 'Shoulder chops/roast'),
('lamb', 'rack',        1.60, 2.00, 1.80, 'Rack of lamb — premium'),
('lamb', 'loin',        1.50, 1.80, 1.65, 'Loin chops'),
('lamb', 'leg',         1.40, 1.60, 1.50, 'Leg of lamb'),
-- Goat
('goat', 'ground',      1.25, 1.45, 1.35, 'Ground goat'),
('goat', 'shoulder',    1.30, 1.50, 1.40, 'Shoulder'),
('goat', 'rack',        1.50, 1.80, 1.65, 'Rack/chops'),
('goat', 'loin',        1.40, 1.65, 1.52, 'Loin chops'),
('goat', 'leg',         1.35, 1.55, 1.45, 'Leg roast')
ON CONFLICT (species, cut_category) DO UPDATE SET
    markup_low = EXCLUDED.markup_low,
    markup_high = EXCLUDED.markup_high,
    markup_default = EXCLUDED.markup_default,
    notes = EXCLUDED.notes;

-- Grants
GRANT SELECT ON weekly_market_prices TO anon, authenticated;
GRANT SELECT ON retail_markup_factors TO anon, authenticated;
