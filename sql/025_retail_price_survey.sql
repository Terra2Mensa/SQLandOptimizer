-- 025_retail_price_survey.sql
-- Monthly manual entry of local grocery retail meat prices.
-- Used for competitive comparison against D2C pricing.

CREATE TABLE IF NOT EXISTS retail_price_survey (
    id              SERIAL PRIMARY KEY,
    survey_date     DATE NOT NULL,
    store_name      TEXT NOT NULL,
    species         TEXT NOT NULL,
    cut_name        TEXT NOT NULL,
    cut_description TEXT,
    price_per_lb    NUMERIC(8,2) NOT NULL,
    is_sale_price   BOOLEAN DEFAULT false,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (survey_date, store_name, species, cut_name)
);

CREATE INDEX IF NOT EXISTS idx_retail_survey_lookup
    ON retail_price_survey(species, survey_date DESC);

GRANT SELECT ON retail_price_survey TO anon, authenticated;
