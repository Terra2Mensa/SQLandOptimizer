-- 017_slaughter_orders.sql
-- Slaughter orders link POs to a processor and animal.
-- Run: psql -d terra_mensa -f sql/017_slaughter_orders.sql

CREATE TABLE IF NOT EXISTS slaughter_orders (
    order_number                VARCHAR(50) PRIMARY KEY,
    animal_id                   UUID REFERENCES farmer_inventory(id),
    profile_id                  UUID REFERENCES profiles(id),
    species                     TEXT NOT NULL,
    status                      TEXT NOT NULL DEFAULT 'planned'
                                CHECK (status IN ('planned','scheduled','processing','complete')),
    scheduled_date              DATE,
    processing_cost             NUMERIC(10,2),
    estimated_hanging_weight    NUMERIC(10,2),
    actual_hanging_weight       NUMERIC(10,2),
    farmer_transport_cost       NUMERIC(10,2),
    total_customer_transport_cost NUMERIC(10,2),
    notes                       TEXT,
    created_at                  TIMESTAMPTZ DEFAULT now(),
    updated_at                  TIMESTAMPTZ DEFAULT now(),
    completed_at                TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_slaughter_orders_animal ON slaughter_orders(animal_id);
CREATE INDEX IF NOT EXISTS idx_slaughter_orders_processor ON slaughter_orders(profile_id);
CREATE INDEX IF NOT EXISTS idx_slaughter_orders_status ON slaughter_orders(status);
