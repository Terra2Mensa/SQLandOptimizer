-- 025_slaughter_order_payments.sql
-- Payment tracking on slaughter orders + payment transaction tables.

-- ─── 1. Add payment fields to slaughter_orders ─────────────────────────

-- Farmer payment (source of truth for what's owed)
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS live_weight_est NUMERIC(10,2);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS market_price_cwt NUMERIC(10,2);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS farmer_rate_per_lb NUMERIC(10,4);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS farmer_payment_est NUMERIC(10,2);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS farmer_payment_final NUMERIC(10,2);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS farmer_payment_status TEXT DEFAULT 'unpaid';

-- Processor payment
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS processor_payment_status TEXT DEFAULT 'unpaid';

-- Customer payment (aggregate across all POs on this order)
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS customer_total_est NUMERIC(10,2);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS customer_total_final NUMERIC(10,2);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS customer_deposit NUMERIC(10,2);
ALTER TABLE slaughter_orders ADD COLUMN IF NOT EXISTS customer_payment_status TEXT DEFAULT 'unpaid';


-- ─── 2. Farmer payment transactions ────────────────────────────────────

CREATE TABLE IF NOT EXISTS farmer_payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number    VARCHAR(50) NOT NULL,
    farmer_id       UUID NOT NULL,
    amount          NUMERIC(10,2) NOT NULL,
    milestone       TEXT NOT NULL CHECK (milestone IN ('milestone_1','milestone_2','adjustment')),
    payment_date    DATE,
    payment_method  TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_farmer_pay_order ON farmer_payments(order_number);


-- ─── 3. Processor payment transactions ─────────────────────────────────

CREATE TABLE IF NOT EXISTS processor_payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number    VARCHAR(50) NOT NULL,
    processor_id    UUID NOT NULL,
    amount          NUMERIC(10,2) NOT NULL,
    invoice_number  TEXT,
    payment_date    DATE,
    payment_method  TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_proc_pay_order ON processor_payments(order_number);


-- ─── 4. Customer payment transactions ──────────────────────────────────

CREATE TABLE IF NOT EXISTS customer_payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    po_number       VARCHAR(50) NOT NULL,
    customer_id     UUID NOT NULL,
    amount          NUMERIC(10,2) NOT NULL,
    payment_type    TEXT NOT NULL CHECK (payment_type IN ('deposit','final','adjustment','refund')),
    payment_date    DATE,
    payment_method  TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cust_pay_po ON customer_payments(po_number);


-- ─── Grants ─────────────────────────────────────────────────────────────
GRANT SELECT ON farmer_payments TO authenticated;
GRANT SELECT ON processor_payments TO authenticated;
GRANT SELECT ON customer_payments TO authenticated;
