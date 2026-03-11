-- Phase 2: Actual-weight fulfillment and invoicing
-- Adds actual cut weights post-processing, reconciliation support, and D2C invoice FK.

-- 1. New table: actual processor output per slaughter order, per cut
CREATE TABLE IF NOT EXISTS actual_cuts (
    id SERIAL PRIMARY KEY,
    slaughter_order_id INTEGER NOT NULL REFERENCES slaughter_orders(id),
    cut_code VARCHAR(30) NOT NULL,
    actual_lbs NUMERIC(8,2) NOT NULL,
    recorded_by VARCHAR(50),
    recorded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    notes TEXT,
    UNIQUE (slaughter_order_id, cut_code)
);
CREATE INDEX IF NOT EXISTS idx_actual_cuts_so ON actual_cuts(slaughter_order_id);

-- 2. Slaughter orders: actual hanging weight + completion timestamp
ALTER TABLE slaughter_orders
    ADD COLUMN IF NOT EXISTS actual_hanging_weight NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;

-- 3. Slaughter order lines: actual weights alongside estimates
ALTER TABLE slaughter_order_lines
    ADD COLUMN IF NOT EXISTS actual_lbs NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS actual_allocated_to_po NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS actual_allocated_to_lor NUMERIC(8,2);

-- 4. PO lines: actual fulfilled weight
ALTER TABLE po_lines
    ADD COLUMN IF NOT EXISTS actual_lbs NUMERIC(10,1);

-- 5. Invoices: add PO FK for D2C (nullable, alongside legacy order_id)
ALTER TABLE invoices
    ADD COLUMN IF NOT EXISTS po_number VARCHAR(50) REFERENCES purchase_orders(po_number);
CREATE INDEX IF NOT EXISTS idx_invoices_po ON invoices(po_number);
