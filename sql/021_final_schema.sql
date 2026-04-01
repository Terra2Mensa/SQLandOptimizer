-- 021_final_schema.sql
-- Final schema additions: PO capabilities, ownership tracking, processor scheduling.

-- ─── 1. Purchase order capabilities (derived from cut sheet) ────────────
ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS requires_capabilities TEXT[] DEFAULT '{}';
ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS ownership_confirmed BOOLEAN DEFAULT false;
ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS ownership_confirmed_at TIMESTAMPTZ;

-- ─── 2. Processor scheduling (kill slot booking) ───────────────────────
-- Different from blackouts: this tracks available vs booked slots per day.
-- The optimizer books slots when creating slaughter orders.

CREATE TABLE IF NOT EXISTS processor_schedule (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processor_id    UUID NOT NULL,
    schedule_date   DATE NOT NULL,
    species         TEXT NOT NULL,
    available_slots INT NOT NULL DEFAULT 0,
    booked_slots    INT NOT NULL DEFAULT 0,
    UNIQUE (processor_id, schedule_date, species)
);

CREATE INDEX IF NOT EXISTS idx_proc_schedule_lookup
    ON processor_schedule(processor_id, schedule_date, species);

-- ─── 3. Seed processor schedules (next 90 days) ───────────────────────
-- Generate available slots based on daily_capacity_head from processor_costs.
-- This is a one-time seed; the optimizer updates booked_slots.

INSERT INTO processor_schedule (processor_id, schedule_date, species, available_slots, booked_slots)
SELECT
    pc.profile_id,
    d.schedule_date,
    pc.species,
    pc.daily_capacity_head,
    0
FROM processor_costs pc
CROSS JOIN generate_series(CURRENT_DATE, CURRENT_DATE + 90, '1 day'::interval) AS d(schedule_date)
WHERE pc.daily_capacity_head > 0
  -- Skip weekends (most custom processors don't process on weekends)
  AND EXTRACT(DOW FROM d.schedule_date) NOT IN (0, 6)
ON CONFLICT (processor_id, schedule_date, species) DO NOTHING;

-- ─── Grants ─────────────────────────────────────────────────────────────
GRANT SELECT ON processor_schedule TO anon, authenticated;
