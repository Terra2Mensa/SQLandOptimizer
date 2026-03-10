-- Add lat/lng to dtc_customers for optimizer distance calculations
ALTER TABLE dtc_customers ADD COLUMN IF NOT EXISTS latitude NUMERIC(9,6);
ALTER TABLE dtc_customers ADD COLUMN IF NOT EXISTS longitude NUMERIC(9,6);
