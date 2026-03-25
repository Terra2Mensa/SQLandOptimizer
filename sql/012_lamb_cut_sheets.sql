-- 012_lamb_cut_sheets.sql
-- Lamb cut sheet tables, validation trigger, and seed defaults (whole + half).
-- Run: psql -d terra_mensa -f sql/012_lamb_cut_sheets.sql

-- ─── Tables ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS lamb_cut_sheets (
    id              SERIAL PRIMARY KEY,
    po_number       VARCHAR(50) REFERENCES purchase_orders(po_number),
    half_label      TEXT NOT NULL CHECK (half_label IN ('A','B')),
    selections      JSONB NOT NULL DEFAULT '{}',
    notes           TEXT,
    share_size      TEXT NOT NULL DEFAULT '1/1'
                    CHECK (share_size IN ('1/1','1/2')),
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (po_number, half_label)
);

CREATE TABLE IF NOT EXISTS lamb_cut_sheet_defaults (
    id              SERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    selections      JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lamb_cut_sheets_po ON lamb_cut_sheets(po_number);

-- ─── Validation trigger function ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION validate_lamb_cut_sheet()
RETURNS TRIGGER AS $$
DECLARE
    sel JSONB := NEW.selections;
    v   JSONB;
    opt TEXT;
BEGIN
    -- ── 1. Rack ──
    v := sel->'rack';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'rack: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('whole_rack','rib_chops','ground') THEN
        RAISE EXCEPTION 'rack: invalid option "%"', opt;
    END IF;
    IF opt = 'rib_chops' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'rack: thickness required when rib_chops';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' != '1' THEN
        RAISE EXCEPTION 'rack: thickness must be "1"';
    END IF;

    -- ── 2. Loin chops ──
    v := sel->'loin_chops';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'loin_chops: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('chops','loin_roast','ground') THEN
        RAISE EXCEPTION 'loin_chops: invalid option "%"', opt;
    END IF;
    IF opt = 'chops' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'loin_chops: thickness required when chops';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' != '1' THEN
        RAISE EXCEPTION 'loin_chops: thickness must be "1"';
    END IF;

    -- ── 3. Leg shanks ──
    v := sel->'leg_shanks';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'leg_shanks: option required';
    END IF;
    IF v->>'option' NOT IN ('shanks','ground') THEN
        RAISE EXCEPTION 'leg_shanks: invalid option "%"', v->>'option';
    END IF;

    -- ── 4. Leg ──
    v := sel->'leg';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'leg: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('roasts','steaks','boneless','ground') THEN
        RAISE EXCEPTION 'leg: invalid option "%"', opt;
    END IF;
    IF opt = 'steaks' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'leg: thickness required when steaks';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' != '1' THEN
        RAISE EXCEPTION 'leg: thickness must be "1"';
    END IF;

    -- ── 5. Shoulder ──
    v := sel->'shoulder';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'shoulder: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('roasts','steaks','ground') THEN
        RAISE EXCEPTION 'shoulder: invalid option "%"', opt;
    END IF;
    IF opt = 'steaks' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'shoulder: thickness required when steaks';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' != '1' THEN
        RAISE EXCEPTION 'shoulder: thickness must be "1"';
    END IF;

    -- ── 6. Stew meat (fixed — always included, validate yield only) ──
    v := sel->'stew_meat';
    IF v IS NOT NULL AND v ? 'yield' AND jsonb_typeof(v->'yield') != 'object' THEN
        RAISE EXCEPTION 'stew_meat: yield must be an object';
    END IF;

    -- ── 7. Ground (catch-all) ──
    v := sel->'ground';
    IF v IS NOT NULL AND v ? 'yield' AND jsonb_typeof(v->'yield') != 'object' THEN
        RAISE EXCEPTION 'ground: yield must be an object';
    END IF;

    -- Update timestamp
    NEW.updated_at := now();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ─── Trigger ─────────────────────────────────────────────────────────────

DROP TRIGGER IF EXISTS trg_lamb_cut_sheet_validate ON lamb_cut_sheets;
CREATE TRIGGER trg_lamb_cut_sheet_validate
    BEFORE INSERT OR UPDATE ON lamb_cut_sheets
    FOR EACH ROW EXECUTE FUNCTION validate_lamb_cut_sheet();

-- ─── Seed defaults ───────────────────────────────────────────────────────

INSERT INTO lamb_cut_sheet_defaults (name, selections) VALUES
('standard', '{
    "rack":             {"option": "whole_rack",
                         "yield": {"qty": "2", "unit": "racks"}},
    "loin_chops":       {"option": "chops", "thickness": "1",
                         "yield": {"qty": "4", "unit": "packs", "note": "2 per pack"}},
    "leg_shanks":       {"option": "shanks",
                         "yield": {"qty": "4", "unit": "shanks"}},
    "leg":              {"option": "roasts",
                         "yield": {"qty": "2", "unit": "roasts"}},
    "shoulder":         {"option": "roasts",
                         "yield": {"qty": "2", "unit": "roasts"}},
    "stew_meat":        {"yield": {"qty": "2-4", "unit": "packs", "weight_each_lbs": "1", "note": "bone-in, always included"}},
    "ground":           {"yield": {"min_lbs": 2, "pack_size_lbs": 1}}
}'::jsonb),
('standard_half', '{
    "rack":             {"option": "whole_rack",
                         "yield": {"qty": "1", "unit": "racks"}},
    "loin_chops":       {"option": "chops", "thickness": "1",
                         "yield": {"qty": "2", "unit": "packs", "note": "2 per pack"}},
    "leg_shanks":       {"option": "shanks",
                         "yield": {"qty": "2", "unit": "shanks"}},
    "leg":              {"option": "roasts",
                         "yield": {"qty": "1", "unit": "roasts"}},
    "shoulder":         {"option": "roasts",
                         "yield": {"qty": "1", "unit": "roasts"}},
    "stew_meat":        {"yield": {"qty": "1-2", "unit": "packs", "weight_each_lbs": "1", "note": "bone-in, always included"}},
    "ground":           {"yield": {"min_lbs": 1, "pack_size_lbs": 1}}
}'::jsonb)
ON CONFLICT (name) DO UPDATE SET selections = EXCLUDED.selections;
