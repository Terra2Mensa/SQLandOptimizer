-- 010_pork_cut_sheets.sql
-- Pork cut sheet tables, validation trigger, and seed defaults (whole hog).
-- Run: psql -d terra_mensa -f sql/010_pork_cut_sheets.sql

-- ─── Tables ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pork_cut_sheets (
    id              SERIAL PRIMARY KEY,
    po_number       VARCHAR(50) REFERENCES purchase_orders(po_number),
    half_label      TEXT NOT NULL CHECK (half_label IN ('A','B')),
    selections      JSONB NOT NULL DEFAULT '{}',
    notes           TEXT,
    share_size      TEXT NOT NULL DEFAULT '1/1'
                    CHECK (share_size IN ('1/1','1/2','1/4')),
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (po_number, half_label)
);

CREATE TABLE IF NOT EXISTS pork_cut_sheet_defaults (
    id              SERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    selections      JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pork_cut_sheets_po ON pork_cut_sheets(po_number);

-- ─── Validation trigger function ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION validate_pork_cut_sheet()
RETURNS TRIGGER AS $$
DECLARE
    sel JSONB := NEW.selections;
    v   JSONB;
    opt TEXT;
    sz  TEXT := COALESCE(NEW.share_size, '1/1');
BEGIN
    -- ── 1. Shoulder ──
    v := sel->'shoulder';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'shoulder: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('roasts','steaks','country_style_ribs','stew_meat','ground') THEN
        RAISE EXCEPTION 'shoulder: invalid option "%"', opt;
    END IF;
    IF opt = 'steaks' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'shoulder: thickness required when steaks';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5') THEN
        RAISE EXCEPTION 'shoulder: invalid thickness "%"', v->>'thickness';
    END IF;

    -- ── 2. Pork chops ──
    v := sel->'pork_chops';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'pork_chops: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('bone_in','boneless','loin_roast','butterfly','ground') THEN
        RAISE EXCEPTION 'pork_chops: invalid option "%"', opt;
    END IF;
    IF opt IN ('bone_in','boneless','butterfly') AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'pork_chops: thickness required when %', opt;
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5') THEN
        RAISE EXCEPTION 'pork_chops: invalid thickness "%"', v->>'thickness';
    END IF;

    -- ── 3. Tenderloin ──
    v := sel->'tenderloin';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'tenderloin: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('whole_roast','medallions','ground') THEN
        RAISE EXCEPTION 'tenderloin: invalid option "%"', opt;
    END IF;
    IF opt = 'medallions' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'tenderloin: thickness required when medallions';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25') THEN
        RAISE EXCEPTION 'tenderloin: invalid thickness "%"', v->>'thickness';
    END IF;

    -- ── 4. Loin ribs (no grind — attached to loin) ──
    v := sel->'loin_ribs';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'loin_ribs: option required';
    END IF;
    IF v->>'option' NOT IN ('baby_back','leave_on_chops') THEN
        RAISE EXCEPTION 'loin_ribs: invalid option "%"', v->>'option';
    END IF;

    -- ── 5. Belly ribs (no grind — attached to belly) ──
    v := sel->'belly_ribs';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'belly_ribs: option required';
    END IF;
    IF v->>'option' NOT IN ('st_louis','spare','leave_on_belly') THEN
        RAISE EXCEPTION 'belly_ribs: invalid option "%"', v->>'option';
    END IF;

    -- ── 6. Bacon / belly ──
    v := sel->'bacon';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'bacon: option required';
    END IF;
    IF v->>'option' NOT IN ('smoked','fresh_belly','ground') THEN
        RAISE EXCEPTION 'bacon: invalid option "%"', v->>'option';
    END IF;

    -- ── 7. Ham ──
    v := sel->'ham';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'ham: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('slices_smoked','fresh','fresh_steaks','shank_butt_split','fresh_boneless','ground') THEN
        RAISE EXCEPTION 'ham: invalid option "%"', opt;
    END IF;
    IF opt = 'fresh_steaks' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'ham: thickness required when fresh_steaks';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25') THEN
        RAISE EXCEPTION 'ham: invalid thickness "%"', v->>'thickness';
    END IF;

    -- ── 8. Hocks (removed for 1/4) ──
    IF sz != '1/4' THEN
        v := sel->'hocks';
        IF v IS NULL OR v->>'option' IS NULL THEN
            RAISE EXCEPTION 'hocks: option required';
        END IF;
        IF v->>'option' NOT IN ('fresh','split','ground') THEN
            RAISE EXCEPTION 'hocks: invalid option "%"', v->>'option';
        END IF;
    END IF;

    -- ── 9. Neck bones (removed for 1/4) ──
    IF sz != '1/4' THEN
        v := sel->'neck_bones';
        IF v IS NULL OR v->>'option' IS NULL THEN
            RAISE EXCEPTION 'neck_bones: option required';
        END IF;
        IF v->>'option' NOT IN ('keep','ground') THEN
            RAISE EXCEPTION 'neck_bones: invalid option "%"', v->>'option';
        END IF;
    END IF;

    -- ── 10. Sausage ──
    v := sel->'sausage';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'sausage: option required';
    END IF;
    IF v->>'option' NOT IN ('bulk','links') THEN
        RAISE EXCEPTION 'sausage: invalid option "%"', v->>'option';
    END IF;

    -- ── 11. Ground pork ──
    v := sel->'ground_pork';
    IF v IS NOT NULL THEN
        IF v ? 'yield' AND jsonb_typeof(v->'yield') != 'object' THEN
            RAISE EXCEPTION 'ground_pork: yield must be an object';
        END IF;
    END IF;

    -- Update timestamp
    NEW.updated_at := now();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ─── Trigger ─────────────────────────────────────────────────────────────

DROP TRIGGER IF EXISTS trg_pork_cut_sheet_validate ON pork_cut_sheets;
CREATE TRIGGER trg_pork_cut_sheet_validate
    BEFORE INSERT OR UPDATE ON pork_cut_sheets
    FOR EACH ROW EXECUTE FUNCTION validate_pork_cut_sheet();

-- ─── Seed defaults (whole hog) ───────────────────────────────────────────

INSERT INTO pork_cut_sheet_defaults (name, selections) VALUES
('standard', '{
    "shoulder":         {"option": "roasts",
                         "yield": {"qty": "4", "unit": "roasts"}},
    "pork_chops":       {"option": "bone_in", "thickness": "1",
                         "yield": {"qty": "14", "unit": "packs", "note": "2 per pack"}},
    "tenderloin":       {"option": "whole_roast",
                         "yield": {"qty": "2", "unit": "tenderloins"}},
    "loin_ribs":        {"option": "leave_on_chops",
                         "yield": {"qty": null, "unit": "none", "note": "left on chops for thicker cut"}},
    "belly_ribs":       {"option": "st_louis",
                         "yield": {"qty": "2", "unit": "slabs"}},
    "bacon":            {"option": "smoked",
                         "yield": {"qty": "18", "unit": "packs", "weight_each_lbs": "1"}},
    "ham":              {"option": "fresh",
                         "yield": {"qty": "4", "unit": "roasts", "weight_each_lbs": "6"}},
    "hocks":            {"option": "fresh",
                         "yield": {"qty": "4", "unit": "hocks"}},
    "neck_bones":       {"option": "keep",
                         "yield": {"qty": "2", "unit": "packs", "weight_each_lbs": "1.5"}},
    "sausage":          {"option": "bulk",
                         "yield": {"qty": "10", "unit": "packs", "weight_each_lbs": "1"}},
    "ground_pork":      {"yield": {"min_lbs": 15, "pack_size_lbs": 1}}
}'::jsonb),
('steak_lover', '{
    "shoulder":         {"option": "steaks", "thickness": "1",
                         "yield": {"qty": "8-10", "unit": "steaks"}},
    "pork_chops":       {"option": "boneless", "thickness": "1.25",
                         "yield": {"qty": "14", "unit": "packs", "note": "2 per pack"}},
    "tenderloin":       {"option": "medallions", "thickness": "1",
                         "yield": {"qty": "6-8", "unit": "pieces"}},
    "loin_ribs":        {"option": "baby_back",
                         "yield": {"qty": "2", "unit": "racks", "note": "reduces chop yield"}},
    "belly_ribs":       {"option": "st_louis",
                         "yield": {"qty": "2", "unit": "slabs"}},
    "bacon":            {"option": "smoked",
                         "yield": {"qty": "18", "unit": "packs", "weight_each_lbs": "1"}},
    "ham":              {"option": "fresh_steaks", "thickness": "1",
                         "yield": {"qty": "8-10", "unit": "steaks"}},
    "hocks":            {"option": "split",
                         "yield": {"qty": "8", "unit": "pieces"}},
    "neck_bones":       {"option": "ground",
                         "yield": {"qty": null, "unit": "ground", "note": "added to ground pork total"}},
    "sausage":          {"option": "links",
                         "yield": {"qty": "10", "unit": "packs", "weight_each_lbs": "1"}},
    "ground_pork":      {"yield": {"min_lbs": 15, "pack_size_lbs": 1}}
}'::jsonb)
ON CONFLICT (name) DO UPDATE SET selections = EXCLUDED.selections;
