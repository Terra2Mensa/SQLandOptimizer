-- 007_beef_cut_sheets.sql
-- Beef cut sheet tables, validation trigger, and seed defaults.
-- Run: psql -d terra_mensa -f sql/007_beef_cut_sheets.sql

-- ─── Tables ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS beef_cut_sheets (
    id              SERIAL PRIMARY KEY,
    po_number       VARCHAR(50) REFERENCES purchase_orders(po_number),
    half_label      TEXT NOT NULL CHECK (half_label IN ('A','B')),
    selections      JSONB NOT NULL DEFAULT '{}',
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (po_number, half_label)
);

CREATE TABLE IF NOT EXISTS beef_cut_sheet_defaults (
    id              SERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    selections      JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_beef_cut_sheets_po ON beef_cut_sheets(po_number);

-- ─── Validation trigger function ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION validate_beef_cut_sheet()
RETURNS TRIGGER AS $$
DECLARE
    sel JSONB := NEW.selections;
    v   JSONB;
    opt TEXT;
    k   TEXT;
    sz  TEXT := COALESCE(NEW.share_size, '1/2');
BEGIN
    -- ── 1. Cut lines: option is required and must be valid ──
    -- Cuts removed for 1/8 shares: neck, rump, pikes_peak, bones, organs, flank, skirt

    -- neck (removed for 1/8)
    IF sz != '1/8' THEN
        v := sel->'neck';
        IF v IS NULL OR v->>'option' IS NULL THEN
            RAISE EXCEPTION 'neck: option required';
        END IF;
        IF v->>'option' NOT IN ('soup_bones','bone_roast','grind') THEN
            RAISE EXCEPTION 'neck: invalid option "%"', v->>'option';
        END IF;
    END IF;

    -- cross_cut_shanks
    v := sel->'cross_cut_shanks';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'cross_cut_shanks: option required';
    END IF;
    IF v->>'option' NOT IN ('shanks','grind') THEN
        RAISE EXCEPTION 'cross_cut_shanks: invalid option "%"', v->>'option';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5','2') THEN
        RAISE EXCEPTION 'cross_cut_shanks: invalid thickness "%"', v->>'thickness';
    END IF;

    -- shoulder
    v := sel->'shoulder';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'shoulder: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('roasts','steaks','grind') THEN
        RAISE EXCEPTION 'shoulder: invalid option "%"', opt;
    END IF;
    IF opt = 'steaks' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'shoulder: thickness required when steaks';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5') THEN
        RAISE EXCEPTION 'shoulder: invalid thickness "%"', v->>'thickness';
    END IF;

    -- chuck
    v := sel->'chuck';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'chuck: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('roasts','mini_roasts','steaks','grind') THEN
        RAISE EXCEPTION 'chuck: invalid option "%"', opt;
    END IF;
    IF opt = 'steaks' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'chuck: thickness required when steaks';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5') THEN
        RAISE EXCEPTION 'chuck: invalid thickness "%"', v->>'thickness';
    END IF;

    -- brisket
    v := sel->'brisket';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'brisket: option required';
    END IF;
    IF v->>'option' NOT IN ('whole','half','grind') THEN
        RAISE EXCEPTION 'brisket: invalid option "%"', v->>'option';
    END IF;

    -- short_ribs (NO grind)
    v := sel->'short_ribs';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'short_ribs: option required';
    END IF;
    IF v->>'option' NOT IN ('traditional','flanken','rack') THEN
        RAISE EXCEPTION 'short_ribs: invalid option "%" (grind not allowed)', v->>'option';
    END IF;

    -- ribeye
    v := sel->'ribeye';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'ribeye: option required';
    END IF;
    IF v->>'option' NOT IN ('boneless','bone_in','grind') THEN
        RAISE EXCEPTION 'ribeye: invalid option "%"', v->>'option';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5','2') THEN
        RAISE EXCEPTION 'ribeye: invalid thickness "%"', v->>'thickness';
    END IF;

    -- big_steak
    v := sel->'big_steak';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'big_steak: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('t_bones','ny_strip_tenderloin','grind') THEN
        RAISE EXCEPTION 'big_steak: invalid option "%"', opt;
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5','2') THEN
        RAISE EXCEPTION 'big_steak: invalid thickness "%"', v->>'thickness';
    END IF;
    IF opt = 'ny_strip_tenderloin' THEN
        IF v->>'ny_strip_bone' IS NULL OR v->>'ny_strip_bone' NOT IN ('boneless','bone_in') THEN
            RAISE EXCEPTION 'big_steak: ny_strip_bone required (boneless|bone_in) when ny_strip_tenderloin';
        END IF;
        IF v->>'tenderloin_style' IS NULL OR v->>'tenderloin_style' NOT IN ('roast','medallions') THEN
            RAISE EXCEPTION 'big_steak: tenderloin_style required (roast|medallions) when ny_strip_tenderloin';
        END IF;
    END IF;

    -- sirloin
    v := sel->'sirloin';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'sirloin: option required';
    END IF;
    IF v->>'option' NOT IN ('steaks','grind') THEN
        RAISE EXCEPTION 'sirloin: invalid option "%"', v->>'option';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25','1.5','2') THEN
        RAISE EXCEPTION 'sirloin: invalid thickness "%"', v->>'thickness';
    END IF;

    -- flank (removed for 1/8)
    IF sz != '1/8' THEN
        v := sel->'flank';
        IF v IS NULL OR v->>'option' IS NULL THEN
            RAISE EXCEPTION 'flank: option required';
        END IF;
        IF v->>'option' NOT IN ('steak','grind') THEN
            RAISE EXCEPTION 'flank: invalid option "%"', v->>'option';
        END IF;
    END IF;

    -- skirt (removed for 1/8)
    IF sz != '1/8' THEN
        v := sel->'skirt';
        IF v IS NULL OR v->>'option' IS NULL THEN
            RAISE EXCEPTION 'skirt: option required';
        END IF;
        IF v->>'option' NOT IN ('steak','grind') THEN
            RAISE EXCEPTION 'skirt: invalid option "%"', v->>'option';
        END IF;
    END IF;

    -- top_round
    v := sel->'top_round';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'top_round: option required';
    END IF;
    opt := v->>'option';
    IF opt NOT IN ('roasts','steaks','cutlets','half_half','grind') THEN
        RAISE EXCEPTION 'top_round: invalid option "%"', opt;
    END IF;
    IF opt = 'steaks' AND v->>'thickness' IS NULL THEN
        RAISE EXCEPTION 'top_round: thickness required when steaks';
    END IF;
    IF v->>'thickness' IS NOT NULL AND v->>'thickness' NOT IN ('0.5','1','1.25') THEN
        RAISE EXCEPTION 'top_round: invalid thickness "%"', v->>'thickness';
    END IF;

    -- eye_of_round
    v := sel->'eye_of_round';
    IF v IS NULL OR v->>'option' IS NULL THEN
        RAISE EXCEPTION 'eye_of_round: option required';
    END IF;
    IF v->>'option' NOT IN ('roast','london_broils','stew_meat','grind') THEN
        RAISE EXCEPTION 'eye_of_round: invalid option "%"', v->>'option';
    END IF;

    -- rump (removed for 1/8)
    IF sz != '1/8' THEN
        v := sel->'rump';
        IF v IS NULL OR v->>'option' IS NULL THEN
            RAISE EXCEPTION 'rump: option required';
        END IF;
        IF v->>'option' NOT IN ('roasts','grind') THEN
            RAISE EXCEPTION 'rump: invalid option "%"', v->>'option';
        END IF;
    END IF;

    -- pikes_peak (removed for 1/8)
    IF sz != '1/8' THEN
        v := sel->'pikes_peak';
        IF v IS NULL OR v->>'option' IS NULL THEN
            RAISE EXCEPTION 'pikes_peak: option required';
        END IF;
        IF v->>'option' NOT IN ('roasts','grind') THEN
            RAISE EXCEPTION 'pikes_peak: invalid option "%"', v->>'option';
        END IF;
    END IF;

    -- ── 2. Ground beef ──
    v := sel->'ground_beef';
    IF v IS NULL OR v->>'ratio' IS NULL THEN
        RAISE EXCEPTION 'ground_beef: ratio required';
    END IF;
    IF v->>'ratio' NOT IN ('80_20','85_15','90_plus') THEN
        RAISE EXCEPTION 'ground_beef: invalid ratio "%"', v->>'ratio';
    END IF;
    IF v ? 'mix_organs' AND jsonb_typeof(v->'mix_organs') != 'boolean' THEN
        RAISE EXCEPTION 'ground_beef: mix_organs must be boolean';
    END IF;

    -- ── 3. Quantity fields (caveman_blend, stew_meat, chili_grind) ──
    FOR k IN SELECT unnest(ARRAY['caveman_blend','stew_meat','chili_grind']) LOOP
        v := sel->k;
        IF v IS NOT NULL THEN
            IF jsonb_typeof(v->'quantity') != 'number' THEN
                RAISE EXCEPTION '%: quantity must be a number', k;
            END IF;
            IF (v->>'quantity')::int < 0 THEN
                RAISE EXCEPTION '%: quantity must be non-negative', k;
            END IF;
        END IF;
    END LOOP;

    -- ── 4. Bones (removed for 1/8) ──
    IF sz != '1/8' THEN
        v := sel->'bones';
        IF v IS NOT NULL THEN
            IF v ? 'include' AND jsonb_typeof(v->'include') != 'boolean' THEN
                RAISE EXCEPTION 'bones: include must be boolean';
            END IF;
            IF v ? 'extra_bones' AND jsonb_typeof(v->'extra_bones') != 'boolean' THEN
                RAISE EXCEPTION 'bones: extra_bones must be boolean';
            END IF;
        END IF;
    END IF;

    -- ── 5. Organs — all values must be boolean (removed for 1/8) ──
    IF sz != '1/8' THEN
        v := sel->'organs';
        IF v IS NOT NULL THEN
            FOR k IN SELECT jsonb_object_keys(v) LOOP
                IF k NOT IN ('liver','heart','kidney','oxtail','tongue','cheek','sweetbread','suet') THEN
                    RAISE EXCEPTION 'organs: unknown organ "%"', k;
                END IF;
                IF jsonb_typeof(v->k) != 'boolean' THEN
                    RAISE EXCEPTION 'organs: % must be boolean', k;
                END IF;
            END LOOP;
        END IF;
    END IF;

    -- Update timestamp
    NEW.updated_at := now();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ─── Trigger ─────────────────────────────────────────────────────────────

DROP TRIGGER IF EXISTS trg_beef_cut_sheet_validate ON beef_cut_sheets;
CREATE TRIGGER trg_beef_cut_sheet_validate
    BEFORE INSERT OR UPDATE ON beef_cut_sheets
    FOR EACH ROW EXECUTE FUNCTION validate_beef_cut_sheet();

-- ─── Seed defaults ──────────────────────────────────────────────────────

INSERT INTO beef_cut_sheet_defaults (name, selections) VALUES
('standard', '{
    "neck":             {"option": "soup_bones",
                         "yield": {"qty": "4-5", "unit": "packs"}},
    "cross_cut_shanks": {"option": "shanks", "thickness": "1",
                         "yield": {"qty": "8", "unit": "pieces", "note": "avg at 2 inch thick"}},
    "shoulder":         {"option": "roasts",
                         "yield": {"qty": "2-3", "unit": "roasts", "weight_each_lbs": "2.5"}},
    "chuck":            {"option": "roasts",
                         "yield": {"qty": "3-4", "unit": "roasts", "weight_each_lbs": "4-5"}},
    "brisket":          {"option": "whole",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "8-10"}},
    "short_ribs":       {"option": "traditional",
                         "yield": {"qty": "4-6", "unit": "packs", "weight_each_lbs": "2-3"}},
    "ribeye":           {"option": "boneless", "thickness": "1",
                         "yield": {"qty": "8-10", "unit": "pieces", "note": "avg at 1.25 inch thick"}},
    "big_steak":        {"option": "t_bones", "thickness": "1",
                         "yield": {"qty": "8-10", "unit": "pieces", "note": "avg at 1.25 inch thick"}},
    "sirloin":          {"option": "steaks", "thickness": "1",
                         "yield": {"qty": "4-5", "unit": "pieces", "note": "avg at 1 inch thick"}},
    "flank":            {"option": "steak",
                         "yield": {"qty": "1", "unit": "packs"}},
    "skirt":            {"option": "steak",
                         "yield": {"qty": "3-5", "unit": "packs"}},
    "top_round":        {"option": "roasts",
                         "yield": {"qty": "2-3", "unit": "roasts", "note": "boneless"}},
    "eye_of_round":     {"option": "roast",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "2", "note": "or 2 if cut in halves"}},
    "rump":             {"option": "roasts",
                         "yield": {"qty": "2-3", "unit": "roasts", "weight_each_lbs": "2.5"}},
    "pikes_peak":       {"option": "roasts",
                         "yield": {"qty": "1-2", "unit": "roasts", "weight_each_lbs": "2.5"}},
    "ground_beef":      {"ratio": "80_20", "mix_organs": false,
                         "yield": {"min_lbs": 45, "pack_size_lbs": 1}},
    "caveman_blend":    {"quantity": 0,
                         "yield": {"max_lbs": 10, "pack_size_lbs": 1, "note": "8-10% liver and heart added"}},
    "stew_meat":        {"quantity": 5,
                         "yield": {"max_lbs": 10, "pack_size_lbs": 1}},
    "chili_grind":      {"quantity": 0,
                         "yield": {"pack_size_lbs": 1, "note": "coarse grind"}},
    "bones":            {"include": true, "extra_bones": false,
                         "yield": {"qty": "3-5", "unit": "packs", "note": "knuckle, leg and other bones"}},
    "organs":           {"liver": false, "heart": false, "kidney": false, "oxtail": false,
                         "tongue": false, "cheek": false, "sweetbread": false, "suet": false}
}'::jsonb),
('steak_lover', '{
    "neck":             {"option": "grind",
                         "yield": {"qty": null, "unit": "ground", "note": "added to ground beef total"}},
    "cross_cut_shanks": {"option": "shanks", "thickness": "1.5",
                         "yield": {"qty": "8", "unit": "pieces", "note": "avg at 2 inch thick"}},
    "shoulder":         {"option": "steaks", "thickness": "1",
                         "yield": {"qty": null, "unit": "pieces", "note": "thin cut roasts, count varies on thickness"}},
    "chuck":            {"option": "steaks", "thickness": "1",
                         "yield": {"qty": null, "unit": "pieces", "note": "thin cut roasts, count varies on thickness"}},
    "brisket":          {"option": "whole",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "8-10"}},
    "short_ribs":       {"option": "traditional",
                         "yield": {"qty": "4-6", "unit": "packs", "weight_each_lbs": "2-3"}},
    "ribeye":           {"option": "bone_in", "thickness": "1.5",
                         "yield": {"qty": "7-9", "unit": "pieces", "note": "avg at 1.25 inch thick, cap/spinalis left on"}},
    "big_steak":        {"option": "ny_strip_tenderloin", "thickness": "1.25",
                         "ny_strip_bone": "bone_in", "tenderloin_style": "medallions",
                         "yield": {"qty": "8-10", "unit": "pieces", "note": "avg for both categories at 1.25 inch thick"}},
    "sirloin":          {"option": "steaks", "thickness": "1.25",
                         "yield": {"qty": "4-5", "unit": "pieces", "note": "avg at 1 inch thick"}},
    "flank":            {"option": "steak",
                         "yield": {"qty": "1", "unit": "packs"}},
    "skirt":            {"option": "steak",
                         "yield": {"qty": "3-5", "unit": "packs"}},
    "top_round":        {"option": "steaks", "thickness": "1",
                         "yield": {"qty": "3-5", "unit": "steaks", "note": "varies based on thickness"}},
    "eye_of_round":     {"option": "london_broils",
                         "yield": {"qty": "4-6", "unit": "pieces"}},
    "rump":             {"option": "grind",
                         "yield": {"qty": null, "unit": "ground", "note": "added to ground beef total"}},
    "pikes_peak":       {"option": "grind",
                         "yield": {"qty": null, "unit": "ground", "note": "added to ground beef total"}},
    "ground_beef":      {"ratio": "85_15", "mix_organs": false,
                         "yield": {"min_lbs": 45, "pack_size_lbs": 1}},
    "caveman_blend":    {"quantity": 0,
                         "yield": {"max_lbs": 10, "pack_size_lbs": 1, "note": "8-10% liver and heart added"}},
    "stew_meat":        {"quantity": 5,
                         "yield": {"max_lbs": 10, "pack_size_lbs": 1}},
    "chili_grind":      {"quantity": 0,
                         "yield": {"pack_size_lbs": 1, "note": "coarse grind"}},
    "bones":            {"include": true, "extra_bones": false,
                         "yield": {"qty": "3-5", "unit": "packs", "note": "knuckle, leg and other bones"}},
    "organs":           {"liver": false, "heart": false, "kidney": false, "oxtail": true,
                         "tongue": false, "cheek": false, "sweetbread": false, "suet": false}
}'::jsonb)
ON CONFLICT (name) DO UPDATE SET selections = EXCLUDED.selections;
