-- 009_eighth_beef.sql
-- Add 1/8 share size and seed eighth defaults.
-- Removed cuts for 1/8: neck, rump, pikes_peak, bones, organs, flank, skirt
-- Yields = half/4, floored, minimum 1.
-- Run: psql -d terra_mensa -f sql/009_eighth_beef.sql

-- ─── Update share_size constraint to include 1/8 ─────────────────────────

ALTER TABLE beef_cut_sheets DROP CONSTRAINT IF EXISTS beef_cut_sheets_share_size_check;
ALTER TABLE beef_cut_sheets ADD CONSTRAINT beef_cut_sheets_share_size_check
    CHECK (share_size IN ('1/2','1/4','1/8'));

-- ─── Seed eighth defaults ─────────────────────────────────────────────────

INSERT INTO beef_cut_sheet_defaults (name, selections) VALUES
('standard_eighth', '{
    "cross_cut_shanks": {"option": "shanks", "thickness": "1",
                         "yield": {"qty": "2", "unit": "pieces", "note": "avg at 2 inch thick"}},
    "shoulder":         {"option": "roasts",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "2.5"}},
    "chuck":            {"option": "roasts",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "4-5"}},
    "brisket":          {"option": "whole",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "2"}},
    "short_ribs":       {"option": "traditional",
                         "yield": {"qty": "1", "unit": "packs", "weight_each_lbs": "2-3"}},
    "ribeye":           {"option": "boneless", "thickness": "1",
                         "yield": {"qty": "2", "unit": "pieces", "note": "avg at 1.25 inch thick"}},
    "big_steak":        {"option": "t_bones", "thickness": "1",
                         "yield": {"qty": "2", "unit": "pieces", "note": "avg at 1.25 inch thick"}},
    "sirloin":          {"option": "steaks", "thickness": "1",
                         "yield": {"qty": "1", "unit": "pieces", "note": "avg at 1 inch thick"}},
    "top_round":        {"option": "roasts",
                         "yield": {"qty": "1", "unit": "roasts", "note": "boneless"}},
    "eye_of_round":     {"option": "roast",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "1"}},
    "ground_beef":      {"ratio": "80_20", "mix_organs": false,
                         "yield": {"min_lbs": 11, "pack_size_lbs": 1}},
    "caveman_blend":    {"quantity": 0,
                         "yield": {"max_lbs": 2, "pack_size_lbs": 1, "note": "8-10% liver and heart added"}},
    "stew_meat":        {"quantity": 2,
                         "yield": {"max_lbs": 2, "pack_size_lbs": 1}},
    "chili_grind":      {"quantity": 0,
                         "yield": {"pack_size_lbs": 1, "note": "coarse grind"}}
}'::jsonb),
('steak_lover_eighth', '{
    "cross_cut_shanks": {"option": "shanks", "thickness": "1.5",
                         "yield": {"qty": "2", "unit": "pieces", "note": "avg at 2 inch thick"}},
    "shoulder":         {"option": "steaks", "thickness": "1",
                         "yield": {"qty": null, "unit": "pieces", "note": "thin cut roasts, count varies on thickness"}},
    "chuck":            {"option": "steaks", "thickness": "1",
                         "yield": {"qty": null, "unit": "pieces", "note": "thin cut roasts, count varies on thickness"}},
    "brisket":          {"option": "whole",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "2"}},
    "short_ribs":       {"option": "traditional",
                         "yield": {"qty": "1", "unit": "packs", "weight_each_lbs": "2-3"}},
    "ribeye":           {"option": "bone_in", "thickness": "1.5",
                         "yield": {"qty": "1-2", "unit": "pieces", "note": "avg at 1.25 inch thick, cap/spinalis left on"}},
    "big_steak":        {"option": "ny_strip_tenderloin", "thickness": "1.25",
                         "ny_strip_bone": "bone_in", "tenderloin_style": "medallions",
                         "yield": {"qty": "2", "unit": "pieces", "note": "avg for both categories at 1.25 inch thick"}},
    "sirloin":          {"option": "steaks", "thickness": "1.25",
                         "yield": {"qty": "1", "unit": "pieces", "note": "avg at 1 inch thick"}},
    "top_round":        {"option": "steaks", "thickness": "1",
                         "yield": {"qty": "1", "unit": "steaks", "note": "varies based on thickness"}},
    "eye_of_round":     {"option": "london_broils",
                         "yield": {"qty": "1", "unit": "pieces"}},
    "ground_beef":      {"ratio": "85_15", "mix_organs": false,
                         "yield": {"min_lbs": 11, "pack_size_lbs": 1}},
    "caveman_blend":    {"quantity": 0,
                         "yield": {"max_lbs": 2, "pack_size_lbs": 1, "note": "8-10% liver and heart added"}},
    "stew_meat":        {"quantity": 2,
                         "yield": {"max_lbs": 2, "pack_size_lbs": 1}},
    "chili_grind":      {"quantity": 0,
                         "yield": {"pack_size_lbs": 1, "note": "coarse grind"}}
}'::jsonb)
ON CONFLICT (name) DO UPDATE SET selections = EXCLUDED.selections;
