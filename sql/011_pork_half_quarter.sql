-- 011_pork_half_quarter.sql
-- Pork half and quarter defaults. Yields = whole/2 (half), whole/4 (quarter), floored, min 1.
-- Quarter removes hocks and neck_bones.
-- Run: psql -d terra_mensa -f sql/011_pork_half_quarter.sql

-- ─── Half defaults ────────────────────────────────────────────────────────

INSERT INTO pork_cut_sheet_defaults (name, selections) VALUES
('standard_half', '{
    "shoulder":         {"option": "roasts",
                         "yield": {"qty": "2", "unit": "roasts"}},
    "pork_chops":       {"option": "bone_in", "thickness": "1",
                         "yield": {"qty": "7", "unit": "packs", "note": "2 per pack"}},
    "tenderloin":       {"option": "whole_roast",
                         "yield": {"qty": "1", "unit": "tenderloins"}},
    "loin_ribs":        {"option": "leave_on_chops",
                         "yield": {"qty": null, "unit": "none", "note": "left on chops for thicker cut"}},
    "belly_ribs":       {"option": "st_louis",
                         "yield": {"qty": "1", "unit": "slabs"}},
    "bacon":            {"option": "smoked",
                         "yield": {"qty": "9", "unit": "packs", "weight_each_lbs": "1"}},
    "ham":              {"option": "fresh",
                         "yield": {"qty": "2", "unit": "roasts", "weight_each_lbs": "6"}},
    "hocks":            {"option": "fresh",
                         "yield": {"qty": "2", "unit": "hocks"}},
    "neck_bones":       {"option": "keep",
                         "yield": {"qty": "1", "unit": "packs", "weight_each_lbs": "1.5"}},
    "sausage":          {"option": "bulk",
                         "yield": {"qty": "5", "unit": "packs", "weight_each_lbs": "1"}},
    "ground_pork":      {"yield": {"min_lbs": 7, "pack_size_lbs": 1}}
}'::jsonb),
('steak_lover_half', '{
    "shoulder":         {"option": "steaks", "thickness": "1",
                         "yield": {"qty": "4-5", "unit": "steaks"}},
    "pork_chops":       {"option": "boneless", "thickness": "1.25",
                         "yield": {"qty": "7", "unit": "packs", "note": "2 per pack"}},
    "tenderloin":       {"option": "medallions", "thickness": "1",
                         "yield": {"qty": "3-4", "unit": "pieces"}},
    "loin_ribs":        {"option": "baby_back",
                         "yield": {"qty": "1", "unit": "racks", "note": "reduces chop yield"}},
    "belly_ribs":       {"option": "st_louis",
                         "yield": {"qty": "1", "unit": "slabs"}},
    "bacon":            {"option": "smoked",
                         "yield": {"qty": "9", "unit": "packs", "weight_each_lbs": "1"}},
    "ham":              {"option": "fresh_steaks", "thickness": "1",
                         "yield": {"qty": "4-5", "unit": "steaks"}},
    "hocks":            {"option": "split",
                         "yield": {"qty": "4", "unit": "pieces"}},
    "neck_bones":       {"option": "ground",
                         "yield": {"qty": null, "unit": "ground", "note": "added to ground pork total"}},
    "sausage":          {"option": "links",
                         "yield": {"qty": "5", "unit": "packs", "weight_each_lbs": "1"}},
    "ground_pork":      {"yield": {"min_lbs": 7, "pack_size_lbs": 1}}
}'::jsonb)
ON CONFLICT (name) DO UPDATE SET selections = EXCLUDED.selections;

-- ─── Quarter defaults (hocks + neck_bones removed) ───────────────────────

INSERT INTO pork_cut_sheet_defaults (name, selections) VALUES
('standard_quarter', '{
    "shoulder":         {"option": "roasts",
                         "yield": {"qty": "1", "unit": "roasts"}},
    "pork_chops":       {"option": "bone_in", "thickness": "1",
                         "yield": {"qty": "3", "unit": "packs", "note": "2 per pack"}},
    "tenderloin":       {"option": "whole_roast",
                         "yield": {"qty": "1", "unit": "tenderloins", "note": "may be smaller"}},
    "loin_ribs":        {"option": "leave_on_chops",
                         "yield": {"qty": null, "unit": "none", "note": "left on chops for thicker cut"}},
    "belly_ribs":       {"option": "st_louis",
                         "yield": {"qty": "1", "unit": "slabs", "note": "may be partial"}},
    "bacon":            {"option": "smoked",
                         "yield": {"qty": "4", "unit": "packs", "weight_each_lbs": "1"}},
    "ham":              {"option": "fresh",
                         "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "6"}},
    "sausage":          {"option": "bulk",
                         "yield": {"qty": "2", "unit": "packs", "weight_each_lbs": "1"}},
    "ground_pork":      {"yield": {"min_lbs": 3, "pack_size_lbs": 1}}
}'::jsonb),
('steak_lover_quarter', '{
    "shoulder":         {"option": "steaks", "thickness": "1",
                         "yield": {"qty": "2", "unit": "steaks"}},
    "pork_chops":       {"option": "boneless", "thickness": "1.25",
                         "yield": {"qty": "3", "unit": "packs", "note": "2 per pack"}},
    "tenderloin":       {"option": "medallions", "thickness": "1",
                         "yield": {"qty": "1-2", "unit": "pieces"}},
    "loin_ribs":        {"option": "baby_back",
                         "yield": {"qty": "1", "unit": "racks", "note": "reduces chop yield"}},
    "belly_ribs":       {"option": "st_louis",
                         "yield": {"qty": "1", "unit": "slabs", "note": "may be partial"}},
    "bacon":            {"option": "smoked",
                         "yield": {"qty": "4", "unit": "packs", "weight_each_lbs": "1"}},
    "ham":              {"option": "fresh_steaks", "thickness": "1",
                         "yield": {"qty": "2", "unit": "steaks"}},
    "sausage":          {"option": "links",
                         "yield": {"qty": "2", "unit": "packs", "weight_each_lbs": "1"}},
    "ground_pork":      {"yield": {"min_lbs": 3, "pack_size_lbs": 1}}
}'::jsonb)
ON CONFLICT (name) DO UPDATE SET selections = EXCLUDED.selections;
