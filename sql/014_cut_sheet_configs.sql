-- 014_cut_sheet_configs.sql
-- UI config for cut sheet forms. One row per species.
-- The website reads this to dynamically build the cut sheet form.
-- Single source of truth — matches validation triggers in 007-013.
-- Run: psql -d terra_mensa -f sql/014_cut_sheet_configs.sql

CREATE TABLE IF NOT EXISTS cut_sheet_configs (
    species     TEXT PRIMARY KEY,
    config      JSONB NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- ─── BEEF ──────────────────────────────────────────────────────────────────

INSERT INTO cut_sheet_configs (species, config) VALUES
('cattle', '{
  "label": "Beef",
  "baseShareSize": "1/2",
  "shareSizes": ["1/2", "1/4", "1/8"],
  "wholeRequiresTwoSheets": true,
  "sections": [
    {
      "id": "front",
      "label": "Front Section",
      "color": "sky",
      "cuts": [
        {
          "id": "neck",
          "label": "Neck",
          "options": [
            {"id": "soup_bones", "label": "Soup Bones", "recommended": true,
             "yield": {"qty": "4-5", "unit": "packs"}},
            {"id": "bone_roast", "label": "Bone Roast",
             "yield": {"qty": "1", "unit": "packs", "note": "or cut in 2 halves"}}
          ],
          "grind": true,
          "thickness": null,
          "removedAt": ["1/8"]
        },
        {
          "id": "cross_cut_shanks",
          "label": "Cross Cut Shanks",
          "options": [
            {"id": "shanks", "label": "Shanks", "recommended": true,
             "yield": {"qty": "8", "unit": "pieces", "note": "avg at 2\" thick"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5", "2"],
          "thicknessRecommended": "2"
        },
        {
          "id": "shoulder",
          "label": "Shoulder",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "2-3", "unit": "roasts", "weight_each_lbs": "2.5"}},
            {"id": "steaks", "label": "Steaks", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "pieces", "note": "thin cut roasts, count varies on thickness"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5"],
          "thicknessRecommended": "1"
        },
        {
          "id": "chuck",
          "label": "Chuck",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "3-4", "unit": "roasts", "weight_each_lbs": "4-5"}},
            {"id": "mini_roasts", "label": "Mini Roasts",
             "yield": {"qty": "6-8", "unit": "roasts", "weight_each_lbs": "2"}},
            {"id": "steaks", "label": "Steaks", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "pieces", "note": "thin cut roasts, count varies"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5"],
          "thicknessRecommended": "1"
        },
        {
          "id": "brisket",
          "label": "Brisket",
          "options": [
            {"id": "whole", "label": "Whole Brisket", "recommended": true,
             "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "8-10"}},
            {"id": "half", "label": "Half Briskets",
             "yield": {"qty": "2", "unit": "roasts", "weight_each_lbs": "4-5"}}
          ],
          "grind": true,
          "thickness": null
        },
        {
          "id": "short_ribs",
          "label": "Short Ribs",
          "options": [
            {"id": "traditional", "label": "Traditional", "recommended": true,
             "yield": {"qty": "4-6", "unit": "packs", "weight_each_lbs": "2-3"}},
            {"id": "flanken", "label": "Flanken",
             "yield": {"qty": "4-6", "unit": "packs", "weight_each_lbs": "2-3"}},
            {"id": "rack", "label": "Rack of Ribs",
             "yield": {"qty": "1", "unit": "racks"}}
          ],
          "grind": false,
          "thickness": null
        }
      ]
    },
    {
      "id": "center",
      "label": "Center Section",
      "color": "white",
      "cuts": [
        {
          "id": "ribeye",
          "label": "Ribeyes",
          "options": [
            {"id": "boneless", "label": "Boneless", "recommended": true,
             "yield": {"qty": "8-10", "unit": "pieces", "note": "avg at 1\u00bc\" thick"}},
            {"id": "bone_in", "label": "Bone-In",
             "yield": {"qty": "7-9", "unit": "pieces", "note": "avg at 1\u00bc\" thick, cap/spinalis left on"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5", "2"],
          "thicknessRecommended": "1.25"
        },
        {
          "id": "big_steak",
          "label": "The Big Steak Choice",
          "isBigSteak": true,
          "options": [
            {"id": "t_bones", "label": "T-Bones", "recommended": true,
             "yield": {"qty": "8-10", "unit": "pieces", "note": "avg at 1\u00bc\" thick"}},
            {"id": "ny_strip_tenderloin", "label": "NY Strip & Tenderloin",
             "yield": {"qty": "8-10", "unit": "pieces", "note": "avg for both at 1\u00bc\" thick"},
             "subOptions": {
               "ny_strip_bone": {
                 "label": "NY Strip",
                 "options": [
                   {"id": "boneless", "label": "Boneless"},
                   {"id": "bone_in", "label": "Bone-In"}
                 ]
               },
               "tenderloin_style": {
                 "label": "Tenderloin",
                 "options": [
                   {"id": "roast", "label": "Roast"},
                   {"id": "medallions", "label": "Medallions"}
                 ]
               }
             }}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5", "2"],
          "thicknessRecommended": "1.25"
        },
        {
          "id": "sirloin",
          "label": "Sirloin Steaks",
          "options": [
            {"id": "steaks", "label": "Steaks", "recommended": true,
             "yield": {"qty": "4-5", "unit": "pieces", "note": "avg at 1\" thick"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5", "2"],
          "thicknessRecommended": "1"
        },
        {
          "id": "flank",
          "label": "Flank Steak",
          "options": [
            {"id": "steak", "label": "Steak", "recommended": true,
             "yield": {"qty": "1", "unit": "packs"}}
          ],
          "grind": true,
          "thickness": null,
          "removedAt": ["1/8"]
        },
        {
          "id": "skirt",
          "label": "Skirt Steak",
          "options": [
            {"id": "steak", "label": "Steak", "recommended": true,
             "yield": {"qty": "3-5", "unit": "packs"}}
          ],
          "grind": true,
          "thickness": null,
          "removedAt": ["1/8"]
        }
      ]
    },
    {
      "id": "back",
      "label": "Back Section",
      "color": "rose",
      "cuts": [
        {
          "id": "top_round",
          "label": "Top Round",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "2-3", "unit": "roasts", "note": "boneless"}},
            {"id": "steaks", "label": "Round Steaks", "requiresThickness": true,
             "yield": {"qty": "3-5", "unit": "steaks", "note": "varies based on thickness"}},
            {"id": "cutlets", "label": "Round Steak Cutlets",
             "yield": {"qty": "6-10", "unit": "packs", "note": "2 per pack"}},
            {"id": "half_half", "label": "Half Roasts / Half Cutlets",
             "yield": {"qty": "1", "unit": "roasts", "note": "remaining as cutlet packs"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25"],
          "thicknessRecommended": "1"
        },
        {
          "id": "eye_of_round",
          "label": "Eye of Round",
          "options": [
            {"id": "roast", "label": "Roast", "recommended": true,
             "yield": {"qty": "1", "unit": "roasts", "weight_each_lbs": "2", "note": "or 2 if cut in halves"}},
            {"id": "london_broils", "label": "London Broils",
             "yield": {"qty": "4-6", "unit": "pieces"}},
            {"id": "stew_meat", "label": "Stew Meat",
             "yield": {"qty": null, "unit": "ground", "note": "goes to stew"}}
          ],
          "grind": true,
          "thickness": null
        },
        {
          "id": "rump",
          "label": "Rump Roasts",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "2-3", "unit": "roasts", "weight_each_lbs": "2.5"}}
          ],
          "grind": true,
          "thickness": null,
          "removedAt": ["1/8"]
        },
        {
          "id": "pikes_peak",
          "label": "Pikes Peak Roast",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "1-2", "unit": "roasts", "weight_each_lbs": "2.5"}}
          ],
          "grind": true,
          "thickness": null,
          "removedAt": ["1/8"]
        }
      ]
    }
  ],
  "ground": {
    "id": "ground_beef",
    "label": "Ground Beef",
    "ratios": [
      {"id": "80_20", "label": "80/20"},
      {"id": "85_15", "label": "85/15"},
      {"id": "90_plus", "label": "90+"}
    ],
    "mixOrgans": true,
    "yield": {"min_lbs": 45, "pack_size_lbs": 1},
    "note": "You will get at least 45 lbs of ground beef per half. Other options (caveman, stew, chili) come out of that amount."
  },
  "specialties": [
    {"id": "caveman_blend", "label": "Caveman Blend", "maxQty": 10,
     "yield": {"max_lbs": 10, "pack_size_lbs": 1, "note": "8-10% liver and heart added"}},
    {"id": "stew_meat", "label": "Stew Meat", "maxQty": 10,
     "yield": {"max_lbs": 10, "pack_size_lbs": 1}},
    {"id": "chili_grind", "label": "Chili Grind", "maxQty": 10,
     "yield": {"pack_size_lbs": 1, "note": "coarse grind"}}
  ],
  "bones": {
    "id": "bones",
    "label": "Chef & Marrow Bones + Salvage Bones",
    "yield": {"qty": "3-5", "unit": "packs", "note": "knuckle, leg and other bones"},
    "extraOption": true,
    "removedAt": ["1/8"]
  },
  "organs": {
    "label": "Organs / Offal & Suet",
    "items": ["liver", "heart", "kidney", "oxtail", "tongue", "cheek", "sweetbread", "suet"],
    "note": "May not always be available. Not part of hanging weight, but free to half customers.",
    "removedAt": ["1/8"]
  }
}'::jsonb)
ON CONFLICT (species) DO UPDATE SET config = EXCLUDED.config, updated_at = now();

-- ─── PORK ──────────────────────────────────────────────────────────────────

INSERT INTO cut_sheet_configs (species, config) VALUES
('pork', '{
  "label": "Pork",
  "baseShareSize": "1/1",
  "shareSizes": ["1/1", "1/2", "1/4"],
  "wholeRequiresTwoSheets": false,
  "sections": [
    {
      "id": "shoulder",
      "label": "Shoulder",
      "color": "sky",
      "cuts": [
        {
          "id": "shoulder",
          "label": "Shoulder",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "4", "unit": "roasts"}},
            {"id": "steaks", "label": "Blade Steaks", "requiresThickness": true,
             "yield": {"qty": "8-10", "unit": "steaks"}},
            {"id": "country_style_ribs", "label": "Country Style Ribs",
             "yield": {"qty": "4", "unit": "packs", "note": "4 per pack"}},
            {"id": "stew_meat", "label": "Stew Meat",
             "yield": {"qty": "5", "unit": "packs", "weight_each_lbs": "1"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5"],
          "thicknessRecommended": "1"
        }
      ]
    },
    {
      "id": "loin",
      "label": "Loin",
      "color": "white",
      "cuts": [
        {
          "id": "pork_chops",
          "label": "Pork Chops",
          "options": [
            {"id": "bone_in", "label": "Bone-In", "recommended": true, "requiresThickness": true,
             "yield": {"qty": "14", "unit": "packs", "note": "2 per pack"}},
            {"id": "boneless", "label": "Boneless", "requiresThickness": true,
             "yield": {"qty": "14", "unit": "packs", "note": "2 per pack"}},
            {"id": "loin_roast", "label": "Loin Roast",
             "yield": {"qty": "2", "unit": "roasts"}},
            {"id": "butterfly", "label": "Butterfly Chops", "requiresThickness": true,
             "yield": {"qty": "10-12", "unit": "pieces"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25", "1.5"],
          "thicknessRecommended": "1"
        },
        {
          "id": "tenderloin",
          "label": "Tenderloin",
          "options": [
            {"id": "whole_roast", "label": "Whole Roast", "recommended": true,
             "yield": {"qty": "2", "unit": "tenderloins"}},
            {"id": "medallions", "label": "Medallions", "requiresThickness": true,
             "yield": {"qty": "6-8", "unit": "pieces"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25"],
          "thicknessRecommended": "1"
        },
        {
          "id": "loin_ribs",
          "label": "Loin Ribs",
          "options": [
            {"id": "baby_back", "label": "Baby Back Ribs",
             "yield": {"qty": "2", "unit": "racks", "note": "reduces chop yield"}},
            {"id": "leave_on_chops", "label": "Leave on Chops", "recommended": true,
             "yield": {"qty": null, "unit": "none", "note": "left on chops for thicker cut"}}
          ],
          "grind": false,
          "thickness": null
        }
      ]
    },
    {
      "id": "belly",
      "label": "Belly",
      "color": "rose",
      "cuts": [
        {
          "id": "bacon",
          "label": "Bacon / Belly",
          "options": [
            {"id": "smoked", "label": "Smoked Bacon", "recommended": true,
             "yield": {"qty": "18", "unit": "packs", "weight_each_lbs": "1"}},
            {"id": "fresh_belly", "label": "Fresh Belly",
             "yield": {"qty": "2", "unit": "slabs"}}
          ],
          "grind": true,
          "thickness": null
        },
        {
          "id": "belly_ribs",
          "label": "Belly Ribs",
          "options": [
            {"id": "st_louis", "label": "St. Louis Style", "recommended": true,
             "yield": {"qty": "2", "unit": "slabs"}},
            {"id": "spare", "label": "Spare Ribs",
             "yield": {"qty": "2", "unit": "slabs", "note": "reduces bacon yield"}},
            {"id": "leave_on_belly", "label": "Leave on Belly",
             "yield": {"qty": null, "unit": "none", "note": "stays on belly"}}
          ],
          "grind": false,
          "thickness": null
        }
      ]
    },
    {
      "id": "ham",
      "label": "Ham",
      "color": "amber",
      "cuts": [
        {
          "id": "ham",
          "label": "Ham",
          "options": [
            {"id": "fresh", "label": "Fresh (Bone-In)", "recommended": true,
             "yield": {"qty": "4", "unit": "roasts", "weight_each_lbs": "6"}},
            {"id": "slices_smoked", "label": "Smoked Slices",
             "yield": {"qty": "4", "unit": "packs"}},
            {"id": "fresh_steaks", "label": "Fresh Steaks", "requiresThickness": true,
             "yield": {"qty": "8-10", "unit": "steaks"}},
            {"id": "shank_butt_split", "label": "Shank & Butt Split",
             "yield": {"qty": "8", "unit": "pieces"}},
            {"id": "fresh_boneless", "label": "Fresh Boneless",
             "yield": {"qty": "4", "unit": "roasts", "weight_each_lbs": "5"}}
          ],
          "grind": true,
          "thickness": ["0.5", "1", "1.25"],
          "thicknessRecommended": "1"
        }
      ]
    },
    {
      "id": "extras",
      "label": "Extras",
      "color": "emerald",
      "cuts": [
        {
          "id": "hocks",
          "label": "Hocks",
          "options": [
            {"id": "fresh", "label": "Fresh", "recommended": true,
             "yield": {"qty": "4", "unit": "hocks"}},
            {"id": "split", "label": "Split",
             "yield": {"qty": "8", "unit": "pieces"}}
          ],
          "grind": true,
          "thickness": null,
          "removedAt": ["1/4"]
        },
        {
          "id": "neck_bones",
          "label": "Neck Bones",
          "options": [
            {"id": "keep", "label": "Keep", "recommended": true,
             "yield": {"qty": "2", "unit": "packs", "weight_each_lbs": "1.5"}}
          ],
          "grind": true,
          "thickness": null,
          "removedAt": ["1/4"]
        }
      ]
    }
  ],
  "ground": {
    "id": "ground_pork",
    "label": "Ground Pork",
    "ratios": null,
    "mixOrgans": false,
    "yield": {"min_lbs": 15, "pack_size_lbs": 1},
    "note": "All grind selections go to plain ground pork."
  },
  "specialties": [],
  "sausage": {
    "id": "sausage",
    "label": "Sausage",
    "options": [
      {"id": "bulk", "label": "Bulk", "recommended": true},
      {"id": "links", "label": "Links"}
    ],
    "yield": {"qty": "10", "unit": "packs", "weight_each_lbs": "1"}
  },
  "bones": null,
  "organs": null
}'::jsonb)
ON CONFLICT (species) DO UPDATE SET config = EXCLUDED.config, updated_at = now();

-- ─── LAMB ──────────────────────────────────────────────────────────────────

INSERT INTO cut_sheet_configs (species, config) VALUES
('lamb', '{
  "label": "Lamb",
  "baseShareSize": "1/1",
  "shareSizes": ["1/1", "1/2"],
  "wholeRequiresTwoSheets": false,
  "sections": [
    {
      "id": "rack_loin",
      "label": "Rack & Loin",
      "color": "sky",
      "cuts": [
        {
          "id": "rack",
          "label": "Rack",
          "options": [
            {"id": "whole_rack", "label": "Whole Rack", "recommended": true,
             "yield": {"qty": "2", "unit": "racks"}},
            {"id": "rib_chops", "label": "Rib Chops", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "pieces"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        },
        {
          "id": "loin_chops",
          "label": "Loin Chops",
          "options": [
            {"id": "chops", "label": "Chops", "recommended": true, "requiresThickness": true,
             "yield": {"qty": "4", "unit": "packs", "note": "2 per pack"}},
            {"id": "loin_roast", "label": "Loin Roast",
             "yield": {"qty": "2", "unit": "roasts"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        }
      ]
    },
    {
      "id": "leg",
      "label": "Leg",
      "color": "rose",
      "cuts": [
        {
          "id": "leg_shanks",
          "label": "Leg Shanks",
          "options": [
            {"id": "shanks", "label": "Shanks", "recommended": true,
             "yield": {"qty": "4", "unit": "shanks"}}
          ],
          "grind": true,
          "thickness": null
        },
        {
          "id": "leg",
          "label": "Leg",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "2", "unit": "roasts"}},
            {"id": "steaks", "label": "Steaks", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "steaks"}},
            {"id": "boneless", "label": "Boneless",
             "yield": {"qty": "2", "unit": "roasts"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        }
      ]
    },
    {
      "id": "shoulder_section",
      "label": "Shoulder",
      "color": "amber",
      "cuts": [
        {
          "id": "shoulder",
          "label": "Shoulder",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "2", "unit": "roasts"}},
            {"id": "steaks", "label": "Steaks", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "steaks"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        }
      ]
    }
  ],
  "ground": {
    "id": "ground",
    "label": "Ground Lamb",
    "ratios": null,
    "mixOrgans": false,
    "yield": {"min_lbs": 2, "pack_size_lbs": 1},
    "note": "All grind selections go to ground lamb."
  },
  "specialties": [],
  "stew": {
    "id": "stew_meat",
    "label": "Stew Meat",
    "fixed": true,
    "yield": {"qty": "2-4", "unit": "packs", "weight_each_lbs": "1", "note": "bone-in, always included"}
  },
  "bones": null,
  "organs": null
}'::jsonb)
ON CONFLICT (species) DO UPDATE SET config = EXCLUDED.config, updated_at = now();

-- ─── GOAT ──────────────────────────────────────────────────────────────────

INSERT INTO cut_sheet_configs (species, config) VALUES
('goat', '{
  "label": "Goat",
  "baseShareSize": "1/1",
  "shareSizes": ["1/1", "1/2"],
  "wholeRequiresTwoSheets": false,
  "sections": [
    {
      "id": "rack_loin",
      "label": "Rack & Loin",
      "color": "sky",
      "cuts": [
        {
          "id": "rack",
          "label": "Rack",
          "options": [
            {"id": "whole_rack", "label": "Whole Rack", "recommended": true,
             "yield": {"qty": "2", "unit": "racks"}},
            {"id": "rib_chops", "label": "Rib Chops", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "pieces"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        },
        {
          "id": "loin_chops",
          "label": "Loin Chops",
          "options": [
            {"id": "chops", "label": "Chops", "recommended": true, "requiresThickness": true,
             "yield": {"qty": "4", "unit": "packs", "note": "2 per pack"}},
            {"id": "loin_roast", "label": "Loin Roast",
             "yield": {"qty": "2", "unit": "roasts"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        }
      ]
    },
    {
      "id": "leg",
      "label": "Leg",
      "color": "rose",
      "cuts": [
        {
          "id": "leg_shanks",
          "label": "Leg Shanks",
          "options": [
            {"id": "shanks", "label": "Shanks", "recommended": true,
             "yield": {"qty": "4", "unit": "shanks"}}
          ],
          "grind": true,
          "thickness": null
        },
        {
          "id": "leg",
          "label": "Leg",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "2", "unit": "roasts"}},
            {"id": "steaks", "label": "Steaks", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "steaks"}},
            {"id": "boneless", "label": "Boneless",
             "yield": {"qty": "2", "unit": "roasts"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        }
      ]
    },
    {
      "id": "shoulder_section",
      "label": "Shoulder",
      "color": "amber",
      "cuts": [
        {
          "id": "shoulder",
          "label": "Shoulder",
          "options": [
            {"id": "roasts", "label": "Roasts", "recommended": true,
             "yield": {"qty": "2", "unit": "roasts"}},
            {"id": "steaks", "label": "Steaks", "requiresThickness": true,
             "yield": {"qty": "varies", "unit": "steaks"}}
          ],
          "grind": true,
          "thickness": ["1"],
          "thicknessRecommended": "1"
        }
      ]
    }
  ],
  "ground": {
    "id": "ground",
    "label": "Ground Goat",
    "ratios": null,
    "mixOrgans": false,
    "yield": {"min_lbs": 2, "pack_size_lbs": 1},
    "note": "All grind selections go to ground goat."
  },
  "specialties": [],
  "stew": {
    "id": "stew_meat",
    "label": "Stew Meat",
    "fixed": true,
    "yield": {"qty": "2-4", "unit": "packs", "weight_each_lbs": "1", "note": "bone-in, always included"}
  },
  "bones": null,
  "organs": null
}'::jsonb)
ON CONFLICT (species) DO UPDATE SET config = EXCLUDED.config, updated_at = now();
