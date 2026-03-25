-- 015_cut_sheet_templates.sql
-- Named presets that auto-fill the cut sheet form.
-- Templates are the "preferred cut preferences" in the buy flow.
-- Run: psql -d terra_mensa -f sql/015_cut_sheet_templates.sql

CREATE TABLE IF NOT EXISTS cut_sheet_templates (
    id          SERIAL PRIMARY KEY,
    species     TEXT NOT NULL REFERENCES cut_sheet_configs(species),
    share_size  TEXT NOT NULL,
    name        TEXT NOT NULL,
    label       TEXT NOT NULL,
    description TEXT,
    selections  JSONB NOT NULL,
    sort_order  INT DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (species, share_size, name)
);

-- ─── BEEF templates (share sizes: 1/2, 1/4, 1/8) ─────────────────────────
-- Seed from existing beef_cut_sheet_defaults

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'beef', '1/2', 'standard', 'Recommended family balance',
       'A balanced cut sheet with steaks, roasts, and ground beef for everyday cooking.',
       selections, 1
FROM beef_cut_sheet_defaults WHERE name = 'standard'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'beef', '1/2', 'steak_lover', 'More steaks, fewer roasts',
       'Shifts more of the animal into premium steak cuts for grilling and hosting.',
       selections, 2
FROM beef_cut_sheet_defaults WHERE name = 'steak_lover'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'beef', '1/4', 'standard', 'Recommended family balance',
       'A balanced quarter with steaks, roasts, and ground beef.',
       selections, 1
FROM beef_cut_sheet_defaults WHERE name = 'standard_quarter'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'beef', '1/4', 'steak_lover', 'More steaks, fewer roasts',
       'Quarter share optimized for premium steak cuts.',
       selections, 2
FROM beef_cut_sheet_defaults WHERE name = 'steak_lover_quarter'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'beef', '1/8', 'standard', 'Recommended family balance',
       'A compact eighth share with the essentials.',
       selections, 1
FROM beef_cut_sheet_defaults WHERE name = 'standard_eighth'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'beef', '1/8', 'steak_lover', 'More steaks, fewer roasts',
       'Eighth share focused on steak cuts.',
       selections, 2
FROM beef_cut_sheet_defaults WHERE name = 'steak_lover_eighth'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

-- ─── PORK templates (share sizes: 1/1, 1/2, 1/4) ─────────────────────────

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'pork', '1/1', 'standard', 'Recommended family balance',
       'A practical mix of chops, roasts, bacon, sausage, and ham.',
       selections, 1
FROM pork_cut_sheet_defaults WHERE name = 'standard'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'pork', '1/1', 'steak_lover', 'More chops and steaks',
       'Leans into boneless chops, ham steaks, and medallions.',
       selections, 2
FROM pork_cut_sheet_defaults WHERE name = 'steak_lover'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'pork', '1/2', 'standard', 'Recommended family balance',
       'Half hog with balanced chops, bacon, and ham.',
       selections, 1
FROM pork_cut_sheet_defaults WHERE name = 'standard_half'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'pork', '1/2', 'steak_lover', 'More chops and steaks',
       'Half hog focused on chops, steaks, and medallions.',
       selections, 2
FROM pork_cut_sheet_defaults WHERE name = 'steak_lover_half'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'pork', '1/4', 'standard', 'Recommended family balance',
       'Quarter hog with the essentials.',
       selections, 1
FROM pork_cut_sheet_defaults WHERE name = 'standard_quarter'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'pork', '1/4', 'steak_lover', 'More chops and steaks',
       'Quarter hog focused on chops and steaks.',
       selections, 2
FROM pork_cut_sheet_defaults WHERE name = 'steak_lover_quarter'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

-- ─── LAMB templates (share sizes: 1/1, 1/2) ───────────────────────────────

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'lamb', '1/1', 'standard', 'Recommended balance',
       'Racks, chops, leg roasts, and stew meat.',
       selections, 1
FROM lamb_cut_sheet_defaults WHERE name = 'standard'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'lamb', '1/2', 'standard', 'Recommended balance',
       'Half lamb with racks, chops, and leg.',
       selections, 1
FROM lamb_cut_sheet_defaults WHERE name = 'standard_half'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

-- ─── GOAT templates (share sizes: 1/1, 1/2) ───────────────────────────────

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'goat', '1/1', 'standard', 'Recommended balance',
       'Racks, chops, leg roasts, and stew meat.',
       selections, 1
FROM goat_cut_sheet_defaults WHERE name = 'standard'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO cut_sheet_templates (species, share_size, name, label, description, selections, sort_order)
SELECT 'goat', '1/2', 'standard', 'Recommended balance',
       'Half goat with racks, chops, and leg.',
       selections, 1
FROM goat_cut_sheet_defaults WHERE name = 'standard_half'
ON CONFLICT (species, share_size, name) DO UPDATE SET
  selections = EXCLUDED.selections, label = EXCLUDED.label, description = EXCLUDED.description;
