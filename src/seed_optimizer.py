"""Seed optimizer reference tables from existing config.py yield dicts.

Populates: config_cut_specs, config_grade_hierarchy
Run: python3 seed_optimizer.py
"""
from db import init_schema, get_connection
from optimizer_db import save_cut_specs_bulk, save_grade_hierarchy_bulk
from config import (
    SUBPRIMAL_YIELDS, GROUND_BEEF_PRODUCTS,
    PORK_CUT_YIELDS, LAMB_SUBPRIMAL_YIELDS,
    CHICKEN_CUT_YIELDS, GOAT_CUT_YIELDS,
    TRIM_YIELD_PCT,
)

# Premium IMPS codes (high-value steaks/cuts)
BEEF_PREMIUM = {"112A", "180", "189A", "185C", "174", "175", "185A", "109E"}
PORK_PREMIUM = {"415", "412B", "409"}
LAMB_PREMIUM = {"204", "204C", "232"}


def _primal_code(primal_name: str) -> str:
    """Normalize primal name to an uppercase code."""
    return primal_name.upper().replace("/", "_").replace(" ", "_")


def build_beef_specs() -> list:
    specs = []
    # Subprimal yields (IMPS codes)
    for code, (desc, yield_pct, primal) in SUBPRIMAL_YIELDS.items():
        specs.append({
            'species': 'cattle',
            'primal_code': _primal_code(primal),
            'primal_name': primal,
            'cut_code': code,
            'cut_name': desc,
            'yield_pct': yield_pct,
            'is_premium': code in BEEF_PREMIUM,
        })
    # Ground beef products (from trim)
    for code, info in GROUND_BEEF_PRODUCTS.items():
        specs.append({
            'species': 'cattle',
            'primal_code': 'TRIM',
            'primal_name': 'Trim',
            'cut_code': code,
            'cut_name': info['description'],
            'yield_pct': TRIM_YIELD_PCT * 100 / len(GROUND_BEEF_PRODUCTS),
            'is_premium': False,
            'notes': f"{info['lean_pct']}/{info['fat_pct']} lean/fat",
        })
    # Misc non-retail (bones, offal, shrink) — complement to ~100%
    existing_total = sum(v[1] for v in SUBPRIMAL_YIELDS.values()) + TRIM_YIELD_PCT * 100
    remainder = max(0, 100.0 - existing_total)
    misc_items = [
        ('bones_stock', 'Soup Bones / Stock Bones', 0.40),
        ('offal', 'Offal / Organ Meat', 0.25),
        ('fat_shrink', 'Fat / Cooler Shrink / Loss', 0.35),
    ]
    for code, name, share in misc_items:
        specs.append({
            'species': 'cattle',
            'primal_code': 'MISC',
            'primal_name': 'Misc',
            'cut_code': code,
            'cut_name': name,
            'yield_pct': round(remainder * share, 2),
            'is_premium': False,
        })
    return specs


def build_pork_specs() -> list:
    specs = []
    for code, (desc, yield_pct, primal) in PORK_CUT_YIELDS.items():
        specs.append({
            'species': 'pork',
            'primal_code': _primal_code(primal),
            'primal_name': primal,
            'cut_code': code,
            'cut_name': desc,
            'yield_pct': yield_pct,
            'is_premium': code in PORK_PREMIUM,
        })
    existing = sum(v[1] for v in PORK_CUT_YIELDS.values())
    remainder = max(0, 100.0 - existing)
    misc = [
        ('pork_ground', 'Ground Pork (trim)', 0.40),
        ('pork_hocks', 'Ham Hocks / Feet', 0.25),
        ('pork_fat_shrink', 'Fat / Shrink / Loss', 0.35),
    ]
    for code, name, share in misc:
        specs.append({
            'species': 'pork',
            'primal_code': 'MISC',
            'primal_name': 'Misc',
            'cut_code': code,
            'cut_name': name,
            'yield_pct': round(remainder * share, 2),
            'is_premium': False,
        })
    return specs


def build_lamb_specs() -> list:
    specs = []
    for code, (desc, yield_pct, primal) in LAMB_SUBPRIMAL_YIELDS.items():
        specs.append({
            'species': 'lamb',
            'primal_code': _primal_code(primal),
            'primal_name': primal,
            'cut_code': code,
            'cut_name': desc,
            'yield_pct': yield_pct,
            'is_premium': code in LAMB_PREMIUM,
        })
    existing = sum(v[1] for v in LAMB_SUBPRIMAL_YIELDS.values())
    remainder = max(0, 100.0 - existing)
    misc = [
        ('lamb_stew', 'Stew Meat (trim)', 0.30),
        ('lamb_bones', 'Bones / Stock', 0.25),
        ('lamb_fat_shrink', 'Fat / Shrink / Loss', 0.45),
    ]
    for code, name, share in misc:
        specs.append({
            'species': 'lamb',
            'primal_code': 'MISC',
            'primal_name': 'Misc',
            'cut_code': code,
            'cut_name': name,
            'yield_pct': round(remainder * share, 2),
            'is_premium': False,
        })
    return specs


def build_chicken_specs() -> list:
    # Chicken yields in config.py include overlapping forms (breast_bnls vs
    # breast_bone). For optimizer we need non-overlapping cuts summing to 100%.
    # Use boneless breast (not bone-in) and normalize.
    skip = {'breast_bone'}  # overlaps with breast_bnls
    specs = []
    raw_total = sum(v[1] for k, v in CHICKEN_CUT_YIELDS.items() if k not in skip)
    # Scale factor to normalize to ~95% (leaving 5% for shrink/loss)
    target = 95.0
    scale = target / raw_total if raw_total > 0 else 1.0
    for code, (desc, yield_pct, category) in CHICKEN_CUT_YIELDS.items():
        if code in skip:
            continue
        specs.append({
            'species': 'chicken',
            'primal_code': _primal_code(category),
            'primal_name': category,
            'cut_code': code,
            'cut_name': desc,
            'yield_pct': round(yield_pct * scale, 2),
            'is_premium': code in ('breast_bnls', 'tender'),
        })
    scaled_total = sum(s['yield_pct'] for s in specs)
    remainder = round(100.0 - scaled_total, 2)
    if remainder > 0:
        specs.append({
            'species': 'chicken',
            'primal_code': 'MISC',
            'primal_name': 'Misc',
            'cut_code': 'chicken_shrink',
            'cut_name': 'Shrink / Loss',
            'yield_pct': remainder,
            'is_premium': False,
        })
    return specs


def build_goat_specs() -> list:
    specs = []
    for code, (desc, yield_pct, primal) in GOAT_CUT_YIELDS.items():
        specs.append({
            'species': 'goat',
            'primal_code': _primal_code(primal),
            'primal_name': primal,
            'cut_code': code,
            'cut_name': desc,
            'yield_pct': yield_pct,
            'is_premium': code in ('loin',),
        })
    existing = sum(v[1] for v in GOAT_CUT_YIELDS.values())
    remainder = max(0, 100.0 - existing)
    if remainder > 0:
        specs.append({
            'species': 'goat',
            'primal_code': 'MISC',
            'primal_name': 'Misc',
            'cut_code': 'goat_shrink',
            'cut_name': 'Shrink / Loss',
            'yield_pct': round(remainder, 2),
            'is_premium': False,
        })
    return specs


def build_grade_hierarchy() -> list:
    return [
        # Cattle
        {'species': 'cattle', 'grade_code': 'prime',    'grade_name': 'USDA Prime',    'rank_order': 5},
        {'species': 'cattle', 'grade_code': 'grassfed', 'grade_name': 'Grass-Fed',     'rank_order': 4},
        {'species': 'cattle', 'grade_code': 'choice',   'grade_name': 'USDA Choice',   'rank_order': 3},
        {'species': 'cattle', 'grade_code': 'select',   'grade_name': 'USDA Select',   'rank_order': 2},
        {'species': 'cattle', 'grade_code': 'standard', 'grade_name': 'USDA Standard', 'rank_order': 1},
        # Pork (USDA pork grades are less common; simplified)
        {'species': 'pork', 'grade_code': 'premium',  'grade_name': 'Premium',        'rank_order': 3},
        {'species': 'pork', 'grade_code': 'standard', 'grade_name': 'Standard',       'rank_order': 2},
        {'species': 'pork', 'grade_code': 'utility',  'grade_name': 'Utility',        'rank_order': 1},
        # Lamb
        {'species': 'lamb', 'grade_code': 'prime',    'grade_name': 'USDA Prime',     'rank_order': 4},
        {'species': 'lamb', 'grade_code': 'choice',   'grade_name': 'USDA Choice',    'rank_order': 3},
        {'species': 'lamb', 'grade_code': 'good',     'grade_name': 'USDA Good',      'rank_order': 2},
        {'species': 'lamb', 'grade_code': 'utility',  'grade_name': 'USDA Utility',   'rank_order': 1},
        # Goat (no official USDA grading; simplified)
        {'species': 'goat', 'grade_code': 'premium',  'grade_name': 'Premium',        'rank_order': 2},
        {'species': 'goat', 'grade_code': 'standard', 'grade_name': 'Standard',       'rank_order': 1},
        # Chicken (no carcass grading relevant here)
        {'species': 'chicken', 'grade_code': 'grade_a', 'grade_name': 'USDA Grade A', 'rank_order': 2},
        {'species': 'chicken', 'grade_code': 'standard', 'grade_name': 'Standard',    'rank_order': 1},
    ]


def seed_all():
    """Seed all optimizer reference data."""
    # Ensure schema is up to date
    init_schema()

    # Run ALTER for dtc_customers geo columns
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE dtc_customers ADD COLUMN IF NOT EXISTS latitude NUMERIC(9,6)")
            cur.execute("ALTER TABLE dtc_customers ADD COLUMN IF NOT EXISTS longitude NUMERIC(9,6)")
        conn.commit()
    finally:
        conn.close()

    all_specs = []
    all_specs.extend(build_beef_specs())
    all_specs.extend(build_pork_specs())
    all_specs.extend(build_lamb_specs())
    all_specs.extend(build_chicken_specs())
    all_specs.extend(build_goat_specs())

    save_cut_specs_bulk(all_specs)
    print(f"Seeded {len(all_specs)} cut specs across 5 species.")

    grades = build_grade_hierarchy()
    save_grade_hierarchy_bulk(grades)
    print(f"Seeded {len(grades)} grade hierarchy entries.")

    # Verify totals
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT species, ROUND(SUM(yield_pct)::numeric, 1) as total_pct,
                       COUNT(*) as cut_count
                FROM config_cut_specs GROUP BY species ORDER BY species
            """)
            print("\nYield totals by species:")
            for row in cur.fetchall():
                print(f"  {row[0]:10s}  {row[2]:3d} cuts  {row[1]:6.1f}%")
    finally:
        conn.close()


if __name__ == '__main__':
    seed_all()
