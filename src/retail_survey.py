"""
Terra Mensa — Retail Price Survey Entry

Monthly manual entry of local grocery meat prices for competitive comparison.

Usage:
    python3 retail_survey.py --species cattle --store "Martin's" [--supabase]
    python3 retail_survey.py --report [--supabase]
"""

import sys
from datetime import date

from optimizer_config import get_connection

# Standard cuts to survey per species
SURVEY_CUTS = {
    'cattle': [
        ('ground_beef_80_20', 'Ground Beef 80/20'),
        ('ground_beef_90_10', 'Ground Beef 90/10'),
        ('chuck_roast', 'Chuck Roast'),
        ('chuck_steak', 'Chuck Steak'),
        ('brisket', 'Brisket (whole/flat)'),
        ('short_ribs', 'Short Ribs'),
        ('ribeye_steak', 'Ribeye Steak'),
        ('ny_strip', 'NY Strip Steak'),
        ('sirloin_steak', 'Sirloin Steak'),
        ('tenderloin', 'Tenderloin / Filet'),
        ('top_round', 'Top Round Roast'),
        ('bottom_round', 'Bottom Round Roast'),
        ('flank_steak', 'Flank Steak'),
        ('stew_meat', 'Stew Meat / Cubes'),
        ('beef_ribs', 'Beef Back Ribs'),
    ],
    'pork': [
        ('ground_pork', 'Ground Pork'),
        ('pork_chops_bone_in', 'Pork Chops (bone-in)'),
        ('pork_chops_boneless', 'Pork Chops (boneless)'),
        ('pork_tenderloin', 'Pork Tenderloin'),
        ('pork_shoulder', 'Pork Shoulder / Butt'),
        ('spare_ribs', 'Spare Ribs'),
        ('baby_back_ribs', 'Baby Back Ribs'),
        ('ham', 'Ham (bone-in)'),
        ('bacon', 'Bacon (per lb)'),
        ('pork_belly', 'Pork Belly'),
    ],
    'lamb': [
        ('ground_lamb', 'Ground Lamb'),
        ('lamb_chops', 'Lamb Loin Chops'),
        ('rack_of_lamb', 'Rack of Lamb'),
        ('leg_of_lamb', 'Leg of Lamb'),
        ('lamb_shoulder', 'Lamb Shoulder'),
        ('lamb_stew', 'Lamb Stew Meat'),
    ],
    'goat': [
        ('goat_stew', 'Goat Stew Meat'),
        ('goat_chops', 'Goat Chops'),
        ('goat_leg', 'Goat Leg'),
        ('ground_goat', 'Ground Goat'),
    ],
}

# Approximate yield % per cut category (for weighted blended average)
# These represent what % of the take-home meat each cut type comprises
CUT_YIELD_WEIGHTS = {
    'cattle': {
        'ground_beef_80_20': 25.0, 'ground_beef_90_10': 15.0,
        'chuck_roast': 6.0, 'chuck_steak': 4.0,
        'brisket': 4.0, 'short_ribs': 3.0,
        'ribeye_steak': 5.0, 'ny_strip': 4.0,
        'sirloin_steak': 5.0, 'tenderloin': 2.0,
        'top_round': 7.0, 'bottom_round': 5.0,
        'flank_steak': 2.0, 'stew_meat': 8.0,
        'beef_ribs': 5.0,
    },
    'pork': {
        'ground_pork': 20.0, 'pork_chops_bone_in': 10.0,
        'pork_chops_boneless': 8.0, 'pork_tenderloin': 5.0,
        'pork_shoulder': 15.0, 'spare_ribs': 10.0,
        'baby_back_ribs': 5.0, 'ham': 15.0,
        'bacon': 7.0, 'pork_belly': 5.0,
    },
    'lamb': {
        'ground_lamb': 30.0, 'lamb_chops': 15.0,
        'rack_of_lamb': 10.0, 'leg_of_lamb': 25.0,
        'lamb_shoulder': 15.0, 'lamb_stew': 5.0,
    },
    'goat': {
        'goat_stew': 30.0, 'goat_chops': 20.0,
        'goat_leg': 30.0, 'ground_goat': 20.0,
    },
}


def enter_prices(conn, species, store_name, survey_date=None):
    """Interactive CLI to enter retail prices for a species at a store."""
    if survey_date is None:
        survey_date = date.today()

    cuts = SURVEY_CUTS.get(species, [])
    if not cuts:
        print(f"Unknown species: {species}")
        return

    print(f"\n═══ Retail Price Survey ═══")
    print(f"  Store: {store_name}")
    print(f"  Species: {species}")
    print(f"  Date: {survey_date}")
    print(f"  Enter $/lb for each cut (blank to skip, 's' for sale price):\n")

    entries = []
    for cut_name, description in cuts:
        raw = input(f"  {description:<30} $/lb: ").strip()
        if not raw:
            continue

        is_sale = False
        if raw.lower().endswith('s'):
            is_sale = True
            raw = raw[:-1].strip()

        try:
            price = float(raw)
            entries.append((cut_name, description, price, is_sale))
            tag = " (SALE)" if is_sale else ""
            print(f"    → ${price:.2f}/lb{tag}")
        except ValueError:
            print(f"    → skipped (invalid)")

    if not entries:
        print("\nNo prices entered.")
        return

    # Write to database
    cur = conn.cursor()
    for cut_name, description, price, is_sale in entries:
        cur.execute("""
            INSERT INTO retail_price_survey
                (survey_date, store_name, species, cut_name, cut_description, price_per_lb, is_sale_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (survey_date, store_name, species, cut_name)
            DO UPDATE SET price_per_lb = EXCLUDED.price_per_lb,
                          is_sale_price = EXCLUDED.is_sale_price,
                          cut_description = EXCLUDED.cut_description
        """, (survey_date, store_name, species, cut_name, description, price, is_sale))

    conn.commit()
    print(f"\n  Saved {len(entries)} prices to retail_price_survey")

    # Compute blended average
    avg = compute_blended_average(conn, species, survey_date, store_name)
    if avg:
        print(f"  Blended average: ${avg:.2f}/lb")


def compute_blended_average(conn, species, survey_date=None, store_name=None):
    """Compute yield-weighted blended average retail price for a species.

    Uses CUT_YIELD_WEIGHTS to weight each cut by its approximate share of the animal.
    """
    cur = conn.cursor()

    if survey_date and store_name:
        cur.execute("""
            SELECT cut_name, price_per_lb FROM retail_price_survey
            WHERE species = %s AND survey_date = %s AND store_name = %s
        """, (species, survey_date, store_name))
    else:
        # Most recent entry per cut across all stores
        cur.execute("""
            SELECT DISTINCT ON (cut_name) cut_name, price_per_lb
            FROM retail_price_survey
            WHERE species = %s
            ORDER BY cut_name, survey_date DESC
        """, (species,))

    prices = {row[0]: float(row[1]) for row in cur.fetchall()}
    if not prices:
        return None

    weights = CUT_YIELD_WEIGHTS.get(species, {})
    total_weight = 0
    weighted_sum = 0

    for cut_name, price in prices.items():
        w = weights.get(cut_name, 1.0)
        weighted_sum += price * w
        total_weight += w

    return round(weighted_sum / total_weight, 2) if total_weight > 0 else None


def print_report(conn):
    """Print latest retail prices and blended averages for all species."""
    cur = conn.cursor()

    print("═══ Retail Price Survey Report ═══\n")

    for species in ['cattle', 'pork', 'lamb', 'goat']:
        cur.execute("""
            SELECT DISTINCT ON (cut_name) cut_name, cut_description, price_per_lb,
                   is_sale_price, store_name, survey_date
            FROM retail_price_survey
            WHERE species = %s
            ORDER BY cut_name, survey_date DESC
        """, (species,))
        rows = cur.fetchall()

        if not rows:
            print(f"  {species.upper()}: no data")
            continue

        print(f"  {species.upper()} (latest entries):")
        for r in rows:
            sale = " (SALE)" if r[3] else ""
            print(f"    {r[1]:<30} ${r[2]:>6.2f}/lb{sale}  [{r[4]}, {r[5]}]")

        avg = compute_blended_average(conn, species)
        if avg:
            print(f"    {'─' * 50}")
            print(f"    {'Blended average':<30} ${avg:>6.2f}/lb")
        print()


if __name__ == '__main__':
    use_supabase = '--supabase' in sys.argv

    if '--report' in sys.argv:
        conn = get_connection(use_supabase=use_supabase)
        print_report(conn)
        conn.close()
    else:
        species = None
        store = None
        if '--species' in sys.argv:
            idx = sys.argv.index('--species')
            if idx + 1 < len(sys.argv):
                species = sys.argv[idx + 1]
        if '--store' in sys.argv:
            idx = sys.argv.index('--store')
            if idx + 1 < len(sys.argv):
                store = sys.argv[idx + 1]

        if not species or not store:
            print("Usage: python3 retail_survey.py --species cattle --store \"Martin's\" [--supabase]")
            print("       python3 retail_survey.py --report [--supabase]")
            sys.exit(1)

        conn = get_connection(use_supabase=use_supabase)

        # Create table if not exists
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM retail_price_survey LIMIT 1")
            conn.rollback()
        except Exception:
            conn.rollback()
            cur.execute(open('../sql/025_retail_price_survey.sql').read())
            conn.commit()

        enter_prices(conn, species, store)
        conn.close()
