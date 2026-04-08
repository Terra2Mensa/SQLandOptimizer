"""
Terra Mensa — Unified Market Data Fetcher

Pulls weekly USDA market data for all species:
- Live/auction prices (FLOOR)
- Wholesale boxed cutout by cut (WHOLESALE)
- Retail feature prices where available (CEILING input)

Usage:
    python3 market_data.py [--species cattle|pork|lamb|goat|all]
"""

import sys
import json
from datetime import date
from collections import defaultdict

from config import (
    MARS_API_KEY,
    REPORT_CHOICE_SELECT, REPORT_PRIME, REPORT_5AREA_WEEKLY,
    REPORT_PREMIUMS, REPORT_IN_AUCTION,
    SUBPRIMAL_YIELDS, PRIMAL_ORDER,
    PROCESSING_RATES, DRESS_PCT_BY_YG, DEFAULT_YIELD_GRADE,
    TRIM_YIELD_PCT,
)
from shared import fetch_datamart, fetch_mars, parse_number

import re

def _extract_imps(description):
    """Extract IMPS code from USDA item description.

    Examples:
        "Rib, ribeye, lip-on, bn-in (109E  1)" → "109E"
        "Rib, ribeye, bnls, light (112A  3)" → "112A"
        "Chuck, semi-bnls, neck/off (113C  4)" → "113C"
    """
    match = re.search(r'\((\d+[A-Za-z]?)\s', description)
    if match:
        return match.group(1)
    # Fallback: look for IMPS-like pattern anywhere
    match = re.search(r'\b(\d{3}[A-Za-z]?)\b', description)
    return match.group(1) if match else None


# Pork and lamb report IDs (from config.py — may need to add if missing)
REPORT_PORK_DAILY = 2498
REPORT_PORK_LIVE = 2510
REPORT_LAMB_CUTOUT = 2649
REPORT_LAMB_BOXED = 2648
REPORT_RETAIL_BEEF_FEATURES = 3228  # NEW: weekly retail beef advertised prices


# ─── Cattle ──────────────────────────────────────────────────────────────

def fetch_cattle_data():
    """Fetch all cattle market data from USDA.

    Returns dict with:
        live_prices: {grade: $/cwt live}
        dressed_prices: {grade: $/cwt dressed}
        cutout_prices: {grade: {imps_code: $/cwt}}
        premiums: {type: $/cwt adjustment}
        auction_prices: [{class, grade, $/cwt, head_count}]
        report_date: str
    """
    result = {
        'species': 'cattle',
        'live_prices': {},
        'dressed_prices': {},
        'cutout_prices': {'choice': {}, 'select': {}, 'prime': {}},
        'premiums': {},
        'auction_prices': [],
        'report_date': None,
    }

    # 1. Boxed beef cutout — Choice + Select (Report 2461)
    try:
        data = fetch_datamart(REPORT_CHOICE_SELECT)
        for section in data:
            section_name = section.get('reportSection', '').lower()
            results = section.get('results', [])

            if not results:
                continue

            # Get report date from first result
            rd = results[0].get('report_date', '')
            if rd and not result['report_date']:
                result['report_date'] = rd[:10]

            # Determine grade from section name
            if 'choice cuts' == section_name or 'choice and select' in section_name:
                grade = 'choice'
            elif 'select cuts' == section_name:
                grade = 'select'
            else:
                continue

            for row in results:
                desc = str(row.get('item_description', '')).strip()
                avg_price = parse_number(row.get('weighted_average'))

                # Extract IMPS code from description: "Rib, ribeye, lip-on, bn-in (109E  1)"
                imps = _extract_imps(desc)
                if imps and avg_price > 0:
                    result['cutout_prices'][grade][imps] = avg_price

        print(f"  Cattle cutout: {len(result['cutout_prices']['choice'])} Choice cuts, "
              f"{len(result['cutout_prices']['select'])} Select cuts")
    except Exception as e:
        print(f"  WARNING: Cattle cutout fetch failed: {e}")

    # 2. Prime cuts (Report 2460)
    try:
        data = fetch_datamart(REPORT_PRIME)
        for section in data:
            results = section.get('results', [])
            for row in results:
                desc = str(row.get('item_description', '')).strip()
                avg_price = parse_number(row.get('weighted_average'))
                imps = _extract_imps(desc)
                if imps and avg_price > 0:
                    result['cutout_prices']['prime'][imps] = avg_price

        print(f"  Cattle prime: {len(result['cutout_prices']['prime'])} cuts")
    except Exception as e:
        print(f"  WARNING: Cattle prime fetch failed: {e}")

    # 3. 5-Area slaughter cattle prices (Report 2477) — Detail section
    try:
        data = fetch_datamart(REPORT_5AREA_WEEKLY)
        for section in data:
            if section.get('reportSection') != 'History':
                continue
            for row in section.get('results', []):
                basis = str(row.get('selling_basis_desc', '')).strip().lower()
                cls = str(row.get('class_description', '')).strip().lower()
                avg_price = parse_number(row.get('weighted_avg_price'))

                if avg_price > 0 and 'steer' in cls:
                    if 'live' in basis:
                        result['live_prices']['fed_cattle'] = avg_price
                    elif 'dress' in basis:
                        result['dressed_prices']['fed_cattle'] = avg_price

        print(f"  Cattle live: ${result['live_prices'].get('fed_cattle', 'N/A')}/cwt, "
              f"dressed: ${result['dressed_prices'].get('fed_cattle', 'N/A')}/cwt")
    except Exception as e:
        print(f"  WARNING: Cattle 5-area fetch failed: {e}")

    # 4. Premiums/discounts (Report 2482)
    try:
        data = fetch_datamart(REPORT_PREMIUMS)
        for section in data:
            for row in section.get('results', []):
                ptype = str(row.get('type', '')).strip()
                pclass = str(row.get('class', '')).strip()
                premium = parse_number(row.get('premium_cwt') or row.get('avg_premium_cwt'))
                if ptype and premium != 0:
                    key = f"{ptype}_{pclass}".lower().replace(' ', '_')
                    result['premiums'][key] = premium
    except Exception as e:
        print(f"  WARNING: Cattle premiums fetch failed: {e}")

    # 5. Indiana auction (MARS 1976)
    if MARS_API_KEY:
        try:
            data = fetch_mars(REPORT_IN_AUCTION, MARS_API_KEY)
            results = data.get('results', []) if isinstance(data, dict) else []
            for row in results:
                avg_price = parse_number(row.get('avg_price_cwt') or row.get('weighted_avg_cwt'))
                head = parse_number(row.get('head_count', 0))
                if avg_price > 0:
                    result['auction_prices'].append({
                        'class': row.get('class', ''),
                        'grade': row.get('grade', ''),
                        'price_cwt': avg_price,
                        'head_count': head,
                    })
            print(f"  Indiana auction: {len(result['auction_prices'])} price points")
        except Exception as e:
            print(f"  WARNING: Indiana auction fetch failed: {e}")

    return result


# ─── Pork ────────────────────────────────────────────────────────────────

def fetch_pork_data():
    """Fetch pork market data."""
    result = {
        'species': 'pork',
        'live_prices': {},
        'dressed_prices': {},
        'cutout_prices': {'standard': {}},
        'report_date': None,
    }

    # Pork cutout (Report 2498)
    try:
        data = fetch_datamart(REPORT_PORK_DAILY)

        # Get primal composite values from "Cutout and Primal Values" section
        for section in data:
            results = section.get('results', [])
            if not results:
                continue

            rd = results[0].get('report_date', '')
            if rd and not result['report_date']:
                result['report_date'] = rd[:10]

            section_name = section.get('reportSection', '')

            # Composite primal values ($/cwt carcass)
            if section_name == 'Cutout and Primal Values':
                row = results[0]
                # Carcass composite IS the dressed price
                carcass_val = parse_number(row.get('pork_carcass'))
                if carcass_val > 0:
                    result['dressed_prices']['carcass'] = carcass_val

                for key in ('pork_carcass', 'pork_loin', 'pork_butt', 'pork_picnic',
                            'pork_rib', 'pork_ham', 'pork_belly'):
                    val = parse_number(row.get(key))
                    if val > 0:
                        result['cutout_prices']['standard'][key] = val

            # Individual cut prices from primal sections
            if section_name in ('Loin Cuts', 'Butt Cuts', 'Sparerib Cuts', 'Trim Cuts',
                                'Picnic Cuts', 'Ham Cuts', 'Belly Cuts'):
                for row in results:
                    desc = str(row.get('Item_Description', '')).strip()
                    avg_price = parse_number(row.get('weighted_average'))
                    if desc and avg_price > 0:
                        result['cutout_prices']['standard'][desc] = avg_price

        print(f"  Pork cutout: {len(result['cutout_prices']['standard'])} price points")
    except Exception as e:
        print(f"  WARNING: Pork cutout fetch failed: {e}")

    # Pork live prices (Report 2510)
    try:
        data = fetch_datamart(REPORT_PORK_LIVE)
        for section in data:
            for row in section.get('results', []):
                avg_price = parse_number(row.get('weighted_average') or row.get('weighted_avg_price'))
                if avg_price > 0:
                    result['live_prices']['national'] = avg_price
                    break

        print(f"  Pork live: ${result['live_prices'].get('national', 'N/A')}/cwt")
    except Exception as e:
        print(f"  WARNING: Pork live fetch failed: {e}")

    return result


# ─── Lamb ────────────────────────────────────────────────────────────────

def fetch_lamb_data():
    """Fetch lamb market data."""
    result = {
        'species': 'lamb',
        'live_prices': {},
        'dressed_prices': {},
        'cutout_prices': {'standard': {}},
        'report_date': None,
    }

    # Lamb cutout (Report 2649)
    try:
        data = fetch_datamart(REPORT_LAMB_CUTOUT)
        for section in data:
            section_name = section.get('reportSection', '')
            results = section.get('results', [])
            if not results:
                continue

            rd = results[0].get('report_date', '')
            if rd and not result['report_date']:
                result['report_date'] = rd[:10]

            # Gross and net carcass values
            if section_name == 'GROSS CARCASS VALUE':
                val = parse_number(results[0].get('gross_carcass_price'))
                if val > 0:
                    result['dressed_prices']['gross_carcass'] = val

            if section_name == 'NET CARCASS VALUE':
                val = parse_number(results[0].get('net_carcass_price'))
                if val > 0:
                    result['dressed_prices']['net_carcass'] = val

            # Detail: individual cuts with IMPS
            if section_name == 'DETAIL':
                for row in results:
                    imps = str(row.get('imps_code', '')).strip()
                    desc = str(row.get('imps_description', '')).strip()
                    fob_price = parse_number(row.get('fob_price'))
                    pct_carcass = parse_number(row.get('percentage_carcass'))

                    if imps and fob_price > 0:
                        result['cutout_prices']['standard'][imps] = {
                            'price_cwt': fob_price,
                            'description': desc,
                            'pct_carcass': pct_carcass,
                        }

        print(f"  Lamb cutout: {len(result['cutout_prices']['standard'])} cuts, "
              f"gross=${result['dressed_prices'].get('gross_carcass', 'N/A')}/cwt")
    except Exception as e:
        print(f"  WARNING: Lamb cutout fetch failed: {e}")

    return result


# ─── Goat ────────────────────────────────────────────────────────────────

def fetch_goat_data():
    """Fetch goat market data. Limited USDA data — primarily auction-based."""
    result = {
        'species': 'goat',
        'live_prices': {},
        'dressed_prices': {},
        'cutout_prices': {},
        'auction_prices': [],
        'report_date': date.today().isoformat(),
    }

    # Try Indiana MARS auction for goat prices
    if MARS_API_KEY:
        try:
            data = fetch_mars(REPORT_IN_AUCTION, MARS_API_KEY)
            results = data.get('results', []) if isinstance(data, dict) else []
            for row in results:
                commodity = str(row.get('commodity', '')).strip().lower()
                if 'goat' in commodity:
                    avg_price = parse_number(row.get('avg_price_cwt') or row.get('weighted_avg_cwt'))
                    if avg_price > 0:
                        result['auction_prices'].append({
                            'class': row.get('class', ''),
                            'price_cwt': avg_price,
                            'head_count': parse_number(row.get('head_count', 0)),
                        })
                        result['live_prices']['auction'] = avg_price

            print(f"  Goat auction: {len(result['auction_prices'])} price points")
        except Exception as e:
            print(f"  WARNING: Goat auction fetch failed: {e}")

    if not result['live_prices'] and not result['cutout_prices']:
        print(f"  Goat: no data available — using lamb prices (comparable species)")
        lamb_data = fetch_lamb_data()
        result['dressed_prices'] = lamb_data.get('dressed_prices', {})
        result['cutout_prices'] = lamb_data.get('cutout_prices', {})
        result['report_date'] = lamb_data.get('report_date', result['report_date'])
        result['data_source_note'] = 'lamb proxy'

    return result


# ─── Unified Fetch ───────────────────────────────────────────────────────

def fetch_all_market_data(species_list=None):
    """Fetch market data for all (or selected) species.

    Returns: dict {species: market_data_dict}
    """
    if species_list is None:
        species_list = ['cattle', 'pork', 'lamb', 'goat']

    fetchers = {
        'cattle': fetch_cattle_data,
        'pork': fetch_pork_data,
        'lamb': fetch_lamb_data,
        'goat': fetch_goat_data,
    }

    results = {}
    for species in species_list:
        if species in fetchers:
            print(f"\n── Fetching {species.upper()} ──")
            results[species] = fetchers[species]()

    return results


# ─── Database Persistence ────────────────────────────────────────────────

def save_to_database(conn, market_data):
    """Write fetched market data to weekly_market_prices table.

    Computes floor (auction value) and cutout value for each species.
    """
    from config import PROCESSING_RATES

    # Typical live weights and dressing %
    typical = {
        'cattle': {'live': 1300, 'dress': 0.60},
        'pork':   {'live': 275,  'dress': 0.72},
        'lamb':   {'live': 115,  'dress': 0.50},
        'goat':   {'live': 90,   'dress': 0.50},
    }

    rows_written = 0
    cur = conn.cursor()

    for species, mdata in market_data.items():
        report_date = mdata.get('report_date')
        if not report_date:
            print(f"  {species}: no report date, skipping DB write")
            continue

        # Normalize date format (USDA uses MM/DD/YYYY)
        if '/' in report_date:
            parts = report_date.split('/')
            if len(parts) == 3:
                report_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"

        t = typical.get(species, {'live': 0, 'dress': 0.60})
        live_wt = t['live']
        dress_pct = t['dress']
        hanging_wt = live_wt * dress_pct

        # Processing cost estimate (avg from config)
        proc = PROCESSING_RATES.get(species, {})
        proc_cost = proc.get('kill_fee', 0) + proc.get('fab_cost_per_lb', 0) * hanging_wt

        # Live price (floor basis)
        live_cwt = None
        for key in ('fed_cattle', 'national', 'auction'):
            if key in mdata.get('live_prices', {}):
                live_cwt = mdata['live_prices'][key]
                break

        # Dressed price
        dressed_cwt = None
        for key in ('fed_cattle', 'gross_carcass', 'net_carcass'):
            if key in mdata.get('dressed_prices', {}):
                dressed_cwt = mdata['dressed_prices'][key]
                break

        # Floor = auction value of whole animal
        floor = round(live_wt * live_cwt / 100, 2) if live_cwt else None

        # Cutout value (avg $/cwt across all cuts × hanging weight)
        cutout_cwt = None
        cutout_total = None
        grades = list(mdata.get('cutout_prices', {}).keys())
        if not grades:
            grades = ['standard']

        for grade in grades:
            cuts = mdata.get('cutout_prices', {}).get(grade, {})
            if not cuts:
                continue

            # Compute avg cutout $/cwt
            vals = []
            for v in cuts.values():
                if isinstance(v, dict):
                    vals.append(v.get('price_cwt', 0))
                else:
                    vals.append(v)
            vals = [v for v in vals if v > 0]

            if not vals:
                continue

            avg_cwt = sum(vals) / len(vals)

            # For cattle, compute weighted cutout from IMPS yields
            if species == 'cattle' and grade in ('choice', 'select', 'prime'):
                weighted_total = 0
                total_yield = 0
                for imps_code, price_cwt in cuts.items():
                    if imps_code in SUBPRIMAL_YIELDS:
                        yield_pct = SUBPRIMAL_YIELDS[imps_code][1]
                        cut_wt = hanging_wt * yield_pct / 100
                        cut_value = cut_wt * price_cwt / 100
                        weighted_total += cut_value
                        total_yield += yield_pct
                if weighted_total > 0:
                    cutout_total = round(weighted_total, 2)
                    cutout_cwt = round(weighted_total / hanging_wt * 100, 2)

            if cutout_cwt is None:
                cutout_cwt = round(avg_cwt, 2)
                cutout_total = round(hanging_wt * avg_cwt / 100, 2)

            cur.execute("""
                INSERT INTO weekly_market_prices
                    (report_date, species, quality_grade,
                     live_price_cwt, dressed_price_cwt, floor_whole_animal,
                     cutout_value_cwt, cutout_whole_animal,
                     typical_live_weight, typical_hanging_weight, processor_cost_est,
                     data_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_date, species, quality_grade) DO UPDATE SET
                    live_price_cwt = EXCLUDED.live_price_cwt,
                    dressed_price_cwt = EXCLUDED.dressed_price_cwt,
                    floor_whole_animal = EXCLUDED.floor_whole_animal,
                    cutout_value_cwt = EXCLUDED.cutout_value_cwt,
                    cutout_whole_animal = EXCLUDED.cutout_whole_animal,
                    typical_live_weight = EXCLUDED.typical_live_weight,
                    typical_hanging_weight = EXCLUDED.typical_hanging_weight,
                    processor_cost_est = EXCLUDED.processor_cost_est,
                    data_source = EXCLUDED.data_source
            """, (
                report_date, species, grade,
                live_cwt, dressed_cwt, floor,
                cutout_cwt, cutout_total,
                live_wt, hanging_wt, round(proc_cost, 2),
                'USDA DataMart + MARS' + (' (lamb proxy)' if mdata.get('data_source_note') == 'lamb proxy' else ''),
            ))
            rows_written += 1

    conn.commit()
    print(f"\n  Wrote {rows_written} rows to weekly_market_prices")
    return rows_written


# ─── CLI ─────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    species_arg = None
    if '--species' in sys.argv:
        idx = sys.argv.index('--species')
        if idx + 1 < len(sys.argv):
            s = sys.argv[idx + 1]
            species_arg = [s] if s != 'all' else None

    print("═══ Terra Mensa Market Data Fetcher ═══")
    data = fetch_all_market_data(species_arg)

    print("\n═══ Summary ═══")
    for species, mdata in data.items():
        print(f"\n{species.upper()} (report date: {mdata.get('report_date', 'N/A')})")
        if mdata.get('live_prices'):
            for k, v in mdata['live_prices'].items():
                print(f"  Live ({k}): ${v}/cwt")
        if mdata.get('dressed_prices'):
            for k, v in mdata['dressed_prices'].items():
                print(f"  Dressed ({k}): ${v}/cwt")
        for grade, cuts in mdata.get('cutout_prices', {}).items():
            if cuts:
                vals = []
                for v in cuts.values():
                    if isinstance(v, dict):
                        vals.append(v.get('price_cwt', 0))
                    else:
                        vals.append(v)
                avg = sum(vals) / len(vals) if vals else 0
                print(f"  Cutout ({grade}): {len(cuts)} cuts, avg ${avg:.2f}/cwt")

    # Write to database if --save flag
    if '--save' in sys.argv:
        from optimizer_config import get_connection
        use_supabase = '--supabase' in sys.argv
        conn = get_connection(use_supabase=use_supabase)
        save_to_database(conn, data)
        conn.close()
    else:
        print("\n  (Use --save [--supabase] to write to database)")
