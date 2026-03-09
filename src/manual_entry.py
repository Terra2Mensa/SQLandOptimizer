#!/usr/bin/env python3
"""
Manual Price Entry for Chicken and Goat
=======================================
Since chicken and goat have no structured USDA API data, this module
provides manual cut-level price entry with JSON file storage and
optional DB persistence.

Usage:
  python3 manual_entry.py chicken set breast_bnls 3.45
  python3 manual_entry.py chicken set-all
  python3 manual_entry.py goat list
  python3 manual_entry.py chicken import prices.csv
  python3 manual_entry.py chicken history breast_bnls
"""

import argparse
import csv
import json
import os
import sys
from datetime import date, datetime

from config import CHICKEN_CUT_YIELDS, GOAT_CUT_YIELDS

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
SPECIES_CUTS = {
    'chicken': CHICKEN_CUT_YIELDS,
    'goat': GOAT_CUT_YIELDS,
}


def _prices_path(species: str) -> str:
    return os.path.join(DATA_DIR, f"{species}_prices.json")


def load_prices(species: str) -> dict:
    path = _prices_path(species)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"date": "", "source": "manual", "cuts": {}}


def save_prices(species: str, prices: dict):
    prices['date'] = date.today().isoformat()
    path = _prices_path(species)
    with open(path, 'w') as f:
        json.dump(prices, f, indent=2)
    print(f"Prices saved to {path}")


# ------------------------------------------------------------------
# Commands
# ------------------------------------------------------------------

def cmd_set(args):
    cuts_cfg = SPECIES_CUTS[args.species]
    if args.cut_code not in cuts_cfg:
        print(f"Unknown cut '{args.cut_code}'. Valid: {list(cuts_cfg.keys())}")
        sys.exit(1)

    desc, yield_pct, category = cuts_cfg[args.cut_code]
    prices = load_prices(args.species)
    prices['cuts'][args.cut_code] = {
        'description': desc,
        'price_per_lb': args.price,
        'yield_pct': yield_pct,
        'category': category,
    }
    save_prices(args.species, prices)
    print(f"Set {args.species} {args.cut_code} ({desc}) = ${args.price:.2f}/lb")

    # Save to DB if available
    try:
        from db import save_manual_price
        save_manual_price(args.species, args.cut_code, desc,
                          args.price, yield_pct, 'manual')
    except Exception:
        pass


def cmd_set_all(args):
    cuts_cfg = SPECIES_CUTS[args.species]
    prices = load_prices(args.species)

    print(f"\nEnter prices for {args.species} cuts ($/lb). Press Enter to skip.\n")
    for code, (desc, yield_pct, category) in sorted(cuts_cfg.items()):
        current = prices.get('cuts', {}).get(code, {}).get('price_per_lb', '')
        current_str = f" [current: ${current:.2f}]" if current else ""
        try:
            val = input(f"  {desc:<30} ({code}){current_str}: $")
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            return
        if val.strip():
            try:
                price = float(val.strip())
                prices.setdefault('cuts', {})[code] = {
                    'description': desc,
                    'price_per_lb': price,
                    'yield_pct': yield_pct,
                    'category': category,
                }
                try:
                    from db import save_manual_price
                    save_manual_price(args.species, code, desc, price, yield_pct, 'manual')
                except Exception:
                    pass
            except ValueError:
                print(f"    Skipping — invalid number: {val}")

    save_prices(args.species, prices)


def cmd_list(args):
    prices = load_prices(args.species)
    cuts_cfg = SPECIES_CUTS[args.species]

    print(f"\n{args.species.upper()} PRICES")
    if prices.get('date'):
        print(f"Last updated: {prices['date']}")
    print(f"{'=' * 65}")
    print(f"{'Code':<16} {'Description':<30} {'$/lb':>8} {'Yield%':>7}")
    print(f"{'-' * 65}")

    for code, (desc, yield_pct, category) in sorted(cuts_cfg.items()):
        entry = prices.get('cuts', {}).get(code, {})
        price = entry.get('price_per_lb', '')
        price_str = f"${price:.2f}" if price else "---"
        print(f"{code:<16} {desc:<30} {price_str:>8} {yield_pct:>6.1f}%")
    print()

    # Check staleness
    if prices.get('date'):
        age = (date.today() - date.fromisoformat(prices['date'])).days
        if age > 7:
            print(f"  WARNING: Prices are {age} days old. Consider updating.")


def cmd_import(args):
    cuts_cfg = SPECIES_CUTS[args.species]
    prices = load_prices(args.species)

    count = 0
    with open(args.file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get('cut_code', '').strip()
            price_str = row.get('price_per_lb', '').strip()
            if code not in cuts_cfg:
                print(f"  Skipping unknown cut: {code}")
                continue
            try:
                price = float(price_str)
            except ValueError:
                print(f"  Skipping invalid price for {code}: {price_str}")
                continue

            desc, yield_pct, category = cuts_cfg[code]
            prices.setdefault('cuts', {})[code] = {
                'description': desc,
                'price_per_lb': price,
                'yield_pct': yield_pct,
                'category': category,
            }
            count += 1
            try:
                from db import save_manual_price
                save_manual_price(args.species, code, desc, price, yield_pct, 'csv_import')
            except Exception:
                pass

    save_prices(args.species, prices)
    print(f"Imported {count} prices from {args.file}")


def cmd_history(args):
    try:
        from db import get_connection
        import psycopg2.extras
        conn = get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT entry_date, price_per_lb, source
                FROM manual_species_prices
                WHERE species = %s AND cut_code = %s
                ORDER BY entry_date DESC
                LIMIT 20
            """, (args.species, args.cut_code))
            rows = cur.fetchall()
            if not rows:
                print(f"No history for {args.species} {args.cut_code}")
                return
            print(f"\nPrice history: {args.species} / {args.cut_code}")
            print(f"{'Date':<14} {'$/lb':>8} {'Source':<12}")
            print(f"{'-' * 36}")
            for row in rows:
                print(f"{str(row['entry_date']):<14} ${row['price_per_lb']:>7.2f} "
                      f"{row['source']:<12}")
        conn.close()
    except Exception as e:
        print(f"Cannot read history: {e}")


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Manual price entry for chicken and goat")
    parser.add_argument('species', choices=['chicken', 'goat'])
    sub = parser.add_subparsers(dest='command')

    p = sub.add_parser('set', help='Set a single cut price')
    p.add_argument('cut_code')
    p.add_argument('price', type=float)

    sub.add_parser('set-all', help='Enter all cut prices interactively')
    sub.add_parser('list', help='List current prices')

    p = sub.add_parser('import', help='Import prices from CSV')
    p.add_argument('file', help='CSV file (columns: cut_code, price_per_lb)')

    p = sub.add_parser('history', help='Show price history from DB')
    p.add_argument('cut_code')

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        'set': cmd_set,
        'set-all': cmd_set_all,
        'list': cmd_list,
        'import': cmd_import,
        'history': cmd_history,
    }
    commands[args.command](args)


if __name__ == '__main__':
    main()
