#!/usr/bin/env python3
"""CLI for the carcass optimizer.

Usage:
    python3 optimizer_cli.py optimize --species cattle --dry-run
    python3 optimizer_cli.py optimize --species cattle --commit
    python3 optimizer_cli.py optimize --species all --dry-run
    python3 optimizer_cli.py demand [--species cattle]
    python3 optimizer_cli.py inventory [--species cattle]
    python3 optimizer_cli.py status
"""
import argparse
import sys

from db import get_connection, get_pending_demand, get_available_animals
from optimizer import run_optimizer
from optimizer_db import get_all_processors, get_slaughter_orders_by_run
import psycopg2.extras


SUPPORTED_SPECIES = ['cattle', 'pork', 'lamb', 'goat', 'chicken']


def cmd_optimize(args):
    species_list = SUPPORTED_SPECIES if args.species == 'all' else [args.species]
    dry_run = not args.commit

    all_results = {}
    for sp in species_list:
        result = run_optimizer(sp, dry_run=dry_run)
        all_results[sp] = result

    # Summary across species
    if len(species_list) > 1:
        print("\n" + "=" * 60)
        print("  MULTI-SPECIES SUMMARY")
        print("=" * 60)
        for sp, r in all_results.items():
            if r['status'] == 'success':
                print(f"  {sp:10s}  {r['animals_selected']} animals  "
                      f"{r['total_hanging_weight']:,.0f} lbs  "
                      f"util={r['avg_utilization_pct']:.1f}%")
            else:
                print(f"  {sp:10s}  {r['status']}")
        print()


def cmd_demand(args):
    species_list = SUPPORTED_SPECIES if args.species == 'all' else [args.species]

    for sp in species_list:
        demand = get_pending_demand(sp)
        if not demand:
            print(f"\n  {sp.upper()}: No pending demand")
            continue

        total = sum(float(d['pending_lbs']) for d in demand)
        print(f"\n  {sp.upper()} — {len(demand)} cuts, {total:,.1f} lbs pending")
        print(f"  {'Cut Code':12s} {'Primal':12s} {'Lbs':>10s} {'Lines':>6s}")
        print(f"  {'-'*42}")
        for d in demand:
            print(f"  {d['cut_code']:12s} {(d.get('primal') or ''):12s} "
                  f"{float(d['pending_lbs']):10.1f} {d['line_count']:6d}")
    print()


def cmd_inventory(args):
    species_list = SUPPORTED_SPECIES if args.species == 'all' else [args.species]

    for sp in species_list:
        animals = get_available_animals(sp)
        if not animals:
            print(f"\n  {sp.upper()}: No available animals")
            continue

        print(f"\n  {sp.upper()} — {len(animals)} available")
        print(f"  {'Animal ID':15s} {'Farmer':15s} {'Breed':12s} "
              f"{'Wt':>7s} {'Grade':8s} {'Sex':8s}")
        print(f"  {'-'*67}")
        for a in animals[:20]:  # cap display at 20
            print(f"  {a['animal_id']:15s} {a['farmer_id']:15s} "
                  f"{(a.get('breed') or ''):12s} "
                  f"{float(a.get('live_weight_est') or 0):7.0f} "
                  f"{(a.get('quality_grade_est') or ''):8s} "
                  f"{(a.get('sex') or ''):8s}")
        if len(animals) > 20:
            print(f"  ... and {len(animals) - 20} more")
    print()


def cmd_status(args):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # PO summary
            cur.execute("""
                SELECT species, status, COUNT(*) as cnt
                FROM purchase_orders
                GROUP BY species, status ORDER BY species, status
            """)
            po_rows = cur.fetchall()

            # Inventory summary
            cur.execute("""
                SELECT species, status, COUNT(*) as cnt
                FROM farmer_inventory
                GROUP BY species, status ORDER BY species, status
            """)
            inv_rows = cur.fetchall()

            # Recent slaughter orders
            cur.execute("""
                SELECT order_number, species, animal_id, processor_key,
                       pct_allocated_to_orders, optimizer_score, status,
                       created_at
                FROM slaughter_orders
                ORDER BY created_at DESC LIMIT 10
            """)
            so_rows = cur.fetchall()

        print("\n  PURCHASE ORDERS")
        print(f"  {'Species':10s} {'Status':12s} {'Count':>6s}")
        print(f"  {'-'*30}")
        for r in po_rows:
            print(f"  {r['species']:10s} {r['status']:12s} {r['cnt']:6d}")

        print(f"\n  INVENTORY")
        print(f"  {'Species':10s} {'Status':12s} {'Count':>6s}")
        print(f"  {'-'*30}")
        for r in inv_rows:
            print(f"  {r['species']:10s} {r['status']:12s} {r['cnt']:6d}")

        print(f"\n  RECENT SLAUGHTER ORDERS")
        if so_rows:
            print(f"  {'Order #':18s} {'Species':8s} {'Animal':12s} "
                  f"{'Processor':12s} {'Util%':>6s} {'Score':>7s} {'Status':10s}")
            print(f"  {'-'*75}")
            for r in so_rows:
                print(f"  {r['order_number']:18s} {r['species']:8s} "
                      f"{r['animal_id']:12s} {r['processor_key']:12s} "
                      f"{float(r['pct_allocated_to_orders'] or 0):6.1f} "
                      f"{float(r['optimizer_score'] or 0):7.1f} "
                      f"{r['status']:10s}")
        else:
            print("  (none)")
        print()

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Carcass Optimizer CLI')
    sub = parser.add_subparsers(dest='command')

    # optimize
    p_opt = sub.add_parser('optimize', help='Run the carcass optimizer')
    p_opt.add_argument('--species', default='cattle',
                       choices=SUPPORTED_SPECIES + ['all'],
                       help='Species to optimize (default: cattle)')
    p_opt.add_argument('--dry-run', action='store_true', default=True,
                       help='Show results without writing to DB (default)')
    p_opt.add_argument('--commit', action='store_true',
                       help='Write results to DB')

    # demand
    p_dem = sub.add_parser('demand', help='Show pending demand')
    p_dem.add_argument('--species', default='all',
                       choices=SUPPORTED_SPECIES + ['all'])

    # inventory
    p_inv = sub.add_parser('inventory', help='Show available inventory')
    p_inv.add_argument('--species', default='all',
                       choices=SUPPORTED_SPECIES + ['all'])

    # status
    sub.add_parser('status', help='Show system status')

    args = parser.parse_args()

    if args.command == 'optimize':
        cmd_optimize(args)
    elif args.command == 'demand':
        cmd_demand(args)
    elif args.command == 'inventory':
        cmd_inventory(args)
    elif args.command == 'status':
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
