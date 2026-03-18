#!/usr/bin/env python3
"""CLI for the carcass optimizer (PO Assembly Engine).

Usage:
    python3 optimizer_cli.py optimize --species cattle --dry-run
    python3 optimizer_cli.py optimize --species cattle --commit
    python3 optimizer_cli.py optimize --species all --dry-run
    python3 optimizer_cli.py assemblies --species cattle
    python3 optimizer_cli.py demand [--species cattle]
    python3 optimizer_cli.py inventory [--species cattle]
    python3 optimizer_cli.py status
"""
import argparse
import sys
from datetime import date
from statistics import mean

from db import get_connection, get_available_animals, get_pending_pos
from optimizer import (run_optimizer, assemble_pos, evaluate_trigger,
                       DEFAULT_FULLNESS_THRESHOLD, DEFAULT_MAX_WAIT_DAYS)
from optimizer_db import get_grade_hierarchy, get_slaughter_order
import psycopg2.extras


SUPPORTED_SPECIES = ['cattle', 'pork', 'lamb', 'goat', 'chicken']


def cmd_optimize(args):
    species_list = SUPPORTED_SPECIES if args.species == 'all' else [args.species]
    dry_run = not args.commit

    all_results = {}
    for sp in species_list:
        result = run_optimizer(sp, dry_run=dry_run)
        all_results[sp] = result

    if len(species_list) > 1:
        print("\n" + "=" * 60)
        print("  MULTI-SPECIES SUMMARY")
        print("=" * 60)
        for sp, r in all_results.items():
            if r['status'] == 'success':
                print(f"  {sp:10s}  {r['animals_selected']} orders  "
                      f"{r['total_hanging_weight']:,.0f} lbs  "
                      f"util={r['avg_utilization_pct']:.1f}%  "
                      f"triggered={r['assemblies_triggered']}  "
                      f"held={r['assemblies_held']} ({r['held_po_count']} POs)")
            elif r['status'] == 'held':
                print(f"  {sp:10s}  held — {r['assemblies_held']} assemblies, "
                      f"{r['held_po_count']} POs waiting  "
                      f"avg fullness={r['avg_fullness']*100:.0f}%")
            else:
                print(f"  {sp:10s}  {r['status']}")
        print()


def cmd_assemblies(args):
    """Dry-run the assembly phase only — show current PO assembly state."""
    species_list = SUPPORTED_SPECIES if args.species == 'all' else [args.species]
    today = date.today()

    for sp in species_list:
        pending_pos = get_pending_pos(sp)
        if not pending_pos:
            print(f"\n  {sp.upper()}: No pending POs")
            continue

        grade_hierarchy = get_grade_hierarchy(sp)
        assemblies = assemble_pos(pending_pos, sp, grade_hierarchy)

        ready = []
        forced = []
        held = []
        for asm in assemblies:
            trigger = evaluate_trigger(asm, today,
                                       DEFAULT_FULLNESS_THRESHOLD,
                                       DEFAULT_MAX_WAIT_DAYS)
            if trigger == 'ready':
                ready.append(asm)
            elif trigger == 'forced':
                forced.append(asm)
            else:
                held.append(asm)

        print(f"\n  {sp.upper()} — {len(assemblies)} assemblies from {len(pending_pos)} POs")
        print(f"  Ready: {len(ready)}  Forced: {len(forced)}  Held: {len(held)}")

        fullness_buckets = {'100%': 0, '75-99%': 0, '50-74%': 0, '25-49%': 0, '<25%': 0}
        for asm in assemblies:
            pct = asm.fullness * 100
            if pct >= 100:
                fullness_buckets['100%'] += 1
            elif pct >= 75:
                fullness_buckets['75-99%'] += 1
            elif pct >= 50:
                fullness_buckets['50-74%'] += 1
            elif pct >= 25:
                fullness_buckets['25-49%'] += 1
            else:
                fullness_buckets['<25%'] += 1

        print(f"\n  Fullness distribution:")
        for bucket, cnt in fullness_buckets.items():
            if cnt > 0:
                print(f"    {bucket:10s}  {cnt} assemblies")

        print(f"\n  {'#':>3s} {'Full%':>6s} {'Trigger':>8s} {'Age':>5s} {'POs':30s} {'Shares'}")
        print(f"  {'-'*80}")
        for i, asm in enumerate(assemblies, 1):
            trigger = evaluate_trigger(asm, today,
                                       DEFAULT_FULLNESS_THRESHOLD,
                                       DEFAULT_MAX_WAIT_DAYS)
            oldest = asm.oldest_order_date
            age = (today - oldest).days if oldest else 0
            shares = [po['share'] for po in asm.pos]
            po_str = ', '.join(asm.po_numbers)
            if len(po_str) > 30:
                po_str = po_str[:27] + '...'
            print(f"  {i:3d} {asm.fullness*100:5.0f}%  "
                  f"{trigger:>8s} {age:4d}d  "
                  f"{po_str:30s} {', '.join(shares)}")

    print()


def cmd_demand(args):
    """Show pending demand aggregated by share size."""
    species_list = SUPPORTED_SPECIES if args.species == 'all' else [args.species]

    for sp in species_list:
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT share, COUNT(*) as po_count
                    FROM purchase_orders
                    WHERE species = %s AND status = 'pending'
                    GROUP BY share ORDER BY share
                """, (sp,))
                rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            print(f"\n  {sp.upper()}: No pending demand")
            continue

        total = sum(r['po_count'] for r in rows)
        print(f"\n  {sp.upper()} — {total} pending POs")
        print(f"  {'Share':12s} {'Count':>6s}")
        print(f"  {'-'*20}")
        for r in rows:
            print(f"  {r['share']:12s} {r['po_count']:6d}")
    print()


def cmd_inventory(args):
    species_list = SUPPORTED_SPECIES if args.species == 'all' else [args.species]

    for sp in species_list:
        animals = get_available_animals(sp)
        if not animals:
            print(f"\n  {sp.upper()}: No available animals")
            continue

        print(f"\n  {sp.upper()} — {len(animals)} available")
        print(f"  {'Animal ID':15s} {'Farmer':20s} "
              f"{'Wt':>7s} {'Grade':10s}")
        print(f"  {'-'*55}")
        for a in animals[:20]:
            print(f"  {a['animal_id']:15s} {(a.get('farmer_name') or a['profile_id']):20s} "
                  f"{float(a.get('live_weight_est') or 0):7.0f} "
                  f"{(a.get('expected_grade') or ''):10s}")
        if len(animals) > 20:
            print(f"  ... and {len(animals) - 20} more")
    print()


def cmd_status(args):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT species, status, COUNT(*) as cnt
                FROM purchase_orders
                GROUP BY species, status ORDER BY species, status
            """)
            po_rows = cur.fetchall()

            cur.execute("""
                SELECT species, status, COUNT(*) as cnt
                FROM farmer_inventory
                GROUP BY species, status ORDER BY species, status
            """)
            inv_rows = cur.fetchall()

            cur.execute("""
                SELECT order_number, species, animal_id, profile_id,
                       status, created_at
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
                  f"{'Processor':15s} {'Status':10s}")
            print(f"  {'-'*65}")
            for r in so_rows:
                print(f"  {r['order_number']:18s} {r['species']:8s} "
                      f"{(r['animal_id'] or ''):12s} {(r['profile_id'] or ''):15s} "
                      f"{r['status']:10s}")
        else:
            print("  (none)")
        print()

    finally:
        conn.close()


def cmd_record_weights(args):
    """Stub — not yet implemented (deferred to Phase C)."""
    print("  record-weights: not yet implemented (deferred to Phase C — invoices & cut sheets)")


def cmd_finalize(args):
    """Stub — not yet implemented (deferred to Phase C)."""
    print("  finalize: not yet implemented (deferred to Phase C — invoices & cut sheets)")


def cmd_invoice(args):
    """Stub — not yet implemented (deferred to Phase C)."""
    print("  invoice: not yet implemented (deferred to Phase C — invoices & cut sheets)")


def main():
    parser = argparse.ArgumentParser(description='Carcass Optimizer CLI (PO Assembly Engine)')
    sub = parser.add_subparsers(dest='command')

    p_opt = sub.add_parser('optimize', help='Run the PO assembly optimizer')
    p_opt.add_argument('--species', default='cattle',
                       choices=SUPPORTED_SPECIES + ['all'],
                       help='Species to optimize (default: cattle)')
    p_opt.add_argument('--dry-run', action='store_true', default=True,
                       help='Show results without writing to DB (default)')
    p_opt.add_argument('--commit', action='store_true',
                       help='Write results to DB')

    p_asm = sub.add_parser('assemblies', help='Show current PO assembly state (dry-run)')
    p_asm.add_argument('--species', default='cattle',
                       choices=SUPPORTED_SPECIES + ['all'],
                       help='Species (default: cattle)')

    p_dem = sub.add_parser('demand', help='Show pending demand')
    p_dem.add_argument('--species', default='all',
                       choices=SUPPORTED_SPECIES + ['all'])

    p_inv = sub.add_parser('inventory', help='Show available inventory')
    p_inv.add_argument('--species', default='all',
                       choices=SUPPORTED_SPECIES + ['all'])

    sub.add_parser('status', help='Show system status')

    p_rw = sub.add_parser('record-weights',
                          help='Record actual post-processing weights (not yet implemented)')
    p_rw.add_argument('order_number', help='Slaughter order number')

    p_fin = sub.add_parser('finalize',
                           help='Finalize a slaughter order (not yet implemented)')
    p_fin.add_argument('order_number', help='Slaughter order number')

    p_inv2 = sub.add_parser('invoice', help='Generate invoice (not yet implemented)')
    p_inv2.add_argument('po_number', help='Purchase order number')

    args = parser.parse_args()

    if args.command == 'optimize':
        cmd_optimize(args)
    elif args.command == 'assemblies':
        cmd_assemblies(args)
    elif args.command == 'demand':
        cmd_demand(args)
    elif args.command == 'inventory':
        cmd_inventory(args)
    elif args.command == 'status':
        cmd_status(args)
    elif args.command == 'record-weights':
        cmd_record_weights(args)
    elif args.command == 'finalize':
        cmd_finalize(args)
    elif args.command == 'invoice':
        cmd_invoice(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
