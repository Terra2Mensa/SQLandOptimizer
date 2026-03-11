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

from db import get_connection, get_pending_demand, get_available_animals
from optimizer import (run_optimizer, assemble_pos, evaluate_trigger,
                       get_pending_pos_for_assembly,
                       DEFAULT_FULLNESS_THRESHOLD, DEFAULT_MAX_WAIT_DAYS)
from optimizer_db import (get_all_processors, get_slaughter_orders_by_run, get_grade_hierarchy,
                          get_slaughter_order, record_actual_cuts, get_actual_cuts,
                          finalize_slaughter_order, generate_po_invoice)
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
        pending_pos = get_pending_pos_for_assembly(sp)
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

        # Fullness distribution
        fullness_buckets = {'100%': 0, '75-99%': 0, '50-74%': 0, '25-49%': 0}
        for asm in assemblies:
            pct = asm.fullness * 100
            if pct >= 100:
                fullness_buckets['100%'] += 1
            elif pct >= 75:
                fullness_buckets['75-99%'] += 1
            elif pct >= 50:
                fullness_buckets['50-74%'] += 1
            else:
                fullness_buckets['25-49%'] += 1

        print(f"\n  Fullness distribution:")
        for bucket, cnt in fullness_buckets.items():
            if cnt > 0:
                print(f"    {bucket:10s}  {cnt} assemblies")

        # Detail
        print(f"\n  {'#':>3s} {'Full%':>6s} {'Slots':>7s} {'Trigger':>8s} {'Age':>5s} {'POs':30s} {'Portions'}")
        print(f"  {'-'*80}")
        for i, asm in enumerate(assemblies, 1):
            trigger = evaluate_trigger(asm, today,
                                       DEFAULT_FULLNESS_THRESHOLD,
                                       DEFAULT_MAX_WAIT_DAYS)
            oldest = asm.oldest_order_date
            age = (today - oldest).days if oldest else 0
            portions = [po['carcass_portion'] for po in asm.pos]
            po_str = ', '.join(asm.po_numbers)
            if len(po_str) > 30:
                po_str = po_str[:27] + '...'
            print(f"  {i:3d} {asm.fullness*100:5.0f}%  "
                  f"{asm.slot_label:>7s}  "
                  f"{trigger:>8s} {age:4d}d  "
                  f"{po_str:30s} {', '.join(portions)}")

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


def cmd_record_weights(args):
    """Record actual post-processing weights for a slaughter order."""
    order_number = args.order_number
    so = get_slaughter_order(order_number)
    if not so:
        print(f"  ERROR: Slaughter order '{order_number}' not found")
        return

    print(f"\n  Record Actual Weights — {order_number}")
    print(f"  Species: {so['species']}  Animal: {so['animal_id']}  "
          f"Processor: {so['processor_key']}")
    print(f"  Estimated HW: {float(so['estimated_hanging_weight'] or 0):.1f} lbs")
    print(f"  Status: {so['status']}")

    lines = so.get('lines', [])
    if not lines:
        print("  No lines found for this order.")
        return

    # Check for existing actual weights
    existing = get_actual_cuts(order_number)
    existing_by_code = {e['cut_code']: float(e['actual_lbs']) for e in existing}

    if args.variance is not None:
        # Apply uniform variance to all cuts
        var = args.variance / 100.0
        cuts = []
        for ln in lines:
            est = float(ln['total_lbs'])
            actual = round(est * (1 + var), 2)
            cuts.append({'cut_code': ln['cut_code'], 'actual_lbs': actual})
        print(f"\n  Applying {args.variance:+.1f}% variance to all {len(cuts)} cuts...")
    else:
        # Interactive entry
        print(f"\n  Enter actual weight for each cut (Enter to accept estimate, 'q' to abort):")
        print(f"  {'Cut Code':20s} {'Estimated':>10s} {'Current':>10s} {'Actual':>10s}")
        print(f"  {'─' * 55}")

        cuts = []
        for ln in lines:
            cut_code = ln['cut_code']
            est = float(ln['total_lbs'])
            cur_val = existing_by_code.get(cut_code)
            cur_str = f"{cur_val:.2f}" if cur_val is not None else "—"

            prompt = f"  {cut_code:20s} {est:10.2f} {cur_str:>10s}  > "
            try:
                val = input(prompt).strip()
            except (EOFError, KeyboardInterrupt):
                print("\n  Aborted.")
                return
            if val.lower() == 'q':
                print("  Aborted.")
                return
            if val == '':
                actual = cur_val if cur_val is not None else est
            else:
                try:
                    actual = float(val)
                except ValueError:
                    print(f"    Invalid number '{val}', using estimate")
                    actual = est
            cuts.append({'cut_code': cut_code, 'actual_lbs': round(actual, 2)})

    record_actual_cuts(order_number, cuts, recorded_by=args.recorded_by)

    # Show result
    total_est = sum(float(ln['total_lbs']) for ln in lines)
    total_act = sum(c['actual_lbs'] for c in cuts)
    delta_pct = (total_act - total_est) / total_est * 100 if total_est > 0 else 0

    print(f"\n  Recorded {len(cuts)} cuts for {order_number}")
    print(f"  Estimated HW: {total_est:,.1f} lbs")
    print(f"  Actual HW:    {total_act:,.1f} lbs  ({delta_pct:+.1f}%)")
    print()


def cmd_finalize(args):
    """Finalize a slaughter order: reconcile actual weights to PO allocations."""
    order_number = args.order_number
    try:
        result = finalize_slaughter_order(order_number)
    except ValueError as e:
        print(f"  ERROR: {e}")
        return

    print(f"\n  Finalized: {order_number} ({result['species']})")
    print(f"  Estimated HW: {result['total_estimated_hw']:,.1f} lbs")
    print(f"  Actual HW:    {result['total_actual_hw']:,.1f} lbs  "
          f"({result['yield_variance_pct']:+.1f}%)")

    print(f"\n  {'Cut Code':20s} {'Est':>8s} {'Actual':>8s} {'Delta':>7s} "
          f"{'→PO':>8s} {'→LOR':>8s} {'PO#':15s}")
    print(f"  {'─' * 80}")
    for ln in result['lines']:
        delta = f"{ln['delta_pct']:+.1f}%"
        po = ln['po_number'] or '(LOR)'
        print(f"  {ln['cut_code']:20s} {ln['estimated_lbs']:8.2f} {ln['actual_lbs']:8.2f} "
              f"{delta:>7s} {ln['actual_to_po']:8.2f} {ln['actual_to_lor']:8.2f} {po:15s}")

    if result['po_finals']:
        print(f"\n  PO Final Totals:")
        for po_num, total in result['po_finals'].items():
            print(f"    {po_num}: ${total:,.2f}")

    if args.invoice:
        for po_num in result['affected_pos']:
            try:
                inv = generate_po_invoice(po_num, due_days=args.due_days)
                print(f"\n  Invoice generated: {inv['invoice_id']}")
                print(f"    PO: {inv['po_number']}  Customer: {inv['customer_id']}")
                print(f"    Estimated: ${inv['total_estimated']:,.2f}")
                print(f"    Actual:    ${inv['total_actual']:,.2f}  "
                      f"(variance: ${inv['variance']:+,.2f})")
                print(f"    Due: {inv['due_date']}")
            except ValueError as e:
                print(f"    Invoice skipped for {po_num}: {e}")
    print()


def cmd_invoice(args):
    """Generate an invoice for a fulfilled PO."""
    try:
        inv = generate_po_invoice(args.po_number, due_days=args.due_days)
    except ValueError as e:
        print(f"  ERROR: {e}")
        return

    print(f"\n  Invoice: {inv['invoice_id']}")
    print(f"  PO: {inv['po_number']}  Customer: {inv['customer_id']}")
    print(f"  Estimated total: ${inv['total_estimated']:,.2f}")
    print(f"  Actual total:    ${inv['total_actual']:,.2f}")
    print(f"  Variance:        ${inv['variance']:+,.2f}")
    print(f"  Due date:        {inv['due_date']}")

    print(f"\n  {'Cut':20s} {'Ordered':>10s} {'Actual':>10s} {'$/lb':>8s} {'Total':>10s}")
    print(f"  {'─' * 62}")
    for ln in inv['lines']:
        qty = float(ln['quantity_lbs'])
        act = float(ln['actual_lbs'] or ln['quantity_lbs'])
        ppl = float(ln['price_per_lb'])
        total = float(ln['line_total'])
        print(f"  {ln['cut_code']:20s} {qty:10.1f} {act:10.1f} {ppl:8.4f} {total:10.2f}")
    print()


def main():
    parser = argparse.ArgumentParser(description='Carcass Optimizer CLI (PO Assembly Engine)')
    sub = parser.add_subparsers(dest='command')

    # optimize
    p_opt = sub.add_parser('optimize', help='Run the PO assembly optimizer')
    p_opt.add_argument('--species', default='cattle',
                       choices=SUPPORTED_SPECIES + ['all'],
                       help='Species to optimize (default: cattle)')
    p_opt.add_argument('--dry-run', action='store_true', default=True,
                       help='Show results without writing to DB (default)')
    p_opt.add_argument('--commit', action='store_true',
                       help='Write results to DB')

    # assemblies
    p_asm = sub.add_parser('assemblies', help='Show current PO assembly state (dry-run)')
    p_asm.add_argument('--species', default='cattle',
                       choices=SUPPORTED_SPECIES + ['all'],
                       help='Species (default: cattle)')

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

    # record-weights
    p_rw = sub.add_parser('record-weights',
                          help='Record actual post-processing weights for a slaughter order')
    p_rw.add_argument('order_number', help='Slaughter order number (e.g. SO-xxxx-001)')
    p_rw.add_argument('--variance', type=float, default=None,
                      help='Apply uniform %% variance to all cuts (e.g. -3.5 for 3.5%% under)')
    p_rw.add_argument('--recorded-by', default=None, help='Who is recording')

    # finalize
    p_fin = sub.add_parser('finalize',
                           help='Finalize a slaughter order (reconcile actual → PO)')
    p_fin.add_argument('order_number', help='Slaughter order number')
    p_fin.add_argument('--invoice', action='store_true',
                       help='Generate invoices for affected POs')
    p_fin.add_argument('--due-days', type=int, default=30,
                       help='Invoice payment terms in days (default: 30)')

    # invoice
    p_inv2 = sub.add_parser('invoice', help='Generate invoice for a fulfilled PO')
    p_inv2.add_argument('po_number', help='Purchase order number')
    p_inv2.add_argument('--due-days', type=int, default=30,
                        help='Payment terms in days (default: 30)')

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
