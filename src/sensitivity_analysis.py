#!/usr/bin/env python3
"""Sensitivity analysis: vary annual PO volume from 50% to 150% of baseline.

Reports lead times, matching/utilization rates, and animal usage per run.
"""
import random
import sys
from datetime import date

from db import get_connection
from simulate_michiana import (
    clean_all, create_processors, create_farms, create_customers,
    create_animals, create_purchase_orders, replenish_animals,
    compute_fulfillment_times, _animal_counter, _po_counter,
)
import simulate_michiana
from optimizer import run_optimizer
from seed_optimizer import seed_all
from statistics import mean

BASELINE_PO_PER_MONTH = 9
ANIMAL_TARGET = 200
MONTHS = 12
ALL_SPECIES = ['cattle', 'pork', 'lamb']


def run_scenario(label, po_per_month):
    """Run one full 12-month simulation and return metrics."""
    simulate_michiana._animal_counter = 0
    simulate_michiana._po_counter = 0
    random.seed(2026)

    clean_all()
    seed_all()

    create_processors()
    create_farms()
    customers = create_customers(150)

    start_date = date(2026, 1, 1)
    create_animals(ANIMAL_TARGET, ref_date=start_date)

    total_animals_used = 0
    total_hw = 0.0
    util_pcts = []

    for month_idx in range(MONTHS):
        month_num = 1 + month_idx % 12
        year = 2026 + month_idx // 12
        order_day = random.randint(1, 25)
        order_date = date(year, month_num, order_day)
        run_date = date(year, month_num, 28)

        replenish_animals(ANIMAL_TARGET, ref_date=order_date)

        for j in range(po_per_month):
            day = min(28, 1 + int((j / po_per_month) * 25))
            po_date = date(year, month_num, day)
            create_purchase_orders(customers, n=1, order_date=po_date, quiet=True)

        new_so_ids = []
        for sp in ALL_SPECIES:
            result = run_optimizer(sp, dry_run=False)
            if result['status'] == 'success':
                total_animals_used += result['animals_selected']
                total_hw += result['total_hanging_weight']
                util_pcts.append(result['avg_utilization_pct'])
                for r in result['results']:
                    new_so_ids.append(r['order']['order_number'])

        if new_so_ids:
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE slaughter_orders SET created_at = %s
                        WHERE order_number = ANY(%s)
                    """, (run_date, new_so_ids))
                conn.commit()
            finally:
                conn.close()

    # Collect metrics
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM purchase_orders")
            total_pos = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM purchase_orders WHERE status != 'cancelled'")
            active_pos = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM slaughter_orders")
            total_sos = cur.fetchone()[0]

            # POs with allocations (fulfilled tracking via slaughter_order_allocations)
            cur.execute("""
                SELECT COUNT(DISTINCT po_number) FROM slaughter_order_allocations
            """)
            allocated_pos = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM farmer_inventory WHERE status = 'reserved'")
            reserved = cur.fetchone()[0]
    finally:
        conn.close()

    fulfillments = compute_fulfillment_times()
    avg_lead = mean([f['days_to_fulfill'] for f in fulfillments]) if fulfillments else 0
    min_lead = min([f['days_to_fulfill'] for f in fulfillments]) if fulfillments else 0
    max_lead = max([f['days_to_fulfill'] for f in fulfillments]) if fulfillments else 0
    avg_util = mean(util_pcts) if util_pcts else 0
    po_fill_rate = (len(fulfillments) / active_pos * 100) if active_pos > 0 else 0

    return {
        'label': label,
        'po_per_month': po_per_month,
        'total_pos': total_pos,
        'pos_fulfilled': len(fulfillments),
        'po_fill_rate': po_fill_rate,
        'total_animals': reserved,
        'total_hw': total_hw,
        'avg_util': avg_util,
        'avg_lead': avg_lead,
        'min_lead': min_lead,
        'max_lead': max_lead,
    }


def main():
    import io
    import contextlib

    scenarios = []
    for pct in range(50, 160, 10):
        po_per_month = max(1, round(BASELINE_PO_PER_MONTH * pct / 100))
        label = f"{pct}%"
        print(f"\n{'='*60}")
        print(f"  Running scenario: {label} ({po_per_month} POs/month, ~{po_per_month*12}/year)")
        print(f"{'='*60}")

        with contextlib.redirect_stdout(io.StringIO()):
            result = run_scenario(label, po_per_month)

        scenarios.append(result)
        print(f"  Done: {result['total_pos']} POs, {result['pos_fulfilled']} fulfilled, "
              f"util={result['avg_util']:.1f}%, lead={result['avg_lead']:.1f}d")

    print(f"\n\n{'='*90}")
    print("  SENSITIVITY ANALYSIS — PO Volume vs Lead Time & Utilization")
    print(f"{'='*90}")
    print(f"\n  {'Volume':>7s}  {'PO/Mo':>5s}  {'POs':>5s}  {'Filled':>6s}  "
          f"{'PO Fill%':>8s}  "
          f"{'Animals':>7s}  {'Avg Util%':>9s}  "
          f"{'Avg Lead':>8s}  {'Min':>4s}  {'Max':>4s}")
    print(f"  {'-'*75}")

    for s in scenarios:
        print(f"  {s['label']:>7s}  {s['po_per_month']:5d}  {s['total_pos']:5d}  "
              f"{s['pos_fulfilled']:6d}  {s['po_fill_rate']:7.1f}%  "
              f"{s['total_animals']:7d}  {s['avg_util']:8.1f}%  "
              f"{s['avg_lead']:7.1f}d  {s['min_lead']:4d}  {s['max_lead']:4d}")

    print()


if __name__ == '__main__':
    main()
