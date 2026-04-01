"""
Benchmark v1 vs v2 on synthetic in-memory data.
No database access needed — pure algorithmic comparison.

Usage: python3 optimizer_benchmark.py
"""

import random
import uuid
import time
from datetime import datetime, timedelta
from optimizer_config import SHARE_FRACTIONS, DRESS_PCT, get_config
from optimizer_v2 import aggregate_pos_ffd, compute_batch_processor_cost, solve_joint_assignment, solve_unified_mip


# ─── Synthetic Data Generation ───────────────────────────────────────────

def gen_uuid():
    return str(uuid.uuid4())

# 6 farms, 5 processors, 40 customers — mirrors real Michiana data
FARMS = [
    {'id': gen_uuid(), 'profile_id': gen_uuid(), 'lat': 41.65, 'lng': -86.10},
    {'id': gen_uuid(), 'profile_id': gen_uuid(), 'lat': 41.70, 'lng': -86.20},
    {'id': gen_uuid(), 'profile_id': gen_uuid(), 'lat': 41.58, 'lng': -85.95},
    {'id': gen_uuid(), 'profile_id': gen_uuid(), 'lat': 41.75, 'lng': -86.05},
    {'id': gen_uuid(), 'profile_id': gen_uuid(), 'lat': 41.55, 'lng': -86.30},
    {'id': gen_uuid(), 'profile_id': gen_uuid(), 'lat': 41.68, 'lng': -85.85},
]

PROCESSORS = [
    {'processor_id': gen_uuid(), 'company_name': 'Proc-A', 'lat': 41.67, 'lng': -86.25,
     'kill_fee': 100, 'fab_cost_per_lb': 0.85, 'shrink_pct': 0, 'daily_capacity_head': 5},
    {'processor_id': gen_uuid(), 'company_name': 'Proc-B', 'lat': 41.66, 'lng': -86.16,
     'kill_fee': 120, 'fab_cost_per_lb': 0.75, 'shrink_pct': 0, 'daily_capacity_head': 3},
    {'processor_id': gen_uuid(), 'company_name': 'Proc-C', 'lat': 41.58, 'lng': -85.83,
     'kill_fee': 90, 'fab_cost_per_lb': 0.95, 'shrink_pct': 0, 'daily_capacity_head': 4},
    {'processor_id': gen_uuid(), 'company_name': 'Proc-D', 'lat': 41.75, 'lng': -86.11,
     'kill_fee': 110, 'fab_cost_per_lb': 0.80, 'shrink_pct': 0, 'daily_capacity_head': 6},
    {'processor_id': gen_uuid(), 'company_name': 'Proc-E', 'lat': 41.83, 'lng': -86.25,
     'kill_fee': 85, 'fab_cost_per_lb': 1.00, 'shrink_pct': 0, 'daily_capacity_head': 3},
]

def haversine_miles(lat1, lng1, lat2, lng2):
    """Approximate distance in miles using haversine."""
    import math
    R = 3959  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def generate_scenario(n_customers=40, n_pos=50, n_inventory=20, species='cattle'):
    """Generate a complete synthetic scenario."""
    random.seed(42)  # Reproducible

    # Customers scattered around South Bend
    customers = {}
    customer_ids = []
    for i in range(n_customers):
        cid = gen_uuid()
        lat = 41.55 + random.uniform(0, 0.30)
        lng = -86.40 + random.uniform(0, 0.55)
        customers[cid] = {'id': cid, 'latitude': lat, 'longitude': lng, 'first_name': f'Cust-{i+1}'}
        customer_ids.append(cid)

    # Purchase orders with realistic share distribution
    shares = ['half', 'half', 'half', 'quarter', 'quarter', 'quarter', 'quarter',
              'eighth', 'eighth', 'whole']
    pos = []
    for i in range(n_pos):
        po = {
            'po_number': f'PO-BENCH-{i+1:03d}',
            'profile_id': random.choice(customer_ids),
            'species': species,
            'share': random.choice(shares),
            'note': None,
            'created_at': datetime.now() - timedelta(days=random.randint(0, 30)),
        }
        pos.append(po)

    # Inventory
    inventory = []
    for i in range(n_inventory):
        farm = random.choice(FARMS)
        inv = {
            'id': gen_uuid(),
            'profile_id': farm['profile_id'],
            'species': species,
            'live_weight_est': random.uniform(1050, 1350) if species == 'cattle' else random.uniform(240, 310),
            'description': f'Animal-{i+1}',
            'company_name': f'Farm-{FARMS.index(farm)+1}',
            'first_name': None,
            'latitude': farm['lat'],
            'longitude': farm['lng'],
        }
        inventory.append(inv)

    # Pre-compute all pairwise distances
    all_ids = set()
    for c in customers.values():
        all_ids.add(c['id'])
    for f in FARMS:
        all_ids.add(f['profile_id'])
    for p in PROCESSORS:
        all_ids.add(p['processor_id'])

    # Build lat/lng lookup
    latlng = {}
    for c in customers.values():
        latlng[c['id']] = (c['latitude'], c['longitude'])
    for f in FARMS:
        latlng[f['profile_id']] = (f['lat'], f['lng'])
    for p in PROCESSORS:
        latlng[p['processor_id']] = (p['lat'], p['lng'])

    distances = {}
    id_list = list(all_ids)
    for i, a in enumerate(id_list):
        for b in id_list[i+1:]:
            origin = min(a, b)
            dest = max(a, b)
            if origin in latlng and dest in latlng:
                d = haversine_miles(latlng[origin][0], latlng[origin][1],
                                    latlng[dest][0], latlng[dest][1])
                distances[(origin, dest)] = d

    return pos, inventory, customers, distances


# ─── FIFO First Fit (v1) ────────────────────────────────────────────────

def aggregate_fifo(pos_list, fill_threshold):
    """v1 FIFO First Fit."""
    batches = []
    whole_pos = [po for po in pos_list if po['share'] in ('whole', 'uncut')]
    partial_pos = [po for po in pos_list if po['share'] not in ('whole', 'uncut')]

    for po in whole_pos:
        batches.append([po])

    current_batch = []
    current_sum = 0.0
    remaining = []

    for po in partial_pos:
        fraction = SHARE_FRACTIONS.get(po['share'], 0)
        if fraction <= 0:
            continue
        current_batch.append(po)
        current_sum += fraction
        if current_sum >= fill_threshold:
            batches.append(current_batch)
            current_batch = []
            current_sum = 0.0

    remaining = current_batch
    return batches, remaining


def greedy_assign(batches, inventory, processors, customers, distances, config):
    """v1 greedy per-batch assignment."""
    inv = list(inventory)
    assignments = []

    for b, batch in enumerate(batches):
        if not inv:
            break
        animal = inv.pop(0)
        a_idx = inventory.index(animal)

        best_cost = float('inf')
        best_p = None
        best_bd = None

        for p, proc in enumerate(processors):
            cost, bd = compute_batch_processor_cost(batch, animal, proc, customers, distances, config)
            if cost is not None and cost < best_cost:
                best_cost = cost
                best_p = p
                best_bd = bd

        if best_p is not None:
            assignments.append((b, a_idx, best_p, best_cost, best_bd))
        else:
            inv.insert(0, animal)

    return assignments


# ─── Run Benchmark ───────────────────────────────────────────────────────

def run_benchmark():
    config = {
        'fill_threshold': 1.0,
        'max_farmer_distance_miles': 50,
        'max_customer_distance_miles': 50,
        'farmer_transport_per_mile': 2,
        'customer_transport_per_mile': 1,
    }
    fill_threshold = 1.0

    scenarios = [
        ('Small (20 POs, 8 animals)', 40, 20, 8),
        ('Medium (50 POs, 20 animals)', 40, 50, 20),
        ('Large (100 POs, 40 animals)', 60, 100, 40),
        ('Stressed (100 POs, 15 animals)', 60, 100, 15),
    ]

    print("═══ Optimizer Benchmark: v1 (Greedy FIFO) vs v2 (FFD + MIP) ═══\n")

    for name, n_cust, n_pos, n_inv in scenarios:
        print(f"── Scenario: {name} ──")
        pos, inventory, customers, distances = generate_scenario(n_cust, n_pos, n_inv)

        fractions = [SHARE_FRACTIONS.get(po['share'], 0) for po in pos]
        print(f"  {n_pos} POs (fraction: {sum(fractions):.2f}), {n_inv} animals, 5 processors")

        share_counts = {}
        for po in pos:
            share_counts[po['share']] = share_counts.get(po['share'], 0) + 1
        print(f"  Share mix: {share_counts}")

        # ── v1 ──
        batches_v1, rem_v1 = aggregate_fifo(pos, fill_threshold)
        assign_v1 = greedy_assign(batches_v1, inventory, PROCESSORS, customers, distances, config)
        cost_v1 = sum(a[3] for a in assign_v1)
        pos_v1 = sum(len(batches_v1[a[0]]) for a in assign_v1)

        # ── v2 ──
        batches_v2, rem_v2 = aggregate_pos_ffd(pos, fill_threshold)
        assign_v2 = solve_joint_assignment(batches_v2, inventory, PROCESSORS, customers, distances, config)
        cost_v2 = sum(a[3] for a in assign_v2)
        pos_v2 = sum(len(batches_v2[a[0]]) for a in assign_v2)

        # ── Results ──
        print(f"\n  {'Metric':<30} {'v1 (Greedy)':<18} {'v2 (MIP)':<18} {'Delta':<15}")
        print(f"  {'─'*30} {'─'*18} {'─'*18} {'─'*15}")
        print(f"  {'Batches formed':<30} {len(batches_v1):<18} {len(batches_v2):<18} {len(batches_v2)-len(batches_v1):<+15}")
        print(f"  {'POs remaining':<30} {len(rem_v1):<18} {len(rem_v2):<18} {len(rem_v2)-len(rem_v1):<+15}")
        print(f"  {'Batches assigned':<30} {len(assign_v1):<18} {len(assign_v2):<18} {len(assign_v2)-len(assign_v1):<+15}")
        print(f"  {'POs confirmed':<30} {pos_v1:<18} {pos_v2:<18} {pos_v2-pos_v1:<+15}")
        print(f"  {'Total cost':<30} {'${:,.2f}'.format(cost_v1):<18} {'${:,.2f}'.format(cost_v2):<18}", end="")
        if cost_v1 > 0:
            delta_pct = (cost_v2 - cost_v1) / cost_v1 * 100
            print(f" {delta_pct:+.1f}%")
        else:
            print()
        if assign_v1:
            print(f"  {'Avg cost/order':<30} {'${:,.2f}'.format(cost_v1/len(assign_v1)):<18}", end="")
        else:
            print(f"  {'Avg cost/order':<30} {'N/A':<18}", end="")
        if assign_v2:
            print(f"{'${:,.2f}'.format(cost_v2/len(assign_v2)):<18}")
        else:
            print(f"{'N/A':<18}")

        # Processor utilization
        proc_load_v1 = {}
        proc_load_v2 = {}
        for a in assign_v1:
            n = PROCESSORS[a[2]]['company_name']
            proc_load_v1[n] = proc_load_v1.get(n, 0) + 1
        for a in assign_v2:
            n = PROCESSORS[a[2]]['company_name']
            proc_load_v2[n] = proc_load_v2.get(n, 0) + 1

        print(f"\n  Processor loads:")
        for p in PROCESSORS:
            n = p['company_name']
            cap = p['daily_capacity_head']
            v1 = proc_load_v1.get(n, 0)
            v2 = proc_load_v2.get(n, 0)
            v1_bar = '█' * v1
            v2_bar = '█' * v2
            print(f"    {n} (cap={cap}): v1={v1:>2} {v1_bar:<10} v2={v2:>2} {v2_bar:<10}", end="")
            if v1 > cap:
                print(" ⚠️ v1 OVER CAPACITY", end="")
            if v2 > cap:
                print(" ⚠️ v2 OVER CAPACITY", end="")
            print()

        print()


def run_unified_benchmark():
    """Compare Phase 1 (FFD+MIP) vs Phase 2 (Unified MIP) on same data."""
    config = {
        'fill_threshold': 1.0,
        'max_farmer_distance_miles': 50,
        'max_customer_distance_miles': 50,
        'farmer_transport_per_mile': 2,
        'customer_transport_per_mile': 1,
        'w_cost': 1.0,
        'w_avg_wait': 0.3,
        'w_max_wait': 0.5,
        'w_util_balance': 0.2,
        'w_geo_penalty': 0.1,
    }

    # Smaller scenarios for unified MIP (more variables)
    scenarios = [
        ('Small (15 POs, 6 animals)', 20, 15, 6),
        ('Medium (25 POs, 10 animals)', 30, 25, 10),
        ('Large (40 POs, 15 animals)', 40, 40, 15),
    ]

    print("═══ Phase 1 vs Phase 2 (Unified MIP) Benchmark ═══\n")

    for name, n_cust, n_pos, n_inv in scenarios:
        print(f"── Scenario: {name} ──")
        pos, inventory, customers, distances = generate_scenario(n_cust, n_pos, n_inv)

        fractions = [SHARE_FRACTIONS.get(po['share'], 0) for po in pos]
        print(f"  {n_pos} POs (fraction: {sum(fractions):.2f}), {n_inv} animals, 5 processors")

        share_counts = {}
        for po in pos:
            share_counts[po['share']] = share_counts.get(po['share'], 0) + 1
        print(f"  Share mix: {share_counts}")

        # ── Phase 1: FFD + joint MIP ──
        print(f"\n  --- Phase 1 (FFD + MIP) ---")
        t1 = time.time()
        batches_p1, rem_p1 = aggregate_pos_ffd(pos, 1.0)
        assign_p1 = solve_joint_assignment(batches_p1, inventory, PROCESSORS, customers, distances, config)
        t1_elapsed = time.time() - t1
        cost_p1 = sum(a[3] for a in assign_p1)
        pos_p1 = sum(len(batches_p1[a[0]]) for a in assign_p1)

        # ── Phase 2: Unified MIP ──
        print(f"\n  --- Phase 2 (Unified MIP) ---")
        t2 = time.time()
        results_p2, rem_p2 = solve_unified_mip(pos, inventory, PROCESSORS, customers, distances, config)
        t2_elapsed = time.time() - t2
        cost_p2 = sum(r['cost'] for r in results_p2)
        pos_p2 = sum(len(r['batch_pos']) for r in results_p2)

        # ── Compare ──
        print(f"\n  {'Metric':<30} {'Phase 1':<18} {'Phase 2 (Unified)':<18} {'Delta':<15}")
        print(f"  {'─'*30} {'─'*18} {'─'*18} {'─'*15}")
        print(f"  {'Batches assigned':<30} {len(assign_p1):<18} {len(results_p2):<18} {len(results_p2)-len(assign_p1):<+15}")
        print(f"  {'POs confirmed':<30} {pos_p1:<18} {pos_p2:<18} {pos_p2-pos_p1:<+15}")
        print(f"  {'POs remaining':<30} {len(rem_p1):<18} {len(rem_p2):<18} {len(rem_p2)-len(rem_p1):<+15}")
        print(f"  {'Total cost':<30} {'${:,.2f}'.format(cost_p1):<18} {'${:,.2f}'.format(cost_p2):<18}", end="")
        if cost_p1 > 0:
            delta_pct = (cost_p2 - cost_p1) / cost_p1 * 100
            print(f" {delta_pct:+.1f}%")
        else:
            print()
        print(f"  {'Solve time':<30} {t1_elapsed:.2f}s{'':<12} {t2_elapsed:.2f}s")

        # Processor loads
        proc_load_p1 = {}
        proc_load_p2 = {}
        for a in assign_p1:
            n = PROCESSORS[a[2]]['company_name']
            proc_load_p1[n] = proc_load_p1.get(n, 0) + 1
        for r in results_p2:
            n = r['processor']['company_name']
            proc_load_p2[n] = proc_load_p2.get(n, 0) + 1

        print(f"\n  Processor loads:")
        for p in PROCESSORS:
            n = p['company_name']
            cap = p['daily_capacity_head']
            l1 = proc_load_p1.get(n, 0)
            l2 = proc_load_p2.get(n, 0)
            print(f"    {n} (cap={cap}): P1={l1:>2}  P2={l2:>2}")
        print()


if __name__ == '__main__':
    import sys
    if '--unified' in sys.argv:
        run_unified_benchmark()
    else:
        run_benchmark()
