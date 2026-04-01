"""
Compare v1 (greedy FIFO) vs v2 (FFD + MIP) on identical synthetic data.
No database writes — pure in-memory simulation.

Usage: python3 optimizer_compare.py [--supabase]
"""

import sys
from optimizer_config import (
    get_connection, load_optimizer_config, get_config,
    SHARE_FRACTIONS, DRESS_PCT,
)
from optimizer_v2 import (
    get_pending_pos, get_available_inventory, get_processors_for_species,
    bulk_load_distances, lookup_distance, get_customer_profiles_bulk,
    aggregate_pos_ffd, compute_batch_processor_cost, solve_joint_assignment,
)
import copy


def aggregate_pos_fifo(pos_list, fill_threshold):
    """Original v1 FIFO First Fit batching (for comparison)."""
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
    """Original v1 greedy assignment (for comparison)."""
    inv = list(inventory)  # copy
    assignments = []

    for b, batch in enumerate(batches):
        if not inv:
            break
        animal = inv.pop(0)

        best_cost = float('inf')
        best_proc_idx = None
        best_breakdown = None

        for p, proc in enumerate(processors):
            cost, breakdown = compute_batch_processor_cost(
                batch, animal, proc, customers, distances, config
            )
            if cost is not None and cost < best_cost:
                best_cost = cost
                best_proc_idx = p
                best_breakdown = breakdown

        if best_proc_idx is not None:
            assignments.append((b, inventory.index(animal), best_proc_idx, best_cost, best_breakdown))
        else:
            inv.insert(0, animal)  # return animal

    return assignments


def run_comparison(use_supabase=False):
    conn = get_connection(use_supabase=use_supabase)
    config = load_optimizer_config(conn)
    fill_threshold = get_config(config, 'fill_threshold', 1.0)

    print("═══ Optimizer Comparison: v1 (Greedy FIFO) vs v2 (FFD + MIP) ═══\n")

    for species in ['cattle', 'pork', 'lamb', 'goat']:
        pending = get_pending_pos(conn, species)
        if not pending:
            continue

        inventory = get_available_inventory(conn, species)
        processors = get_processors_for_species(conn, species)
        if not inventory or not processors:
            continue

        # Pre-load distances and customers
        all_ids = set()
        for a in inventory:
            all_ids.add(str(a['profile_id']))
        for p in processors:
            all_ids.add(str(p['processor_id']))
        for po in pending:
            all_ids.add(str(po['profile_id']))

        distances = bulk_load_distances(conn, all_ids)
        cust_ids = set(str(po['profile_id']) for po in pending)
        customers = get_customer_profiles_bulk(conn, cust_ids)

        fractions = [SHARE_FRACTIONS.get(po['share'], 0) for po in pending]
        print(f"── {species.upper()} ──")
        print(f"  {len(pending)} POs (fraction: {sum(fractions):.3f}), {len(inventory)} animals, {len(processors)} processors")

        # ── v1: FIFO First Fit + Greedy ──
        batches_v1, rem_v1 = aggregate_pos_fifo(pending, fill_threshold)
        assign_v1 = greedy_assign(batches_v1, inventory, processors, customers, distances, config)
        cost_v1 = sum(a[3] for a in assign_v1)
        pos_v1 = sum(len(batches_v1[a[0]]) for a in assign_v1)

        # ── v2: FFD + MIP ──
        batches_v2, rem_v2 = aggregate_pos_ffd(pending, fill_threshold)
        assign_v2 = solve_joint_assignment(batches_v2, inventory, processors, customers, distances, config)
        cost_v2 = sum(a[3] for a in assign_v2)
        pos_v2 = sum(len(batches_v2[a[0]]) for a in assign_v2)

        # ── Compare ──
        print(f"\n  {'Metric':<30} {'v1 (Greedy)':<18} {'v2 (MIP)':<18} {'Delta':<15}")
        print(f"  {'─'*30} {'─'*18} {'─'*18} {'─'*15}")
        print(f"  {'Batches formed':<30} {len(batches_v1):<18} {len(batches_v2):<18} {len(batches_v2)-len(batches_v1):<+15}")
        print(f"  {'POs remaining (unfilled)':<30} {len(rem_v1):<18} {len(rem_v2):<18} {len(rem_v2)-len(rem_v1):<+15}")
        print(f"  {'Batches assigned':<30} {len(assign_v1):<18} {len(assign_v2):<18} {len(assign_v2)-len(assign_v1):<+15}")
        print(f"  {'POs confirmed':<30} {pos_v1:<18} {pos_v2:<18} {pos_v2-pos_v1:<+15}")
        print(f"  {'Total cost':<30} {'${:.2f}'.format(cost_v1):<18} {'${:.2f}'.format(cost_v2):<18}", end="")
        if cost_v1 > 0:
            delta_pct = (cost_v2 - cost_v1) / cost_v1 * 100
            print(f" {delta_pct:+.1f}%")
        else:
            print()
        if assign_v1:
            print(f"  {'Avg cost/order':<30} {'${:.2f}'.format(cost_v1/len(assign_v1)):<18}", end="")
        else:
            print(f"  {'Avg cost/order':<30} {'N/A':<18}", end="")
        if assign_v2:
            print(f"{'${:.2f}'.format(cost_v2/len(assign_v2)):<18}")
        else:
            print(f"{'N/A':<18}")

        # Show processor utilization
        proc_load_v1 = {}
        proc_load_v2 = {}
        for a in assign_v1:
            name = processors[a[2]]['company_name']
            proc_load_v1[name] = proc_load_v1.get(name, 0) + 1
        for a in assign_v2:
            name = processors[a[2]]['company_name']
            proc_load_v2[name] = proc_load_v2.get(name, 0) + 1

        all_proc_names = sorted(set(list(proc_load_v1.keys()) + list(proc_load_v2.keys())))
        if all_proc_names:
            print(f"\n  Processor utilization:")
            for name in all_proc_names:
                v1_load = proc_load_v1.get(name, 0)
                v2_load = proc_load_v2.get(name, 0)
                print(f"    {name:<30} v1={v1_load:<5} v2={v2_load:<5}")
        print()

    conn.close()


if __name__ == '__main__':
    use_supabase = '--supabase' in sys.argv
    run_comparison(use_supabase=use_supabase)
