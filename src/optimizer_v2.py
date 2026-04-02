"""
Terra Mensa Optimizer v2
MIP-based optimizer using PuLP + CBC solver.

Phase 1: FFD batching + joint processor assignment MIP
Phase 2: Unified bin-packing + assignment + multi-objective weights

Usage: python3 optimizer_v2.py [--supabase] [--dry-run] [--mode unified|phase1]
"""

import sys
import os
import math
import psycopg2
import psycopg2.extras
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
import pulp

from optimizer_config import (
    get_connection, load_optimizer_config, get_config,
    SHARE_FRACTIONS, SPECIES_LIST, get_dress_pct,
)

load_dotenv()

# CBC solver path (Homebrew on Apple Silicon)
CBC_PATH = '/opt/homebrew/bin/cbc'

# Try to load advanced features (Phase 4) — graceful fallback if not available
try:
    from optimizer_advanced import AdvancedFeatures
    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False


# ─── Database Queries ────────────────────────────────────────────────────

def get_pending_pos(conn, species):
    """Get all pending POs for a species, ordered by creation date (FIFO)."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT po_number, profile_id, species, share, note, created_at
            FROM purchase_orders
            WHERE species = %s AND status = 'pending' AND inventory_id IS NULL
            ORDER BY created_at ASC
        """, (species,))
        return [dict(row) for row in cur.fetchall()]


def get_available_inventory(conn, species):
    """Get available farmer inventory for a species."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT fi.id, fi.profile_id, fi.species, fi.live_weight_est, fi.description,
                   p.company_name, p.first_name, p.latitude, p.longitude
            FROM farmer_inventory fi
            JOIN profiles p ON p.id = fi.profile_id
            WHERE fi.species = %s AND fi.status = 'available'
            ORDER BY fi.created_at ASC
        """, (species,))
        return [dict(row) for row in cur.fetchall()]


def get_processors_for_species(conn, species):
    """Get all processors with costs, per-processor radius, and state for a species."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT p.id as processor_id, p.company_name, p.latitude, p.longitude,
                   p.state_code,
                   pc.kill_fee, pc.fab_cost_per_lb, pc.shrink_pct, pc.daily_capacity_head,
                   pc.farmer_radius_miles, pc.customer_radius_miles
            FROM profiles p
            JOIN processor_costs pc ON pc.profile_id = p.id AND pc.species = %s
            WHERE p.type = 'processor'
        """, (species,))
        return [dict(row) for row in cur.fetchall()]


def bulk_load_distances(conn, profile_ids):
    """Load all distances between a set of profile IDs in one query.

    Returns dict: (min_id, max_id) -> distance_miles
    """
    if not profile_ids:
        return {}
    ids = list(set(str(pid) for pid in profile_ids))
    with conn.cursor() as cur:
        cur.execute("""
            SELECT origin_profile_id, destination_profile_id, distance_miles
            FROM distance_matrix
            WHERE origin_profile_id = ANY(%s::uuid[]) AND destination_profile_id = ANY(%s::uuid[])
        """, (ids, ids))
        distances = {}
        for row in cur.fetchall():
            key = (str(row[0]), str(row[1]))
            distances[key] = float(row[2])
        return distances


def lookup_distance(distances, id_a, id_b):
    """Look up distance from pre-loaded distance dict."""
    origin = min(str(id_a), str(id_b))
    dest = max(str(id_a), str(id_b))
    return distances.get((origin, dest))


def get_customer_profiles_bulk(conn, profile_ids):
    """Get multiple customer profiles in one query."""
    if not profile_ids:
        return {}
    ids = list(set(str(pid) for pid in profile_ids))
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, latitude, longitude, first_name FROM profiles WHERE id = ANY(%s::uuid[])", (ids,))
        return {str(row['id']): dict(row) for row in cur.fetchall()}


# ─── FFD Batching ────────────────────────────────────────────────────────

def aggregate_pos_ffd(pos_list, fill_threshold):
    """Group POs into batches using First Fit Decreasing algorithm.

    Pass 1: Whole/uncut POs each get a dedicated animal (unchanged).
    Pass 2: Partial POs sorted by share size DESCENDING (FFD), then packed.

    FFD achieves 11/9 * OPT + 4 bound vs FIFO's 17/10 * OPT.
    The key improvement: larger shares placed first leave fewer gaps,
    allowing smaller shares to fill them more efficiently.

    Returns: (batches, remaining) where remaining are unfilled POs.
    """
    batches = []

    # Pass 1: Whole/uncut POs — each gets its own dedicated animal
    whole_pos = [po for po in pos_list if po['share'] in ('whole', 'uncut')]
    partial_pos = [po for po in pos_list if po['share'] not in ('whole', 'uncut')]

    for po in whole_pos:
        batches.append([po])

    if whole_pos:
        print(f"    Pass 1: {len(whole_pos)} whole/uncut POs → {len(whole_pos)} dedicated animals")

    # Pass 2: FFD — sort partials by share fraction DESCENDING
    partial_pos.sort(key=lambda po: SHARE_FRACTIONS.get(po['share'], 0), reverse=True)

    # First Fit Decreasing: try to fit each PO into the first batch with room
    open_batches = []  # list of (batch_list, current_sum)

    for po in partial_pos:
        fraction = SHARE_FRACTIONS.get(po['share'], 0)
        if fraction <= 0:
            continue

        placed = False
        for j, (batch, batch_sum) in enumerate(open_batches):
            if batch_sum + fraction <= 1.0 + 1e-9:  # fits in this batch
                batch.append(po)
                open_batches[j] = (batch, batch_sum + fraction)
                if batch_sum + fraction >= fill_threshold - 1e-9:
                    # Batch is full enough — close it
                    batches.append(batch)
                    open_batches.pop(j)
                placed = True
                break

        if not placed:
            # Start new batch
            if fraction >= fill_threshold - 1e-9:
                batches.append([po])
            else:
                open_batches.append(([po], fraction))

    # Close any remaining batches that meet threshold
    remaining = []
    for batch, batch_sum in open_batches:
        if batch_sum >= fill_threshold - 1e-9:
            batches.append(batch)
        else:
            remaining.extend(batch)

    if partial_pos:
        filled = len(batches) - len(whole_pos)
        print(f"    Pass 2 (FFD): {len(partial_pos)} partial POs → {filled} shared animal(s)")

    if remaining:
        rem_str = [f"{po['po_number']}({po['share']})" for po in remaining]
        rem_sum = sum(SHARE_FRACTIONS.get(po['share'], 0) for po in remaining)
        print(f"    {len(remaining)} partial POs remaining (sum={rem_sum:.3f}): {rem_str}")

    return batches, remaining


# ─── Cost Computation ────────────────────────────────────────────────────

def compute_batch_processor_cost(batch, animal, proc, customers, distances, config):
    """Compute total cost of assigning a batch to a processor.

    Returns (cost, breakdown) or (None, None) if infeasible.
    """
    farmer_id = animal['profile_id']
    proc_id = proc['processor_id']
    # Per-processor radius (falls back to global config)
    max_farmer_dist = float(proc.get('farmer_radius_miles') or 0) or get_config(config, 'max_farmer_distance_miles', 50)
    max_customer_dist = float(proc.get('customer_radius_miles') or 0) or get_config(config, 'max_customer_distance_miles', 30)
    farmer_rate = get_config(config, 'farmer_transport_per_mile', 2)
    customer_rate = get_config(config, 'customer_transport_per_mile', 1)

    live_weight = float(animal.get('live_weight_est') or 0)
    dress_pct = get_dress_pct(config, animal['species'])
    hanging_weight = live_weight * dress_pct

    # Farmer → processor distance
    farmer_dist = lookup_distance(distances, farmer_id, proc_id)
    if farmer_dist is None or farmer_dist > max_farmer_dist:
        return None, None

    # Check all customers within range
    customer_distances = []
    for po in batch:
        cust = customers.get(str(po['profile_id']))
        if not cust:
            return None, None
        cust_dist = lookup_distance(distances, proc_id, cust['id'])
        if cust_dist is None or cust_dist > max_customer_dist:
            return None, None
        customer_distances.append(cust_dist)

    # Calculate costs
    kill_fee = float(proc.get('kill_fee') or 0)
    fab_per_lb = float(proc.get('fab_cost_per_lb') or 0)
    processing_cost = kill_fee + (fab_per_lb * hanging_weight)
    farmer_transport = farmer_dist * farmer_rate
    customer_transport = sum(d * customer_rate for d in customer_distances)
    total_cost = processing_cost + farmer_transport + customer_transport

    breakdown = {
        'processing_cost': round(processing_cost, 2),
        'farmer_transport': round(farmer_transport, 2),
        'customer_transport': round(customer_transport, 2),
        'total_cost': round(total_cost, 2),
        'farmer_distance': farmer_dist,
        'hanging_weight': round(hanging_weight, 1),
    }

    return total_cost, breakdown


# ─── Joint MIP Assignment ────────────────────────────────────────────────

def solve_joint_assignment(batches, inventory, processors, customers, distances, config):
    """Solve the joint batch→animal + batch→processor assignment as a MIP.

    Instead of greedily assigning each batch independently, this solves
    all assignments simultaneously, respecting processor capacity constraints
    and minimizing total system cost.

    Returns: list of (batch_idx, animal_idx, proc_idx, cost, breakdown)
    """
    n_batches = len(batches)
    n_animals = len(inventory)
    n_procs = len(processors)

    if n_batches == 0 or n_animals == 0 or n_procs == 0:
        return []

    # Pre-compute cost matrix: cost[b][a][p] = total cost or None (infeasible)
    print(f"    Building cost matrix: {n_batches} batches × {n_animals} animals × {n_procs} processors")
    cost_matrix = {}
    breakdown_matrix = {}

    for b in range(n_batches):
        for a in range(n_animals):
            for p in range(n_procs):
                cost, breakdown = compute_batch_processor_cost(
                    batches[b], inventory[a], processors[p],
                    customers, distances, config
                )
                if cost is not None:
                    cost_matrix[(b, a, p)] = cost
                    breakdown_matrix[(b, a, p)] = breakdown

    feasible_keys = list(cost_matrix.keys())
    if not feasible_keys:
        print(f"    No feasible assignments found")
        return []

    # Build MIP
    prob = pulp.LpProblem("TerraMessaAssignment", pulp.LpMinimize)

    # Decision variables: x[b,a,p] = 1 if batch b uses animal a at processor p
    x = {}
    for key in feasible_keys:
        x[key] = pulp.LpVariable(f"x_{key[0]}_{key[1]}_{key[2]}", cat='Binary')

    # Objective: maximize assignments first, then minimize cost among those
    # BIG_BONUS ensures assigning one more batch always beats any cost savings
    BIG_BONUS = int(get_config(config, 'mip_assignment_bonus', 100000))
    prob += pulp.lpSum(cost_matrix[key] * x[key] for key in feasible_keys) - \
            BIG_BONUS * pulp.lpSum(x[key] for key in feasible_keys)

    # Constraint 1: Each batch assigned to at most one (animal, processor) pair
    for b in range(n_batches):
        batch_keys = [key for key in feasible_keys if key[0] == b]
        if batch_keys:
            prob += pulp.lpSum(x[key] for key in batch_keys) <= 1

    # Constraint 2: Each animal used at most once
    for a in range(n_animals):
        animal_keys = [key for key in feasible_keys if key[1] == a]
        if animal_keys:
            prob += pulp.lpSum(x[key] for key in animal_keys) <= 1

    # Constraint 3: Processor daily capacity
    for p in range(n_procs):
        capacity = int(processors[p].get('daily_capacity_head') or 999)
        proc_keys = [key for key in feasible_keys if key[2] == p]
        if proc_keys:
            prob += pulp.lpSum(x[key] for key in proc_keys) <= capacity

    # Solve
    prob.solve(pulp.COIN_CMD(msg=0, path=CBC_PATH))

    if prob.status != pulp.constants.LpStatusOptimal:
        print(f"    MIP solver status: {pulp.LpStatus[prob.status]}")
        return []

    # Extract solution
    assignments = []
    for key in feasible_keys:
        if x[key].varValue and x[key].varValue > 0.5:
            b, a, p = key
            assignments.append((b, a, p, cost_matrix[key], breakdown_matrix[key]))

    # Sort by batch index for consistent output
    assignments.sort(key=lambda t: t[0])

    assigned_batches = len(assignments)
    total_cost = sum(a[3] for a in assignments)
    print(f"    MIP solved: {assigned_batches}/{n_batches} batches assigned, total cost=${total_cost:.2f}")

    return assignments


# ─── Unified MIP (Phase 2) ──────────────────────────────────────────────

def solve_unified_mip(pos_list, inventory, processors, customers, distances, config):
    """Unified MIP: simultaneously decides PO→batch assignment AND batch→processor.

    Instead of two phases (FFD batching, then assignment), this single MIP:
    1. Decides which POs go into which batch (bin packing)
    2. Decides which animal + processor handles each batch (assignment)
    3. Optimizes a multi-objective weighted sum (cost, wait time, utilization, geography)

    Variables:
        x[i,b] ∈ {0,1} — PO i assigned to batch b
        y[b,a,p] ∈ {0,1} — batch b uses animal a at processor p
        z[b] ∈ {0,1} — batch b is activated

    Returns: list of dicts with keys: batch_pos, animal, processor, cost, breakdown
    """
    now = datetime.now()
    n_pos = len(pos_list)
    n_animals = len(inventory)
    n_procs = len(processors)

    if n_pos == 0 or n_animals == 0 or n_procs == 0:
        return [], pos_list  # return all POs as remaining

    # Config weights (from optimizer_config table, with defaults)
    w_cost = get_config(config, 'w_cost', 1.0)
    w_avg_wait = get_config(config, 'w_avg_wait', 0.3)
    w_max_wait = get_config(config, 'w_max_wait', 0.5)
    w_util_balance = get_config(config, 'w_util_balance', 0.2)
    w_geo_penalty = get_config(config, 'w_geo_penalty', 0.1)
    fill_threshold = get_config(config, 'fill_threshold', 1.0)
    global_farmer_dist = get_config(config, 'max_farmer_distance_miles', 50)
    global_customer_dist = get_config(config, 'max_customer_distance_miles', 30)
    farmer_rate = get_config(config, 'farmer_transport_per_mile', 2)
    customer_rate = get_config(config, 'customer_transport_per_mile', 1)

    # Build per-processor radius (falls back to global)
    proc_farmer_radius = {}
    proc_customer_radius = {}
    for p, proc in enumerate(processors):
        proc_farmer_radius[p] = float(proc.get('farmer_radius_miles') or 0) or global_farmer_dist
        proc_customer_radius[p] = float(proc.get('customer_radius_miles') or 0) or global_customer_dist

    # ── Pre-compute feasibility and costs ──

    # Separate whole/uncut POs (pre-assigned to dedicated batches, not in MIP)
    whole_pos_list = [po for po in pos_list if po['share'] in ('whole', 'uncut')]
    partial_pos_list = [po for po in pos_list if po['share'] not in ('whole', 'uncut')]
    n_whole = len(whole_pos_list)
    n_partial = len(partial_pos_list)

    # Whole POs get dedicated batches (handled outside MIP for efficiency)
    # We reserve n_whole animals and n_whole processor slots for them

    # Max batches for partials
    partial_fraction = sum(SHARE_FRACTIONS.get(po['share'], 0) for po in partial_pos_list)
    max_batches = min(n_partial, int(math.ceil(partial_fraction)) + 2)
    max_batches = min(max_batches, n_animals - n_whole)  # reserve animals for whole POs
    max_batches = max(max_batches, 0)

    total_cap = sum(int(p.get('daily_capacity_head') or 999) for p in processors)
    max_batches = min(max_batches, total_cap - n_whole)  # reserve proc capacity
    max_batches = max(max_batches, 0)

    print(f"    Unified MIP: {n_whole} whole + {n_partial} partial POs, "
          f"{n_animals} animals, {n_procs} processors, up to {max_batches} partial batches")

    # Pre-compute: which (customer, processor) pairs are feasible? (per-processor radius)
    cust_proc_dist = {}
    for i, po in enumerate(partial_pos_list):
        cid = str(po['profile_id'])
        for p, proc in enumerate(processors):
            pid = proc['processor_id']
            d = lookup_distance(distances, cid, pid)
            if d is not None and d <= proc_customer_radius[p]:
                cust_proc_dist[(i, p)] = d

    # Also for whole POs
    whole_cust_proc_dist = {}
    for i, po in enumerate(whole_pos_list):
        cid = str(po['profile_id'])
        for p, proc in enumerate(processors):
            pid = proc['processor_id']
            d = lookup_distance(distances, cid, pid)
            if d is not None and d <= proc_customer_radius[p]:
                whole_cust_proc_dist[(i, p)] = d

    # Pre-compute: which (farmer/animal, processor) pairs are feasible? (per-processor radius)
    farmer_proc_dist = {}
    for a, animal in enumerate(inventory):
        fid = animal['profile_id']
        for p, proc in enumerate(processors):
            pid = proc['processor_id']
            d = lookup_distance(distances, fid, pid)
            if d is not None and d <= proc_farmer_radius[p]:
                farmer_proc_dist[(a, p)] = d

    # Pre-compute processing cost per (animal, processor)
    proc_cost = {}
    for a, animal in enumerate(inventory):
        live_weight = float(animal.get('live_weight_est') or 0)
        dress_pct = get_dress_pct(config, animal['species'])
        hw = live_weight * dress_pct
        for p, proc in enumerate(processors):
            if (a, p) in farmer_proc_dist:
                kf = float(proc.get('kill_fee') or 0)
                fab = float(proc.get('fab_cost_per_lb') or 0)
                pc = kf + (fab * hw)
                ft = farmer_proc_dist[(a, p)] * farmer_rate
                proc_cost[(a, p)] = (pc, hw, farmer_proc_dist[(a, p)], ft)

    # Pre-compute PO wait times
    wait_days = []
    for po in partial_pos_list:
        created = po.get('created_at')
        if created:
            if hasattr(created, 'date'):
                delta = (now - created.replace(tzinfo=None)).days if created.tzinfo else (now - created).days
            else:
                delta = 0
        else:
            delta = 0
        wait_days.append(max(delta, 0))

    # ── Solve whole POs greedily (simple — each is one batch, one animal) ──
    whole_results = []
    used_animals = set()
    used_proc_slots = defaultdict(int)

    for wi, po in enumerate(whole_pos_list):
        best_cost = float('inf')
        best_result = None
        for a, animal in enumerate(inventory):
            if a in used_animals:
                continue
            for p, proc in enumerate(processors):
                cap = int(proc.get('daily_capacity_head') or 999)
                if used_proc_slots[p] >= cap:
                    continue
                if (a, p) not in proc_cost:
                    continue
                if (wi, p) not in whole_cust_proc_dist:
                    continue
                pc_data = proc_cost[(a, p)]
                ct = whole_cust_proc_dist[(wi, p)] * customer_rate
                total = pc_data[0] + pc_data[3] + ct
                if total < best_cost:
                    best_cost = total
                    best_result = {
                        'batch_pos': [po],
                        'animal': animal,
                        'animal_idx': a,
                        'processor': proc,
                        'proc_idx': p,
                        'cost': round(total, 2),
                        'breakdown': {
                            'processing_cost': round(pc_data[0], 2),
                            'farmer_transport': round(pc_data[3], 2),
                            'customer_transport': round(ct, 2),
                            'total_cost': round(total, 2),
                            'farmer_distance': pc_data[2],
                            'hanging_weight': round(pc_data[1], 1),
                        },
                    }
        if best_result:
            whole_results.append(best_result)
            used_animals.add(best_result['animal_idx'])
            used_proc_slots[best_result['proc_idx']] += 1

    if whole_results:
        wc = sum(r['cost'] for r in whole_results)
        print(f"    Whole/uncut: {len(whole_results)}/{n_whole} assigned, cost=${wc:.2f}")

    # ── Build MIP for partial POs only ──
    if max_batches == 0 or n_partial == 0:
        remaining = list(partial_pos_list) + whole_pos_list[len(whole_results):]
        return whole_results, remaining

    # Filter out used animals and adjust processor capacities
    avail_animals = [(a, animal) for a, animal in enumerate(inventory) if a not in used_animals]
    avail_animal_indices = [a for a, _ in avail_animals]
    remaining_cap = {p: int(processors[p].get('daily_capacity_head') or 999) - used_proc_slots[p]
                     for p in range(n_procs)}

    prob = pulp.LpProblem("TerraMessaUnified", pulp.LpMinimize)

    # x[i,b] = partial PO i assigned to batch b
    x = {}
    for i in range(n_partial):
        for b in range(max_batches):
            x[(i, b)] = pulp.LpVariable(f"x_{i}_{b}", cat='Binary')

    # y[b,a,p] = batch b uses animal a at processor p (only for available animals)
    y = {}
    feasible_yaps = []
    for b in range(max_batches):
        for a in avail_animal_indices:
            for p in range(n_procs):
                if (a, p) in proc_cost and remaining_cap[p] > 0:
                    key = (b, a, p)
                    y[key] = pulp.LpVariable(f"y_{b}_{a}_{p}", cat='Binary')
                    feasible_yaps.append(key)

    # z[b] = batch b is activated
    z = {}
    for b in range(max_batches):
        z[b] = pulp.LpVariable(f"z_{b}", cat='Binary')

    # Auxiliary variables
    W_max = pulp.LpVariable("W_max", lowBound=0)
    U_max = pulp.LpVariable("U_max", lowBound=0)

    # ── Objective ──

    # y_bp helper: is processor p used for batch b?
    y_bp = {}
    for b in range(max_batches):
        for p in range(n_procs):
            bp_keys = [k for k in feasible_yaps if k[0] == b and k[2] == p]
            if bp_keys:
                y_bp[(b, p)] = pulp.lpSum(y[k] for k in bp_keys)

    # Customer transport: approximate using expected processor cost per PO
    # Instead of v[i,b,p] variables, use a simpler upper bound:
    # For each PO i in batch b, the customer transport depends on which processor
    # the batch uses. We compute expected transport as:
    # sum over p: cust_dist(i,p) * y_bp(b,p) for each (i,b) — linear!
    # This works because y_bp(b,p) is already linear and at most one p is active per batch.

    # Objective component: processing + farmer transport
    obj_processing = pulp.lpSum(
        (proc_cost[key[1], key[2]][0] + proc_cost[key[1], key[2]][3]) * y[key]
        for key in feasible_yaps
    )

    # Customer transport per (PO, batch) — linear because exactly one proc per batch
    obj_customer_transport = pulp.lpSum(
        cust_proc_dist[(i, p)] * customer_rate * x[(i, b)]
        for i in range(n_partial)
        for b in range(max_batches)
        for p in range(n_procs)
        if (i, p) in cust_proc_dist and (b, p) in y_bp
    )
    # Note: this over-counts — it adds customer transport for ALL processors, not just
    # the one the batch actually uses. We need the linking.
    # Correct approach: define ct[i,b] = sum_p cust_dist(i,p) * y_bp(b,p) * x[i,b]
    # This is bilinear. Linearize: ct[i,b] = sum_p cust_dist(i,p) * v[i,b,p]
    # where v[i,b,p] = x[i,b] AND y_bp(b,p)

    # Minimal v variables: only create for (i,b,p) where both (i,p) and (b,p) feasible
    v = {}
    for i in range(n_partial):
        feasible_p = [p for p in range(n_procs) if (i, p) in cust_proc_dist]
        for b in range(max_batches):
            for p in feasible_p:
                if (b, p) in y_bp:
                    v[(i, b, p)] = pulp.LpVariable(f"v_{i}_{b}_{p}", cat='Binary')

    # Link v: v[i,b,p] = x[i,b] AND y_bp[b,p]
    for (i, b, p) in v:
        prob += v[(i, b, p)] <= x[(i, b)]
        prob += v[(i, b, p)] <= y_bp[(b, p)]
        prob += v[(i, b, p)] >= x[(i, b)] + y_bp[(b, p)] - 1

    # Correct customer transport
    obj_customer_transport = pulp.lpSum(
        cust_proc_dist[(i, p)] * customer_rate * v[(i, b, p)]
        for (i, b, p) in v
    )

    obj_cost = obj_processing + obj_customer_transport

    # Wait time: prefer assigning older POs
    obj_avg_wait = -pulp.lpSum(
        wait_days[i] * pulp.lpSum(x[(i, b)] for b in range(max_batches))
        for i in range(n_partial)
    )

    # Max unassigned wait
    for i in range(n_partial):
        assigned_i = pulp.lpSum(x[(i, b)] for b in range(max_batches))
        prob += W_max >= wait_days[i] * (1 - assigned_i)

    # Utilization balance
    for p in range(n_procs):
        proc_load_p = pulp.lpSum(y[key] for key in feasible_yaps if key[2] == p)
        prob += U_max >= proc_load_p

    # Combined objective
    prob += (
        w_cost * obj_cost
        + w_avg_wait * obj_avg_wait
        + w_max_wait * W_max
        + w_util_balance * U_max
        + w_geo_penalty * obj_customer_transport
        - int(get_config(config, 'mip_assignment_bonus', 100000)) * pulp.lpSum(z[b] for b in range(max_batches))
    )

    # ── Constraints ──

    # C1: Each partial PO in at most one batch
    for i in range(n_partial):
        prob += pulp.lpSum(x[(i, b)] for b in range(max_batches)) <= 1

    # C2: Batch capacity
    for b in range(max_batches):
        prob += pulp.lpSum(
            SHARE_FRACTIONS.get(partial_pos_list[i]['share'], 0) * x[(i, b)]
            for i in range(n_partial)
        ) <= z[b]

    # C3: Fill threshold
    for b in range(max_batches):
        prob += pulp.lpSum(
            SHARE_FRACTIONS.get(partial_pos_list[i]['share'], 0) * x[(i, b)]
            for i in range(n_partial)
        ) >= fill_threshold * z[b]

    # C4: (no whole POs in MIP — handled above)

    # C5: Active batch → exactly one (animal, processor)
    for b in range(max_batches):
        batch_yaps = [k for k in feasible_yaps if k[0] == b]
        if batch_yaps:
            prob += pulp.lpSum(y[k] for k in batch_yaps) == z[b]
        else:
            prob += z[b] == 0

    # C6: Each animal used at most once
    for a in avail_animal_indices:
        animal_yaps = [k for k in feasible_yaps if k[1] == a]
        if animal_yaps:
            prob += pulp.lpSum(y[k] for k in animal_yaps) <= 1

    # C7: Processor remaining capacity
    for p in range(n_procs):
        cap = remaining_cap[p]
        proc_yaps = [k for k in feasible_yaps if k[2] == p]
        if proc_yaps:
            prob += pulp.lpSum(y[k] for k in proc_yaps) <= cap

    # C8: Infeasible POs (no reachable processor)
    for i in range(n_partial):
        feasible_procs = [p for p in range(n_procs) if (i, p) in cust_proc_dist]
        if not feasible_procs:
            for b in range(max_batches):
                prob += x[(i, b)] == 0

    # C9: PO must reach the batch's processor
    for i in range(n_partial):
        for b in range(max_batches):
            v_keys = [(i, b, p) for p in range(n_procs) if (i, b, p) in v]
            if v_keys:
                prob += x[(i, b)] <= pulp.lpSum(v[k] for k in v_keys)
            else:
                prob += x[(i, b)] == 0

    # C10: Symmetry breaking
    for b in range(1, max_batches):
        prob += z[b] <= z[b - 1]

    # ── Solve ──
    var_count = len(x) + len(y) + len(z) + len(v) + 2
    constraint_count = len(prob.constraints)
    print(f"    Variables: {var_count} ({len(x)} x + {len(y)} y + {len(z)} z + {len(v)} v)")
    print(f"    Constraints: {constraint_count}")
    print(f"    Weights: cost={w_cost}, avg_wait={w_avg_wait}, max_wait={w_max_wait}, "
          f"util={w_util_balance}, geo={w_geo_penalty}")

    time_limit = int(get_config(config, 'mip_time_limit_seconds', 60))
    prob.solve(pulp.COIN_CMD(msg=0, path=CBC_PATH, timeLimit=time_limit))

    if prob.status != pulp.constants.LpStatusOptimal:
        print(f"    MIP solver status: {pulp.LpStatus[prob.status]}")
        if prob.status not in (pulp.constants.LpStatusOptimal, 1):
            # Return whole results only
            remaining = list(partial_pos_list) + whole_pos_list[len(whole_results):]
            return whole_results, remaining

    # ── Extract partial solution ──
    partial_results = []
    assigned_partial_indices = set()

    for b in range(max_batches):
        if z[b].varValue and z[b].varValue > 0.5:
            batch_pos = []
            for i in range(n_partial):
                if x[(i, b)].varValue and x[(i, b)].varValue > 0.5:
                    batch_pos.append(partial_pos_list[i])
                    assigned_partial_indices.add(i)

            for key in feasible_yaps:
                if key[0] == b and y[key].varValue and y[key].varValue > 0.5:
                    _, a_idx, p_idx = key
                    animal = inventory[a_idx]
                    proc = processors[p_idx]

                    pc_data = proc_cost[(a_idx, p_idx)]
                    cust_transport = sum(
                        cust_proc_dist.get((i, p_idx), 0) * customer_rate
                        for i, po in enumerate(partial_pos_list)
                        if po in batch_pos and (i, p_idx) in cust_proc_dist
                    )

                    breakdown = {
                        'processing_cost': round(pc_data[0], 2),
                        'farmer_transport': round(pc_data[3], 2),
                        'customer_transport': round(cust_transport, 2),
                        'total_cost': round(pc_data[0] + pc_data[3] + cust_transport, 2),
                        'farmer_distance': pc_data[2],
                        'hanging_weight': round(pc_data[1], 1),
                    }

                    partial_results.append({
                        'batch_pos': batch_pos,
                        'animal': animal,
                        'animal_idx': a_idx,
                        'processor': proc,
                        'proc_idx': p_idx,
                        'cost': breakdown['total_cost'],
                        'breakdown': breakdown,
                    })
                    break

    # Combine whole + partial results
    all_results = whole_results + partial_results
    remaining = [po for i, po in enumerate(partial_pos_list) if i not in assigned_partial_indices]
    remaining += whole_pos_list[len(whole_results):]  # any unassigned whole POs

    total_assigned = len(whole_results) + len(assigned_partial_indices)
    total_cost = sum(r['cost'] for r in all_results)
    print(f"    Unified MIP solved: {len(all_results)} batches "
          f"({len(whole_results)} whole + {len(partial_results)} partial), "
          f"{total_assigned}/{n_pos} POs, cost=${total_cost:.2f}")

    if remaining:
        rem_str = [f"{po['po_number']}({po['share']})" for po in remaining[:10]]
        if len(remaining) > 10:
            rem_str.append(f"...+{len(remaining)-10} more")
        rem_sum = sum(SHARE_FRACTIONS.get(po['share'], 0) for po in remaining)
        print(f"    {len(remaining)} POs remaining (fraction={rem_sum:.3f}): {rem_str}")

    if all_results:
        proc_loads = defaultdict(int)
        for r in all_results:
            proc_loads[r['processor']['company_name']] += 1
        load_str = ", ".join(f"{n}={c}" for n, c in sorted(proc_loads.items()))
        print(f"    Processor loads: {load_str}")
        if w_max_wait > 0 and W_max.varValue is not None:
            print(f"    Max unassigned wait: {W_max.varValue:.0f} days")

    return all_results, remaining


# ─── Execution ───────────────────────────────────────────────────────────

def create_slaughter_order(conn, animal, processor, batch_pos, cost_breakdown):
    """Create a slaughter order and update POs + inventory."""
    import random, string
    rand = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    order_number = f"SO-{datetime.now().strftime('%Y%m%d%H%M%S')}-{animal['species'][:3].upper()}-{rand}"

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO slaughter_orders (
                order_number, animal_id, profile_id, species, status,
                processing_cost, estimated_hanging_weight,
                farmer_transport_cost, total_customer_transport_cost
            )
            VALUES (%s, %s, %s, %s, 'planned', %s, %s, %s, %s)
        """, (
            order_number,
            animal['id'],
            processor['processor_id'],
            animal['species'],
            cost_breakdown['processing_cost'],
            cost_breakdown['hanging_weight'],
            cost_breakdown['farmer_transport'],
            cost_breakdown['customer_transport'],
        ))

        cur.execute("""
            UPDATE farmer_inventory SET status = 'reserved', updated_at = now()
            WHERE id = %s
        """, (animal['id'],))

        for po in batch_pos:
            cur.execute("""
                UPDATE purchase_orders SET
                    inventory_id = %s,
                    slaughter_order_number = %s,
                    status = 'confirmed',
                    updated_at = now()
                WHERE po_number = %s
            """, (animal['id'], order_number, po['po_number']))

    conn.commit()
    return order_number


# ─── Main Optimizer ──────────────────────────────────────────────────────

def run_optimizer(use_supabase=False, dry_run=False, mode='unified'):
    """Main entry point. Runs MIP-based optimization for all species.

    Modes:
        'unified' — Phase 2: single MIP for batching + assignment + multi-objective
        'phase1'  — Phase 1: FFD batching then joint assignment MIP (fallback)
    """
    import time as _time

    conn = get_connection(use_supabase=use_supabase)
    config = load_optimizer_config(conn)
    fill_threshold = get_config(config, 'fill_threshold', 1.0)

    # Initialize advanced features if available
    adv = None
    if HAS_ADVANCED:
        try:
            adv = AdvancedFeatures(conn, config)
        except Exception:
            pass

    mode_label = "Unified MIP (Phase 2)" if mode == 'unified' else "FFD + Assignment MIP (Phase 1)"
    print(f"═══ Terra Mensa Optimizer v2 ═══")
    print(f"  Mode: {mode_label}")
    print(f"  Advanced features: {'enabled' if adv else 'disabled'}")
    print(f"  Solver: PuLP + CBC")
    print(f"  Fill threshold: {fill_threshold}")
    print(f"  Farmer transport: ${get_config(config, 'farmer_transport_per_mile')}/mi")
    print(f"  Customer transport: ${get_config(config, 'customer_transport_per_mile')}/mi")
    print(f"  Default farmer radius: {get_config(config, 'max_farmer_distance_miles')} mi (per-processor override supported)")
    print(f"  Default customer radius: {get_config(config, 'max_customer_distance_miles')} mi (per-processor override supported)")
    if mode == 'unified':
        print(f"  Weights: cost={get_config(config, 'w_cost', 1.0)}, "
              f"avg_wait={get_config(config, 'w_avg_wait', 0.3)}, "
              f"max_wait={get_config(config, 'w_max_wait', 0.5)}, "
              f"util={get_config(config, 'w_util_balance', 0.2)}, "
              f"geo={get_config(config, 'w_geo_penalty', 0.1)}")
    if dry_run:
        print(f"  *** DRY RUN — no database changes ***")
    print()

    # Record demand snapshot (Phase 4)
    if adv and not dry_run:
        try:
            adv.record_demand_snapshot()
        except Exception:
            conn.rollback()

    total_orders_created = 0
    total_cost_sum = 0.0

    for species in SPECIES_LIST:
        print(f"── {species.upper()} ──")

        # Step 1: Get pending POs
        pending = get_pending_pos(conn, species)
        if not pending:
            print(f"  No pending POs")
            continue

        fractions = [SHARE_FRACTIONS.get(po['share'], 0) for po in pending]
        print(f"  {len(pending)} pending POs (total fraction: {sum(fractions):.3f})")

        # Step 2: Load data
        species_start = _time.time()
        inventory = get_available_inventory(conn, species)
        processors = get_processors_for_species(conn, species)

        if not inventory:
            print(f"  WARNING: No available inventory for {species}")
            continue
        if not processors:
            print(f"  WARNING: No processors for {species}")
            continue

        # Phase 4: Filter processors by blackouts and capabilities
        if adv:
            pre_filter = len(processors)
            processors = adv.filter_processors(processors, species)
            if len(processors) < pre_filter:
                print(f"  Processors: {pre_filter} total, {len(processors)} after blackout/capability filter")
            # Rank inventory by quality
            inventory = adv.rank_inventory_by_quality(inventory)

        print(f"  {len(inventory)} available animals, {len(processors)} processors")

        # Bulk-load distances and customer profiles
        all_profile_ids = set()
        for animal in inventory:
            all_profile_ids.add(str(animal['profile_id']))
        for proc in processors:
            all_profile_ids.add(str(proc['processor_id']))
        for po in pending:
            all_profile_ids.add(str(po['profile_id']))

        distances = bulk_load_distances(conn, all_profile_ids)
        customer_ids = set(str(po['profile_id']) for po in pending)
        customers = get_customer_profiles_bulk(conn, customer_ids)

        if mode == 'unified':
            # ── Phase 2: Unified MIP ──
            results, remaining = solve_unified_mip(
                pending, inventory, processors, customers, distances, config
            )

            for r in results:
                batch = r['batch_pos']
                animal = r['animal']
                proc = r['processor']
                breakdown = r['breakdown']

                po_numbers = [po['po_number'] for po in batch]
                batch_fraction = sum(SHARE_FRACTIONS.get(po['share'], 0) for po in batch)
                farm_name = animal.get('company_name') or animal.get('first_name')

                print(f"\n  Batch: {len(batch)} POs ({batch_fraction:.2f} animal) — {po_numbers}")
                print(f"    Animal: {farm_name} — {animal.get('live_weight_est')} lbs")
                print(f"    Processor: {proc['company_name']} at ${breakdown['total_cost']:.2f}")
                print(f"      Processing: ${breakdown['processing_cost']:.2f}")
                print(f"      Farmer transport: ${breakdown['farmer_transport']:.2f} ({breakdown['farmer_distance']:.1f} mi)")
                print(f"      Customer transport: ${breakdown['customer_transport']:.2f}")

                if not dry_run:
                    so_number = create_slaughter_order(conn, animal, proc, batch, breakdown)
                    print(f"    → Slaughter order: {so_number}")

                total_orders_created += 1
                total_cost_sum += r['cost']

        else:
            # ── Phase 1: FFD + joint assignment ──
            batches, remaining = aggregate_pos_ffd(pending, fill_threshold)
            if not batches:
                print(f"  No complete animals to process")
                continue

            print(f"  {len(batches)} complete animal(s) to process")

            assignments = solve_joint_assignment(
                batches, inventory, processors, customers, distances, config
            )

            for batch_idx, animal_idx, proc_idx, cost, breakdown in assignments:
                batch = batches[batch_idx]
                animal = inventory[animal_idx]
                proc = processors[proc_idx]

                po_numbers = [po['po_number'] for po in batch]
                batch_fraction = sum(SHARE_FRACTIONS.get(po['share'], 0) for po in batch)
                farm_name = animal.get('company_name') or animal.get('first_name')

                print(f"\n  Batch {batch_idx+1}: {len(batch)} POs ({batch_fraction:.2f} animal) — {po_numbers}")
                print(f"    Animal: {farm_name} — {animal.get('live_weight_est')} lbs")
                print(f"    Processor: {proc['company_name']} at ${breakdown['total_cost']:.2f}")
                print(f"      Processing: ${breakdown['processing_cost']:.2f}")
                print(f"      Farmer transport: ${breakdown['farmer_transport']:.2f} ({breakdown['farmer_distance']:.1f} mi)")
                print(f"      Customer transport: ${breakdown['customer_transport']:.2f}")

                if not dry_run:
                    so_number = create_slaughter_order(conn, animal, proc, batch, breakdown)
                    print(f"    → Slaughter order: {so_number}")

                total_orders_created += 1
                total_cost_sum += cost

    print(f"\n═══ Optimizer v2 Complete ═══")
    print(f"  Slaughter orders: {total_orders_created}")
    print(f"  Total system cost: ${total_cost_sum:.2f}")
    if total_orders_created > 0:
        print(f"  Average cost/order: ${total_cost_sum / total_orders_created:.2f}")

    # Phase 4: Log run
    if adv and not dry_run:
        try:
            adv.log_optimizer_run(
                mode=mode, species='all',
                pos_pending=0, pos_assigned=0,
                batches_formed=0, batches_assigned=total_orders_created,
                total_cost=total_cost_sum, solve_time=0,
                solver_status='complete',
            )
        except Exception:
            conn.rollback()

    conn.close()


if __name__ == '__main__':
    use_supabase = '--supabase' in sys.argv
    dry_run = '--dry-run' in sys.argv
    mode = 'phase1' if '--mode' in sys.argv and 'phase1' in sys.argv else 'unified'
    run_optimizer(use_supabase=use_supabase, dry_run=dry_run, mode=mode)
