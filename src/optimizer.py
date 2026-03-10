"""Carcass optimizer: matches D2C purchase order demand to whole animals,
selects processors, and routes remainder cuts to buyer of last resort.

Algorithm: Modified first-fit-decreasing bin packing.
"""
import uuid
from collections import defaultdict
from datetime import datetime
from statistics import mean
from typing import Optional

import psycopg2.extras

from db import get_connection, get_pending_demand, get_available_animals, update_animal_status
from optimizer_db import (
    get_cut_specs, get_eligible_processors, get_grade_hierarchy,
    grade_meets_requirement, save_slaughter_order, fulfill_po_lines,
)
from geo import safe_distance
from config import (
    DRESS_PCT_BY_YG, DEFAULT_YIELD_GRADE,
    DEFAULT_PORK_DRESS_PCT, DEFAULT_LAMB_DRESS_PCT,
    DEFAULT_CHICKEN_DRESS_PCT, DEFAULT_GOAT_DRESS_PCT,
)

# ---------------------------------------------------------------------------
# Optimizer weights (lower score = better)
# ---------------------------------------------------------------------------

OPTIMIZER_WEIGHTS = {
    'customer_distance': 1.0,   # $/mile equivalent
    'farmer_distance': 0.7,
    'processing_cost': 0.5,     # normalized $/head
    'waste_penalty': 0.3,       # penalty per % of carcass to last resort
}


# ---------------------------------------------------------------------------
# Step 1: Demand Aggregation
# ---------------------------------------------------------------------------

def aggregate_demand(species: str) -> dict:
    """Build demand vector: {cut_code: {demand_lbs, min_grade, po_details}}.

    Returns dict keyed by cut_code with aggregated demand and the individual
    PO line records that contribute to it.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT pl.id AS po_line_id, pl.cut_code, pl.description, pl.primal,
                       pl.quantity_lbs, pl.fulfilled_lbs, pl.po_number,
                       po.customer_id, po.quality_grade,
                       dc.latitude AS cust_lat, dc.longitude AS cust_lng
                FROM po_lines pl
                JOIN purchase_orders po ON po.po_number = pl.po_number
                JOIN dtc_customers dc ON dc.customer_id = po.customer_id
                WHERE po.species = %s
                  AND pl.status IN ('pending', 'partial')
                  AND po.status NOT IN ('cancelled', 'fulfilled')
                ORDER BY (pl.quantity_lbs - COALESCE(pl.fulfilled_lbs, 0)) DESC
            """, (species,))

            demand = {}
            for row in cur.fetchall():
                remaining = float(row['quantity_lbs']) - float(row['fulfilled_lbs'] or 0)
                if remaining <= 0:
                    continue
                code = row['cut_code']
                if code not in demand:
                    demand[code] = {
                        'cut_code': code,
                        'description': row['description'],
                        'primal': row['primal'],
                        'demand_lbs': 0.0,
                        'min_grade': row['quality_grade'],
                        'lines': [],
                    }
                demand[code]['demand_lbs'] += remaining
                demand[code]['lines'].append({
                    'po_line_id': row['po_line_id'],
                    'po_number': row['po_number'],
                    'customer_id': row['customer_id'],
                    'remaining_lbs': remaining,
                    'quality_grade': row['quality_grade'],
                    'cust_lat': row['cust_lat'],
                    'cust_lng': row['cust_lng'],
                })
                # Track strictest grade requirement
                if row['quality_grade']:
                    current_min = demand[code]['min_grade']
                    if current_min is None:
                        demand[code]['min_grade'] = row['quality_grade']
            return demand
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Step 2: Carcass Yield Calculation
# ---------------------------------------------------------------------------

def estimate_hanging_weight(animal: dict) -> float:
    """Estimate hanging (carcass) weight from animal record."""
    live_wt = float(animal.get('live_weight_est') or 0)
    if live_wt <= 0:
        return 0.0
    dress_pct = animal.get('dressing_pct_est')
    if dress_pct:
        return live_wt * float(dress_pct)
    species = animal['species']
    if species == 'cattle':
        yg = animal.get('yield_grade_est') or DEFAULT_YIELD_GRADE
        return live_wt * DRESS_PCT_BY_YG.get(yg, 0.60)
    elif species == 'pork':
        return live_wt * DEFAULT_PORK_DRESS_PCT
    elif species == 'lamb':
        return live_wt * DEFAULT_LAMB_DRESS_PCT
    elif species == 'chicken':
        return live_wt * DEFAULT_CHICKEN_DRESS_PCT
    elif species == 'goat':
        return live_wt * DEFAULT_GOAT_DRESS_PCT
    return live_wt * 0.55


def compute_yield_vector(hanging_weight: float, cut_specs: list) -> dict:
    """Given hanging weight and species cut specs, return {cut_code: expected_lbs}."""
    return {
        spec['cut_code']: round(hanging_weight * float(spec['yield_pct']) / 100.0, 1)
        for spec in cut_specs
    }


# ---------------------------------------------------------------------------
# Step 3: Bin Packing — Match Demand to Carcasses
# ---------------------------------------------------------------------------

def _get_farmer_location(farmer_id: str) -> tuple:
    """Fetch farmer lat/lng."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT latitude, longitude FROM farmers WHERE farmer_id = %s",
                        (farmer_id,))
            row = cur.fetchone()
            if row:
                return (row['latitude'], row['longitude'])
            return (None, None)
    finally:
        conn.close()


class CarcassSlot:
    """Tracks remaining capacity for a single cut on a carcass."""

    def __init__(self, cut_code: str, total_lbs: float):
        self.cut_code = cut_code
        self.total_lbs = total_lbs
        self.allocated_po = 0.0      # lbs assigned to customer POs
        self.allocated_lor = 0.0     # lbs assigned to last-resort
        self.po_assignments = []     # [(po_line_id, po_number, lbs)]

    @property
    def remaining(self) -> float:
        return self.total_lbs - self.allocated_po - self.allocated_lor

    def assign_po(self, po_line_id: int, po_number: str, lbs: float) -> float:
        """Assign lbs from a PO line. Returns actual lbs assigned."""
        assignable = min(lbs, self.remaining)
        if assignable <= 0:
            return 0.0
        self.allocated_po += assignable
        self.po_assignments.append((po_line_id, po_number, assignable))
        return assignable

    def assign_lor(self):
        """Route all remaining lbs to last-resort buyer."""
        self.allocated_lor = self.remaining


class CarcassBin:
    """Represents a single whole animal as a bin with cut slots."""

    def __init__(self, animal: dict, yield_vector: dict):
        self.animal = animal
        self.animal_id = animal['animal_id']
        self.farmer_id = animal['farmer_id']
        self.hanging_weight = sum(yield_vector.values())
        self.slots = {
            code: CarcassSlot(code, lbs)
            for code, lbs in yield_vector.items()
        }
        self.customer_locations = []  # [(lat, lng)] of customers assigned

    def has_cut(self, cut_code: str) -> bool:
        return cut_code in self.slots and self.slots[cut_code].remaining > 0

    def available_lbs(self, cut_code: str) -> float:
        if cut_code not in self.slots:
            return 0.0
        return self.slots[cut_code].remaining

    @property
    def total_allocated_po(self) -> float:
        return sum(s.allocated_po for s in self.slots.values())

    @property
    def total_allocated_lor(self) -> float:
        return sum(s.allocated_lor for s in self.slots.values())

    @property
    def utilization_pct(self) -> float:
        if self.hanging_weight <= 0:
            return 0.0
        return (self.total_allocated_po / self.hanging_weight) * 100

    def finalize_lor(self):
        """Route all unfilled capacity to last-resort buyer."""
        for slot in self.slots.values():
            slot.assign_lor()


def bin_pack_demand(demand: dict, animals: list, cut_specs: list,
                    grade_hierarchy: dict, species: str) -> list:
    """Modified first-fit-decreasing bin packing.

    Args:
        demand: {cut_code: {demand_lbs, lines: [...]}}
        animals: list of available animal dicts
        cut_specs: list of cut spec dicts for the species
        grade_hierarchy: {grade_code: rank_order}
        species: species string

    Returns:
        List of CarcassBin objects with allocations filled.
    """
    if not demand or not animals:
        return []

    # Build bins from available animals
    bins = []
    for animal in animals:
        hw = estimate_hanging_weight(animal)
        if hw <= 0:
            continue
        yv = compute_yield_vector(hw, cut_specs)
        bins.append(CarcassBin(animal, yv))

    if not bins:
        return []

    # Build a flat list of demand lines sorted by remaining_lbs descending (FFD)
    all_lines = []
    for cut_code, info in demand.items():
        for line in info['lines']:
            all_lines.append({
                **line,
                'cut_code': cut_code,
                'primal': info.get('primal'),
            })
    all_lines.sort(key=lambda x: x['remaining_lbs'], reverse=True)

    # First-fit-decreasing: for each demand line, find the best bin
    for line in all_lines:
        cut_code = line['cut_code']
        required_grade = line.get('quality_grade')
        remaining = line['remaining_lbs']
        best_bin = None
        best_available = 0

        for b in bins:
            if not b.has_cut(cut_code):
                continue
            # Grade check
            animal_grade = b.animal.get('quality_grade_est')
            if required_grade and animal_grade:
                a_rank = grade_hierarchy.get(animal_grade, 0)
                r_rank = grade_hierarchy.get(required_grade, 0)
                if a_rank < r_rank:
                    continue
            avail = b.available_lbs(cut_code)
            if avail > best_available:
                best_available = avail
                best_bin = b

        if best_bin is not None and best_available > 0:
            assigned = best_bin.slots[cut_code].assign_po(
                line['po_line_id'], line['po_number'],
                min(remaining, best_available),
            )
            if assigned > 0:
                best_bin.customer_locations.append(
                    (line.get('cust_lat'), line.get('cust_lng'))
                )

    # Remove bins with zero customer allocations (no demand matched)
    active_bins = [b for b in bins if b.total_allocated_po > 0]

    # Finalize: route remaining capacity to last-resort
    for b in active_bins:
        b.finalize_lor()

    return active_bins


# ---------------------------------------------------------------------------
# Step 4: Processor Selection & Scoring
# ---------------------------------------------------------------------------

def score_processor(carcass_bin: CarcassBin, processor: dict,
                    farmer_lat, farmer_lng,
                    processing_cost: float,
                    weights: dict = None) -> float:
    """Score a processor assignment. Lower = better."""
    w = weights or OPTIMIZER_WEIGHTS

    # Average customer-to-processor distance
    cust_dists = []
    for clat, clng in carcass_bin.customer_locations:
        d = safe_distance(clat, clng,
                          processor.get('latitude'), processor.get('longitude'))
        cust_dists.append(d)
    avg_cust_dist = mean(cust_dists) if cust_dists else 9999.0

    # Farmer-to-processor distance
    farmer_dist = safe_distance(farmer_lat, farmer_lng,
                                processor.get('latitude'), processor.get('longitude'))

    # Waste penalty
    lor_pct = 0.0
    if carcass_bin.hanging_weight > 0:
        lor_pct = (carcass_bin.total_allocated_lor / carcass_bin.hanging_weight) * 100

    score = (
        w['customer_distance'] * avg_cust_dist +
        w['farmer_distance'] * farmer_dist +
        w['processing_cost'] * processing_cost +
        w['waste_penalty'] * lor_pct
    )
    return score


def select_processor(carcass_bin: CarcassBin, processors: list,
                     species: str, weights: dict = None) -> tuple:
    """Select the best processor for a carcass bin.

    Returns (processor_dict, score, processing_cost, farmer_dist, avg_cust_dist).
    """
    from config import PROCESSORS, PORK_PROCESSORS, LAMB_PROCESSORS

    farmer_lat, farmer_lng = _get_farmer_location(carcass_bin.farmer_id)

    best = None
    best_score = float('inf')
    best_meta = {}

    for proc in processors:
        # Get processing cost from config (kill_fee + fab * hanging_weight)
        pkey = proc['processor_key']
        cost_cfg = None
        if species == 'cattle':
            cost_cfg = PROCESSORS.get(pkey)
        elif species == 'pork':
            cost_cfg = PORK_PROCESSORS.get(pkey)
        elif species == 'lamb':
            cost_cfg = LAMB_PROCESSORS.get(pkey)

        if cost_cfg:
            proc_cost = cost_cfg['kill_fee'] + (
                cost_cfg['fab_cost_per_lb'] * carcass_bin.hanging_weight
            )
        else:
            proc_cost = 500.0  # fallback estimate

        score = score_processor(carcass_bin, proc, farmer_lat, farmer_lng,
                                proc_cost, weights)

        if score < best_score:
            best_score = score
            best = proc
            # Compute individual metrics for reporting
            cust_dists = [
                safe_distance(clat, clng, proc.get('latitude'), proc.get('longitude'))
                for clat, clng in carcass_bin.customer_locations
            ]
            best_meta = {
                'processing_cost': proc_cost,
                'farmer_dist': safe_distance(farmer_lat, farmer_lng,
                                             proc.get('latitude'), proc.get('longitude')),
                'avg_cust_dist': mean(cust_dists) if cust_dists else 0,
            }

    return best, best_score, best_meta


# ---------------------------------------------------------------------------
# Step 5: Output — Write Results
# ---------------------------------------------------------------------------

def write_results(bins: list, species: str, run_id: str,
                  processors: list, dry_run: bool = False) -> list:
    """Write optimizer results to DB. Returns list of result summaries."""
    results = []

    for i, cbin in enumerate(bins, 1):
        proc, score, meta = select_processor(cbin, processors, species)
        if proc is None:
            print(f"  WARNING: No eligible processor for {cbin.animal_id} — skipping")
            continue

        order_number = f"SO-{run_id[:8]}-{i:03d}"
        pct_po = cbin.utilization_pct
        pct_lor = 100.0 - pct_po

        order = {
            'order_number': order_number,
            'species': species,
            'animal_id': cbin.animal_id,
            'processor_key': proc['processor_key'],
            'optimizer_run_id': run_id,
            'estimated_hanging_weight': round(cbin.hanging_weight, 2),
            'processing_cost_total': round(meta['processing_cost'], 2),
            'farmer_to_proc_distance': round(meta['farmer_dist'], 2),
            'avg_cust_to_proc_distance': round(meta['avg_cust_dist'], 2),
            'pct_allocated_to_orders': round(pct_po, 2),
            'pct_to_last_resort': round(pct_lor, 2),
            'optimizer_score': round(score, 4),
        }

        # Build lines
        lines = []
        po_fulfillments = []
        for slot in cbin.slots.values():
            if slot.total_lbs <= 0:
                continue
            # If slot has PO assignments, create a line per assignment
            if slot.po_assignments:
                for po_line_id, po_number, lbs in slot.po_assignments:
                    lines.append({
                        'cut_code': slot.cut_code,
                        'total_lbs': round(slot.total_lbs, 2),
                        'allocated_to_po': round(lbs, 2),
                        'allocated_to_lor': 0,
                        'po_number': po_number,
                        'po_line_id': po_line_id,
                    })
                    po_fulfillments.append({
                        'po_line_id': po_line_id,
                        'lbs': round(lbs, 2),
                    })
            # LOR portion (if any remaining)
            if slot.allocated_lor > 0:
                lines.append({
                    'cut_code': slot.cut_code,
                    'total_lbs': round(slot.total_lbs, 2),
                    'allocated_to_po': 0,
                    'allocated_to_lor': round(slot.allocated_lor, 2),
                    'po_number': None,
                    'po_line_id': None,
                })

        result = {
            'order': order,
            'lines': lines,
            'fulfillments': po_fulfillments,
            'processor': proc,
        }
        results.append(result)

        if not dry_run:
            save_slaughter_order(order, lines)
            fulfill_po_lines(po_fulfillments)
            update_animal_status(cbin.animal_id, 'reserved')

    return results


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def run_optimizer(species: str, dry_run: bool = True,
                  weights: dict = None) -> dict:
    """Run the full optimization pipeline for a species.

    Returns summary dict with results and statistics.
    """
    run_id = str(uuid.uuid4())
    started = datetime.now()

    print(f"\n{'='*60}")
    print(f"  Carcass Optimizer — {species.upper()}")
    print(f"  Run ID: {run_id}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'COMMIT'}")
    print(f"{'='*60}\n")

    # Step 1: Aggregate demand
    demand = aggregate_demand(species)
    if not demand:
        print("  No pending demand found.")
        return {'run_id': run_id, 'status': 'no_demand', 'results': []}

    total_demand_lbs = sum(d['demand_lbs'] for d in demand.values())
    print(f"  Step 1: Demand — {len(demand)} cuts, {total_demand_lbs:,.1f} lbs total")
    for code, d in sorted(demand.items(), key=lambda x: x[1]['demand_lbs'], reverse=True)[:5]:
        print(f"    {code:10s}  {d['demand_lbs']:8.1f} lbs  ({len(d['lines'])} PO lines)")

    # Step 2: Get available animals and cut specs
    animals = get_available_animals(species)
    if not animals:
        print("  No available animals found.")
        return {'run_id': run_id, 'status': 'no_inventory', 'results': []}

    cut_specs = get_cut_specs(species)
    if not cut_specs:
        print(f"  ERROR: No cut specs found for {species}. Run seed_optimizer.py first.")
        return {'run_id': run_id, 'status': 'no_cut_specs', 'results': []}

    grade_hierarchy = get_grade_hierarchy(species)
    print(f"  Step 2: Inventory — {len(animals)} available animals, {len(cut_specs)} cut specs")

    # Step 3: Bin packing
    bins = bin_pack_demand(demand, animals, cut_specs, grade_hierarchy, species)
    if not bins:
        print("  No feasible carcass assignments found.")
        return {'run_id': run_id, 'status': 'no_match', 'results': []}

    print(f"  Step 3: Matched — {len(bins)} carcasses selected")
    for b in bins:
        print(f"    {b.animal_id}  HW={b.hanging_weight:,.0f} lbs  "
              f"util={b.utilization_pct:.1f}%  "
              f"LOR={100-b.utilization_pct:.1f}%")

    # Step 4 & 5: Processor selection + output
    processors = get_eligible_processors(species)
    if not processors:
        print(f"  WARNING: No eligible processors for {species}.")
        return {'run_id': run_id, 'status': 'no_processors', 'results': []}

    print(f"  Step 4: {len(processors)} eligible processors")

    results = write_results(bins, species, run_id, processors, dry_run=dry_run)

    # Summary
    elapsed = (datetime.now() - started).total_seconds()
    total_hw = sum(r['order']['estimated_hanging_weight'] for r in results)
    avg_util = mean([r['order']['pct_allocated_to_orders'] for r in results]) if results else 0
    avg_score = mean([r['order']['optimizer_score'] for r in results]) if results else 0

    print(f"\n  Step 5: Output — {len(results)} slaughter orders")
    for r in results:
        o = r['order']
        p = r['processor']
        print(f"    {o['order_number']}  {o['animal_id']}  "
              f"→ {p['company_name']}  "
              f"score={o['optimizer_score']:.1f}  "
              f"util={o['pct_allocated_to_orders']:.1f}%")

    print(f"\n  {'='*50}")
    print(f"  Total HW: {total_hw:,.0f} lbs across {len(results)} animals")
    print(f"  Avg utilization: {avg_util:.1f}%")
    print(f"  Avg score: {avg_score:.1f}")
    print(f"  Elapsed: {elapsed:.2f}s")
    if dry_run:
        print(f"  *** DRY RUN — nothing written to DB ***")
    print(f"  {'='*50}\n")

    return {
        'run_id': run_id,
        'status': 'success',
        'species': species,
        'dry_run': dry_run,
        'animals_selected': len(results),
        'total_hanging_weight': total_hw,
        'avg_utilization_pct': avg_util,
        'avg_score': avg_score,
        'elapsed_seconds': elapsed,
        'results': results,
    }
