"""Carcass optimizer: PO Assembly Engine.

Accumulates share-based purchase orders into assemblies that tile a carcass.
Share fractions: whole=1.0, half=0.5, quarter=0.25, eighth=0.125.
Only triggers a slaughter order when an assembly reaches sufficient fullness.
"""
import uuid
from collections import defaultdict
from datetime import datetime, date, timedelta
from statistics import mean
from typing import Optional

import psycopg2.extras

from db import (get_connection, get_available_animals, update_animal_status,
                update_po_status, get_pending_pos)
from optimizer_db import (
    get_cut_specs, get_eligible_processors, get_grade_hierarchy,
    grade_meets_requirement, save_slaughter_order,
    get_processor_scheduled_count,
)
from geo import safe_distance
from config import (
    DRESS_PCT_BY_YG, DEFAULT_YIELD_GRADE,
    DEFAULT_PORK_DRESS_PCT, DEFAULT_LAMB_DRESS_PCT,
    DEFAULT_CHICKEN_DRESS_PCT, DEFAULT_GOAT_DRESS_PCT,
    PROCESSING_RATES,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHARE_FRACTIONS = {
    'whole': 1.0,
    'half': 0.5,
    'quarter': 0.25,
    'eighth': 0.125,
}

OPTIMIZER_WEIGHTS = {
    'customer_distance': 1.0,
    'farmer_distance': 0.7,
    'processing_cost': 0.5,
    'waste_penalty': 0.3,
}

DEFAULT_FULLNESS_THRESHOLD = 0.90
DEFAULT_MAX_WAIT_DAYS = 30

SPECIES_TARGET_WEIGHT = {
    'cattle': (1200, 1400),
    'pork':   (260, 290),
    'lamb':   (120, 145),
    'goat':   (75, 95),
}

MATCH_WEIGHTS = {
    'farmer_distance': 1.0,
    'size_penalty': 0.8,
}

AGE_PROXIMITY_RELAXATION = [
    (45, 0.25),
    (30, 0.50),
    (0,  1.00),
]


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def estimate_hanging_weight(animal: dict) -> float:
    """Estimate hanging (carcass) weight from animal record."""
    live_wt = float(animal.get('live_weight_est') or 0)
    if live_wt <= 0:
        return 0.0
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
# Assembly class — share fraction model
# ---------------------------------------------------------------------------

class Assembly:
    """Tracks a virtual carcass being assembled from complementary POs.

    Uses share fractions: whole=1.0, half=0.5, quarter=0.25, eighth=0.125.
    An assembly is full when used_fraction reaches 1.0.
    """

    def __init__(self, species: str):
        self.species = species
        self.pos = []
        self.strictest_grade = None
        self.customer_locations = []
        self.used_fraction = 0.0

    def can_add(self, po: dict, grade_hierarchy: dict) -> bool:
        """Check if a PO fits in this assembly (fraction + grade compatibility)."""
        share = po['share']
        fraction = SHARE_FRACTIONS.get(share, 0)
        if fraction <= 0:
            return False
        return self.used_fraction + fraction <= 1.0 + 1e-9

    def add(self, po: dict, grade_hierarchy: dict):
        """Consume fraction and update strictest grade."""
        share = po['share']
        fraction = SHARE_FRACTIONS.get(share, 0)
        self.used_fraction += fraction
        self.pos.append(po)

        if po.get('cust_lat') is not None and po.get('cust_lng') is not None:
            self.customer_locations.append((po['cust_lat'], po['cust_lng']))

        po_grade = po.get('expected_grade') or po.get('quality_grade')
        if po_grade and grade_hierarchy:
            po_rank = grade_hierarchy.get(po_grade, 0)
            if self.strictest_grade is None:
                self.strictest_grade = po_grade
            else:
                current_rank = grade_hierarchy.get(self.strictest_grade, 0)
                if po_rank > current_rank:
                    self.strictest_grade = po_grade

    @property
    def fullness(self) -> float:
        """Fraction of carcass used (0.0 to 1.0)."""
        return min(self.used_fraction, 1.0)

    @property
    def slot_label(self) -> str:
        """Human-readable fraction usage."""
        return f"{self.used_fraction*100:.0f}%"

    @property
    def oldest_order_date(self) -> Optional[date]:
        dates = []
        for po in self.pos:
            od = po.get('order_date')
            if od:
                if isinstance(od, datetime):
                    dates.append(od.date())
                elif isinstance(od, date):
                    dates.append(od)
        return min(dates) if dates else None

    @property
    def po_numbers(self) -> list:
        return [po['po_number'] for po in self.pos]


# ---------------------------------------------------------------------------
# Phase 1: Assemble POs
# ---------------------------------------------------------------------------

def _avg_location(assembly: Assembly) -> tuple:
    """Average lat/lng of assembly's customers."""
    lats = [loc[0] for loc in assembly.customer_locations if loc[0] is not None]
    lngs = [loc[1] for loc in assembly.customer_locations if loc[1] is not None]
    if not lats:
        return (None, None)
    return (mean(lats), mean(lngs))


def assemble_pos(pos: list, species: str, grade_hierarchy: dict) -> list:
    """Group POs into assemblies that tile a carcass.

    Sort POs by share size desc (whole first), then order_date asc.
    For each PO, find best open assembly by can_add() + geographic proximity.
    """
    sorted_pos = sorted(pos,
                        key=lambda po: (-SHARE_FRACTIONS.get(po['share'], 0),
                                        po.get('order_date') or date.max))

    assemblies = []

    for po in sorted_pos:
        best_assembly = None
        best_distance = float('inf')
        po_lat = po.get('cust_lat')
        po_lng = po.get('cust_lng')

        for asm in assemblies:
            if asm.fullness >= 1.0:
                continue
            if not asm.can_add(po, grade_hierarchy):
                continue
            avg_lat, avg_lng = _avg_location(asm)
            dist = safe_distance(po_lat, po_lng, avg_lat, avg_lng)
            if dist < best_distance:
                best_distance = dist
                best_assembly = asm

        if best_assembly is not None:
            best_assembly.add(po, grade_hierarchy)
        else:
            asm = Assembly(species)
            asm.add(po, grade_hierarchy)
            assemblies.append(asm)

    return assemblies


# ---------------------------------------------------------------------------
# Evaluate Trigger
# ---------------------------------------------------------------------------

def evaluate_trigger(assembly: Assembly, today: date,
                     threshold: float = DEFAULT_FULLNESS_THRESHOLD,
                     max_wait: int = DEFAULT_MAX_WAIT_DAYS) -> str:
    """Determine if an assembly should be triggered.

    Returns 'ready', 'forced', or 'hold'.
    """
    if assembly.fullness >= threshold:
        return 'ready'

    if assembly.species == 'cattle':
        oldest = assembly.oldest_order_date
        if oldest and (today - oldest).days > max_wait and assembly.fullness >= 0.75:
            return 'forced'

    return 'hold'


# ---------------------------------------------------------------------------
# Match Animal
# ---------------------------------------------------------------------------

def _get_po_age_days(assembly: Assembly, today: date) -> int:
    oldest = assembly.oldest_order_date
    if oldest:
        return (today - oldest).days
    return 0


def _get_farmer_distance_weight(species: str, po_age_days: int) -> float:
    if species == 'cattle':
        return 1.0
    for min_days, multiplier in AGE_PROXIMITY_RELAXATION:
        if po_age_days >= min_days:
            return multiplier
    return 1.0


def _compute_size_penalty(animal: dict, species: str) -> float:
    target = SPECIES_TARGET_WEIGHT.get(species)
    if not target:
        return 0.0
    live_wt = float(animal.get('live_weight_est') or 0)
    if live_wt <= 0:
        return 0.0
    low, high = target
    if live_wt < low:
        return ((low - live_wt) / low) * 2.0
    elif live_wt > high:
        return (live_wt - high) / high
    return 0.0


def match_animal(assembly: Assembly, animals: list, cut_specs: list,
                 grade_hierarchy: dict, today: date = None) -> tuple:
    """Find the best animal for an assembly.

    Ranking: grade hard filter, then weighted(farmer_distance + size_score).
    Returns (animal, yield_vector) or (None, None).
    """
    if not animals or not cut_specs:
        return None, None

    today = today or date.today()

    avg_lat, avg_lng = _avg_location(assembly)
    po_age = _get_po_age_days(assembly, today)
    farmer_dist_weight = _get_farmer_distance_weight(assembly.species, po_age)

    required_rank = 0
    if assembly.strictest_grade and grade_hierarchy:
        required_rank = grade_hierarchy.get(assembly.strictest_grade, 0)

    candidates = []
    for animal in animals:
        if animal.get('status') != 'available':
            continue

        # Grade hard filter
        animal_grade = animal.get('expected_grade')
        if assembly.strictest_grade and grade_hierarchy:
            if animal_grade:
                a_rank = grade_hierarchy.get(animal_grade, 0)
                if a_rank < required_rank:
                    continue

        hw = estimate_hanging_weight(animal)
        if hw <= 0:
            continue
        yv = compute_yield_vector(hw, cut_specs)

        # Grade preference: prefer exact match
        grade_excess = 0
        if animal_grade and grade_hierarchy and required_rank > 0:
            a_rank = grade_hierarchy.get(animal_grade, 0)
            grade_excess = a_rank - required_rank

        # Farmer proximity + size penalty
        farmer_lat = animal.get('farmer_lat')
        farmer_lng = animal.get('farmer_lng')
        farmer_dist = safe_distance(farmer_lat, farmer_lng, avg_lat, avg_lng)
        size_penalty = _compute_size_penalty(animal, assembly.species)

        score = (MATCH_WEIGHTS['farmer_distance'] * farmer_dist_weight * farmer_dist +
                 MATCH_WEIGHTS['size_penalty'] * size_penalty)

        candidates.append((animal, yv, grade_excess, score, farmer_dist))

    if not candidates:
        return None, None

    candidates.sort(key=lambda c: (c[2], c[3]))
    best = candidates[0]
    return best[0], best[1]


# ---------------------------------------------------------------------------
# Select Processor
# ---------------------------------------------------------------------------

def select_processor(assembly: Assembly, animal: dict, processors: list,
                     species: str, run_date: date,
                     weights: dict = None) -> tuple:
    """Select the best processor for an assembly + animal.

    Returns (processor_dict, score, meta) or (None, None, None).
    """
    w = weights or OPTIMIZER_WEIGHTS
    farmer_lat = animal.get('farmer_lat')
    farmer_lng = animal.get('farmer_lng')
    hw = estimate_hanging_weight(animal)

    best = None
    best_score = float('inf')
    best_meta = {}

    for proc in processors:
        profile_id = proc['profile_id']

        # Capacity check
        capacity = proc.get('daily_capacity_head')
        if capacity:
            scheduled = get_processor_scheduled_count(profile_id, run_date, species)
            if scheduled >= capacity:
                continue

        # Processing cost from processor_costs
        kill_fee = float(proc.get('kill_fee', 0))
        fab_cost = float(proc.get('fab_cost_per_lb', 0))
        if kill_fee > 0 or fab_cost > 0:
            proc_cost = kill_fee + (fab_cost * hw)
        else:
            # Fallback to species-level rates
            rates = PROCESSING_RATES.get(species)
            if rates:
                proc_cost = rates['kill_fee'] + (rates['fab_cost_per_lb'] * hw)
            else:
                proc_cost = 500.0

        # Average customer-to-processor distance
        cust_dists = [
            safe_distance(clat, clng, proc.get('latitude'), proc.get('longitude'))
            for clat, clng in assembly.customer_locations
        ]
        avg_cust_dist = mean(cust_dists) if cust_dists else 9999.0

        farmer_dist = safe_distance(farmer_lat, farmer_lng,
                                    proc.get('latitude'), proc.get('longitude'))

        lor_pct = (1.0 - assembly.fullness) * 100

        score = (
            w['customer_distance'] * avg_cust_dist +
            w['farmer_distance'] * farmer_dist +
            w['processing_cost'] * proc_cost +
            w['waste_penalty'] * lor_pct
        )

        if score < best_score:
            best_score = score
            best = proc
            best_meta = {
                'processing_cost': proc_cost,
                'farmer_dist': farmer_dist,
                'avg_cust_dist': avg_cust_dist,
            }

    if best is None:
        return None, None, None
    return best, best_score, best_meta


# ---------------------------------------------------------------------------
# Create Slaughter Order
# ---------------------------------------------------------------------------

def create_slaughter_order(assembly: Assembly, animal: dict, yield_vector: dict,
                           processor: dict, score: float, meta: dict,
                           run_id: str, index: int,
                           dry_run: bool = True) -> dict:
    """Build slaughter order + allocations from an assembly.

    Each PO in the assembly becomes a slaughter_order_allocation.
    """
    order_number = f"SO-{run_id[:8]}-{index:03d}"
    hw = sum(yield_vector.values())
    pct_po = assembly.fullness * 100
    pct_lor = 100.0 - pct_po

    order = {
        'order_number': order_number,
        'species': assembly.species,
        'animal_id': animal['animal_id'],
        'profile_id': processor['profile_id'],
        'estimated_hanging_weight': round(hw, 2),
        'processing_cost_total': round(meta['processing_cost'], 2),
        'farmer_to_proc_distance': round(meta['farmer_dist'], 2),
        'avg_cust_to_proc_distance': round(meta['avg_cust_dist'], 2),
        'pct_allocated_to_orders': round(pct_po, 2),
        'pct_to_last_resort': round(pct_lor, 2),
        'optimizer_score': round(score, 4),
    }

    allocations = []
    for po in assembly.pos:
        allocations.append({
            'po_number': po['po_number'],
            'share': po['share'],
        })

    result = {
        'order': order,
        'allocations': allocations,
        'processor': processor,
        'trigger': 'ready' if assembly.fullness >= DEFAULT_FULLNESS_THRESHOLD else 'forced',
    }

    if not dry_run:
        save_slaughter_order(order, allocations)
        update_animal_status(animal['animal_id'], 'reserved')
        for po in assembly.pos:
            try:
                update_po_status(po['po_number'], 'planned')
            except (ValueError, Exception):
                pass

    return result


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def run_optimizer(species: str, dry_run: bool = True,
                  weights: dict = None,
                  fullness_threshold: float = None,
                  max_wait_days: int = None,
                  run_date: date = None) -> dict:
    """Run the PO assembly optimizer pipeline for a species."""
    run_id = str(uuid.uuid4())
    started = datetime.now()
    today = run_date or date.today()
    threshold = fullness_threshold or DEFAULT_FULLNESS_THRESHOLD
    max_wait = max_wait_days or DEFAULT_MAX_WAIT_DAYS

    print(f"\n{'='*60}")
    print(f"  PO Assembly Optimizer — {species.upper()}")
    print(f"  Run ID: {run_id}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'COMMIT'}")
    print(f"  Date: {today}  Threshold: {threshold*100:.0f}%  Max wait: {max_wait}d")
    print(f"{'='*60}\n")

    # Step 1: Fetch pending POs
    pending_pos = get_pending_pos(species)
    if not pending_pos:
        print("  No pending POs found.")
        return {
            'run_id': run_id, 'status': 'no_demand', 'results': [],
            'assemblies_triggered': 0, 'assemblies_held': 0,
            'held_po_count': 0, 'avg_fullness': 0,
        }

    print(f"  Step 1: {len(pending_pos)} pending POs")
    share_counts = defaultdict(int)
    for po in pending_pos:
        share_counts[po['share']] += 1
    for share, cnt in sorted(share_counts.items()):
        print(f"    {share:15s}  {cnt:3d} POs")

    # Step 2: Assemble
    grade_hierarchy = get_grade_hierarchy(species)
    assemblies = assemble_pos(pending_pos, species, grade_hierarchy)
    print(f"\n  Step 2: {len(assemblies)} assemblies formed")
    for i, asm in enumerate(assemblies, 1):
        shares = [po['share'] for po in asm.pos]
        print(f"    Assembly {i}: {asm.fullness*100:.0f}% full  "
              f"({asm.slot_label})  "
              f"POs: {', '.join(asm.po_numbers)}  [{', '.join(shares)}]")

    # Step 3: Evaluate triggers
    triggered = []
    held = []
    for asm in assemblies:
        trigger = evaluate_trigger(asm, today, threshold, max_wait)
        if trigger in ('ready', 'forced'):
            triggered.append((asm, trigger))
        else:
            held.append(asm)

    print(f"\n  Step 3: Triggers — {len(triggered)} triggered, {len(held)} held")
    for asm, trig in triggered:
        print(f"    TRIGGER ({trig}): {asm.fullness*100:.0f}% full  "
              f"POs: {', '.join(asm.po_numbers)}")
    for asm in held:
        oldest = asm.oldest_order_date
        age = (today - oldest).days if oldest else 0
        print(f"    HOLD: {asm.fullness*100:.0f}% full  age={age}d  "
              f"POs: {', '.join(asm.po_numbers)}")

    if not triggered:
        held_po_count = sum(len(a.pos) for a in held)
        all_fullness = [a.fullness for a in assemblies]
        avg_f = mean(all_fullness) if all_fullness else 0
        print(f"\n  No assemblies triggered. {held_po_count} POs waiting.")
        return {
            'run_id': run_id, 'status': 'held',
            'species': species, 'dry_run': dry_run,
            'assemblies_triggered': 0,
            'assemblies_held': len(held),
            'held_po_count': held_po_count,
            'avg_fullness': avg_f,
            'results': [],
        }

    # Step 4: Match animal, select processor, create order
    animals = get_available_animals(species)
    if not animals:
        print("  No available animals found.")
        return {
            'run_id': run_id, 'status': 'no_inventory', 'results': [],
            'assemblies_triggered': len(triggered), 'assemblies_held': len(held),
            'held_po_count': sum(len(a.pos) for a in held), 'avg_fullness': 0,
        }

    cut_specs = get_cut_specs(species)
    if not cut_specs:
        print(f"  ERROR: No cut specs found for {species}.")
        return {
            'run_id': run_id, 'status': 'no_cut_specs', 'results': [],
            'assemblies_triggered': len(triggered), 'assemblies_held': len(held),
            'held_po_count': sum(len(a.pos) for a in held), 'avg_fullness': 0,
        }

    processors = get_eligible_processors(species)
    if not processors:
        print(f"  WARNING: No eligible processors for {species}.")
        return {
            'run_id': run_id, 'status': 'no_processors', 'results': [],
            'assemblies_triggered': len(triggered), 'assemblies_held': len(held),
            'held_po_count': sum(len(a.pos) for a in held), 'avg_fullness': 0,
        }

    print(f"\n  Step 4: {len(animals)} animals, {len(cut_specs)} cut specs, "
          f"{len(processors)} processors")

    results = []
    used_animal_ids = set()

    for i, (asm, trigger) in enumerate(triggered, 1):
        avail = [a for a in animals if a['animal_id'] not in used_animal_ids]
        animal, yv = match_animal(asm, avail, cut_specs, grade_hierarchy, today=today)
        if animal is None:
            print(f"    Assembly {i}: No matching animal found — skipping")
            held.append(asm)
            continue

        proc, score, meta = select_processor(asm, animal, processors, species,
                                             today, weights)
        if proc is None:
            print(f"    Assembly {i}: No eligible processor — skipping")
            held.append(asm)
            continue

        result = create_slaughter_order(asm, animal, yv, proc, score, meta,
                                        run_id, i, dry_run=dry_run)
        results.append(result)
        used_animal_ids.add(animal['animal_id'])

        o = result['order']
        print(f"    SO {o['order_number']}  {o['animal_id']}  "
              f"-> {proc['company_name']}  "
              f"score={o['optimizer_score']:.1f}  "
              f"util={o['pct_allocated_to_orders']:.1f}%  "
              f"[{trigger}]")

    # Summary
    elapsed = (datetime.now() - started).total_seconds()
    held_po_count = sum(len(a.pos) for a in held)
    all_fullness = [a.fullness for a in assemblies]
    avg_fullness = mean(all_fullness) if all_fullness else 0

    if results:
        total_hw = sum(r['order']['estimated_hanging_weight'] for r in results)
        avg_util = mean([r['order']['pct_allocated_to_orders'] for r in results])
        avg_score = mean([r['order']['optimizer_score'] for r in results])
    else:
        total_hw = avg_util = avg_score = 0

    print(f"\n  {'='*50}")
    print(f"  {len(results)} slaughter orders created")
    print(f"  Total HW: {total_hw:,.0f} lbs")
    print(f"  Avg utilization: {avg_util:.1f}%")
    print(f"  Assemblies held: {len(held)} ({held_po_count} POs waiting)")
    print(f"  Avg assembly fullness: {avg_fullness*100:.0f}%")
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
        'assemblies_triggered': len(triggered),
        'assemblies_held': len(held),
        'held_po_count': held_po_count,
        'avg_fullness': avg_fullness,
        'results': results,
    }
