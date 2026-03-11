"""Carcass optimizer: PO Assembly Engine.

Accumulates purchase orders into assemblies that tile a carcass.
Cattle use a 4-slot model (2 front + 2 hind quarters).
Pork, lamb, and goat use a 2-slot model (2 sides: left + right).
Only triggers a slaughter order when an assembly reaches sufficient fullness.
"""
import uuid
from collections import defaultdict
from datetime import datetime, date, timedelta
from statistics import mean
from typing import Optional

import psycopg2.extras

from db import (get_connection, get_available_animals, update_animal_status,
                update_po_status)
from optimizer_db import (
    get_cut_specs, get_eligible_processors, get_grade_hierarchy,
    grade_meets_requirement, save_slaughter_order, fulfill_po_lines,
    get_processor_scheduled_count, get_lor_processor,
)
from geo import safe_distance
from config import (
    DRESS_PCT_BY_YG, DEFAULT_YIELD_GRADE,
    DEFAULT_PORK_DRESS_PCT, DEFAULT_LAMB_DRESS_PCT,
    DEFAULT_CHICKEN_DRESS_PCT, DEFAULT_GOAT_DRESS_PCT,
    PROCESSORS, PORK_PROCESSORS, LAMB_PROCESSORS,
    PROCESSING_RATES,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OPTIMIZER_WEIGHTS = {
    'customer_distance': 1.0,   # $/mile equivalent
    'farmer_distance': 0.7,
    'processing_cost': 0.5,     # normalized $/head
    'waste_penalty': 0.3,       # penalty per % of carcass to last resort
}

SPECIES_PORTION_CONFIG = {
    'cattle': {
        'allowed_portions': ['whole', 'half', 'quarter_front', 'quarter_hind'],
        'slots': {'whole': 4, 'half': 2, 'quarter_front': 1, 'quarter_hind': 1},
        'slots_per_carcass': 4,  # 2F + 2H
        'portion_slots': {
            'whole':         {'front': 2, 'hind': 2},
            'half':          {'front': 1, 'hind': 1},
            'quarter_front': {'front': 1, 'hind': 0},
            'quarter_hind':  {'front': 0, 'hind': 1},
        },
    },
    'pork': {
        'allowed_portions': ['whole', 'half'],
        'slots': {'whole': 2, 'half': 1},
        'slots_per_carcass': 2,  # 2 sides
    },
    'lamb': {
        'allowed_portions': ['whole', 'half'],
        'slots': {'whole': 2, 'half': 1},
        'slots_per_carcass': 2,
    },
    'goat': {
        'allowed_portions': ['whole', 'half'],
        'slots': {'whole': 2, 'half': 1},
        'slots_per_carcass': 2,
    },
}

# Legacy constants for backward compatibility in non-species-aware code
PORTION_SLOTS = SPECIES_PORTION_CONFIG['cattle']['portion_slots']
SLOTS_PER_CARCASS = {'front': 2, 'hind': 2}

DEFAULT_FULLNESS_THRESHOLD = 0.90
DEFAULT_MAX_WAIT_DAYS = 30

# Target live weight ranges by species (for match_animal size scoring)
SPECIES_TARGET_WEIGHT = {
    'cattle': (1200, 1400),
    'pork':   (260, 290),
    'lamb':   (120, 145),
    'goat':   (75, 95),
}

# match_animal scoring weights
MATCH_WEIGHTS = {
    'farmer_distance': 1.0,
    'size_penalty': 0.8,
}

# Age-based proximity relaxation for pork/lamb/goat (days -> weight multiplier)
AGE_PROXIMITY_RELAXATION = [
    (45, 0.25),  # 45+ days: farmer_distance weight × 0.25
    (30, 0.50),  # 30-44 days: farmer_distance weight × 0.50
    (0,  1.00),  # 0-29 days: normal weight
]


# ---------------------------------------------------------------------------
# Kept from previous version
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


# ---------------------------------------------------------------------------
# Assembly class
# ---------------------------------------------------------------------------

class Assembly:
    """Tracks a virtual carcass being assembled from complementary POs.

    Cattle: 4-slot model (2 front + 2 hind quarters).
    Pork/lamb/goat: 2-slot model (2 sides, left + right).
    """

    def __init__(self, species: str):
        self.species = species
        self.pos = []              # list of PO dicts
        self.strictest_grade = None
        self.customer_locations = []  # [(lat, lng)]

        self._config = SPECIES_PORTION_CONFIG.get(species,
                                                   SPECIES_PORTION_CONFIG['cattle'])
        self._is_cattle = (species == 'cattle')

        # Cattle tracks front/hind separately; others track sides_used
        if self._is_cattle:
            self.front_used = 0
            self.hind_used = 0
        else:
            self.sides_used = 0

    def _slot_count(self, portion: str) -> int:
        """Total slots consumed by a portion."""
        return self._config['slots'].get(portion, 0)

    def can_add(self, po: dict, grade_hierarchy: dict) -> bool:
        """Check if a PO fits in this assembly (slot + grade compatibility)."""
        portion = po['carcass_portion']
        if portion not in self._config['allowed_portions']:
            return False

        if self._is_cattle:
            slots = self._config['portion_slots'].get(portion)
            if not slots:
                return False
            if self.front_used + slots['front'] > 2:
                return False
            if self.hind_used + slots['hind'] > 2:
                return False
        else:
            slot_cost = self._config['slots'].get(portion, 0)
            if self.sides_used + slot_cost > self._config['slots_per_carcass']:
                return False
        return True

    def add(self, po: dict, grade_hierarchy: dict):
        """Consume slots and update strictest grade."""
        portion = po['carcass_portion']
        if self._is_cattle:
            slots = self._config['portion_slots'][portion]
            self.front_used += slots['front']
            self.hind_used += slots['hind']
        else:
            self.sides_used += self._config['slots'][portion]

        self.pos.append(po)
        if po.get('cust_lat') is not None and po.get('cust_lng') is not None:
            self.customer_locations.append((po['cust_lat'], po['cust_lng']))
        # Update strictest grade
        po_grade = po.get('quality_grade')
        if po_grade and grade_hierarchy:
            po_rank = grade_hierarchy.get(po_grade, 0)
            if self.strictest_grade is None:
                self.strictest_grade = po_grade
            else:
                current_rank = grade_hierarchy.get(self.strictest_grade, 0)
                if po_rank > current_rank:
                    self.strictest_grade = po_grade

    @property
    def slots_used(self) -> int:
        """Total slots consumed."""
        if self._is_cattle:
            return self.front_used + self.hind_used
        return self.sides_used

    @property
    def fullness(self) -> float:
        """Fraction of carcass slots used (0.0 to 1.0)."""
        return self.slots_used / self._config['slots_per_carcass']

    @property
    def slot_label(self) -> str:
        """Human-readable slot usage string for display."""
        if self._is_cattle:
            return f"{self.front_used}F+{self.hind_used}H"
        return f"{self.sides_used}S"

    @property
    def oldest_order_date(self) -> Optional[date]:
        """Earliest order_date among POs in this assembly."""
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

def _avg_location(assembly: 'Assembly') -> tuple:
    """Average lat/lng of assembly's customers."""
    lats = [loc[0] for loc in assembly.customer_locations if loc[0] is not None]
    lngs = [loc[1] for loc in assembly.customer_locations if loc[1] is not None]
    if not lats:
        return (None, None)
    return (mean(lats), mean(lngs))


def assemble_pos(pos: list, species: str, grade_hierarchy: dict) -> list:
    """Group POs into assemblies that tile a carcass.

    Sort POs by slot size desc (whole first), then order_date asc.
    For each PO, find best open assembly by can_add() feasibility + geographic proximity.
    If no fit, start new Assembly.

    Returns all assemblies (complete + partial).
    """
    sp_config = SPECIES_PORTION_CONFIG.get(species, SPECIES_PORTION_CONFIG['cattle'])
    # Sort: largest portion first, then oldest order first
    slot_size = lambda po: sp_config['slots'].get(po['carcass_portion'], 0)
    sorted_pos = sorted(pos, key=lambda po: (-slot_size(po),
                                              po.get('order_date') or date.max))

    assemblies = []

    for po in sorted_pos:
        best_assembly = None
        best_distance = float('inf')
        po_lat = po.get('cust_lat')
        po_lng = po.get('cust_lng')

        for asm in assemblies:
            if asm.fullness >= 1.0:
                continue  # already full
            if not asm.can_add(po, grade_hierarchy):
                continue
            # Score by geographic proximity
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
# Phase 4: Evaluate Trigger
# ---------------------------------------------------------------------------

def evaluate_trigger(assembly: Assembly, today: date,
                     threshold: float = DEFAULT_FULLNESS_THRESHOLD,
                     max_wait: int = DEFAULT_MAX_WAIT_DAYS) -> str:
    """Determine if an assembly should be triggered.

    Returns 'ready', 'forced', or 'hold'.

    Species-specific rules:
    - Cattle: 'ready' at >= threshold, 'forced' if oldest PO > max_wait AND >= 75%
    - Pork/lamb/goat: 'ready' at >= threshold, never forced (rely on age-based
      proximity relaxation in match_animal instead)
    """
    if assembly.fullness >= threshold:
        return 'ready'

    # Only cattle gets forced triggers
    if assembly.species == 'cattle':
        oldest = assembly.oldest_order_date
        if oldest and (today - oldest).days > max_wait and assembly.fullness >= 0.75:
            return 'forced'

    return 'hold'


# ---------------------------------------------------------------------------
# Phase 2: Match Animal
# ---------------------------------------------------------------------------

def _get_po_age_days(assembly: Assembly, today: date) -> int:
    """Get the age in days of the oldest PO in an assembly."""
    oldest = assembly.oldest_order_date
    if oldest:
        return (today - oldest).days
    return 0


def _get_farmer_distance_weight(species: str, po_age_days: int) -> float:
    """Get age-based farmer_distance weight multiplier.

    For pork/lamb/goat, stale POs get relaxed proximity requirements
    instead of forced triggers.
    """
    if species == 'cattle':
        return 1.0  # cattle uses forced triggers, not relaxation
    for min_days, multiplier in AGE_PROXIMITY_RELAXATION:
        if po_age_days >= min_days:
            return multiplier
    return 1.0


def _compute_size_penalty(animal: dict, species: str) -> float:
    """Compute size penalty: deviation from species target weight range.

    Asymmetric — too small penalized 2x more than too large.
    Returns 0 if within target range.
    """
    target = SPECIES_TARGET_WEIGHT.get(species)
    if not target:
        return 0.0
    live_wt = float(animal.get('live_weight_est') or 0)
    if live_wt <= 0:
        return 0.0
    low, high = target
    if live_wt < low:
        # Too small: penalize more heavily (customer gets less product)
        return ((low - live_wt) / low) * 2.0
    elif live_wt > high:
        # Too large: mild penalty (more waste/LOR)
        return (live_wt - high) / high
    return 0.0


def match_animal(assembly: Assembly, animals: list, cut_specs: list,
                 grade_hierarchy: dict, today: date = None) -> tuple:
    """Find the best animal for an assembly.

    Ranking: grade → weighted(farmer_distance + size_score).
    - Grade: hard filter (must meet strictest), prefer exact match over higher
      (don't waste a prime animal on a choice order).
    - Farmer proximity + size: weighted composite, subject to age-based relaxation.

    Returns (animal, yield_vector) or (None, None).
    """
    if not animals or not cut_specs:
        return None, None

    today = today or date.today()

    # Get PO lines for all POs in assembly to check yield sufficiency
    po_lines_by_po = {}
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for po in assembly.pos:
                cur.execute("""
                    SELECT id AS po_line_id, cut_code, description, primal,
                           quantity_lbs, fulfilled_lbs, po_number
                    FROM po_lines
                    WHERE po_number = %s AND status IN ('pending', 'partial')
                """, (po['po_number'],))
                lines = []
                for row in cur.fetchall():
                    remaining = float(row['quantity_lbs']) - float(row['fulfilled_lbs'] or 0)
                    if remaining > 0:
                        lines.append({**dict(row), 'remaining_lbs': remaining})
                po_lines_by_po[po['po_number']] = lines
    finally:
        conn.close()

    # Aggregate demand by cut_code
    demand_by_cut = defaultdict(float)
    for lines in po_lines_by_po.values():
        for line in lines:
            demand_by_cut[line['cut_code']] += line['remaining_lbs']

    # Average customer location for proximity scoring
    avg_lat, avg_lng = _avg_location(assembly)

    # Age-based proximity relaxation
    po_age = _get_po_age_days(assembly, today)
    farmer_dist_weight = _get_farmer_distance_weight(assembly.species, po_age)

    # Grade ranking info
    required_rank = 0
    if assembly.strictest_grade and grade_hierarchy:
        required_rank = grade_hierarchy.get(assembly.strictest_grade, 0)

    candidates = []
    for animal in animals:
        if animal.get('status') != 'available':
            continue
        # Grade hard filter
        animal_grade = animal.get('quality_grade_est')
        if assembly.strictest_grade and grade_hierarchy:
            if animal_grade:
                a_rank = grade_hierarchy.get(animal_grade, 0)
                if a_rank < required_rank:
                    continue

        hw = estimate_hanging_weight(animal)
        if hw <= 0:
            continue
        yv = compute_yield_vector(hw, cut_specs)

        # Check yield sufficiency
        total_demand = sum(demand_by_cut.values())
        total_available = sum(yv.get(cut, 0) for cut in demand_by_cut)
        if total_demand > 0 and total_available < total_demand * 0.80:
            continue

        # Grade preference: prefer exact match (0) over higher grade (1)
        grade_excess = 0
        if animal_grade and grade_hierarchy and required_rank > 0:
            a_rank = grade_hierarchy.get(animal_grade, 0)
            grade_excess = a_rank - required_rank  # 0 = exact match, >0 = higher than needed

        # Composite score: farmer proximity + size penalty
        farmer_lat, farmer_lng = _get_farmer_location(animal['farmer_id'])
        farmer_dist = safe_distance(farmer_lat, farmer_lng, avg_lat, avg_lng)
        size_penalty = _compute_size_penalty(animal, assembly.species)

        score = (MATCH_WEIGHTS['farmer_distance'] * farmer_dist_weight * farmer_dist +
                 MATCH_WEIGHTS['size_penalty'] * size_penalty)

        candidates.append((animal, yv, grade_excess, score, farmer_dist))

    if not candidates:
        return None, None

    # Sort by: grade_excess asc (exact match first), then composite score asc
    candidates.sort(key=lambda c: (c[2], c[3]))
    best = candidates[0]
    return best[0], best[1]


# ---------------------------------------------------------------------------
# Phase 3: Select Processor (with capacity enforcement)
# ---------------------------------------------------------------------------

def select_processor(assembly: Assembly, animal: dict, processors: list,
                     species: str, run_date: date,
                     weights: dict = None) -> tuple:
    """Select the best processor for an assembly + animal.

    Same scoring as before but with capacity enforcement: skip processor if
    daily_capacity_head already reached for that date.

    Returns (processor_dict, score, meta) or (None, None, None).
    """
    w = weights or OPTIMIZER_WEIGHTS
    farmer_lat, farmer_lng = _get_farmer_location(animal['farmer_id'])
    hw = estimate_hanging_weight(animal)

    best = None
    best_score = float('inf')
    best_meta = {}

    for proc in processors:
        pkey = proc['processor_key']

        # Capacity check
        capacity = proc.get('daily_capacity_head')
        if capacity:
            scheduled = get_processor_scheduled_count(pkey, run_date, species)
            if scheduled >= capacity:
                continue

        # Processing cost — try processor-specific config, fall back to species rates
        cost_cfg = None
        if species == 'cattle':
            cost_cfg = PROCESSORS.get(pkey)
        elif species == 'pork':
            cost_cfg = PORK_PROCESSORS.get(pkey)
        elif species == 'lamb':
            cost_cfg = LAMB_PROCESSORS.get(pkey)

        if not cost_cfg:
            cost_cfg = PROCESSING_RATES.get(species)

        if cost_cfg:
            proc_cost = cost_cfg['kill_fee'] + (cost_cfg['fab_cost_per_lb'] * hw)
        else:
            proc_cost = 500.0

        # Average customer-to-processor distance
        cust_dists = [
            safe_distance(clat, clng, proc.get('latitude'), proc.get('longitude'))
            for clat, clng in assembly.customer_locations
        ]
        avg_cust_dist = mean(cust_dists) if cust_dists else 9999.0

        # Farmer-to-processor distance
        farmer_dist = safe_distance(farmer_lat, farmer_lng,
                                    proc.get('latitude'), proc.get('longitude'))

        # Waste penalty — based on assembly fullness gap
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
# Phase 5: Create Slaughter Order
# ---------------------------------------------------------------------------

def create_slaughter_order(assembly: Assembly, animal: dict, yield_vector: dict,
                           processor: dict, score: float, meta: dict,
                           run_id: str, index: int,
                           dry_run: bool = True) -> dict:
    """Build slaughter order header + lines from an assembly.

    Each PO's po_lines become slaughter_order_lines with po_number, po_line_id.
    Gap between assembly coverage and 100% goes to LOR lines.
    """
    order_number = f"SO-{run_id[:8]}-{index:03d}"
    hw = sum(yield_vector.values())
    pct_po = assembly.fullness * 100
    pct_lor = 100.0 - pct_po

    order = {
        'order_number': order_number,
        'species': assembly.species,
        'animal_id': animal['animal_id'],
        'processor_key': processor['processor_key'],
        'optimizer_run_id': run_id,
        'estimated_hanging_weight': round(hw, 2),
        'processing_cost_total': round(meta['processing_cost'], 2),
        'farmer_to_proc_distance': round(meta['farmer_dist'], 2),
        'avg_cust_to_proc_distance': round(meta['avg_cust_dist'], 2),
        'pct_allocated_to_orders': round(pct_po, 2),
        'pct_to_last_resort': round(pct_lor, 2),
        'optimizer_score': round(score, 4),
    }

    # Fetch po_lines for all POs in assembly
    conn = get_connection()
    po_lines_all = []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for po in assembly.pos:
                cur.execute("""
                    SELECT id AS po_line_id, cut_code, description, primal,
                           quantity_lbs, fulfilled_lbs, po_number
                    FROM po_lines
                    WHERE po_number = %s AND status IN ('pending', 'partial')
                """, (po['po_number'],))
                for row in cur.fetchall():
                    remaining = float(row['quantity_lbs']) - float(row['fulfilled_lbs'] or 0)
                    if remaining > 0:
                        po_lines_all.append({**dict(row), 'remaining_lbs': remaining})
    finally:
        conn.close()

    # Allocate PO lines to yield
    remaining_yield = dict(yield_vector)  # {cut_code: available_lbs}
    lines = []
    po_fulfillments = []

    for pl in po_lines_all:
        cut = pl['cut_code']
        avail = remaining_yield.get(cut, 0)
        if avail <= 0:
            continue
        allocated = min(pl['remaining_lbs'], avail)
        remaining_yield[cut] = avail - allocated

        lines.append({
            'cut_code': cut,
            'total_lbs': round(yield_vector.get(cut, 0), 2),
            'allocated_to_po': round(allocated, 2),
            'allocated_to_lor': 0,
            'po_number': pl['po_number'],
            'po_line_id': pl['po_line_id'],
        })
        po_fulfillments.append({
            'po_line_id': pl['po_line_id'],
            'lbs': round(allocated, 2),
        })

    # LOR lines for remaining yield
    for cut_code, remaining_lbs in remaining_yield.items():
        if remaining_lbs > 0.1:  # skip dust
            lines.append({
                'cut_code': cut_code,
                'total_lbs': round(yield_vector.get(cut_code, 0), 2),
                'allocated_to_po': 0,
                'allocated_to_lor': round(remaining_lbs, 2),
                'po_number': None,
                'po_line_id': None,
            })

    result = {
        'order': order,
        'lines': lines,
        'fulfillments': po_fulfillments,
        'processor': processor,
        'trigger': 'ready' if assembly.fullness >= DEFAULT_FULLNESS_THRESHOLD else 'forced',
    }

    if not dry_run:
        save_slaughter_order(order, lines)
        fulfill_po_lines(po_fulfillments)
        update_animal_status(animal['animal_id'], 'reserved')
        # Update PO statuses to 'planned'
        for po in assembly.pos:
            try:
                update_po_status(po['po_number'], 'planned')
            except (ValueError, Exception):
                pass  # PO may already be in another status

    return result


# ---------------------------------------------------------------------------
# DB helpers (assembly-specific queries)
# ---------------------------------------------------------------------------

def get_pending_pos_for_assembly(species: str) -> list:
    """Fetch pending POs for assembly — one row per PO (not aggregated).

    Returns: list of dicts with po_number, customer_id, quality_grade,
             carcass_portion, order_date, cust_lat, cust_lng, total_pending_lbs.
    Sorted by order_date ASC.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT po.po_number, po.customer_id, po.quality_grade,
                       po.carcass_portion, po.order_date,
                       dc.latitude AS cust_lat, dc.longitude AS cust_lng,
                       COALESCE(SUM(pl.quantity_lbs - pl.fulfilled_lbs), 0) AS total_pending_lbs
                FROM purchase_orders po
                JOIN dtc_customers dc ON dc.customer_id = po.customer_id
                JOIN po_lines pl ON pl.po_number = po.po_number
                WHERE po.species = %s
                  AND po.status = 'pending'
                  AND pl.status IN ('pending', 'partial')
                GROUP BY po.po_number, po.customer_id, po.quality_grade,
                         po.carcass_portion, po.order_date,
                         dc.latitude, dc.longitude
                HAVING SUM(pl.quantity_lbs - pl.fulfilled_lbs) > 0
                ORDER BY po.order_date ASC
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def run_optimizer(species: str, dry_run: bool = True,
                  weights: dict = None,
                  fullness_threshold: float = None,
                  max_wait_days: int = None,
                  run_date: date = None) -> dict:
    """Run the PO assembly optimizer pipeline for a species.

    Pipeline:
    1. Fetch pending POs
    2. Assemble into carcass-tiling groups
    3. Evaluate triggers on each assembly
    4. For triggered assemblies: match animal -> select processor -> create slaughter order
    5. Return summary
    """
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
    pending_pos = get_pending_pos_for_assembly(species)
    if not pending_pos:
        print("  No pending POs found.")
        return {
            'run_id': run_id, 'status': 'no_demand', 'results': [],
            'assemblies_triggered': 0, 'assemblies_held': 0,
            'held_po_count': 0, 'avg_fullness': 0,
        }

    print(f"  Step 1: {len(pending_pos)} pending POs")
    portion_counts = defaultdict(int)
    for po in pending_pos:
        portion_counts[po['carcass_portion']] += 1
    for portion, cnt in sorted(portion_counts.items()):
        print(f"    {portion:15s}  {cnt:3d} POs")

    # Step 2: Assemble
    grade_hierarchy = get_grade_hierarchy(species)
    assemblies = assemble_pos(pending_pos, species, grade_hierarchy)
    print(f"\n  Step 2: {len(assemblies)} assemblies formed")
    for i, asm in enumerate(assemblies, 1):
        portions = [po['carcass_portion'] for po in asm.pos]
        print(f"    Assembly {i}: {asm.fullness*100:.0f}% full  "
              f"({asm.slot_label})  "
              f"POs: {', '.join(asm.po_numbers)}  [{', '.join(portions)}]")

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

    # Step 4: For triggered assemblies — match animal, select processor, create order
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
        # Filter out already-used animals
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
