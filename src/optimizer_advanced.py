"""
Terra Mensa Optimizer — Advanced Features (Phase 4)

1. Processor blackout / capability filtering
2. Processor reliability scoring
3. Geographic zone clustering
4. Quality-based matching (grade, breed, finish)
5. Demand snapshot recording (for future forecasting)
6. Optimizer run logging
7. Hold-back logic for rolling horizon
8. Pareto frontier analysis

Usage:
    from optimizer_advanced import AdvancedFeatures
    adv = AdvancedFeatures(conn)
    processors = adv.filter_processors(processors, species, target_date)
    zone_map = adv.assign_zones(customer_profiles)
"""

import math
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta
from collections import defaultdict


class AdvancedFeatures:
    """Advanced optimizer features that augment the base MIP solver."""

    def __init__(self, conn):
        self.conn = conn
        self._load_blackouts()
        self._load_zones()
        self._load_reliability()

    # ─── Data Loading ────────────────────────────────────────────────────

    def _load_blackouts(self):
        """Load processor blackout periods."""
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT processor_id, start_date, end_date, reason, species FROM processor_blackouts")
                self.blackouts = [dict(r) for r in cur.fetchall()]
        except psycopg2.errors.UndefinedTable:
            self.conn.rollback()
            self.blackouts = []

    def _load_zones(self):
        """Load geographic zones."""
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT zone_name, center_lat, center_lng, radius_miles FROM geographic_zones")
                self.zones = [dict(r) for r in cur.fetchall()]
        except psycopg2.errors.UndefinedTable:
            self.conn.rollback()
            self.zones = []

    def _load_reliability(self):
        """Load processor reliability scores (avg days variance, avg quality)."""
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT processor_id,
                           COUNT(*) as total_orders,
                           AVG(ABS(days_variance)) as avg_delay,
                           AVG(quality_score) as avg_quality,
                           SUM(CASE WHEN days_variance <= 0 THEN 1 ELSE 0 END)::float / COUNT(*) as on_time_rate
                    FROM processor_performance
                    GROUP BY processor_id
                """)
                self.reliability = {str(r['processor_id']): dict(r) for r in cur.fetchall()}
        except psycopg2.errors.UndefinedTable:
            self.conn.rollback()
            self.reliability = {}

    # ─── 1. Processor Filtering ──────────────────────────────────────────

    def filter_processors(self, processors, species, target_date=None,
                          required_capabilities=None):
        """Filter processors by blackouts, capabilities, and species.

        Args:
            processors: list of processor dicts from get_processors_for_species()
            species: species being processed
            target_date: planned processing date (default: today + 14 days)
            required_capabilities: list of required services (e.g., ['smoking', 'curing'])

        Returns: filtered list of processors
        """
        if target_date is None:
            target_date = date.today() + timedelta(days=14)
        if isinstance(target_date, datetime):
            target_date = target_date.date()

        filtered = []
        for proc in processors:
            pid = str(proc['processor_id'])

            # Check blackouts
            blacked_out = False
            for bo in self.blackouts:
                if str(bo['processor_id']) != pid:
                    continue
                if bo['species'] and bo['species'] != species:
                    continue
                if bo['start_date'] <= target_date <= bo['end_date']:
                    blacked_out = True
                    break

            if blacked_out:
                print(f"      BLACKOUT: {proc['company_name']} blocked on {target_date}")
                continue

            # Check capabilities
            if required_capabilities:
                proc_caps = proc.get('capabilities') or []
                missing = [c for c in required_capabilities if c not in proc_caps]
                if missing:
                    print(f"      CAPABILITY: {proc['company_name']} missing {missing}")
                    continue

            filtered.append(proc)

        return filtered

    # ─── 2. Reliability Scoring ──────────────────────────────────────────

    def get_reliability_score(self, processor_id):
        """Get reliability score for a processor (0-1, higher is better).

        Based on on-time rate and quality score.
        Returns 0.8 default if no history exists.
        """
        pid = str(processor_id)
        if pid not in self.reliability:
            return 0.8  # default for unknown processors

        data = self.reliability[pid]
        on_time = float(data.get('on_time_rate', 0.8))
        quality = float(data.get('avg_quality', 3.5)) / 5.0  # normalize to 0-1

        # Weighted: 60% on-time, 40% quality
        return round(on_time * 0.6 + quality * 0.4, 3)

    def adjust_cost_for_reliability(self, base_cost, processor_id):
        """Adjust cost by reliability: EffectiveCost = BaseCost / ReliabilityScore.

        A processor with 0.9 reliability has costs inflated by ~11%.
        A perfect 1.0 reliability processor pays no penalty.
        """
        score = self.get_reliability_score(processor_id)
        if score <= 0:
            score = 0.1  # floor
        return round(base_cost / score, 2)

    # ─── 3. Geographic Clustering ────────────────────────────────────────

    def assign_zone(self, lat, lng):
        """Assign a lat/lng to the nearest geographic zone.

        Returns: zone_name or 'unzoned' if no zone within radius.
        """
        best_zone = 'unzoned'
        best_dist = float('inf')

        for zone in self.zones:
            d = self._haversine(lat, lng, float(zone['center_lat']), float(zone['center_lng']))
            radius = float(zone.get('radius_miles', 15))
            if d <= radius and d < best_dist:
                best_dist = d
                best_zone = zone['zone_name']

        return best_zone

    def assign_zones_bulk(self, profiles):
        """Assign zones to a dict of customer profiles.

        Args:
            profiles: dict of {profile_id: {id, latitude, longitude, ...}}

        Returns: dict of {profile_id: zone_name}
        """
        zones = {}
        for pid, prof in profiles.items():
            lat = float(prof.get('latitude') or 0)
            lng = float(prof.get('longitude') or 0)
            if lat and lng:
                zones[pid] = self.assign_zone(lat, lng)
            else:
                zones[pid] = 'unzoned'
        return zones

    def compute_cross_zone_penalty(self, po_zones, batch_pos):
        """Compute a penalty for POs in different zones within a batch.

        Returns: number of distinct zones in the batch (1 = all same zone = no penalty).
        """
        zones_in_batch = set()
        for po in batch_pos:
            pid = str(po['profile_id'])
            zone = po_zones.get(pid, 'unzoned')
            zones_in_batch.add(zone)

        return len(zones_in_batch)

    # ─── 4. Quality Matching ────────────────────────────────────────────

    def score_quality_match(self, animal, customer_preferences=None):
        """Score how well an animal matches customer quality expectations.

        Returns: 0.0 (worst) to 1.0 (best) match score.

        Current simple model:
            premium tier → 1.0
            standard tier → 0.7
            economy tier → 0.4
            grass-finished → +0.1 bonus
            known breed → +0.05 bonus
        """
        score = 0.7  # default standard

        tier = animal.get('quality_tier', 'standard')
        if tier == 'premium':
            score = 1.0
        elif tier == 'economy':
            score = 0.4

        finish = animal.get('finish_method', 'grain')
        if finish == 'grass':
            score = min(score + 0.1, 1.0)

        breed = animal.get('breed')
        if breed:
            score = min(score + 0.05, 1.0)

        return round(score, 2)

    def rank_inventory_by_quality(self, inventory, preference='standard'):
        """Sort inventory by quality score, optionally filtering by preference.

        Returns: sorted list (best first).
        """
        scored = [(self.score_quality_match(animal), animal) for animal in inventory]
        scored.sort(key=lambda x: x[0], reverse=True)
        return [animal for _, animal in scored]

    # ─── 5. Demand Snapshots ────────────────────────────────────────────

    def record_demand_snapshot(self):
        """Record current demand state for future forecasting.

        Captures pending/confirmed counts by species and share.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO demand_snapshots (snapshot_date, species, share, pending_count, confirmed_count, total_fraction)
                SELECT
                    CURRENT_DATE,
                    species,
                    share,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END),
                    SUM(CASE
                        WHEN status = 'pending' THEN
                            CASE share
                                WHEN 'whole' THEN 1.0 WHEN 'uncut' THEN 1.0
                                WHEN 'half' THEN 0.5 WHEN 'quarter' THEN 0.25
                                WHEN 'eighth' THEN 0.125 ELSE 0
                            END
                        ELSE 0
                    END)
                FROM purchase_orders
                GROUP BY species, share
                ON CONFLICT (snapshot_date, species, share) DO UPDATE SET
                    pending_count = EXCLUDED.pending_count,
                    confirmed_count = EXCLUDED.confirmed_count,
                    total_fraction = EXCLUDED.total_fraction
            """)
        self.conn.commit()

    def get_demand_forecast(self, species, lookback_days=90):
        """Simple demand forecast based on historical snapshots.

        Returns: dict with avg_daily_fraction, trend, expected_fill_days
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute("""
                    SELECT snapshot_date, SUM(total_fraction) as daily_fraction
                    FROM demand_snapshots
                    WHERE species = %s AND snapshot_date >= CURRENT_DATE - %s
                    GROUP BY snapshot_date
                    ORDER BY snapshot_date
                """, (species, lookback_days))
                rows = cur.fetchall()
            except psycopg2.errors.UndefinedTable:
                self.conn.rollback()
                return {'avg_daily_fraction': 0, 'trend': 'unknown', 'data_points': 0}

        if not rows:
            return {'avg_daily_fraction': 0, 'trend': 'unknown', 'data_points': 0}

        fractions = [float(r['daily_fraction']) for r in rows]
        avg = sum(fractions) / len(fractions)

        # Simple trend: compare first half vs second half
        mid = len(fractions) // 2
        if mid > 0:
            first_half = sum(fractions[:mid]) / mid
            second_half = sum(fractions[mid:]) / (len(fractions) - mid)
            if second_half > first_half * 1.1:
                trend = 'increasing'
            elif second_half < first_half * 0.9:
                trend = 'decreasing'
            else:
                trend = 'stable'
        else:
            trend = 'insufficient_data'

        # Expected days to fill one animal
        expected_fill_days = round(1.0 / avg, 1) if avg > 0 else float('inf')

        return {
            'avg_daily_fraction': round(avg, 4),
            'trend': trend,
            'expected_fill_days': expected_fill_days,
            'data_points': len(fractions),
        }

    # ─── 6. Optimizer Run Logging ───────────────────────────────────────

    def log_optimizer_run(self, mode, species, pos_pending, pos_assigned,
                          batches_formed, batches_assigned, total_cost,
                          solve_time, solver_status, notes=None):
        """Record an optimizer run for auditing and analysis."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO optimizer_runs
                        (mode, species, pos_pending, pos_assigned, batches_formed,
                         batches_assigned, total_cost, solve_time_seconds, solver_status, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (mode, species, pos_pending, pos_assigned, batches_formed,
                      batches_assigned, total_cost, solve_time, solver_status, notes))
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    # ─── 7. Hold-Back Logic ─────────────────────────────────────────────

    def should_hold_batch(self, batch_pos, fill_fraction, config):
        """Determine if a batch should be held for the next optimizer cycle.

        Hold-back rules (from Batched Bin Packing research, Gutin et al.):
        - Batch >= 85% full → dispatch immediately
        - Batch 60-84% full with oldest PO > 7 days → dispatch
        - Batch < 60% full or all POs < 3 days old → hold

        Returns: (should_hold, reason)
        """
        now = datetime.now()

        # Calculate oldest PO age
        oldest_age = 0
        for po in batch_pos:
            created = po.get('created_at')
            if created:
                if hasattr(created, 'tzinfo') and created.tzinfo:
                    created = created.replace(tzinfo=None)
                age = (now - created).days
                oldest_age = max(oldest_age, age)

        hold_dispatch_threshold = config.get('hold_dispatch_threshold', 0.85)
        hold_age_threshold = config.get('hold_age_threshold', 7)
        hold_min_fill = config.get('hold_min_fill', 0.60)
        hold_min_age = config.get('hold_min_age', 3)

        if fill_fraction >= hold_dispatch_threshold:
            return False, f"fill {fill_fraction:.2f} >= {hold_dispatch_threshold} → dispatch"

        if fill_fraction >= hold_min_fill and oldest_age >= hold_age_threshold:
            return False, f"fill {fill_fraction:.2f} >= {hold_min_fill} and oldest PO {oldest_age}d >= {hold_age_threshold}d → dispatch"

        if oldest_age < hold_min_age:
            return True, f"all POs < {hold_min_age} days old → hold for next cycle"

        if fill_fraction < hold_min_fill:
            return True, f"fill {fill_fraction:.2f} < {hold_min_fill} → hold for more orders"

        return False, "default: dispatch"

    # ─── 8. Pareto Frontier Analysis ────────────────────────────────────

    def pareto_analysis(self, solutions):
        """Identify Pareto-optimal solutions from a set of multi-objective results.

        Each solution is a dict with objective values:
            {'cost': float, 'wait': float, 'utilization_var': float, 'pos_assigned': int}

        Returns: list of Pareto-optimal solutions (non-dominated).

        A solution is Pareto-optimal if no other solution is better in ALL objectives.
        """
        if not solutions:
            return []

        # Objectives: minimize cost, minimize wait, minimize util_var, maximize pos_assigned
        def dominates(a, b):
            """True if a dominates b (a is at least as good in all, better in at least one)."""
            a_vals = (a['cost'], a['wait'], a['utilization_var'], -a['pos_assigned'])
            b_vals = (b['cost'], b['wait'], b['utilization_var'], -b['pos_assigned'])
            at_least_as_good = all(av <= bv for av, bv in zip(a_vals, b_vals))
            strictly_better = any(av < bv for av, bv in zip(a_vals, b_vals))
            return at_least_as_good and strictly_better

        pareto = []
        for i, sol in enumerate(solutions):
            dominated = False
            for j, other in enumerate(solutions):
                if i != j and dominates(other, sol):
                    dominated = True
                    break
            if not dominated:
                pareto.append(sol)

        return pareto

    # ─── Utilities ──────────────────────────────────────────────────────

    @staticmethod
    def _haversine(lat1, lng1, lat2, lng2):
        """Approximate distance in miles."""
        R = 3959
        dlat = math.radians(lat2 - lat1)
        dlng = math.radians(lng2 - lng1)
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlng / 2) ** 2)
        return R * 2 * math.asin(math.sqrt(a))


# ─── CLI Demo ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    sys.path.insert(0, '.')
    from optimizer_config import get_connection

    use_supabase = '--supabase' in sys.argv
    conn = get_connection(use_supabase=use_supabase)

    try:
        adv = AdvancedFeatures(conn)
    except Exception as e:
        print(f"Error: {e}")
        conn.close()
        sys.exit(1)

    print("═══ Terra Mensa Advanced Features ═══\n")

    # Geographic zones
    print("── Geographic Zones ──")
    test_points = [
        ("South Bend customer", 41.68, -86.25),
        ("Elkhart customer", 41.69, -85.98),
        ("Goshen customer", 41.58, -85.83),
        ("Rural customer", 41.40, -86.50),
    ]
    for name, lat, lng in test_points:
        zone = adv.assign_zone(lat, lng)
        print(f"  {name:<25} ({lat}, {lng}) → {zone}")

    # Hold-back logic
    print("\n── Hold-Back Logic ──")
    config = {
        'hold_dispatch_threshold': 0.85,
        'hold_age_threshold': 7,
        'hold_min_fill': 0.60,
        'hold_min_age': 3,
    }

    test_batches = [
        ("90% full, 2 day old", 0.90, 2),
        ("75% full, 10 day old", 0.75, 10),
        ("50% full, 1 day old", 0.50, 1),
        ("50% full, 8 day old", 0.50, 8),
        ("25% full, 15 day old", 0.25, 15),
    ]
    for desc, fill, age_days in test_batches:
        fake_pos = [{'created_at': datetime.now() - timedelta(days=age_days)}]
        hold, reason = adv.should_hold_batch(fake_pos, fill, config)
        action = "HOLD" if hold else "DISPATCH"
        print(f"  {desc:<30} → {action}: {reason}")

    # Demand forecast
    print("\n── Demand Forecast ──")
    for species in ['cattle', 'pork', 'lamb', 'goat']:
        forecast = adv.get_demand_forecast(species)
        if forecast['data_points'] > 0:
            print(f"  {species}: avg={forecast['avg_daily_fraction']:.3f} frac/day, "
                  f"trend={forecast['trend']}, fill in ~{forecast['expected_fill_days']} days")
        else:
            print(f"  {species}: no historical data (run demand snapshots to populate)")

    # Quality scoring
    print("\n── Quality Scoring ──")
    test_animals = [
        {'quality_tier': 'premium', 'finish_method': 'grass', 'breed': 'Black Angus'},
        {'quality_tier': 'standard', 'finish_method': 'grain', 'breed': None},
        {'quality_tier': 'economy', 'finish_method': 'mixed', 'breed': None},
        {'quality_tier': 'standard', 'finish_method': 'grass', 'breed': 'Hereford'},
    ]
    for animal in test_animals:
        score = adv.score_quality_match(animal)
        desc = f"{animal['quality_tier']}/{animal['finish_method']}"
        if animal['breed']:
            desc += f"/{animal['breed']}"
        print(f"  {desc:<35} → quality score: {score}")

    # Pareto demo
    print("\n── Pareto Frontier (Demo) ──")
    solutions = [
        {'name': 'Min Cost', 'cost': 5000, 'wait': 12, 'utilization_var': 3.5, 'pos_assigned': 40},
        {'name': 'Min Wait', 'cost': 6500, 'wait': 3, 'utilization_var': 2.0, 'pos_assigned': 45},
        {'name': 'Balanced', 'cost': 5500, 'wait': 7, 'utilization_var': 1.5, 'pos_assigned': 43},
        {'name': 'Max POs', 'cost': 7000, 'wait': 8, 'utilization_var': 4.0, 'pos_assigned': 48},
        {'name': 'Dominated', 'cost': 7000, 'wait': 12, 'utilization_var': 4.0, 'pos_assigned': 38},
    ]
    pareto = adv.pareto_analysis(solutions)
    print(f"  {len(solutions)} solutions → {len(pareto)} Pareto-optimal:")
    for sol in pareto:
        print(f"    {sol['name']}: cost=${sol['cost']}, wait={sol['wait']}d, "
              f"util_var={sol['utilization_var']}, POs={sol['pos_assigned']}")

    # Reliability
    print("\n── Processor Reliability ──")
    if adv.reliability:
        for pid, data in adv.reliability.items():
            score = adv.get_reliability_score(pid)
            print(f"  {pid[:8]}...: on_time={data['on_time_rate']:.0%}, "
                  f"quality={data['avg_quality']:.1f}/5, score={score}")
    else:
        print("  No performance data yet (will populate as slaughter orders complete)")

    conn.close()
