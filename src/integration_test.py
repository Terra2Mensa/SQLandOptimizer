"""
Terra Mensa — End-to-End Integration Test

Exercises the full pipeline:
  1. Pricing engine: compute customer prices with all modifiers
  2. Optimizer v2: run unified MIP on live data (dry-run)
  3. Farmer payment: compute payment breakdowns for each slaughter order
  4. Advanced features: zones, hold-back, quality scoring, demand forecast
  5. Summary report

Usage: python3 integration_test.py [--supabase]
"""

import sys
import time
from datetime import datetime

from optimizer_config import get_connection, load_optimizer_config, get_config, SPECIES_LIST, SHARE_FRACTIONS
from optimizer_v2 import (
    get_pending_pos, get_available_inventory, get_processors_for_species,
    bulk_load_distances, get_customer_profiles_bulk, solve_unified_mip,
    aggregate_pos_ffd, solve_joint_assignment,
)
from pricing_engine import PricingEngine
from optimizer_advanced import AdvancedFeatures


def run_integration_test(use_supabase=False):
    conn = get_connection(use_supabase=use_supabase)
    config = load_optimizer_config(conn)

    print("═══════════════════════════════════════════════════")
    print("  Terra Mensa — Integration Test")
    print(f"  Database: {'Supabase' if use_supabase else 'Local'}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═══════════════════════════════════════════════════\n")

    errors = []
    warnings = []

    # ── 1. Pricing Engine ────────────────────────────────────────────────
    print("── 1. PRICING ENGINE ──")
    try:
        engine = PricingEngine(conn)
        print("  [OK] PricingEngine initialized")

        # Test all species/shares
        total_prices = 0
        for species in SPECIES_LIST:
            prices = engine.get_all_prices(species)
            for share, data in prices.items():
                assert data['final_price'] > 0, f"Price for {species}/{share} is 0"
                assert data['share_modifier'] > 0, f"Modifier for {species}/{share} is 0"
                total_prices += 1
        print(f"  [OK] {total_prices} prices computed, all > 0")

        # Test batch-fill dynamics
        base = engine.compute_customer_price('cattle', 'quarter')
        early = engine.compute_customer_price('cattle', 'quarter', fill_fraction=0.10)
        last = engine.compute_customer_price('cattle', 'quarter', fill_fraction=0.80)
        if engine.batch_rules:
            assert early['final_price'] <= base['final_price'], "Early-bird should be <= base"
            assert last['final_price'] >= base['final_price'], "Last-share should be >= base"
            print(f"  [OK] Batch-fill dynamics: early ${early['final_price']:,.2f} ≤ base ${base['final_price']:,.2f} ≤ last ${last['final_price']:,.2f}")
        else:
            warnings.append("batch_pricing_rules table not found — batch-fill dynamics inactive")
            print(f"  [WARN] No batch_pricing_rules table — dynamics inactive")

        # Test seasonal
        seasonal = engine.get_seasonal_adjustment('cattle')
        print(f"  [OK] Seasonal adjustment (cattle, current month): {seasonal:.4f}")

        # Test farmer payment
        farmer = engine.compute_farmer_payment('cattle', 720, 712, 40)
        assert farmer['farmer_gross'] > 0, "Farmer gross should be > 0"
        assert farmer['milestone_1'] > 0, "Milestone 1 should be > 0"
        print(f"  [OK] Farmer payment: gross=${farmer['farmer_gross']:,.2f}, "
              f"farmer gets {farmer['farmer_pct_of_retail']}% of retail")

        # Test payment processing
        card_fee = engine.compute_payment_processing_fee(1000, 'card')
        ach_fee = engine.compute_payment_processing_fee(1000, 'ach')
        assert card_fee > ach_fee, "Card fee should exceed ACH"
        print(f"  [OK] Payment fees on $1000: card=${card_fee:.2f}, ACH=${ach_fee:.2f}")

    except Exception as e:
        errors.append(f"Pricing engine: {e}")
        print(f"  [FAIL] {e}")

    # ── 2. Advanced Features ─────────────────────────────────────────────
    print("\n── 2. ADVANCED FEATURES ──")
    try:
        adv = AdvancedFeatures(conn)
        print("  [OK] AdvancedFeatures initialized")

        # Geographic zones
        zone = adv.assign_zone(41.68, -86.25)
        print(f"  [OK] Zone assignment: (41.68, -86.25) → {zone}")

        # Quality scoring
        score = adv.score_quality_match({'quality_tier': 'premium', 'finish_method': 'grass', 'breed': 'Angus'})
        assert score == 1.0, f"Premium grass Angus should score 1.0, got {score}"
        print(f"  [OK] Quality scoring: premium/grass/Angus → {score}")

        # Hold-back logic
        from datetime import timedelta
        hold, reason = adv.should_hold_batch(
            [{'created_at': datetime.now() - timedelta(days=10)}],
            0.90, config
        )
        assert not hold, "90% full batch should dispatch"
        print(f"  [OK] Hold-back: 90% fill → {'hold' if hold else 'dispatch'}: {reason}")

        # Reliability (default)
        score = adv.get_reliability_score('nonexistent-id')
        assert score == 0.8, f"Default reliability should be 0.8, got {score}"
        print(f"  [OK] Default reliability score: {score}")

        # Pareto
        solutions = [
            {'cost': 5000, 'wait': 12, 'utilization_var': 3.5, 'pos_assigned': 40},
            {'cost': 6500, 'wait': 3, 'utilization_var': 2.0, 'pos_assigned': 45},
            {'cost': 7000, 'wait': 12, 'utilization_var': 4.0, 'pos_assigned': 38},  # dominated
        ]
        pareto = adv.pareto_analysis(solutions)
        assert len(pareto) == 2, f"Expected 2 Pareto-optimal, got {len(pareto)}"
        print(f"  [OK] Pareto analysis: 3 solutions → {len(pareto)} non-dominated")

    except Exception as e:
        errors.append(f"Advanced features: {e}")
        print(f"  [FAIL] {e}")

    # ── 3. Optimizer v2 (Dry Run) ────────────────────────────────────────
    print("\n── 3. OPTIMIZER v2 (DRY RUN) ──")
    total_assigned = 0
    total_cost = 0

    for species in SPECIES_LIST:
        try:
            pending = get_pending_pos(conn, species)
            if not pending:
                print(f"  [{species}] No pending POs — skipping")
                continue

            inventory = get_available_inventory(conn, species)
            processors = get_processors_for_species(conn, species)

            if not inventory:
                print(f"  [{species}] {len(pending)} POs but no inventory — skipping")
                continue
            if not processors:
                print(f"  [{species}] {len(pending)} POs but no processors — skipping")
                continue

            # Load distances
            all_ids = set()
            for a in inventory: all_ids.add(str(a['profile_id']))
            for p in processors: all_ids.add(str(p['processor_id']))
            for po in pending: all_ids.add(str(po['profile_id']))

            distances = bulk_load_distances(conn, all_ids)
            customers = get_customer_profiles_bulk(conn, set(str(po['profile_id']) for po in pending))

            # Filter processors
            if adv:
                processors = adv.filter_processors(processors, species)

            # Run unified MIP
            t0 = time.time()
            results, remaining = solve_unified_mip(
                pending, inventory, processors, customers, distances, config
            )
            elapsed = time.time() - t0

            species_assigned = sum(len(r['batch_pos']) for r in results)
            species_cost = sum(r['cost'] for r in results)
            total_assigned += species_assigned
            total_cost += species_cost

            print(f"  [{species}] {species_assigned}/{len(pending)} POs assigned, "
                  f"{len(results)} batches, ${species_cost:,.2f}, {elapsed:.1f}s")

            # Compute farmer payments for each result
            for r in results:
                fp = engine.compute_farmer_payment(
                    species,
                    r['breakdown']['hanging_weight'],
                    r['breakdown']['processing_cost'],
                    r['breakdown']['farmer_transport'],
                )
                # Validate
                assert fp['farmer_gross'] >= 0, f"Negative farmer gross for {species}"

        except Exception as e:
            errors.append(f"Optimizer ({species}): {e}")
            print(f"  [{species}] FAIL: {e}")

    print(f"\n  Total: {total_assigned} POs assigned, ${total_cost:,.2f}")

    # ── 4. Phase 1 Comparison ────────────────────────────────────────────
    print("\n── 4. PHASE 1 COMPARISON ──")
    for species in SPECIES_LIST:
        try:
            pending = get_pending_pos(conn, species)
            if not pending:
                continue
            inventory = get_available_inventory(conn, species)
            processors = get_processors_for_species(conn, species)
            if not inventory or not processors:
                continue

            all_ids = set()
            for a in inventory: all_ids.add(str(a['profile_id']))
            for p in processors: all_ids.add(str(p['processor_id']))
            for po in pending: all_ids.add(str(po['profile_id']))
            distances = bulk_load_distances(conn, all_ids)
            customers = get_customer_profiles_bulk(conn, set(str(po['profile_id']) for po in pending))

            batches, rem = aggregate_pos_ffd(pending, get_config(config, 'fill_threshold', 1.0))
            if batches:
                assignments = solve_joint_assignment(batches, inventory, processors, customers, distances, config)
                p1_cost = sum(a[3] for a in assignments)
                p1_assigned = sum(len(batches[a[0]]) for a in assignments)
                print(f"  [{species}] Phase 1: {p1_assigned} POs, ${p1_cost:,.2f}")

        except Exception as e:
            print(f"  [{species}] Phase 1 error: {e}")

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n═══════════════════════════════════════════════════")
    print(f"  RESULTS: {len(errors)} errors, {len(warnings)} warnings")
    print("═══════════════════════════════════════════════════")

    if errors:
        print("\n  ERRORS:")
        for e in errors:
            print(f"    - {e}")

    if warnings:
        print("\n  WARNINGS:")
        for w in warnings:
            print(f"    - {w}")

    if not errors:
        print("\n  All tests passed.")

    conn.close()
    return len(errors) == 0


if __name__ == '__main__':
    use_supabase = '--supabase' in sys.argv
    success = run_integration_test(use_supabase=use_supabase)
    sys.exit(0 if success else 1)
