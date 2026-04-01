"""
Terra Mensa Optimizer
Aggregates pending purchase orders into whole animals, matches to farmer inventory
and optimal processor, creates slaughter orders.

Usage: python3 optimizer.py [--supabase]
"""

import sys
import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv

from optimizer_config import (
    get_connection, load_optimizer_config, get_config,
    SHARE_FRACTIONS, DRESS_PCT, SPECIES_LIST,
)

load_dotenv()


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
    """Get all processors with costs for a species."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT p.id as processor_id, p.company_name, p.latitude, p.longitude,
                   pc.kill_fee, pc.fab_cost_per_lb, pc.shrink_pct, pc.daily_capacity_head
            FROM profiles p
            JOIN processor_costs pc ON pc.profile_id = p.id AND pc.species = %s
            WHERE p.type = 'processor'
        """, (species,))
        return [dict(row) for row in cur.fetchall()]


def get_customer_profile(conn, profile_id):
    """Get a customer's profile for distance lookup."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, latitude, longitude, first_name FROM profiles WHERE id = %s", (profile_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_distance(conn, id_a, id_b):
    """Look up cached distance from distance_matrix. Returns miles or None."""
    # Sort UUIDs for consistent lookup
    origin = min(str(id_a), str(id_b))
    dest = max(str(id_a), str(id_b))
    with conn.cursor() as cur:
        cur.execute("""
            SELECT distance_miles FROM distance_matrix
            WHERE origin_profile_id = %s AND destination_profile_id = %s
        """, (origin, dest))
        row = cur.fetchone()
        return float(row[0]) if row else None


# ─── Aggregation ─────────────────────────────────────────────────────────

def aggregate_pos_into_batches(pos_list, fill_threshold):
    """Group POs into batches that fill at least one whole animal.

    Returns: list of batches, where each batch is a list of PO dicts.
    Remaining POs that don't fill an animal are excluded.
    """
    batches = []
    current_batch = []
    current_sum = 0.0

    for po in pos_list:
        fraction = SHARE_FRACTIONS.get(po['share'], 0)
        if fraction <= 0:
            continue

        current_batch.append(po)
        current_sum += fraction

        if current_sum >= fill_threshold:
            batches.append(current_batch)
            current_batch = []
            current_sum = 0.0

    # Remaining POs don't fill a batch — leave them pending
    if current_batch:
        remaining = [po['po_number'] for po in current_batch]
        print(f"    {len(current_batch)} POs remaining (sum={current_sum:.3f}): {remaining}")

    return batches


# ─── Processor Selection ─────────────────────────────────────────────────

def find_optimal_processor(conn, animal, batch_pos, processors, config):
    """Find the processor with lowest total cost that satisfies all constraints.

    Returns: (processor_dict, cost_breakdown) or (None, None) if no valid processor.
    """
    farmer_id = animal['profile_id']
    max_farmer_dist = get_config(config, 'max_farmer_distance_miles', 50)
    max_customer_dist = get_config(config, 'max_customer_distance_miles', 50)
    farmer_rate = get_config(config, 'farmer_transport_per_mile', 2)
    customer_rate = get_config(config, 'customer_transport_per_mile', 1)

    live_weight = float(animal.get('live_weight_est') or 0)
    dress_pct = DRESS_PCT.get(animal['species'], 0.60)
    hanging_weight = live_weight * dress_pct

    # Get customer profiles for distance lookups
    customers = []
    for po in batch_pos:
        cust = get_customer_profile(conn, po['profile_id'])
        if cust:
            customers.append(cust)

    best_processor = None
    best_cost = float('inf')
    best_breakdown = None

    for proc in processors:
        proc_id = proc['processor_id']

        # Farmer → processor distance
        farmer_dist = get_distance(conn, farmer_id, proc_id)
        if farmer_dist is None:
            print(f"      SKIP {proc['company_name']}: no distance data for farmer")
            continue
        if farmer_dist > max_farmer_dist:
            print(f"      SKIP {proc['company_name']}: farmer distance {farmer_dist:.1f} mi > {max_farmer_dist} mi")
            continue

        # Check all customers within range
        customer_distances = []
        all_customers_ok = True
        for cust in customers:
            cust_dist = get_distance(conn, proc_id, cust['id'])
            if cust_dist is None:
                print(f"      SKIP {proc['company_name']}: no distance data for customer {cust['first_name']}")
                all_customers_ok = False
                break
            if cust_dist > max_customer_dist:
                print(f"      SKIP {proc['company_name']}: customer {cust['first_name']} distance {cust_dist:.1f} mi > {max_customer_dist} mi")
                all_customers_ok = False
                break
            customer_distances.append(cust_dist)

        if not all_customers_ok:
            continue

        # Calculate costs
        kill_fee = float(proc.get('kill_fee') or 0)
        fab_per_lb = float(proc.get('fab_cost_per_lb') or 0)
        processing_cost = kill_fee + (fab_per_lb * hanging_weight)
        farmer_transport = farmer_dist * farmer_rate
        customer_transport = sum(d * customer_rate for d in customer_distances)

        total_cost = processing_cost + farmer_transport + customer_transport

        print(f"      {proc['company_name']}: processing=${processing_cost:.2f} + farmer_trans=${farmer_transport:.2f} + cust_trans=${customer_transport:.2f} = ${total_cost:.2f}")

        if total_cost < best_cost:
            best_cost = total_cost
            best_processor = proc
            best_breakdown = {
                'processing_cost': round(processing_cost, 2),
                'farmer_transport': round(farmer_transport, 2),
                'customer_transport': round(customer_transport, 2),
                'total_cost': round(total_cost, 2),
                'farmer_distance': farmer_dist,
                'hanging_weight': round(hanging_weight, 1),
            }

    return best_processor, best_breakdown


# ─── Execution ───────────────────────────────────────────────────────────

def create_slaughter_order(conn, animal, processor, batch_pos, cost_breakdown):
    """Create a slaughter order and update POs + inventory."""
    import random, string
    rand = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    order_number = f"SO-{datetime.now().strftime('%Y%m%d%H%M%S')}-{animal['species'][:3].upper()}-{rand}"

    with conn.cursor() as cur:
        # Create slaughter order with full cost breakdown
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

        # Reserve the animal
        cur.execute("""
            UPDATE farmer_inventory SET status = 'reserved', updated_at = now()
            WHERE id = %s
        """, (animal['id'],))

        # Confirm each PO, link to animal AND slaughter order
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

def run_optimizer(use_supabase=False):
    """Main entry point. Runs optimization for all species."""
    conn = get_connection(use_supabase=use_supabase)
    config = load_optimizer_config(conn)
    fill_threshold = get_config(config, 'fill_threshold', 1.0)

    print(f"═══ Terra Mensa Optimizer ═══")
    print(f"  Fill threshold: {fill_threshold}")
    print(f"  Farmer transport: ${get_config(config, 'farmer_transport_per_mile')}/mi")
    print(f"  Customer transport: ${get_config(config, 'customer_transport_per_mile')}/mi")
    print(f"  Max farmer distance: {get_config(config, 'max_farmer_distance_miles')} mi")
    print(f"  Max customer distance: {get_config(config, 'max_customer_distance_miles')} mi")
    print()

    total_orders_created = 0

    for species in SPECIES_LIST:
        print(f"── {species.upper()} ──")

        # Step 1: Get pending POs
        pending = get_pending_pos(conn, species)
        if not pending:
            print(f"  No pending POs")
            continue

        fractions = [SHARE_FRACTIONS.get(po['share'], 0) for po in pending]
        print(f"  {len(pending)} pending POs (total fraction: {sum(fractions):.3f})")

        # Step 2: Aggregate into batches
        batches = aggregate_pos_into_batches(pending, fill_threshold)
        if not batches:
            print(f"  No complete animals to process")
            continue

        print(f"  {len(batches)} complete animal(s) to process")

        # Step 3: Match each batch
        inventory = get_available_inventory(conn, species)
        processors = get_processors_for_species(conn, species)

        if not inventory:
            print(f"  WARNING: No available inventory for {species}")
            continue
        if not processors:
            print(f"  WARNING: No processors for {species}")
            continue

        for i, batch in enumerate(batches):
            po_numbers = [po['po_number'] for po in batch]
            batch_fraction = sum(SHARE_FRACTIONS.get(po['share'], 0) for po in batch)
            print(f"\n  Batch {i+1}: {len(batch)} POs ({batch_fraction:.2f} animal) — {po_numbers}")

            # Pick farmer inventory (FIFO)
            if not inventory:
                print(f"    No more available inventory — skipping batch")
                break

            animal = inventory.pop(0)  # Take first available
            farm_name = animal.get('company_name') or animal.get('first_name')
            print(f"    Matched animal: {farm_name} — {animal['species']}, {animal.get('live_weight_est')} lbs")

            # Find optimal processor
            print(f"    Evaluating {len(processors)} processors:")
            best_proc, breakdown = find_optimal_processor(conn, animal, batch, processors, config)

            if not best_proc:
                print(f"    NO VALID PROCESSOR found — batch skipped")
                # Return animal to inventory
                inventory.insert(0, animal)
                continue

            print(f"    ✓ WINNER: {best_proc['company_name']} at ${breakdown['total_cost']:.2f}")
            print(f"      Processing: ${breakdown['processing_cost']:.2f}")
            print(f"      Farmer transport: ${breakdown['farmer_transport']:.2f} ({breakdown['farmer_distance']:.1f} mi)")
            print(f"      Customer transport: ${breakdown['customer_transport']:.2f}")

            # Create slaughter order
            so_number = create_slaughter_order(conn, animal, best_proc, batch, breakdown)
            print(f"    → Slaughter order: {so_number}")
            total_orders_created += 1

    print(f"\n═══ Optimizer Complete: {total_orders_created} slaughter order(s) created ═══")
    conn.close()


if __name__ == '__main__':
    use_supabase = '--supabase' in sys.argv
    run_optimizer(use_supabase=use_supabase)
