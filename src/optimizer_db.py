"""Database CRUD for optimizer tables: processors, cut_specs, grade_hierarchy,
slaughter_orders, slaughter_order_lines."""
import json
from typing import Optional

import psycopg2
import psycopg2.extras

from db import get_connection


# ---------------------------------------------------------------------------
# Processors
# ---------------------------------------------------------------------------

def save_processor(proc: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO processors
                (processor_key, company_name, address_line1, city, state,
                 zip_code, latitude, longitude, phone,
                 is_buyer_of_last_resort, active, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (processor_key) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    address_line1 = EXCLUDED.address_line1,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    phone = EXCLUDED.phone,
                    is_buyer_of_last_resort = EXCLUDED.is_buyer_of_last_resort,
                    active = EXCLUDED.active,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
            """, (
                proc['processor_key'], proc['company_name'],
                proc.get('address_line1'), proc.get('city'), proc.get('state'),
                proc.get('zip_code'), proc.get('latitude'), proc.get('longitude'),
                proc.get('phone'),
                proc.get('is_buyer_of_last_resort', False),
                proc.get('active', True), proc.get('notes'),
            ))
        conn.commit()
    finally:
        conn.close()


def get_processor(processor_key: str) -> Optional[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM processors WHERE processor_key = %s",
                        (processor_key,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_all_processors(active_only: bool = True) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if active_only:
                cur.execute("SELECT * FROM processors WHERE active = TRUE ORDER BY company_name")
            else:
                cur.execute("SELECT * FROM processors ORDER BY company_name")
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_eligible_processors(species: str) -> list:
    """Return active processors that have capability for the given species."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT p.*, cpc.daily_capacity_head,
                       cpc.organic_certified, cpc.usda_inspected
                FROM processors p
                JOIN config_processor_capabilities cpc
                    ON cpc.processor_key = p.processor_key
                WHERE p.active = TRUE
                  AND cpc.species = %s
                  AND cpc.effective_date = (
                      SELECT MAX(effective_date)
                      FROM config_processor_capabilities c2
                      WHERE c2.processor_key = cpc.processor_key
                        AND c2.species = cpc.species
                        AND c2.effective_date <= CURRENT_DATE
                  )
                ORDER BY p.company_name
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Cut Specs
# ---------------------------------------------------------------------------

def save_cut_spec(spec: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config_cut_specs
                (species, primal_code, primal_name, cut_code, cut_name,
                 yield_pct, min_grade, is_premium, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (species, cut_code) DO UPDATE SET
                    primal_code = EXCLUDED.primal_code,
                    primal_name = EXCLUDED.primal_name,
                    cut_name = EXCLUDED.cut_name,
                    yield_pct = EXCLUDED.yield_pct,
                    min_grade = EXCLUDED.min_grade,
                    is_premium = EXCLUDED.is_premium,
                    notes = EXCLUDED.notes
            """, (
                spec['species'], spec['primal_code'], spec['primal_name'],
                spec['cut_code'], spec['cut_name'], spec['yield_pct'],
                spec.get('min_grade'), spec.get('is_premium', False),
                spec.get('notes'),
            ))
        conn.commit()
    finally:
        conn.close()


def save_cut_specs_bulk(specs: list):
    """Insert/upsert many cut specs in a single transaction."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for spec in specs:
                cur.execute("""
                    INSERT INTO config_cut_specs
                    (species, primal_code, primal_name, cut_code, cut_name,
                     yield_pct, min_grade, is_premium, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (species, cut_code) DO UPDATE SET
                        primal_code = EXCLUDED.primal_code,
                        primal_name = EXCLUDED.primal_name,
                        cut_name = EXCLUDED.cut_name,
                        yield_pct = EXCLUDED.yield_pct,
                        min_grade = EXCLUDED.min_grade,
                        is_premium = EXCLUDED.is_premium,
                        notes = EXCLUDED.notes
                """, (
                    spec['species'], spec['primal_code'], spec['primal_name'],
                    spec['cut_code'], spec['cut_name'], spec['yield_pct'],
                    spec.get('min_grade'), spec.get('is_premium', False),
                    spec.get('notes'),
                ))
        conn.commit()
    finally:
        conn.close()


def get_cut_specs(species: str) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT * FROM config_cut_specs
                WHERE species = %s ORDER BY primal_code, cut_code
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Grade Hierarchy
# ---------------------------------------------------------------------------

def save_grade_hierarchy_bulk(rows: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO config_grade_hierarchy
                    (species, grade_code, grade_name, rank_order)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (species, grade_code) DO UPDATE SET
                        grade_name = EXCLUDED.grade_name,
                        rank_order = EXCLUDED.rank_order
                """, (r['species'], r['grade_code'], r['grade_name'], r['rank_order']))
        conn.commit()
    finally:
        conn.close()


def get_grade_hierarchy(species: str) -> dict:
    """Return {grade_code: rank_order} for a species."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT grade_code, rank_order FROM config_grade_hierarchy
                WHERE species = %s
            """, (species,))
            return {row['grade_code']: row['rank_order'] for row in cur.fetchall()}
    finally:
        conn.close()


def grade_meets_requirement(species: str, animal_grade: str, required_grade: str) -> bool:
    """Check if animal_grade >= required_grade in the hierarchy."""
    hierarchy = get_grade_hierarchy(species)
    if not hierarchy:
        return True  # no hierarchy defined = no restriction
    animal_rank = hierarchy.get(animal_grade, 0)
    required_rank = hierarchy.get(required_grade, 0)
    return animal_rank >= required_rank


# ---------------------------------------------------------------------------
# Slaughter Orders (optimizer output)
# ---------------------------------------------------------------------------

SLAUGHTER_ORDER_STATUSES = ["planned", "confirmed", "in_progress", "completed", "cancelled"]


def save_slaughter_order(order: dict, lines: list) -> int:
    """Insert slaughter order header + lines. Returns the order id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO slaughter_orders
                (order_number, status, species, animal_id, processor_key,
                 optimizer_run_id, estimated_hanging_weight,
                 processing_cost_total, farmer_to_proc_distance,
                 avg_cust_to_proc_distance, pct_allocated_to_orders,
                 pct_to_last_resort, optimizer_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                order['order_number'], order.get('status', 'planned'),
                order['species'], order['animal_id'], order['processor_key'],
                order.get('optimizer_run_id'),
                order.get('estimated_hanging_weight'),
                order.get('processing_cost_total'),
                order.get('farmer_to_proc_distance'),
                order.get('avg_cust_to_proc_distance'),
                order.get('pct_allocated_to_orders'),
                order.get('pct_to_last_resort'),
                order.get('optimizer_score'),
            ))
            order_id = cur.fetchone()[0]

            for ln in lines:
                cur.execute("""
                    INSERT INTO slaughter_order_lines
                    (slaughter_order_id, cut_code, total_lbs,
                     allocated_to_po, allocated_to_lor, po_number, po_line_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_id, ln['cut_code'], ln['total_lbs'],
                    ln.get('allocated_to_po', 0), ln.get('allocated_to_lor', 0),
                    ln.get('po_number'), ln.get('po_line_id'),
                ))
        conn.commit()
        return order_id
    finally:
        conn.close()


def get_slaughter_order(order_number: str) -> Optional[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM slaughter_orders WHERE order_number = %s",
                        (order_number,))
            row = cur.fetchone()
            if not row:
                return None
            order = dict(row)
            cur.execute("""
                SELECT * FROM slaughter_order_lines
                WHERE slaughter_order_id = %s
            """, (order['id'],))
            order['lines'] = [dict(r) for r in cur.fetchall()]
            return order
    finally:
        conn.close()


def get_slaughter_orders_by_run(run_id: str) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT * FROM slaughter_orders
                WHERE optimizer_run_id = %s ORDER BY order_number
            """, (run_id,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def update_slaughter_order_status(order_number: str, status: str):
    if status not in SLAUGHTER_ORDER_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {SLAUGHTER_ORDER_STATUSES}")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE slaughter_orders SET status = %s, updated_at = NOW()
                WHERE order_number = %s
            """, (status, order_number))
            if cur.rowcount == 0:
                raise ValueError(f"Slaughter order '{order_number}' not found")
        conn.commit()
    finally:
        conn.close()


def fulfill_po_lines(allocations: list):
    """Update po_lines.fulfilled_lbs and status based on optimizer allocations.

    allocations: list of {'po_line_id': int, 'lbs': float}
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for alloc in allocations:
                cur.execute("""
                    UPDATE po_lines
                    SET fulfilled_lbs = fulfilled_lbs + %s,
                        status = CASE
                            WHEN fulfilled_lbs + %s >= quantity_lbs THEN 'fulfilled'
                            ELSE 'partial'
                        END
                    WHERE id = %s
                """, (alloc['lbs'], alloc['lbs'], alloc['po_line_id']))
        conn.commit()
    finally:
        conn.close()
