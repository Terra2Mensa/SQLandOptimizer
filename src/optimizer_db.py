"""Database CRUD for optimizer tables: cut_specs, grade_hierarchy,
slaughter_orders, slaughter_order_allocations."""
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras

from db import get_connection


# ---------------------------------------------------------------------------
# Cut Specs (unchanged tables)
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
# Grade Hierarchy (unchanged tables)
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
        return True
    animal_rank = hierarchy.get(animal_grade, 0)
    required_rank = hierarchy.get(required_grade, 0)
    return animal_rank >= required_rank


# ---------------------------------------------------------------------------
# Processors (rewritten for profiles + processor_costs)
# ---------------------------------------------------------------------------

def get_eligible_processors(species: str) -> list:
    """Return active processors that have costs defined for the given species."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (p.profile_id)
                       p.profile_id, p.company_name, p.address,
                       p.latitude, p.longitude, p.active,
                       pc.kill_fee, pc.fab_cost_per_lb, pc.shrink_pct,
                       pc.daily_capacity_head
                FROM profiles p
                JOIN processor_costs pc ON pc.profile_id = p.profile_id
                WHERE p.type = 'processor'
                  AND p.active = TRUE
                  AND pc.species = %s
                  AND pc.effective_date <= CURRENT_DATE
                ORDER BY p.profile_id, pc.effective_date DESC
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Slaughter Orders (rewritten for allocations instead of lines)
# ---------------------------------------------------------------------------

SLAUGHTER_ORDER_STATUSES = ["planned", "confirmed", "in_progress", "completed", "cancelled"]


def save_slaughter_order(order: dict, allocations: list):
    """Insert slaughter order header + allocations (one per PO in assembly)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO slaughter_orders
                (order_number, animal_id, profile_id, species, status,
                 actual_hanging_weight)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                order['order_number'],
                order.get('animal_id'),
                order['profile_id'],
                order['species'],
                order.get('status', 'planned'),
                order.get('actual_hanging_weight'),
            ))

            for alloc in allocations:
                cur.execute("""
                    INSERT INTO slaughter_order_allocations
                    (slaughter_order_number, po_number, share)
                    VALUES (%s, %s, %s)
                """, (order['order_number'], alloc['po_number'], alloc['share']))

        conn.commit()
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
                SELECT * FROM slaughter_order_allocations
                WHERE slaughter_order_number = %s
            """, (order_number,))
            order['allocations'] = [dict(r) for r in cur.fetchall()]
            return order
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


# ---------------------------------------------------------------------------
# Assembly Engine Helpers
# ---------------------------------------------------------------------------

def get_processor_scheduled_count(profile_id: str, schedule_date, species: str) -> int:
    """Count slaughter_orders for this processor on this date (status != cancelled)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM slaughter_orders
                WHERE profile_id = %s
                  AND species = %s
                  AND status != 'cancelled'
                  AND created_at::date = %s
            """, (profile_id, species, schedule_date))
            return cur.fetchone()[0]
    finally:
        conn.close()
