"""Database CRUD for optimizer tables: processors, cut_specs, grade_hierarchy,
slaughter_orders, slaughter_order_lines, actual_cuts, invoicing."""
import json
from datetime import date, timedelta
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


# ---------------------------------------------------------------------------
# Assembly Engine Helpers
# ---------------------------------------------------------------------------

def get_processor_scheduled_count(processor_key: str, schedule_date, species: str) -> int:
    """Count slaughter_orders for this processor on this date (status != cancelled)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM slaughter_orders
                WHERE processor_key = %s
                  AND species = %s
                  AND status != 'cancelled'
                  AND created_at::date = %s
            """, (processor_key, species, schedule_date))
            return cur.fetchone()[0]
    finally:
        conn.close()


def get_lor_processor(species: str) -> Optional[dict]:
    """Find processor with is_buyer_of_last_resort = TRUE for this species."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT p.* FROM processors p
                JOIN config_processor_capabilities cpc
                    ON cpc.processor_key = p.processor_key
                WHERE p.is_buyer_of_last_resort = TRUE
                  AND p.active = TRUE
                  AND cpc.species = %s
                ORDER BY p.company_name
                LIMIT 1
            """, (species,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 2: Actual Weights, Reconciliation, Invoicing
# ---------------------------------------------------------------------------

def record_actual_cuts(order_number: str, cuts: list, recorded_by: str = None):
    """Record actual post-processing weights for a slaughter order.

    Args:
        order_number: slaughter order number (e.g. 'SO-xxxx-001')
        cuts: list of {'cut_code': str, 'actual_lbs': float, 'notes': str (optional)}
        recorded_by: who entered the data

    Upserts into actual_cuts, updates slaughter_order_lines.actual_lbs,
    and sets slaughter_orders.actual_hanging_weight.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Get order id and validate status
            cur.execute("""
                SELECT id, status FROM slaughter_orders
                WHERE order_number = %s
            """, (order_number,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Slaughter order '{order_number}' not found")
            so_id = row['id']
            if row['status'] not in ('planned', 'confirmed', 'in_progress', 'completed'):
                raise ValueError(
                    f"Cannot record weights for order in '{row['status']}' status")

            # Upsert actual_cuts
            for cut in cuts:
                cur.execute("""
                    INSERT INTO actual_cuts
                        (slaughter_order_id, cut_code, actual_lbs, recorded_by, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (slaughter_order_id, cut_code)
                    DO UPDATE SET actual_lbs = EXCLUDED.actual_lbs,
                                  recorded_by = EXCLUDED.recorded_by,
                                  recorded_at = NOW(),
                                  notes = EXCLUDED.notes
                """, (so_id, cut['cut_code'], cut['actual_lbs'],
                      recorded_by, cut.get('notes')))

            # Update slaughter_order_lines.actual_lbs from actual_cuts
            cur.execute("""
                UPDATE slaughter_order_lines sol
                SET actual_lbs = ac.actual_lbs
                FROM actual_cuts ac
                WHERE sol.slaughter_order_id = ac.slaughter_order_id
                  AND sol.cut_code = ac.cut_code
                  AND sol.slaughter_order_id = %s
            """, (so_id,))

            # Update actual_hanging_weight on the order
            cur.execute("""
                UPDATE slaughter_orders
                SET actual_hanging_weight = (
                        SELECT COALESCE(SUM(actual_lbs), 0)
                        FROM actual_cuts WHERE slaughter_order_id = %s
                    ),
                    status = CASE WHEN status = 'planned' THEN 'in_progress'
                                  ELSE status END,
                    updated_at = NOW()
                WHERE id = %s
            """, (so_id, so_id))

        conn.commit()
    finally:
        conn.close()


def get_actual_cuts(order_number: str) -> list:
    """Get recorded actual cuts for a slaughter order."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT ac.cut_code, ac.actual_lbs, ac.recorded_by,
                       ac.recorded_at, ac.notes,
                       sol.total_lbs AS estimated_lbs,
                       sol.allocated_to_po AS est_po_lbs,
                       sol.allocated_to_lor AS est_lor_lbs
                FROM actual_cuts ac
                JOIN slaughter_orders so ON so.id = ac.slaughter_order_id
                LEFT JOIN slaughter_order_lines sol
                    ON sol.slaughter_order_id = ac.slaughter_order_id
                    AND sol.cut_code = ac.cut_code
                WHERE so.order_number = %s
                ORDER BY ac.cut_code
            """, (order_number,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def finalize_slaughter_order(order_number: str) -> dict:
    """Reconcile actual weights against PO allocations and finalize the order.

    1. Validates all lines have actual_lbs recorded
    2. For each PO-allocated line: actual_allocated_to_po = min(actual_lbs, quantity ordered)
    3. Remainder goes to actual_allocated_to_lor
    4. Updates po_lines.actual_lbs (summed across all contributing SOs)
    5. Recalculates purchase_orders.total_final
    6. Marks SO completed

    Returns summary dict with per-line reconciliation.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Load slaughter order
            cur.execute("""
                SELECT id, order_number, status, species
                FROM slaughter_orders WHERE order_number = %s
            """, (order_number,))
            so = cur.fetchone()
            if not so:
                raise ValueError(f"Slaughter order '{order_number}' not found")
            if so['status'] == 'cancelled':
                raise ValueError(f"Cannot finalize cancelled order")
            so_id = so['id']

            # Load lines with actual weights
            cur.execute("""
                SELECT sol.id, sol.cut_code, sol.total_lbs, sol.actual_lbs,
                       sol.allocated_to_po, sol.allocated_to_lor,
                       sol.po_number, sol.po_line_id
                FROM slaughter_order_lines sol
                WHERE sol.slaughter_order_id = %s
                ORDER BY sol.cut_code
            """, (so_id,))
            lines = [dict(r) for r in cur.fetchall()]

            # Check all lines have actual weights
            missing = [ln for ln in lines if ln['actual_lbs'] is None]
            if missing:
                codes = ', '.join(ln['cut_code'] for ln in missing)
                raise ValueError(
                    f"Missing actual weights for: {codes}. "
                    f"Run record-weights first.")

            # Reconcile each line
            affected_po_numbers = set()
            reconciled = []

            for ln in lines:
                actual = float(ln['actual_lbs'])
                est_total = float(ln['total_lbs'])
                est_po = float(ln['allocated_to_po'] or 0)
                est_lor = float(ln['allocated_to_lor'] or 0)

                if ln['po_line_id']:
                    # Get how much the customer ordered on this PO line
                    cur.execute("""
                        SELECT quantity_lbs, COALESCE(actual_lbs, 0) AS already_actual
                        FROM po_lines WHERE id = %s
                    """, (ln['po_line_id'],))
                    pl = cur.fetchone()
                    if pl:
                        ordered = float(pl['quantity_lbs'])
                        already = float(pl['already_actual'])
                        remaining_need = max(0, ordered - already)
                        # Allocate actual to PO: capped at what customer needs
                        actual_to_po = min(actual, remaining_need)
                    else:
                        actual_to_po = 0.0
                    actual_to_lor = max(0, actual - actual_to_po)
                else:
                    # Pure LOR line
                    actual_to_po = 0.0
                    actual_to_lor = actual

                # Update slaughter_order_lines
                cur.execute("""
                    UPDATE slaughter_order_lines
                    SET actual_allocated_to_po = %s,
                        actual_allocated_to_lor = %s
                    WHERE id = %s
                """, (round(actual_to_po, 2), round(actual_to_lor, 2), ln['id']))

                if ln['po_number']:
                    affected_po_numbers.add(ln['po_number'])

                reconciled.append({
                    'cut_code': ln['cut_code'],
                    'estimated_lbs': est_total,
                    'actual_lbs': actual,
                    'delta_pct': ((actual - est_total) / est_total * 100)
                        if est_total > 0 else 0,
                    'est_to_po': est_po,
                    'actual_to_po': actual_to_po,
                    'est_to_lor': est_lor,
                    'actual_to_lor': actual_to_lor,
                    'po_number': ln['po_number'],
                })

            # Update po_lines.actual_lbs for each affected PO line
            # Sum across all slaughter orders that contribute to each po_line
            cur.execute("""
                UPDATE po_lines pl
                SET actual_lbs = sub.total_actual
                FROM (
                    SELECT po_line_id, SUM(actual_allocated_to_po) AS total_actual
                    FROM slaughter_order_lines
                    WHERE po_line_id IS NOT NULL
                      AND actual_allocated_to_po IS NOT NULL
                      AND slaughter_order_id = %s
                    GROUP BY po_line_id
                ) sub
                WHERE pl.id = sub.po_line_id
            """, (so_id,))

            # Recalculate total_final for each affected PO
            po_finals = {}
            for po_num in affected_po_numbers:
                cur.execute("""
                    SELECT COALESCE(SUM(
                        COALESCE(actual_lbs, fulfilled_lbs) * price_per_lb
                    ), 0) AS total_final
                    FROM po_lines WHERE po_number = %s
                """, (po_num,))
                total_final = float(cur.fetchone()['total_final'])
                cur.execute("""
                    UPDATE purchase_orders
                    SET total_final = %s, updated_at = NOW()
                    WHERE po_number = %s
                """, (round(total_final, 2), po_num))
                po_finals[po_num] = total_final

                # Check if all po_lines for this PO have actual_lbs
                cur.execute("""
                    SELECT COUNT(*) AS total,
                           COUNT(actual_lbs) AS with_actual
                    FROM po_lines WHERE po_number = %s
                """, (po_num,))
                counts = cur.fetchone()
                if counts['total'] == counts['with_actual']:
                    cur.execute("""
                        UPDATE purchase_orders
                        SET status = 'fulfilled', updated_at = NOW()
                        WHERE po_number = %s AND status != 'cancelled'
                    """, (po_num,))

            # Mark SO completed
            cur.execute("""
                UPDATE slaughter_orders
                SET status = 'completed', completed_at = NOW(), updated_at = NOW()
                WHERE id = %s
            """, (so_id,))

        conn.commit()

        # Build summary
        total_est = sum(ln['estimated_lbs'] for ln in reconciled)
        total_act = sum(ln['actual_lbs'] for ln in reconciled)
        return {
            'order_number': order_number,
            'species': so['species'],
            'lines': reconciled,
            'total_estimated_hw': total_est,
            'total_actual_hw': total_act,
            'yield_variance_pct': ((total_act - total_est) / total_est * 100)
                if total_est > 0 else 0,
            'po_finals': po_finals,
            'affected_pos': list(affected_po_numbers),
        }
    finally:
        conn.close()


def generate_po_invoice(po_number: str, due_days: int = 30) -> dict:
    """Generate an invoice for a fulfilled PO based on actual weights.

    Args:
        po_number: the purchase order number
        due_days: payment terms in days from today

    Returns invoice dict.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Validate PO
            cur.execute("""
                SELECT po_number, customer_id, status, total_estimated, total_final
                FROM purchase_orders WHERE po_number = %s
            """, (po_number,))
            po = cur.fetchone()
            if not po:
                raise ValueError(f"PO '{po_number}' not found")
            if po['status'] not in ('fulfilled', 'processing', 'planned'):
                raise ValueError(
                    f"PO '{po_number}' is in '{po['status']}' status. "
                    f"Expected 'fulfilled' (or 'processing'/'planned' for early invoice).")

            # Check for existing invoice
            cur.execute("""
                SELECT invoice_id FROM invoices WHERE po_number = %s
            """, (po_number,))
            existing = cur.fetchone()
            if existing:
                raise ValueError(
                    f"Invoice '{existing['invoice_id']}' already exists for PO '{po_number}'")

            # Calculate total from actual weights where available, else estimated
            cur.execute("""
                SELECT cut_code, quantity_lbs, actual_lbs, price_per_lb,
                       COALESCE(actual_lbs, quantity_lbs) * price_per_lb AS line_total
                FROM po_lines WHERE po_number = %s
            """, (po_number,))
            lines = [dict(r) for r in cur.fetchall()]
            total_amount = sum(float(ln['line_total']) for ln in lines)

            invoice_id = f"INV-{po_number}"
            due = date.today() + timedelta(days=due_days)

            cur.execute("""
                INSERT INTO invoices
                (invoice_id, po_number, customer_id, invoice_date, due_date,
                 total_amount, status)
                VALUES (%s, %s, %s, CURRENT_DATE, %s, %s, 'draft')
            """, (invoice_id, po_number, po['customer_id'], due,
                  round(total_amount, 2)))

            # Update PO status to invoiced
            cur.execute("""
                UPDATE purchase_orders
                SET status = 'fulfilled', total_final = %s, updated_at = NOW()
                WHERE po_number = %s
            """, (round(total_amount, 2), po_number))

        conn.commit()

        return {
            'invoice_id': invoice_id,
            'po_number': po_number,
            'customer_id': po['customer_id'],
            'total_estimated': float(po['total_estimated'] or 0),
            'total_actual': total_amount,
            'variance': total_amount - float(po['total_estimated'] or 0),
            'due_date': due.isoformat(),
            'lines': lines,
        }
    finally:
        conn.close()
