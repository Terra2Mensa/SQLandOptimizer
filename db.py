"""PostgreSQL persistence layer for cattle valuation data."""
import json
from datetime import datetime
from typing import Optional

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from config import DB_CONFIG

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS usda_subprimal_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    grade VARCHAR(20) NOT NULL,
    imps_code VARCHAR(10) NOT NULL,
    item_description TEXT,
    weighted_avg_cwt NUMERIC(10,2),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    number_trades INTEGER,
    total_pounds BIGINT
);

CREATE TABLE IF NOT EXISTS usda_composites (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    primal VARCHAR(30),
    choice_cwt NUMERIC(10,2),
    select_cwt NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS slaughter_cattle_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    source VARCHAR(50),
    class_description VARCHAR(50),
    selling_basis VARCHAR(30),
    grade_description VARCHAR(30),
    head_count INTEGER,
    avg_weight NUMERIC(10,1),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    weighted_avg_price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS premiums_discounts (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    type VARCHAR(20),
    class_description VARCHAR(40),
    avg_price NUMERIC(10,2),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    price_change NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS indiana_auction (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    commodity VARCHAR(40),
    class VARCHAR(40),
    quality_grade VARCHAR(30),
    frame VARCHAR(20),
    dressing VARCHAR(10),
    yield_grade VARCHAR(10),
    head_count INTEGER,
    avg_weight NUMERIC(10,1),
    avg_price_min NUMERIC(10,2),
    avg_price_max NUMERIC(10,2),
    avg_price NUMERIC(10,2),
    receipts INTEGER
);

CREATE TABLE IF NOT EXISTS valuations (
    id SERIAL PRIMARY KEY,
    valuation_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    live_weight NUMERIC(10,1),
    yield_grade INTEGER,
    quality_grade VARCHAR(20),
    dressing_pct NUMERIC(5,3),
    hot_carcass_weight NUMERIC(10,1),
    total_subprimal_value NUMERIC(12,2),
    byproduct_value NUMERIC(10,2),
    gross_carcass_value NUMERIC(12,2),
    broker_fee NUMERIC(10,2),
    net_carcass_value NUMERIC(12,2),
    value_per_cwt_carcass NUMERIC(10,2),
    value_per_cwt_live NUMERIC(10,2),
    cut_detail_json JSONB
);

CREATE TABLE IF NOT EXISTS purchase_price_analysis (
    id SERIAL PRIMARY KEY,
    valuation_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    quality_grade VARCHAR(20),
    processor_name VARCHAR(50),
    live_weight NUMERIC(10,1),
    yield_grade INTEGER,
    dressing_pct NUMERIC(5,3),
    live_basis_cwt NUMERIC(10,2),
    dressed_basis_cwt NUMERIC(10,2),
    grid_formula_cwt NUMERIC(10,2),
    cutout_minus_margin_cwt NUMERIC(10,2),
    kill_fee NUMERIC(10,2),
    fab_cost_per_lb NUMERIC(10,4),
    shrink_pct NUMERIC(5,4),
    detail_json JSONB
);

CREATE TABLE IF NOT EXISTS buyers (
    id SERIAL PRIMARY KEY,
    buyer_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    buyer_type VARCHAR(30) NOT NULL,
    city VARCHAR(50),
    state VARCHAR(10),
    region VARCHAR(30),
    min_quality_grade VARCHAR(20),
    payment_terms_days INTEGER,
    active BOOLEAN DEFAULT TRUE,
    contact_name VARCHAR(100),
    contact_email VARCHAR(100),
    contact_phone VARCHAR(30),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS buyer_cut_preferences (
    id SERIAL PRIMARY KEY,
    buyer_id VARCHAR(50) NOT NULL REFERENCES buyers(buyer_id) ON DELETE CASCADE,
    cut_code VARCHAR(20) NOT NULL,
    form VARCHAR(30),
    markup_pct NUMERIC(6,4),
    fixed_premium_per_lb NUMERIC(8,4),
    volume_lbs_week NUMERIC(10,1),
    use_fixed_premium BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS demand_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_date TIMESTAMP NOT NULL DEFAULT NOW(),
    quality_grade VARCHAR(20) NOT NULL,
    region VARCHAR(30),
    animals_needed NUMERIC(10,1),
    bottleneck_cut VARCHAR(20),
    detail JSONB
);

CREATE TABLE IF NOT EXISTS allocations (
    id SERIAL PRIMARY KEY,
    allocation_date TIMESTAMP NOT NULL DEFAULT NOW(),
    quality_grade VARCHAR(20) NOT NULL,
    region VARCHAR(30),
    hcw NUMERIC(10,1),
    live_weight NUMERIC(10,1),
    total_revenue NUMERIC(12,2),
    unallocated_value NUMERIC(12,2),
    farmer_cost NUMERIC(12,2),
    processing_cost NUMERIC(12,2),
    gross_margin NUMERIC(12,2),
    margin_pct NUMERIC(6,2),
    line_items JSONB
);

CREATE INDEX IF NOT EXISTS idx_subprimal_report_date ON usda_subprimal_prices(report_date, grade);
CREATE INDEX IF NOT EXISTS idx_subprimal_imps ON usda_subprimal_prices(imps_code, report_date);
CREATE INDEX IF NOT EXISTS idx_slaughter_report_date ON slaughter_cattle_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_auction_report_date ON indiana_auction(report_date);
CREATE INDEX IF NOT EXISTS idx_valuations_date ON valuations(valuation_date);
CREATE INDEX IF NOT EXISTS idx_purchase_price_date ON purchase_price_analysis(valuation_date);
CREATE INDEX IF NOT EXISTS idx_buyers_buyer_id ON buyers(buyer_id);
CREATE INDEX IF NOT EXISTS idx_buyer_prefs_buyer_id ON buyer_cut_preferences(buyer_id);
CREATE INDEX IF NOT EXISTS idx_demand_snapshots_date ON demand_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_allocations_date ON allocations(allocation_date);
"""


def get_connection():
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 not installed. Run: pip3 install psycopg2-binary")
    return psycopg2.connect(**DB_CONFIG)


def init_schema():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        print("Database schema initialized.")
    finally:
        conn.close()


def save_subprimal_prices(report_date: str, report_id: int, grade: str, cuts: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for cut in cuts:
                cur.execute("""
                    INSERT INTO usda_subprimal_prices
                    (report_date, report_id, grade, imps_code, item_description,
                     weighted_avg_cwt, price_range_low, price_range_high,
                     number_trades, total_pounds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    report_date, report_id, grade,
                    cut.imps_code, cut.description,
                    cut.weighted_avg_cwt, cut.price_range_low, cut.price_range_high,
                    cut.number_trades, cut.total_pounds,
                ))
        conn.commit()
    finally:
        conn.close()


def save_composites(report_date: str, composites: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for primal, vals in composites.items():
                cur.execute("""
                    INSERT INTO usda_composites (report_date, primal, choice_cwt, select_cwt)
                    VALUES (%s, %s, %s, %s)
                """, (report_date, primal, vals.get('choice', 0), vals.get('select', 0)))
        conn.commit()
    finally:
        conn.close()


def save_slaughter_cattle(report_date: str, report_id: int, source: str, rows: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO slaughter_cattle_prices
                    (report_date, report_id, source, class_description, selling_basis,
                     grade_description, head_count, avg_weight,
                     price_range_low, price_range_high, weighted_avg_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    report_date, report_id, source,
                    r.get('class'), r.get('basis'), r.get('grade'),
                    r.get('head_count'), r.get('avg_weight'),
                    r.get('price_low'), r.get('price_high'), r.get('avg_price'),
                ))
        conn.commit()
    finally:
        conn.close()


def save_premiums_discounts(report_date: str, rows: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO premiums_discounts
                    (report_date, type, class_description, avg_price,
                     price_range_low, price_range_high, price_change)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    report_date, r.get('type'), r.get('class'),
                    r.get('avg_price'), r.get('price_low'),
                    r.get('price_high'), r.get('price_change'),
                ))
        conn.commit()
    finally:
        conn.close()


def save_indiana_auction(report_date: str, rows: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO indiana_auction
                    (report_date, commodity, class, quality_grade, frame, dressing,
                     yield_grade, head_count, avg_weight,
                     avg_price_min, avg_price_max, avg_price, receipts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    report_date, r.get('commodity'), r.get('class'),
                    r.get('quality_grade'), r.get('frame'), r.get('dressing'),
                    r.get('yield_grade'), r.get('head_count'), r.get('avg_weight'),
                    r.get('avg_price_min'), r.get('avg_price_max'),
                    r.get('avg_price'), r.get('receipts'),
                ))
        conn.commit()
    finally:
        conn.close()


def save_valuation(valuation):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO valuations
                (report_date, live_weight, yield_grade, quality_grade, dressing_pct,
                 hot_carcass_weight, total_subprimal_value, byproduct_value,
                 gross_carcass_value, broker_fee, net_carcass_value,
                 value_per_cwt_carcass, value_per_cwt_live, cut_detail_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                valuation.report_date, valuation.live_weight,
                valuation.yield_grade, valuation.quality_grade,
                valuation.dressing_pct, valuation.hot_carcass_weight,
                valuation.total_subprimal_value, valuation.byproduct_value,
                valuation.total_carcass_value, valuation.broker_fee,
                valuation.net_value, valuation.value_per_cwt_carcass,
                valuation.value_per_cwt_live,
                json.dumps(valuation.cut_values),
            ))
        conn.commit()
    finally:
        conn.close()


def save_purchase_prices(report_date: str, purchase_result, yield_grade: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            pp = purchase_result
            detail = {
                'live_market_ref_cwt': pp.live_market_ref_cwt,
                'dressed_market_ref_cwt': pp.dressed_market_ref_cwt,
                'auction_ref_cwt': pp.auction_ref_cwt,
                'grid_base_cwt': pp.grid_base_cwt,
                'grid_quality_adj': pp.grid_quality_adj,
                'grid_yg_adj': pp.grid_yg_adj,
                'grid_wt_adj': pp.grid_wt_adj,
                'processor_costs': pp.processor_costs,
            }
            cur.execute("""
                INSERT INTO purchase_price_analysis
                (report_date, quality_grade, processor_name, live_weight, yield_grade,
                 dressing_pct, live_basis_cwt, dressed_basis_cwt, grid_formula_cwt,
                 cutout_minus_margin_cwt, kill_fee, fab_cost_per_lb, shrink_pct, detail_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                report_date, pp.quality_grade, pp.processor_name,
                pp.live_weight, yield_grade, pp.dressing_pct,
                pp.live_basis_cwt, pp.dressed_basis_cwt,
                pp.grid_formula_cwt, pp.cutout_minus_margin_cwt,
                pp.processor_costs.get('kill_fee', 0),
                pp.processor_costs.get('fab_cost_per_lb', 0),
                pp.processor_costs.get('shrink_pct', 0),
                json.dumps(detail),
            ))
        conn.commit()
    finally:
        conn.close()


def get_latest_prices(grade: str = "Choice") -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT * FROM usda_subprimal_prices
                WHERE grade = %s
                AND report_date = (SELECT MAX(report_date) FROM usda_subprimal_prices WHERE grade = %s)
                ORDER BY imps_code
            """, (grade, grade))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_price_history(imps_code: str, grade: str = "Choice", days: int = 90) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT report_date, weighted_avg_cwt
                FROM usda_subprimal_prices
                WHERE imps_code = %s AND grade = %s
                AND report_date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY report_date
            """, (imps_code, grade, days))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Demand-side persistence
# ---------------------------------------------------------------------------

def save_buyer(buyer):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO buyers
                (buyer_id, name, buyer_type, city, state, region,
                 min_quality_grade, payment_terms_days, active,
                 contact_name, contact_email, contact_phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (buyer_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    buyer_type = EXCLUDED.buyer_type,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    region = EXCLUDED.region,
                    min_quality_grade = EXCLUDED.min_quality_grade,
                    payment_terms_days = EXCLUDED.payment_terms_days,
                    active = EXCLUDED.active,
                    contact_name = EXCLUDED.contact_name,
                    contact_email = EXCLUDED.contact_email,
                    contact_phone = EXCLUDED.contact_phone,
                    updated_at = NOW()
            """, (
                buyer.buyer_id, buyer.name, buyer.buyer_type,
                buyer.city, buyer.state, buyer.region,
                buyer.min_quality_grade, buyer.payment_terms_days, buyer.active,
                buyer.contact_name, buyer.contact_email, buyer.contact_phone,
            ))
        conn.commit()
    finally:
        conn.close()


def save_buyer_preferences(buyer):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM buyer_cut_preferences WHERE buyer_id = %s", (buyer.buyer_id,))
            for pref in buyer.cut_preferences:
                cur.execute("""
                    INSERT INTO buyer_cut_preferences
                    (buyer_id, cut_code, form, markup_pct, fixed_premium_per_lb,
                     volume_lbs_week, use_fixed_premium)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    buyer.buyer_id, pref.cut_code, pref.form,
                    pref.markup_pct, pref.fixed_premium_per_lb,
                    pref.volume_lbs_week, pref.use_fixed_premium,
                ))
        conn.commit()
    finally:
        conn.close()


def get_all_buyers() -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM buyers ORDER BY buyer_id")
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_demand_snapshot(quality_grade: str, region: str, animals_needed: float,
                         bottleneck: str, detail: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO demand_snapshots
                (quality_grade, region, animals_needed, bottleneck_cut, detail)
                VALUES (%s, %s, %s, %s, %s)
            """, (quality_grade, region, animals_needed, bottleneck, json.dumps(detail)))
        conn.commit()
    finally:
        conn.close()


def save_allocation(result):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            line_items = [
                {"buyer_id": a.buyer_id, "buyer_name": a.buyer_name,
                 "cut_code": a.cut_code, "lbs": a.lbs_allocated,
                 "price_lb": a.price_per_lb, "revenue": a.line_revenue}
                for a in result.allocations
            ]
            cur.execute("""
                INSERT INTO allocations
                (quality_grade, region, hcw, live_weight, total_revenue,
                 unallocated_value, farmer_cost, processing_cost,
                 gross_margin, margin_pct, line_items)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                result.quality_grade, None, result.hcw, result.live_weight,
                result.total_revenue, result.unallocated_value,
                result.farmer_cost, result.processing_cost,
                result.gross_margin, result.margin_pct,
                json.dumps(line_items),
            ))
        conn.commit()
    finally:
        conn.close()
