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
    species VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    live_weight NUMERIC(10,1),
    yield_grade INTEGER,
    quality_grade VARCHAR(20),
    dressing_pct NUMERIC(5,3),
    hot_carcass_weight NUMERIC(10,1),
    total_cut_value NUMERIC(12,2),
    byproduct_value NUMERIC(10,2),
    gross_value NUMERIC(12,2),
    processing_cost NUMERIC(10,2),
    net_value NUMERIC(12,2),
    value_per_lb_carcass NUMERIC(10,4),
    value_per_lb_live NUMERIC(10,4),
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
    business_name VARCHAR(150),
    address_line1 VARCHAR(150),
    address_line2 VARCHAR(150),
    zip_code VARCHAR(20),
    license_number VARCHAR(50),
    delivery_zone VARCHAR(50),
    delivery_day VARCHAR(20),
    credit_limit NUMERIC(12,2) DEFAULT 0,
    notes TEXT,
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

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    buyer_id VARCHAR(50) NOT NULL REFERENCES buyers(buyer_id),
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    delivery_date DATE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    quality_grade VARCHAR(20),
    region VARCHAR(30),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_lines (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    cut_code VARCHAR(20) NOT NULL,
    form VARCHAR(30),
    quantity_lbs NUMERIC(10,1) NOT NULL,
    price_per_lb NUMERIC(8,2) NOT NULL,
    usda_base_cwt NUMERIC(10,2),
    markup_pct NUMERIC(6,4),
    line_total NUMERIC(10,2) NOT NULL,
    fulfilled_lbs NUMERIC(10,1) DEFAULT 0,
    source_animal_id VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS invoices (
    id SERIAL PRIMARY KEY,
    invoice_id VARCHAR(50) UNIQUE NOT NULL,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    buyer_id VARCHAR(50) NOT NULL REFERENCES buyers(buyer_id),
    invoice_date DATE NOT NULL DEFAULT CURRENT_DATE,
    due_date DATE NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    paid_amount NUMERIC(12,2) DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS income_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    quality_grade VARCHAR(20) NOT NULL,
    region VARCHAR(30),
    live_weight NUMERIC(10,1),
    hcw NUMERIC(10,1),
    allocated_revenue NUMERIC(12,2),
    unallocated_revenue NUMERIC(12,2),
    byproduct_revenue NUMERIC(10,2),
    total_revenue NUMERIC(12,2),
    animal_cost NUMERIC(12,2),
    kill_fee NUMERIC(10,2),
    fabrication_cost NUMERIC(10,2),
    shrink_cost NUMERIC(10,2),
    total_cogs NUMERIC(12,2),
    gross_profit NUMERIC(12,2),
    gross_margin_pct NUMERIC(6,2),
    broker_fee NUMERIC(10,2),
    net_margin NUMERIC(12,2),
    net_margin_pct NUMERIC(6,2),
    revenue_by_primal JSONB,
    revenue_by_channel JSONB,
    allocation_detail JSONB
);

CREATE INDEX IF NOT EXISTS idx_subprimal_report_date ON usda_subprimal_prices(report_date, grade);
CREATE INDEX IF NOT EXISTS idx_subprimal_imps ON usda_subprimal_prices(imps_code, report_date);
CREATE INDEX IF NOT EXISTS idx_slaughter_report_date ON slaughter_cattle_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_auction_report_date ON indiana_auction(report_date);
CREATE INDEX IF NOT EXISTS idx_valuations_species_date ON valuations(species, valuation_date);
CREATE INDEX IF NOT EXISTS idx_valuations_grade ON valuations(quality_grade, report_date);
CREATE INDEX IF NOT EXISTS idx_purchase_price_date ON purchase_price_analysis(valuation_date);
CREATE INDEX IF NOT EXISTS idx_buyers_buyer_id ON buyers(buyer_id);
CREATE INDEX IF NOT EXISTS idx_buyer_prefs_buyer_id ON buyer_cut_preferences(buyer_id);
CREATE INDEX IF NOT EXISTS idx_demand_snapshots_date ON demand_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_allocations_date ON allocations(allocation_date);
CREATE INDEX IF NOT EXISTS idx_orders_buyer ON orders(buyer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_lines_order ON order_lines(order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_buyer ON invoices(buyer_id);
CREATE INDEX IF NOT EXISTS idx_invoices_order ON invoices(order_id);
CREATE INDEX IF NOT EXISTS idx_income_snapshots_date ON income_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_income_snapshots_grade ON income_snapshots(quality_grade, report_date);

-- Business configuration tables (effective-date versioned)

CREATE TABLE IF NOT EXISTS config_processors (
    id SERIAL PRIMARY KEY,
    processor_key VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    kill_fee NUMERIC(10,2) NOT NULL,
    fab_cost_per_lb NUMERIC(10,4) NOT NULL,
    shrink_pct NUMERIC(6,4) NOT NULL,
    payment_terms_days INTEGER NOT NULL DEFAULT 30,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (processor_key, effective_date)
);

CREATE TABLE IF NOT EXISTS config_regions (
    id SERIAL PRIMARY KEY,
    region_key VARCHAR(50) NOT NULL,
    label VARCHAR(100) NOT NULL,
    city VARCHAR(50),
    state VARCHAR(10),
    pricing_adjustment NUMERIC(6,4) NOT NULL DEFAULT 1.0,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (region_key, effective_date)
);

CREATE TABLE IF NOT EXISTS config_parameters (
    id SERIAL PRIMARY KEY,
    param_key VARCHAR(50) NOT NULL,
    param_value NUMERIC(12,4) NOT NULL,
    description TEXT,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (param_key, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_config_processors_lookup
    ON config_processors(processor_key, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_config_regions_lookup
    ON config_regions(region_key, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_config_parameters_lookup
    ON config_parameters(param_key, effective_date DESC);

-- Multi-species tables

CREATE TABLE IF NOT EXISTS pork_cutout_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    section VARCHAR(30) NOT NULL,
    item_description TEXT NOT NULL,
    weighted_average NUMERIC(10,2),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    total_pounds BIGINT
);

CREATE TABLE IF NOT EXISTS pork_primal_values (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    pork_carcass NUMERIC(10,2),
    pork_loin NUMERIC(10,2),
    pork_butt NUMERIC(10,2),
    pork_picnic NUMERIC(10,2),
    pork_rib NUMERIC(10,2),
    pork_ham NUMERIC(10,2),
    pork_belly NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS pork_live_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    purchase_type VARCHAR(50),
    head_count INTEGER,
    avg_weight NUMERIC(10,1),
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    weighted_avg_price NUMERIC(10,2),
    carcass_basis NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS lamb_cutout_prices (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    report_id INTEGER NOT NULL,
    imps_code VARCHAR(10),
    imps_description TEXT,
    fob_price NUMERIC(10,4),
    percentage_carcass NUMERIC(6,2),
    cut_weight NUMERIC(10,4),
    saddle VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS lamb_carcass_summary (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP NOT NULL DEFAULT NOW(),
    report_date DATE NOT NULL,
    gross_carcass_price NUMERIC(10,4),
    foresaddle_price NUMERIC(10,4),
    hindsaddle_price NUMERIC(10,4),
    net_carcass_price NUMERIC(10,4),
    processing_cost NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS manual_species_prices (
    id SERIAL PRIMARY KEY,
    entry_date DATE NOT NULL DEFAULT CURRENT_DATE,
    species VARCHAR(20) NOT NULL,
    cut_code VARCHAR(30) NOT NULL,
    description TEXT,
    price_per_lb NUMERIC(10,4) NOT NULL,
    yield_pct NUMERIC(6,2),
    source VARCHAR(50) DEFAULT 'manual',
    UNIQUE (species, cut_code, entry_date)
);


CREATE INDEX IF NOT EXISTS idx_pork_cutout_date ON pork_cutout_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_pork_primal_date ON pork_primal_values(report_date);
CREATE INDEX IF NOT EXISTS idx_pork_live_date ON pork_live_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_lamb_cutout_date ON lamb_cutout_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_lamb_summary_date ON lamb_carcass_summary(report_date);
CREATE INDEX IF NOT EXISTS idx_manual_species ON manual_species_prices(species, entry_date);

-- D2C + Farmer tables

CREATE TABLE IF NOT EXISTS farmers (
    id SERIAL PRIMARY KEY,
    farmer_id VARCHAR(50) UNIQUE NOT NULL,
    company_name VARCHAR(150) NOT NULL,
    contact_name VARCHAR(100),
    contact_email VARCHAR(100),
    contact_phone VARCHAR(30),
    address_line1 VARCHAR(150),
    address_line2 VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS farmer_inventory (
    id SERIAL PRIMARY KEY,
    animal_id VARCHAR(50) UNIQUE NOT NULL,
    farmer_id VARCHAR(50) NOT NULL REFERENCES farmers(farmer_id),
    species VARCHAR(20) NOT NULL,
    breed VARCHAR(50),
    lot_number VARCHAR(50),
    live_weight_est NUMERIC(10,1),
    quality_grade_est VARCHAR(20),
    yield_grade_est INTEGER,
    dressing_pct_est NUMERIC(5,3),
    age_months INTEGER,
    sex VARCHAR(10),
    frame_score INTEGER,
    expected_finish_date DATE,
    asking_price_per_lb NUMERIC(10,4),
    asking_price_head NUMERIC(10,2),
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dtc_customers (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(30) NOT NULL,
    zip_code VARCHAR(20) NOT NULL,
    address_line1 VARCHAR(150),
    address_line2 VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(10),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS purchase_orders (
    id SERIAL PRIMARY KEY,
    po_number VARCHAR(50) UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL REFERENCES dtc_customers(customer_id),
    species VARCHAR(20) NOT NULL,
    quality_grade VARCHAR(20),
    carcass_portion VARCHAR(20) NOT NULL,
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    requested_delivery_date DATE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    deposit_amount NUMERIC(10,2) DEFAULT 0,
    total_estimated NUMERIC(12,2),
    total_final NUMERIC(12,2),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS po_lines (
    id SERIAL PRIMARY KEY,
    po_number VARCHAR(50) NOT NULL REFERENCES purchase_orders(po_number) ON DELETE CASCADE,
    cut_code VARCHAR(20) NOT NULL,
    description VARCHAR(100),
    primal VARCHAR(20),
    quantity_lbs NUMERIC(10,1) NOT NULL,
    price_per_lb NUMERIC(8,4) NOT NULL,
    line_total NUMERIC(10,2) NOT NULL,
    fulfilled_lbs NUMERIC(10,1) DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS config_processor_capabilities (
    id SERIAL PRIMARY KEY,
    processor_key VARCHAR(50) NOT NULL,
    species VARCHAR(20) NOT NULL,
    daily_capacity_head INTEGER,
    city VARCHAR(50),
    state VARCHAR(10),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    organic_certified BOOLEAN DEFAULT FALSE,
    usda_inspected BOOLEAN DEFAULT TRUE,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (processor_key, species, effective_date)
);

-- Farmer indexes
CREATE INDEX IF NOT EXISTS idx_farmers_active ON farmers(active) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_farmers_state ON farmers(state);

-- Inventory indexes
CREATE INDEX IF NOT EXISTS idx_inventory_species_status ON farmer_inventory(species, status);
CREATE INDEX IF NOT EXISTS idx_inventory_species_grade ON farmer_inventory(species, quality_grade_est, status);
CREATE INDEX IF NOT EXISTS idx_inventory_farmer ON farmer_inventory(farmer_id);
CREATE INDEX IF NOT EXISTS idx_inventory_finish_date ON farmer_inventory(expected_finish_date) WHERE status = 'available';

-- DTC Customer indexes
CREATE INDEX IF NOT EXISTS idx_dtc_customers_email ON dtc_customers(email);

-- Purchase Order indexes
CREATE INDEX IF NOT EXISTS idx_po_customer ON purchase_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_po_status ON purchase_orders(status);
CREATE INDEX IF NOT EXISTS idx_po_species_status ON purchase_orders(species, status);

-- PO Line indexes
CREATE INDEX IF NOT EXISTS idx_po_lines_po ON po_lines(po_number);
CREATE INDEX IF NOT EXISTS idx_po_lines_cut_status ON po_lines(cut_code, status);

-- Processor Capabilities indexes
CREATE INDEX IF NOT EXISTS idx_proc_cap_lookup ON config_processor_capabilities(processor_key, species, effective_date DESC);
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


def save_valuation(species: str, report_date: str, valuation: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO valuations
                (species, report_date, live_weight, yield_grade, quality_grade,
                 dressing_pct, hot_carcass_weight, total_cut_value, byproduct_value,
                 gross_value, processing_cost, net_value,
                 value_per_lb_carcass, value_per_lb_live, cut_detail_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                species, report_date,
                valuation.get('live_weight'),
                valuation.get('yield_grade'),
                valuation.get('quality_grade'),
                valuation.get('dressing_pct'),
                valuation.get('hot_carcass_weight'),
                valuation.get('total_cut_value'),
                valuation.get('byproduct_value'),
                valuation.get('gross_value'),
                valuation.get('processing_cost'),
                valuation.get('net_value'),
                valuation.get('value_per_lb_carcass'),
                valuation.get('value_per_lb_live'),
                json.dumps(valuation.get('cut_detail', [])),
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
                 contact_name, contact_email, contact_phone,
                 business_name, address_line1, address_line2, zip_code,
                 license_number, delivery_zone, delivery_day, credit_limit, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    business_name = EXCLUDED.business_name,
                    address_line1 = EXCLUDED.address_line1,
                    address_line2 = EXCLUDED.address_line2,
                    zip_code = EXCLUDED.zip_code,
                    license_number = EXCLUDED.license_number,
                    delivery_zone = EXCLUDED.delivery_zone,
                    delivery_day = EXCLUDED.delivery_day,
                    credit_limit = EXCLUDED.credit_limit,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
            """, (
                buyer.buyer_id, buyer.name, buyer.buyer_type,
                buyer.city, buyer.state, buyer.region,
                buyer.min_quality_grade, buyer.payment_terms_days, buyer.active,
                buyer.contact_name, buyer.contact_email, buyer.contact_phone,
                buyer.business_name, buyer.address_line1, buyer.address_line2,
                buyer.zip_code, buyer.license_number, buyer.delivery_zone,
                buyer.delivery_day, buyer.credit_limit, buyer.notes,
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


# ---------------------------------------------------------------------------
# Order / Invoice / Income Snapshot CRUD
# ---------------------------------------------------------------------------

def save_order(order_id: str, buyer_id: str, lines: list,
               delivery_date=None, quality_grade: str = None,
               region: str = None, notes: str = None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO orders
                (order_id, buyer_id, delivery_date, quality_grade, region, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (order_id, buyer_id, delivery_date, quality_grade, region, notes))
            for ln in lines:
                line_total = ln["quantity_lbs"] * ln["price_per_lb"]
                cur.execute("""
                    INSERT INTO order_lines
                    (order_id, cut_code, form, quantity_lbs, price_per_lb,
                     usda_base_cwt, markup_pct, line_total)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_id, ln["cut_code"], ln.get("form"),
                    ln["quantity_lbs"], ln["price_per_lb"],
                    ln.get("usda_base_cwt"), ln.get("markup_pct"),
                    round(line_total, 2),
                ))
        conn.commit()
    finally:
        conn.close()


def update_order_status(order_id: str, status: str):
    from config import ORDER_STATUSES
    if status not in ORDER_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {ORDER_STATUSES}")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orders SET status = %s, updated_at = NOW()
                WHERE order_id = %s
            """, (status, order_id))
            if cur.rowcount == 0:
                raise ValueError(f"Order '{order_id}' not found")
        conn.commit()
    finally:
        conn.close()


def get_order(order_id: str) -> Optional[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            row = cur.fetchone()
            if not row:
                return None
            order = dict(row)
            cur.execute("SELECT * FROM order_lines WHERE order_id = %s", (order_id,))
            order["lines"] = [dict(r) for r in cur.fetchall()]
            return order
    finally:
        conn.close()


def get_orders_by_buyer(buyer_id: str) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT * FROM orders WHERE buyer_id = %s ORDER BY order_date DESC
            """, (buyer_id,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_invoice(invoice_id: str, order_id: str, buyer_id: str,
                 due_date, total_amount: float):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO invoices
                (invoice_id, order_id, buyer_id, due_date, total_amount)
                VALUES (%s, %s, %s, %s, %s)
            """, (invoice_id, order_id, buyer_id, due_date, total_amount))
        conn.commit()
    finally:
        conn.close()


def save_income_snapshot(inc):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO income_snapshots
                (report_date, quality_grade, region, live_weight, hcw,
                 allocated_revenue, unallocated_revenue, byproduct_revenue,
                 total_revenue, animal_cost, kill_fee, fabrication_cost,
                 shrink_cost, total_cogs, gross_profit, gross_margin_pct,
                 broker_fee, net_margin, net_margin_pct,
                 revenue_by_primal, revenue_by_channel, allocation_detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                inc.report_date, inc.quality_grade, inc.region,
                inc.live_weight, inc.hcw,
                inc.allocated_revenue, inc.unallocated_revenue,
                inc.byproduct_revenue, inc.total_revenue,
                inc.animal_cost, inc.kill_fee, inc.fabrication_cost,
                inc.shrink_cost, inc.total_cogs,
                inc.gross_profit, inc.gross_margin_pct,
                inc.broker_fee, inc.net_margin, inc.net_margin_pct,
                json.dumps(inc.revenue_by_primal),
                json.dumps(inc.revenue_by_channel),
                json.dumps(inc.allocation_detail),
            ))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Multi-species persistence
# ---------------------------------------------------------------------------

def save_pork_cutout(report_date: str, report_id: int, section: str, cuts: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for cut in cuts:
                cur.execute("""
                    INSERT INTO pork_cutout_prices
                    (report_date, report_id, section, item_description,
                     weighted_average, price_range_low, price_range_high, total_pounds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (report_date, report_id, section,
                      cut['description'], cut.get('weighted_average'),
                      cut.get('price_range_low'), cut.get('price_range_high'),
                      cut.get('total_pounds')))
        conn.commit()
    finally:
        conn.close()


def save_pork_primals(report_date: str, primals: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pork_primal_values
                (report_date, pork_carcass, pork_loin, pork_butt, pork_picnic,
                 pork_rib, pork_ham, pork_belly)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (report_date, primals.get('carcass'), primals.get('loin'),
                  primals.get('butt'), primals.get('picnic'),
                  primals.get('rib'), primals.get('ham'), primals.get('belly')))
        conn.commit()
    finally:
        conn.close()


def save_pork_live(report_date: str, report_id: int, rows: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO pork_live_prices
                    (report_date, report_id, purchase_type, head_count, avg_weight,
                     price_range_low, price_range_high, weighted_avg_price, carcass_basis)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (report_date, report_id,
                      r.get('purchase_type'), r.get('head_count'), r.get('avg_weight'),
                      r.get('price_low'), r.get('price_high'),
                      r.get('avg_price'), r.get('carcass_basis')))
        conn.commit()
    finally:
        conn.close()


def save_lamb_cutout(report_date: str, report_id: int, cuts: list):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for cut in cuts:
                cur.execute("""
                    INSERT INTO lamb_cutout_prices
                    (report_date, report_id, imps_code, imps_description,
                     fob_price, percentage_carcass, cut_weight, saddle)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (report_date, report_id,
                      cut.get('imps_code'), cut.get('description'),
                      cut.get('fob_price'), cut.get('percentage_carcass'),
                      cut.get('cut_weight'), cut.get('saddle')))
        conn.commit()
    finally:
        conn.close()


def save_lamb_summary(report_date: str, summary: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO lamb_carcass_summary
                (report_date, gross_carcass_price, foresaddle_price,
                 hindsaddle_price, net_carcass_price, processing_cost)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (report_date, summary.get('gross'), summary.get('foresaddle'),
                  summary.get('hindsaddle'), summary.get('net'),
                  summary.get('processing_cost')))
        conn.commit()
    finally:
        conn.close()


def save_manual_price(species: str, cut_code: str, description: str,
                      price_per_lb: float, yield_pct: float = None,
                      source: str = 'manual'):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO manual_species_prices
                (species, cut_code, description, price_per_lb, yield_pct, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (species, cut_code, entry_date) DO UPDATE SET
                    description = EXCLUDED.description,
                    price_per_lb = EXCLUDED.price_per_lb,
                    yield_pct = EXCLUDED.yield_pct,
                    source = EXCLUDED.source
            """, (species, cut_code, description, price_per_lb, yield_pct, source))
        conn.commit()
    finally:
        conn.close()


def get_latest_manual_prices(species: str) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (cut_code)
                    cut_code, description, price_per_lb, yield_pct, entry_date, source
                FROM manual_species_prices
                WHERE species = %s
                ORDER BY cut_code, entry_date DESC
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# D2C + Farmer CRUD
# ---------------------------------------------------------------------------

def save_farmer(farmer: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO farmers
                (farmer_id, company_name, contact_name, contact_email, contact_phone,
                 address_line1, address_line2, city, state, zip_code,
                 latitude, longitude, active, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (farmer_id) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    contact_name = EXCLUDED.contact_name,
                    contact_email = EXCLUDED.contact_email,
                    contact_phone = EXCLUDED.contact_phone,
                    address_line1 = EXCLUDED.address_line1,
                    address_line2 = EXCLUDED.address_line2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    active = EXCLUDED.active,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
            """, (
                farmer.get('farmer_id'), farmer.get('company_name'),
                farmer.get('contact_name'), farmer.get('contact_email'),
                farmer.get('contact_phone'),
                farmer.get('address_line1'), farmer.get('address_line2'),
                farmer.get('city'), farmer.get('state'), farmer.get('zip_code'),
                farmer.get('latitude'), farmer.get('longitude'),
                farmer.get('active', True), farmer.get('notes'),
            ))
        conn.commit()
    finally:
        conn.close()


def get_all_farmers(active_only: bool = True) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if active_only:
                cur.execute("SELECT * FROM farmers WHERE active = TRUE ORDER BY company_name")
            else:
                cur.execute("SELECT * FROM farmers ORDER BY company_name")
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_farmer(farmer_id: str) -> Optional[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM farmers WHERE farmer_id = %s", (farmer_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def save_animal(animal: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO farmer_inventory
                (animal_id, farmer_id, species, breed, lot_number,
                 live_weight_est, quality_grade_est, yield_grade_est,
                 dressing_pct_est, age_months, sex, frame_score,
                 expected_finish_date, asking_price_per_lb, asking_price_head,
                 status, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                animal.get('animal_id'), animal.get('farmer_id'),
                animal.get('species'), animal.get('breed'),
                animal.get('lot_number'), animal.get('live_weight_est'),
                animal.get('quality_grade_est'), animal.get('yield_grade_est'),
                animal.get('dressing_pct_est'), animal.get('age_months'),
                animal.get('sex'), animal.get('frame_score'),
                animal.get('expected_finish_date'),
                animal.get('asking_price_per_lb'), animal.get('asking_price_head'),
                animal.get('status', 'available'), animal.get('notes'),
            ))
        conn.commit()
    finally:
        conn.close()


def update_animal_status(animal_id: str, status: str):
    from config import FARMER_INVENTORY_STATUSES
    if status not in FARMER_INVENTORY_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {FARMER_INVENTORY_STATUSES}")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE farmer_inventory SET status = %s, updated_at = NOW()
                WHERE animal_id = %s
            """, (status, animal_id))
            if cur.rowcount == 0:
                raise ValueError(f"Animal '{animal_id}' not found")
        conn.commit()
    finally:
        conn.close()


def get_available_animals(species: str, quality_grade: str = None) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if quality_grade:
                cur.execute("""
                    SELECT * FROM farmer_inventory
                    WHERE species = %s AND status = 'available'
                      AND quality_grade_est = %s
                    ORDER BY expected_finish_date, live_weight_est DESC
                """, (species, quality_grade))
            else:
                cur.execute("""
                    SELECT * FROM farmer_inventory
                    WHERE species = %s AND status = 'available'
                    ORDER BY expected_finish_date, live_weight_est DESC
                """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_farmer_animals(farmer_id: str) -> list:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT * FROM farmer_inventory
                WHERE farmer_id = %s ORDER BY created_at DESC
            """, (farmer_id,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_dtc_customer(customer: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dtc_customers
                (customer_id, first_name, last_name, email, phone, zip_code,
                 address_line1, address_line2, city, state, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    zip_code = EXCLUDED.zip_code,
                    address_line1 = EXCLUDED.address_line1,
                    address_line2 = EXCLUDED.address_line2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
            """, (
                customer.get('customer_id'), customer.get('first_name'),
                customer.get('last_name'), customer.get('email'),
                customer.get('phone'), customer.get('zip_code'),
                customer.get('address_line1'), customer.get('address_line2'),
                customer.get('city'), customer.get('state'), customer.get('notes'),
            ))
        conn.commit()
    finally:
        conn.close()


def get_dtc_customer(customer_id: str) -> Optional[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM dtc_customers WHERE customer_id = %s", (customer_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def save_purchase_order(po_number: str, customer_id: str, species: str,
                        quality_grade: str, carcass_portion: str,
                        requested_delivery_date=None, deposit_amount: float = 0,
                        notes: str = None, carcass_weight: float = None,
                        price_per_lb_override: dict = None):
    """Insert PO header and auto-generate po_lines from species yield tables.

    Args:
        carcass_weight: estimated carcass weight in lbs (used to calculate line quantities).
                        If None, uses species defaults from config.
        price_per_lb_override: optional dict of {cut_code: price_per_lb} overrides.
                               If a cut_code is missing, defaults to 0.
    """
    from config import (SUBPRIMAL_YIELDS, PORK_CUT_YIELDS, LAMB_SUBPRIMAL_YIELDS,
                        CARCASS_PORTIONS, FRONT_QUARTER_PRIMALS, HIND_QUARTER_PRIMALS,
                        DEFAULT_LIVE_WEIGHT, DRESS_PCT_BY_YG, DEFAULT_YIELD_GRADE,
                        DEFAULT_PORK_LIVE_WEIGHT, DEFAULT_PORK_DRESS_PCT,
                        DEFAULT_LAMB_LIVE_WEIGHT, DEFAULT_LAMB_DRESS_PCT)

    if carcass_portion not in CARCASS_PORTIONS:
        raise ValueError(f"Invalid carcass_portion '{carcass_portion}'. Must be one of: {CARCASS_PORTIONS}")

    # Select yield table and default carcass weight by species
    if species == 'cattle':
        yield_table = SUBPRIMAL_YIELDS
        if carcass_weight is None:
            carcass_weight = DEFAULT_LIVE_WEIGHT * DRESS_PCT_BY_YG[DEFAULT_YIELD_GRADE]
    elif species == 'pork':
        yield_table = PORK_CUT_YIELDS
        if carcass_weight is None:
            carcass_weight = DEFAULT_PORK_LIVE_WEIGHT * DEFAULT_PORK_DRESS_PCT
    elif species == 'lamb':
        yield_table = LAMB_SUBPRIMAL_YIELDS
        if carcass_weight is None:
            carcass_weight = DEFAULT_LAMB_LIVE_WEIGHT * DEFAULT_LAMB_DRESS_PCT
    else:
        raise ValueError(f"Unsupported species '{species}' for PO line generation")

    if price_per_lb_override is None:
        price_per_lb_override = {}

    # Build line items from yield table filtered by portion
    lines = []
    for cut_code, (desc, yield_pct, primal) in yield_table.items():
        if carcass_portion == 'quarter_front':
            if primal not in FRONT_QUARTER_PRIMALS:
                continue
        elif carcass_portion == 'quarter_hind':
            if primal not in HIND_QUARTER_PRIMALS:
                continue

        qty = carcass_weight * (yield_pct / 100.0)
        if carcass_portion == 'half':
            qty *= 0.5

        ppl = price_per_lb_override.get(cut_code, 0)
        lines.append({
            'cut_code': cut_code,
            'description': desc,
            'primal': primal,
            'quantity_lbs': round(qty, 1),
            'price_per_lb': ppl,
            'line_total': round(qty * ppl, 2),
        })

    total_estimated = sum(ln['line_total'] for ln in lines)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO purchase_orders
                (po_number, customer_id, species, quality_grade, carcass_portion,
                 requested_delivery_date, deposit_amount, total_estimated, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (po_number, customer_id, species, quality_grade, carcass_portion,
                  requested_delivery_date, deposit_amount, total_estimated, notes))
            for ln in lines:
                cur.execute("""
                    INSERT INTO po_lines
                    (po_number, cut_code, description, primal,
                     quantity_lbs, price_per_lb, line_total)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (po_number, ln['cut_code'], ln['description'], ln['primal'],
                      ln['quantity_lbs'], ln['price_per_lb'], ln['line_total']))
        conn.commit()
    finally:
        conn.close()


def update_po_status(po_number: str, status: str):
    from config import PO_STATUSES
    if status not in PO_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {PO_STATUSES}")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE purchase_orders SET status = %s, updated_at = NOW()
                WHERE po_number = %s
            """, (status, po_number))
            if cur.rowcount == 0:
                raise ValueError(f"PO '{po_number}' not found")
        conn.commit()
    finally:
        conn.close()


def get_purchase_order(po_number: str) -> Optional[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM purchase_orders WHERE po_number = %s", (po_number,))
            row = cur.fetchone()
            if not row:
                return None
            po = dict(row)
            cur.execute("SELECT * FROM po_lines WHERE po_number = %s", (po_number,))
            po["lines"] = [dict(r) for r in cur.fetchall()]
            return po
    finally:
        conn.close()


def get_pending_demand(species: str) -> list:
    """Aggregate unfulfilled PO line demand by cut_code for a species."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT pl.cut_code, pl.description, pl.primal,
                       SUM(pl.quantity_lbs - pl.fulfilled_lbs) AS pending_lbs,
                       COUNT(*) AS line_count
                FROM po_lines pl
                JOIN purchase_orders po ON po.po_number = pl.po_number
                WHERE po.species = %s
                  AND pl.status IN ('pending', 'partial')
                  AND po.status NOT IN ('cancelled', 'fulfilled')
                GROUP BY pl.cut_code, pl.description, pl.primal
                HAVING SUM(pl.quantity_lbs - pl.fulfilled_lbs) > 0
                ORDER BY pending_lbs DESC
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_processor_capability(processor_key: str, species: str,
                              daily_capacity_head: int = None,
                              city: str = None, state: str = None,
                              latitude: float = None, longitude: float = None,
                              organic_certified: bool = False,
                              usda_inspected: bool = True,
                              effective_date=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config_processor_capabilities
                (processor_key, species, daily_capacity_head,
                 city, state, latitude, longitude,
                 organic_certified, usda_inspected, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, CURRENT_DATE))
                ON CONFLICT (processor_key, species, effective_date) DO UPDATE SET
                    daily_capacity_head = EXCLUDED.daily_capacity_head,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    organic_certified = EXCLUDED.organic_certified,
                    usda_inspected = EXCLUDED.usda_inspected
            """, (processor_key, species, daily_capacity_head,
                  city, state, latitude, longitude,
                  organic_certified, usda_inspected, effective_date))
        conn.commit()
    finally:
        conn.close()


