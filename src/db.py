"""PostgreSQL persistence layer for Terra Mensa valuation data."""
import json
from datetime import datetime
from typing import Optional

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from config import DB_CONFIG, DB_TARGET, SUPABASE_DB_CONFIG

SCHEMA_SQL = """
-- USDA market data tables (kept as-is)

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

-- USDA indexes
CREATE INDEX IF NOT EXISTS idx_subprimal_report_date ON usda_subprimal_prices(report_date, grade);
CREATE INDEX IF NOT EXISTS idx_subprimal_imps ON usda_subprimal_prices(imps_code, report_date);
CREATE INDEX IF NOT EXISTS idx_slaughter_report_date ON slaughter_cattle_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_auction_report_date ON indiana_auction(report_date);
CREATE INDEX IF NOT EXISTS idx_pork_cutout_date ON pork_cutout_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_pork_primal_date ON pork_primal_values(report_date);
CREATE INDEX IF NOT EXISTS idx_pork_live_date ON pork_live_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_lamb_cutout_date ON lamb_cutout_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_lamb_summary_date ON lamb_carcass_summary(report_date);
CREATE INDEX IF NOT EXISTS idx_manual_species ON manual_species_prices(species, entry_date);

-- Core business tables

CREATE TABLE IF NOT EXISTS profiles (
    profile_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(20) NOT NULL,
    first VARCHAR(50),
    last VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(30),
    address TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    company_name VARCHAR(150),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS farmer_inventory (
    animal_id VARCHAR(50) PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    live_weight_est NUMERIC(10,1),
    expected_grade VARCHAR(20),
    expected_finish_date DATE,
    active BOOLEAN DEFAULT TRUE,
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS purchase_orders (
    po_number VARCHAR(50) PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    share VARCHAR(20) NOT NULL,
    note TEXT,
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    deposit NUMERIC(10,2) DEFAULT 0,
    customer_preferences TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS po_cut_instructions (
    id SERIAL PRIMARY KEY,
    po_number VARCHAR(50) NOT NULL REFERENCES purchase_orders(po_number) ON DELETE CASCADE,
    cut_code VARCHAR(30) NOT NULL,
    instruction VARCHAR(100),
    quantity VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS config_cut_specs (
    id SERIAL PRIMARY KEY,
    species VARCHAR(20) NOT NULL,
    primal_code VARCHAR(30) NOT NULL,
    primal_name VARCHAR(80) NOT NULL,
    cut_code VARCHAR(30) NOT NULL,
    cut_name VARCHAR(100) NOT NULL,
    yield_pct NUMERIC(5,2) NOT NULL,
    min_grade VARCHAR(20),
    is_premium BOOLEAN DEFAULT FALSE,
    notes TEXT,
    UNIQUE (species, cut_code)
);

CREATE TABLE IF NOT EXISTS config_grade_hierarchy (
    id SERIAL PRIMARY KEY,
    species VARCHAR(20) NOT NULL,
    grade_code VARCHAR(20) NOT NULL,
    grade_name VARCHAR(50) NOT NULL,
    rank_order INTEGER NOT NULL,
    UNIQUE (species, grade_code)
);

CREATE TABLE IF NOT EXISTS slaughter_orders (
    order_number VARCHAR(50) PRIMARY KEY,
    animal_id VARCHAR(50) REFERENCES farmer_inventory(animal_id),
    profile_id VARCHAR(50) REFERENCES profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'planned',
    actual_hanging_weight NUMERIC(10,2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS slaughter_order_allocations (
    id SERIAL PRIMARY KEY,
    slaughter_order_number VARCHAR(50) NOT NULL REFERENCES slaughter_orders(order_number) ON DELETE CASCADE,
    po_number VARCHAR(50) NOT NULL REFERENCES purchase_orders(po_number),
    share VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS processor_costs (
    id SERIAL PRIMARY KEY,
    profile_id VARCHAR(50) NOT NULL REFERENCES profiles(profile_id),
    species VARCHAR(20) NOT NULL,
    kill_fee NUMERIC(10,2) NOT NULL,
    fab_cost_per_lb NUMERIC(10,4) NOT NULL,
    shrink_pct NUMERIC(6,4) NOT NULL,
    daily_capacity_head INTEGER,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (profile_id, species, effective_date)
);

-- Core business indexes
CREATE INDEX IF NOT EXISTS idx_profiles_type ON profiles(type);
CREATE INDEX IF NOT EXISTS idx_profiles_active ON profiles(active) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_inventory_profile ON farmer_inventory(profile_id);
CREATE INDEX IF NOT EXISTS idx_inventory_species_status ON farmer_inventory(species, status);
CREATE INDEX IF NOT EXISTS idx_po_profile ON purchase_orders(profile_id);
CREATE INDEX IF NOT EXISTS idx_po_status ON purchase_orders(status);
CREATE INDEX IF NOT EXISTS idx_po_species_status ON purchase_orders(species, status);
CREATE INDEX IF NOT EXISTS idx_po_cut_instructions_po ON po_cut_instructions(po_number);
CREATE INDEX IF NOT EXISTS idx_cut_specs_species ON config_cut_specs(species);
CREATE INDEX IF NOT EXISTS idx_cut_specs_cut ON config_cut_specs(cut_code);
CREATE INDEX IF NOT EXISTS idx_grade_hierarchy_species ON config_grade_hierarchy(species);
CREATE INDEX IF NOT EXISTS idx_slaughter_status ON slaughter_orders(status);
CREATE INDEX IF NOT EXISTS idx_slaughter_animal ON slaughter_orders(animal_id);
CREATE INDEX IF NOT EXISTS idx_so_alloc_order ON slaughter_order_allocations(slaughter_order_number);
CREATE INDEX IF NOT EXISTS idx_so_alloc_po ON slaughter_order_allocations(po_number);
CREATE INDEX IF NOT EXISTS idx_processor_costs_lookup ON processor_costs(profile_id, species, effective_date DESC);

CREATE TABLE IF NOT EXISTS weekly_pricing (
    id SERIAL PRIMARY KEY,
    species VARCHAR(20) NOT NULL,
    grade VARCHAR(20) NOT NULL,
    price_per_lb NUMERIC(10,4) NOT NULL,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (species, grade, effective_date)
);

CREATE TABLE IF NOT EXISTS share_adjustments (
    id SERIAL PRIMARY KEY,
    share VARCHAR(20) NOT NULL,
    adjustment_pct NUMERIC(6,4) NOT NULL DEFAULT 0,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (share, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_weekly_pricing_lookup ON weekly_pricing(species, grade, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_share_adjustments_lookup ON share_adjustments(share, effective_date DESC);
"""


def get_connection():
    """Return a database connection based on DB_TARGET setting."""
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 not installed. Run: pip3 install psycopg2-binary")
    if DB_TARGET == "local":
        return psycopg2.connect(**DB_CONFIG)
    return get_supabase_connection()


def get_supabase_connection():
    """Connect to Supabase PostgreSQL (production database).

    Sets search_path to 'public, engine' so unqualified table names resolve
    correctly: public tables (profiles, purchase_orders, etc.) resolve first,
    engine tables (usda_* etc.) resolve second.
    """
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 not installed. Run: pip3 install psycopg2-binary")
    if not SUPABASE_DB_CONFIG.get("host"):
        raise ValueError("SUPABASE_DB_HOST not configured. Check .env file.")
    conn = psycopg2.connect(**SUPABASE_DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO engine, public;")
    return conn


def init_schema():
    if DB_TARGET != "local":
        print("Database schema initialized.")  # Supabase schema managed via sql/supabase_init.sql
        return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        print("Database schema initialized.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# USDA market data persistence (kept — these tables are unchanged)
# ---------------------------------------------------------------------------

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
# Multi-species persistence (kept — these tables are unchanged)
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
# Core business CRUD (profiles, farmer_inventory, purchase_orders, etc.)
# ---------------------------------------------------------------------------

def save_profile(profile: dict):
    """Upsert a profile (farmer, customer, or processor)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO profiles
                (profile_id, type, first, last, email, phone, address,
                 latitude, longitude, company_name, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (profile_id) DO UPDATE SET
                    type = EXCLUDED.type,
                    first = EXCLUDED.first,
                    last = EXCLUDED.last,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    company_name = EXCLUDED.company_name,
                    active = EXCLUDED.active,
                    updated_at = NOW()
            """, (
                profile['profile_id'], profile['type'],
                profile.get('first'), profile.get('last'),
                profile.get('email'), profile.get('phone'),
                profile.get('address'),
                profile.get('latitude'), profile.get('longitude'),
                profile.get('company_name'),
                profile.get('active', True),
            ))
        conn.commit()
    finally:
        conn.close()


def save_farmer_animal(animal: dict):
    """Insert a new animal into farmer_inventory."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO farmer_inventory
                (animal_id, profile_id, species, live_weight_est,
                 expected_grade, expected_finish_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (animal_id) DO UPDATE SET
                    profile_id = EXCLUDED.profile_id,
                    species = EXCLUDED.species,
                    live_weight_est = EXCLUDED.live_weight_est,
                    expected_grade = EXCLUDED.expected_grade,
                    expected_finish_date = EXCLUDED.expected_finish_date,
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, (
                animal['animal_id'], animal['profile_id'],
                animal['species'], animal.get('live_weight_est'),
                animal.get('expected_grade'),
                animal.get('expected_finish_date'),
                animal.get('status', 'available'),
            ))
        conn.commit()
    finally:
        conn.close()


def update_animal_status(animal_id: str, status: str):
    """Update farmer_inventory status."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE farmer_inventory
                SET status = %s, updated_at = NOW()
                WHERE animal_id = %s
            """, (status, animal_id))
        conn.commit()
    finally:
        conn.close()


def get_available_animals(species: str) -> list:
    """Return available animals for a species, with farmer profile location."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT fi.animal_id, fi.profile_id, fi.species,
                       fi.live_weight_est, fi.expected_grade,
                       fi.expected_finish_date, fi.status,
                       p.latitude AS farmer_lat, p.longitude AS farmer_lng,
                       p.company_name AS farmer_name
                FROM farmer_inventory fi
                JOIN profiles p ON p.profile_id = fi.profile_id
                WHERE fi.species = %s
                  AND fi.status = 'available'
                  AND fi.active = TRUE
                ORDER BY fi.expected_finish_date ASC NULLS LAST
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_purchase_order(po: dict):
    """Insert a share-based purchase order."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO purchase_orders
                (po_number, profile_id, species, share, note,
                 deposit, customer_preferences, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (po_number) DO UPDATE SET
                    profile_id = EXCLUDED.profile_id,
                    species = EXCLUDED.species,
                    share = EXCLUDED.share,
                    note = EXCLUDED.note,
                    deposit = EXCLUDED.deposit,
                    customer_preferences = EXCLUDED.customer_preferences,
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, (
                po['po_number'], po['profile_id'],
                po['species'], po['share'],
                po.get('note'), po.get('deposit', 0),
                po.get('customer_preferences'),
                po.get('status', 'pending'),
            ))
        conn.commit()
    finally:
        conn.close()


def update_po_status(po_number: str, status: str):
    """Update purchase_orders status."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE purchase_orders
                SET status = %s, updated_at = NOW()
                WHERE po_number = %s
            """, (status, po_number))
        conn.commit()
    finally:
        conn.close()


def get_pending_pos(species: str) -> list:
    """Return pending POs for a species, with customer profile location."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT po.po_number, po.profile_id, po.species,
                       po.share, po.note, po.order_date, po.deposit,
                       po.customer_preferences, po.status,
                       p.latitude AS cust_lat, p.longitude AS cust_lng,
                       p.first, p.last, p.company_name
                FROM purchase_orders po
                JOIN profiles p ON p.profile_id = po.profile_id
                WHERE po.species = %s
                  AND po.status = 'pending'
                ORDER BY po.order_date ASC
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_processor_cost(cost: dict):
    """Upsert a processor_costs row."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO processor_costs
                (profile_id, species, kill_fee, fab_cost_per_lb,
                 shrink_pct, daily_capacity_head, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (profile_id, species, effective_date) DO UPDATE SET
                    kill_fee = EXCLUDED.kill_fee,
                    fab_cost_per_lb = EXCLUDED.fab_cost_per_lb,
                    shrink_pct = EXCLUDED.shrink_pct,
                    daily_capacity_head = EXCLUDED.daily_capacity_head
            """, (
                cost['profile_id'], cost['species'],
                cost['kill_fee'], cost['fab_cost_per_lb'],
                cost['shrink_pct'],
                cost.get('daily_capacity_head'),
                cost.get('effective_date', datetime.now().date()),
            ))
        conn.commit()
    finally:
        conn.close()


def get_processor_costs(species: str) -> list:
    """Return processor costs for a species (most recent effective_date per processor)."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (pc.profile_id)
                       pc.profile_id, pc.species, pc.kill_fee,
                       pc.fab_cost_per_lb, pc.shrink_pct,
                       pc.daily_capacity_head, pc.effective_date,
                       p.company_name, p.latitude, p.longitude,
                       p.address, p.active
                FROM processor_costs pc
                JOIN profiles p ON p.profile_id = pc.profile_id
                WHERE pc.species = %s
                  AND p.type = 'processor'
                  AND p.active = TRUE
                  AND pc.effective_date <= CURRENT_DATE
                ORDER BY pc.profile_id, pc.effective_date DESC
            """, (species,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def save_slaughter_order(order: dict, allocations: list):
    """Insert a slaughter order + its PO allocations."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO slaughter_orders
                (order_number, animal_id, profile_id, species,
                 status, actual_hanging_weight)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                order['order_number'], order.get('animal_id'),
                order['profile_id'], order['species'],
                order.get('status', 'planned'),
                order.get('actual_hanging_weight'),
            ))

            for alloc in allocations:
                cur.execute("""
                    INSERT INTO slaughter_order_allocations
                    (slaughter_order_number, po_number, share)
                    VALUES (%s, %s, %s)
                """, (
                    order['order_number'],
                    alloc['po_number'], alloc['share'],
                ))
        conn.commit()
    finally:
        conn.close()


def get_weekly_price(species: str, grade: str) -> Optional[float]:
    """Return most recent weekly price per lb for species+grade."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price_per_lb FROM weekly_pricing
                WHERE species = %s AND grade = %s
                  AND effective_date <= CURRENT_DATE
                ORDER BY effective_date DESC LIMIT 1
            """, (species, grade))
            row = cur.fetchone()
            return float(row[0]) if row else None
    finally:
        conn.close()


def get_share_adjustment(share: str) -> Optional[float]:
    """Return most recent share adjustment percentage."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT adjustment_pct FROM share_adjustments
                WHERE share = %s
                  AND effective_date <= CURRENT_DATE
                ORDER BY effective_date DESC LIMIT 1
            """, (share,))
            row = cur.fetchone()
            return float(row[0]) if row else None
    finally:
        conn.close()
