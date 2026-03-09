"""DB-first configuration loader with config.py fallback.

Loads processors, regions, and scalar parameters from PostgreSQL
(effective-date versioned). Falls back to config.py constants when
the database is unavailable or tables are missing.
"""
from datetime import date
from typing import Optional

from config import (
    PROCESSORS, REGIONS,
    DEFAULT_BROKER_FEE_PCT, DEFAULT_BYPRODUCT_PCT,
    DEFAULT_BYPRODUCT_VALUE_PER_LB, DEFAULT_GRASSFED_PREMIUM_CWT,
    TRIM_YIELD_PCT,
)

_cache: dict = {}


def _try_db(query_fn):
    """Run *query_fn(cursor)* against the DB; return None on any failure."""
    try:
        from db import get_connection
        conn = get_connection()
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            return query_fn(cur)
    except Exception:
        return None
    finally:
        conn.close()


# ------------------------------------------------------------------
# Processors
# ------------------------------------------------------------------

def load_processors(as_of_date: Optional[date] = None) -> dict:
    cache_key = ("processors", as_of_date)
    if cache_key in _cache:
        return _cache[cache_key]

    target = as_of_date or date.today()

    def _query(cur):
        cur.execute("""
            SELECT DISTINCT ON (processor_key)
                   processor_key, name, kill_fee, fab_cost_per_lb,
                   shrink_pct, payment_terms_days
            FROM config_processors
            WHERE effective_date <= %s
            ORDER BY processor_key, effective_date DESC
        """, (target,))
        rows = cur.fetchall()
        if not rows:
            return None
        result = {}
        for r in rows:
            result[r[0]] = {
                "name": r[1],
                "kill_fee": float(r[2]),
                "fab_cost_per_lb": float(r[3]),
                "shrink_pct": float(r[4]),
                "payment_terms_days": int(r[5]),
            }
        return result

    db_result = _try_db(_query)
    result = db_result if db_result else dict(PROCESSORS)
    _cache[cache_key] = result
    return result


# ------------------------------------------------------------------
# Regions
# ------------------------------------------------------------------

def load_regions(as_of_date: Optional[date] = None) -> dict:
    cache_key = ("regions", as_of_date)
    if cache_key in _cache:
        return _cache[cache_key]

    target = as_of_date or date.today()

    def _query(cur):
        cur.execute("""
            SELECT DISTINCT ON (region_key)
                   region_key, label, city, state, pricing_adjustment
            FROM config_regions
            WHERE effective_date <= %s
            ORDER BY region_key, effective_date DESC
        """, (target,))
        rows = cur.fetchall()
        if not rows:
            return None
        result = {}
        for r in rows:
            result[r[0]] = {
                "label": r[1],
                "city": r[2] or "",
                "state": r[3] or "",
                "pricing_adjustment": float(r[4]),
            }
        return result

    db_result = _try_db(_query)
    result = db_result if db_result else dict(REGIONS)
    _cache[cache_key] = result
    return result


# ------------------------------------------------------------------
# Scalar parameters
# ------------------------------------------------------------------

_PARAM_DEFAULTS = {
    "broker_fee_pct": DEFAULT_BROKER_FEE_PCT,
    "byproduct_pct": DEFAULT_BYPRODUCT_PCT,
    "byproduct_value_per_lb": DEFAULT_BYPRODUCT_VALUE_PER_LB,
    "grassfed_premium_cwt": DEFAULT_GRASSFED_PREMIUM_CWT,
    "trim_yield_pct": TRIM_YIELD_PCT,
}


def load_param(param_key: str, default: float = None,
               as_of_date: Optional[date] = None) -> float:
    if default is None:
        default = _PARAM_DEFAULTS.get(param_key, 0.0)
    cache_key = ("param", param_key, as_of_date)
    if cache_key in _cache:
        return _cache[cache_key]

    target = as_of_date or date.today()

    def _query(cur):
        cur.execute("""
            SELECT param_value FROM config_parameters
            WHERE param_key = %s AND effective_date <= %s
            ORDER BY effective_date DESC LIMIT 1
        """, (param_key, target))
        row = cur.fetchone()
        return float(row[0]) if row else None

    db_val = _try_db(_query)
    result = db_val if db_val is not None else default
    _cache[cache_key] = result
    return result


def load_broker_fee_pct(as_of_date=None) -> float:
    return load_param("broker_fee_pct", DEFAULT_BROKER_FEE_PCT, as_of_date)


def load_byproduct_pct(as_of_date=None) -> float:
    return load_param("byproduct_pct", DEFAULT_BYPRODUCT_PCT, as_of_date)


def load_byproduct_value_per_lb(as_of_date=None) -> float:
    return load_param("byproduct_value_per_lb", DEFAULT_BYPRODUCT_VALUE_PER_LB, as_of_date)


def load_grassfed_premium_cwt(as_of_date=None) -> float:
    return load_param("grassfed_premium_cwt", DEFAULT_GRASSFED_PREMIUM_CWT, as_of_date)


def load_trim_yield_pct(as_of_date=None) -> float:
    return load_param("trim_yield_pct", TRIM_YIELD_PCT, as_of_date)


# ------------------------------------------------------------------
# Seed DB from config.py defaults
# ------------------------------------------------------------------

_PARAM_DESCRIPTIONS = {
    "broker_fee_pct": "Broker fee as decimal (e.g. 0.02 = 2%)",
    "byproduct_pct": "Byproduct weight as fraction of HCW",
    "byproduct_value_per_lb": "Byproduct value $/lb",
    "grassfed_premium_cwt": "Grassfed premium $/cwt over Choice",
    "trim_yield_pct": "Trim yield as fraction of HCW for ground beef",
}


def seed_config_to_db():
    """Populate config tables from config.py constants. Idempotent."""
    try:
        from db import get_connection
        conn = get_connection()
    except Exception as e:
        print(f"Cannot connect to DB: {e}")
        return

    try:
        with conn.cursor() as cur:
            # Processors
            for key, proc in PROCESSORS.items():
                cur.execute("""
                    INSERT INTO config_processors
                        (processor_key, name, kill_fee, fab_cost_per_lb,
                         shrink_pct, payment_terms_days, effective_date)
                    VALUES (%s, %s, %s, %s, %s, %s, '2020-01-01')
                    ON CONFLICT (processor_key, effective_date) DO NOTHING
                """, (key, proc["name"], proc["kill_fee"],
                      proc["fab_cost_per_lb"], proc["shrink_pct"],
                      proc["payment_terms_days"]))

            # Regions
            for key, reg in REGIONS.items():
                cur.execute("""
                    INSERT INTO config_regions
                        (region_key, label, city, state,
                         pricing_adjustment, effective_date)
                    VALUES (%s, %s, %s, %s, %s, '2020-01-01')
                    ON CONFLICT (region_key, effective_date) DO NOTHING
                """, (key, reg["label"], reg["city"], reg["state"],
                      reg["pricing_adjustment"]))

            # Scalar parameters
            for param_key, value in _PARAM_DEFAULTS.items():
                desc = _PARAM_DESCRIPTIONS.get(param_key, "")
                cur.execute("""
                    INSERT INTO config_parameters
                        (param_key, param_value, description, effective_date)
                    VALUES (%s, %s, %s, '2020-01-01')
                    ON CONFLICT (param_key, effective_date) DO NOTHING
                """, (param_key, value, desc))

        conn.commit()
        print("Config tables seeded from config.py defaults.")
    finally:
        conn.close()
