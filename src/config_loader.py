"""Configuration loader with config.py defaults.

Loads processors from processor_costs + profiles tables,
regions and scalar parameters from config.py constants.
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
            SELECT DISTINCT ON (p.profile_id)
                   p.profile_id, p.company_name,
                   pc.kill_fee, pc.fab_cost_per_lb, pc.shrink_pct
            FROM processor_costs pc
            JOIN profiles p ON p.profile_id = pc.profile_id
            WHERE p.type = 'processor'
              AND p.active = TRUE
              AND pc.effective_date <= %s
            ORDER BY p.profile_id, pc.effective_date DESC
        """, (target,))
        rows = cur.fetchall()
        if not rows:
            return None
        result = {}
        for r in rows:
            result[r[0]] = {
                "name": r[1] or r[0],
                "kill_fee": float(r[2]),
                "fab_cost_per_lb": float(r[3]),
                "shrink_pct": float(r[4]),
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
    """Always returns config.py REGIONS dict (no DB table for regions)."""
    return dict(REGIONS)


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
    """Always returns config.py default (no DB table for parameters)."""
    if default is None:
        default = _PARAM_DEFAULTS.get(param_key, 0.0)
    return default


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
