"""Read optimizer_config table into a dict for use by the optimizer."""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

SUPABASE_POOLER = os.getenv('SUPABASE_POOLER',
    'postgresql://postgres.qspuiymdznftyalixzdl:46656Irish26%40@aws-0-us-west-2.pooler.supabase.com:5432/postgres')

LOCAL_DB = os.getenv('LOCAL_DB', 'terra_mensa')


def get_connection(use_supabase=False):
    if use_supabase:
        return psycopg2.connect(SUPABASE_POOLER)
    return psycopg2.connect(dbname=LOCAL_DB, user=os.getenv('DB_USER', 'spolisini'), host='localhost')


def load_optimizer_config(conn=None):
    """Load all optimizer_config rows into a dict: {key: value}."""
    close = False
    if conn is None:
        conn = get_connection(use_supabase=True)
        close = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT key, value FROM optimizer_config")
            return {row['key']: float(row['value']) for row in cur.fetchall()}
    finally:
        if close:
            conn.close()


# Convenience accessors
def get_config(config, key, default=0):
    return config.get(key, default)


# Constants
SHARE_FRACTIONS = {
    'whole': 1.0,
    'half': 0.5,
    'quarter': 0.25,
    'eighth': 0.125,
    'uncut': 1.0,
}

DRESS_PCT = {
    'cattle': 0.60,
    'pork': 0.72,
    'lamb': 0.50,
    'goat': 0.50,
}

SPECIES_LIST = ['cattle', 'pork', 'lamb', 'goat']
