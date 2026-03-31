"""Populate the distance_matrix with all relevant farm↔processor and processor↔customer pairs."""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from distance import call_google_routes, _sort_pair

load_dotenv()

SUPABASE_POOLER = os.getenv('SUPABASE_POOLER',
    'postgresql://postgres.qspuiymdznftyalixzdl:46656Irish26%40@aws-0-us-west-2.pooler.supabase.com:5432/postgres')


def get_supabase_connection():
    return psycopg2.connect(SUPABASE_POOLER)


def get_profiles_by_type(conn, profile_type):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, type, first_name, company_name, latitude, longitude
            FROM profiles
            WHERE type = %s AND latitude IS NOT NULL AND longitude IS NOT NULL
        """, (profile_type,))
        return [dict(row) for row in cur.fetchall()]


def get_cached_pair(conn, id_a, id_b):
    origin, dest = _sort_pair(str(id_a), str(id_b))
    with conn.cursor() as cur:
        cur.execute("""
            SELECT distance_miles FROM distance_matrix
            WHERE origin_profile_id = %s AND destination_profile_id = %s
        """, (origin, dest))
        row = cur.fetchone()
        return row[0] if row else None


def cache_pair(conn, id_a, id_b, distance_miles, duration_minutes, source='google_routes'):
    origin, dest = _sort_pair(str(id_a), str(id_b))
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO distance_matrix
                (origin_profile_id, destination_profile_id, distance_miles, duration_minutes, route_source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (origin_profile_id, destination_profile_id)
            DO UPDATE SET distance_miles = EXCLUDED.distance_miles,
                          duration_minutes = EXCLUDED.duration_minutes,
                          route_source = EXCLUDED.route_source,
                          calculated_at = now()
        """, (origin, dest, distance_miles, duration_minutes, source))
    conn.commit()


def calculate_and_cache(conn, profile_a, profile_b):
    name_a = profile_a.get('company_name') or profile_a.get('first_name')
    name_b = profile_b.get('company_name') or profile_b.get('first_name')

    # Check cache
    existing = get_cached_pair(conn, profile_a['id'], profile_b['id'])
    if existing is not None:
        print(f"  CACHED: {name_a} ↔ {name_b} = {existing} mi")
        return existing

    # Call Google Routes
    result = call_google_routes(
        float(profile_a['latitude']), float(profile_a['longitude']),
        float(profile_b['latitude']), float(profile_b['longitude']),
    )

    if not result:
        print(f"  FAILED: {name_a} ↔ {name_b}")
        return None

    cache_pair(conn, profile_a['id'], profile_b['id'],
               result['distance_miles'], result['duration_minutes'])

    print(f"  NEW: {name_a} ↔ {name_b} = {result['distance_miles']} mi ({result['duration_minutes']} min)")
    return result['distance_miles']


def main():
    conn = get_supabase_connection()

    farmers = get_profiles_by_type(conn, 'farmer')
    processors = get_profiles_by_type(conn, 'processor')
    customers = get_profiles_by_type(conn, 'customer')

    print(f"Profiles: {len(farmers)} farmers, {len(processors)} processors, {len(customers)} customers")

    # Farm ↔ Processor distances
    print(f"\n--- Farmer ↔ Processor ({len(farmers) * len(processors)} pairs) ---")
    for farm in farmers:
        for proc in processors:
            calculate_and_cache(conn, farm, proc)

    # Processor ↔ Customer distances
    print(f"\n--- Processor ↔ Customer ({len(processors) * len(customers)} pairs) ---")
    for proc in processors:
        for cust in customers:
            calculate_and_cache(conn, proc, cust)

    # Count total
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM distance_matrix")
        total = cur.fetchone()[0]

    print(f"\nDone. {total} distances in matrix.")
    conn.close()


if __name__ == '__main__':
    main()
