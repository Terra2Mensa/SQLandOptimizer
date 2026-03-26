"""Google Routes API integration + distance_matrix caching."""

import json
import os
import urllib.request
import urllib.error
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

GOOGLE_ROUTES_API_KEY = os.getenv('GOOGLE_ROUTES_API_KEY')
ROUTES_API_URL = 'https://routes.googleapis.com/directions/v2:computeRoutes'

METERS_TO_MILES = 0.000621371
SECONDS_TO_MINUTES = 1 / 60


def _get_connection():
    """Get a local DB connection."""
    return psycopg2.connect(
        dbname='terra_mensa',
        user=os.getenv('DB_USER', 'spolisini'),
        host='localhost',
    )


def _sort_pair(profile_id_a: str, profile_id_b: str) -> tuple:
    """Return (smaller_uuid, larger_uuid) for consistent storage."""
    if profile_id_a < profile_id_b:
        return profile_id_a, profile_id_b
    return profile_id_b, profile_id_a


def call_google_routes(origin_lat: float, origin_lng: float,
                       dest_lat: float, dest_lng: float) -> Optional[dict]:
    """Call Google Routes API for driving distance and duration.

    Returns: {'distance_miles': float, 'duration_minutes': float} or None on failure.
    """
    if not GOOGLE_ROUTES_API_KEY:
        print('WARNING: GOOGLE_ROUTES_API_KEY not set')
        return None

    body = {
        'origin': {
            'location': {
                'latLng': {'latitude': origin_lat, 'longitude': origin_lng}
            }
        },
        'destination': {
            'location': {
                'latLng': {'latitude': dest_lat, 'longitude': dest_lng}
            }
        },
        'travelMode': 'DRIVE',
        'routingPreference': 'TRAFFIC_UNAWARE',
    }

    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_ROUTES_API_KEY,
        'X-Goog-FieldMask': 'routes.distanceMeters,routes.duration',
    }

    req = urllib.request.Request(
        ROUTES_API_URL,
        data=json.dumps(body).encode(),
        headers=headers,
        method='POST',
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f'Google Routes API error: {e.code} {e.read().decode()}')
        return None
    except Exception as e:
        print(f'Google Routes API request failed: {e}')
        return None

    routes = data.get('routes', [])
    if not routes:
        print('Google Routes API returned no routes')
        return None

    route = routes[0]
    distance_meters = route.get('distanceMeters', 0)
    duration_str = route.get('duration', '0s')

    # Duration comes as "1234s" string
    duration_seconds = int(duration_str.rstrip('s')) if duration_str.endswith('s') else 0

    return {
        'distance_miles': round(distance_meters * METERS_TO_MILES, 2),
        'duration_minutes': round(duration_seconds * SECONDS_TO_MINUTES, 1),
    }


def get_cached_distance(profile_id_a: str, profile_id_b: str) -> Optional[dict]:
    """Look up cached distance from distance_matrix. Returns dict or None."""
    origin, dest = _sort_pair(profile_id_a, profile_id_b)
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT distance_miles, duration_minutes, route_source, calculated_at
                FROM distance_matrix
                WHERE origin_profile_id = %s AND destination_profile_id = %s
            """, (origin, dest))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def cache_distance(profile_id_a: str, profile_id_b: str,
                   distance_miles: float, duration_minutes: float = None,
                   route_source: str = 'google_routes'):
    """Store a distance in the matrix. Upserts on conflict."""
    origin, dest = _sort_pair(profile_id_a, profile_id_b)
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO distance_matrix
                    (origin_profile_id, destination_profile_id, distance_miles, duration_minutes, route_source)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (origin_profile_id, destination_profile_id)
                DO UPDATE SET
                    distance_miles = EXCLUDED.distance_miles,
                    duration_minutes = EXCLUDED.duration_minutes,
                    route_source = EXCLUDED.route_source,
                    calculated_at = now()
            """, (origin, dest, distance_miles, duration_minutes, route_source))
        conn.commit()
    finally:
        conn.close()


def get_or_calculate_distance(profile_id_a: str, profile_id_b: str,
                              lat_a: float, lng_a: float,
                              lat_b: float, lng_b: float) -> Optional[dict]:
    """Get distance from cache, or calculate via Google Routes and cache it.

    Returns: {'distance_miles': float, 'duration_minutes': float} or None.
    """
    # Check cache first
    cached = get_cached_distance(profile_id_a, profile_id_b)
    if cached:
        return cached

    # Calculate via API
    result = call_google_routes(lat_a, lng_a, lat_b, lng_b)
    if not result:
        return None

    # Cache it
    cache_distance(
        profile_id_a, profile_id_b,
        result['distance_miles'],
        result['duration_minutes'],
        'google_routes',
    )

    return result


# ─── CLI testing ─────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys

    if len(sys.argv) == 5:
        # Test with raw coordinates: python3 distance.py lat1 lng1 lat2 lng2
        lat1, lng1, lat2, lng2 = [float(x) for x in sys.argv[1:5]]
        print(f'Calculating driving distance...')
        result = call_google_routes(lat1, lng1, lat2, lng2)
        if result:
            print(f"  Distance: {result['distance_miles']} miles")
            print(f"  Duration: {result['duration_minutes']} minutes")
        else:
            print('  Failed to get route')
    else:
        print('Usage: python3 distance.py <lat1> <lng1> <lat2> <lng2>')
        print('Example: python3 distance.py 41.6764 -86.2520 41.7508 -86.0903')
        print('  (South Bend to Mishawaka)')
