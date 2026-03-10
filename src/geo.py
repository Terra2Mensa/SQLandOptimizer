"""Geographic distance utilities for the carcass optimizer."""
from math import radians, cos, sin, asin, sqrt


def haversine(lat1, lon1, lat2, lon2):
    """Return distance in miles between two lat/lng points."""
    R = 3959  # Earth radius in miles
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))


def safe_distance(lat1, lon1, lat2, lon2, default=9999.0):
    """Haversine with None-safe fallback."""
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return default
    return haversine(float(lat1), float(lon1), float(lat2), float(lon2))
