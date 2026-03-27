"""
POI Real-World Validation Service.

Cross-references internal change-detection results with live Geoapify Places API
data to provide trust badges:
  - VALIDATED   ✅  Internal status matches external reality
  - CONFLICT    ⚠️  Internal status contradicts external data
  - NOT_VERIFIED ❌  Unable to verify (no data / API failure)

Caching: Results are cached in memory with a 1-hour TTL to minimise API usage.
"""

from __future__ import annotations

import os
import time
import threading
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import requests

from backend.data_loader import get_detection_for_poi, load_all_pois
from backend.models import ValidationBadge
from backend.utils import logger, normalize_name

# Try to use thefuzz for better matching, fall back to difflib
try:
    from thefuzz import fuzz as _fuzz

    def _fuzzy_ratio(a: str, b: str) -> float:
        """Fuzzy similarity 0–1 using thefuzz token_sort_ratio."""
        return _fuzz.token_sort_ratio(a, b) / 100.0
except ImportError:
    logger.info("thefuzz not installed — falling back to difflib for fuzzy matching")

    def _fuzzy_ratio(a: str, b: str) -> float:
        return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_GEOAPIFY_PLACES_URL = "https://api.geoapify.com/v2/places"
_SEARCH_RADIUS_M = 500          # metres around the POI centroid
_MIN_NAME_SIMILARITY = 0.65     # strict threshold to prevent mismatching completely different POIs
_CACHE_TTL_S = 3600             # 1 hour
_REQUEST_TIMEOUT_S = 10

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------

_cache_lock = threading.Lock()
_validation_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}  # key → (timestamp, result)


def _cache_key(name: str) -> str:
    return normalize_name(name)


def _get_cached(name: str) -> Optional[Dict[str, Any]]:
    key = _cache_key(name)
    with _cache_lock:
        entry = _validation_cache.get(key)
        if entry is None:
            return None
        ts, result = entry
        if time.time() - ts > _CACHE_TTL_S:
            del _validation_cache[key]
            return None
        return result


def _set_cached(name: str, result: Dict[str, Any]) -> None:
    key = _cache_key(name)
    with _cache_lock:
        _validation_cache[key] = (time.time(), result)


# ---------------------------------------------------------------------------
# Geoapify API interaction
# ---------------------------------------------------------------------------


def _get_api_key() -> str:
    return os.environ.get("GEOAPIFY_API_KEY", "")


def _map_category_to_geoapify(internal_category: str) -> Optional[str]:
    """
    Best-effort mapping from internal category strings to Geoapify categories.

    Returns None if no clear mapping exists (the name filter alone is usually enough).
    """
    cat = (internal_category or "").lower()
    mappings = {
        "fast_food": "catering.fast_food",
        "restaurant": "catering.restaurant",
        "cafe": "catering.cafe",
        "bar": "catering.bar",
        "pub": "catering.pub",
        "bank": "service.financial.bank",
        "pharmacy": "commercial.health_and_beauty.pharmacy",
        "supermarket": "commercial.supermarket",
        "hotel": "accommodation.hotel",
        "hospital": "healthcare.hospital",
        "school": "education.school",
        "fuel": "service.vehicle.fuel",
        "atm": "service.financial.atm",
    }
    for key, val in mappings.items():
        if key in cat:
            return val
    return None


def _fetch_geoapify_places(
    name: str,
    lat: float,
    lon: float,
    radius_m: float = _SEARCH_RADIUS_M,
    category: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Search Geoapify Places API for places matching `name` near (lat, lon).

    Returns a list of GeoJSON feature dicts, or [] on failure.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.warning("GEOAPIFY_API_KEY not set — cannot validate POI externally")
        return []

    params: Dict[str, Any] = {
        "apiKey": api_key,
        "filter": f"circle:{lon},{lat},{int(radius_m)}",
        "bias": f"proximity:{lon},{lat}",
        "limit": 5,
        "lang": "en",
        "name": name,
    }

    # Add category filter if we have a mapping
    geo_cat = _map_category_to_geoapify(category) if category else None
    if geo_cat:
        params["categories"] = geo_cat

    try:
        resp = requests.get(
            _GEOAPIFY_PLACES_URL,
            params=params,
            timeout=_REQUEST_TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        logger.info(
            "Geoapify search '%s' near (%.4f, %.4f): %d features returned",
            name, lat, lon, len(features),
        )
        return features
    except requests.exceptions.Timeout:
        logger.warning("Geoapify API timeout for '%s'", name)
        return []
    except requests.exceptions.RequestException as exc:
        logger.warning("Geoapify API error for '%s': %s", name, exc)
        return []
    except Exception as exc:
        logger.error("Unexpected error calling Geoapify for '%s': %s", name, exc)
        return []


def _retry_without_category(
    name: str, lat: float, lon: float, radius_m: float = _SEARCH_RADIUS_M
) -> List[Dict[str, Any]]:
    """
    Retry the Geoapify search without a category filter for broader results.
    """
    return _fetch_geoapify_places(name, lat, lon, radius_m, category=None)


# ---------------------------------------------------------------------------
# Matching & comparison
# ---------------------------------------------------------------------------

import math


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    r = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _pick_best_match(
    name: str, lat: float, lon: float, features: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Pick the best matching feature from Geoapify results using fuzzy name + proximity.

    Returns a dict with external place details, or None if nothing matches.
    """
    if not features:
        return None

    best: Optional[Dict[str, Any]] = None
    best_score = 0.0

    for feat in features:
        props = feat.get("properties", {})
        ext_name = props.get("name") or props.get("address_line1") or ""
        if not ext_name:
            continue

        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [0, 0])
        ext_lon, ext_lat = coords[0], coords[1]

        sim = _fuzzy_ratio(name, ext_name)
        dist = _haversine_m(lat, lon, ext_lat, ext_lon)

        # Distance penalty: rapid drop-off for distances beyond 50m
        if dist <= 20:
            dist_score = 1.0
        elif dist <= 100:
            # Linear decay from 1.0 at 20m down to ~0.60 at 100m
            dist_score = 1.0 - ((dist - 20) / 200.0)
        else:
            # Steep exponential decay after 100m
            dist_score = math.exp(-((dist - 100) / 100.0))

        # We heavily weight name similarity (0.6) and distance (0.4)
        combined = 0.6 * sim + 0.4 * dist_score

        # Explicit confidence penalty if name is very different
        if sim < 0.70:
            combined *= 0.70

        if sim >= _MIN_NAME_SIMILARITY and combined > best_score:
            best_score = combined
            best = {
                "name": ext_name,
                "lat": ext_lat,
                "lon": ext_lon,
                "category": props.get("categories", ["unknown"])[0] if props.get("categories") else "unknown",
                "distance_m": round(dist, 1),
                "name_similarity": round(sim, 3),
                "combined_score": round(combined, 3),
                "match_confidence": round(combined, 3),
                "place_id": props.get("place_id", ""),
                "address": props.get("formatted", ""),
            }

    return best


def _compare_statuses(
    internal_status: Optional[str], external_found: bool
) -> Tuple[str, ValidationBadge]:
    """
    Compare internal detection status with external existence.

    Returns (external_status_label, badge).
    """
    if internal_status is None:
        return ("UNKNOWN", ValidationBadge.NOT_VERIFIED)

    internal = internal_status.upper()

    if not external_found:
        # Place not found externally
        if internal == "CLOSED":
            return ("NOT_FOUND", ValidationBadge.VALIDATED)
        elif internal == "NEW":
            # New in our data but not found externally — can't validate
            return ("NOT_FOUND", ValidationBadge.NOT_VERIFIED)
        else:
            # UNCHANGED or MODIFIED but not found externally — conflict
            return ("NOT_FOUND", ValidationBadge.CONFLICT)

    # Place found externally → it exists / is operational
    if internal == "CLOSED":
        return ("OPERATIONAL", ValidationBadge.CONFLICT)
    elif internal in ("UNCHANGED", "NEW", "MODIFIED"):
        return ("OPERATIONAL", ValidationBadge.VALIDATED)
    else:
        return ("OPERATIONAL", ValidationBadge.VALIDATED)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_poi(name: str) -> Dict[str, Any]:
    """
    Validate a POI by cross-referencing internal data with live Geoapify API.

    Args:
        name: POI name to search and validate.

    Returns:
        Dict with keys:
          - query: original search name
          - internal: {name, status, confidence, lat, lon, category, ...} or None
          - external: {name, lat, lon, category, distance_m, name_similarity, ...} or None
          - validation: {internal_status, external_status, badge, source}
    """
    # Check cache first
    cached = _get_cached(name)
    if cached is not None:
        logger.info("Validation cache hit for '%s'", name)
        return cached

    # 1. Look up internal detection result
    internal_result = get_detection_for_poi(name=name)
    internal_status = None
    search_lat = 1.35  # Default to Singapore centre (where POI data is)
    search_lon = 103.82
    internal_category = None

    if internal_result:
        internal_status = internal_result.get("status")
        if internal_result.get("lat") is not None:
            search_lat = internal_result["lat"]
        if internal_result.get("lon") is not None:
            search_lon = internal_result["lon"]
        internal_category = internal_result.get("category")
    else:
        # Try finding the POI in our loaded data for coordinates
        all_pois = load_all_pois()
        name_lower = normalize_name(name)
        for p in all_pois:
            if normalize_name(p.name) == name_lower:
                search_lat = p.lat
                search_lon = p.lon
                internal_category = p.category
                break

    # 2. Fetch from Geoapify
    features = _fetch_geoapify_places(
        name=name,
        lat=search_lat,
        lon=search_lon,
        category=internal_category,
    )

    # If category-filtered search returned nothing, retry without category
    if not features and internal_category:
        features = _retry_without_category(name, search_lat, search_lon)

    # 3. Match best external result
    external_match = _pick_best_match(name, search_lat, search_lon, features)

    # 4. Compare statuses
    external_status, badge = _compare_statuses(internal_status, external_match is not None)

    # 5. Build result
    result: Dict[str, Any] = {
        "query": name,
        "internal": {
            "name": internal_result.get("name") if internal_result else None,
            "status": internal_status,
            "confidence": internal_result.get("confidence") if internal_result else None,
            "lat": internal_result.get("lat") if internal_result else None,
            "lon": internal_result.get("lon") if internal_result else None,
            "category": internal_result.get("category") if internal_result else None,
            "osm_id": internal_result.get("osm_id") if internal_result else None,
        } if internal_result else None,
        "external": external_match,
        "validation": {
            "internal_status": internal_status or "UNKNOWN",
            "external_status": external_status,
            "badge": badge.value,
            "source": "Geoapify (OpenStreetMap)",
        },
    }

    # Cache the result
    _set_cached(name, result)

    logger.info(
        "Validation for '%s': internal=%s, external=%s, badge=%s",
        name,
        internal_status or "N/A",
        external_status,
        badge.value,
    )

    return result


def validate_poi_batch(names: List[str]) -> List[Dict[str, Any]]:
    """
    Validate multiple POIs. Returns a list of validation results.
    """
    return [validate_poi(name) for name in names]
