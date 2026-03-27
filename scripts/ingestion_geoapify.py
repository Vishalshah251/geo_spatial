"""
Geoapify Places API ingestion.

Always fetches live data from the Geoapify API.
No offline mode, no dummy fallbacks.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

from backend.csv_utils import save_pois_to_csv
from backend.database import save_pois
from backend.models import POI
from backend.utils import (
    cache_get_json,
    cache_set_json,
    env_int,
    env_str,
    logger,
    normalize_name,
    sha1_text,
)

GEOAPIFY_URL = "https://api.geoapify.com/v2/places"

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ✅ FIXED categories
CATEGORIES = [
    "catering.restaurant",
    "catering.cafe",
    "accommodation.hotel",
    "tourism.attraction",
    "commercial.supermarket",
    "commercial.shopping_mall",
    "healthcare.pharmacy",
    "service.vehicle.fuel",  # ✅ FIXED
]


# ---------------------------------------------------------------------------
# Grid generator
# ---------------------------------------------------------------------------

def _grid_points_sg(step_deg: float = 0.06) -> List[Tuple[float, float]]:
    min_lat, max_lat = 1.20, 1.47
    min_lon, max_lon = 103.60, 104.05

    pts: List[Tuple[float, float]] = []
    lat = min_lat
    while lat <= max_lat + 1e-9:
        lon = min_lon
        while lon <= max_lon + 1e-9:
            pts.append((round(lat, 5), round(lon, 5)))
            lon += step_deg
        lat += step_deg
    return pts


# ---------------------------------------------------------------------------
# API request
# ---------------------------------------------------------------------------

def _request_geoapify(
    api_key: str,
    category: str,
    lat: float,
    lon: float,
    radius_m: int,
    limit: int,
    offset: int,
    timeout_s: int,
) -> Dict:

    params = {
        "categories": category,  # ✅ single category
        "filter": f"circle:{lon},{lat},{radius_m}",
        "limit": limit,
        "offset": offset,
        "apiKey": api_key,
    }

    resp = requests.get(GEOAPIFY_URL, params=params, timeout=timeout_s)

    if resp.status_code == 401:
        raise ValueError("Invalid Geoapify API key")

    if resp.status_code != 200:
        raise RuntimeError(f"Geoapify HTTP {resp.status_code}: {resp.text[:200]}")

    return resp.json()


# ---------------------------------------------------------------------------
# MAIN FUNCTION
# ---------------------------------------------------------------------------

def fetch_geoapify_places(
    cache_ttl_s: int = 6 * 3600,
    grid_step_deg: float = 0.06,
    radius_m: int = 2500,
    per_request_delay_s: float = 1.0,
) -> List[POI]:

    api_key = env_str("GEOAPIFY_API_KEY", "")
    if not api_key or api_key == "PASTE_YOUR_KEY_HERE":
        raise ValueError("GEOAPIFY_API_KEY is missing")

    logger.info("Geoapify ingestion started")

    limit = env_int("GEOAPIFY_LIMIT", 50)
    max_pages = env_int("GEOAPIFY_MAX_PAGES", 3)
    request_timeout = env_int("GEOAPIFY_REQUEST_TIMEOUT_S", 15)

    # Cache key
    cache_key = sha1_text(f"geoapify:{CATEGORIES}:{grid_step_deg}:{radius_m}")
    cached = cache_get_json(cache_key, ttl_s=cache_ttl_s)

    if cached.hit and "pois" in cached.value:
        pois = [POI(**p) for p in cached.value["pois"]]
        save_pois(pois)
        csv_path = _DATA_DIR / "geoapify_data.csv"
        save_pois_to_csv(pois, csv_path)
        logger.info("Loaded from cache → %d POIs", len(pois))
        return pois

    # Live fetch
    points = _grid_points_sg(step_deg=grid_step_deg)
    logger.info("Scanning %d grid points", len(points))

    all_pois: List[POI] = []
    seen_ids = set()
    total_requests = 0

    # ✅ Per-category loop (robust)
    for category in CATEGORIES:
        logger.info(f"Processing category: {category}")

        for lat, lon in points:
            offset = 0

            for page in range(max_pages):
                try:
                    data = _request_geoapify(
                        api_key,
                        category,
                        lat,
                        lon,
                        radius_m,
                        limit,
                        offset,
                        request_timeout,
                    )
                    total_requests += 1

                except Exception as e:
                    logger.error(f"{category} failed at ({lat},{lon}): {e}")
                    break

                features = data.get("features") or []
                if not features:
                    break

                for feat in features:
                    props = feat.get("properties", {})

                    name = props.get("name")
                    plat = props.get("lat")
                    plon = props.get("lon")

                    if not name or plat is None or plon is None:
                        continue

                    place_id = props.get("place_id") or sha1_text(f"{name}:{plat}:{plon}")
                    pid = f"geoapify:{place_id}"

                    if pid in seen_ids:
                        continue
                    seen_ids.add(pid)

                    all_pois.append(
                        POI(
                            id=pid,
                            name=name,
                            lat=float(plat),
                            lon=float(plon),
                            category=category,
                            source="geoapify",
                            raw={
                                "properties": props,
                                "normalized_name": normalize_name(name),
                            },
                        )
                    )

                offset += limit
                time.sleep(per_request_delay_s)

    if not all_pois:
        raise RuntimeError("Geoapify ingestion failed: no data")

    # Save
    save_pois(all_pois)
    csv_path = _DATA_DIR / "geoapify_data.csv"
    save_pois_to_csv(all_pois, csv_path)

    cache_set_json(cache_key, {"pois": [p.model_dump() for p in all_pois]})

    logger.info(
        "Geoapify ingestion complete: %d POIs from %d requests",
        len(all_pois),
        total_requests,
    )

    return all_pois