from __future__ import annotations

import time
from typing import Dict, List, Tuple

import requests

from backend.models import POI
from backend.utils import cache_get_json, cache_set_json, env_int, env_str, log, normalize_name, sha1_text


GEOAPIFY_URL = "https://api.geoapify.com/v2/places"


def _grid_points_sg(step_deg: float = 0.06) -> List[Tuple[float, float]]:
    # Rough Singapore bbox
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


def _request_geoapify(
    api_key: str,
    categories: str,
    lat: float,
    lon: float,
    radius_m: int,
    limit: int,
    offset: int,
    timeout_s: int = 30,
) -> Dict:
    params = {
        "categories": categories,
        "filter": f"circle:{lon},{lat},{radius_m}",
        "limit": limit,
        "offset": offset,
        "apiKey": api_key,
    }
    r = requests.get(GEOAPIFY_URL, params=params, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"Geoapify non-200 {r.status_code}: {r.text[:200]}")
    return r.json()


def fetch_geoapify_places(
    cache_ttl_s: int = 6 * 60 * 60,
    grid_step_deg: float = 0.06,
    radius_m: int = 2500,
    per_request_delay_s: float = 1.0,
) -> List[POI]:
    api_key = env_str("GEOAPIFY_API_KEY", "")

    categories_list = [
        "catering.restaurant",
        "catering.cafe",
        "accommodation.hotel",
        "tourism.attraction",
        "commercial.supermarket",
        "commercial.shopping_mall",
        "healthcare.pharmacy",
        "service.fuel",
    ]
    categories = ",".join(categories_list)

    limit = env_int("GEOAPIFY_LIMIT", 50)
    max_pages = env_int("GEOAPIFY_MAX_PAGES", 3)

    cache_key = sha1_text(
        f"geoapify:{categories}:step={grid_step_deg}:r={radius_m}:limit={limit}:pages={max_pages}"
    )

    # Always prefer cached response if fresh enough
    cached = cache_get_json(cache_key, ttl_s=cache_ttl_s)
    if cached.hit and isinstance(cached.value, dict) and "pois" in cached.value:
        pois = [POI(**p) for p in cached.value["pois"]]
        log(f"Geoapify cache hit: {len(pois)}")
        return pois

    if not api_key:
        # No API key: only allow cached; no mock/sample data
        raise RuntimeError("GEOAPIFY_API_KEY not set and no cached Geoapify data available")

    points = _grid_points_sg(step_deg=grid_step_deg)
    all_pois: List[POI] = []
    seen_ids: set[str] = set()

    log(f"Geoapify grid points: {len(points)}")
    for (lat, lon) in points:
        offset = 0
        for page in range(max_pages):
            data = _request_geoapify(
                api_key=api_key,
                categories=categories,
                lat=lat,
                lon=lon,
                radius_m=radius_m,
                limit=limit,
                offset=offset,
            )

            features = data.get("features", []) or []
            if not features:
                break

            for f in features:
                props = f.get("properties", {}) or {}
                name = props.get("name")
                if not name:
                    continue
                plat = props.get("lat")
                plon = props.get("lon")
                if plat is None or plon is None:
                    continue

                place_id = props.get("place_id") or sha1_text(f"{name}:{plat}:{plon}")
                pid = f"geoapify:{place_id}"
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)

                cat = (props.get("categories") or ["unknown"])[0]
                all_pois.append(
                    POI(
                        id=pid,
                        name=name,
                        lat=float(plat),
                        lon=float(plon),
                        category=str(cat),
                        source="geoapify",
                        raw={"properties": props, "normalized_name": normalize_name(name)},
                    )
                )

            offset += limit
            time.sleep(per_request_delay_s)

    log(f"Geoapify POIs: {len(all_pois)}")
    cache_set_json(cache_key, {"pois": [p.model_dump() for p in all_pois]})
    return all_pois

