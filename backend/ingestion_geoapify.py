from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

from backend.database import load_pois_by_source, save_pois
from backend.models import POI
from backend.utils import cache_get_json, cache_set_json, env_int, env_str, log, normalize_name, sha1_text


GEOAPIFY_URL = "https://api.geoapify.com/v2/places"

def _offline_enabled(offline: bool) -> bool:
    v = env_str("GEO_SENTINEL_OFFLINE", "0").strip().lower()
    return offline or v in ("1", "true", "yes", "y", "on")


def _seed_geoapify_from_file() -> List[POI]:
    """Load seed POIs from data/geoapify_sample.json, persist to DB, and return them."""
    sample_path = Path(__file__).resolve().parent.parent / "data" / "geoapify_sample.json"
    if not sample_path.exists():
        return []

    data = json.loads(sample_path.read_text(encoding="utf-8"))
    features = data.get("features", []) or []

    pois: List[POI] = []
    for f in features:
        props = f.get("properties", {}) or {}
        name = props.get("name")
        if not name:
            continue
        lat = props.get("lat")
        lon = props.get("lon")
        if lat is None or lon is None:
            continue
        category = (props.get("categories") or ["unknown"])[0]
        place_id = props.get("place_id") or sha1_text(f"{name}:{lat}:{lon}")
        pois.append(
            POI(
                id=f"geoapify:{place_id}",
                name=name,
                lat=float(lat),
                lon=float(lon),
                category=str(category),
                source="geoapify",
                raw={"properties": props, "normalized_name": normalize_name(name), "seed": True},
            )
        )

    if pois:
        save_pois(pois)
        log(f"Geoapify seed: persisted {len(pois)} POIs from data/geoapify_sample.json")
    return pois


def _load_geoapify_from_db() -> List[POI]:
    """Load Geoapify POIs from the database."""
    rows = load_pois_by_source("geoapify")
    return [POI(**r) for r in rows]


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
    offline: bool = False,
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
        save_pois(pois)
        log(f"Geoapify cache hit: {len(pois)} → DB")
        return pois

    # ── Offline / no API key: read from DB, seed from file if empty ──
    if _offline_enabled(offline) or not api_key:
        if _offline_enabled(offline):
            log("Geoapify offline mode; checking DB")
        else:
            log("GEOAPIFY_API_KEY not set; checking DB")
        db_pois = _load_geoapify_from_db()
        if db_pois:
            log(f"Geoapify loaded {len(db_pois)} POIs from DB")
            return db_pois
        pois = _seed_geoapify_from_file()
        log(f"Geoapify seeded {len(pois)} POIs from file → DB")
        return pois

    request_timeout_s = env_int("GEOAPIFY_REQUEST_TIMEOUT_S", 15)

    points = _grid_points_sg(step_deg=grid_step_deg)
    all_pois: List[POI] = []
    seen_ids: set[str] = set()

    log(f"Geoapify grid points: {len(points)}")
    for (lat, lon) in points:
        offset = 0
        for page in range(max_pages):
            try:
                data = _request_geoapify(
                    api_key=api_key,
                    categories=categories,
                    lat=lat,
                    lon=lon,
                    radius_m=radius_m,
                    limit=limit,
                    offset=offset,
                    timeout_s=request_timeout_s,
                )
            except Exception as e:
                log(f"Geoapify request failed at ({lat},{lon}) page={page+1}: {e}")
                break

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
    if not all_pois:
        db_pois = _load_geoapify_from_db()
        if db_pois:
            log(f"Geoapify fetched 0; loaded {len(db_pois)} from DB")
            return db_pois
        sample = _seed_geoapify_from_file()
        if sample:
            log(f"Geoapify fetched 0; seeded {len(sample)} from file → DB")
            cache_set_json(cache_key, {"pois": [p.model_dump() for p in sample]})
            return sample
        log("Geoapify fetched 0 POIs and no seed data; returning []")
        return []

    save_pois(all_pois)
    log(f"Geoapify persisted {len(all_pois)} POIs → DB")
    cache_set_json(cache_key, {"pois": [p.model_dump() for p in all_pois]})
    return all_pois
