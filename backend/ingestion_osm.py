from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List

import requests

from backend.database import load_pois_by_source, save_pois
from backend.models import POI
from backend.utils import cache_get_json, cache_set_json, env_int, env_str, log, normalize_name, sha1_text


OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def _offline_enabled(offline: bool) -> bool:
    v = env_str("GEO_SENTINEL_OFFLINE", "0").strip().lower()
    return offline or v in ("1", "true", "yes", "y", "on")


def _seed_osm_from_file() -> List[POI]:
    """Load seed POIs from data/sample.json, persist to DB, and return them."""
    sample_path = Path(__file__).resolve().parent.parent / "data" / "sample.json"
    if not sample_path.exists():
        return []

    data = json.loads(sample_path.read_text(encoding="utf-8"))
    elements = data.get("elements", []) or []
    pois: List[POI] = []
    for el in elements:
        tags = el.get("tags", {}) or {}
        name = tags.get("name")
        if not name:
            continue
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue
        el_id = str(el.get("id"))
        el_type = str(el.get("type", "node"))
        pois.append(POI(
            id=f"osm:{el_type}:{el_id}",
            name=name,
            lat=float(lat),
            lon=float(lon),
            category=_extract_category(tags),
            source="osm",
            raw={"tags": tags, "normalized_name": normalize_name(name), "seed": True},
        ))

    if pois:
        save_pois(pois)
        log(f"OSM seed: persisted {len(pois)} POIs from data/sample.json")
    return pois


def _load_osm_from_db() -> List[POI]:
    """Load OSM POIs from the database."""
    rows = load_pois_by_source("osm")
    return [POI(**r) for r in rows]


def _extract_category(tags: Dict) -> str:
    for key in ("amenity", "shop", "tourism", "leisure"):
        v = tags.get(key)
        if v:
            return f"{key}:{v}"
    return "other"


def _post_overpass(query: str, retries: int = 3, timeout_s: int = 60) -> Dict:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(
                OVERPASS_URL,
                data=query,
                headers={"User-Agent": "geo-sentinel/1.0"},
                timeout=timeout_s,
            )
            if r.status_code == 200:
                return r.json()
            log(f"Overpass non-200 ({r.status_code}); attempt={attempt}")
        except Exception as e:
            last_err = e
            log(f"Overpass error; attempt={attempt}; err={e}")
        time.sleep(2 * attempt)
    if last_err:
        raise last_err
    raise RuntimeError("Overpass failed after retries")


def fetch_osm_pois(cache_ttl_s: int = 6 * 60 * 60, offline: bool = False) -> List[POI]:
    # ── Offline mode: read from DB, seed from file if DB empty ──
    if _offline_enabled(offline):
        db_pois = _load_osm_from_db()
        if db_pois:
            log(f"OSM offline mode; loaded {len(db_pois)} POIs from DB")
            return db_pois
        pois = _seed_osm_from_file()
        log(f"OSM offline mode; seeded {len(pois)} POIs from file → DB")
        return pois

    # ── Split by categories (Singapore area) ──
    queries = [
        ("restaurants_cafes", """
        [out:json][timeout:25];
        area["name"="Singapore"]->.a;
        (
          node["amenity"~"restaurant|cafe|fast_food"](area.a);
          way["amenity"~"restaurant|cafe|fast_food"](area.a);
        );
        out center;
        """),
        ("hotels", """
        [out:json][timeout:25];
        area["name"="Singapore"]->.a;
        (
          node["tourism"="hotel"](area.a);
          way["tourism"="hotel"](area.a);
        );
        out center;
        """),
        ("tourist_attractions", """
        [out:json][timeout:25];
        area["name"="Singapore"]->.a;
        (
          node["tourism"~"attraction|museum|zoo|theme_park"](area.a);
          way["tourism"~"attraction|museum|zoo|theme_park"](area.a);
        );
        out center;
        """),
        ("supermarkets_malls", """
        [out:json][timeout:25];
        area["name"="Singapore"]->.a;
        (
          node["shop"~"supermarket|mall|department_store"](area.a);
          way["shop"~"supermarket|mall|department_store"](area.a);
        );
        out center;
        """),
        ("pharmacies_fuel", """
        [out:json][timeout:25];
        area["name"="Singapore"]->.a;
        (
          node["amenity"~"pharmacy|fuel"](area.a);
          way["amenity"~"pharmacy|fuel"](area.a);
        );
        out center;
        """),
    ]

    overpass_retries = env_int("OSM_OVERPASS_RETRIES", 1)
    overpass_timeout_s = env_int("OSM_OVERPASS_TIMEOUT_S", 8)
    max_categories = env_int("OSM_MAX_CATEGORIES", 3)
    category_delay_s = env_int("OSM_CATEGORY_DELAY_S", 0)
    queries = queries[: max_categories] if max_categories > 0 else []

    cache_key = sha1_text("overpass:" + "|".join(k for k, _ in queries))
    cached = cache_get_json(cache_key, ttl_s=cache_ttl_s)
    if cached.hit and isinstance(cached.value, dict) and "pois" in cached.value:
        pois = [POI(**p) for p in cached.value["pois"]]
        save_pois(pois)
        log(f"OSM cache hit: {len(pois)} → DB")
        return pois

    if not queries:
        db_pois = _load_osm_from_db()
        if db_pois:
            log(f"OSM max categories=0; loaded {len(db_pois)} from DB")
            return db_pois
        pois = _seed_osm_from_file()
        log(f"OSM max categories=0; seeded {len(pois)} from file → DB")
        return pois

    all_elements: List[Dict] = []
    for key, q in queries:
        log(f"Fetching OSM category: {key}")
        try:
            data = _post_overpass(q, retries=overpass_retries, timeout_s=overpass_timeout_s)
        except Exception as e:
            log(f"OSM fetch failed ({key}): {e}")
            continue
        elements = data.get("elements", []) or []
        log(f"Fetched OSM elements: {len(elements)} ({key})")
        all_elements.extend(elements)
        time.sleep(category_delay_s)

    seen: set[str] = set()
    pois: List[POI] = []
    for el in all_elements:
        el_id = str(el.get("id"))
        el_type = str(el.get("type", "node"))
        stable_id = f"osm:{el_type}:{el_id}"
        if stable_id in seen:
            continue
        seen.add(stable_id)

        tags = el.get("tags", {}) or {}
        name = tags.get("name")
        if not name:
            continue

        if el_type == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")
        if lat is None or lon is None:
            continue

        poi = POI(
            id=stable_id,
            name=name,
            lat=float(lat),
            lon=float(lon),
            category=_extract_category(tags),
            source="osm",
            raw={"tags": tags, "normalized_name": normalize_name(name)},
        )
        pois.append(poi)

    log(f"OSM clean POIs: {len(pois)}")
    if not pois:
        db_pois = _load_osm_from_db()
        if db_pois:
            log(f"OSM fetched 0; loaded {len(db_pois)} from DB")
            return db_pois
        pois = _seed_osm_from_file()
        log(f"OSM fetched 0; seeded {len(pois)} from file → DB")
        cache_set_json(cache_key, {"pois": [p.model_dump() for p in pois]})
        return pois

    save_pois(pois)
    log(f"OSM persisted {len(pois)} POIs → DB")
    cache_set_json(cache_key, {"pois": [p.model_dump() for p in pois]})
    return pois
