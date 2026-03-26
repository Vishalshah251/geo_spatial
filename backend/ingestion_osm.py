from __future__ import annotations

import time
from typing import Dict, List

import requests

from backend.models import POI
from backend.utils import cache_get_json, cache_set_json, env_str, log, normalize_name, sha1_text


OVERPASS_URL = "https://overpass-api.de/api/interpreter"


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


def fetch_osm_pois(cache_ttl_s: int = 6 * 60 * 60) -> List[POI]:
    # Split by categories (Singapore area)
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

    cache_key = sha1_text("overpass:" + "|".join(k for k, _ in queries))
    cached = cache_get_json(cache_key, ttl_s=cache_ttl_s)
    if cached.hit and isinstance(cached.value, dict) and "pois" in cached.value:
        pois = [POI(**p) for p in cached.value["pois"]]
        log(f"OSM cache hit: {len(pois)}")
        return pois

    all_elements: List[Dict] = []
    for key, q in queries:
        log(f"Fetching OSM category: {key}")
        try:
            data = _post_overpass(q, retries=2, timeout_s=20)
        except Exception as e:
            log(f"OSM fetch failed ({key}): {e}")
            continue
        elements = data.get("elements", []) or []
        log(f"Fetched OSM elements: {len(elements)} ({key})")
        all_elements.extend(elements)
        time.sleep(1)

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
    cache_set_json(cache_key, {"pois": [p.model_dump() for p in pois]})
    return pois

