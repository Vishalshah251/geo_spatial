"""
OSM POI ingestion via the Overpass API.

Always fetches live data from the Overpass API. No offline mode,
no seed files, no dummy fallbacks.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List

import requests

from backend.csv_utils import save_pois_to_csv
from backend.database import save_pois
from backend.models import POI
from backend.utils import (
    cache_get_json,
    cache_set_json,
    env_int,
    logger,
    normalize_name,
    sha1_text,
)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Overpass queries scoped to Singapore
QUERIES = [
    ("restaurants_cafes", """
    [out:json][timeout:30];
    area["name"="Singapore"]->.a;
    (
      node["amenity"~"restaurant|cafe|fast_food"](area.a);
      way["amenity"~"restaurant|cafe|fast_food"](area.a);
    );
    out center;
    """),
    ("hotels", """
    [out:json][timeout:30];
    area["name"="Singapore"]->.a;
    (
      node["tourism"="hotel"](area.a);
      way["tourism"="hotel"](area.a);
    );
    out center;
    """),
    ("shops", """
    [out:json][timeout:30];
    area["name"="Singapore"]->.a;
    (
      node["shop"~"supermarket|mall|department_store"](area.a);
      way["shop"~"supermarket|mall|department_store"](area.a);
    );
    out center;
    """),
    ("pharmacies_fuel", """
    [out:json][timeout:30];
    area["name"="Singapore"]->.a;
    (
      node["amenity"="pharmacy"](area.a);
      node["amenity"="fuel"](area.a);
      way["amenity"="pharmacy"](area.a);
      way["amenity"="fuel"](area.a);
    );
    out center;
    """),
    ("tourism", """
    [out:json][timeout:30];
    area["name"="Singapore"]->.a;
    (
      node["tourism"~"attraction|museum"](area.a);
      way["tourism"~"attraction|museum"](area.a);
      node["leisure"~"park|amusement_arcade"](area.a);
      way["leisure"~"park|amusement_arcade"](area.a);
    );
    out center;
    """),
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_category(tags: Dict[str, str]) -> str:
    for key in ("amenity", "shop", "tourism", "leisure"):
        val = tags.get(key)
        if val:
            return f"{key}:{val}"
    return "other"


def _post_overpass(query: str, retries: int = 3, timeout_s: int = 60) -> dict:
    """Post a query to the Overpass API with retries and exponential backoff."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                OVERPASS_URL,
                data=query,
                headers={"User-Agent": "geo-sentinel/1.0"},
                timeout=timeout_s,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.warning("Overpass HTTP %d on attempt %d", resp.status_code, attempt)
        except Exception as exc:
            last_err = exc
            logger.warning("Overpass error on attempt %d: %s", attempt, exc)
        time.sleep(2 * attempt)

    if last_err:
        raise last_err
    raise RuntimeError("Overpass API failed after all retries")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_osm_pois(cache_ttl_s: int = 6 * 3600) -> List[POI]:
    """
    Fetch OSM POIs for Singapore from the live Overpass API.

    Flow:
      1. Check local cache (JSON, TTL-based)
      2. If miss → call Overpass API for each category
      3. Parse, deduplicate, persist to DB
      4. Export to ``data/osm_data.csv``

    Raises ``RuntimeError`` if the API returns no data after all retries.
    """

    logger.info("OSM ingestion started")

    overpass_retries = env_int("OSM_OVERPASS_RETRIES", 3)
    overpass_timeout = env_int("OSM_OVERPASS_TIMEOUT_S", 30)

    # ── Cache check ──
    cache_key = sha1_text("overpass:" + "|".join(k for k, _ in QUERIES))
    cached = cache_get_json(cache_key, ttl_s=cache_ttl_s)

    if cached.hit and isinstance(cached.value, dict) and "pois" in cached.value:
        pois = [POI(**p) for p in cached.value["pois"]]
        save_pois(pois)
        csv_path = _DATA_DIR / "osm_data.csv"
        save_pois_to_csv(pois, csv_path)
        logger.info("OSM cache hit: %d POIs → DB + %s", len(pois), csv_path.name)
        return pois

    # ── Fetch from Overpass ──
    all_elements: List[Dict] = []
    for key, query in QUERIES:
        logger.info("Fetching OSM category: %s", key)
        try:
            data = _post_overpass(query, retries=overpass_retries, timeout_s=overpass_timeout)
            elements = data.get("elements") or []
            all_elements.extend(elements)
            logger.info("OSM %s: received %d elements", key, len(elements))
        except Exception as exc:
            logger.error("OSM fetch failed (%s): %s", key, exc)
            continue

    # ── Parse & deduplicate ──
    seen: set[str] = set()
    pois: List[POI] = []

    for el in all_elements:
        el_type = str(el.get("type", "node"))
        stable_id = f"osm:{el_type}:{el.get('id')}"

        if stable_id in seen:
            continue
        seen.add(stable_id)

        tags = el.get("tags") or {}
        name = tags.get("name")
        lat = el.get("lat")
        lon = el.get("lon")

        if not name or lat is None or lon is None:
            continue

        pois.append(
            POI(
                id=stable_id,
                name=name,
                lat=float(lat),
                lon=float(lon),
                category=_extract_category(tags),
                source="osm",
                raw={"tags": tags, "normalized_name": normalize_name(name)},
            )
        )

    if not pois:
        raise RuntimeError(
            "OSM ingestion failed: Overpass API returned 0 valid POIs. "
            "Check network connectivity and Overpass API status."
        )

    # ── Persist to DB + CSV ──
    save_pois(pois)
    csv_path = _DATA_DIR / "osm_data.csv"
    save_pois_to_csv(pois, csv_path)

    # ── Update cache ──
    cache_set_json(cache_key, {"pois": [p.model_dump() for p in pois]})

    logger.info("OSM ingestion complete: %d POIs → DB + %s", len(pois), csv_path.name)
    return pois