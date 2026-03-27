"""
CSV-based data loader with in-memory caching and indexed lookups.

This module is the single source of truth for the pipeline — all
downstream processing (matching, scoring, API responses) reads from
here instead of calling external APIs.

Data is loaded from:
  - data/osm_data.csv
  - data/geoapify_data.csv
  - data/reviews_data.csv

Caching strategy:
  - Parsed data is held in memory as a singleton.
  - Cache is invalidated when any CSV file's mtime changes.
  - Thread-safe via a module-level lock.
"""

from __future__ import annotations

import csv
import threading
from pathlib import Path
from typing import Dict, List, Optional

from backend.models import POI, Review
from backend.utils import logger

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ---------------------------------------------------------------------------
# Cache internals
# ---------------------------------------------------------------------------

_lock = threading.Lock()


class _DataCache:
    """In-memory cache for parsed CSV data with mtime-based invalidation."""

    def __init__(self) -> None:
        self.osm_pois: List[POI] = []
        self.geoapify_pois: List[POI] = []
        self.reviews: Dict[str, Review] = {}

        # Indexes for O(1) lookups
        self.poi_by_id: Dict[str, POI] = {}
        self.poi_by_name: Dict[str, List[POI]] = {}

        # Detection results cache (populated after detection runs)
        self.detection_results: List[Dict] = []
        self.detection_by_name: Dict[str, Dict] = {}  # lowercase name -> result dict
        self.detection_by_id: Dict[str, Dict] = {}    # osm_id or external_id -> result dict

        # File mtimes for cache invalidation
        self._mtimes: Dict[str, float] = {}
        self._loaded = False

    def _file_changed(self, path: Path) -> bool:
        """Check if a file's mtime differs from the cached value."""
        key = str(path)
        if not path.exists():
            return key in self._mtimes
        current = path.stat().st_mtime
        return self._mtimes.get(key) != current

    def _record_mtime(self, path: Path) -> None:
        if path.exists():
            self._mtimes[str(path)] = path.stat().st_mtime

    @property
    def stale(self) -> bool:
        if not self._loaded:
            return True
        for name in ("osm_data.csv", "geoapify_data.csv", "reviews_data.csv"):
            if self._file_changed(_DATA_DIR / name):
                return True
        return False

    def reload(self) -> None:
        """Parse all CSV files and rebuild indexes."""
        osm_path = _DATA_DIR / "osm_data.csv"
        geo_path = _DATA_DIR / "geoapify_data.csv"
        rev_path = _DATA_DIR / "reviews_data.csv"

        self.osm_pois = _parse_poi_csv(osm_path, "osm")
        self.geoapify_pois = _parse_poi_csv(geo_path, "geoapify")
        self.reviews = _parse_review_csv(rev_path)

        # Build indexes
        self.poi_by_id = {}
        self.poi_by_name = {}

        for poi in self.osm_pois + self.geoapify_pois:
            self.poi_by_id[poi.id] = poi
            key = poi.name.strip().lower()
            self.poi_by_name.setdefault(key, []).append(poi)

        # Record mtimes
        for p in (osm_path, geo_path, rev_path):
            self._record_mtime(p)
        self._loaded = True

        logger.info(
            "DataLoader: %d OSM + %d Geoapify POIs, %d reviews loaded",
            len(self.osm_pois),
            len(self.geoapify_pois),
            len(self.reviews),
        )


_cache = _DataCache()


# ---------------------------------------------------------------------------
# CSV parsers
# ---------------------------------------------------------------------------


def _parse_poi_csv(path: Path, default_source: str) -> List[POI]:
    """Parse a POI CSV file into a list of POI objects."""
    if not path.exists():
        logger.warning("POI CSV not found: %s", path)
        return []

    pois: List[POI] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                pois.append(
                    POI(
                        id=row["id"],
                        name=row["name"],
                        lat=float(row["lat"]),
                        lon=float(row["lon"]),
                        category=row.get("category", "other"),
                        source=row.get("source", default_source),
                    )
                )
            except Exception as exc:
                logger.warning("Skipping malformed POI row: %s", exc)

    logger.info("Parsed %d POIs from %s", len(pois), path.name)
    return pois


def _parse_review_csv(path: Path) -> Dict[str, Review]:
    """Parse the reviews CSV into a dict keyed by place_id."""
    if not path.exists():
        logger.warning("Reviews CSV not found: %s — scoring will run without review signals", path)
        return {}

    reviews: Dict[str, Review] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                r = Review(
                    place_id=row["place_id"],
                    name=row["name"],
                    rating=float(row["rating"]),
                    review_count=int(row["review_count"]),
                    last_review_date=row["last_review_date"],
                    sentiment=row.get("sentiment", "neutral"),
                )
                reviews[r.place_id] = r
            except Exception as exc:
                logger.warning("Skipping malformed review row: %s", exc)

    logger.info("Parsed %d reviews from %s", len(reviews), path.name)
    return reviews


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _ensure_loaded() -> None:
    """Reload data if the cache is stale."""
    with _lock:
        if _cache.stale:
            _cache.reload()


def load_osm_pois() -> List[POI]:
    """Return all OSM POIs (from data/osm_data.csv)."""
    _ensure_loaded()
    return _cache.osm_pois


def load_geoapify_pois() -> List[POI]:
    """Return all Geoapify POIs (from data/geoapify_data.csv)."""
    _ensure_loaded()
    return _cache.geoapify_pois


def load_all_pois() -> List[POI]:
    """Return combined OSM + Geoapify POIs."""
    _ensure_loaded()
    return _cache.osm_pois + _cache.geoapify_pois


def load_reviews() -> Dict[str, Review]:
    """Return reviews dict keyed by place_id."""
    _ensure_loaded()
    return _cache.reviews


def get_poi_by_id(poi_id: str) -> Optional[POI]:
    """O(1) lookup of a single POI by its ID."""
    _ensure_loaded()
    return _cache.poi_by_id.get(poi_id)


def get_review_for_poi(poi_id: str) -> Optional[Review]:
    """O(1) lookup of review data for a POI."""
    _ensure_loaded()
    return _cache.reviews.get(poi_id)


def get_data_stats() -> Dict[str, int]:
    """Return counts of loaded data for the dashboard."""
    _ensure_loaded()
    return {
        "osm_pois": len(_cache.osm_pois),
        "geoapify_pois": len(_cache.geoapify_pois),
        "total_pois": len(_cache.poi_by_id),
        "reviews": len(_cache.reviews),
    }


# ---------------------------------------------------------------------------
# Detection results cache
# ---------------------------------------------------------------------------


def cache_detection_results(results: List[Dict]) -> None:
    """
    Store detection results in memory and build lookup indexes.

    Called by services.detect_changes() / run_pipeline() after computing results.
    This is the single source of truth for POI status data.
    """
    with _lock:
        _cache.detection_results = results
        _cache.detection_by_name = {}
        _cache.detection_by_id = {}

        for r in results:
            # Index by lowercase name (keep first/highest-priority occurrence)
            key = (r.get("name") or "").strip().lower()
            if key and key not in _cache.detection_by_name:
                _cache.detection_by_name[key] = r

            # Index by osm_id and external_id for precise lookups
            osm_id = r.get("osm_id")
            ext_id = r.get("external_id")
            if osm_id and osm_id not in _cache.detection_by_id:
                _cache.detection_by_id[osm_id] = r
            if ext_id and ext_id not in _cache.detection_by_id:
                _cache.detection_by_id[ext_id] = r

        logger.info(
            "Detection cache: %d results, %d name keys, %d id keys",
            len(results),
            len(_cache.detection_by_name),
            len(_cache.detection_by_id),
        )


def get_cached_detection_results() -> List[Dict]:
    """Return all cached detection results (empty list if detection hasn't run)."""
    with _lock:
        return _cache.detection_results


def has_cached_detection() -> bool:
    """Check whether detection results have been cached."""
    with _lock:
        return len(_cache.detection_results) > 0


def get_detection_for_poi(poi_id: Optional[str] = None, name: Optional[str] = None) -> Optional[Dict]:
    """
    Look up cached detection result for a POI by ID or name.

    Returns the detection result dict (with status, confidence, etc.) or None.
    """
    with _lock:
        if poi_id and poi_id in _cache.detection_by_id:
            return _cache.detection_by_id[poi_id]
        if name:
            key = name.strip().lower()
            if key in _cache.detection_by_name:
                return _cache.detection_by_name[key]
        return None

