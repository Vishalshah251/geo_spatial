"""
Spatial matching engine — pairs OSM POIs with external POIs
using haversine distance and fuzzy name similarity.

Uses H3 hexagonal indexing for spatial pre-filtering to avoid
the O(n×m) brute-force comparison.
"""

from __future__ import annotations

import math
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

import h3

from backend.models import MatchCandidate, POI
from backend.utils import logger, normalize_name

# H3 resolution 9 ≈ ~175 m edge length — good for 100 m matching radius
_H3_RESOLUTION = 9


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two lat/lon points."""
    r = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def name_similarity(a: str, b: str) -> float:
    """Fuzzy similarity between two POI names (0–1)."""
    a2 = normalize_name(a)
    b2 = normalize_name(b)
    if not a2 or not b2:
        return 0.0
    if a2 == b2:
        return 1.0
    return SequenceMatcher(None, a2, b2).ratio()


def _build_h3_index(pois: List[POI]) -> Dict[str, List[POI]]:
    """Index POIs by their H3 cell (and k=1 neighbours)."""
    idx: Dict[str, List[POI]] = {}
    for p in pois:
        cell = h3.latlng_to_cell(p.lat, p.lon, _H3_RESOLUTION)
        # Include the cell itself and its immediate neighbours so that
        # POIs near cell boundaries are still discoverable.
        for c in h3.grid_disk(cell, 1):
            idx.setdefault(c, []).append(p)
    return idx


def match_pois(
    osm_pois: List[POI],
    external_pois: List[POI],
    max_distance_m: float = 100.0,
    min_name_similarity: float = 0.6,
) -> Tuple[List[MatchCandidate], Dict[str, Optional[MatchCandidate]], Dict[str, Optional[MatchCandidate]]]:
    """
    Greedy 1-to-1 matching with H3 spatial pre-filtering.

    For each OSM POI, pick the best external candidate within
    ``max_distance_m`` whose name similarity ≥ ``min_name_similarity``,
    prioritising name similarity then proximity.
    """

    ext_index = _build_h3_index(external_pois)
    used_external: set[str] = set()

    matches: List[MatchCandidate] = []
    best_for_osm: Dict[str, Optional[MatchCandidate]] = {p.id: None for p in osm_pois}
    best_for_external: Dict[str, Optional[MatchCandidate]] = {p.id: None for p in external_pois}

    for o in osm_pois:
        cell = h3.latlng_to_cell(o.lat, o.lon, _H3_RESOLUTION)
        # Only check external POIs in nearby H3 cells
        candidates = ext_index.get(cell, [])

        best: Optional[MatchCandidate] = None
        for e in candidates:
            if e.id in used_external:
                continue
            d = haversine_m(o.lat, o.lon, e.lat, e.lon)
            if d > max_distance_m:
                continue
            s = name_similarity(o.name, e.name)
            if s < min_name_similarity:
                continue
            cand = MatchCandidate(osm=o, external=e, distance_m=d, name_similarity=s)
            if best is None or (cand.name_similarity, -cand.distance_m) > (best.name_similarity, -best.distance_m):
                best = cand

        if best is not None:
            used_external.add(best.external.id)
            matches.append(best)
            best_for_osm[o.id] = best
            best_for_external[best.external.id] = best

    logger.info("Matching: %d OSM × %d external → %d pairs", len(osm_pois), len(external_pois), len(matches))
    return matches, best_for_osm, best_for_external
