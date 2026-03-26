from __future__ import annotations

import math
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

from backend.models import MatchCandidate, POI
from backend.utils import normalize_name


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def name_similarity(a: str, b: str) -> float:
    a2 = normalize_name(a)
    b2 = normalize_name(b)
    if not a2 or not b2:
        return 0.0
    if a2 == b2:
        return 1.0
    return SequenceMatcher(None, a2, b2).ratio()


def match_pois(
    osm_pois: List[POI],
    external_pois: List[POI],
    max_distance_m: float = 100.0,
    min_name_similarity: float = 0.6,
) -> Tuple[List[MatchCandidate], Dict[str, Optional[MatchCandidate]], Dict[str, Optional[MatchCandidate]]]:
    """
    Greedy matching:
    - For each OSM POI, pick best external candidate within distance, prioritizing name similarity then distance.
    - Ensure each external POI is matched at most once.
    """
    externals_by_idx = list(external_pois)
    used_external: set[str] = set()

    matches: List[MatchCandidate] = []
    best_for_osm: Dict[str, Optional[MatchCandidate]] = {p.id: None for p in osm_pois}
    best_for_external: Dict[str, Optional[MatchCandidate]] = {p.id: None for p in external_pois}

    for o in osm_pois:
        best: Optional[MatchCandidate] = None
        for e in externals_by_idx:
            if e.id in used_external:
                continue
            d = haversine_m(o.lat, o.lon, e.lat, e.lon)
            if d > max_distance_m:
                continue
            s = name_similarity(o.name, e.name)
            if s < min_name_similarity:
                continue
            cand = MatchCandidate(osm=o, external=e, distance_m=d, name_similarity=s)
            if best is None:
                best = cand
            else:
                if (cand.name_similarity, -cand.distance_m) > (best.name_similarity, -best.distance_m):
                    best = cand
        if best is not None:
            used_external.add(best.external.id)
            matches.append(best)
            best_for_osm[o.id] = best
            best_for_external[best.external.id] = best

    return matches, best_for_osm, best_for_external

