from __future__ import annotations

from typing import Dict, List, Optional

from backend.models import ChangeResult, MatchCandidate, POI, POIStatus
from backend.utils import clamp01


def _confidence_from_match(m: MatchCandidate) -> float:
    dist_score = clamp01(1.0 - (m.distance_m / 100.0))
    name_score = clamp01(m.name_similarity)
    return clamp01(0.55 * name_score + 0.45 * dist_score)


def classify_changes(
    osm_pois: List[POI],
    external_pois: List[POI],
    best_for_osm: Dict[str, Optional[MatchCandidate]],
    best_for_external: Dict[str, Optional[MatchCandidate]],
    last_seen_days: Optional[int] = None,
    closure_days_threshold: int = 30,
) -> List[ChangeResult]:
    results: List[ChangeResult] = []

    # OSM exists + no external => CLOSED
    for o in osm_pois:
        m = best_for_osm.get(o.id)
        if m is None:
            conf = 0.70
            if last_seen_days is not None:
                conf = 0.85 if last_seen_days >= closure_days_threshold else 0.55
            results.append(
                ChangeResult(
                    name=o.name,
                    status=POIStatus.CLOSED,
                    confidence=conf,
                    category=o.category,
                    lat=o.lat,
                    lon=o.lon,
                    osm_id=o.id,
                )
            )
            continue

        # Both exist => UNCHANGED or MODIFIED
        status = POIStatus.UNCHANGED
        # Simple "modified" signal: name differs notably OR coarse category changes
        if m.name_similarity < 0.90:
            status = POIStatus.MODIFIED
        elif o.category and m.external.category and (o.category.split(":")[0] != m.external.category.split(".")[0]):
            status = POIStatus.MODIFIED

        results.append(
            ChangeResult(
                name=o.name,
                status=status,
                confidence=_confidence_from_match(m),
                category=o.category,
                lat=o.lat,
                lon=o.lon,
                osm_id=o.id,
                external_id=m.external.id,
                distance_m=m.distance_m,
                name_similarity=m.name_similarity,
            )
        )

    # No OSM + external exists => NEW
    for e in external_pois:
        if best_for_external.get(e.id) is None:
            results.append(
                ChangeResult(
                    name=e.name,
                    status=POIStatus.NEW,
                    confidence=0.65,
                    category=e.category,
                    lat=e.lat,
                    lon=e.lon,
                    external_id=e.id,
                )
            )

    order = {POIStatus.NEW: 0, POIStatus.CLOSED: 1, POIStatus.MODIFIED: 2, POIStatus.UNCHANGED: 3}
    results.sort(key=lambda r: (order.get(r.status, 9), -r.confidence, r.name.lower()))
    return results
