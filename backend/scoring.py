"""
Change-detection scoring engine.

Given match results, classifies each POI as NEW / CLOSED / MODIFIED / UNCHANGED
and assigns a confidence score. Optionally enriched with review signals.

Review signal integration:
  - Recent reviews (< 30 days) → boost confidence for MODIFIED/UNCHANGED
  - Stale reviews (> 90 days) → boost confidence for CLOSED
  - Low rating (< 2.5)        → slight boost for MODIFIED
  - High activity (count > 50 + recent) → activity signal
  - No review data             → no adjustment
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from backend.models import ChangeResult, MatchCandidate, POI, POIStatus, Review
from backend.utils import clamp01, logger

# Import ML components
from backend.ml_features import extract_features_batch
from backend.ml_model import is_available as ml_is_available
from backend.ml_model import predict_batch as ml_predict_batch


def _confidence_from_match(m: MatchCandidate) -> float:
    """Base confidence from spatial + name match quality."""
    dist_score = clamp01(1.0 - (m.distance_m / 100.0))
    name_score = clamp01(m.name_similarity)
    return clamp01(0.55 * name_score + 0.45 * dist_score)


def _review_adjustment(review: Optional[Review], status: POIStatus) -> float:
    """
    Calculate confidence adjustment based on review signals.

    Returns a value in [-0.10, +0.15] that gets added to the base confidence.
    """
    if review is None or review.review_count == 0:
        return 0.0

    adj = 0.0

    # Days since last review
    try:
        last_date = datetime.strptime(review.last_review_date, "%Y-%m-%d")
        days_since = (datetime.now() - last_date).days
    except (ValueError, TypeError):
        days_since = None

    if days_since is not None:
        if status == POIStatus.CLOSED:
            # No recent reviews supports closure hypothesis
            if days_since > 90:
                adj += 0.12
            elif days_since < 30:
                # Recent reviews contradict closure → reduce confidence
                adj -= 0.10
        elif status in (POIStatus.MODIFIED, POIStatus.UNCHANGED):
            # Recent reviews confirm the place is active
            if days_since < 30:
                adj += 0.10
            elif days_since > 90:
                adj -= 0.05

    # Low rating → possible issue
    if review.rating < 2.5 and status == POIStatus.MODIFIED:
        adj += 0.05

    # High activity signal
    if review.review_count > 50 and days_since is not None and days_since < 30:
        if status in (POIStatus.UNCHANGED, POIStatus.MODIFIED):
            adj += 0.05

    return adj


def _enrich_result(result: ChangeResult, review: Optional[Review]) -> ChangeResult:
    """Attach review fields to a ChangeResult."""
    if review is None:
        return result
    result.review_rating = review.rating
    result.review_count = review.review_count
    result.last_review_date = review.last_review_date
    result.review_sentiment = review.sentiment
    return result


def classify_changes(
    osm_pois: List[POI],
    external_pois: List[POI],
    best_for_osm: Dict[str, Optional[MatchCandidate]],
    best_for_external: Dict[str, Optional[MatchCandidate]],
    reviews: Optional[Dict[str, Review]] = None,
) -> List[ChangeResult]:
    """
    Produce a sorted list of change-detection results,
    optionally enriched with review signals.

    Args:
        reviews: Dict of place_id → Review. If provided, review signals
                 adjust confidence scores and fields are attached to results.
    """
    if reviews is None:
        reviews = {}

    results: List[ChangeResult] = []
    
    # ── Attempt ML-based Scoring ──
    ml_used = False
    if ml_is_available():
        try:
            results = _ml_classify(
                osm_pois, external_pois, best_for_osm, best_for_external, reviews
            )
            ml_used = True
            logger.info("Successfully used ML-based scoring engine")
        except Exception as e:
            logger.warning("ML scoring failed: %s — falling back to rule-based scoring", e)

    # ── Fallback to Rule-based Scoring ──
    # If ML scoring succeeded, we return those results immediately.
    # Otherwise, we proceed with the existing rule-based logic.
    if ml_used:
        return results

    # OSM exists + no external match ⇒ potentially CLOSED
    for o in osm_pois:
        m = best_for_osm.get(o.id)
        if m is None:
            status = POIStatus.CLOSED
            base_conf = 0.70
            review = reviews.get(o.id)
            adj = _review_adjustment(review, status)
            conf = clamp01(base_conf + adj)

            result = ChangeResult(
                name=o.name,
                status=status,
                confidence=round(conf, 3),
                category=o.category,
                lat=o.lat,
                lon=o.lon,
                osm_id=o.id,
            )
            results.append(_enrich_result(result, review))
            continue

        # Both exist ⇒ check for modifications
        status = POIStatus.UNCHANGED
        if m.name_similarity < 0.90:
            status = POIStatus.MODIFIED
        elif (
            o.category
            and m.external.category
            and o.category.split(":")[0] != m.external.category.split(".")[0]
        ):
            status = POIStatus.MODIFIED

        base_conf = _confidence_from_match(m)
        review = reviews.get(o.id)
        adj = _review_adjustment(review, status)
        conf = clamp01(base_conf + adj)

        result = ChangeResult(
            name=o.name,
            status=status,
            confidence=round(conf, 3),
            category=o.category,
            lat=o.lat,
            lon=o.lon,
            osm_id=o.id,
            external_id=m.external.id,
            distance_m=m.distance_m,
            name_similarity=m.name_similarity,
        )
        results.append(_enrich_result(result, review))

    # External exists + no OSM match ⇒ NEW
    for e in external_pois:
        if best_for_external.get(e.id) is None:
            status = POIStatus.NEW
            base_conf = 0.65
            review = reviews.get(e.id)
            adj = _review_adjustment(review, status)
            conf = clamp01(base_conf + adj)

            result = ChangeResult(
                name=e.name,
                status=status,
                confidence=round(conf, 3),
                category=e.category,
                lat=e.lat,
                lon=e.lon,
                external_id=e.id,
            )
            results.append(_enrich_result(result, review))

    _STATUS_ORDER = {
        POIStatus.NEW: 0,
        POIStatus.CLOSED: 1,
        POIStatus.MODIFIED: 2,
        POIStatus.UNCHANGED: 3,
    }
    results.sort(key=lambda r: (_STATUS_ORDER.get(r.status, 9), -r.confidence, r.name.lower()))
    return results

def _ml_classify(
    osm_pois: List[POI],
    external_pois: List[POI],
    best_for_osm: Dict[str, Optional[MatchCandidate]],
    best_for_external: Dict[str, Optional[MatchCandidate]],
    reviews: Dict[str, Review],
) -> List[ChangeResult]:
    """Helper function to perform bulk scoring using the ML model."""
    results: List[ChangeResult] = []
    
    # We build parallel lists to pass to extract_features_batch
    batch_pois = []
    batch_matches = []
    batch_reviews = []
    batch_flags = []
    
    # OSM points
    for o in osm_pois:
        m = best_for_osm.get(o.id)
        batch_pois.append(o)
        batch_matches.append(m)
        batch_reviews.append(reviews.get(o.id))
        batch_flags.append(False)
        
    # External points without match
    for e in external_pois:
        if best_for_external.get(e.id) is None:
            batch_pois.append(e)
            batch_matches.append(None)
            batch_reviews.append(reviews.get(e.id))
            batch_flags.append(True)
            
    # Extract feature matrix
    import numpy as np
    feature_matrix = extract_features_batch(
        batch_pois, batch_matches, batch_reviews, batch_flags
    )
    
    # Run prediction
    predictions = ml_predict_batch(feature_matrix)
    
    # Reconstruct results
    for i, (poi, prediction) in enumerate(zip(batch_pois, predictions)):
        status_str, conf = prediction
        status = POIStatus(status_str)
        review = batch_reviews[i]
        match = batch_matches[i]
        
        # Determine internal/external IDs based on origin
        if not batch_flags[i]:
            # OSM origin
            result = ChangeResult(
                name=poi.name,
                status=status,
                confidence=conf,
                category=poi.category,
                lat=poi.lat,
                lon=poi.lon,
                osm_id=poi.id,
                external_id=match.external.id if match else None,
                distance_m=match.distance_m if match else None,
                name_similarity=match.name_similarity if match else None,
            )
        else:
            # External origin
            result = ChangeResult(
                name=poi.name,
                status=status,
                confidence=conf,
                category=poi.category,
                lat=poi.lat,
                lon=poi.lon,
                external_id=poi.id,
            )
            
        results.append(_enrich_result(result, review))

    return results
