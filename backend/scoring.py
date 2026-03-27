"""
Change-detection scoring engine.

Given match results, classifies each POI as NEW / CLOSED / MODIFIED / UNCHANGED
and assigns a confidence score. Uses soft / probabilistic scoring:
  - Sigmoid-squashed base scores (no linear saturation)
  - Missing-data penalty (absent reviews, no match)
  - Conflict penalty (contradictory signals)
  - Source-availability scaling (fewer sources → lower ceiling)
  - Review adjustment (stale / recent / sentiment)

Design goals:
  - Realistic 0.30–0.90 spread instead of everything near 1.0
  - No single signal can dominate
  - Uncertainty is explicit
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Dict, List, Optional

from backend.models import ChangeResult, MatchCandidate, POI, POIStatus, Review
from backend.utils import clamp01, logger

# Import ML components
from backend.ml_features import extract_features_batch
from backend.ml_model import is_available as ml_is_available
from backend.ml_model import predict_batch as ml_predict_batch

# ───────────────────────────────────────────────────────────────────────
# Constants
# ───────────────────────────────────────────────────────────────────────
_MAX_CONFIDENCE = 0.95          # hard ceiling
_SIGMOID_TEMP = 4.0             # sigmoid steepness for base score squashing


# ───────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────

def _sigmoid(x: float, temp: float = _SIGMOID_TEMP) -> float:
    """Shifted sigmoid squashing centred at 0.5."""
    z = temp * (x - 0.5)
    return 1.0 / (1.0 + math.exp(-z))


def _days_since_review(review: Optional[Review]) -> Optional[int]:
    """Days since last review, or None if unknown."""
    if review is None or not review.last_review_date:
        return None
    try:
        last_date = datetime.strptime(review.last_review_date, "%Y-%m-%d")
        return max(0, (datetime.now() - last_date).days)
    except (ValueError, TypeError):
        return None


# ───────────────────────────────────────────────────────────────────────
# Confidence components
# ───────────────────────────────────────────────────────────────────────

def _confidence_from_match(m: MatchCandidate) -> float:
    """
    Base confidence from spatial + name match quality.

    Uses sigmoid squashing so even near-perfect matches
    cap around 0.80–0.85 instead of saturating at 1.0.
    """
    dist_score = clamp01(1.0 - (m.distance_m / 100.0))
    name_score = clamp01(m.name_similarity)
    raw = 0.55 * name_score + 0.45 * dist_score
    return _sigmoid(raw)  # squash into a softer range


def _missing_data_penalty(
    match: Optional[MatchCandidate],
    review: Optional[Review],
) -> float:
    """
    Penalty for absent data sources.

    No review data   → -0.10  (we lack corroboration)
    No match at all  → -0.05  (purely single-source)
    Both missing     → -0.15
    """
    penalty = 0.0
    if review is None or review.review_count == 0:
        penalty -= 0.10
    if match is None:
        penalty -= 0.05
    return penalty


def _conflict_penalty(
    match: Optional[MatchCandidate],
    review: Optional[Review],
    status: POIStatus,
) -> float:
    """
    Penalty when signals contradict each other.

    Examples:
      - CLOSED but recent reviews          → conflict
      - High name similarity but far away   → spatial mismatch
      - UNCHANGED but low rating            → possible undetected issue
      - MODIFIED but reviews are positive   → weak conflict
    """
    penalty = 0.0
    days = _days_since_review(review)

    # Status vs. review recency conflict
    if status == POIStatus.CLOSED and days is not None and days < 30:
        penalty -= 0.15  # strong conflict: CLOSED yet actively reviewed
    if status in (POIStatus.UNCHANGED, POIStatus.MODIFIED) and days is not None and days > 180:
        penalty -= 0.08  # stale reviews weaken confidence in active status

    # Spatial vs. name mismatch
    if match is not None:
        high_name = match.name_similarity >= 0.85
        far_away = match.distance_m > 60
        if high_name and far_away:
            penalty -= 0.10  # names match but location is off
        low_name = match.name_similarity < 0.60
        close_by = match.distance_m < 20
        if low_name and close_by:
            penalty -= 0.08  # close but names don't match well

    # Status vs. rating conflict
    if review and review.review_count > 0:
        if status == POIStatus.UNCHANGED and review.rating < 2.0:
            penalty -= 0.07  # "unchanged" but poorly rated

    return penalty


def _source_availability_factor(
    match: Optional[MatchCandidate],
    review: Optional[Review],
) -> float:
    """
    Multiplicative scaling based on how many independent sources confirm.

    3 sources (match + review + spatial) → 1.00
    2 sources                            → 0.85
    1 source only                        → 0.70
    """
    sources = 0
    if match is not None:
        sources += 1                    # spatial / name evidence
        if match.name_similarity > 0.5:
            sources += 1                # name corroboration
    if review is not None and review.review_count > 0:
        sources += 1                    # review evidence

    if sources >= 3:
        return 1.00
    elif sources == 2:
        return 0.85
    elif sources == 1:
        return 0.70
    return 0.60  # no corroboration at all


def _review_adjustment(review: Optional[Review], status: POIStatus) -> float:
    """
    Confidence adjustment from review signals.

    Returns a value in [-0.15, +0.10] added to base confidence.
    """
    if review is None or review.review_count == 0:
        return 0.0

    adj = 0.0
    days = _days_since_review(review)

    if days is not None:
        if status == POIStatus.CLOSED:
            if days > 90:
                adj += 0.08   # no recent activity supports closure
            # (recent-reviews-vs-closed conflict handled by _conflict_penalty)
        elif status in (POIStatus.MODIFIED, POIStatus.UNCHANGED):
            if days < 30:
                adj += 0.06   # recent reviews confirm activity
            elif days > 90:
                adj -= 0.05   # stale reviews weaken evidence

    # Low rating → slight signal for MODIFIED
    if review.rating < 2.5 and status == POIStatus.MODIFIED:
        adj += 0.04

    # High activity confirms presence
    if review.review_count > 50 and days is not None and days < 30:
        if status in (POIStatus.UNCHANGED, POIStatus.MODIFIED):
            adj += 0.04

    return max(-0.15, min(0.10, adj))


# ───────────────────────────────────────────────────────────────────────
# Confidence assembly
# ───────────────────────────────────────────────────────────────────────

def _assemble_confidence(
    base: float,
    match: Optional[MatchCandidate],
    review: Optional[Review],
    status: POIStatus,
) -> float:
    """
    Combine all confidence signals into a final score.

      final = (base + review_adj + conflict_penalty + missing_penalty)
              × source_factor
              capped at _MAX_CONFIDENCE
    """
    adj = _review_adjustment(review, status)
    conflict = _conflict_penalty(match, review, status)
    missing = _missing_data_penalty(match, review)
    factor = _source_availability_factor(match, review)

    raw = (base + adj + conflict + missing) * factor
    return round(clamp01(min(raw, _MAX_CONFIDENCE)), 3)


# ───────────────────────────────────────────────────────────────────────
# Result enrichment
# ───────────────────────────────────────────────────────────────────────

def _enrich_result(result: ChangeResult, review: Optional[Review]) -> ChangeResult:
    """Attach review fields to a ChangeResult."""
    if review is None:
        result.review_rating = 0.0
        result.review_count = 0
        return result
    result.review_rating = review.rating
    result.review_count = review.review_count
    result.last_review_date = review.last_review_date
    result.review_sentiment = review.sentiment
    return result


# ───────────────────────────────────────────────────────────────────────
# Main classification entry point
# ───────────────────────────────────────────────────────────────────────

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

    if ml_used:
        return results

    # ── Rule-based Scoring ──

    # OSM exists + no external match ⇒ potentially CLOSED
    for o in osm_pois:
        m = best_for_osm.get(o.id)
        if m is None:
            status = POIStatus.CLOSED
            base_conf = _sigmoid(0.55)   # moderate base, not hardcoded 0.70
            review = reviews.get(o.id)
            conf = _assemble_confidence(base_conf, None, review, status)

            result = ChangeResult(
                name=o.name,
                status=status,
                confidence=conf,
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
        conf = _assemble_confidence(base_conf, m, review, status)

        result = ChangeResult(
            name=o.name,
            status=status,
            confidence=conf,
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
            base_conf = _sigmoid(0.50)   # moderate base for new POIs
            review = reviews.get(e.id)
            conf = _assemble_confidence(base_conf, None, review, status)

            result = ChangeResult(
                name=e.name,
                status=status,
                confidence=conf,
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


# ───────────────────────────────────────────────────────────────────────
# ML-based scoring
# ───────────────────────────────────────────────────────────────────────

def _ml_classify(
    osm_pois: List[POI],
    external_pois: List[POI],
    best_for_osm: Dict[str, Optional[MatchCandidate]],
    best_for_external: Dict[str, Optional[MatchCandidate]],
    reviews: Dict[str, Review],
) -> List[ChangeResult]:
    """
    Bulk scoring using the ML model, with post-processing penalties
    for missing data and conflicting signals.
    """
    results: List[ChangeResult] = []

    # Build parallel lists for extract_features_batch
    batch_pois: List[POI] = []
    batch_matches: List[Optional[MatchCandidate]] = []
    batch_reviews: List[Optional[Review]] = []
    batch_flags: List[bool] = []

    for o in osm_pois:
        m = best_for_osm.get(o.id)
        batch_pois.append(o)
        batch_matches.append(m)
        batch_reviews.append(reviews.get(o.id))
        batch_flags.append(False)

    for e in external_pois:
        if best_for_external.get(e.id) is None:
            batch_pois.append(e)
            batch_matches.append(None)
            batch_reviews.append(reviews.get(e.id))
            batch_flags.append(True)

    import numpy as np
    feature_matrix = extract_features_batch(
        batch_pois, batch_matches, batch_reviews, batch_flags
    )

    # predict_batch already returns calibrated confidence (entropy + sigmoid)
    predictions = ml_predict_batch(feature_matrix)

    for i, (poi, prediction) in enumerate(zip(batch_pois, predictions)):
        status_str, ml_conf = prediction
        status = POIStatus(status_str)
        review = batch_reviews[i]
        match = batch_matches[i]

        # Post-process: layer penalties on top of ML calibrated score
        missing = _missing_data_penalty(match, review)
        conflict = _conflict_penalty(match, review, status)
        factor = _source_availability_factor(match, review)

        conf = clamp01(min((ml_conf + missing + conflict) * factor, _MAX_CONFIDENCE))
        conf = round(conf, 3)

        if not batch_flags[i]:
            result = ChangeResult(
                name=poi.name,
                status=status,
                confidence=conf,
                category=poi.category,
                lat=poi.lat,
                lon=poi.lon,
                osm_id=poi.id,
                external_id=match.external.id if match else None,
                distance_m=match.distance_m if match else 0.0,
                name_similarity=match.name_similarity if match else 0.0,
            )
        else:
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
