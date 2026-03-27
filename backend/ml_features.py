"""
ML Feature Extraction for POI Change Detection.

Extracts 9 numeric features from match candidates, POI data, and review signals
for use with the trained Random Forest classifier.

Feature Vector (9 dimensions):
  [0] has_match           — 1 if OSM POI has an external match, else 0
  [1] name_similarity     — fuzzy name match score (0.0–1.0)
  [2] distance_m          — haversine distance in metres
  [3] category_match      — 1 if top-level categories align, else 0
  [4] review_rating       — star rating (0.0 if no review)
  [5] review_count         — number of reviews (0 if no review)
  [6] review_recency_days — days since last review (999 if unknown)
  [7] review_sentiment_score — -1 (negative), 0 (neutral/none), 1 (positive)
  [8] is_external_only    — 1 if external POI with no OSM match (NEW signal)
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np

from backend.models import MatchCandidate, POI, Review
from backend.utils import logger

# Feature names (must match training data column order)
FEATURE_NAMES = [
    "has_match",
    "name_similarity",
    "distance_m",
    "category_match",
    "review_rating",
    "review_count",
    "review_recency_days",
    "review_sentiment_score",
    "is_external_only",
]

NUM_FEATURES = len(FEATURE_NAMES)

# ── Category mapping ──
# Maps top-level OSM categories to Geoapify top-level categories for comparison.
_OSM_TO_GEO_CATEGORY = {
    "amenity:fast_food": "catering",
    "amenity:restaurant": "catering",
    "amenity:cafe": "catering",
    "amenity:bar": "catering",
    "amenity:fuel": "service",
    "amenity:pharmacy": "healthcare",
    "amenity:internet_cafe": "catering",
    "shop:supermarket": "commercial",
    "shop:mall": "commercial",
    "shop:department_store": "commercial",
    "tourism:attraction": "tourism",
    "tourism:hotel": "accommodation",
    "tourism:museum": "entertainment",
    "leisure:park": "leisure",
    "leisure:water_park": "leisure",
    "leisure:amusement_arcade": "entertainment",
    "leisure:dog_park": "leisure",
}


def _category_matches(osm_category: Optional[str], ext_category: Optional[str]) -> int:
    """Check if OSM and external categories refer to the same type of place."""
    if not osm_category or not ext_category:
        return 0

    osm_cat = osm_category.lower().strip()
    ext_cat = ext_category.lower().strip()

    # Direct mapping check
    mapped = _OSM_TO_GEO_CATEGORY.get(osm_cat)
    if mapped and ext_cat.startswith(mapped):
        return 1

    # Fallback: check if the main keywords overlap
    osm_parts = set(osm_cat.replace(":", " ").replace("_", " ").split())
    ext_parts = set(ext_cat.replace(".", " ").replace("_", " ").split())
    if osm_parts & ext_parts:
        return 1

    return 0


def _sentiment_to_score(sentiment: Optional[str]) -> int:
    """Convert sentiment string to numeric score."""
    if not sentiment:
        return 0
    s = sentiment.lower().strip()
    if s == "positive":
        return 1
    elif s == "negative":
        return -1
    return 0


def _review_recency_days(review: Optional[Review]) -> float:
    """Calculate days since last review. Returns 999 if unknown."""
    if review is None or not review.last_review_date:
        return 999.0
    try:
        last_date = datetime.strptime(review.last_review_date, "%Y-%m-%d")
        return max(0.0, (datetime.now() - last_date).days)
    except (ValueError, TypeError):
        return 999.0


def extract_features(
    poi: POI,
    match: Optional[MatchCandidate],
    review: Optional[Review],
    is_external_only: bool = False,
) -> np.ndarray:
    """
    Extract a feature vector for a single POI.

    Args:
        poi: The POI being classified.
        match: The best match candidate (None if unmatched).
        review: Review data for this POI (None if unavailable).
        is_external_only: True if this is an external POI with no OSM match.

    Returns:
        numpy array of shape (9,) with feature values.
    """
    features = np.zeros(NUM_FEATURES, dtype=np.float64)

    # [0] has_match
    features[0] = 1.0 if match is not None else 0.0

    # [1] name_similarity
    features[1] = match.name_similarity if match else 0.0

    # [2] distance_m
    features[2] = match.distance_m if match else 0.0

    # [3] category_match
    if match:
        features[3] = _category_matches(poi.category, match.external.category)
    else:
        features[3] = 0.0

    # [4] review_rating
    features[4] = review.rating if review and review.review_count > 0 else 0.0

    # [5] review_count
    features[5] = review.review_count if review else 0.0

    # [6] review_recency_days
    features[6] = _review_recency_days(review)

    # [7] review_sentiment_score
    features[7] = _sentiment_to_score(review.sentiment if review else None)

    # [8] is_external_only
    features[8] = 1.0 if is_external_only else 0.0

    return features


def extract_features_batch(
    pois: List[POI],
    matches: List[Optional[MatchCandidate]],
    reviews: List[Optional[Review]],
    external_only_flags: List[bool],
) -> np.ndarray:
    """
    Extract features for a batch of POIs.

    Returns numpy array of shape (n, 9).
    """
    n = len(pois)
    if n == 0:
        return np.empty((0, NUM_FEATURES), dtype=np.float64)

    matrix = np.zeros((n, NUM_FEATURES), dtype=np.float64)
    for i in range(n):
        matrix[i] = extract_features(
            pois[i],
            matches[i] if i < len(matches) else None,
            reviews[i] if i < len(reviews) else None,
            external_only_flags[i] if i < len(external_only_flags) else False,
        )
    return matrix
