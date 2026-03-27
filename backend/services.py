"""
Pipeline orchestration service.

All data is read from CSVs via the data_loader module.
No external API calls happen here.

Entry points:
  - ``run_pipeline()``       — full pipeline: load CSVs → match → score → persist
  - ``detect_changes()``     — match + score only (no DB persist)
  - ``get_dashboard_stats()``— aggregated statistics for the dashboard
"""

from __future__ import annotations

from typing import Any, Dict, List

from backend.data_loader import (
    cache_detection_results,
    get_data_stats,
    has_cached_detection,
    load_geoapify_pois,
    load_osm_pois,
    load_reviews,
)
from backend.database import save_pipeline_run, save_results, get_latest_run
from backend.matching import match_pois
from backend.ml_model import is_available as ml_is_available
from backend.scoring import classify_changes
from backend.utils import env_int, logger


def detect_changes() -> Dict[str, Any]:
    """
    Run change detection on CSV data without persisting to DB.

    Returns a summary with all change results.
    """
    logger.info("Change detection started (CSV-driven)")

    osm_pois = load_osm_pois()
    geoapify_pois = load_geoapify_pois()
    reviews = load_reviews()

    matches, best_for_osm, best_for_external = match_pois(
        osm_pois=osm_pois,
        external_pois=geoapify_pois,
        max_distance_m=100.0,
        min_name_similarity=0.60,
    )

    change_results = classify_changes(
        osm_pois=osm_pois,
        external_pois=geoapify_pois,
        best_for_osm=best_for_osm,
        best_for_external=best_for_external,
        reviews=reviews,
    )

    # Count by status
    status_counts: Dict[str, int] = {}
    for r in change_results:
        status_counts[r.status.value] = status_counts.get(r.status.value, 0) + 1

    logger.info(
        "Change detection complete: %d total, %s",
        len(change_results),
        status_counts,
    )

    result_dicts = [r.model_dump() for r in change_results]

    # Cache results in memory for search and initial load
    cache_detection_results(result_dicts)

    return {
        "total_osm_pois": len(osm_pois),
        "total_external_pois": len(geoapify_pois),
        "matched_pairs": len(matches),
        "total_results": len(change_results),
        "status_counts": status_counts,
        "results": result_dicts,
    }


def run_pipeline() -> Dict[str, Any]:
    """
    Execute the full change-detection pipeline from CSV data.

    1. Load OSM + Geoapify POIs from CSVs
    2. Load reviews from CSV
    3. Match OSM ↔ external candidates (H3 spatial pre-filtering)
    4. Classify changes with review-enriched scoring
    5. Persist run metadata + results to the database

    Returns a summary dict suitable for API responses.
    """
    logger.info("Pipeline started (CSV-driven)")

    osm_pois = load_osm_pois()
    geoapify_pois = load_geoapify_pois()
    reviews = load_reviews()

    matches, best_for_osm, best_for_external = match_pois(
        osm_pois=osm_pois,
        external_pois=geoapify_pois,
        max_distance_m=100.0,
        min_name_similarity=0.60,
    )

    change_results = classify_changes(
        osm_pois=osm_pois,
        external_pois=geoapify_pois,
        best_for_osm=best_for_osm,
        best_for_external=best_for_external,
        reviews=reviews,
    )

    max_results = env_int("GEO_SENTINEL_MAX_RESULTS", 200)
    limited: List = change_results[:max_results] if max_results > 0 else []

    run_id = save_pipeline_run(
        total_osm=len(osm_pois),
        total_external=len(geoapify_pois),
        matched_pairs=len(matches),
        total_results=len(limited),
        status="done",
    )
    save_results(run_id, limited)
    logger.info("Pipeline run #%d complete: %d results → DB", run_id, len(limited))

    result_dicts = [r.model_dump() for r in limited]

    # Cache results in memory for search and initial load
    cache_detection_results(result_dicts)

    return {
        "run_id": run_id,
        "total_osm_pois": len(osm_pois),
        "total_external_pois": len(geoapify_pois),
        "matched_pairs": len(matches),
        "total_results": len(change_results),
        "results": result_dicts,
    }


def get_dashboard_stats() -> Dict[str, Any]:
    """
    Return aggregated statistics for the dashboard.

    Combines data stats from CSVs with the latest pipeline run results.
    """
    data = get_data_stats()

    # Category breakdown
    osm_pois = load_osm_pois()
    geo_pois = load_geoapify_pois()
    reviews = load_reviews()

    osm_categories: Dict[str, int] = {}
    for p in osm_pois:
        osm_categories[p.category] = osm_categories.get(p.category, 0) + 1

    geo_categories: Dict[str, int] = {}
    for p in geo_pois:
        geo_categories[p.category] = geo_categories.get(p.category, 0) + 1

    # Review sentiment summary
    sentiment_counts: Dict[str, int] = {"positive": 0, "neutral": 0, "negative": 0}
    total_rating = 0.0
    for r in reviews.values():
        sentiment_counts[r.sentiment] = sentiment_counts.get(r.sentiment, 0) + 1
        total_rating += r.rating
    avg_rating = round(total_rating / len(reviews), 2) if reviews else 0.0

    # Latest run summary
    latest = get_latest_run()
    latest_run = None
    if latest:
        latest_run = {
            "run_id": latest.get("run_id"),
            "status": latest.get("status"),
            "total_results": latest.get("total_results", 0),
            "matched_pairs": latest.get("matched_pairs", 0),
        }

    return {
        "data": data,
        "osm_categories": dict(sorted(osm_categories.items(), key=lambda x: -x[1])[:20]),
        "geoapify_categories": dict(sorted(geo_categories.items(), key=lambda x: -x[1])[:20]),
        "reviews": {
            "total": len(reviews),
            "avg_rating": avg_rating,
            "sentiment": sentiment_counts,
        },
        "latest_run": latest_run,
        "ml_available": ml_is_available(),
    }


def ensure_detection_cached() -> None:
    """
    Ensure detection results are available in the in-memory cache.

    If no cached results exist, runs detection automatically.
    Called during app startup (lifespan) so that the first request
    always has status data available.
    """
    if has_cached_detection():
        logger.info("Detection results already cached — skipping auto-run")
        return

    logger.info("No cached detection results — running auto-detection on startup")
    try:
        detect_changes()
        logger.info("Auto-detection complete")
    except Exception as exc:
        logger.error("Auto-detection failed on startup: %s", exc)
