"""
FastAPI application — Geo Sentinel API v2.

All data is served from CSV files loaded by the data_loader module.
External validation uses the Geoapify Places API for real-world cross-referencing.

Routes
------
- GET  /                   : Health ping
- GET  /healthz            : Database connectivity check
- POST /detect-changes     : Run change detection on CSV data (no DB persist)
- POST /run                : Full pipeline (CSV → match → score → persist)
- GET  /poi/{poi_id}       : Look up a single POI by ID (with reviews)
- GET  /dashboard          : Aggregated statistics
- GET  /results            : Latest pipeline results
- GET  /results/{run_id}   : Results for a specific run
- GET  /runs               : List all pipeline runs
- GET  /search             : Search POIs from in-memory data + DB results
- GET  /validate-poi       : Validate a POI against external Geoapify data
- GET  /validate-poi-batch : Validate a batch of POIs
- GET  /ml-status          : Get the status of the ML scoring model
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.data_loader import (
    get_cached_detection_results,
    get_data_stats,
    get_detection_for_poi,
    get_poi_by_id,
    get_review_for_poi,
    has_cached_detection,
    load_all_pois,
    load_reviews,
)
from backend.database import (
    create_tables,
    get_all_runs,
    get_latest_run,
    get_run_by_id,
    test_database_connection,
)
from backend.ml_model import get_model_info
from backend.services import detect_changes, ensure_detection_cached, get_dashboard_stats, run_pipeline
from backend.utils import logger
from backend.validation import validate_poi, validate_poi_batch

load_dotenv()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    db_ok: bool
    db_error: Optional[str] = None


class ResultItem(BaseModel):
    name: str
    status: str
    confidence: float
    category: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    osm_id: Optional[str] = None
    external_id: Optional[str] = None
    distance_m: float = 0.0
    name_similarity: Optional[float] = None
    review_rating: float = 0.0
    review_count: int = 0
    last_review_date: Optional[str] = None
    review_sentiment: Optional[str] = None


class DetectChangesResponse(BaseModel):
    total_osm_pois: int = 0
    total_external_pois: int = 0
    matched_pairs: int = 0
    total_results: int = 0
    status_counts: Dict[str, int] = {}
    results: List[ResultItem] = []


class PipelineRunResponse(BaseModel):
    run_id: Optional[int] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    total_osm_pois: int = 0
    total_external_pois: int = 0
    matched_pairs: int = 0
    total_results: int = 0
    status: Optional[str] = None
    results: List[ResultItem] = []


class RunSummary(BaseModel):
    id: int
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    total_osm: int = 0
    total_external: int = 0
    matched_pairs: int = 0
    total_results: int = 0
    status: str = "unknown"


class ReviewInfo(BaseModel):
    rating: float
    review_count: int
    last_review_date: str
    sentiment: str


class POIDetailResponse(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    category: str
    source: str
    review: Optional[ReviewInfo] = None


class SearchMatch(BaseModel):
    id: str
    name: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    category: Optional[str] = None
    source: Optional[str] = None
    status: Optional[str] = None
    confidence: Optional[float] = None
    distance_m: float = 0.0
    match_score: float = 0.0
    match_type: str = "poi"


class SearchResponse(BaseModel):
    query: str
    matches: List[SearchMatch] = []


class ExternalMatch(BaseModel):
    name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    category: Optional[str] = None
    distance_m: float = 0.0
    name_similarity: Optional[float] = None
    combined_score: Optional[float] = None
    address: Optional[str] = None


class InternalMatch(BaseModel):
    name: Optional[str] = None
    status: Optional[str] = None
    confidence: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    category: Optional[str] = None
    osm_id: Optional[str] = None


class ValidationInfo(BaseModel):
    internal_status: str = "UNKNOWN"
    external_status: str = "UNKNOWN"
    badge: str = "NOT_VERIFIED"
    source: str = "Geoapify (OpenStreetMap)"


class ValidatePOIResponse(BaseModel):
    query: str
    internal: Optional[InternalMatch] = None
    external: Optional[ExternalMatch] = None
    validation: ValidationInfo


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(application: FastAPI):
    create_tables()
    ok, err = test_database_connection(allow_fallback=True)
    application.state.db_ok = ok
    application.state.db_error = err
    logger.info("Geo Sentinel started — DB ok=%s", ok)

    # Auto-run detection so status data is available on first request
    ensure_detection_cached()

    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Geo Sentinel API", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", tags=["health"])
def root():
    return {"message": "Geo Sentinel Running"}


@app.get("/healthz", response_model=HealthResponse, tags=["health"])
def healthz():
    return HealthResponse(
        status="ok",
        db_ok=app.state.db_ok,
        db_error=app.state.db_error,
    )


@app.post("/detect-changes", response_model=DetectChangesResponse, tags=["detection"])
def api_detect_changes():
    """
    Run change detection on CSV data (force re-run).

    Reads data/osm_data.csv, data/geoapify_data.csv, and data/reviews_data.csv,
    performs spatial matching and scoring, and returns all results.
    Does NOT persist to the database — use POST /run for that.
    """
    try:
        return detect_changes()
    except Exception as exc:
        logger.exception("Change detection failed")
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/detect-changes", response_model=DetectChangesResponse, tags=["detection"])
def api_get_detection_results():
    """
    Return cached detection results without re-running.

    If detection has not been run yet, triggers it automatically.
    This is the primary endpoint for the frontend initial load.
    """
    try:
        if not has_cached_detection():
            return detect_changes()

        cached = get_cached_detection_results()
        # Compute status counts from cached results
        status_counts: Dict[str, int] = {}
        for r in cached:
            s = r.get("status", "UNKNOWN")
            status_counts[s] = status_counts.get(s, 0) + 1

        return {
            "total_osm_pois": 0,  # Not tracked in cache; dashboard has this
            "total_external_pois": 0,
            "matched_pairs": 0,
            "total_results": len(cached),
            "status_counts": status_counts,
            "results": cached,
        }
    except Exception as exc:
        logger.exception("Failed to retrieve detection results")
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/run", response_model=PipelineRunResponse, tags=["pipeline"])
def api_run():
    """Trigger a full pipeline run from CSV data (match → score → persist)."""
    try:
        return run_pipeline()
    except Exception as exc:
        logger.exception("Pipeline run failed")
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/poi/{poi_id:path}", response_model=POIDetailResponse, tags=["pois"])
def api_get_poi(poi_id: str):
    """Look up a single POI by its ID, enriched with review data."""
    poi = get_poi_by_id(poi_id)
    if not poi:
        raise HTTPException(status_code=404, detail=f"POI '{poi_id}' not found")

    review = get_review_for_poi(poi_id)
    return POIDetailResponse(
        id=poi.id,
        name=poi.name,
        lat=poi.lat,
        lon=poi.lon,
        category=poi.category,
        source=poi.source,
        review=ReviewInfo(
            rating=review.rating,
            review_count=review.review_count,
            last_review_date=review.last_review_date,
            sentiment=review.sentiment,
        ) if review else None,
    )


@app.get("/dashboard", tags=["dashboard"])
def api_dashboard():
    """Return aggregated statistics for the dashboard."""
    try:
        return get_dashboard_stats()
    except Exception as exc:
        logger.exception("Dashboard stats failed")
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/results", response_model=PipelineRunResponse, tags=["results"])
def results_latest():
    """Return the most recent pipeline run + results."""
    data = get_latest_run()
    if not data:
        return PipelineRunResponse()
    return data


@app.get("/results/{run_id}", response_model=PipelineRunResponse, tags=["results"])
def results_by_run(run_id: int):
    """Return a specific pipeline run + results."""
    data = get_run_by_id(run_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return data


@app.get("/runs", response_model=List[RunSummary], tags=["results"])
def runs_list():
    """Return history of all pipeline runs (metadata only)."""
    return get_all_runs()


@app.get("/search", response_model=SearchResponse, tags=["search"])
def search(
    q: str = Query("", description="Search query"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
):
    """
    Search POIs from in-memory CSV data by name and category.

    Uses the data_loader's in-memory index for fast search.
    Falls back gracefully if data hasn't been loaded.
    """
    query = q.strip()
    if not query:
        return SearchResponse(query=q)

    q_lower = query.lower()
    tokens = q_lower.split()

    # Search in-memory POI data from CSVs
    all_pois = load_all_pois()
    scored: List[Dict[str, Any]] = []

    for p in all_pois:
        name_lower = (p.name or "").lower()
        cat_lower = (p.category or "").lower()

        # Quick relevance check (skip completely unrelated POIs)
        if not any(t in name_lower or t in cat_lower for t in tokens):
            continue

        name_score = SequenceMatcher(None, q_lower, name_lower).ratio()
        cat_score = max(
            (SequenceMatcher(None, t, cat_lower).ratio() for t in tokens),
            default=0.0,
        )
        exact_bonus = 0.25 if q_lower in name_lower else 0.0
        score = round(0.60 * name_score + 0.25 * cat_score + exact_bonus, 4)

        if score > 0.2:
            # Enrich with detection status from cache
            detection = get_detection_for_poi(poi_id=p.id, name=p.name)
            status = detection.get("status") if detection else None
            confidence = detection.get("confidence") if detection else None
            distance_m = detection.get("distance_m") if detection else None

            scored.append({
                "id": p.id,
                "name": p.name,
                "lat": p.lat,
                "lon": p.lon,
                "category": p.category,
                "source": p.source,
                "status": status or "UNCHANGED",
                "confidence": confidence,
                "distance_m": distance_m,
                "match_score": score,
                "match_type": "poi",
            })

    # De-duplicate by name (keep highest score)
    seen_names: Dict[str, int] = {}
    deduped: List[Dict[str, Any]] = []
    for item in scored:
        key = (item["name"] or "").lower()
        if key in seen_names:
            idx = seen_names[key]
            if item["match_score"] > deduped[idx]["match_score"]:
                deduped[idx] = item
        else:
            seen_names[key] = len(deduped)
            deduped.append(item)

    deduped.sort(key=lambda x: -x["match_score"])
    return SearchResponse(query=query, matches=deduped[:limit])


@app.get("/validate-poi", response_model=ValidatePOIResponse, tags=["validation"])
def api_validate_poi(
    name: str = Query(..., description="POI name to validate against external source"),
):
    """
    Validate a POI by cross-referencing internal detection results with
    live Geoapify Places API data.

    Returns internal status, external status, and a validation badge:
    - VALIDATED   ✅  Statuses agree
    - CONFLICT    ⚠️  Statuses disagree
    - NOT_VERIFIED ❌  Could not verify
    """
    try:
        result = validate_poi(name.strip())
        return result
    except Exception as exc:
        logger.exception("POI validation failed for '%s'", name)
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/validate-poi-batch", tags=["validation"])
def api_validate_poi_batch(
    names: str = Query(..., description="Comma-separated POI names to validate"),
):
    """
    Validate multiple POIs at once.

    Pass a comma-separated list of names, e.g. ?names=Starbucks,KFC,McDonald's
    """
    try:
        name_list = [n.strip() for n in names.split(",") if n.strip()]
        if not name_list:
            raise HTTPException(status_code=400, detail="No names provided")
        if len(name_list) > 10:
            raise HTTPException(status_code=400, detail="Max 10 names per batch")
        results = validate_poi_batch(name_list)
        return {"results": results}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Batch validation failed")
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/ml-status", tags=["system"])
def api_ml_status():
    """Get the current loaded status and accuracy of the ML scoring model."""
    try:
        return get_model_info()
    except Exception as exc:
        logger.exception("Failed to get ML status")
        raise HTTPException(status_code=500, detail=str(exc))