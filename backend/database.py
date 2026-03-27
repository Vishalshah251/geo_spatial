"""
Database layer – SQLAlchemy ORM tables and CRUD helpers.

Tables
------
- pois            : Every POI ingested from any source.
- pipeline_runs   : Metadata for each detection run.
- results         : Change-detection results linked to a run.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


# ---------------------------------------------------------------------------
# ORM Base & Models
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class POIRow(Base):
    __tablename__ = "pois"

    id = Column(String, primary_key=True)          # e.g. "osm:node:12345"
    name = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    category = Column(String, default="other")
    source = Column(String, default="osm")          # "osm" | "geoapify"
    raw_json = Column(Text, default="{}")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class PipelineRunRow(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = Column(DateTime, nullable=True)
    total_osm = Column(Integer, default=0)
    total_external = Column(Integer, default=0)
    matched_pairs = Column(Integer, default=0)
    total_results = Column(Integer, default=0)
    status = Column(String, default="running")       # running | done | failed


class ResultRow(Base):
    __tablename__ = "results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("pipeline_runs.id"), nullable=False)
    name = Column(String, nullable=False)
    status = Column(String, nullable=False)           # NEW | CLOSED | MODIFIED | UNCHANGED
    confidence = Column(Float, default=0.0)
    category = Column(String, nullable=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    osm_id = Column(String, nullable=True)
    external_id = Column(String, nullable=True)
    distance_m = Column(Float, nullable=True)
    name_similarity = Column(Float, nullable=True)


# ---------------------------------------------------------------------------
# Engine / Session
# ---------------------------------------------------------------------------

_DB_URL_DEFAULT = "sqlite:///./geo_sentinel.db"


def _get_url() -> str:
    return os.environ.get("DATABASE_URL") or _DB_URL_DEFAULT


def get_engine() -> Engine:
    return create_engine(_get_url(), echo=False, future=True)


_SessionFactory: Optional[sessionmaker] = None


def get_session() -> Session:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine())
    return _SessionFactory()


# ---------------------------------------------------------------------------
# Schema management
# ---------------------------------------------------------------------------

def create_tables() -> None:
    """Create all tables if they don't exist."""
    engine = get_engine()
    Base.metadata.create_all(engine)


def test_database_connection(allow_fallback: bool = True) -> Tuple[bool, Optional[str]]:
    """Returns (ok, error_message)."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        if allow_fallback:
            return False, str(e)
        raise


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------

def save_pois(pois: list) -> int:
    """
    Upsert a list of backend.models.POI objects into the pois table.
    Returns the number of rows written.
    """
    if not pois:
        return 0
    session = get_session()
    try:
        count = 0
        for p in pois:
            raw = json.dumps(p.raw) if p.raw else "{}"
            existing = session.get(POIRow, p.id)
            if existing:
                existing.name = p.name
                existing.lat = p.lat
                existing.lon = p.lon
                existing.category = p.category
                existing.source = p.source
                existing.raw_json = raw
            else:
                session.add(POIRow(
                    id=p.id, name=p.name, lat=p.lat, lon=p.lon,
                    category=p.category, source=p.source, raw_json=raw,
                ))
            count += 1
        session.commit()
        return count
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def load_pois_by_source(source: str) -> List[Dict[str, Any]]:
    """Return all POIs for a given source as dicts."""
    session = get_session()
    try:
        rows = session.query(POIRow).filter(POIRow.source == source).all()
        return [
            {
                "id": r.id, "name": r.name, "lat": r.lat, "lon": r.lon,
                "category": r.category, "source": r.source,
                "raw": json.loads(r.raw_json) if r.raw_json else None,
            }
            for r in rows
        ]
    finally:
        session.close()


def save_pipeline_run(
    total_osm: int, total_external: int, matched_pairs: int, total_results: int,
    status: str = "done",
) -> int:
    """Insert a pipeline run row. Returns the new run id."""
    session = get_session()
    try:
        now = datetime.now(timezone.utc)
        row = PipelineRunRow(
            started_at=now, finished_at=now,
            total_osm=total_osm, total_external=total_external,
            matched_pairs=matched_pairs, total_results=total_results,
            status=status,
        )
        session.add(row)
        session.commit()
        run_id = row.id
        return run_id
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def save_results(run_id: int, results: list) -> int:
    """Persist ChangeResult dicts (or model_dump() output) to the results table."""
    if not results:
        return 0
    session = get_session()
    try:
        for r in results:
            d = r if isinstance(r, dict) else r.model_dump()
            session.add(ResultRow(
                run_id=run_id,
                name=d["name"],
                status=d["status"],
                confidence=d.get("confidence", 0.0),
                category=d.get("category"),
                lat=d.get("lat"),
                lon=d.get("lon"),
                osm_id=d.get("osm_id"),
                external_id=d.get("external_id"),
                distance_m=d.get("distance_m"),
                name_similarity=d.get("name_similarity"),
            ))
        session.commit()
        return len(results)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_latest_run() -> Optional[Dict[str, Any]]:
    """Return the most recent pipeline run + its results, or None."""
    session = get_session()
    try:
        run = (
            session.query(PipelineRunRow)
            .order_by(PipelineRunRow.id.desc())
            .first()
        )
        if not run:
            return None
        rows = session.query(ResultRow).filter(ResultRow.run_id == run.id).all()
        return _run_to_dict(run, rows)
    finally:
        session.close()


def get_run_by_id(run_id: int) -> Optional[Dict[str, Any]]:
    """Return a specific pipeline run + its results, or None."""
    session = get_session()
    try:
        run = session.get(PipelineRunRow, run_id)
        if not run:
            return None
        rows = session.query(ResultRow).filter(ResultRow.run_id == run.id).all()
        return _run_to_dict(run, rows)
    finally:
        session.close()


def get_all_runs() -> List[Dict[str, Any]]:
    """Return metadata for all pipeline runs (no results attached)."""
    session = get_session()
    try:
        runs = session.query(PipelineRunRow).order_by(PipelineRunRow.id.desc()).all()
        return [
            {
                "id": r.id,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "total_osm": r.total_osm,
                "total_external": r.total_external,
                "matched_pairs": r.matched_pairs,
                "total_results": r.total_results,
                "status": r.status,
            }
            for r in runs
        ]
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Search / Query
# ---------------------------------------------------------------------------

def search_pois(query: str, limit: int = 50) -> Dict[str, Any]:
    """
    Interpret user query and return best matching POIs + results from DB.
    Uses fuzzy matching on name, category, and status.  Never hallucinated —
    only returns data that exists in the ingested pipeline.
    """
    from difflib import SequenceMatcher

    q = query.strip().lower()
    if not q:
        return {"query": query, "matches": []}

    tokens = q.split()

    # ── Detect if the query mentions a status keyword ──
    STATUS_KEYWORDS = {"new", "closed", "modified", "unchanged"}
    status_filter = None
    for t in tokens:
        if t.upper() in STATUS_KEYWORDS:
            status_filter = t.upper()
            break

    session = get_session()
    try:
        # ── Search POIs table ──
        poi_query = session.query(POIRow)
        for token in tokens:
            like = f"%{token}%"
            poi_query = poi_query.filter(
                POIRow.name.ilike(like) | POIRow.category.ilike(like)
            )
        candidate_pois = poi_query.limit(limit * 3).all()

        # ── Search Results table (from latest run) ──
        result_matches = []
        latest_run = (
            session.query(PipelineRunRow)
            .order_by(PipelineRunRow.id.desc())
            .first()
        )
        if latest_run:
            res_query = session.query(ResultRow).filter(ResultRow.run_id == latest_run.id)
            if status_filter:
                res_query = res_query.filter(ResultRow.status == status_filter)
            for token in tokens:
                if token.upper() in STATUS_KEYWORDS:
                    continue  # already filtered
                like = f"%{token}%"
                res_query = res_query.filter(
                    ResultRow.name.ilike(like) | ResultRow.category.ilike(like)
                )
            result_matches = res_query.limit(limit * 3).all()

        # ── Score & rank POIs ──
        scored = []
        for p in candidate_pois:
            name_lower = (p.name or "").lower()
            cat_lower = (p.category or "").lower()
            name_score = SequenceMatcher(None, q, name_lower).ratio()
            cat_score = max(
                (SequenceMatcher(None, t, cat_lower).ratio() for t in tokens),
                default=0.0,
            )
            # Exact substring bonus
            exact_bonus = 0.25 if q in name_lower else 0.0
            score = round(0.60 * name_score + 0.25 * cat_score + exact_bonus, 4)
            scored.append({
                "id": p.id,
                "name": p.name,
                "lat": p.lat,
                "lon": p.lon,
                "category": p.category,
                "source": p.source,
                "match_score": score,
                "match_type": "poi",
            })

        # ── Score & rank Results ──
        for r in result_matches:
            name_lower = (r.name or "").lower()
            cat_lower = (r.category or "").lower()
            name_score = SequenceMatcher(None, q, name_lower).ratio()
            cat_score = max(
                (SequenceMatcher(None, t, cat_lower).ratio() for t in tokens),
                default=0.0,
            )
            exact_bonus = 0.25 if q in name_lower else 0.0
            status_bonus = 0.15 if status_filter and r.status == status_filter else 0.0
            score = round(0.50 * name_score + 0.20 * cat_score + exact_bonus + status_bonus, 4)
            scored.append({
                "id": r.osm_id or r.external_id or str(r.id),
                "name": r.name,
                "lat": r.lat,
                "lon": r.lon,
                "category": r.category,
                "source": "result",
                "status": r.status,
                "confidence": r.confidence,
                "distance_m": r.distance_m,
                "match_score": score,
                "match_type": "result",
            })

        # De-duplicate by name (keep highest score)
        seen_names: Dict[str, int] = {}
        deduped = []
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
        return {"query": query, "matches": deduped[:limit]}
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _run_to_dict(run: PipelineRunRow, result_rows: list) -> Dict[str, Any]:
    return {
        "run_id": run.id,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "total_osm_pois": run.total_osm,
        "total_external_pois": run.total_external,
        "matched_pairs": run.matched_pairs,
        "total_results": run.total_results,
        "status": run.status,
        "results": [
            {
                "name": r.name,
                "status": r.status,
                "confidence": r.confidence,
                "category": r.category,
                "lat": r.lat,
                "lon": r.lon,
                "osm_id": r.osm_id,
                "external_id": r.external_id,
                "distance_m": r.distance_m,
                "name_similarity": r.name_similarity,
            }
            for r in result_rows
        ],
    }
