"""
Pydantic domain models for Geo Sentinel.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class POIStatus(str, Enum):
    NEW = "NEW"
    CLOSED = "CLOSED"
    MODIFIED = "MODIFIED"
    UNCHANGED = "UNCHANGED"


class ValidationBadge(str, Enum):
    VALIDATED = "VALIDATED"
    CONFLICT = "CONFLICT"
    NOT_VERIFIED = "NOT_VERIFIED"


class POI(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    category: str
    source: str = Field(default="osm")
    raw: Optional[Dict[str, Any]] = None


class Review(BaseModel):
    """Review signals for a POI, loaded from reviews_data.csv."""
    place_id: str
    name: str
    rating: float = Field(ge=0.0, le=5.0)
    review_count: int = Field(ge=0)
    last_review_date: str          # ISO date YYYY-MM-DD
    sentiment: str = "neutral"     # positive | neutral | negative


class MatchCandidate(BaseModel):
    osm: POI
    external: POI
    distance_m: float
    name_similarity: float = Field(ge=0.0, le=1.0)


class ChangeResult(BaseModel):
    name: str
    status: POIStatus
    confidence: float = Field(ge=0.0, le=1.0)
    category: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    osm_id: Optional[str] = None
    external_id: Optional[str] = None
    distance_m: float = 0.0
    name_similarity: Optional[float] = None
    # Review-enriched fields
    review_rating: float = 0.0
    review_count: int = 0
    last_review_date: Optional[str] = None
    review_sentiment: Optional[str] = None
