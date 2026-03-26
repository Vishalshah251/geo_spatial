from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class POIStatus(str, Enum):
    NEW = "NEW"
    CLOSED = "CLOSED"
    MODIFIED = "MODIFIED"
    UNCHANGED = "UNCHANGED"


class POI(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    category: str
    source: str = Field(default="osm")
    raw: Optional[Dict[str, Any]] = None


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
    distance_m: Optional[float] = None
    name_similarity: Optional[float] = None

