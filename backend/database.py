"""
Database module placeholder.

Geo-Sentinel can be extended to persist POIs/matches in Postgres/PostGIS later.
This file exists to match the required structure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DatabaseConfig:
    url: str = "sqlite:///./geo_sentinel.db"
    echo: bool = False


def get_database_config() -> DatabaseConfig:
    return DatabaseConfig()

