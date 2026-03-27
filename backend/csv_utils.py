"""
CSV export helpers for POI data.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import List

from backend.models import POI
from backend.utils import logger


def save_pois_to_csv(pois: List[POI], file_path: Path) -> None:
    """Write POI objects to a CSV file."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "lat", "lon", "category", "source"])
        for p in pois:
            writer.writerow([p.id, p.name, p.lat, p.lon, p.category, p.source])
    logger.info("Exported %d POIs → %s", len(pois), file_path.name)