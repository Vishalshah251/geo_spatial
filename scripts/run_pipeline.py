"""
CLI entry point for running the full change-detection pipeline.

Uses CSV data — no live API calls. Run ingestion scripts first
to generate the CSV files.

Usage:
    python -m scripts.run_pipeline
"""

from dotenv import load_dotenv

load_dotenv()

from backend.services import run_pipeline
from backend.utils import logger


def main() -> None:
    payload = run_pipeline()
    logger.info("Total OSM POIs: %d", payload["total_osm_pois"])
    logger.info("Total external POIs: %d", payload["total_external_pois"])
    logger.info("Matched POIs: %d", payload["matched_pairs"])

    for r in payload["results"][:20]:
        print({"name": r["name"], "status": r["status"], "confidence": round(r["confidence"], 3)})


if __name__ == "__main__":
    main()