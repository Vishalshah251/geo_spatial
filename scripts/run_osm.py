"""
CLI entry point for standalone OSM ingestion.

Usage:
    python -m scripts.run_osm
"""

from dotenv import load_dotenv

load_dotenv()

from scripts.ingestion_osm import fetch_osm_pois
from backend.utils import logger


def main() -> None:
    logger.info("Running OSM ingestion...")
    pois = fetch_osm_pois()
    logger.info("Done. Total POIs: %d", len(pois))


if __name__ == "__main__":
    main()