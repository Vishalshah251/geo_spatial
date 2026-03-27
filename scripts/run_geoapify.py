"""
CLI entry point for standalone Geoapify ingestion.

Usage:
    python -m scripts.run_geoapify

Requires GEOAPIFY_API_KEY to be set in .env
"""

from dotenv import load_dotenv

load_dotenv()

from scripts.ingestion_geoapify import fetch_geoapify_places
from backend.utils import logger


def main() -> None:
    logger.info("Running Geoapify ingestion...")
    pois = fetch_geoapify_places()
    logger.info("Done. Total POIs: %d", len(pois))


if __name__ == "__main__":
    main()
