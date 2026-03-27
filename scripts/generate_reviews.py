"""
Generate a reviews dataset from existing POI CSVs.

Reads data/osm_data.csv and data/geoapify_data.csv, generates
realistic review signals for each POI, and writes data/reviews_data.csv.

Usage:
    python -m scripts.generate_reviews
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _load_poi_ids(csv_path: Path) -> list[tuple[str, str]]:
    """Return list of (id, name) from a POI CSV."""
    if not csv_path.exists():
        print(f"⚠ {csv_path.name} not found, skipping")
        return []
    pairs = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pairs.append((row["id"], row["name"]))
    return pairs


def _generate_review(place_id: str, name: str, today: datetime) -> dict:
    """Generate one realistic review record."""
    # Rating distribution: skewed toward 3.5-4.5 (most real places)
    rating = round(random.triangular(1.0, 5.0, 4.0), 1)
    rating = max(1.0, min(5.0, rating))

    # Review count: exponential-ish distribution
    review_count = int(random.expovariate(1 / 50))
    review_count = min(review_count, 500)

    # Last review date: most are recent, some old
    if review_count == 0:
        # No reviews → set date far in the past
        days_ago = random.randint(180, 365)
    elif random.random() < 0.7:
        # 70% have recent reviews
        days_ago = random.randint(0, 30)
    else:
        days_ago = random.randint(31, 365)

    last_review_date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")

    # Sentiment derived from rating
    if rating >= 4.0:
        sentiment = "positive"
    elif rating >= 2.5:
        sentiment = "neutral"
    else:
        sentiment = "negative"

    return {
        "place_id": place_id,
        "name": name,
        "rating": rating,
        "review_count": review_count,
        "last_review_date": last_review_date,
        "sentiment": sentiment,
    }


def main() -> None:
    today = datetime.now()

    # Load POI IDs from both sources
    osm_pois = _load_poi_ids(DATA_DIR / "osm_data.csv")
    geo_pois = _load_poi_ids(DATA_DIR / "geoapify_data.csv")
    all_pois = osm_pois + geo_pois

    if not all_pois:
        print("❌ No POI data found. Run ingestion first.")
        return

    print(f"Generating reviews for {len(all_pois)} POIs...")

    # Generate reviews (not every POI gets one — ~80% coverage)
    random.seed(42)  # Reproducible
    reviews = []
    for place_id, name in all_pois:
        if random.random() < 0.80:
            reviews.append(_generate_review(place_id, name, today))

    # Write CSV
    out_path = DATA_DIR / "reviews_data.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["place_id", "name", "rating", "review_count", "last_review_date", "sentiment"],
        )
        writer.writeheader()
        writer.writerows(reviews)

    print(f"✅ Generated {len(reviews)} reviews → {out_path}")
    print(f"   Coverage: {len(reviews)}/{len(all_pois)} ({100*len(reviews)/len(all_pois):.1f}%)")

    # Stats
    sentiments = {"positive": 0, "neutral": 0, "negative": 0}
    for r in reviews:
        sentiments[r["sentiment"]] += 1
    print(f"   Sentiment: {sentiments}")


if __name__ == "__main__":
    main()
