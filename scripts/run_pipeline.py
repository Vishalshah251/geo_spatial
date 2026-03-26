from backend.ingestion_osm import fetch_osm_pois
from backend.ingestion_geoapify import fetch_geoapify_places
from backend.matching import match_pois
from backend.scoring import classify_changes
from backend.utils import log


def run_pipeline() -> dict:
    osm_pois = fetch_osm_pois()
    geoapify_pois = fetch_geoapify_places()

    matches, best_for_osm, best_for_external = match_pois(
        osm_pois=osm_pois,
        external_pois=geoapify_pois,
        max_distance_m=100.0,
        min_name_similarity=0.60,
    )

    results = classify_changes(
        osm_pois=osm_pois,
        external_pois=geoapify_pois,
        best_for_osm=best_for_osm,
        best_for_external=best_for_external,
    )

    payload = {
        "total_osm_pois": len(osm_pois),
        "total_external_pois": len(geoapify_pois),
        "matched_pairs": len(matches),
        "results": [r.model_dump() for r in results],
    }
    return payload


def main() -> None:
    payload = run_pipeline()
    log(f"total OSM POIs: {payload['total_osm_pois']}")
    log(f"total external POIs: {payload['total_external_pois']}")
    log(f"matched POIs: {payload['matched_pairs']}")

    top = payload["results"][:20]
    for r in top:
        print({"name": r["name"], "status": r["status"], "confidence": round(r["confidence"], 3)})

if __name__ == "__main__":
    main()