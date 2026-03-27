"""
Generate ML Training Data from existing rule-based logic.

This script runs the existing matching and rule-based scoring engines
to assign labels (NEW, CLOSED, MODIFIED, UNCHANGED) to POIs. It then
extracts features for each POI and saves them to a CSV for training
the Random Forest classifier.
"""

import csv
import os
from pathlib import Path

from backend.data_loader import load_geoapify_pois, load_osm_pois, load_reviews
from backend.matching import match_pois
from backend.ml_features import FEATURE_NAMES, extract_features
# We import the original rule-based logic manually to bypass ML
import backend.scoring as scoring
from backend.utils import logger

# Output path
OS_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_FILE = OS_DIR / "training_data.csv"

def generate_training_data():
    logger.info("Generating ML training data from existing rules...")
    
    # 1. Load data
    osm_pois = load_osm_pois()
    geoapify_pois = load_geoapify_pois()
    reviews = load_reviews()
    
    if not osm_pois or not geoapify_pois:
        logger.error("Missing POI data. Cannot generate training set.")
        return
        
    # 2. Run matching
    matches, best_for_osm, best_for_external = match_pois(osm_pois, geoapify_pois)
    
    # 3. Temporarily disable ML to get rule-based labels
    # We do this by mocking the ml_used return
    scoring._ml_classify_temp = scoring._ml_classify
    def _mock_ml_classify(*args, **kwargs):
        raise Exception("Force fallback to rules")
        
    # Let's just bypass the _ml_classify call entirely by unsetting the model
    from backend import ml_model
    original_availability = ml_model.is_available
    ml_model.is_available = lambda: False
    
    try:
        # Run rule-based scoring to get ground truth labels
        labels_result = scoring.classify_changes(
            osm_pois, geoapify_pois, best_for_osm, best_for_external, reviews
        )
    finally:
        ml_model.is_available = original_availability
        
    # Create label mapping
    # Note: external_id is not guaranteed for OSM points without matches. However, name + category will suffice.
    label_map = {}
    for r in labels_result:
        # We'll use osm_id if available, else external_id
        if r.osm_id:
            label_map[f"osm:{r.osm_id}"] = r.status.value
        elif r.external_id:
            label_map[f"ext:{r.external_id}"] = r.status.value

    # 4. Extract features for all POIs
    dataset = []
    
    # Process OSM POIs
    for o in osm_pois:
        m = best_for_osm.get(o.id)
        r = reviews.get(o.id)
        
        feats = extract_features(o, m, r, is_external_only=False)
        label = label_map.get(f"osm:{o.id}")
        
        if label:
            row = {fname: float(fval) for fname, fval in zip(FEATURE_NAMES, feats)}
            row["label"] = label
            row["poi_id"] = f"osm:{o.id}"
            row["name"] = o.name
            dataset.append(row)
            
    # Process external POIs without matches (NEW)
    for e in geoapify_pois:
        if best_for_external.get(e.id) is None:
            r = reviews.get(e.id)
            feats = extract_features(e, None, r, is_external_only=True)
            label = label_map.get(f"ext:{e.id}")
            
            if label:
                row = {fname: float(fval) for fname, fval in zip(FEATURE_NAMES, feats)}
                row["label"] = label
                row["poi_id"] = f"ext:{e.id}"
                row["name"] = e.name
                dataset.append(row)
                
    # 5. Save to CSV
    OS_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(OUTPUT_FILE, "w", newline="") as f:
        fieldnames = ["poi_id", "name"] + FEATURE_NAMES + ["label"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(dataset)
        
    logger.info("Successfully generated %d training samples saved to %s", len(dataset), OUTPUT_FILE)
    
    # Print label distribution
    from collections import Counter
    counts = Counter([d["label"] for d in dataset])
    logger.info("Class distribution: %s", counts)

if __name__ == "__main__":
    generate_training_data()
