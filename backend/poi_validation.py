"""
POI Validation Engine

Implements ground-truth validation methodology using OSM as the primary source
and Geoapify as the supporting/validation source.
"""

import math
from typing import List, Dict, Any, Tuple, Optional
from difflib import SequenceMatcher

# ==========================================
# STEP 3: DISTANCE CALCULATION
# ==========================================
def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate Haversine distance in meters between two coordinates."""
    r = 6371000.0  # radius of Earth in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2)**2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def calculate_similarity(name1: str, name2: str) -> float:
    """Calculate string similarity ratio between 0.0 and 1.0."""
    if not name1 or not name2:
        return 0.0
    return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()

# ==========================================
# STEP 2: ENTITY MATCHING
# ==========================================
def match_entities(
    osm_poi: Dict[str, Any], 
    geoapify_pois: List[Dict[str, Any]], 
    distance_threshold: float = 100.0, 
    similarity_threshold: float = 0.65
) -> Tuple[Optional[Dict[str, Any]], float, float]:
    """
    Find the best matching Geoapify POI for a given OSM POI.
    Only pairs if similarity is high and distance is small.
    """
    best_match = None
    best_score = 0.0
    best_dist = float('inf')
    best_sim = 0.0
    
    for ext_poi in geoapify_pois:
        dist = calculate_distance(osm_poi['lat'], osm_poi['lon'], ext_poi['lat'], ext_poi['lon'])
        sim = calculate_similarity(osm_poi['name'], ext_poi['name'])
        
        # Only pair records if: similarity high AND distance small
        if dist <= distance_threshold and sim >= similarity_threshold:
            # Weighted score for best candidate selection
            score = (0.6 * sim) + (0.4 * max(0, 1 - (dist / distance_threshold)))
            if score > best_score:
                best_match = ext_poi
                best_score = score
                best_dist = dist
                best_sim = sim
                
    return best_match, best_dist, best_sim

# ==========================================
# STEP 4-8: COMPARISON, SCORING, OUTPUT
# ==========================================
def validate_poi(osm_poi: Dict[str, Any], geoapify_pois: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate an OSM POI against external Geoapify POIs.
    OSM is considered the ultimate ground truth.
    """
    # STEP 5: GROUND TRUTH RULE
    osm_status = osm_poi.get('status', 'OPEN')
    osm_category = osm_poi.get('category', 'unknown')
    osm_name = osm_poi.get('name', 'Unknown')
    
    ext_match, distance, similarity = match_entities(osm_poi, geoapify_pois)
    
    # Initialize defaults
    final_status = osm_status # Final truth always anchors on OSM
    change_status = "UNCHANGED" 
    external_status = "NOT_FOUND"
    confidence_score = 0.0
    reason = ""
    
    if not ext_match:
        if osm_status == 'CLOSED':
            change_status = "UNCHANGED"
            confidence_score = 0.90
            reason = "External absence confirms OSM CLOSED status"
        else:
            change_status = "CONFLICT"
            confidence_score = 0.40 # Low confidence: weak signals/mismatch
            reason = "No external match found for active OSM POI"
    else:
        # STEP 4: COMPARISON
        external_status = ext_match.get('status', 'OPEN')
        ext_category = ext_match.get('category', 'unknown')
        
        cat_match = (osm_category.lower() == ext_category.lower() or 'unknown' in [osm_category, ext_category])
        
        # STEP 6 & 7: CHANGE DETECTION & CONFIDENCE SCORING
        if osm_status == 'OPEN':
            if external_status == 'OPEN':
                if similarity >= 0.85 and cat_match and distance < 50:
                    change_status = "UNCHANGED"
                    confidence_score = 0.95 # High: match + close distance
                    reason = "Strong match on name, location, and category"
                else:
                    change_status = "MODIFIED"
                    confidence_score = 0.70 # Medium-high: Matched but with differences
                    reason = f"Matched but differences noted (Category match: {cat_match}, Sim: {similarity:.2f})"
            else:
                change_status = "CONFLICT"
                confidence_score = 0.50 # Low/Medium: Contradiction
                reason = "External source reports CLOSED, but OSM reports OPEN"
                
        else: # OSM says CLOSED
            if external_status == 'OPEN':
                change_status = "CONFLICT"
                confidence_score = 0.30 # Low: Contradiction
                reason = "OSM reports CLOSED, but found active external match"
            else:
                change_status = "UNCHANGED" 
                confidence_score = 0.95 # High
                reason = "Both sources confirm CLOSED status"
                
    # STEP 8: OUTPUT
    return {
        "osm_id": osm_poi.get('id'),
        "name": osm_name,
        "final_status": change_status, # The computed state of the POI based on comparison
        "ground_truth_status": osm_status,
        "external_status": external_status,
        "confidence_score": confidence_score,
        "distance": round(distance, 2) if ext_match else None,
        "reason": reason
    }

# ==========================================
# STEP 1: DATA INGESTION
# ==========================================
def process_pipeline(osm_dataset: List[Dict[str, Any]], geoapify_dataset: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Load POIs from OSM (primary) and Geoapify (external) and process validation.
    """
    results = []
    for osm_poi in osm_dataset:
        result = validate_poi(osm_poi, geoapify_dataset)
        results.append(result)
    return results

if __name__ == "__main__":
    # Example dataset to demonstrate ingestion and results
    osm_data = [
        {"id": "node/1", "name": "Starbucks Clementi", "lat": 1.3151, "lon": 103.7650, "status": "OPEN", "category": "cafe"},
        {"id": "node/2", "name": "Old Coffee Shop", "lat": 1.3160, "lon": 103.7660, "status": "CLOSED", "category": "cafe"},
        {"id": "node/3", "name": "Ghost Mart", "lat": 1.3170, "lon": 103.7670, "status": "OPEN", "category": "retail"}
    ]
    
    geoapify_data = [
        {"id": "geo_1", "name": "Starbucks", "lat": 1.3152, "lon": 103.7651, "status": "OPEN", "category": "cafe"},
        {"id": "geo_2", "name": "Old Coffee Shop", "lat": 1.3160, "lon": 103.7660, "status": "CLOSED", "category": "cafe"}
    ]
    
    validated_results = process_pipeline(osm_data, geoapify_data)
    import json
    print(json.dumps(validated_results, indent=2))
