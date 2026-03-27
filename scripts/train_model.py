"""
Train Random Forest ML Model for POI Classification.

Loads the generated training data, trains a RandomForestClassifier,
evaluates it using cross-validation, and saves the trained model to disk.
"""

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import StratifiedKFold, cross_val_predict

from backend.ml_features import FEATURE_NAMES, NUM_FEATURES
from backend.ml_model import STATUS_LABELS
from backend.utils import logger

# Paths
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT_DIR / "data" / "training_data.csv"
MODEL_DIR = ROOT_DIR / "models"
MODEL_PATH = MODEL_DIR / "poi_classifier.pkl"
METADATA_PATH = MODEL_DIR / "model_metadata.json"
FEATURES_PATH = MODEL_DIR / "feature_names.json"

def train():
    if not DATA_PATH.exists():
        logger.error("Training data not found at %s", DATA_PATH)
        logger.error("Please run scripts/generate_training_data.py first.")
        return
        
    logger.info("Loading training data from %s", DATA_PATH)
    df = pd.read_csv(DATA_PATH)
    
    # Setup Features (X) and Labels (y)
    X = df[FEATURE_NAMES].values
    y = df["label"].values
    
    # 1. Model Configuration
    # We use a lightweight Random Forest.
    # class_weight="balanced" helps deal with the skewed data (lots of CLOSED).
    clf = RandomForestClassifier(
        n_estimators=100,
        max_depth=12,
        min_samples_split=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1  # Use all CPU cores
    )
    
    # 2. Cross Validation Evaluation
    logger.info("Running 5-fold cross validation...")
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    y_pred = cross_val_predict(clf, X, y, cv=cv)
    
    report = classification_report(y, y_pred, output_dict=True)
    logger.info("\n%s", classification_report(y, y_pred))
    
    accuracy = report["accuracy"]
    logger.info("CV Accuracy: %.3f", accuracy)
    
    # 3. Train on full dataset
    logger.info("Training final model on full dataset...")
    clf.fit(X, y)
    
    # 4. Save Model and Metadata
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    
    import joblib
    joblib.dump(clf, MODEL_PATH)
    
    # Save feature names to ensure order consistency
    FEATURES_PATH.write_text(json.dumps(FEATURE_NAMES))
    
    # Save metadata
    metadata = {
        "accuracy": accuracy,
        "n_samples": len(X),
        "n_features": NUM_FEATURES,
        "classes": list(clf.classes_),
        "feature_importances": {
            name: float(imp) for name, imp in zip(FEATURE_NAMES, clf.feature_importances_)
        }
    }
    METADATA_PATH.write_text(json.dumps(metadata, indent=2))
    
    logger.info("Model saved successfully:")
    logger.info("  - Model:    %s", MODEL_PATH)
    logger.info("  - Metadata: %s", METADATA_PATH)
    logger.info("  - Features: %s", FEATURES_PATH)
    
    # Print top feature importances
    importances = list(zip(FEATURE_NAMES, clf.feature_importances_))
    importances.sort(key=lambda x: -x[1])
    logger.info("Top features:")
    for name, imp in importances[:5]:
        logger.info("  %s: %.3f", name, imp)

if __name__ == "__main__":
    train()
