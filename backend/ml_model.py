"""
ML Model Manager for POI Change Detection.

Loads a pre-trained Random Forest classifier from disk and provides
thread-safe prediction methods. Falls back gracefully if the model
file is missing or corrupt.

Model files expected at:
  models/poi_classifier.pkl      — trained sklearn model
  models/feature_names.json      — ordered feature name list
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from backend.utils import logger

_MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
_MODEL_PATH = _MODELS_DIR / "poi_classifier.pkl"
_FEATURES_PATH = _MODELS_DIR / "feature_names.json"

# Status label mapping (must match training labels)
STATUS_LABELS = ["CLOSED", "MODIFIED", "NEW", "UNCHANGED"]

# ── Singleton model holder ──

_lock = threading.Lock()


class _ModelHolder:
    """Thread-safe lazy-loading model singleton."""

    def __init__(self) -> None:
        self.model: Any = None
        self.feature_names: List[str] = []
        self.loaded: bool = False
        self.load_error: Optional[str] = None
        self.accuracy: Optional[float] = None

    def load(self) -> bool:
        """Load model from disk. Returns True if successful."""
        if not _MODEL_PATH.exists():
            self.load_error = f"Model file not found: {_MODEL_PATH}"
            logger.info("ML model not found at %s — using rule-based scoring", _MODEL_PATH)
            return False

        try:
            import joblib
            self.model = joblib.load(_MODEL_PATH)
            self.loaded = True
            self.load_error = None

            # Load feature names if available
            if _FEATURES_PATH.exists():
                self.feature_names = json.loads(_FEATURES_PATH.read_text())

            # Try to read accuracy from model metadata
            meta_path = _MODELS_DIR / "model_metadata.json"
            if meta_path.exists():
                meta = json.loads(meta_path.read_text())
                self.accuracy = meta.get("accuracy")

            logger.info(
                "ML model loaded: %s (accuracy=%.3f)",
                type(self.model).__name__,
                self.accuracy or 0.0,
            )
            return True

        except Exception as exc:
            self.load_error = str(exc)
            self.loaded = False
            logger.warning("Failed to load ML model: %s — falling back to rules", exc)
            return False


_holder = _ModelHolder()


# ── Public API ──


def load_model() -> bool:
    """Load/reload the ML model. Thread-safe. Returns True if model is ready."""
    with _lock:
        return _holder.load()


def is_available() -> bool:
    """Check if the ML model is loaded and ready for predictions."""
    with _lock:
        if not _holder.loaded:
            # Try loading once
            _holder.load()
        return _holder.loaded


def get_model_info() -> Dict[str, Any]:
    """Return model metadata for the /ml-status endpoint."""
    with _lock:
        if not _holder.loaded:
            _holder.load()
        return {
            "available": _holder.loaded,
            "model_type": type(_holder.model).__name__ if _holder.model else None,
            "accuracy": _holder.accuracy,
            "feature_count": len(_holder.feature_names),
            "feature_names": _holder.feature_names,
            "model_path": str(_MODEL_PATH),
            "error": _holder.load_error if not _holder.loaded else None,
        }


def predict_status(features: np.ndarray) -> Tuple[str, float]:
    """
    Predict POI status from a feature vector.

    Args:
        features: numpy array of shape (9,) or (1, 9).

    Returns:
        (status_label, confidence) tuple.
        status_label is one of: NEW, CLOSED, MODIFIED, UNCHANGED
        confidence is the max class probability (0.0–1.0).

    Raises:
        RuntimeError if model is not available.
    """
    with _lock:
        if not _holder.loaded:
            raise RuntimeError("ML model not loaded")

        x = features.reshape(1, -1) if features.ndim == 1 else features[:1]
        proba = _holder.model.predict_proba(x)[0]
        predicted_idx = int(np.argmax(proba))
        confidence = float(proba[predicted_idx])

        # Map index to label
        classes = list(_holder.model.classes_)
        status = classes[predicted_idx]

        return status, round(confidence, 3)


def predict_batch(feature_matrix: np.ndarray) -> List[Tuple[str, float]]:
    """
    Predict POI status for a batch of feature vectors.

    Args:
        feature_matrix: numpy array of shape (n, 9).

    Returns:
        List of (status_label, confidence) tuples.
    """
    with _lock:
        if not _holder.loaded:
            raise RuntimeError("ML model not loaded")

        if feature_matrix.shape[0] == 0:
            return []

        probas = _holder.model.predict_proba(feature_matrix)
        classes = list(_holder.model.classes_)

        results = []
        for proba in probas:
            idx = int(np.argmax(proba))
            results.append((classes[idx], round(float(proba[idx]), 3)))

        return results
