import React, { useCallback, useEffect, useState } from "react";
import ValidationBadge from "./ValidationBadge";
import { validatePOI } from "../services/api";

/**
 * ValidationPanel — shows real-world validation status for a searched POI.
 *
 * Automatically validates the top search result against the live Geoapify API
 * and displays a side-by-side comparison of internal vs external status.
 */
export default function ValidationPanel({ poiName, autoValidate = true }) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  const doValidate = useCallback(async (name) => {
    if (!name || !name.trim()) return;
    setLoading(true);
    setError("");
    setResult(null);
    try {
      const data = await validatePOI(name.trim());
      setResult(data);
    } catch (e) {
      setError(
        e?.response?.data?.detail || e?.message || "Validation failed"
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (autoValidate && poiName) {
      doValidate(poiName);
    } else {
      setResult(null);
      setError("");
    }
  }, [poiName, autoValidate, doValidate]);

  if (!poiName) return null;

  return (
    <div className="validationPanel">
      <div className="validationPanel__header">
        <div className="validationPanel__titleRow">
          <span className="validationPanel__icon">🔍</span>
          <h3 className="validationPanel__title">Real-World Validation</h3>
        </div>
        {!autoValidate && !result && !loading && (
          <button
            className="btn btn--sm"
            onClick={() => doValidate(poiName)}
            disabled={loading}
          >
            Validate
          </button>
        )}
      </div>

      {loading && (
        <div className="validationPanel__loading">
          <div className="validationPanel__skeleton" />
          <div className="validationPanel__skeleton validationPanel__skeleton--short" />
          <p className="validationPanel__loadingText">
            Querying Geoapify API for &ldquo;{poiName}&rdquo;…
          </p>
        </div>
      )}

      {error && (
        <div className="validationPanel__error">
          <span>⚠ {error}</span>
        </div>
      )}

      {result && !loading && (
        <div className="validationPanel__body">
          {/* Badge row */}
          <div className="validationPanel__badgeRow">
            <ValidationBadge badge={result.validation?.badge} />
            <span className="validationPanel__source">
              {result.validation?.source}
            </span>
          </div>

          {/* Comparison grid */}
          <div className="validationPanel__grid">
            {/* Internal */}
            <div className="validationPanel__col">
              <div className="validationPanel__colLabel">
                <span className="validationPanel__dot validationPanel__dot--internal" />
                Our System
              </div>
              {result.internal ? (
                <>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Name</span>
                    <span className="validationPanel__fieldValue">
                      {result.internal.name || "—"}
                    </span>
                  </div>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Status</span>
                    <span className={`validationPanel__status validationPanel__status--${(result.internal.status || "").toLowerCase()}`}>
                      {result.internal.status || "—"}
                    </span>
                  </div>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Confidence</span>
                    <span className="validationPanel__fieldValue">
                      {typeof result.internal.confidence === "number"
                        ? `${Math.round(result.internal.confidence * 100)}%`
                        : "—"}
                    </span>
                  </div>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Category</span>
                    <span className="validationPanel__fieldValue">
                      {result.internal.category || "—"}
                    </span>
                  </div>
                </>
              ) : (
                <div className="validationPanel__noData">No internal data</div>
              )}
            </div>

            {/* Divider */}
            <div className="validationPanel__divider">
              <span className="validationPanel__vs">VS</span>
            </div>

            {/* External */}
            <div className="validationPanel__col">
              <div className="validationPanel__colLabel">
                <span className="validationPanel__dot validationPanel__dot--external" />
                External Source
              </div>
              {result.external ? (
                <>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Name</span>
                    <span className="validationPanel__fieldValue">
                      {result.external.name || "—"}
                    </span>
                  </div>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Status</span>
                    <span className={`validationPanel__status validationPanel__status--${(result.validation?.external_status || "").toLowerCase()}`}>
                      {result.validation?.external_status || "—"}
                    </span>
                  </div>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Similarity</span>
                    <span className="validationPanel__fieldValue">
                      {typeof result.external.name_similarity === "number"
                        ? `${Math.round(result.external.name_similarity * 100)}%`
                        : "—"}
                    </span>
                  </div>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Match Conf.</span>
                    <span className="validationPanel__fieldValue">
                      {typeof result.external.match_confidence === "number"
                        ? `${Math.round(result.external.match_confidence * 100)}%`
                        : "—"}
                    </span>
                  </div>
                  <div className="validationPanel__field">
                    <span className="validationPanel__fieldLabel">Distance</span>
                    <span className="validationPanel__fieldValue">
                      {typeof result.external.distance_m === "number"
                        ? `${Math.round(result.external.distance_m)} m`
                        : "—"}
                    </span>
                  </div>
                  {result.external.address && (
                    <div className="validationPanel__field">
                      <span className="validationPanel__fieldLabel">Address</span>
                      <span className="validationPanel__fieldValue validationPanel__fieldValue--wrap">
                        {result.external.address}
                      </span>
                    </div>
                  )}
                </>
              ) : (
                <div className="validationPanel__noData">
                  Not found in external source
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
