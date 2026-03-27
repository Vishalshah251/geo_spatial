import React, { useCallback, useState } from "react";
import StatusBadge from "./StatusBadge";
import ValidationBadge from "./ValidationBadge";
import { validatePOI } from "../services/api";

function formatConfidence(c) {
  if (typeof c !== "number") return "—";
  return `${Math.round(c * 100)}%`;
}

function formatRating(r) {
  if (typeof r !== "number") return null;
  return `★${r.toFixed(1)}`;
}

export default function ResultsTable({ rows, onSelectValidation }) {
  const [validating, setValidating] = useState({});   // name → true/false
  const [validations, setValidations] = useState({}); // name → badge string

  const handleValidate = useCallback(async (name) => {
    if (!name) return;
    setValidating((prev) => ({ ...prev, [name]: true }));
    try {
      const data = await validatePOI(name);
      const badge = data?.validation?.badge || "NOT_VERIFIED";
      setValidations((prev) => ({ ...prev, [name]: badge }));
      if (onSelectValidation) onSelectValidation(data);
    } catch {
      setValidations((prev) => ({ ...prev, [name]: "NOT_VERIFIED" }));
    } finally {
      setValidating((prev) => ({ ...prev, [name]: false }));
    }
  }, [onSelectValidation]);

  if (!rows || rows.length === 0) {
    return (
      <div className="emptyState">
        Click <strong>Run Detection</strong> to load results, or search for POIs above.
      </div>
    );
  }

  return (
    <div className="tableScroller">
      <table className="table">
        <thead>
          <tr>
            <th style={{ width: "28%" }}>Name</th>
            <th style={{ width: "10%" }}>Status</th>
            <th style={{ width: "9%" }}>Confidence</th>
            <th style={{ width: "14%" }}>Category</th>
            <th style={{ width: "9%" }}>Distance</th>
            <th style={{ width: "8%" }}>Rating</th>
            <th style={{ width: "7%" }}>Reviews</th>
            <th style={{ width: "15%" }}>Verify</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, idx) => {
            const key = `${r.osm_id || ""}:${r.external_id || r.id || ""}:${idx}`;
            const name = r.name || "";
            const isValidating = validating[name];
            const badge = validations[name];

            return (
              <tr key={key}>
                <td className="nameCell">
                  <div className="nameMain">{name}</div>
                  <div className="nameSub">
                    {r.osm_id ? <span className="chip">OSM</span> : null}
                    {r.external_id ? <span className="chip">EXT</span> : null}
                    {r.source && !r.osm_id && !r.external_id ? (
                      <span className="chip">{r.source.toUpperCase()}</span>
                    ) : null}
                    {r.review_sentiment ? (
                      <span className={`chip chip--${r.review_sentiment}`}>
                        {r.review_sentiment}
                      </span>
                    ) : null}
                  </div>
                </td>
                <td>
                  <StatusBadge status={r.status} />
                </td>
                <td className="mono">{formatConfidence(r.confidence)}</td>
                <td className="mono catCell">{r.category || "—"}</td>
                <td className="mono">
                  {typeof r.distance_m === "number"
                    ? `${Math.round(r.distance_m)} m`
                    : "—"}
                </td>
                <td className="mono">
                  {formatRating(r.review_rating) || "—"}
                </td>
                <td className="mono">
                  {typeof r.review_count === "number" ? r.review_count : "—"}
                </td>
                <td>
                  {badge ? (
                    <ValidationBadge badge={badge} size="small" />
                  ) : (
                    <button
                      className="validateBtn"
                      onClick={() => handleValidate(name)}
                      disabled={isValidating}
                      title={`Validate "${name}" against external API`}
                    >
                      {isValidating ? (
                        <span className="validateBtn__spinner" />
                      ) : (
                        <>
                          <span className="validateBtn__icon">🔍</span>
                          Verify
                        </>
                      )}
                    </button>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
