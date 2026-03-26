import React from "react";
import StatusBadge from "./StatusBadge";

function formatConfidence(c) {
  if (typeof c !== "number") return "—";
  return `${Math.round(c * 100)}%`;
}

export default function ResultsTable({ rows }) {
  if (!rows || rows.length === 0) {
    return (
      <div className="emptyState">
        Click <strong>Run Detection</strong> to load results.
      </div>
    );
  }

  return (
    <div className="tableScroller">
      <table className="table">
        <thead>
          <tr>
            <th style={{ width: "42%" }}>Name</th>
            <th style={{ width: "14%" }}>Status</th>
            <th style={{ width: "12%" }}>Confidence</th>
            <th style={{ width: "18%" }}>Category</th>
            <th style={{ width: "14%" }}>Distance</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, idx) => (
            <tr key={`${r.osm_id || ""}:${r.external_id || ""}:${idx}`}>
              <td className="nameCell">
                <div className="nameMain">{r.name}</div>
                <div className="nameSub">
                  <span className={`chip ${r.osm_id ? "" : "chip--ghost"}`}>OSM</span>
                  <span className={`chip ${r.external_id ? "" : "chip--ghost"}`}>EXT</span>
                </div>
              </td>
              <td>
                <StatusBadge status={r.status} />
              </td>
              <td className="mono">{formatConfidence(r.confidence)}</td>
              <td className="mono">{r.category || "—"}</td>
              <td className="mono">
                {typeof r.distance_m === "number" ? `${Math.round(r.distance_m)} m` : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

