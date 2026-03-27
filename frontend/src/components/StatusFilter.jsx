import React from "react";

const STATUSES = [
  { key: "NEW", label: "New", cls: "pill--new" },
  { key: "CLOSED", label: "Closed", cls: "pill--closed" },
  { key: "MODIFIED", label: "Modified", cls: "pill--modified" },
  { key: "UNCHANGED", label: "Unchanged", cls: "pill--unchanged" },
];

export default function StatusFilter({ active, onChange, counts = {} }) {
  return (
    <div className="statusFilter">
      <button
        className={`filterBtn ${!active ? "filterBtn--active" : ""}`}
        onClick={() => onChange(null)}
      >
        All
      </button>
      {STATUSES.map((s) => (
        <button
          key={s.key}
          className={`filterBtn ${s.cls} ${active === s.key ? "filterBtn--active" : ""}`}
          onClick={() => onChange(active === s.key ? null : s.key)}
        >
          {s.label} ({counts[s.key] || 0})
        </button>
      ))}
    </div>
  );
}
