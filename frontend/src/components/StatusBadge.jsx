import React from "react";

const STATUS_STYLES = {
  UNCHANGED: "badge badge--green",
  CLOSED: "badge badge--red",
  NEW: "badge badge--blue",
  MODIFIED: "badge badge--orange",
};

export default function StatusBadge({ status }) {
  const cls = STATUS_STYLES[status] || "badge";
  return <span className={cls}>{status || "—"}</span>;
}

