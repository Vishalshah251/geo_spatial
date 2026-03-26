import React from "react";

export default function RunDetectionButton({ onClick, loading }) {
  return (
    <button className="btn" type="button" onClick={onClick} disabled={loading}>
      {loading ? "Running..." : "Run Detection"}
    </button>
  );
}

