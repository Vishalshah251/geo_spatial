import React from "react";

const BADGE_CONFIG = {
  VALIDATED: {
    emoji: "✅",
    label: "Validated",
    className: "vBadge vBadge--validated",
  },
  CONFLICT: {
    emoji: "⚠️",
    label: "Conflict",
    className: "vBadge vBadge--conflict",
  },
  NOT_VERIFIED: {
    emoji: "❌",
    label: "Not Verified",
    className: "vBadge vBadge--not-verified",
  },
};

export default function ValidationBadge({ badge, size = "normal" }) {
  const config = BADGE_CONFIG[badge] || BADGE_CONFIG.NOT_VERIFIED;
  const sizeClass = size === "small" ? "vBadge--sm" : "";

  return (
    <span className={`${config.className} ${sizeClass}`} title={config.label}>
      <span className="vBadge__emoji">{config.emoji}</span>
      <span className="vBadge__label">{config.label}</span>
    </span>
  );
}
