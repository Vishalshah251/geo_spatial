import React, { useMemo } from "react";
import { CircleMarker, MapContainer, Popup, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";

const SG_CENTER = [1.3521, 103.8198];

function colorForStatus(status) {
  switch (status) {
    case "UNCHANGED":
      return "#2ecc71";
    case "CLOSED":
      return "#ff4d4f";
    case "NEW":
      return "#2f80ed";
    case "MODIFIED":
      return "#f2994a";
    default:
      return "#cbd5e1";
  }
}

export default function MapView({ rows }) {
  const points = useMemo(() => {
    return (rows || [])
      .filter((r) => typeof r.lat === "number" && typeof r.lon === "number")
      .map((r) => ({
        key: `${r.osm_id || ""}:${r.external_id || ""}:${r.name}`,
        name: r.name,
        status: r.status,
        lat: r.lat,
        lon: r.lon,
      }));
  }, [rows]);

  return (
    <div className="mapWrap">
      <MapContainer center={SG_CENTER} zoom={12} scrollWheelZoom className="map">
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {points.map((p) => (
          <CircleMarker
            key={p.key}
            center={[p.lat, p.lon]}
            radius={7}
            pathOptions={{
              color: colorForStatus(p.status),
              fillColor: colorForStatus(p.status),
              fillOpacity: 0.9,
              weight: 2,
            }}
          >
            <Popup>
              <div style={{ fontWeight: 700 }}>{p.name}</div>
              <div style={{ marginTop: 4 }}>Status: {p.status}</div>
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
    </div>
  );
}

