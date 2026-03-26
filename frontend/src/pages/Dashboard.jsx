import React, { useMemo, useState } from "react";
import MapView from "../components/MapView";
import RunDetectionButton from "../components/RunDetectionButton";
import SearchBar from "../components/SearchBar";
import ResultsTable from "../components/ResultsTable";
import { runDetection } from "../services/api";
import "../styles/dashboard.css";

function summarize(rows) {
  const c = { NEW: 0, CLOSED: 0, MODIFIED: 0, UNCHANGED: 0 };
  for (const r of rows || []) c[r.status] = (c[r.status] || 0) + 1;
  return c;
}

export default function Dashboard() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [meta, setMeta] = useState(null);
  const [rows, setRows] = useState([]);
  const [query, setQuery] = useState("");

  const counts = useMemo(() => summarize(rows), [rows]);
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => String(r.name || "").toLowerCase().includes(q));
  }, [rows, query]);

  async function handleRun() {
    setLoading(true);
    setError("");
    try {
      const data = await runDetection();
      setMeta({
        total_osm_pois: data?.total_osm_pois ?? 0,
        total_external_pois: data?.total_external_pois ?? 0,
        matched_pairs: data?.matched_pairs ?? 0,
      });
      setRows(Array.isArray(data?.results) ? data.results : []);
    } catch (e) {
      const msg =
        e?.response?.data?.detail ||
        e?.message ||
        "Failed to run detection. Check backend server.";
      setError(String(msg));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="page">
      <div className="shell">
        <header className="header">
          <div>
            <h1 className="title">Geo-Sentinel Dashboard</h1>
            <p className="subtitle">POI change detection results from your backend pipeline.</p>
          </div>
          <div className="headerActions">
            <RunDetectionButton onClick={handleRun} loading={loading} />
          </div>
        </header>

        <section className="cards">
          <div className="card">
            <div className="cardLabel">OSM POIs</div>
            <div className="cardValue">{meta?.total_osm_pois ?? "—"}</div>
          </div>
          <div className="card">
            <div className="cardLabel">External POIs</div>
            <div className="cardValue">{meta?.total_external_pois ?? "—"}</div>
          </div>
          <div className="card">
            <div className="cardLabel">Matched Pairs</div>
            <div className="cardValue">{meta?.matched_pairs ?? "—"}</div>
          </div>
          <div className="card">
            <div className="cardLabel">Statuses</div>
            <div className="pillRow">
              <span className="pill pill--blue">NEW: {counts.NEW}</span>
              <span className="pill pill--red">CLOSED: {counts.CLOSED}</span>
              <span className="pill pill--orange">MODIFIED: {counts.MODIFIED}</span>
              <span className="pill pill--green">UNCHANGED: {counts.UNCHANGED}</span>
            </div>
          </div>
        </section>

        {error ? (
          <div className="alert" role="alert">
            <div className="alertTitle">Request failed</div>
            <div className="alertBody">{error}</div>
            <div className="alertHint">
              Backend should be running at <code className="codeInline">http://127.0.0.1:8000</code>.
            </div>
          </div>
        ) : null}

        <section className="mainGrid">
          <div className="leftPanel">
            <div className="panelHeader">
              <div>
                <h2 className="h2">Results</h2>
                <p className="muted">
                  Showing {filtered.length} result{filtered.length === 1 ? "" : "s"}.
                </p>
              </div>
            </div>

            <SearchBar value={query} onChange={setQuery} placeholder="Search POI name..." />
            <div className="leftContent">
              <ResultsTable rows={filtered} />
            </div>
          </div>

          <div className="rightPanel">
            <div className="panelHeader">
              <div>
                <h2 className="h2">Map</h2>
                <p className="muted">Markers are color-coded by status.</p>
              </div>
            </div>
            <MapView rows={filtered} />
          </div>
        </section>

        <footer className="footer">
          <span className="muted">
            API: <code className="codeInline">http://127.0.0.1:8000/run</code>
          </span>
        </footer>
      </div>
    </div>
  );
}

