import React, { useCallback, useEffect, useMemo, useState } from "react";
import MapView from "../components/MapView";
import RunDetectionButton from "../components/RunDetectionButton";
import SearchBar from "../components/SearchBar";
import ResultsTable from "../components/ResultsTable";
import { fetchLatestResults, runDetection, searchPOIs } from "../services/api";
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
  const [searchResults, setSearchResults] = useState(null);
  const [searching, setSearching] = useState(false);
  const [lastRun, setLastRun] = useState(null);

  // Pick the right row set depending on whether a search is active
  const displayRows = searchResults !== null ? searchResults : rows;
  const counts = useMemo(() => summarize(displayRows), [displayRows]);

  // ── Load persisted results from DB on mount ──
  useEffect(() => {
    fetchLatestResults()
      .then((data) => {
        if (data && Array.isArray(data.results) && data.results.length > 0) {
          setMeta({
            total_osm_pois: data.total_osm_pois ?? 0,
            total_external_pois: data.total_external_pois ?? 0,
            matched_pairs: data.matched_pairs ?? 0,
          });
          setRows(data.results);
          setLastRun(data.started_at || data.finished_at || null);
        }
      })
      .catch(() => {});
  }, []);

  // ── Debounced backend search ──
  useEffect(() => {
    const q = query.trim();
    if (!q) {
      setSearchResults(null);
      return;
    }
    const timeout = setTimeout(async () => {
      setSearching(true);
      try {
        const data = await searchPOIs(q);
        const matches = Array.isArray(data?.matches) ? data.matches : [];
        setSearchResults(matches);
      } catch {
        // Fall back to client-side filter if search endpoint fails
        const lower = q.toLowerCase();
        setSearchResults(
          rows.filter((r) => String(r.name || "").toLowerCase().includes(lower))
        );
      } finally {
        setSearching(false);
      }
    }, 300);
    return () => clearTimeout(timeout);
  }, [query, rows]);

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
      setLastRun(new Date().toISOString());
      setQuery("");
      setSearchResults(null);
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
            <p className="subtitle">
              POI change detection results from your backend pipeline.
              {lastRun ? (
                <span className="lastRun"> Last run: {new Date(lastRun).toLocaleString()}</span>
              ) : null}
            </p>
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
                  {searching
                    ? "Searching..."
                    : searchResults !== null
                    ? `Found ${displayRows.length} match${displayRows.length === 1 ? "" : "es"} for "${query}"`
                    : `Showing ${displayRows.length} result${displayRows.length === 1 ? "" : "s"}.`}
                </p>
              </div>
            </div>

            <SearchBar
              value={query}
              onChange={setQuery}
              placeholder="Search POIs by name, category, or status..."
            />
            <div className="leftContent">
              <ResultsTable rows={displayRows} />
            </div>
          </div>

          <div className="rightPanel">
            <div className="panelHeader">
              <div>
                <h2 className="h2">Map</h2>
                <p className="muted">Markers are color-coded by status.</p>
              </div>
            </div>
            <MapView rows={displayRows} />
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
