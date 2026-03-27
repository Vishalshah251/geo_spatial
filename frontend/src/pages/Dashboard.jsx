import React, { useCallback, useEffect, useMemo, useState } from "react";
import MapView from "../components/MapView";
import RunDetectionButton from "../components/RunDetectionButton";
import SearchBar from "../components/SearchBar";
import ResultsTable from "../components/ResultsTable";
import StatusFilter from "../components/StatusFilter";
import ValidationPanel from "../components/ValidationPanel";
import {
  detectChanges,
  fetchDashboard,
  getMLStatus,
  searchPOIs,
} from "../services/api";
import "../styles/dashboard.css";

const ALL_STATUSES = ["NEW", "CLOSED", "MODIFIED", "UNCHANGED"];

function summarize(rows) {
  const c = { NEW: 0, CLOSED: 0, MODIFIED: 0, UNCHANGED: 0 };
  for (const r of rows || []) c[r.status] = (c[r.status] || 0) + 1;
  return c;
}

export default function Dashboard() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [dashStats, setDashStats] = useState(null);
  const [meta, setMeta] = useState(null);
  const [rows, setRows] = useState([]);
  const [query, setQuery] = useState("");
  const [searchResults, setSearchResults] = useState(null);
  const [searching, setSearching] = useState(false);
  const [lastRun, setLastRun] = useState(null);
  const [statusFilter, setStatusFilter] = useState(null); // null = all

  // Validation state
  const [validationTarget, setValidationTarget] = useState(null); // POI name to validate
  const [selectedValidation, setSelectedValidation] = useState(null); // full result from table click

  // ML State
  const [mlStatus, setMlStatus] = useState(null);

  // ── Filtered rows (status + search) ──
  const baseRows = searchResults !== null ? searchResults : rows;
  const displayRows = useMemo(() => {
    if (!statusFilter) return baseRows;
    return baseRows.filter((r) => r.status === statusFilter);
  }, [baseRows, statusFilter]);
  const counts = useMemo(() => summarize(baseRows), [baseRows]);

  // ── Load lightweight dashboard stats on mount (NO bulk data) ──
  useEffect(() => {
    fetchDashboard()
      .then((data) => setDashStats(data))
      .catch(() => {});

    getMLStatus()
      .then((data) => setMlStatus(data))
      .catch(() => {});
  }, []);

  // ── Debounced backend search ──
  useEffect(() => {
    const q = query.trim();
    if (!q) {
      setSearchResults(null);
      setValidationTarget(null);
      setSelectedValidation(null);
      return;
    }
    const timeout = setTimeout(async () => {
      setSearching(true);
      try {
        const data = await searchPOIs(q);
        const matches = Array.isArray(data?.matches) ? data.matches : [];
        setSearchResults(matches);

        // Auto-validate the top result
        if (matches.length > 0 && matches[0].name) {
          setValidationTarget(matches[0].name);
        } else {
          setValidationTarget(null);
        }
        setSelectedValidation(null);
      } catch {
        // Fallback: filter from current rows (which already have status)
        const lower = q.toLowerCase();
        const filtered = rows.filter((r) =>
          String(r.name || "").toLowerCase().includes(lower)
        );
        setSearchResults(filtered);
        if (filtered.length > 0 && filtered[0].name) {
          setValidationTarget(filtered[0].name);
        }
      } finally {
        setSearching(false);
      }
    }, 300);
    return () => clearTimeout(timeout);
  }, [query, rows]);

  // ── Run detection ──
  async function handleRun() {
    setLoading(true);
    setError("");
    try {
      const data = await detectChanges();
      setMeta({
        total_osm_pois: data?.total_osm_pois ?? 0,
        total_external_pois: data?.total_external_pois ?? 0,
        matched_pairs: data?.matched_pairs ?? 0,
      });
      const allResults = Array.isArray(data?.results) ? data.results : [];
      setRows(allResults.slice(0, 100));  // cap at 100 records
      setLastRun(new Date().toISOString());
      setQuery("");
      setSearchResults(null);
      setStatusFilter(null);
      setValidationTarget(null);
      setSelectedValidation(null);
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

  // ── Handle validation selection from table row ──
  const handleSelectValidation = useCallback((data) => {
    setSelectedValidation(data);
    if (data?.query) setValidationTarget(data.query);
  }, []);

  return (
    <div className="page">
      <div className="shell">
        <header className="header">
          <div>
            <h1 className="title">
              <span className="titleIcon">🛰️</span>
              Geo-Sentinel Dashboard
              {mlStatus?.available && (
                <span style={{
                  marginLeft: '12px',
                  fontSize: '11px',
                  padding: '4px 8px',
                  borderRadius: '12px',
                  background: 'rgba(155, 89, 182, 0.2)',
                  color: '#e056fd',
                  border: '1px solid rgba(155, 89, 182, 0.4)',
                  verticalAlign: 'middle',
                  fontWeight: 600,
                  letterSpacing: '0.5px'
                }} title={`ML Powered Accuracy: ${Math.round((mlStatus.accuracy || 0) * 100)}%`}>
                  ✨ ML POWERED
                </span>
              )}
            </h1>
            <p className="subtitle">
              POI change detection — powered by CSV data from OSM & Geoapify.
              {lastRun ? (
                <span className="lastRun">
                  {" "}Last run: {new Date(lastRun).toLocaleString()}
                </span>
              ) : null}
            </p>
          </div>
          <div className="headerActions">
            <RunDetectionButton onClick={handleRun} loading={loading} />
          </div>
        </header>

        {/* ── Summary Cards ── */}
        <section className="cards">
          <div className="card">
            <div className="cardLabel">OSM POIs</div>
            <div className="cardValue">
              {dashStats?.data?.osm_pois?.toLocaleString() ?? meta?.total_osm_pois?.toLocaleString() ?? "—"}
            </div>
          </div>
          <div className="card">
            <div className="cardLabel">Geoapify POIs</div>
            <div className="cardValue">
              {dashStats?.data?.geoapify_pois?.toLocaleString() ?? meta?.total_external_pois?.toLocaleString() ?? "—"}
            </div>
          </div>
          <div className="card">
            <div className="cardLabel">Reviews</div>
            <div className="cardValue">
              {dashStats?.reviews?.total?.toLocaleString() ?? "—"}
              {dashStats?.reviews?.avg_rating ? (
                <span className="cardSub"> (avg ★{dashStats.reviews.avg_rating})</span>
              ) : null}
            </div>
          </div>
          <div className="card">
            <div className="cardLabel">Changes Detected</div>
            <div className="pillRow">
              {ALL_STATUSES.map((s) => (
                <button
                  key={s}
                  className={`pill pill--${s.toLowerCase()} ${statusFilter === s ? "pill--active" : ""}`}
                  onClick={() => setStatusFilter(statusFilter === s ? null : s)}
                  title={`Filter by ${s}`}
                >
                  {s}: {counts[s]}
                </button>
              ))}
            </div>
          </div>
        </section>

        {/* ── Error Alert ── */}
        {error ? (
          <div className="alert" role="alert">
            <div className="alertTitle">⚠ Request failed</div>
            <div className="alertBody">{error}</div>
            <div className="alertHint">
              Backend should be running at{" "}
              <code className="codeInline">http://127.0.0.1:8000</code>.
            </div>
          </div>
        ) : null}

        {/* ── Validation Panel (appears during search) ── */}
        {validationTarget && (
          <ValidationPanel
            poiName={validationTarget}
            autoValidate={false}
          />
        )}

        {/* ── Main Grid ── */}
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
                    : statusFilter
                    ? `${displayRows.length} ${statusFilter} result${displayRows.length === 1 ? "" : "s"}`
                    : `${displayRows.length} result${displayRows.length === 1 ? "" : "s"}`}
                </p>
              </div>
            </div>

            <SearchBar
              value={query}
              onChange={setQuery}
              placeholder="Search POIs by name, category, or status..."
            />
            <StatusFilter
              active={statusFilter}
              onChange={setStatusFilter}
              counts={counts}
            />
            <div className="leftContent">
              {displayRows.length === 0 && !searching && !loading ? (
              <div className="emptyState">
                <div className="emptyStateIcon">🔍</div>
                <div className="emptyStateText">
                  Search for a POI or click <strong>Run Detection</strong> to load results.
                </div>
              </div>
            ) : (
              <ResultsTable
                rows={displayRows}
                onSelectValidation={handleSelectValidation}
              />
            )}
            </div>
          </div>

          <div className="rightPanel">
            <div className="panelHeader">
              <div>
                <h2 className="h2">Map</h2>
                <p className="muted">
                  Markers are color-coded by status.
                  {statusFilter ? ` Showing: ${statusFilter}` : " Showing all."}
                </p>
              </div>
            </div>
            <MapView rows={displayRows} />
          </div>
        </section>

        {/* ── Sentiment Summary ── */}
        {dashStats?.reviews?.sentiment ? (
          <section className="cards sentimentBar">
            <div className="card card--compact">
              <div className="cardLabel">Positive Reviews</div>
              <div className="cardValue cardValue--green">
                {dashStats.reviews.sentiment.positive?.toLocaleString() ?? 0}
              </div>
            </div>
            <div className="card card--compact">
              <div className="cardLabel">Neutral Reviews</div>
              <div className="cardValue cardValue--muted">
                {dashStats.reviews.sentiment.neutral?.toLocaleString() ?? 0}
              </div>
            </div>
            <div className="card card--compact">
              <div className="cardLabel">Negative Reviews</div>
              <div className="cardValue cardValue--red">
                {dashStats.reviews.sentiment.negative?.toLocaleString() ?? 0}
              </div>
            </div>
          </section>
        ) : null}

        <footer className="footer">
          <span className="muted">
            Geo Sentinel v2 — CSV-driven pipeline · Real-world validated via Geoapify
          </span>
          <span className="muted">
            API: <code className="codeInline">http://127.0.0.1:8000</code>
          </span>
        </footer>
      </div>
    </div>
  );
}
