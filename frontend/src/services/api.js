import axios from "axios";

const BASE_URL = process.env.REACT_APP_API_URL || "http://127.0.0.1:8000";

export const api = axios.create({
  baseURL: BASE_URL,
  timeout: 120000,
});

// ── Pipeline ──

/** Run the full change-detection pipeline (POST /run). */
export async function runPipeline() {
  const res = await api.post("/run");
  return res.data;
}

/** Run change detection without persisting to DB (POST /detect-changes). */
export async function detectChanges() {
  const res = await api.post("/detect-changes");
  return res.data;
}

/** Fetch cached detection results (GET /detect-changes). No re-run. */
export async function fetchDetectionResults() {
  const res = await api.get("/detect-changes");
  return res.data;
}

export async function getMLStatus() {
  const res = await api.get("/ml-status");
  return res.data;
}

// ── Data Retrieval ──

/** Fetch the most recent pipeline run + results (GET /results). */
export async function fetchLatestResults() {
  const res = await api.get("/results");
  return res.data;
}

/** Fetch all pipeline run metadata (GET /runs). */
export async function fetchRuns() {
  const res = await api.get("/runs");
  return res.data;
}

/** Fetch a single POI by ID, with review data (GET /poi/{id}). */
export async function fetchPOI(poiId) {
  const res = await api.get(`/poi/${encodeURIComponent(poiId)}`);
  return res.data;
}

/** Fetch aggregated dashboard stats (GET /dashboard). */
export async function fetchDashboard() {
  const res = await api.get("/dashboard");
  return res.data;
}

// ── Search ──

/** Search POIs by name, category, or status (GET /search). */
export async function searchPOIs(query, limit = 50) {
  const res = await api.get("/search", { params: { q: query, limit } });
  return res.data;
}

// ── Health ──

/** Check backend health (GET /healthz). */
export async function healthCheck() {
  const res = await api.get("/healthz");
  return res.data;
}

// ── Validation ──

/** Validate a single POI against external source (GET /validate-poi). */
export async function validatePOI(name) {
  const res = await api.get("/validate-poi", { params: { name } });
  return res.data;
}

/** Validate multiple POIs at once (GET /validate-poi-batch). */
export async function validatePOIBatch(names) {
  const res = await api.get("/validate-poi-batch", {
    params: { names: names.join(",") },
  });
  return res.data;
}
