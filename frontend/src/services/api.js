import axios from "axios";

const BASE_URL = "http://127.0.0.1:8000";

export const api = axios.create({
  baseURL: BASE_URL,
  timeout: 120000,
});

export async function runDetection(offline = true) {
  const res = await api.get("/run", { params: { offline } });
  return res.data;
}

export async function fetchLatestResults() {
  const res = await api.get("/results");
  return res.data;
}

export async function fetchRuns() {
  const res = await api.get("/runs");
  return res.data;
}

export async function searchPOIs(query) {
  const res = await api.get("/search", { params: { q: query } });
  return res.data;
}

export async function healthCheck() {
  const res = await api.get("/healthz");
  return res.data;
}
