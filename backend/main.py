from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from backend.database import create_tables, get_all_runs, get_latest_run, get_run_by_id, search_pois, test_database_connection
from scripts.run_pipeline import run_pipeline

load_dotenv()


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Ensure DB tables exist on startup
    create_tables()
    # Startup: DB connectivity should never prevent server boot.
    ok, err = test_database_connection(allow_fallback=True)
    application.state.db_ok = ok
    application.state.db_error = err
    yield
    # Shutdown (nothing to clean up)


app = FastAPI(title="Geo Sentinel API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "Geo Sentinel Running"}


@app.get("/healthz")
def healthz():
    return {"status": "ok", "db_ok": app.state.db_ok, "db_error": app.state.db_error}


@app.get("/run")
def run(offline: bool = True):
    try:
        return run_pipeline(offline=offline)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/results")
def results_latest():
    """Return the most recent pipeline run + results from the DB."""
    data = get_latest_run()
    if not data:
        return {"run_id": None, "total_osm_pois": 0, "total_external_pois": 0,
                "matched_pairs": 0, "total_results": 0, "results": []}
    return data


@app.get("/results/{run_id}")
def results_by_run(run_id: int):
    """Return a specific pipeline run + results from the DB."""
    data = get_run_by_id(run_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return data


@app.get("/runs")
def runs_list():
    """Return history of all pipeline runs (metadata only)."""
    return get_all_runs()


@app.get("/search")
def search(q: str = "", limit: int = 50):
    """
    Interpret a user query and return best matching POIs / results from DB.
    Only returns data that exists in the ingested pipeline — never hallucinated.
    """
    if not q.strip():
        return {"query": q, "matches": []}
    return search_pois(query=q, limit=limit)