from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from scripts.run_pipeline import run_pipeline

load_dotenv()

app = FastAPI(title="Geo Sentinel API")

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


@app.get("/run")
def run():
    try:
        return run_pipeline()
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))