"""
Shared utilities — logging, caching, env helpers, text normalisation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
logging.basicConfig(format=_LOG_FORMAT, level=logging.INFO)
logger = logging.getLogger("geo_sentinel")


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return default if v is None else v


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


# ---------------------------------------------------------------------------
# File-system cache
# ---------------------------------------------------------------------------

def _stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class CacheResult:
    hit: bool
    value: Optional[Any]


def _cache_dir() -> Path:
    d = Path(os.environ.get("GEO_SENTINEL_CACHE_DIR", ".cache/geo_sentinel"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def cache_get_json(key: str, ttl_s: int) -> CacheResult:
    path = _cache_dir() / f"{key}.json"
    if not path.exists():
        return CacheResult(hit=False, value=None)
    try:
        age = time.time() - path.stat().st_mtime
        if ttl_s > 0 and age > ttl_s:
            return CacheResult(hit=False, value=None)
        return CacheResult(hit=True, value=json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return CacheResult(hit=False, value=None)


def cache_set_json(key: str, value: Any) -> None:
    path = _cache_dir() / f"{key}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(_stable_json_dumps(value), encoding="utf-8")
    tmp.replace(path)
