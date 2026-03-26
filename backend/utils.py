from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


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
        st = path.stat()
        age = time.time() - st.st_mtime
        if ttl_s > 0 and age > ttl_s:
            return CacheResult(hit=False, value=None)
        return CacheResult(hit=True, value=json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return CacheResult(hit=False, value=None)


def cache_set_json(key: str, value: Any) -> None:
    path = _cache_dir() / f"{key}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(stable_json_dumps(value), encoding="utf-8")
    tmp.replace(path)


def env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    return float(v)


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    if v is None:
        return default
    return v


def normalize_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x

