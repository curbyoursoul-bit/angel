# utils/symbols_cache.py
from __future__ import annotations
import json
from pathlib import Path

CACHE = Path("data/symbols_cache.json")

def load() -> dict:
    try:
        return json.loads(CACHE.read_text())
    except Exception:
        return {}

def save(obj: dict):
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(obj, indent=2))
