# execution/audit.py
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime

LOG = Path("data/orders_audit.jsonl")

def log(event: str, payload: dict):
    rec = {"ts": datetime.utcnow().isoformat(), "event": event, "data": payload}
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
