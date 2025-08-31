# backtest/data_loader.py
from __future__ import annotations
import csv
from typing import List, Dict, Any

def load_csv(path: str, limit: int | None = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for i, row in enumerate(r):
            rows.append({
                "ts": row.get("ts") or row.get("datetime") or row.get("date") or "",
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0)),
            })
            if limit and i+1 >= limit:
                break
    return rows
