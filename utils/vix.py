# utils/vix.py
from __future__ import annotations
import json
import urllib.request, urllib.error

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
_URL = "https://www.nseindia.com/api/allIndices?async=true"

def get_india_vix(timeout: int = 10) -> float | None:
    """Fetch current India VIX (best-effort). Returns float or None."""
    try:
        req = urllib.request.Request(_URL, headers={
            "User-Agent": _UA,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(body)
    except Exception:
        return None

    for idx in data.get("data", []):
        name = (idx.get("index") or idx.get("indexSymbol") or "").strip().upper()
        if "VIX" in name:
            try:
                return float(idx.get("last"))
            except Exception:
                pass
    return None
