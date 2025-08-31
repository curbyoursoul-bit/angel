# utils/dedupe.py
from __future__ import annotations
import hashlib, time
from typing import Dict, Any

_seen = {}

def hash_order(order: Dict[str, Any]) -> str:
    m = hashlib.sha1()
    m.update(str(sorted(order.items())).encode())
    return m.hexdigest()

def is_duplicate(order: Dict[str, Any], window_ms: int = 1500) -> bool:
    h = hash_order(order)
    now = time.time()*1000
    last = _seen.get(h, 0)
    _seen[h] = now
    return (now - last) < window_ms
