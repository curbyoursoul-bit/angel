# risk/trailing_stop.py
from __future__ import annotations
from typing import Optional
def trail_price(entry: float, current: float, atr: float, k: float=2.0, side: str="LONG") -> Optional[float]:
    if atr<=0: return None
    if side.upper()=="LONG":
        return max(current - k*atr, entry)
    return min(current + k*atr, entry)
