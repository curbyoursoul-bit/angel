# utils/market_hours.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Iterable, Tuple, Optional
import os

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    # Fallback: naive localtime (less accurate)
    ZoneInfo = None  # type: ignore

from config import (
    TIMEZONE_IST as _TZ_NAME,
    MARKET_OPEN_HH, MARKET_OPEN_MM,
    MARKET_CLOSE_HH, MARKET_CLOSE_MM,
)

IST = ZoneInfo(_TZ_NAME) if ZoneInfo else None  # exported for others

# Simple holiday hook (extend/replace from a file if you like)
# Format: "YYYY-MM-DD"
_HOLIDAYS: set[str] = set(map(str, [
    # add official NSE holidays here, e.g.
    # "2025-01-26", "2025-03-14", ...
]))

@dataclass(frozen=True)
class MarketWindow:
    open: time = time(MARKET_OPEN_HH, MARKET_OPEN_MM)
    close: time = time(MARKET_CLOSE_HH, MARKET_CLOSE_MM)

WINDOW = MarketWindow()

def _now_ist() -> datetime:
    if IST:
        return datetime.now(IST)
    return datetime.now()

def _is_weekday(dt: datetime) -> bool:
    # Monday=0 ... Sunday=6
    return dt.weekday() <= 4

def _is_holiday(dt: datetime) -> bool:
    return dt.strftime("%Y-%m-%d") in _HOLIDAYS

def is_market_open(now: Optional[datetime] = None) -> bool:
    """
    NSE cash/options regular hours (Mon–Fri 09:15–15:30 IST by default).
    Honors env BYPASS_MARKET_HOURS=true for testing.
    """
    if str(os.getenv("BYPASS_MARKET_HOURS","")).strip().lower() in {"1","true","yes","on"}:
        return True
    now = now or _now_ist()
    if not _is_weekday(now) or _is_holiday(now):
        return False
    t = now.timetz() if hasattr(now, "timetz") else now.time()
    return (t >= WINDOW.open) and (t <= WINDOW.close)

def next_session_bounds(now: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """Return (next_open_dt, next_close_dt) in IST."""
    now = now or _now_ist()
    cur = now
    for _ in range(10):  # search up to next two weeks
        d = cur.date()
        open_dt = datetime(d.year, d.month, d.day, WINDOW.open.hour, WINDOW.open.minute, tzinfo=IST)
        close_dt = datetime(d.year, d.month, d.day, WINDOW.close.hour, WINDOW.close.minute, tzinfo=IST)
        if _is_weekday(open_dt) and not _is_holiday(open_dt):
            if now <= close_dt:
                if now <= open_dt:
                    return open_dt, close_dt
                # already inside session; return today’s bounds
                return open_dt, close_dt
        cur += timedelta(days=1)
    # fallback
    d = now.date()
    return (
        datetime(d.year, d.month, d.day, WINDOW.open.hour, WINDOW.open.minute, tzinfo=IST),
        datetime(d.year, d.month, d.day, WINDOW.close.hour, WINDOW.close.minute, tzinfo=IST),
    )
