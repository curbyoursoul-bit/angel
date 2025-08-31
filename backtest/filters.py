# backtest/filters.py
from __future__ import annotations
import pandas as pd

def session_mask(index: pd.DatetimeIndex, start: str, end: str, tz: str = "Asia/Kolkata") -> pd.Series:
    """
    Keep bars whose LOCAL time-of-day is within [start, end].
    Works with tz-aware or naive indices.
    """
    if index.tz is None:
        idx_local = index.tz_localize(tz)
    else:
        idx_local = index.tz_convert(tz)
    t = idx_local.time
    start_h, start_m = map(int, start.split(":"))
    end_h, end_m = map(int, end.split(":"))
    start_t = pd.Timestamp(1, 1, 1, start_h, start_m).time()
    end_t   = pd.Timestamp(1, 1, 1, end_h, end_m).time()
    if start_t <= end_t:
        keep = (t >= start_t) & (t <= end_t)
    else:
        # crosses midnight
        keep = (t >= start_t) | (t <= end_t)
    return pd.Series(keep, index=index)
