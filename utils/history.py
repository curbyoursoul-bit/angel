# utils/history.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
from typing import Dict, Literal, Optional

import pandas as pd

# IST helpers
IST = timezone(timedelta(hours=5, minutes=30))   # for datetime arithmetic
IST_TZSTR = "Asia/Kolkata"                       # for pandas tz handling

Interval = Literal[
    "ONE_MINUTE",
    "THREE_MINUTE",
    "FIVE_MINUTE",
    "TEN_MINUTE",
    "FIFTEEN_MINUTE",
    "THIRTY_MINUTE",
    "ONE_HOUR",
    "ONE_DAY",
]


@dataclass
class CandleRequest:
    exchange: Literal["NSE", "NFO", "BSE"]
    symboltoken: str
    interval: Interval
    from_dt_ist: datetime
    to_dt_ist: datetime

    def to_payload(self) -> Dict[str, str]:
        """
        Angel expects *naive* local timestamps in "YYYY-MM-DD HH:MM".
        We build times in IST and format without timezone info.
        """
        fd = self.from_dt_ist.astimezone(IST).strftime("%Y-%m-%d %H:%M")
        td = self.to_dt_ist.astimezone(IST).strftime("%Y-%m-%d %H:%M")
        return {
            "exchange": self.exchange,
            "symboltoken": self.symboltoken,
            "interval": self.interval,
            "fromdate": fd,
            "todate": td,
        }


def _now_ist() -> datetime:
    return datetime.now(IST)


def _clean_window(interval: Interval, bars: int) -> timedelta:
    mins = {
        "ONE_MINUTE": 1,
        "THREE_MINUTE": 3,
        "FIVE_MINUTE": 5,
        "TEN_MINUTE": 10,
        "FIFTEEN_MINUTE": 15,
        "THIRTY_MINUTE": 30,
        "ONE_HOUR": 60,
        "ONE_DAY": 24 * 60,
    }[interval]
    return timedelta(minutes=mins * bars)


def _safe_dates(interval: Interval, bars: int) -> tuple[datetime, datetime]:
    now_ist = _now_ist()
    # Avoid requesting the "future" minute
    to_dt = now_ist - timedelta(minutes=1)
    # Add a small buffer to the lookback
    lookback = _clean_window(interval, int(bars * 1.1) + 5)
    from_dt = to_dt - lookback
    if from_dt >= to_dt:
        from_dt = to_dt - timedelta(hours=1)
    return from_dt, to_dt


def _post_once(smart, payload: Dict[str, str]) -> Dict:
    """
    Installed SmartAPI expects a single dict arg: getCandleData(historicDataParams)
    """
    return smart.getCandleData(payload)


def _is_ab1004(resp: Dict) -> bool:
    msg = str(resp.get("message", "")).upper()
    code = str(resp.get("errorcode", "")).upper()
    return (code == "AB1004") or ("SOMETHING WENT WRONG" in msg)


def _resp_to_df(resp: Dict) -> pd.DataFrame:
    """
    Angel returns:
      {"status": True, "data": [[ts, o,h,l,c,v], ...], "message": "SUCCESS", ...}
    """
    if not resp or not resp.get("status") or not resp.get("data"):
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    rows = resp["data"]
    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])

    # Parse time; handle tz-naive vs tz-aware safely.
    ts = pd.to_datetime(df["time"], errors="coerce")
    try:
        # If tz attr is None → tz-naive → localize to IST
        if getattr(ts.dt, "tz", None) is None:
            df["time"] = ts.dt.tz_localize(IST_TZSTR, nonexistent="shift_forward", ambiguous="NaT")
        else:
            # Already tz-aware (likely UTC) → convert to IST
            df["time"] = ts.dt.tz_convert(IST_TZSTR)
    except Exception:
        # Defensive fallback: strip tz then localize to IST
        ts_naive = pd.to_datetime(df["time"], errors="coerce").dt.tz_localize(None)
        df["time"] = ts_naive.dt.tz_localize(IST_TZSTR, nonexistent="shift_forward", ambiguous="NaT")

    # Ensure numeric types
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    return df


def get_recent_candles(
    smart,
    exchange: Literal["NSE", "NFO", "BSE"],
    symboltoken: str,
    interval: Interval,
    bars: int = 300,
    max_retries: int = 5,
) -> pd.DataFrame:
    """
    Fetch recent candles in one shot. Retries briefly on transient AB1004 or other hiccups.
    Always uses the dict-style getCandleData(payload) signature.
    """
    from_dt, to_dt = _safe_dates(interval, bars)
    payload = CandleRequest(
        exchange=exchange,
        symboltoken=str(symboltoken),
        interval=interval,
        from_dt_ist=from_dt,
        to_dt_ist=to_dt,
    ).to_payload()

    last_err: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = _post_once(smart, payload)
            if not isinstance(resp, dict):
                last_err = f"Unexpected response type: {type(resp)}"
            else:
                if resp.get("status"):
                    return _resp_to_df(resp)
                if _is_ab1004(resp):
                    last_err = f"AB1004 on attempt {attempt}: {resp.get('message','')}"
                else:
                    last_err = f"API error on attempt {attempt}: {resp}"
        except Exception as e:
            last_err = f"Exception on attempt {attempt}: {e}"

        # tiny randomized backoff
        import time as _t
        _t.sleep(0.5 + random.random() * 0.8)

        # Slide window back a minute to avoid brushing the current/future minute
        to_dt = to_dt - timedelta(minutes=1)
        from_dt = from_dt - timedelta(minutes=1)
        payload = CandleRequest(
            exchange=exchange,
            symboltoken=str(symboltoken),
            interval=interval,
            from_dt_ist=from_dt,
            to_dt_ist=to_dt,
        ).to_payload()

    # Exhausted retries: return empty frame with context in attrs (for logging upstream)
    df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df.attrs["error"] = last_err or "Unknown error"
    df.attrs["payload"] = payload
    return df
