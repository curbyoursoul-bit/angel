# backtest/data.py
from __future__ import annotations
from pathlib import Path
from typing import Optional
import re

import pandas as pd

_COL_ALIASES = {
    "timestamp": "timestamp",
    "time": "timestamp",
    "date": "timestamp",
    "datetime": "timestamp",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "vol": "volume",
}

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # lower-case/trim/map aliases
    rename = {}
    for c in df.columns:
        k = c.strip().lower()
        if k in _COL_ALIASES:
            rename[c] = _COL_ALIASES[k]
    df = df.rename(columns=rename)

    # required OHLC
    needed = {"open", "high", "low", "close"}
    if not needed.issubset(set(df.columns)):
        raise ValueError(f"CSV must contain columns at least {sorted(needed)}; got {list(df.columns)}")

    # timestamp handling
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], utc=False, errors="coerce")
        if ts.isna().any():
            raise ValueError("Could not parse some timestamps in 'timestamp' column.")
        df = df.drop(columns=["timestamp"])
        df.index = pd.DatetimeIndex(ts, name="timestamp")
    else:
        # assume first column is datetime-like
        first = pd.to_datetime(df.iloc[:, 0], utc=False, errors="coerce")
        if first.isna().any():
            raise ValueError("Could not infer a timestamp column; provide 'timestamp' explicitly.")
        df = df.drop(columns=[df.columns[0]])
        df.index = pd.DatetimeIndex(first, name="timestamp")

    # ensure numeric dtypes
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "volume" not in df.columns:
        df["volume"] = 0.0

    df = df.sort_index()
    df = df[["open", "high", "low", "close", "volume"]]
    return df.dropna(subset=["open", "high", "low", "close"])

def load_ohlcv_csv(csv_path: str | Path) -> pd.DataFrame:
    """Load OHLCV CSV into a datetime-indexed DataFrame with columns:
    open, high, low, close, volume
    """
    df = pd.read_csv(csv_path)
    return _normalize_cols(df)

def _to_pandas_rule(tf: str) -> str:
    tf = (tf or "").strip().lower()
    if tf in ("day", "1d", "d"):
        return "1D"
    if tf.endswith("min") or tf.endswith("m"):
        n = tf.replace("min", "").replace("m", "")
        return f"{int(n)}min"  # use 'min' (not 'T') to avoid deprecation warning
    if tf.endswith("h") or tf.endswith("hr") or tf.endswith("hour"):
        n = tf.rstrip("h").rstrip("r").replace("ou", "")
        n = "".join([ch for ch in n if ch.isdigit()]) or "1"
        return f"{int(n)}H"
    return tf  # assume a valid pandas offset alias

def _normalize_rule(rule: str) -> str:
    """
    Normalize pandas offset alias:
      - '15T', '15t', '15m'  -> '15min'
      - leave '1H','1D', etc. as-is
    """
    r = rule.strip()
    r_low = r.lower()
    r_low = re.sub(r"^(\d+)\s*(t|m|min)$", r"\1min", r_low)
    return r_low

def resample(df: pd.DataFrame, timeframe: Optional[str]) -> pd.DataFrame:
    """Resample OHLCV to a new timeframe."""
    if not timeframe:
        return df

    # make a tz-naive copy to avoid tz conversion overhead during resample
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_convert(None)

    rule = _to_pandas_rule(timeframe)
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    out = df.resample(rule, label="right", closed="right").agg(agg).dropna()
    return out
