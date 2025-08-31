# utils/indicators.py
from __future__ import annotations
from typing import Tuple
import numpy as np
import pandas as pd

# -------- helpers ------------------------------------------------------------

def _as_series(x: pd.Series | pd.Array | np.ndarray, name: str = "") -> pd.Series:
    s = pd.Series(x, copy=False)
    if name and not s.name:
        s.name = name
    return pd.to_numeric(s, errors="coerce")

def _rolling_std(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=max(1, n // 2)).std(ddof=0)

# -------- basics -------------------------------------------------------------

def ema(s: pd.Series, span: int) -> pd.Series:
    """Exponential moving average (adjust=False)."""
    s = _as_series(s, "ema_src")
    return s.ewm(span=span, adjust=False).mean()

def sma(s: pd.Series, n: int) -> pd.Series:
    """Simple moving average."""
    s = _as_series(s, "sma_src")
    return s.rolling(n, min_periods=max(1, n // 2)).mean()

def std(s: pd.Series, n: int) -> pd.Series:
    """Rolling population std (ddof=0)."""
    s = _as_series(s, "std_src")
    return _rolling_std(s, n)

def bollinger(close: pd.Series, n: int = 20, k: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Returns (middle, upper, lower) bands.
    """
    c = _as_series(close, "close")
    m = sma(c, n)
    sd = _rolling_std(c, n)
    upper = m + k * sd
    lower = m - k * sd
    return m, upper, lower

def zscore(s: pd.Series, n: int = 20) -> pd.Series:
    """Rolling z-score with safe divide-by-zero handling."""
    x = _as_series(s, "z_src")
    m = sma(x, n)
    sd = _rolling_std(x, n)
    z = np.divide((x - m), sd, out=np.full_like(x, np.nan, dtype="float64"), where=(sd != 0))
    return pd.Series(z, index=x.index, name="zscore")

# -------- popular extras -----------------------------------------------------

def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    """Wilder's RSI."""
    c = _as_series(close, "close")
    delta = c.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    # Wilder’s smoothing via ewm(alpha=1/n, adjust=False)
    avg_gain = gain.ewm(alpha=1.0 / n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / n, adjust=False).mean()

    rs = np.divide(avg_gain, avg_loss, out=np.full_like(avg_gain, np.nan, dtype="float64"), where=(avg_loss != 0))
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return pd.Series(rsi, index=c.index, name="rsi")

def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    """Average True Range (Wilder)."""
    h = _as_series(high, "high")
    l = _as_series(low, "low")
    c = _as_series(close, "close")
    prev_close = c.shift(1)

    tr = pd.concat(
        [(h - l).abs(),
         (h - prev_close).abs(),
         (l - prev_close).abs()],
        axis=1
    ).max(axis=1)

    atr = tr.ewm(alpha=1.0 / n, adjust=False).mean()
    return atr.rename("atr")

def vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    """Session VWAP from typical price."""
    h = _as_series(high, "high")
    l = _as_series(low, "low")
    c = _as_series(close, "close")
    v = _as_series(volume, "volume").fillna(0)

    tp = (h + l + c) / 3.0
    cum_pv = (tp * v).cumsum()
    cum_v = v.cumsum().replace(0, np.nan)
    out = cum_pv / cum_v
    return out.rename("vwap")

def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """MACD line, Signal line, Histogram."""
    c = _as_series(close, "close")
    ema_fast = ema(c, fast)
    ema_slow = ema(c, slow)
    line = (ema_fast - ema_slow).rename("macd")
    sig = ema(line, signal).rename("signal")
    hist = (line - sig).rename("hist")
    return line, sig, hist

def supertrend(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 10, multiplier: float = 3.0) -> pd.Series:
    """
    Classic SuperTrend implementation (returns the SuperTrend line).
    """
    h = _as_series(high, "high")
    l = _as_series(low, "low")
    c = _as_series(close, "close")

    # ATR-based bands
    _atr = atr(h, l, c, n=period)
    hl2 = (h + l) / 2.0
    upperband = (hl2 + multiplier * _atr).rename("upperband")
    lowerband = (hl2 - multiplier * _atr).rename("lowerband")

    st = pd.Series(index=c.index, dtype="float64", name="supertrend")
    dir_up = pd.Series(index=c.index, dtype="bool")

    # initialize
    st.iloc[0] = upperband.iloc[0]
    dir_up.iloc[0] = True

    # iterate (vectorized ST needs careful state; loop is clearer and fast enough)
    for i in range(1, len(c)):
        prev_st = st.iloc[i - 1]
        prev_dir_up = dir_up.iloc[i - 1]

        cur_upper = max(upperband.iloc[i], prev_st) if prev_dir_up else upperband.iloc[i]
        cur_lower = min(lowerband.iloc[i], prev_st) if not prev_dir_up else lowerband.iloc[i]

        if c.iloc[i] > cur_upper:
            st.iloc[i] = cur_lower
            dir_up.iloc[i] = True
        elif c.iloc[i] < cur_lower:
            st.iloc[i] = cur_upper
            dir_up.iloc[i] = False
        else:
            st.iloc[i] = cur_lower if prev_dir_up else cur_upper
            dir_up.iloc[i] = prev_dir_up

    return st
