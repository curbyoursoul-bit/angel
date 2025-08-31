# intelligence/regime.py
from __future__ import annotations
from typing import Literal
import pandas as pd
import numpy as np

# Very light regime detector from OHLCV dataframe (1m/5m candles)
# df columns: ['open','high','low','close','volume']
Regime = Literal["trend","range","volatile"]

def _adx(df: pd.DataFrame, period: int=14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm = (high - high.shift(1)).clip(lower=0)
    minus_dm = (low.shift(1) - low).clip(lower=0)
    tr = np.maximum(high - low, np.maximum((high - close.shift(1)).abs(), (low - close.shift(1)).abs()))
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0,np.nan) ) * 100
    adx = dx.ewm(alpha=1/period, adjust=False).mean().fillna(0)
    return adx

def _bb_width(df: pd.DataFrame, period: int=20, k: float=2.0) -> pd.Series:
    ma = df["close"].rolling(period).mean()
    sd = df["close"].rolling(period).std()
    upper, lower = ma + k*sd, ma - k*sd
    return ((upper - lower) / ma.replace(0,np.nan)).fillna(0)

def regime(df: pd.DataFrame) -> Regime:
    if len(df) < 30:
        return "range"
    adx = _adx(df).iloc[-1]
    bbw = _bb_width(df).iloc[-1]
    # simple heuristic thresholds; tune later
    if adx >= 25 and bbw >= 0.02:
        return "trend"
    if bbw >= 0.05:
        return "volatile"
    return "range"
