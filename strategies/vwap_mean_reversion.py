# strategies/vwap_mean_reversion.py
from __future__ import annotations
from typing import List, Dict, Tuple, Optional
import os
import numpy as np
import pandas as pd
from loguru import logger
from utils.history import get_recent_candles
from utils.resolve import resolve_nse_token  # your util

SYMBOLS   = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY").replace(" ", "").split(",")
INTERVAL  = os.getenv("STRAT_INTERVAL", "FIVE_MINUTE").upper()
BARS      = int(os.getenv("STRAT_BARS", "400"))
QTY       = int(os.getenv("STRAT_QTY", "1"))
PRODUCT   = os.getenv("STRAT_PRODUCT", "INTRADAY").upper()
ORDERTYPE = os.getenv("STRAT_ORDERTYPE", "MARKET").upper()
DURATION  = "DAY"
K_BANDS   = float(os.getenv("STRAT_VWAP_K", "2.0"))

def _ensure_dt_index(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    if "time" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        d = df.copy()
        d["time"] = pd.to_datetime(d["time"], errors="coerce")
        d = d.set_index("time").sort_index()
    else:
        d = df.copy()
        if not isinstance(d.index, pd.DatetimeIndex):
            d.index = pd.to_datetime(d.index, errors="coerce")
            d = d.sort_index()
    if d.index.tz is None:
        d.index = d.index.tz_localize("Asia/Kolkata", nonexistent="shift_forward", ambiguous="NaT")
    else:
        d.index = d.index.tz_convert("Asia/Kolkata")
    return d

def _session_df(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    last_day = df.index.max().date()
    start = pd.Timestamp(last_day, tz="Asia/Kolkata") + pd.Timedelta(hours=9, minutes=15)
    end   = pd.Timestamp(last_day, tz="Asia/Kolkata") + pd.Timedelta(hours=15, minutes=30)
    return df.loc[start:end]

def _calc_vwap(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    pv = tp * d["volume"].clip(lower=0)
    vol_cum = d["volume"].clip(lower=0).cumsum()
    pv_cum  = pv.cumsum()
    d["vwap"] = np.where(vol_cum > 0, pv_cum / vol_cum, np.nan)
    d["dev"]  = (d["close"] - d["vwap"]).rolling(20, min_periods=10).std()
    return d

def _signal(last: pd.Series) -> str | None:
    if pd.isna(last["vwap"]) or pd.isna(last["dev"]) or last["dev"] <= 0:
        return None
    upper = last["vwap"] + K_BANDS * last["dev"]
    lower = last["vwap"] - K_BANDS * last["dev"]
    c = last["close"]
    if c > upper: return "SELL"
    if c < lower: return "BUY"
    return None

def run(smart) -> List[Dict]:
    orders: List[Dict] = []
    for sym in SYMBOLS:
        try:
            ts, token = resolve_nse_token(sym)
            df = get_recent_candles(smart, exchange="NSE", symboltoken=token, interval=INTERVAL, bars=BARS)
            df = _ensure_dt_index(df)
            df = _session_df(df)
            if df is None or df.empty:
                logger.info(f"[VWAP MR] {sym}: no session data")
                continue

            d = _calc_vwap(df).iloc[-1]
            sig = _signal(d)
            logger.info(f"[VWAP MR k={K_BANDS}] {sym} close={d['close']:.2f} vwap={d['vwap']:.2f} sig={sig}")
            if not sig:
                continue

            orders.append({
                "variety": "NORMAL",
                "tradingsymbol": ts,
                "symboltoken": token,
                "transactiontype": sig,
                "exchange": "NSE",
                "ordertype": ORDERTYPE,
                "producttype": PRODUCT,
                "duration": DURATION,
                "price": 0,          # MARKET
                "squareoff": 0,      # optional metadata; executor may ignore
                "stoploss": 0,
                "quantity": QTY,
            })
        except Exception as e:
            logger.exception(f"[VWAP MR] {sym} failed: {e}")
    return orders
