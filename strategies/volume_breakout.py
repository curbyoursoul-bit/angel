# strategies/volume_breakout.py
from __future__ import annotations
from typing import List, Dict
import os
import pandas as pd
from loguru import logger

from utils.history import get_recent_candles
from utils.resolve import resolve_nse_token

# ---- Tunables (env overridable) ----
SYMBOLS   = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY").replace(" ", "").split(",")
INTERVAL  = os.getenv("STRAT_INTERVAL", "FIVE_MINUTE").upper()
BARS      = int(os.getenv("STRAT_BARS", "240"))          # ~ past 2–3 sessions on 5m
VOL_LK    = int(os.getenv("STRAT_VOL_LOOKBACK", "20"))   # avg volume window
VOL_MULT  = float(os.getenv("STRAT_VOL_MULT", "1.5"))    # spike threshold
BASE_LK   = int(os.getenv("STRAT_BASE_LOOKBACK", "40"))  # recent hi/lo window for S/R
QTY       = int(os.getenv("STRAT_QTY", "1"))

EXCHANGE  = "NSE"
PRODUCT   = os.getenv("STRAT_PRODUCT", "INTRADAY").upper()
ORDERTYPE = os.getenv("STRAT_ORDERTYPE", "MARKET").upper()
DURATION  = "DAY"


def _calc_obv(df: pd.DataFrame) -> pd.Series:
    """Classic OBV: add vol when close↑, subtract when close↓ (flat → 0)."""
    d = df.copy()
    chg = d["close"].diff()
    sign = (chg > 0).astype(int) - (chg < 0).astype(int)
    obv = (sign * d["volume"].fillna(0)).cumsum()
    return obv


def _signal_from_df(df: pd.DataFrame) -> str | None:
    """
    Breakout confirmation when:
      1) Close breaks recent resistance/support, AND
      2) Volume spike vs. rolling average, AND
      3) OBV is rising (for BUY) / falling (for SELL).
    """
    if df is None or df.empty or len(df) < max(VOL_LK + 2, BASE_LK + 2):
        return None

    d = df.copy()
    d["vol_ma"] = d["volume"].rolling(VOL_LK).mean()
    d["obv"] = _calc_obv(d)

    prev, cur = d.iloc[-2], d.iloc[-1]
    if pd.isna(cur["vol_ma"]) or cur["vol_ma"] <= 0:
        return None

    # recent S/R (Volume-by-Price concept approximated by local hi/lo bands)
    recent = d.iloc[-BASE_LK:]
    recent_high = float(recent["high"].max())
    recent_low  = float(recent["low"].min())

    vol_spike = cur["volume"] > VOL_MULT * cur["vol_ma"]

    # OBV trend filter (directional confirmation)
    obv_rising  = cur["obv"] > prev["obv"]
    obv_falling = cur["obv"] < prev["obv"]

    # Price breakout vs prior close (helps avoid intra-bar noise)
    bullish = cur["close"] > recent_high and prev["close"] <= recent_high
    bearish = cur["close"] < recent_low  and prev["close"] >= recent_low

    if vol_spike and bullish and obv_rising:
        return "BUY"
    if vol_spike and bearish and obv_falling:
        return "SELL"
    return None


def run(smart) -> List[Dict]:
    orders: List[Dict] = []
    for sym in SYMBOLS:
        try:
            ts, token = resolve_nse_token(sym)
            df = get_recent_candles(smart, exchange=EXCHANGE, symboltoken=token, interval=INTERVAL, bars=BARS)
            sig = _signal_from_df(df)
            logger.info(f"[VOL+OBV breakout] {sym} sig={sig}")
            if not sig:
                continue

            orders.append({
                "variety": "NORMAL",
                "tradingsymbol": ts,
                "symboltoken": token,
                "transactiontype": sig,
                "exchange": EXCHANGE,
                "ordertype": ORDERTYPE,
                "producttype": PRODUCT,
                "duration": DURATION,
                "price": 0,        # MARKET
                "squareoff": 0,
                "stoploss": 0,
                "quantity": QTY,
            })
        except Exception as e:
            logger.exception(f"[VOL+OBV breakout] {sym} failed: {e}")
    return orders
