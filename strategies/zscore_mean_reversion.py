# strategies/zscore_mean_reversion.py
from __future__ import annotations
from typing import List, Dict
import os
import pandas as pd

from utils.history import get_recent_candles
from utils.resolve import resolve_nse_token

SYMBOLS = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY").replace(" ", "").split(",")
INTERVAL = os.getenv("STRAT_INTERVAL", "FIVE_MINUTE")
BARS = int(os.getenv("STRAT_BARS", "300"))
LOOKBACK = int(os.getenv("STRAT_Z_LOOKBACK", "20"))
Z_ENTRY  = float(os.getenv("STRAT_Z_ENTRY", "1.5"))
QTY  = int(os.getenv("STRAT_QTY", "1"))

EXCHANGE = "NSE"
PRODUCT  = os.getenv("STRAT_PRODUCT", "INTRADAY")
ORDERTYPE= os.getenv("STRAT_ORDERTYPE", "MARKET")
DURATION = "DAY"

def _signal_from_df(df: pd.DataFrame) -> str | None:
    if df is None or df.empty or len(df) < LOOKBACK + 1:
        return None
    df = df.copy()
    mu = df["close"].rolling(LOOKBACK).mean()
    sd = df["close"].rolling(LOOKBACK).std(ddof=0)
    z = (df["close"] - mu) / sd
    z_last = z.iloc[-1]
    if pd.isna(z_last):
        return None
    if z_last <= -Z_ENTRY:
        return "BUY"
    if z_last >= Z_ENTRY:
        return "SELL"
    return None

def run(smart) -> List[Dict]:
    orders: List[Dict] = []
    from loguru import logger
    for sym in SYMBOLS:
        try:
            ts, token = resolve_nse_token(sym)
            df = get_recent_candles(smart, exchange=EXCHANGE, symboltoken=token, interval=INTERVAL, bars=BARS)
            sig = _signal_from_df(df)
            logger.info(f"[ZScore MR] {sym} z_entry={Z_ENTRY} signal: {sig}")
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
                "price": 0,
                "squareoff": 0,
                "stoploss": 0,
                "quantity": QTY,
            })
        except Exception as e:
            logger.exception(f"[ZScore MR] {sym} failed: {e}")
    return orders
