# strategies/bollinger_breakout.py
from __future__ import annotations
from typing import List, Dict
import os
import pandas as pd

from utils.history import get_recent_candles
from utils.resolve import resolve_nse_token

SYMBOLS = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY").replace(" ", "").split(",")
INTERVAL = os.getenv("STRAT_INTERVAL", "FIVE_MINUTE")
BARS = int(os.getenv("STRAT_BARS", "300"))
N = int(os.getenv("STRAT_BB_N", "20"))
K = float(os.getenv("STRAT_BB_K", "2.0"))
QTY  = int(os.getenv("STRAT_QTY", "1"))

EXCHANGE = "NSE"
PRODUCT  = os.getenv("STRAT_PRODUCT", "INTRADAY")
ORDERTYPE= os.getenv("STRAT_ORDERTYPE", "MARKET")
DURATION = "DAY"

def _signal_from_df(df: pd.DataFrame) -> str | None:
    if df is None or df.empty or len(df) < N + 2:
        return None
    df = df.copy()
    ma = df["close"].rolling(N).mean()
    sd = df["close"].rolling(N).std(ddof=0)
    upper = ma + K * sd
    lower = ma - K * sd

    c_prev, c = df["close"].iloc[-2], df["close"].iloc[-1]
    u_prev, u = upper.iloc[-2], upper.iloc[-1]
    l_prev, l = lower.iloc[-2], lower.iloc[-1]

    if pd.notna(u_prev) and pd.notna(u) and c > u and c_prev <= u_prev:
        return "BUY"
    if pd.notna(l_prev) and pd.notna(l) and c < l and c_prev >= l_prev:
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
            logger.info(f"[BB {N},{K}] {sym} signal: {sig}")
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
            logger.exception(f"[BB breakout] {sym} failed: {e}")
    return orders
