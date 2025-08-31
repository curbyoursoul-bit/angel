# strategies/ema_crossover.py
from __future__ import annotations
from typing import List, Dict
import os
import pandas as pd

from utils.history import get_recent_candles
from utils.resolve import resolve_nse_token

# --- Tunables / env overrides ---
SYMBOLS = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY").replace(" ", "").split(",")
INTERVAL = os.getenv("STRAT_INTERVAL", "FIVE_MINUTE")
BARS = int(os.getenv("STRAT_BARS", "300"))
FAST = int(os.getenv("STRAT_EMA_FAST", "9"))
SLOW = int(os.getenv("STRAT_EMA_SLOW", "21"))
QTY  = int(os.getenv("STRAT_QTY", "1"))

EXCHANGE = "NSE"
PRODUCT  = os.getenv("STRAT_PRODUCT", "INTRADAY")   # INTRADAY/CNC/...
ORDERTYPE= os.getenv("STRAT_ORDERTYPE", "MARKET")   # MARKET/LIMIT
DURATION = "DAY"

def _signal_from_df(df: pd.DataFrame) -> str | None:
    if df is None or df.empty or len(df) < max(FAST, SLOW) + 2:
        return None
    df = df.copy()
    df["ema_f"] = df["close"].ewm(span=FAST, adjust=False).mean()
    df["ema_s"] = df["close"].ewm(span=SLOW, adjust=False).mean()

    # use a clean cross: previous relationship vs current
    prev_up = df["ema_f"].iloc[-2] > df["ema_s"].iloc[-2]
    curr_up = df["ema_f"].iloc[-1] > df["ema_s"].iloc[-1]
    if not prev_up and curr_up:
        return "BUY"
    if prev_up and not curr_up:
        return "SELL"
    return None

def run(smart) -> List[Dict]:
    orders: List[Dict] = []
    for sym in SYMBOLS:
        try:
            ts, token = resolve_nse_token(sym)
            df = get_recent_candles(
                smart,
                exchange=EXCHANGE,
                symboltoken=token,
                interval=INTERVAL,
                bars=BARS,
            )
            sig = _signal_from_df(df)
            from loguru import logger
            logger.info(f"[EMA {FAST}/{SLOW}] {sym} signal: {sig}")
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
            from loguru import logger
            logger.exception(f"[EMA crossover] {sym} failed: {e}")
            continue
    return orders
