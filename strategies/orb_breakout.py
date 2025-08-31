# strategies/orb_breakout.py
from __future__ import annotations

from typing import List, Dict, Tuple, Optional
import os, math
import pandas as pd
from loguru import logger

from utils.history import get_recent_candles
from utils.instruments import pick_nse_equity_tokens

# -------------------- knobs (env‑tunable) --------------------

SYMBOLS   = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY").replace(" ", "").split(",")
INTERVAL  = os.getenv("STRAT_INTERVAL", "FIFTEEN_MINUTE").upper()     # SmartAPI interval
BARS      = int(os.getenv("STRAT_BARS", "400"))
QTY       = int(os.getenv("STRAT_QTY", "1"))
PRODUCT   = os.getenv("STRAT_PRODUCT", "INTRADAY").upper()
ORDERTYPE = os.getenv("STRAT_ORDERTYPE", "MARKET").upper()
DURATION  = "DAY"

# ORB parameters
ORB_MIN   = int(os.getenv("STRAT_ORB_MIN", "15"))       # opening range minutes
PAD_BPS   = float(os.getenv("STRAT_ORB_PAD_BPS", "5"))  # entry confirmation in basis points (5 = 0.05%)
# Risk sizing off the band width (hi - lo)
TP_X      = float(os.getenv("STRAT_ORB_TP_X", "1.0"))   # take profit = TP_X * band
SL_X      = float(os.getenv("STRAT_ORB_SL_X", "1.0"))   # stop loss   = SL_X * band

# Map Angel interval -> minutes
_INTERVAL_MIN = {
    "ONE_MINUTE": 1, "THREE_MINUTE": 3, "FIVE_MINUTE": 5, "TEN_MINUTE": 10,
    "FIFTEEN_MINUTE": 15, "THIRTY_MINUTE": 30, "ONE_HOUR": 60, "ONE_DAY": 1440,
}

# -------------------- helpers --------------------

def _session_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the most recent session (IST) and window 09:15–15:30.
    Works whether df has a 'time' column or already uses a DatetimeIndex.
    """
    if df is None or df.empty:
        return df

    ts = pd.to_datetime(df["time"] if "time" in df.columns else df.index, errors="coerce")
    ts = pd.DatetimeIndex(ts)  # ensure DatetimeIndex (not Series)

    # Ensure tz-aware IST
    if ts.tz is None:
        ts = ts.tz_localize("Asia/Kolkata", nonexistent="shift_forward", ambiguous="NaT")
    else:
        ts = ts.tz_convert("Asia/Kolkata")

    w = df.copy()
    w.index = ts

    # numeric coercion + drop malformed rows
    for c in ("open", "high", "low", "close", "volume"):
        if c in w.columns:
            w[c] = pd.to_numeric(w[c], errors="coerce")
    w = w.dropna(subset=["open", "high", "low", "close"])

    if w.empty:
        return w

    last_day = w.index.max().date()
    start = pd.Timestamp(last_day, tz="Asia/Kolkata") + pd.Timedelta(hours=9, minutes=15)
    end   = pd.Timestamp(last_day, tz="Asia/Kolkata") + pd.Timedelta(hours=15, minutes=30)
    return w.loc[start:end]

def _opening_range(df: pd.DataFrame, minutes: int) -> Optional[Tuple[float, float, int]]:
    """Return (high, low, bars_used) of the first `minutes` of the session."""
    if df is None or df.empty:
        return None
    m = _INTERVAL_MIN.get(INTERVAL, 5)
    bars_needed = max(1, math.ceil(minutes / m))
    if len(df) < bars_needed:
        return None
    head = df.sort_index().iloc[:bars_needed]
    return float(head["high"].max()), float(head["low"].min()), bars_needed

def _signal_from_breakout(df: pd.DataFrame, hi: float, lo: float, pad_bps: float, start_idx: int) -> Optional[str]:
    """
    Return 'BUY' if a close breaks above hi*(1+pad), 'SELL' if close breaks below lo*(1-pad),
    scanning bars *after* the opening-range window.
    """
    if df is None or len(df) <= start_idx:
        return None
    up = hi * (1.0 + pad_bps / 10000.0)
    dn = lo * (1.0 - pad_bps / 10000.0)
    tail = df.sort_index().iloc[start_idx:]
    # use cross logic on bar closes
    for _, row in tail.iterrows():
        c = float(row["close"])
        if c > up:
            return "BUY"
        if c < dn:
            return "SELL"
    return None

def _display(ts: str, sym: str, hi: float, lo: float, sig: Optional[str]) -> None:
    s = f"[ORB {ORB_MIN}m] {sym} range=({lo:.2f},{hi:.2f})"
    if sig:
        logger.info(f"{s} signal={sig}")
    else:
        logger.info(f"{s} signal=None")

# -------------------- strategy entry --------------------

def run(smart) -> List[Dict]:
    """
    Build Angel order dicts for ORB breakout entries on NSE equities.
    Returns a flat list of order dicts (or empty if no signals).
    """
    orders: List[Dict] = []

    # Resolve tokens once (robust + cached)
    sym2tok = pick_nse_equity_tokens(SYMBOLS)
    if not sym2tok:
        logger.error("[ORB] No NSE equity tokens resolved for requested symbols.")
        return orders

    for sym in SYMBOLS:
        try:
            tok = sym2tok.get(sym.strip().upper())
            if not tok:
                logger.error(f"[ORB] {sym}: token not found in instruments.")
                continue

            df = get_recent_candles(
                smart,
                exchange="NSE",
                symboltoken=tok,
                interval=INTERVAL,
                bars=BARS,
            )
            if df is None or df.empty:
                logger.error(f"[ORB] {sym}: empty candles (reason={df.attrs.get('error')})")
                continue

            sess = _session_df(df)
            if sess is None or sess.empty:
                logger.info(f"[ORB] {sym}: no session data")
                continue

            rng = _opening_range(sess, ORB_MIN)
            if rng is None:
                logger.info(f"[ORB] {sym}: insufficient bars for ORB {ORB_MIN}m")
                continue

            hi, lo, k = rng
            sig = _signal_from_breakout(sess, hi, lo, PAD_BPS, k)
            _display(INTERVAL, sym, hi, lo, sig)

            if not sig:
                continue

            # band‑based sizing for SL/TP (price deltas)
            band = max(0.0, hi - lo)
            tp_points = round(band * TP_X, 2)
            sl_points = round(band * SL_X, 2)

            # Angel order dict (your executor can ignore/override squareoff/stoploss if unsupported)
            orders.append({
                "variety": "NORMAL",
                "tradingsymbol": f"{sym}-EQ",  # display only; execution uses token
                "symboltoken": tok,
                "transactiontype": sig,          # BUY / SELL
                "exchange": "NSE",
                "ordertype": ORDERTYPE,          # MARKET/LIMIT
                "producttype": PRODUCT,          # INTRADAY/DELIVERY
                "duration": DURATION,
                "price": 0,                      # MARKET
                "quantity": QTY,
                # Optional: pass tp/sl as points; executor may translate to linked orders
                "squareoff": tp_points,          # take profit (points)
                "stoploss": sl_points,           # stop loss   (points)
            })

        except Exception as e:
            logger.exception(f"[ORB] {sym} failed: {e}")

    return orders
