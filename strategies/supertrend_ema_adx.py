# strategies/supertrend_ema_adx.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple, Optional
import math
import os
import time
import datetime as dt
import pandas as pd
from loguru import logger

from core.portfolio import risk_qty_by_rupee, fit_lot  # week-1 module you added
# token lookup from instruments master
try:
    from utils.instruments import load_instruments
except Exception:
    load_instruments = None  # fallback handled below

# --- Config via env overrides -----------------------------------------------
SYMBOLS   = os.getenv("STRAT_SYMBOLS", "HDFCBANK,ICICIBANK,SBIN").split(",")
INTERVAL  = os.getenv("STRAT_INTERVAL", "FIFTEEN_MINUTE")   # ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, etc.
BARS      = int(os.getenv("STRAT_BARS", "300"))
QTY_ENV   = os.getenv("STRAT_QTY")  # hard cap per order (optional)

EXCHANGE      = "NSE"
PRODUCT_TYPE  = "INTRADAY"
VARIETY       = "NORMAL"
DURATION      = "DAY"
ORDER_TYPE    = "MARKET"  # keep MARKET for first live; you can flip to LIMIT

EMA_LEN       = int(os.getenv("ST_EMA_LEN", "200"))
STR_PERIOD    = int(os.getenv("ST_PERIOD", "10"))
STR_MULT      = float(os.getenv("ST_MULT", "3.0"))
ADX_LEN       = int(os.getenv("ST_ADX_LEN", "14"))
ADX_TH        = float(os.getenv("ST_ADX_TH", "20"))
DEBUG_SIGNALS = os.getenv("DEBUG_SIGNALS", "false").lower() in ("1","true","yes","y")

# --- Token resolver ----------------------------------------------------------
_df_cache: Optional[pd.DataFrame] = None

def _ensure_df() -> Optional[pd.DataFrame]:
    global _df_cache
    if _df_cache is not None:
        return _df_cache
    if load_instruments is None:
        return None
    try:
        _df_cache = load_instruments()
        return _df_cache
    except Exception as e:
        logger.warning(f"Could not load instruments for token lookup: {e}")
        return None
    
def _equity_token(symbol: str, exchange: str = "NSE") -> Optional[str]:
    df = _ensure_df()
    if df is None:
        return None

    cols = {c.lower(): c for c in df.columns}
    symcols = [cols.get(k) for k in ("symbol", "tradingsymbol", "name")]
    symcols = [c for c in symcols if c]
    exchcol = cols.get("exch_seg") or cols.get("exchange") or cols.get("exch")
    tokcol  = cols.get("symboltoken") or cols.get("token") or cols.get("tokens")
    if not (symcols and exchcol and tokcol):
        return None

    try:
        # Build masks on the same index
        mask_sym = pd.Series(False, index=df.index)
        for sc in symcols:
            mask_sym = mask_sym | df[sc].astype(str).str.fullmatch(symbol, case=False, na=False)

        exps = df[exchcol].astype(str).str.upper()
        # prefer strict 'NSE' first; if none, fall back to broader equity labels
        mask_ex_strict = (exps == exchange.upper()) | (exps == "NSE")
        mask_ex_loose  = exps.isin([exchange.upper(), "NSE", "EQUITY", "NSE_EQ"])

        cand = df[mask_sym & mask_ex_strict]
        if cand.empty:
            cand = df[mask_sym & mask_ex_loose]
        if cand.empty:
            return None

        tok = str(cand.iloc[0][tokcol]).strip()
        return tok or None
    except Exception:
        return None

# --- Candle fetch (robust across SDKs) --------------------------------------
def _to_from_dates(interval: str, bars: int) -> Tuple[str, str]:
    # Angel expects 'YYYY-MM-DD HH:MM'
    now = dt.datetime.now()
    # approximate minutes per bar
    minutes = {
        "ONE_MINUTE": 1, "THREE_MINUTE": 3, "FIVE_MINUTE": 5, "TEN_MINUTE": 10,
        "FIFTEEN_MINUTE": 15, "THIRTY_MINUTE": 30, "ONE_HOUR": 60, "ONE_DAY": 60*24
    }.get(interval.upper(), 15)
    start = now - dt.timedelta(minutes=minutes * (bars + 5))
    fmt = "%Y-%m-%d %H:%M"
    return start.strftime(fmt), now.strftime(fmt)

def _candles(smart, symbol: str, interval: str, bars: int, *, exchange: str = "NSE") -> pd.DataFrame:
    """
    Handles multiple SmartAPI variants:
      - getCandleData(payload_dict)
      - getCandleData(historicalDataParams=payload_dict)
      - getCandleData(exchange=.., symboltoken=.., interval=.., fromdate=.., todate=..)
    And different response shapes:
      - {"data": [...]}, {"Data": [...]}, {"candles": [...]}, or a raw list
    """
    fn = getattr(smart, "getCandleData", None) or getattr(smart, "candleData", None)
    if not fn:
        raise RuntimeError("SmartAPI client has no candle API (getCandleData).")

    token = _equity_token(symbol, exchange=exchange)
    if not token:
        raise RuntimeError(f"No NSE token found for {symbol} — refresh instruments JSON/CSV and try again.")

    itv = interval.upper()
    fromdate, todate = _to_from_dates(itv, bars)

    payload = {
        "exchange": exchange,
        "symboltoken": str(token),
        "interval": itv,
        "fromdate": fromdate,
        "todate": todate,
    }

    data = None
    # Try known call signatures in order
    try:
        data = fn(payload)
    except TypeError:
        try:
            data = fn(historicalDataParams=payload)
        except TypeError:
            try:
                data = fn(exchange=exchange, symboltoken=str(token),
                          interval=itv, fromdate=fromdate, todate=todate)
            except TypeError:
                # Some builds require exchangeType=1 for NSE cash
                data = fn(exchange=exchange, symboltoken=str(token),
                          interval=itv, fromdate=fromdate, todate=todate, exchangeType=1)

    # --- Normalize rows from various response shapes
    rows = None
    if isinstance(data, dict):
        rows = data.get("data") or data.get("Data") or data.get("candles")
    elif isinstance(data, list):
        rows = data

    if not rows:
        # help debugging odd SDKs
        head = (repr(data)[:300] if data is not None else "None")
        raise RuntimeError(f"No candles for {symbol} ({itv}); raw response head={head}")

    # Keep only the last N bars
    rows = rows[-bars:]

    # Build DataFrame for either list rows [ts, o,h,l,c,v] or dict rows
    if rows and isinstance(rows[0], (list, tuple)) and len(rows[0]) >= 6:
        df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
    else:
        def _row_to_list(r):
            return [
                r.get("time") or r.get("timestamp") or r.get("date") or r.get("datetime"),
                r.get("open"), r.get("high"), r.get("low"), r.get("close"),
                r.get("volume") or r.get("vol") or r.get("qty"),
            ]
        df = pd.DataFrame([_row_to_list(r) for r in rows],
                          columns=["time","open","high","low","close","volume"])

    for c in ("open","high","low","close","volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.dropna(subset=["open","high","low","close"], inplace=True)
    if df.empty:
        raise RuntimeError(f"Candle frame empty for {symbol} ({itv}); rows head={str(rows)[:200]}")
    return df

# --- Indicators --------------------------------------------------------------
def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _supertrend(df: pd.DataFrame, period: int, mult: float) -> pd.Series:
    hl2 = (df["high"] + df["low"]) / 2.0
    tr1 = (df["high"] - df["low"]).abs()
    tr2 = (df["high"] - df["close"].shift()).abs()
    tr3 = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()

    upper = hl2 + mult * atr
    lower = hl2 - mult * atr

    st = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=int)

    st.iloc[0] = upper.iloc[0]
    direction.iloc[0] = 1

    for i in range(1, len(df)):
        if df["close"].iloc[i] > st.iloc[i-1]:
            direction.iloc[i] = 1
        elif df["close"].iloc[i] < st.iloc[i-1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i-1]

        if direction.iloc[i] == 1:
            st.iloc[i] = min(upper.iloc[i], st.iloc[i-1])
        else:
            st.iloc[i] = max(lower.iloc[i], st.iloc[i-1])

    return st

def _adx(df: pd.DataFrame, n: int) -> pd.Series:
    up = df["high"].diff()
    down = -df["low"].diff()
    plus_dm = ((up > down) & (up > 0)) * up
    minus_dm = ((down > up) & (down > 0)) * down

    tr1 = (df["high"] - df["low"]).abs()
    tr2 = (df["high"] - df["close"].shift()).abs()
    tr3 = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=n, adjust=False).mean()

    plus_di = 100 * (plus_dm.ewm(span=n, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(span=n, adjust=False).mean() / atr)
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di)).fillna(0)
    adx = dx.ewm(span=n, adjust=False).mean()
    return adx

# --- Signal logic ------------------------------------------------------------
def _signal_row(df: pd.DataFrame) -> Tuple[str, float, float]:
    if len(df) < max(EMA_LEN, STR_PERIOD, ADX_LEN) + 2:
        return "HOLD", math.nan, math.nan

    ema = _ema(df["close"], EMA_LEN)
    st  = _supertrend(df, STR_PERIOD, STR_MULT)
    adx = _adx(df, ADX_LEN)

    c  = float(df["close"].iloc[-1])
    e  = float(ema.iloc[-1])
    s0 = float(st.iloc[-2])
    s1 = float(st.iloc[-1])
    a1 = float(adx.iloc[-1])

    # gates
    g_trend_up   = c > e
    g_trend_down = c < e
    g_flip_up    = (s1 < c) and (s0 >= c or s1 >= s0)  # flip/transition notion
    g_flip_down  = (s1 > c) and (s0 <= c or s1 <= s0)
    g_strength   = a1 >= ADX_TH

    if DEBUG_SIGNALS:
        logger.info(f"GATES: close={c:.2f} ema{EMA_LEN}={e:.2f} st[-1]={s1:.2f} st[-2]={s0:.2f} adx{ADX_LEN}={a1:.1f} | "
                    f"up:{g_trend_up} flip_up:{g_flip_up} strength:{g_strength} | "
                    f"down:{g_trend_down} flip_down:{g_flip_down}")

    if g_trend_up and g_flip_up and g_strength:
        stop = min(s1, float(df['low'].iloc[-2]))
        return "BUY", stop, max(c - stop, 0.05)

    if g_trend_down and g_flip_down and g_strength:
        stop = max(s1, float(df['high'].iloc[-2]))
        return "SELL", stop, max(stop - c, 0.05)

    return "HOLD", math.nan, math.nan

# --- Public: run(smart) ------------------------------------------------------
def run(smart) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    hard_cap_qty = int(QTY_ENV) if QTY_ENV else None

    for sym in [s.strip().upper() for s in SYMBOLS if s.strip()]:
        try:
            df = _candles(smart, sym, INTERVAL, BARS, exchange=EXCHANGE)
            side, stop_px, stop_rupees = _signal_row(df)
            if side == "HOLD":
                logger.info(f"{sym}: no signal")
                continue

            last = float(df["close"].iloc[-1])
            # risk-based sizing (rupees per share to stop)
            base_qty = risk_qty_by_rupee(stop_rupees, max_qty_cap=hard_cap_qty)
            qty = max(base_qty, 1)

            token = _equity_token(sym, exchange=EXCHANGE)
            if not token:
                logger.error(f"{sym}: no token — skip order")
                continue

            od = {
                "variety": VARIETY,
                "tradingsymbol": sym,
                "symboltoken": str(token),          # REQUIRED by broker.normalize
                "transactiontype": side,
                "exchange": EXCHANGE,
                "ordertype": ORDER_TYPE,
                "producttype": PRODUCT_TYPE,
                "duration": DURATION,
                "quantity": qty,
                "ordertag": f"ST_EMA_ADX_{INTERVAL}",
            }
            orders.append(od)
            logger.success(f"{sym}: {side} qty={qty} @ ~{last:.2f} (stop~{stop_px:.2f}, risk/share~{stop_rupees:.2f})")

        except TypeError as e:
            logger.exception(f"{sym}: candle API signature mismatch: {e}")
        except Exception as e:
            logger.exception(f"{sym}: strategy failed: {e}")

    return orders
