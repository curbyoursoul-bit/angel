# strategies/pairs_trading.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple, Optional

import os, math, datetime as dt, time, random
import pandas as pd
from loguru import logger

# ── Config via env (paste-ready) ──────────────────────────────────────────────
# Define pairs like: "HDFCBANK:ICICIBANK;SBIN:PNB;AXISBANK:KOTAKBANK"
PAIRS_STR        = os.getenv("PAIRS", "HDFCBANK:ICICIBANK")
INTERVAL         = os.getenv("PAIRS_INTERVAL", "FIVE_MINUTE")
BARS             = int(os.getenv("PAIRS_BARS", "400"))
LOOKBACK         = int(os.getenv("PAIRS_LOOKBACK", "120"))   # bars for beta & z
ENTRY_Z          = float(os.getenv("PAIRS_ENTRY_Z", "2.0"))
EXIT_Z           = float(os.getenv("PAIRS_EXIT_Z", "0.5"))
MAX_QTY          = int(os.getenv("PAIRS_MAX_QTY", "50"))     # hard cap per leg
RUPEES_PER_LEG   = float(os.getenv("PAIRS_RUPEES_PER_LEG", "25000"))

EXCHANGE         = "NSE"
VARIETY          = "NORMAL"
PRODUCT_TYPE     = "INTRADAY"
ORDER_TYPE       = "MARKET"
DURATION         = "DAY"

# SmartAPI throttling
_MIN_GAP_SEC     = float(os.getenv("SMARTAPI_MIN_GAP_SEC", "0.7"))   # min gap between HTTP calls
_PAIR_GAP_SEC    = float(os.getenv("PAIRS_PAIR_GAP_SEC", "0.4"))     # gap between pairs to avoid bursts
_last_call_ts    = 0.0

# ── Shared token/candle helpers ──────────────────────────────────────────────
_df_cache: Optional[pd.DataFrame] = None
_tok_cache: dict[tuple[str, str], str] = {}

def _respect_rate_limit():
    """Client-side gap between SmartAPI calls to reduce 403 rate errors."""
    global _last_call_ts
    now = time.time()
    wait = _MIN_GAP_SEC - (now - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()

def _ensure_df() -> Optional[pd.DataFrame]:
    global _df_cache
    if _df_cache is not None:
        return _df_cache
    try:
        from utils.instruments import load_instruments
    except Exception:
        load_instruments = None
    if load_instruments is None:
        return None
    try:
        _df_cache = load_instruments()
        return _df_cache
    except Exception as e:
        logger.warning(f"Pairs: instruments load failed: {e}")
        return None

def _resolve_equity(symbol: str, exchange: str = "NSE") -> Optional[Tuple[str, str]]:
    """
    Return (tradingsymbol, symboltoken) for the given human symbol.
    This guarantees the tradingsymbol matches the token → avoids AB1019.
    """
    key = (symbol.upper(), exchange.upper())
    df = _ensure_df()
    if df is None:
        return None

    cols = {c.lower(): c for c in df.columns}
    symcols = [cols.get(k) for k in ("symbol", "tradingsymbol", "name")]
    symcols = [c for c in symcols if c]
    exchcol = cols.get("exch_seg") or cols.get("exchange") or cols.get("exch")
    tokcol  = cols.get("symboltoken") or cols.get("token") or cols.get("tokens")
    tsc     = cols.get("tradingsymbol") or cols.get("symbol") or cols.get("name")
    if not (symcols and exchcol and tokcol and tsc):
        return None

    try:
        mask_sym = pd.Series(False, index=df.index)
        for sc in symcols:
            mask_sym = mask_sym | df[sc].astype(str).str.fullmatch(symbol, case=False, na=False)
        exps = df[exchcol].astype(str).str.upper()
        mask_ex = (exps == exchange.upper()) | (exps == "NSE") | exps.isin(["EQUITY", "NSE_EQ"])
        cand = df[mask_sym & mask_ex]
        if cand.empty:
            return None
        row = cand.iloc[0]
        tradingsymbol = str(row[tsc]).strip()
        token = str(row[tokcol]).strip()
        if not (tradingsymbol and token):
            return None
        # cache token under the human request key too
        _tok_cache[key] = token
        return tradingsymbol, token
    except Exception:
        return None

def _to_from_dates(interval: str, bars: int) -> Tuple[str, str]:
    now = dt.datetime.now()
    minutes = {
        "ONE_MINUTE": 1, "THREE_MINUTE": 3, "FIVE_MINUTE": 5, "TEN_MINUTE": 10,
        "FIFTEEN_MINUTE": 15, "THIRTY_MINUTE": 30, "ONE_HOUR": 60, "ONE_DAY": 1440
    }.get(interval.upper(), 5)
    start = now - dt.timedelta(minutes=minutes * (bars + 5))
    fmt = "%Y-%m-%d %H:%M"
    return start.strftime(fmt), now.strftime(fmt)

def _candles(smart, *, tradingsymbol: str, symboltoken: str, interval: str, bars: int, exchange: str = "NSE") -> pd.DataFrame:
    """Fetch candles by the exact (tradingsymbol, token) pair that will be used for orders."""
    from datetime import datetime, timedelta
    import pytz

    fn = getattr(smart, "getCandleData", None) or getattr(smart, "candleData", None)
    if not fn:
        raise RuntimeError("SmartAPI has no getCandleData")

    itv = interval.upper()

    def _try_call(callable_fn, **kwargs):
        for attempt in range(5):
            _respect_rate_limit()
            try:
                return callable_fn(**kwargs)
            except Exception as e:
                msg = str(e).lower()
                rate_limited = ("exceeding access rate" in msg) or ("too many requests" in msg) or ("429" in msg)
                if rate_limited and attempt < 4:
                    sleep_s = (0.6 * (2 ** attempt)) + random.random() * 0.3
                    time.sleep(sleep_s)
                    continue
                raise

    def _call(fromdate: str, todate: str):
        payload = {
            "exchange": exchange,
            "symboltoken": str(symboltoken),
            "interval": itv,
            "fromdate": fromdate,
            "todate": todate,
        }
        try:
            return _try_call(lambda payload: fn(payload), payload=payload)
        except TypeError:
            try:
                return _try_call(lambda historicalDataParams: fn(historicalDataParams=historicalDataParams),
                                 historicalDataParams=payload)
            except TypeError:
                try:
                    return _try_call(lambda **kw: fn(**kw),
                                     exchange=exchange, symboltoken=str(symboltoken),
                                     interval=itv, fromdate=fromdate, todate=todate)
                except TypeError:
                    return _try_call(lambda **kw: fn(**kw),
                                     exchange=exchange, symboltoken=str(symboltoken),
                                     interval=itv, fromdate=fromdate, todate=todate, exchangeType=1)

    fromdate, todate = _to_from_dates(itv, bars)
    data = _call(fromdate, todate)

    def _extract_rows(d):
        if isinstance(d, dict):
            return d.get("data") or d.get("Data") or d.get("candles") or []
        if isinstance(d, list):
            return d
        return []

    rows = _extract_rows(data)

    if not rows:
        ist = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.now(ist)
        start = (now_ist - timedelta(days=7)).replace(hour=9, minute=15, second=0, microsecond=0)
        end   =  now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
        if now_ist.hour < 9:
            yday = now_ist - timedelta(days=1)
            end = yday.replace(hour=15, minute=30, second=0, microsecond=0)
        if end <= start:
            start = now_ist - timedelta(days=10)

        fmt = "%Y-%m-%d %H:%M"
        data = _call(start.strftime(fmt), end.strftime(fmt))
        rows = _extract_rows(data)

    if not rows:
        head = (repr(data)[:300] if data is not None else "None")
        raise RuntimeError(f"No candles for {tradingsymbol} ({itv}); raw={head}")

    rows = rows[-bars:]

    if rows and isinstance(rows[0], (list, tuple)) and len(rows[0]) >= 6:
        df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
    else:
        def _row_to_list(r):
            return [
                r.get("time") or r.get("timestamp") or r.get("date") or r.get("datetime"),
                r.get("open"), r.get("high"), r.get("low"), r.get("close"),
                r.get("volume") or r.get("vol") or r.get("qty")
            ]
        df = pd.DataFrame([_row_to_list(r) for r in rows],
                          columns=["time", "open", "high", "low", "close", "volume"])

    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.dropna(subset=["close"], inplace=True)
    if df.empty:
        raise RuntimeError(f"Empty candle frame for {tradingsymbol}")
    return df

# ── Core math ────────────────────────────────────────────────────────────────
def _hedge_ratio(y: pd.Series, x: pd.Series) -> float:
    """OLS slope (beta) using last LOOKBACK points; fallback to 1.0."""
    y = y.tail(LOOKBACK)
    x = x.tail(LOOKBACK)
    if len(y) < 2 or len(x) < 2:
        return 1.0
    xm = x.mean(); ym = y.mean()
    cov = ((x - xm) * (y - ym)).sum()
    var = ((x - xm) ** 2).sum()
    if var <= 1e-9:
        return 1.0
    return float(cov / var)

def _zscore(s: pd.Series, n: int) -> float:
    s = s.tail(n)
    mu = s.mean(); sd = s.std(ddof=0)
    if sd <= 1e-12:
        return 0.0
    return float((s.iloc[-1] - mu) / sd)

def _notional_neutral_qty(price_y: float, price_x: float, beta: float, cap_each_leg: int) -> Tuple[int, int]:
    """Rupee-notional sizing per leg, gently nudged towards beta neutrality."""
    if price_y <= 0 or price_x <= 0:
        return 0, 0
    qy = max(1, int(RUPEES_PER_LEG // price_y))
    qx = max(1, int(RUPEES_PER_LEG // price_x))
    # beta-tilt for x leg so |y| ≈ |beta * x|
    qx = max(1, int(round(abs(beta) * qx)))
    qy = min(qy, cap_each_leg)
    qx = min(qx, cap_each_leg)
    return qy, qx

# ── Strategy ─────────────────────────────────────────────────────────────────
def run(smart) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []

    pairs: List[Tuple[str, str]] = []
    for blk in [p.strip() for p in PAIRS_STR.split(";") if p.strip()]:
        if ":" in blk:
            a, b = blk.split(":", 1)
            pairs.append((a.strip().upper(), b.strip().upper()))

    if not pairs:
        logger.warning("Pairs: no valid PAIRS provided")
        return orders

    for y_sym, x_sym in pairs:
        try:
            # Resolve exact broker identifiers for both legs
            ry = _resolve_equity(y_sym, EXCHANGE)
            rx = _resolve_equity(x_sym, EXCHANGE)
            if not ry or not rx:
                logger.warning(f"{y_sym}/{x_sym}: missing token/ts; skip")
                time.sleep(_PAIR_GAP_SEC)
                continue
            ts_y, tok_y = ry
            ts_x, tok_x = rx

            # Fetch candles using the *same* (tradingsymbol, token) we will order with
            df_y = _candles(smart, tradingsymbol=ts_y, symboltoken=tok_y, interval=INTERVAL, bars=BARS, exchange=EXCHANGE)
            df_x = _candles(smart, tradingsymbol=ts_x, symboltoken=tok_x, interval=INTERVAL, bars=BARS, exchange=EXCHANGE)

            y = df_y["close"].astype(float)
            x = df_x["close"].astype(float)

            beta = _hedge_ratio(y, x)
            spread = y - beta * x
            z = _zscore(spread, LOOKBACK)

            py = float(y.iloc[-1]); px = float(x.iloc[-1])
            qty_y, qty_x = _notional_neutral_qty(py, px, beta, MAX_QTY)
            if qty_y == 0 or qty_x == 0:
                logger.info(f"{y_sym}/{x_sym}: qtys zero; skip")
                time.sleep(_PAIR_GAP_SEC)
                continue

            logger.info(f"{y_sym}/{x_sym}: beta={beta:.3f} z={z:.2f} px={px:.2f} py={py:.2f} "
                        f"qty_y={qty_y} qty_x={qty_x} | ts_y={ts_y} ts_x={ts_x}")

            if z > ENTRY_Z:
                # Short spread: SELL y, BUY x
                orders.append({
                    "variety": VARIETY, "tradingsymbol": ts_y, "symboltoken": str(tok_y),
                    "transactiontype": "SELL", "exchange": EXCHANGE, "ordertype": ORDER_TYPE,
                    "producttype": PRODUCT_TYPE, "duration": DURATION, "quantity": qty_y,
                    "ordertag": f"PAIRS_SHORT_{y_sym}_{x_sym}_{INTERVAL}"
                })
                orders.append({
                    "variety": VARIETY, "tradingsymbol": ts_x, "symboltoken": str(tok_x),
                    "transactiontype": "BUY", "exchange": EXCHANGE, "ordertype": ORDER_TYPE,
                    "producttype": PRODUCT_TYPE, "duration": DURATION, "quantity": qty_x,
                    "ordertag": f"PAIRS_SHORT_{y_sym}_{x_sym}_{INTERVAL}"
                })
                logger.success(f"{y_sym}/{x_sym}: SHORT spread → SELL {ts_y} qty={qty_y}, BUY {ts_x} qty={qty_x}")

            elif z < -ENTRY_Z:
                # Long spread: BUY y, SELL x
                orders.append({
                    "variety": VARIETY, "tradingsymbol": ts_y, "symboltoken": str(tok_y),
                    "transactiontype": "BUY", "exchange": EXCHANGE, "ordertype": ORDER_TYPE,
                    "producttype": PRODUCT_TYPE, "duration": DURATION, "quantity": qty_y,
                    "ordertag": f"PAIRS_LONG_{y_sym}_{x_sym}_{INTERVAL}"
                })
                orders.append({
                    "variety": VARIETY, "tradingsymbol": ts_x, "symboltoken": str(tok_x),
                    "transactiontype": "SELL", "exchange": EXCHANGE, "ordertype": ORDER_TYPE,
                    "producttype": PRODUCT_TYPE, "duration": DURATION, "quantity": qty_x,
                    "ordertag": f"PAIRS_LONG_{y_sym}_{x_sym}_{INTERVAL}"
                })
                logger.success(f"{y_sym}/{x_sym}: LONG spread → BUY {ts_y} qty={qty_y}, SELL {ts_x} qty={qty_x}")
            else:
                logger.info(f"{y_sym}/{x_sym}: |z|={abs(z):.2f} ≤ entry {ENTRY_Z}; no entry")

        except Exception as e:
            logger.exception(f"{y_sym}/{x_sym}: pairs strat failed: {e}")

        # gentle gap between pairs to avoid bursty API hits
        time.sleep(_PAIR_GAP_SEC)

    return orders
