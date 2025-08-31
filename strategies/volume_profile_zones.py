# strategies/volume_profile_zones.py
from __future__ import annotations
from typing import List, Dict, Tuple, Optional
import os
import numpy as np
import pandas as pd
from loguru import logger

from utils.history import get_recent_candles
from utils.resolve import resolve_nse_token

# --- Tunables (env overrides) ---
SYMBOLS   = os.getenv("STRAT_SYMBOLS", "RELIANCE,TCS,INFY").replace(" ", "").split(",")
INTERVAL  = os.getenv("STRAT_INTERVAL", "FIVE_MINUTE").upper()
BARS      = int(os.getenv("STRAT_BARS", "400"))         # ~yesterday + today on 5m
QTY       = int(os.getenv("STRAT_QTY", "1"))
PRODUCT   = os.getenv("STRAT_PRODUCT", "INTRADAY").upper()
ORDERTYPE = os.getenv("STRAT_ORDERTYPE", "MARKET").upper()
DURATION  = "DAY"

# Volume Profile params
BINS          = int(os.getenv("STRAT_VP_BINS", "60"))   # number of price bins for VbP
VA_PCT        = float(os.getenv("STRAT_VA_PCT", "0.70"))# value area ~70%
LVN_THRES_PCT = float(os.getenv("STRAT_LVN_PCT", "0.35"))# LVN = < 35% of POC volume
USE_SESSION_ONLY = os.getenv("STRAT_VP_SESSION_ONLY", "true").lower() == "true"

# Signal gates
REQUIRE_VOL_SPIKE = os.getenv("STRAT_VP_NEED_SPIKE", "true").lower() == "true"
VOL_LK    = int(os.getenv("STRAT_VOL_LOOKBACK", "20"))
VOL_MULT  = float(os.getenv("STRAT_VOL_MULT", "1.3"))   # volume spike threshold

EXCHANGE = "NSE"

def _session_df(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.to_datetime(df["time"] if "time" in df.columns else df.index, errors="coerce")
    idx = pd.DatetimeIndex(idx)
    if idx.tz is None:
        idx = idx.tz_localize("Asia/Kolkata", nonexistent="shift_forward", ambiguous="NaT")
    else:
        idx = idx.tz_convert("Asia/Kolkata")
    d = df.copy()
    d.index = idx
    # session window 09:15–15:30 IST
    if d.empty:
        return d
    day = d.index.max().date()
    start = pd.Timestamp(day, tz="Asia/Kolkata") + pd.Timedelta(hours=9, minutes=15)
    end   = pd.Timestamp(day, tz="Asia/Kolkata") + pd.Timedelta(hours=15, minutes=30)
    return d.loc[start:end]

def _volume_by_price(df: pd.DataFrame, bins: int) -> Tuple[np.ndarray, np.ndarray]:
    """Histogram of price by traded volume using typical price as bin locator."""
    d = df.copy()
    # typical price per bar; use bar volume as weight
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    vol = d["volume"].clip(lower=0).astype(float)
    lo, hi = float(d["low"].min()), float(d["high"].max())
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return np.array([]), np.array([])
    hist, edges = np.histogram(tp, bins=bins, range=(lo, hi), weights=vol)
    centers = (edges[:-1] + edges[1:]) / 2.0
    return hist, centers

def _poc_va_hvn_lvn(hist: np.ndarray, centers: np.ndarray, va_pct: float, lvn_frac: float):
    if hist.size == 0:
        return None
    # POC = bin with highest volume
    poc_idx = int(hist.argmax())
    poc_price = float(centers[poc_idx])
    poc_vol   = float(hist[poc_idx])

    # Build Value Area around POC covering ~va_pct of total volume
    total = hist.sum()
    if total <= 0:
        return None
    target = va_pct * total
    used = hist[poc_idx]
    l = r = poc_idx
    while used < target and (l > 0 or r < len(hist)-1):
        left_next  = hist[l-1] if l > 0 else -1
        right_next = hist[r+1] if r < len(hist)-1 else -1
        if right_next >= left_next and r < len(hist)-1:
            r += 1; used += hist[r]
        elif l > 0:
            l -= 1; used += hist[l]
        else:
            break
    vah = float(centers[r])
    val = float(centers[l])

    # LVN/HVN: label bins by fraction of POC volume
    lvn_mask = hist < (lvn_frac * poc_vol)
    hvn_mask = hist >= (lvn_frac * poc_vol)

    return {
        "poc": poc_price,
        "vah": vah,
        "val": val,
        "lvn_mask": lvn_mask,
        "hvn_mask": hvn_mask,
        "centers": centers,
        "hist": hist,
    }

def _vol_spike_ok(df: pd.DataFrame) -> bool:
    if not REQUIRE_VOL_SPIKE:
        return True
    if len(df) < VOL_LK + 1:  # need lookback
        return False
    vma = df["volume"].rolling(VOL_LK).mean().iloc[-1]
    return pd.notna(vma) and df["volume"].iloc[-1] > VOL_MULT * vma

def _last_close(df: pd.DataFrame) -> float:
    return float(df["close"].iloc[-1])

def _prev_close(df: pd.DataFrame) -> float:
    return float(df["close"].iloc[-2])

def _nearest_bin_price(price: float, centers: np.ndarray) -> int:
    return int(np.abs(centers - price).argmin())

def _signal_from_profile(df: pd.DataFrame) -> Optional[str]:
    """
    Implements three setups from the Volume Profile guide:
      1) POC Rejection (fade back into value)
      2) Value Area Rotation (re-entry leads toward POC)
      3) LVN Breakout (momentum through low-volume gap)
    """
    d = _session_df(df) if USE_SESSION_ONLY else df
    if d is None or d.empty:
        return None

    hist, centers = _volume_by_price(d, BINS)
    prof = _poc_va_hvn_lvn(hist, centers, VA_PCT, LVN_THRES_PCT)
    if prof is None:
        return None

    c  = _last_close(d)
    pc = _prev_close(d)
    poc, vah, val = prof["poc"], prof["vah"], prof["val"]
    lvn_mask, hvn_mask = prof["lvn_mask"], prof["hvn_mask"]

    # guard: require volume confirmation at signal bar
    if not _vol_spike_ok(d):
        return None

    # 1) POC Rejection: price tags POC and rejects (reverse toward VA edge)
    #    BUY if price bounced up from POC (pc <= poc < c); SELL if bounced down (pc >= poc > c)
    if pc <= poc < c:
        return "BUY"
    if pc >= poc > c:
        return "SELL"

    # 2) Value Area Rotation: re-enter VA from outside → rotate toward POC
    #    If below VAL and closes back inside VA → BUY; if above VAH and closes back inside → SELL
    if pc < val and c >= val:
        return "BUY"
    if pc > vah and c <= vah:
        return "SELL"

    # 3) LVN Breakout: if current close crosses an LVN bin boundary with impulse
    #    Approximation: identify current bin; if it's LVN and move continued beyond prev close, trade in that direction
    bin_idx = _nearest_bin_price(c, centers)
    if 0 <= bin_idx < len(lvn_mask) and lvn_mask[bin_idx]:
        if c > pc:
            return "BUY"
        if c < pc:
            return "SELL"

    return None

def run(smart) -> List[Dict]:
    orders: List[Dict] = []
    for sym in SYMBOLS:
        try:
            ts, token = resolve_nse_token(sym)
            df = get_recent_candles(smart, exchange=EXCHANGE, symboltoken=token, interval=INTERVAL, bars=BARS)
            if df is None or df.empty:
                logger.info(f"[VP] {sym}: no candles")
                continue

            sig = _signal_from_profile(df)
            logger.info(f"[VP] {sym} sig={sig}")
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
                "price": 0,     # MARKET
                "squareoff": 0,
                "stoploss": 0,
                "quantity": QTY,
            })
        except Exception as e:
            logger.exception(f"[VP] {sym} failed: {e}")
    return orders
