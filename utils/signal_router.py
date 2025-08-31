# utils/signal_router.py
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from loguru import logger

import pandas as pd

from config import (
    INSTRUMENTS_CSV,
    DEFAULT_ORDER_TYPE,
    _s as _cs,
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _today_floor() -> datetime:
    now = datetime.now()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)

def _norm_idx(sym: str) -> str:
    s = (sym or "").strip().upper()
    if s in {"NIFTY50", "NIFTY 50"}:
        return "NIFTY"
    if s in {"BANK NIFTY", "NIFTYBANK", "NIFTY BANK"}:
        return "BANKNIFTY"
    return s

def _load_instruments() -> pd.DataFrame:
    path = _cs("INSTRUMENTS_CSV", INSTRUMENTS_CSV or "data/OpenAPIScripMaster.csv")
    df = pd.read_csv(path)
    # normalize expected columns defensively
    for c in (
        "exch_seg","exchange","instrumenttype","name","symbol","tradingsymbol",
        "symboltoken","expiry","lotsize","tick_size","optiontype","strike"
    ):
        if c not in df.columns:
            df[c] = None
    return df

def _parse_expiry(x) -> Optional[datetime]:
    if x in (None, "", "0"):
        return None
    # try common formats; fall back to pandas
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(x)[:10], fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(x).to_pydatetime()
    except Exception:
        return None

def _nearest_index_future(df: pd.DataFrame, symbol: str) -> Optional[dict]:
    """
    Find the nearest (>= today) NFO index future row for NIFTY / BANKNIFTY.
    """
    sym = _norm_idx(symbol)
    # Angel CSV typically marks index futures as FUTIDX under NFO
    excol = df.get("exch_seg").fillna(df.get("exchange"))
    mask = (
        excol.astype(str).str.upper().eq("NFO")
        & df.get("instrumenttype").astype(str).str.upper().isin(["FUTIDX","FUT"])
        & df.get("name").astype(str).str.upper().eq(sym)
    )
    futs = df[mask].copy()
    if futs.empty:
        logger.warning(f"[router] No NFO FUT found for {sym}")
        return None

    futs["expiry_dt"] = futs["expiry"].apply(_parse_expiry)
    futs = futs.dropna(subset=["expiry_dt"]).sort_values("expiry_dt")
    if futs.empty:
        logger.warning(f"[router] FUT rows for {sym} have no parsable expiries.")
        return None

    floor = _today_floor()
    near = futs[futs["expiry_dt"] >= floor]
    row = (near.iloc[0] if not near.empty else futs.iloc[0])
    return row.to_dict()

def _qty_lots(row: dict, lots: int) -> int:
    try:
        lot = int(row.get("lotsize") or 1)
    except Exception:
        lot = 1
    lots = max(1, int(lots))
    return lot * lots

# ---------------------------------------------------------------------
# Public API expected by core.engine.run_all(...)
# ---------------------------------------------------------------------

def build_orders_from_signal(
    smart: Any,
    pkt: dict,
    *,
    qty: int = 1,                 # lots for futures
    ordertype: str = "MARKET",    # MARKET / LIMIT
    producttype: str = "INTRADAY",
    prefer_futures: bool = True,
    tag_prefix: str = "auto",
) -> List[dict]:
    """
    Convert a single signal packet into 0..N broker order dict(s).

    Expected signal packet (your EMA strategy):
      {
        "name": "ema_x_5_20_banknifty_5m",
        "signal": "BUY" | "SELL" | "NO_OP" | "HOLD",
        "meta": {
            "template": "ema_crossover",
            "fast": 5, "slow": 20,
            "symbol": "BANKNIFTY",
            "timeframe": "15m"
        }
      }
    """
    if not isinstance(pkt, dict):
        return []

    action = str(pkt.get("signal", "")).upper()
    if action in {"", "NO_OP", "HOLD"}:
        logger.info("[router] NO_OP/HOLD — no order.")
        return []

    meta = pkt.get("meta") or {}
    symbol = _norm_idx(meta.get("symbol") or os.getenv("STRAT_SYMBOLS", "BANKNIFTY"))
    side = "BUY" if action == "BUY" else "SELL"

    # Only futures path implemented (cleanest for indices).
    if not prefer_futures:
        logger.warning("[router] prefer_futures=False path not implemented; returning no orders.")
        return []

    df = _load_instruments()
    row = _nearest_index_future(df, symbol)
    if not row:
        return []

    tradingsymbol = (row.get("tradingsymbol") or row.get("symbol") or "").strip()
    token = str(row.get("symboltoken") or row.get("token") or "").strip()
    exch = str(row.get("exch_seg") or row.get("exchange") or "NFO").upper()
    if not tradingsymbol or not token:
        logger.error(f"[router] Missing tradingsymbol/token for {symbol} FUT row: {row}")
        return []

    q = _qty_lots(row, qty)

    # Resolve order type defaults correctly
    ord_type = (ordertype or DEFAULT_ORDER_TYPE or "MARKET").upper()
    prod_type = (producttype or "INTRADAY").upper()

    order = {
        "variety": "NORMAL",
        "tradingsymbol": tradingsymbol,
        "symboltoken": token,
        "transactiontype": side,
        "exchange": exch,
        "ordertype": ord_type,     # MARKET or LIMIT
        "producttype": prod_type,  # INTRADAY or CARRYFORWARD
        "duration": "DAY",
        "quantity": q,
        # Price intentionally omitted for MARKET (executor will normalize anyway)
        "ordertag": f"{tag_prefix}-{symbol[:6]}-{side[0]}",
    }

    logger.info(f"[router] {symbol} {side} → FUT {tradingsymbol} qty={q} type={ord_type} prod={prod_type}")
    return [order]
