from __future__ import annotations
from typing import Dict, Optional
from loguru import logger

def _round_tick(px: float, *, exchange: str = "NFO") -> float:
    tick = 0.05 if (exchange or "").upper() == "NFO" else 0.01
    steps = round(float(px) / tick)
    return round(steps * tick, 2)

def make_sl_buy_for_short(
    primary_sell: Dict,
    entry_ltp: float,
    sl_pct: float,
    stop_limit_buffer_pct: float,
    *,
    producttype: str = "INTRADAY",
    variety: str = "NORMAL",
    duration: str = "DAY",
    amo: Optional[bool] = None,
    ordertag: Optional[str] = None,
) -> Dict:
    """
    STOPLOSS_LIMIT BUY to cover a short option.
    triggerprice = (1 + sl_pct) * entry_ltp
    price        = triggerprice * (1 + stop_limit_buffer_pct)
    """
    qty   = int(primary_sell.get("quantity", 0))
    exch  = (primary_sell.get("exchange") or "NFO").upper()
    tsym  = primary_sell["tradingsymbol"]
    token = str(primary_sell.get("symboltoken", ""))

    trig_raw  = float(entry_ltp) * (1.0 + float(sl_pct))
    limit_raw = trig_raw * (1.0 + float(stop_limit_buffer_pct))

    trig  = _round_tick(trig_raw, exchange=exch)
    limit = _round_tick(limit_raw, exchange=exch)

    o = {
        "variety":         variety,
        "tradingsymbol":   tsym,
        "symboltoken":     token,
        "transactiontype": "BUY",
        "exchange":        exch,
        "ordertype":       "STOPLOSS_LIMIT",
        "producttype":     producttype,
        "duration":        duration,
        "price":           limit,
        "triggerprice":    trig,
        "quantity":        qty,
    }
    if amo is not None:
        o["amo"] = "YES" if amo else "NO"
    if ordertag:
        o["ordertag"] = ordertag

    logger.debug(f"SL for short {tsym}: trig={trig:.2f} limit={limit:.2f} qty={qty}")
    return o

def make_tp_buy_for_short(
    primary_sell: Dict,
    entry_ltp: float,
    target_pct: float,
    *,
    producttype: str = "INTRADAY",
    variety: str = "NORMAL",
    duration: str = "DAY",
    amo: Optional[bool] = None,
    ordertag: Optional[str] = None,
) -> Dict:
    """
    LIMIT BUY to book profit on a short option.
    target price = entry_ltp * (1 - target_pct)  (clamped & tick-rounded)
    """
    qty   = int(primary_sell.get("quantity", 0))
    exch  = (primary_sell.get("exchange") or "NFO").upper()
    tsym  = primary_sell["tradingsymbol"]
    token = str(primary_sell.get("symboltoken", ""))

    tgt_raw = max(float(entry_ltp) * (1.0 - float(target_pct)), 0.05)
    tgt = _round_tick(tgt_raw, exchange=exch)

    o = {
        "variety":         variety,
        "tradingsymbol":   tsym,
        "symboltoken":     token,
        "transactiontype": "BUY",
        "exchange":        exch,
        "ordertype":       "LIMIT",
        "producttype":     producttype,
        "duration":        duration,
        "price":           tgt,
        "quantity":        qty,
    }
    if amo is not None:
        o["amo"] = "YES" if amo else "NO"
    if ordertag:
        o["ordertag"] = ordertag

    logger.debug(f"TP for short {tsym}: limit={tgt:.2f} qty={qty}")
    return o
