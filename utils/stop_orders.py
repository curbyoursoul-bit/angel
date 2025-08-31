# utils/stop_orders.py
from __future__ import annotations

from typing import Literal, Dict

def _round_tick(px: float, tick: float = 0.05) -> float:
    steps = round(float(px) / tick)
    return round(steps * tick, 2)

def _qty_int(x) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0

def make_sl_buy_for_short(
    short_order: Dict,
    ltp: float,
    stop_pct: float,
    limit_buffer_pct: float,
    *,
    tick: float = 0.05,                     # NFO options = ₹0.05
    use_stoploss_variety: bool = True,      # flip to False if your SDK wants NORMAL
    amo: bool | None = None,                # set True to force AMO, None = omit
) -> Dict:
    """
    Build a STOPLOSS-LIMIT BUY to cover a short option.
    - trigger = LTP * (1 + stop_pct)
    - limit   = trigger * (1 + limit_buffer_pct), rounded to tick
    Ensures limit >= trigger + one tick.
    """
    exch  = (short_order.get("exchange") or "NFO").upper()
    tsym  = str(short_order["tradingsymbol"])
    token = str(short_order.get("symboltoken", "") or "")
    prod  = (short_order.get("producttype") or "INTRADAY").upper()
    dur   = (short_order.get("duration") or "DAY").upper()
    qty   = _qty_int(short_order.get("quantity", 0))

    # compute prices
    trig_raw  = float(ltp) * (1.0 + float(stop_pct))
    trig      = _round_tick(trig_raw, tick)
    limit_raw = trig * (1.0 + float(limit_buffer_pct))
    limit     = _round_tick(limit_raw, tick)
    if limit <= trig:
        limit = _round_tick(trig + tick, tick)

    variety: Literal["STOPLOSS","NORMAL"] = "STOPLOSS" if use_stoploss_variety else "NORMAL"

    o: Dict = {
        "variety":         variety,
        "tradingsymbol":   tsym,
        "symboltoken":     token,
        "transactiontype": "BUY",
        "exchange":        exch,
        "ordertype":       "STOPLOSS_LIMIT",
        "producttype":     prod,
        "duration":        dur,
        "price":           f"{limit:.2f}",
        "triggerprice":    f"{trig:.2f}",
        "squareoff":       "0",
        "stoploss":        "0",
        "quantity":        qty,
    }
    if amo is True:
        o["amo"] = "YES"
    elif amo is False:
        o["amo"] = "NO"
    return o

def make_tp_buy_for_short(
    short_order: Dict,
    ltp: float,
    target_pct: float,
    *,
    tick: float = 0.05,
    amo: bool | None = None,
) -> Dict:
    """
    Profit-target LIMIT BUY for a short option.
    Buys back below current LTP by target_pct.
    """
    exch  = (short_order.get("exchange") or "NFO").upper()
    tsym  = str(short_order["tradingsymbol"])
    token = str(short_order.get("symboltoken", "") or "")
    prod  = (short_order.get("producttype") or "INTRADAY").upper()
    dur   = (short_order.get("duration") or "DAY").upper()
    qty   = _qty_int(short_order.get("quantity", 0))

    tgt_raw = max(float(ltp) * (1.0 - float(target_pct)), 0.05)
    tgt     = _round_tick(tgt_raw, tick)

    o: Dict = {
        "variety":         "NORMAL",
        "tradingsymbol":   tsym,
        "symboltoken":     token,
        "transactiontype": "BUY",
        "exchange":        exch,
        "ordertype":       "LIMIT",
        "producttype":     prod,
        "duration":        dur,
        "price":           f"{tgt:.2f}",
        "quantity":        qty,
        "squareoff":       "0",
        "stoploss":        "0",
    }
    if amo is True:
        o["amo"] = "YES"
    elif amo is False:
        o["amo"] = "NO"
    return o
