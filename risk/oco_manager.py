# risk/oco_manager.py
from __future__ import annotations
from typing import Dict, Any, Tuple
def build_stop_target(primary: Dict[str,Any], fill_price: float,
                      stop_pct: float=0.5, target_pct: float=0.5,
                      sl_limit_buffer_pct: float=0.05) -> Tuple[Dict[str,Any],Dict[str,Any]]:
    side = primary.get("transactiontype","BUY").upper()
    qty  = int(float(primary.get("quantity",1)))
    base = dict(
        exchange=primary.get("exchange"),
        tradingsymbol=primary.get("tradingsymbol"),
        symboltoken=primary.get("symboltoken"),
        producttype=primary.get("producttype","INTRADAY"),
        duration=primary.get("duration","DAY"),
        variety=primary.get("variety","NORMAL"),
        quantity=qty,
    )
    if side=="BUY":
        stop_trig = fill_price*(1 - stop_pct/100); tgt = fill_price*(1 + target_pct/100)
        stop_side=tgt_side="SELL"
    else:
        stop_trig = fill_price*(1 + stop_pct/100); tgt = fill_price*(1 - target_pct/100)
        stop_side=tgt_side="BUY"
    stop_price = stop_trig*(1 - sl_limit_buffer_pct/100) if stop_side=="SELL" else stop_trig*(1 + sl_limit_buffer_pct/100)
    stop = dict(base, transactiontype=stop_side, ordertype="STOPLOSS_LIMIT",
                triggerprice=round(stop_trig,2), price=round(stop_price,2))
    target = dict(base, transactiontype=tgt_side, ordertype="LIMIT",
                  price=round(tgt,2))
    return stop, target
