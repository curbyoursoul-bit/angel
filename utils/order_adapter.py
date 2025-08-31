# utils/order_adapter.py
from __future__ import annotations
from typing import Dict, Any
from loguru import logger

# All possible keys your strategies may output
KNOWN_KEYS = {
    "exchange", "tradingsymbol", "symboltoken",
    "transactiontype", "ordertype", "producttype", "duration",
    "quantity", "price", "triggerprice",
    "variety", "disclosedquantity",
    "squareoff", "stoploss", "trailingStopLoss",
    "client_order_id",
}

def _as_str_price(x: Any) -> str | None:
    if x in (None, "", 0, "0"):
        return None
    try:
        return f"{float(x):.2f}"
    except Exception:
        return None

def to_smart_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a strategy/engine order dict to what Angel One's SmartAPI
    expects for SmartConnect.placeOrder. Adjust here if SDK signatures change.
    """
    o = dict(order)

    # Normalize cases
    for k in ("exchange", "transactiontype", "ordertype", "producttype", "duration"):
        if k in o and o[k] is not None:
            o[k] = str(o[k]).upper()

    # Angel SDK variations:
    # - Some builds reject 'variety' if empty/None
    if not o.get("variety"):
        o.pop("variety", None)

    # Basic sanity
    if not o.get("tradingsymbol"):
        logger.warning("Order missing tradingsymbol — check strategy output")

    # Type hygiene
    if "quantity" in o:
        try:
            o["quantity"] = int(o["quantity"])
        except Exception:
            pass

    # Ensure price fields are strings with 2dp where present
    if "price" in o:
        px = _as_str_price(o.get("price"))
        if px is None and str(o.get("ordertype","")).upper() == "MARKET":
            px = "0"
        if px is not None:
            o["price"] = px
        else:
            o.pop("price", None)

    if "triggerprice" in o:
        tpx = _as_str_price(o.get("triggerprice"))
        if tpx is not None:
            o["triggerprice"] = tpx
        else:
            o.pop("triggerprice", None)

    # Some SDKs use client_order_id; others ignore it—keeping is harmless
    cleaned = {k: v for k, v in o.items() if k in KNOWN_KEYS and v is not None}
    return cleaned
