# utils/order_exec.py
from __future__ import annotations
from typing import List, Dict, Any
from loguru import logger

REQUIRED_KEYS = {"tradingsymbol", "transactiontype", "exchange", "ordertype", "producttype", "quantity"}


def _safe_log_order(order: dict) -> str:
    """Compact log string for an order (redacts tokens)."""
    return (
        f"{order.get('transactiontype','?')} {order.get('quantity','?')}x "
        f"{order.get('tradingsymbol','?')} "
        f"@{order.get('price','MKT')}/{order.get('ordertype','?')}"
    )


def place_order(smart, order: dict) -> dict:
    """
    Unified wrapper so we don't care whether SmartAPI expects a dict or kwargs.
    order: {
      variety, tradingsymbol, symboltoken, transactiontype, exchange,
      ordertype, producttype, duration, price, squareoff, stoploss, quantity, ...
    }
    """
    if not isinstance(order, dict):
        logger.error(f"place_order got non-dict: {order!r}")
        return {"status": False, "message": "order not a dict", "input": order}

    missing = REQUIRED_KEYS - set(order.keys())
    if missing:
        logger.warning(f"Order missing required fields: {missing} -> {order}")
        # we allow SmartAPI to error, but warn

    try:
        res = smart.placeOrder(order)  # some SDK versions expect dict
        logger.info(f"✓ Placed [{_safe_log_order(order)}] (dict) → {res}")
        return res if isinstance(res, dict) else {"status": True, "raw": res}
    except TypeError:
        try:
            res = smart.placeOrder(**order)  # others expect kwargs
            logger.info(f"✓ Placed [{_safe_log_order(order)}] (kwargs) → {res}")
            return res if isinstance(res, dict) else {"status": True, "raw": res}
        except Exception as e:
            logger.exception(f"placeOrder failed for [{_safe_log_order(order)}]: {e}")
            return {"status": False, "message": str(e), "order": order}
    except Exception as e:
        logger.exception(f"placeOrder failed for [{_safe_log_order(order)}]: {e}")
        return {"status": False, "message": str(e), "order": order}


def preview(order: dict) -> dict:
    """Return a normalized preview of the order (dry-run mode)."""
    norm = {**order}
    logger.info(f"[DRY-RUN] Would place: {_safe_log_order(order)}")
    return norm


def place_or_preview(smart, orders: List[Dict[str, Any]], dry_run: bool = False) -> List[dict]:
    """
    Execute a batch of orders.
    - In DRY_RUN, only logs and returns previews.
    - In live mode, calls place_order one by one.
    """
    results: List[dict] = []
    for od in orders or []:
        if not isinstance(od, dict):
            logger.warning(f"Skipping non-dict order: {od!r}")
            continue
        if dry_run:
            results.append(preview(od))
        else:
            results.append(place_order(smart, od))
    return results
