# execution/router.py
from __future__ import annotations
from typing import List, Dict, Any
from loguru import logger
from utils.order_exec import place_or_preview

def route(smart, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Route a batch of orders.
    TODO: add rollback if one leg fails.
    """
    results = []
    for o in orders:
        try:
            res = place_or_preview(smart, [o])
            results.append(res)
        except Exception as e:
            logger.error(f"order failed: {e}")
            # TODO: rollback logic
    return results
