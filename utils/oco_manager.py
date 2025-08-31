# utils/oco_manager.py
from __future__ import annotations
import json, time
from pathlib import Path
from typing import Dict, Any
from loguru import logger

from config import STOP_LOSS_PCT, TARGET_PCT
from utils.oco_registry import new_group_id, record_primary, record_stop, record_target
from utils.order_exec import place_or_preview, cancel_order

REG = Path("data/oco_registry.json")

def _pct(x: float, pct: float) -> float:
    return round(x * (1 + pct), 2)

def build_exits(entry: Dict[str, Any], ltp: float) -> tuple[Dict[str, Any], Dict[str, Any]]:
    q = entry["quantity"]
    stop_price   = _pct(ltp, +STOP_LOSS_PCT)
    target_price = _pct(ltp, -TARGET_PCT)

    stop = {**entry, "transactiontype": "BUY", "ordertype": "STOPLOSS_LIMIT",
            "price": stop_price, "triggerprice": stop_price,
            "client_order_id": f"SL-{new_group_id()}"}
    tgt  = {**entry, "transactiontype": "BUY", "ordertype": "LIMIT",
            "price": target_price, "client_order_id": f"TG-{new_group_id()}"}
    return stop, tgt

def run_watcher(smart, poll_secs: int = 3):
    logger.info("OCO watcher started")
    while True:
        reg = json.loads(REG.read_text()) if REG.exists() else {}
        for tag, rec in reg.items():
            if rec.get("state") == "closed": continue
            prim = rec.get("primary") or {}
            if not prim: continue

            # TODO: call broker API for order status and place exits if filled
            # When either SL or Target fills, cancel the sibling and mark closed
        time.sleep(poll_secs)
