# utils/risk_gates.py
from __future__ import annotations
import os
from typing import Tuple
from loguru import logger
from utils.pnl_guard import estimate_realized_pnl_today, sum_live_quantities_today

def _env_float(name: str, default: float = 0.0) -> float:
    try:
        v = str(os.getenv(name, "")).strip()
        if v == "":
            return default
        return float(v)
    except Exception:
        return default

def _env_int(name: str, default: int = 0) -> int:
    try:
        v = str(os.getenv(name, "")).strip()
        if v == "":
            return default
        return int(float(v))
    except Exception:
        return default

def _normalize_loss_limit(v: float) -> float:
    """
    Accepts -2000 (preferred) or 2000 (human-friendly).
    Returns a negative threshold in both cases: -2000.
    0 disables the gate.
    """
    if v == 0:
        return 0.0
    return -abs(v)

RISK_MAX_LOSS = _normalize_loss_limit(_env_float("RISK_MAX_LOSS", 0.0))
RISK_MAX_QTY  = _env_int("RISK_MAX_QTY", 0)

def pretrade_global_risk_ok() -> bool:
    ok, _ = pretrade_global_risk_check()
    return ok

def pretrade_global_risk_check() -> Tuple[bool, str]:
    """
    Returns (ok, reason). ok=False means block with reason message.
    """
    # P&L cap (negative threshold)
    if RISK_MAX_LOSS:
        realized = estimate_realized_pnl_today()
        if realized <= RISK_MAX_LOSS:
            reason = (f"Daily P&L cap breached: realized ₹{realized:.2f} <= "
                      f"limit ₹{RISK_MAX_LOSS:.2f}. Blocking new trades.")
            logger.error(reason)
            return False, reason
        else:
            logger.info(f"[gate] P&L OK: realized ₹{realized:.2f} > limit ₹{RISK_MAX_LOSS:.2f}")

    # Quantity cap
    if RISK_MAX_QTY:
        q = sum_live_quantities_today()
        if q >= RISK_MAX_QTY:
            reason = (f"Daily quantity cap reached: placed {q} >= "
                      f"limit {RISK_MAX_QTY}. Blocking new trades.")
            logger.error(reason)
            return False, reason
        else:
            logger.info(f"[gate] Quantity OK: placed {q} < limit {RISK_MAX_QTY}")

    return True, "OK"
