# core/portfolio.py
from __future__ import annotations
from math import floor
from typing import Optional
import os

DEFAULT_EQUITY_LOT = 1

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def apply_env_qty_caps(qty: int) -> int:
    """
    Applies global min/max caps from env:
      - MIN_QTY (default 0)
      - MAX_QTY (default very high)
    """
    min_q = _env_int("MIN_QTY", 0)
    max_q = _env_int("MAX_QTY", 10**9)
    return max(min(qty, max_q), min_q)

def risk_qty_by_rupee(
    stop_rupees: float,
    *,
    max_risk_rupees: Optional[float] = None,
    max_qty_cap: Optional[int] = None,
    max_exposure_rupees: Optional[float] = None,
    entry_price: Optional[float] = None,
) -> int:
    """
    Position size (shares/units) by risk:
        qty_risk = max_risk_rupees / stop_rupees

    Args:
      stop_rupees: per-share (or per-lot) distance to the stop (must be > 0).
      max_risk_rupees: override per-trade risk budget (₹). Defaults to env RISK_PER_TRADE (1500).
      max_qty_cap: hard cap on qty (optional).
      max_exposure_rupees: cap on notional exposure (₹). If provided with entry_price, will also constrain qty.
      entry_price: needed if you want exposure capping.

    Returns:
      Non-negative integer quantity.
    """
    if stop_rupees is None or stop_rupees <= 0:
        return 0

    if max_risk_rupees is None:
        max_risk_rupees = _env_float("RISK_PER_TRADE", 1500.0)

    qty = int(max_risk_rupees // stop_rupees)

    # Exposure cap (optional)
    if max_exposure_rupees is not None and entry_price and entry_price > 0:
        max_by_exposure = int(max_exposure_rupees // entry_price)
        qty = min(qty, max_by_exposure)

    if max_qty_cap is not None:
        qty = min(qty, max_qty_cap)

    qty = max(qty, 0)
    return apply_env_qty_caps(qty)

def fit_lot(qty: int, lotsize: Optional[int]) -> int:
    """
    Round quantity to exchange lot size.
    - If qty <= 0, returns 0 (do nothing).
    - If lotsize <= 0 or None, falls back to DEFAULT_EQUITY_LOT.
    - Rounds DOWN to nearest lot (conservative).
    """
    if qty is None or qty <= 0:
        return 0
    ls = int(lotsize or DEFAULT_EQUITY_LOT)
    if ls <= 0:
        ls = DEFAULT_EQUITY_LOT
    lots = qty // ls
    if lots <= 0:
        return 0
    return lots * ls
