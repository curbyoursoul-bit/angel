# agent/policies.py
from __future__ import annotations
import os
import datetime
from typing import Dict, Any, Optional

# --- timezone (pytz optional) ---
try:
    import pytz  # type: ignore
    IST = pytz.timezone("Asia/Kolkata")
except Exception:
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# ---- Market hours -----------------------------------------------------------
def market_is_open(
    now_ts: Optional[float] = None,
    *,
    preopen_minutes: int = 0,
    grace_close_minutes: int = 0,
    bypass_env: str = "BYPASS_MARKET_HOURS",
) -> bool:
    """
    NSE regular session: 09:15–15:30 IST (Mon–Fri).
    - If `now_ts` is provided (epoch seconds), it is used; else current IST time.
    - `preopen_minutes`: allow starting N minutes before 09:15 (e.g., for data warmup).
    - `grace_close_minutes`: allow N minutes after 15:30 (e.g., for cleanup).
    - If env BYPASS_MARKET_HOURS=1/true -> always True.
    """
    if str(os.getenv(bypass_env, "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
        return True

    if now_ts is not None:
        now = datetime.datetime.fromtimestamp(float(now_ts), tz=IST)
    else:
        now = datetime.datetime.now(tz=IST)

    # Weekend check
    if now.weekday() >= 5:  # 5=Sat, 6=Sun
        return False

    start = datetime.time(9, 15)
    end   = datetime.time(15, 30)

    # Apply grace windows
    start_dt = datetime.datetime.combine(now.date(), start, tzinfo=IST) - datetime.timedelta(minutes=max(0, preopen_minutes))
    end_dt   = datetime.datetime.combine(now.date(), end,   tzinfo=IST) + datetime.timedelta(minutes=max(0, grace_close_minutes))

    return start_dt <= now <= end_dt

# ---- Risk caps --------------------------------------------------------------
def enforce_risk_caps(order: Dict[str, Any], caps: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clamp order['quantity'] to caps:
      - MAX_QTY (default: current qty)
      - MIN_QTY (default: 0)
    Never returns a negative quantity.
    """
    try:
        q = int(order.get("quantity", 0) or 0)
    except Exception:
        q = 0

    try:
        maxq = int(caps.get("MAX_QTY", q))
    except Exception:
        maxq = q

    try:
        minq = int(caps.get("MIN_QTY", 0))
    except Exception:
        minq = 0

    q = max(min(q, maxq), minq)
    order["quantity"] = max(q, 0)
    return order

# ---- Live-trade safety ------------------------------------------------------
def allow_live_trading(max_clock_skew_s: float = 3.0, *, bypass_env: str = "ALLOW_LIVE_ANYWAY") -> bool:
    """
    Block LIVE if system clock is out of sync (helps with TOTP/session issues).
    Returns True if:
      - Clock drift within threshold, OR
      - We cannot measure (no utils.clock), OR
      - Env ALLOW_LIVE_ANYWAY=1/true.
    """
    if str(os.getenv(bypass_env, "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
        return True
    try:
        from utils.clock import check_clock_drift
        ok, skew, succ = check_clock_drift(max_skew_seconds=max_clock_skew_s)  # type: ignore
        return bool(ok or succ == 0)
    except Exception:
        # If the check fails, do not hard-block.
        return True
