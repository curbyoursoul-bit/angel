# utils/pnl_guard.py
from __future__ import annotations
import os
import json
from pathlib import Path
from datetime import datetime, date
from loguru import logger

# ---- limits: prefer config.py, fallback to env ----
def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _get_limits() -> tuple[float, float]:
    """
    Returns (WARN, HARD). Both are negative numbers (rupees).
    Prefers config.RISK_* if available; else ENV.
    """
    warn = -1500.0
    hard = -2000.0
    try:
        # If you have these in config.py, we honor them
        from config import RISK_WARN_LOSS as _WARN  # type: ignore
        from config import RISK_MAX_LOSS as _HARD  # type: ignore
        warn = float(_WARN)
        hard = float(_HARD)
    except Exception:
        warn = _float_env("RISK_WARN_LOSS", warn)
        hard = _float_env("RISK_MAX_LOSS", hard)
    # sanity: ensure warn is not past hard (i.e., warn >= hard since they are negative)
    if warn < hard:
        warn = hard
    return warn, hard

# ---- sticky flag (per-day) ----
def _flag_path() -> Path:
    # Set PNL_GUARD_FLAG_PATH to override; default ./data/pnl_guard.flag
    p = os.getenv("PNL_GUARD_FLAG_PATH", "data/pnl_guard.flag")
    path = Path(p)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path

def _today_str() -> str:
    return date.today().isoformat()

def check_flag_today() -> bool:
    """
    True if a hard-breach flag exists for *today*.
    """
    fp = _flag_path()
    if not fp.exists():
        return False
    try:
        payload = json.loads(fp.read_text() or "{}")
        return payload.get("date") == _today_str()
    except Exception:
        # If unreadable but present, be conservative and block
        return True

def mark_flag(current_pnl: float, note: str = "hard_breach") -> None:
    """
    Persist today's breach so restarts won't re-enable trading.
    """
    fp = _flag_path()
    data = {
        "date": _today_str(),
        "pnl": float(current_pnl),
        "note": note,
        "ts": datetime.now().isoformat(timespec="seconds"),
    }
    fp.write_text(json.dumps(data, indent=2))
    logger.critical(f"PNL guard flagged for today at PnL ₹{current_pnl:.2f} → {fp}")

def clear_flag() -> bool:
    """
    Remove the sticky flag (manual override). Returns True on success.
    """
    fp = _flag_path()
    try:
        if fp.exists():
            fp.unlink()
            logger.info(f"PNL guard flag cleared: {fp}")
        return True
    except Exception as e:
        logger.error(f"Failed to clear PNL guard flag: {e}")
        return False

# ---- public API ----
def breach_level(current_pnl: float) -> str:
    """
    Returns "HARD", "WARN", or "OK".
    HARD → at/under RISK_MAX_LOSS
    WARN → at/under RISK_WARN_LOSS (but above HARD)
    """
    warn, hard = _get_limits()
    if current_pnl <= hard:
        return "HARD"
    if current_pnl <= warn:
        return "WARN"
    return "OK"

def is_breached(current_pnl: float, *, sticky: bool = True) -> bool:
    """
    Returns True if trading should be blocked now.
    - If sticky and a flag exists for *today*, block immediately.
    - Otherwise, check thresholds; on HARD breach, optionally set the sticky flag.
    """
    # Sticky block already set for today?
    if sticky and check_flag_today():
        logger.error("PNL guard sticky flag is set for today — blocking new orders.")
        return True

    lvl = breach_level(current_pnl)
    if lvl == "HARD":
        warn, hard = _get_limits()
        logger.error(f"Day P&L ₹{current_pnl:.2f} <= hard limit ₹{hard:.2f} — blocking new orders.")
        if sticky and os.getenv("PNL_GUARD_STICKY", "1").strip().lower() in {"1","true","yes","y"}:
            mark_flag(current_pnl, note="auto_hard_breach")
        return True

    if lvl == "WARN":
        warn, _ = _get_limits()
        logger.warning(f"Day P&L ₹{current_pnl:.2f} <= warn limit ₹{warn:.2f} — proceed with caution.")
        return False

    # OK
    return False
