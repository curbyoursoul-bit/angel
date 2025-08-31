# scripts/exit_at_time.py
from __future__ import annotations
import argparse
from typing import Tuple
from loguru import logger

from core.login import restore_or_login
from utils.market_hours import now_ist
from config import (
    EXIT_ON_TIME_ENABLED,
    EXIT_SQUARE_OFF,
    EXIT_TIME_IST,
    EXIT_WINDOW_MINUTES,
)

# Support either module name (your tree had both patterns across snippets)
try:
    from scripts.cancel_all_open import cancel_all_open_orders  # type: ignore
except Exception:
    try:
        from scripts.cancel_open_orders import cancel_all_open_orders  # type: ignore
    except Exception:
        cancel_all_open_orders = None  # type: ignore

from scripts.panic_button import panic_squareoff  # your existing function


def _parse_hhmm(s: str) -> Tuple[int, int]:
    s = (s or "").strip()
    parts = s.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid time '{s}'. Expected HH:MM (IST).")
    hh, mm = parts
    h = int(hh)
    m = int(mm)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"Invalid time '{s}'. Hour 0–23, minute 0–59.")
    return h, m


def _within_window(target_hhmm: str, window_min: int) -> bool:
    now = now_ist()
    h, m = _parse_hhmm(target_hhmm)
    tgt = now.replace(hour=h, minute=m, second=0, microsecond=0)
    diff_min = abs((now - tgt).total_seconds()) / 60.0
    return diff_min <= float(max(0, window_min))


def main():
    ap = argparse.ArgumentParser(description="Timed exit: cancel orders (and optional square-off) around a target IST time")
    ap.add_argument("--force", action="store_true", help="Ignore EXIT_ON_TIME_ENABLED/window and run now")
    ap.add_argument("--at", help=f"Target time HH:MM IST (default from config: {EXIT_TIME_IST})")
    ap.add_argument("--window", type=int, help=f"Window minutes (default from config: {EXIT_WINDOW_MINUTES})")
    ap.add_argument("--square", dest="square", action="store_true", help="Force square-off positions too")
    ap.add_argument("--no-square", dest="square", action="store_false", help="Do not square-off (cancel only)")
    ap.add_argument("--dry-run", action="store_true", help="Log what would happen but do nothing")
    ap.set_defaults(square=None)
    args = ap.parse_args()

    target = (args.at or EXIT_TIME_IST or "15:20").strip()
    window = args.window if args.window is not None else int(EXIT_WINDOW_MINUTES)
    square = EXIT_SQUARE_OFF if args.square is None else bool(args.square)

    # Basic config sanity
    try:
        _parse_hhmm(target)
    except Exception as e:
        logger.error(f"Bad --at/EXIT_TIME_IST value: {e}")
        return

    if not args.force:
        if not EXIT_ON_TIME_ENABLED:
            print("Timed exit disabled (EXIT_ON_TIME_ENABLED=False). Use --force to override.")
            return
        if not _within_window(target, window):
            print(f"Not time yet (target {target} IST, window ±{window} min). No action.")
            return

    if args.dry_run:
        print(f"[DRY_RUN] Would cancel all open orders now (target {target} ±{window}m). Square-off={square}")
        return

    # Execute
    smart = restore_or_login()

    if cancel_all_open_orders is None:
        logger.error("cancel_all_open_orders not available (missing scripts.cancel_all_open/_open_orders).")
        return

    logger.info("Timed exit: cancel all open orders …")
    try:
        cancel_out = cancel_all_open_orders(smart)  # supports our robust wrapper
    except TypeError:
        # backwards compat if function signature lacks 'smart'
        cancel_out = cancel_all_open_orders()  # type: ignore

    print(cancel_out)

    if square:
        logger.info("Timed exit: square-off all positions …")
        out = panic_squareoff(smart=smart)
        # Align with your earlier UX
        if isinstance(out, dict) and out.get("message") == "No positions":
            print("No positions")

if __name__ == "__main__":
    main()
