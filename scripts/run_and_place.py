# scripts/run_and_place.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# --- make project root importable when run as a file (not -m) ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.executor import Executor
from core.login import restore_or_login
import config as cfg


def ensure_tokens(exe: Executor, orders):
    """
    If any order lacks symboltoken, preview once in DRY_RUN to let the Angel
    tool enrich them in-place. We don't care about the return value here.
    """
    if any(not o.get("symboltoken") for o in orders):
        # force DRY preview just for enrichment
        exe.run("angel", "place_orders", orders=orders, mode="DRY_RUN")
    return orders


def main() -> int:
    ap = argparse.ArgumentParser(description="Run strategy and place/preview orders")
    ap.add_argument("--strategy", "-s", default="atm_straddle", help="strategy name")
    ap.add_argument("--lots", "-l", type=int, default=1, help="number of lots")
    ap.add_argument("--mode", "-m", choices=["DRY_RUN", "LIVE"], default="DRY_RUN",
                    help="order mode (default: DRY_RUN)")
    args = ap.parse_args()

    # Single SmartAPI session
    smart = restore_or_login()

    # Minimal context object for Executor
    class Ctx:
        def __init__(self, smart):
            self.smart = smart
            self.cfg = cfg
            self.risk = None

    exe = Executor()
    exe.set_context(Ctx(smart))

    # Switch DRY/LIVE globally at runtime so utils/order_exec reads it
    cfg.DRY_RUN = (args.mode.upper() != "LIVE")

    # 1) Run strategy
    strat_res = exe.run("strategy", "run", strategy=args.strategy, params={"lots": args.lots})
    if not strat_res.get("ok"):
        print("[strategy] error:", strat_res.get("error"))
        return 2

    data = strat_res.get("data") or {}
    orders = data.get("orders") or []
    notes = (data.get("notes") or "").strip()

    print(f"[strategy] {args.strategy} lots={args.lots} | orders={len(orders)} | mode={'DRY_RUN' if cfg.DRY_RUN else 'LIVE'}")
    if notes:
        print(f"[notes] {notes}")
    if not orders:
        print("no orders generated")
        return 0

    # 2) Ensure symboltoken present
    ensure_tokens(exe, orders)

    # 3) Place or preview
    # AngelTool will also read args.mode and set cfg.DRY_RUN internally; passing mode here is fine.
    place_res = exe.run("angel", "place_orders", orders=orders, mode=args.mode)
    if not place_res.get("ok"):
        print("order error:", place_res.get("error"))
        return 3

    print("[DRY_RUN] result:" if cfg.DRY_RUN else "[LIVE] result:", place_res.get("data"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
