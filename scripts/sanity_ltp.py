# scripts/sanity_ltp.py
from __future__ import annotations
from pathlib import Path
import sys

# --- bootstrap project root on sys.path ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.executor import Executor
from core.login import restore_or_login

def ensure_tokens(exe, orders):
    """Ensure each order has symboltoken; DRY_RUN preview fills it if missing."""
    if any(not od.get("symboltoken") for od in orders):
        exe.run("angel", "place_orders", orders=orders, mode="DRY_RUN")
    return orders

def ltp_from_order(exe, od):
    return exe.run(
        "angel", "ltp",
        exchange=od["exchange"],
        tradingsymbol=od["tradingsymbol"],
        symboltoken=str(od["symboltoken"]),
    )

def main():
    # one login/session; tools reuse via ctx.smart
    smart = restore_or_login()

    class EngineCtx:
        def __init__(self, smart):
            self.smart, self.cfg, self.risk = smart, None, None

    exe = Executor()
    exe.set_context(EngineCtx(smart))

    # run strategy → ensure tokens
    resp = exe.run("strategy", "run", strategy="atm_straddle", params={"lots": 1})
    if not resp.get("ok"):
        raise SystemExit(f"strategy error: {resp.get('error')}")
    orders = (resp["data"] or {}).get("orders") or []
    ensure_tokens(exe, orders)

    # print both LTPs
    for od in orders:
        print(ltp_from_order(exe, od))

if __name__ == "__main__":
    main()
