# scripts/oco_watcher.py
"""
Polls order book and cancels the sibling order in each OCO group
as soon as STOP or TARGET is 'complete'.

Run once (default) or loop during market hours:
  python -m scripts.oco_watcher --loop --interval 5
"""
from __future__ import annotations
import time, argparse, json, random
from typing import Dict, Any, Tuple, List
from loguru import logger

from core.login import restore_or_login
from config import DRY_RUN, OCO_REGISTRY_JSON
from utils.oco_registry import all_groups, mark_closed

# ---------------- helpers: order book shapes & cancel ----------------

_OPEN_OR_DONE = {
    "open",
    "trigger pending",
    "put order req received",
    "put order received",
    "after market order req received",
    "after market order received",
    "complete",
    "cancelled",
    "rejected",
}

def _normalize_ob(resp: Any) -> List[Dict[str, Any]]:
    """Return a list of order rows from any SmartAPI variant."""
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        data = resp.get("data")
        if isinstance(data, list):
            return data
    return []

def _order_map(ob_rows: List[Dict[str, Any]]) -> Dict[str, Tuple[str, str]]:
    """returns {orderid: (status_lower, variety_upper)}"""
    m: Dict[str, Tuple[str, str]] = {}
    for row in ob_rows or []:
        oid = str(row.get("orderid") or row.get("orderId") or "").strip()
        if not oid:
            continue
        st = str(row.get("status") or row.get("orderstatus") or "").strip().lower()
        var = str(row.get("variety") or "").strip().upper()
        m[oid] = (st, var)
    return m

def _looks_transient(err: str) -> bool:
    msg = (err or "").lower()
    needles = (
        "timed out","timeout","read timeout","temporarily unavailable",
        "bad gateway","service unavailable","max retries exceeded",
        "connection aborted","502","503","504",
        "couldn't parse the json","jsondecode","expecting value","b''",
        "exceed"
    )
    return any(n in msg for n in needles)

def _sleep_backoff(i: int) -> None:
    time.sleep(min(0.5 * i, 2.5) + random.uniform(0.12, 0.4))

def _cancel_variants(smart, orderid: str, variety: str) -> Dict[str, Any]:
    """
    Try cancelOrder with:
      - kwargs: cancelOrder(variety=..., orderid=...)
      - positional: cancelOrder(orderid, variety)
      - positional: cancelOrder(variety, orderid)
    Normalize to dict.
    """
    fn = getattr(smart, "cancelOrder", None)
    if not callable(fn):
        return {"status": False, "message": "cancelOrder missing", "orderid": orderid}

    # 1) kwargs
    try:
        r = fn(variety=variety, orderid=orderid)
        return r if isinstance(r, dict) else {"status": True, "raw": r}
    except TypeError:
        pass
    except Exception as e:
        return {"status": False, "message": str(e)}

    # 2) positional (orderid, variety)
    try:
        r = fn(orderid, variety)
        return r if isinstance(r, dict) else {"status": True, "raw": r}
    except TypeError:
        pass
    except Exception as e:
        return {"status": False, "message": str(e)}

    # 3) positional (variety, orderid)
    try:
        r = fn(variety, orderid)
        return r if isinstance(r, dict) else {"status": True, "raw": r}
    except Exception as e:
        return {"status": False, "message": str(e)}

def _cancel_safe(smart, oid: str, variety_hint: str, retries: int = 3) -> Dict[str, Any]:
    """Cancel with retries + AMO→NORMAL fallback."""
    variety = (variety_hint or "NORMAL").upper()
    last = None
    for i in range(1, max(1, retries) + 1):
        resp = _cancel_variants(smart, oid, variety)
        ok = bool(resp.get("status")) or (str(resp.get("message","")).strip().upper() == "SUCCESS")
        if ok:
            return resp
        last = resp
        msg = str(resp.get("message") or resp.get("raw") or "")
        if _looks_transient(msg) and i < retries:
            logger.warning(f"Transient cancel error for {oid}: {msg} (retry {i}/{retries})")
            _sleep_backoff(i)
            continue
        # AMO fallback → NORMAL
        if variety == "AMO":
            logger.info(f"{oid}: AMO cancel failed; retry as NORMAL")
            resp2 = _cancel_variants(smart, oid, "NORMAL")
            ok2 = bool(resp2.get("status")) or (str(resp2.get("message","")).strip().upper() == "SUCCESS")
            return resp2 if ok2 else {"status": False, "message": "AMO->NORMAL failed", "first": resp, "second": resp2}
        break
    return last or {"status": False, "message": "cancel failed"}

# ---------------- core pass ----------------

def _one_pass(smart, *, dry_run: bool) -> int:
    """
    Returns number of groups acted upon in this pass.
    """
    reg = all_groups()
    if not reg:
        logger.info("No OCO groups in registry.")
        return 0

    # Fetch order book robustly
    try:
        ob = smart.orderBook()
    except Exception as e:
        logger.error(f"orderBook() failed: {e}")
        return 0

    rows = _normalize_ob(ob)
    if not rows:
        logger.info(f"order book empty/unavailable: {ob}")
        return 0

    omap = _order_map(rows)
    acted = 0

    for gid, g in reg.items():
        if g.get("closed"):
            continue

        stop = g.get("stop") or {}
        targ = g.get("target") or {}

        stop_id  = str(stop.get("orderid") or "").strip()
        targ_id  = str(targ.get("orderid") or "").strip()
        stop_var = str(stop.get("variety") or "STOPLOSS").upper()
        targ_var = str(targ.get("variety") or "NORMAL").upper()

        if not stop_id and not targ_id:
            continue

        stop_state = omap.get(stop_id, ("", stop_var))
        targ_state = omap.get(targ_id, ("", targ_var))

        # If STOP is complete -> cancel TARGET
        if stop_state[0] == "complete" and targ_id:
            if dry_run or DRY_RUN:
                logger.info(f"[DRY-RUN] Would cancel TARGET {targ_id} (variety={targ_var}) for group {gid}")
                mark_closed(gid, "exit_by_stop")
                acted += 1
            else:
                resp = _cancel_safe(smart, targ_id, targ_var)
                logger.info(f"Cancel TARGET {targ_id} => {resp}")
                mark_closed(gid, "exit_by_stop")
                acted += 1
            continue

        # If TARGET is complete -> cancel STOP
        if targ_state[0] == "complete" and stop_id:
            if dry_run or DRY_RUN:
                logger.info(f"[DRY-RUN] Would cancel STOP {stop_id} (variety={stop_var}) for group {gid}")
                mark_closed(gid, "exit_by_target")
                acted += 1
            else:
                resp = _cancel_safe(smart, stop_id, stop_var)
                logger.info(f"Cancel STOP {stop_id} => {resp}")
                mark_closed(gid, "exit_by_target")
                acted += 1
            continue

        # If either leg is already cancelled/rejected/complete, and the other is missing from book,
        # conservatively mark closed to avoid repeated churn (optional).
        if stop_id and stop_state[0] in {"cancelled", "rejected"} and not targ_id:
            mark_closed(gid, "stop_final_other_missing")
            acted += 1
        if targ_id and targ_state[0] in {"cancelled", "rejected"} and not stop_id:
            mark_closed(gid, "target_final_other_missing")
            acted += 1

    logger.info(f"OCO watcher pass done. acted_on={acted}")
    return acted

# ---------------- CLI / main ----------------

def _build_cli():
    p = argparse.ArgumentParser(description="OCO watcher: cancel sibling order when one completes")
    p.add_argument("--loop", action="store_true", help="Run continuously (daemon-like)")
    p.add_argument("--interval", type=int, default=5, help="Seconds between passes in loop mode (default 5)")
    p.add_argument("--dry-run", action="store_true", help="Force dry-run regardless of config.DRY_RUN")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    return p

def main(argv: list[str] | None = None):
    args = _build_cli().parse_args(argv)
    logger.remove()
    logger.add(lambda m: print(m, end=""), level=("DEBUG" if args.verbose else "INFO"))

    smart = restore_or_login()

    if not args.loop:
        _one_pass(smart, dry_run=args.dry_run)
        return

    # loop
    try:
        while True:
            _one_pass(smart, dry_run=args.dry_run)
            time.sleep(max(1, int(args.interval)))
    except KeyboardInterrupt:
        logger.info("OCO watcher stopping (Ctrl+C).")

if __name__ == "__main__":
    main()
