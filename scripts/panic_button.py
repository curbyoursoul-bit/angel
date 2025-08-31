# scripts/panic_button.py
from __future__ import annotations
import argparse
import os
from time import sleep
from typing import Any, Dict, List, Optional
from loguru import logger

from core.login import restore_or_login
from core.broker import place_batch, preview


# ---------- helpers: positions across SDK variants ----------

def _call_first(smart, names: List[str]):
    for n in names:
        fn = getattr(smart, n, None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                pass
    return None

def _get_positions_rows(smart) -> List[Dict[str, Any]]:
    """
    Accepts a variety of SmartAPI shapes:
      - dict: {"status": True, "data": [...]}
      - dict: {"status": True, "data": {"netPositions": [...]}}
      - list: [...]
      - method names: positionBook / positions / position / getPositions
    """
    resp = _call_first(smart, ["positionBook", "positions", "position", "getPositions"])
    if resp is None:
        return []

    if isinstance(resp, list):
        return resp

    if isinstance(resp, dict):
        data = resp.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("netPositions", "positions", "NetPositions"):
                arr = data.get(key)
                if isinstance(arr, list):
                    return arr
        return []

    return []


# ---------- order builder ----------

def _to_int(x, default=0) -> int:
    try:
        # handle '5.0' safely
        f = float(x)
        return int(f) if f.is_integer() else int(round(f))
    except Exception:
        try:
            return int(x)
        except Exception:
            return default

def _mk_close_order(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Convert a positions row to a MARKET close order (flatten net exposure).
    Returns None if net qty is zero or symbol info missing.
    """
    netqty = _to_int(row.get("netqty") or row.get("netQty") or row.get("net_quantity") or 0, 0)
    if netqty == 0:
        return None

    exch = str(row.get("exchange") or row.get("exch_seg") or row.get("exch") or "NSE").upper()
    tsym = str(row.get("tradingsymbol") or row.get("tradingSymbol") or row.get("symbol") or row.get("symbolname") or "").strip()
    token = str(row.get("symboltoken") or row.get("token") or "").strip()
    product = str(row.get("producttype") or row.get("productType") or row.get("product") or "INTRADAY").upper()

    if not tsym or not token:
        return None

    side = "SELL" if netqty > 0 else "BUY"
    qty = abs(netqty)

    # Minimal, broker-normalized payload (price/trigger omitted for MARKET)
    return {
        "variety": "NORMAL",
        "tradingsymbol": tsym,
        "symboltoken": token,
        "transactiontype": side,
        "exchange": exch,
        "ordertype": "MARKET",
        "producttype": product,
        "duration": "DAY",
        "quantity": qty,
        # optional metadata helpful for auditing
        "ordertag": "PANIC_SQUAREOFF",
    }


# ---------- core API ----------

def close_all_positions(
    smart=None,
    *,
    dry_run: Optional[bool] = None,
    include_products: Optional[List[str]] = None,
    include_segments: Optional[List[str]] = None,
    max_legs: Optional[int] = None,
    rollback: bool = False,
) -> Dict[str, Any]:
    """
    Build and place MARKET close orders to flatten all open positions.

    include_products: e.g., ["INTRADAY", "MIS"]. If None, close all products.
    include_segments: e.g., ["NFO", "NSE"]. If None, all segments.
    max_legs       : safety cap on number of orders to submit.
    rollback       : if True, cancel already-placed legs when any later leg fails.
    """
    smart = smart or restore_or_login()
    if dry_run is None:
        dry_run = str(os.getenv("DRY_RUN", "")).strip().lower() in {"1", "true", "yes", "y"}

    rows = _get_positions_rows(smart)
    if not rows:
        logger.info("No positions returned by broker API.")
        return {"status": True, "message": "No positions", "closed": [], "failed": []}

    pf = {p.upper() for p in (include_products or [])}
    sf = {s.upper() for s in (include_segments or [])}

    orders: List[Dict[str, Any]] = []
    for r in rows:
        o = _mk_close_order(r)
        if not o:
            continue
        if pf and o.get("producttype", "").upper() not in pf:
            continue
        if sf and o.get("exchange", "").upper() not in sf:
            continue
        orders.append(o)

    if not orders:
        logger.info("No qualifying positions to close.")
        return {"status": True, "message": "No positions", "closed": [], "failed": []}

    # Optional safety cap
    if max_legs is not None and len(orders) > int(max_legs):
        logger.warning(f"Legs to close ({len(orders)}) exceed max_legs={max_legs}; truncating.")
        orders = orders[: int(max_legs)]

    # Preview for logs
    logger.info(f"Square-off {len(orders)} leg(s) …")
    previews = [preview(o) for o in orders]

    if dry_run:
        for p in previews:
            logger.info(f"[DRY-RUN] {p}")
        return {"status": True, "message": "DRY_RUN", "closed": [{"order": p, "dry_run": True} for p in previews], "failed": []}

    # Live placement via unified batch API
    batch = place_batch(
        smart,
        orders,
        mode=("rollback" if rollback else "continue"),
        dry_run=False,
    )

    # Summarize
    ok = batch.get("overall") in ("success",)
    closed, failed = [], []
    for r in (batch.get("results") or []):
        item = {
            "request": r.get("request"),
            "normalized": r.get("normalized"),
            "response": r.get("response"),
            "status": r.get("status"),
            "order_id": r.get("order_id"),
        }
        if r.get("status") == "success":
            closed.append(item)
        else:
            failed.append({**item, "error": r.get("error")})

    return {
        "status": ok and not failed,
        "message": ("Done" if ok and not failed else ("rolled_back_due_to_failure" if batch.get("overall") == "rolled_back_due_to_failure" else "Some failed")),
        "closed": closed,
        "failed": failed,
        "batch": batch,
    }


def panic_squareoff(smart=None, **kwargs):
    """
    Backwards-compatible wrapper (used elsewhere in your code).
    kwargs may include: dry_run, include_products, include_segments, max_legs, rollback
    """
    smart = smart or restore_or_login()
    return close_all_positions(smart=smart, **kwargs)


# ---------- CLI ----------

def _build_cli():
    p = argparse.ArgumentParser(description="PANIC: square-off all open positions (SmartAPI)")
    p.add_argument("--live", action="store_true", help="Place live orders (default: DRY_RUN from env)")
    p.add_argument("--dry-run", action="store_true", help="Force dry run regardless of env")
    p.add_argument("--product", action="append", help="Filter product types (e.g., INTRADAY). Can repeat.")
    p.add_argument("--segment", action="append", help="Filter exchange segments (e.g., NFO, NSE). Can repeat.")
    p.add_argument("--max-legs", type=int, help="Safety cap on number of closing orders")
    p.add_argument("--rollback", action="store_true", help="If any order fails, cancel previously placed ones")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    return p

def main(argv: Optional[List[str]] = None):
    args = _build_cli().parse_args(argv)
    logger.remove()
    logger.add(lambda m: print(m, end=""), level=("DEBUG" if args.verbose else "INFO"))

    # DRY_RUN resolution priority: --dry-run flag > --live flag > env
    dry_run = True
    if args.dry_run:
        dry_run = True
    elif args.live:
        dry_run = False
    else:
        dry_run = str(os.getenv("DRY_RUN", "")).strip().lower() in {"1", "true", "yes", "y"}

    s = restore_or_login()
    res = close_all_positions(
        smart=s,
        dry_run=dry_run,
        include_products=args.product,
        include_segments=args.segment,
        max_legs=args.max_legs,
        rollback=args.rollback,
    )

    # Pretty print outcome
    print(res)
    return 0 if res.get("status") else 1


if __name__ == "__main__":
    raise SystemExit(main())
