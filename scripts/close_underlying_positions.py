# scripts/close_underlying_positions.py
from __future__ import annotations
import argparse
from typing import Any, Dict, List, Optional
from loguru import logger

from core.login import restore_or_login
from core.broker import place_batch, preview

# -------- positions helpers (SDK variants safe) --------
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
    Works across SmartAPI variants:
      - dict: {"status": True, "data": [...]}
      - plain list: [...]
      - method names: positionBook / positions / position / getPositions
    """
    resp = _call_first(smart, ["positionBook", "positions", "position", "getPositions"])
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        data = resp.get("data")
        return data if isinstance(data, list) else []
    return []

# -------- close-order builder --------
def _is_option_symbol(tsym: str) -> bool:
    t = tsym.upper()
    return ("CE" in t or "PE" in t)

def _side_to_flatten(qty: int) -> str:
    # positive netqty => SELL to flatten; negative => BUY to flatten
    return "SELL" if qty > 0 else "BUY"

def _close_orders_from_positions(
    rows: List[Dict[str, Any]],
    *,
    underlying: str = "BANKNIFTY",
    product_filter: Optional[List[str]] = None,
    segment_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Build market close orders for open option legs matching `underlying`.
    """
    underlying = (underlying or "").upper()
    pf = {p.upper() for p in (product_filter or [])}
    sf = {s.upper() for s in (segment_filter or [])}

    orders: List[Dict[str, Any]] = []
    for r in rows:
        tsym = str(r.get("tradingsymbol") or r.get("symbol") or "")
        if not tsym or underlying not in tsym.upper():
            continue
        if not _is_option_symbol(tsym):
            continue

        try:
            netqty = int(r.get("netqty") or r.get("netQty") or r.get("quantity") or 0)
        except Exception:
            netqty = 0
        if netqty == 0:
            continue

        exch = str(r.get("exchange") or r.get("exch_seg") or "NFO").upper()
        prod = str(r.get("producttype") or r.get("product") or "INTRADAY").upper()
        tok  = str(r.get("symboltoken") or r.get("token") or "")

        if pf and prod not in pf:
            continue
        if sf and exch not in sf:
            continue

        orders.append({
            "variety": "NORMAL",
            "tradingsymbol": tsym,
            "symboltoken": tok,
            "transactiontype": _side_to_flatten(netqty),
            "exchange": exch,
            "ordertype": "MARKET",
            "producttype": prod,         # keep same product if we can
            "duration": "DAY",
            "quantity": abs(int(netqty)),
            # price/trigger omitted for MARKET; core.broker will normalize
        })
    return orders

# -------- CLI / main --------
def _build_cli():
    p = argparse.ArgumentParser(description="Close open option legs for an underlying (SmartAPI)")
    p.add_argument("--underlying", default="BANKNIFTY", help="Underlying name to match in tradingsymbol")
    p.add_argument("--product", action="append", help="Filter product types (e.g., INTRADAY). Can repeat.")
    p.add_argument("--segment", action="append", help="Filter exchange segments (e.g., NFO, NSE). Can repeat.")
    p.add_argument("--dry-run", dest="dry_run", action="store_true", help="Preview orders only (default)")
    p.add_argument("--live", dest="live", action="store_true", help="Place live orders")
    p.add_argument("--rollback", action="store_true", help="If any order fails, cancel previously placed ones")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    return p

def main(argv: Optional[List[str]] = None):
    args = _build_cli().parse_args(argv)
    logger.remove()
    logger.add(lambda m: print(m, end=""), level=("DEBUG" if args.verbose else "INFO"))

    mode = "LIVE" if args.live else "DRY_RUN"
    logger.info(f"Mode: {mode}")

    s = restore_or_login()

    rows = _get_positions_rows(s)
    if not rows:
        logger.info("No positions returned by broker API.")
        return 0

    orders = _close_orders_from_positions(
        rows,
        underlying=args.underlying,
        product_filter=args.product,
        segment_filter=args.segment,
    )

    if not orders:
        print(f"No {args.underlying} option positions to close (filters may have excluded rows).")
        return 0

    print("Close orders (normalized preview):")
    for o in orders:
        print(preview(o))

    if mode == "DRY_RUN":
        print("\nDRY_RUN: no orders placed.")
        return 0

    batch = place_batch(
        s,
        orders,
        mode=("rollback" if args.rollback else "continue"),
        dry_run=False,
    )

    print("\nBatch result:")
    print(batch)
    status = batch.get("overall", "error")
    return 0 if status in ("success",) else 1

if __name__ == "__main__":
    raise SystemExit(main())
