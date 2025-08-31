# tools/square_off_by_tag.py
from __future__ import annotations

import os, sys, json, argparse
from pathlib import Path
from math import gcd, floor
from typing import Dict, List
from loguru import logger
from dotenv import load_dotenv

# Make package imports work with `python -m`
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.login import restore_or_login
from core.broker import place_batch
from utils.market_hours import is_market_open, now_ist


# ----------------------- file IO -----------------------

def _read_tag_file(tag: str) -> List[Dict]:
    fp = Path("data") / "tags" / f"{tag}.json"
    if not fp.exists():
        raise FileNotFoundError(f"Tag file not found: {fp}")
    legs = json.loads(fp.read_text() or "[]")
    if not isinstance(legs, list):
        raise ValueError("Tag file must contain a JSON list of legs")
    return legs


# ----------------------- tiny utils -----------------------

def _reverse_side(side: str) -> str:
    s = str(side or "").strip().upper()
    if s == "BUY":
        return "SELL"
    if s == "SELL":
        return "BUY"
    return s

def _try_call(smart, names: List[str]):
    for nm in names:
        fn = getattr(smart, nm, None)
        if fn:
            try:
                return fn()
            except Exception as e:
                logger.debug(f"{nm} failed: {e}")
    return None

def _fetch_positions(smart) -> List[Dict]:
    """
    Get a flat list of today's/net positions, tolerant to SDK variants.
    """
    resp = _try_call(smart, ["position", "getPosition", "positions", "getPositions", "positionBook"])
    rows: List[Dict] = []

    if isinstance(resp, list):
        rows = resp
    elif isinstance(resp, dict):
        data = resp.get("data") or resp.get("Data") or {}
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            for k in ("netPositions", "NetPosition", "net", "day", "positions", "DayPosition"):
                v = data.get(k)
                if isinstance(v, list):
                    rows.extend(v)
    return rows

def _qty_int(*vals) -> int:
    for v in vals:
        try:
            if v is None:
                continue
            return int(float(v))
        except Exception:
            continue
    return 0

def _str_up(x) -> str:
    return str(x or "").strip().upper()

def _str_norm(x) -> str:
    return str(x or "").strip()

def _is_option_symbol(tsym: str) -> bool:
    u = (tsym or "").upper()
    return ("CE" in u or "PE" in u) and any(k in u for k in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"))

def _exchange_for_leg(leg: Dict) -> str:
    exch = _str_up(leg.get("exchange"))
    if not exch:
        ts = _str_up(leg.get("tradingsymbol"))
        exch = "NFO" if _is_option_symbol(ts) else "NSE"
    return exch


# ----------------------- reconciliation -----------------------

def _net_open_qty_for_leg(pos_rows: List[Dict], leg: Dict) -> int:
    """
    Compute *closable* quantity for a leg, based on live positions.
    If the tag leg was SELL, we close BUY up to current net short.
    If the tag leg was BUY, we close SELL up to current net long.
    """
    ts = _str_up(leg.get("tradingsymbol"))
    tok = _str_norm(leg.get("symboltoken"))

    # Aggregate matches (by token or tradingsymbol)
    buy_qty = sell_qty = 0
    for r in pos_rows:
        rts = _str_up(r.get("tradingsymbol") or r.get("TradingSymbol") or r.get("symbol"))
        rtok = _str_norm(r.get("symboltoken") or r.get("symbolToken") or r.get("Token"))

        if tok and rtok and tok == rtok:
            pass
        elif ts and rts and ts == rts:
            pass
        else:
            continue

        buy_qty += _qty_int(r.get("buyqty"), r.get("buyQty"), r.get("BuyQty"))
        sell_qty += _qty_int(r.get("sellqty"), r.get("sellQty"), r.get("SellQty"))

    orig_side = _str_up(leg.get("transactiontype"))
    if orig_side == "SELL":
        # You are short; you can buy back at most (sells - buys)
        return max(sell_qty - buy_qty, 0)
    elif orig_side == "BUY":
        # You are long; you can sell at most (buys - sells)
        return max(buy_qty - sell_qty, 0)
    return 0

def _infer_lot_size_for_symbol(group_legs: List[Dict], live_open_qty: int | None) -> int:
    """
    Infer lot size without instruments CSV.
    Take the GCD of all tagged quantities for the symbol and of live open qty (if present).
    """
    g = 0
    for leg in group_legs:
        q = _qty_int(leg.get("quantity"))
        if q > 0:
            g = q if g == 0 else gcd(g, q)
    if live_open_qty and live_open_qty > 0:
        g = live_open_qty if g == 0 else gcd(g, live_open_qty)
    return max(g, 1)

def _group_legs_by_symbol(legs: List[Dict]) -> Dict[str, List[Dict]]:
    groups: Dict[str, List[Dict]] = {}
    for leg in legs:
        key = _str_up(leg.get("tradingsymbol")) or _str_norm(leg.get("symboltoken")) or json.dumps(leg, sort_keys=True)
        groups.setdefault(key, []).append(leg)
    return groups

def _compute_close_qty(
    *,
    leg: Dict,
    lot_size: int,
    reconcile: bool,
    live_open_qty: int | None,
    close_lots: int | None,
    close_percent: float | None,
) -> int:
    if lot_size < 1:
        lot_size = 1

    tag_qty = _qty_int(leg.get("quantity"))
    base = live_open_qty if (reconcile and live_open_qty is not None) else tag_qty
    base = max(base, 0)
    if base <= 0:
        return 0

    if close_lots is not None:
        want = close_lots * lot_size
    elif close_percent is not None:
        lots = floor((base * (close_percent / 100.0)) / lot_size)
        want = lots * lot_size
    else:
        want = base  # full close

    want = min(want, base)
    want = (want // lot_size) * lot_size
    return max(want, 0)


# ----------------------- main flow -----------------------

def main(argv=None):
    load_dotenv()
    ap = argparse.ArgumentParser(description="Square-off previously tagged legs")
    ap.add_argument("--tag", required=True, help="Tag name used while placing entries")
    ap.add_argument("--amo", action="store_true", help="Mark orders as AMO")
    ap.add_argument("--dry-run", action="store_true", help="Preview only")
    ap.add_argument("--rollback", action="store_true", help="Rollback if a leg fails")
    ap.add_argument("--only-if-market-open", action="store_true", help="Skip when market closed (ignored if --amo)")
    ap.add_argument("--no-reconcile", action="store_true", help="Do NOT reconcile with current live positions")
    ap.add_argument("--close-lots", type=int, help="Close exactly this many lots per leg")
    ap.add_argument("--close-percent", type=float, help="Close N%% of the open qty, rounded down to lot multiples")
    args = ap.parse_args(argv)

    # Arg validation
    if args.close_lots is not None and args.close_percent is not None:
        logger.error("Use either --close-lots or --close-percent, not both.")
        return 2
    if args.close_percent is not None and not (0.0 <= args.close_percent <= 100.0):
        logger.error("--close-percent must be between 0 and 100.")
        return 2

    # Market-hours guard
    if args.only_if_market_open and not args.amo and not is_market_open():
        logger.warning(f"Market closed at {now_ist().strftime('%Y-%m-%d %H:%M:%S')} IST. Skipping.")
        return 0

    # Read tag legs
    try:
        legs = _read_tag_file(args.tag)
    except Exception as e:
        logger.error(str(e))
        return 2

    if not legs:
        logger.warning("No legs in tag file.")
        print(json.dumps({"mode": "continue", "overall": "success", "results": []}, indent=2))
        return 0

    # Session
    smart = restore_or_login()

    # Reconcile?
    pos_rows = _fetch_positions(smart) if not args.no_reconcile else []

    # Group legs by symbol to infer lot size robustly
    groups = _group_legs_by_symbol(legs)

    orders: List[Dict] = []
    for _, sym_legs in groups.items():
        # For inference, try a live qty from the first leg
        live_for_infer = 0
        if pos_rows:
            live_for_infer = _net_open_qty_for_leg(pos_rows, sym_legs[0])
        lot_size = _infer_lot_size_for_symbol(sym_legs, live_for_infer)

        for leg in sym_legs:
            live_open = _net_open_qty_for_leg(pos_rows, leg) if pos_rows else None
            qty_to_close = _compute_close_qty(
                leg=leg,
                lot_size=lot_size,
                reconcile=not args.no_reconcile,
                live_open_qty=live_open,
                close_lots=args.close_lots,
                close_percent=args.close_percent,
            )
            if qty_to_close <= 0:
                logger.warning(f"Skip leg (nothing to close after partial/reconcile): {leg}")
                continue

            orders.append({
                "variety": "NORMAL",
                "tradingsymbol": _str_up(leg.get("tradingsymbol")),
                "symboltoken": _str_norm(leg.get("symboltoken")),
                "transactiontype": _reverse_side(leg.get("transactiontype")),
                "exchange": _exchange_for_leg(leg),
                "ordertype": "MARKET",
                "producttype": _str_up(leg.get("producttype")) or "INTRADAY",
                "duration": "DAY",
                "price": 0,
                "quantity": qty_to_close,
                "ordertag": f"{args.tag}_SQUAREOFF",
                "amo": "YES" if args.amo else "NO",
            })

    if not orders:
        print("No valid square-off orders to place (after partial/reconcile).")
        return 0

    # Place (or preview)
    batch = place_batch(
        smart,
        orders,
        mode="rollback" if args.rollback else "continue",
        dry_run=args.dry_run,
    )
    print(json.dumps(batch, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
