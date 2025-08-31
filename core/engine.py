# core/engine.py
from __future__ import annotations

import argparse
import os
from time import sleep
from typing import Callable, Dict, List, Any, Optional, Union
from loguru import logger

# --- login ---
from core.login import restore_or_login

# --- risk (import only; call inside run flow) ---
from core import risk

# --- optional external router (falls back to place_or_preview) ---
try:
    from execution.router import route as route_orders
    USE_ROUTER = True
except Exception:
    route_orders = None  # type: ignore
    USE_ROUTER = False

# --- optional signal->order builder (utils) ---
try:
    from utils.signal_router import build_orders_from_signal  # optional helper
except Exception:
    build_orders_from_signal = None  # type: ignore

# --- attach exits helper (one-shot mode) ---
try:
    from core.attach_exits import attach_exits_for_recent_shorts  # optional
except Exception:
    attach_exits_for_recent_shorts = None  # type: ignore

# --- config flags ---
try:
    from config import DRY_RUN, ENFORCE_MARKET_HOURS
except Exception:
    DRY_RUN = True
    ENFORCE_MARKET_HOURS = False

# --- market hours (defensive import) ---
try:
    from utils.market_hours import is_market_open
except Exception:
    def is_market_open() -> bool:
        logger.warning("is_market_open unavailable; assuming market OPEN for testing.")
        return True

# --- order executor (fallback to print) ---
try:
    from utils.order_exec import place_or_preview  # your executor wrapper
except Exception:
    def place_or_preview(_smart: Any, orders: List[dict]) -> None:
        logger.warning("[fallback] utils.order_exec.place_or_preview missing. Printing orders instead.")
        if isinstance(orders, dict):
            orders = [orders]
        for od in orders:
            logger.info(f"[DRY] Would place: {od}")

# --- strategy registry (single source of truth) ---
try:
    from core.strategy_registry import REGISTRY as STRATEGY_REGISTRY
    from core.strategy_registry import ALIASES
except Exception:
    STRATEGY_REGISTRY: Dict[str, Callable] = {}
    ALIASES: Dict[str, str] = {}

# -------------------------------
# Robust cancel helpers (AMO safe)
# -------------------------------

# Statuses that mean "can still be cancelled"
OPEN_STATUSES = {
    "open",
    "pending",
    "trigger pending",
    "open pending",
    "open pending,modify",
    "modify pending",
    "open pending,cancel",
    "put order req received",
    "put order received",
    "amo req received",
    "after market order req received",
    "after market order received",
}
ALLOWED_CANCEL = {"NORMAL", "STOPLOSS", "ROBO"}


def _canonical_cancel_variety(order_or_hint: Union[dict, str, None]) -> str:
    """
    Angel quirk: AMO is NOT a valid cancel variety.
    - Map AMO -> NORMAL
    - Pass through NORMAL/STOPLOSS/ROBO
    - Infer STOPLOSS from ordertype if it looks like SL/SL-M/STOPLOSS_LIMIT
    - Else default to NORMAL
    """
    if isinstance(order_or_hint, str):
        raw = (order_or_hint or "").upper()
        if raw == "AMO":
            return "NORMAL"
        if raw in ALLOWED_CANCEL:
            return raw
        return "NORMAL"

    o = order_or_hint or {}
    raw = (o.get("variety") or "").upper()
    if raw == "AMO":
        return "NORMAL"
    if raw in ALLOWED_CANCEL:
        return raw

    ot = (o.get("ordertype") or o.get("orderType") or "").upper().replace("-", "").replace("_", "")
    if ot.startswith("SL") or "STOPLOSS" in ot:
        return "STOPLOSS"
    return "NORMAL"


def _cancel_one(smart, order_id: str, variety_hint: Union[str, None], order_row: Union[dict, None] = None) -> dict:
    """
    Cancel an order using Angel's SmartAPI.
    - Compute canonical variety (AMO->NORMAL, infer STOPLOSS)
    - Try positional signature cancelOrder(order_id, variety)
    - Fallback to cancelOrder(variety, order_id)
    - Retry NORMAL once on typical errors
    """
    def _do(variety: str) -> dict:
        v = (variety or "NORMAL").upper()
        try:
            res = smart.cancelOrder(order_id, v)  # signature #1
            return res if isinstance(res, dict) else {"status": True, "raw": res}
        except TypeError:
            try:
                res = smart.cancelOrder(v, order_id)  # signature #2 (older SDKs)
                return res if isinstance(res, dict) else {"status": True, "raw": res}
            except Exception as e2:
                return {"status": False, "message": str(e2), "variety": v, "orderid": order_id}
        except Exception as e:
            return {"status": False, "message": str(e), "variety": v, "orderid": order_id}

    v1 = _canonical_cancel_variety(order_row or variety_hint)
    out1 = _do(v1)
    if out1.get("status"):
        return out1

    if v1 != "NORMAL":
        out2 = _do("NORMAL")
        if out2.get("status"):
            return out2
        if any(s in str(out2.get("message", "")).lower() for s in ("exceed", "limit", "timeout")):
            sleep(0.6)
            return _do("NORMAL")
        return out2

    if any(s in str(out1.get("message", "")).lower() for s in ("exceed", "limit", "timeout")):
        sleep(0.6)
        return _do("NORMAL")

    return out1


def cancel_all_open_before_trading(smart, preserve_amo: bool = False, dry_run: bool = False) -> None:
    """Fetch order book and cancel any lingering open/pending orders (AMO-safe)."""
    try:
        ob = smart.orderBook()
    except Exception as e:
        logger.warning(f"Could not fetch order book pre-trade: {e}")
        return

    data = (ob or {}).get("data")
    if not (isinstance(data, list) and data):
        logger.info("Order book unavailable or empty; continuing.")
        return

    pending: List[dict] = []
    for row in data:
        status = str(row.get("status", "")).strip().lower()
        if status not in OPEN_STATUSES:
            continue

        variety = (row.get("variety") or row.get("ordervariety") or "").upper()
        if preserve_amo and variety == "AMO":
            continue

        pending.append(
            {
                "orderid": row.get("orderid") or row.get("order_id") or row.get("orderID"),
                "variety": variety or None,
                "ordertype": row.get("ordertype") or row.get("orderType"),
                "tradingsymbol": row.get("tradingsymbol") or row.get("tradingSymbol") or row.get("symbol"),
                "qty": row.get("quantity") or row.get("qty"),
                "_raw": row,
            }
        )

    if not pending:
        logger.info("No open orders to cancel.")
        return

    logger.warning(f"Found {len(pending)} open orders. Cancelling before placing new trades...")
    for p in pending:
        oid = p["orderid"]
        if not oid:
            continue
        vraw = p["variety"]
        vcan = _canonical_cancel_variety(p)

        if dry_run:
            logger.info(f"[CANCEL-DRY] Would cancel {oid} ({p['tradingsymbol']}, qty={p['qty']}, variety={vraw} → {vcan})")
            continue

        logger.info(f"Cancel → {oid} ({p['tradingsymbol']}, qty={p['qty']}, variety={vraw} → {vcan})")
        res = _cancel_one(smart, oid, vraw, order_row=p)
        if res.get("status"):
            logger.success(f"Cancelled {oid}")
        else:
            msg = res.get("message") or ""
            logger.error(f"Failed to cancel {oid}: {msg or res}")

# -------------------------------
# Runner helpers
# -------------------------------
def _banner() -> None:
    mode = "LIVE" if not DRY_RUN else "DRY RUN"
    line = "=" * 54
    warn = "(orders WILL be placed!)" if mode == "LIVE" else "(no orders will be placed)"
    print(f"\n{line}\n  MODE: {mode}  {warn}\n{line}\n")


def _apply_overrides(
    symbols: Optional[str],
    interval: Optional[str],
    bars: Optional[int],
    qty: Optional[int],
) -> None:
    """Set env overrides consumed by strategies (keeps them decoupled)."""
    if symbols is not None:
        os.environ["STRAT_SYMBOLS"] = symbols
        os.environ["ENGINE_SYMBOLS"] = symbols  # for newer templates
        os.environ["SYMBOLS"] = symbols
    if interval is not None:
        os.environ["STRAT_INTERVAL"] = interval
        os.environ["ENGINE_INTERVAL"] = interval
        os.environ["INTERVAL"] = interval
    if bars is not None:
        os.environ["STRAT_BARS"] = str(bars)
        os.environ["ENGINE_BARS"] = str(bars)
        os.environ["BARS"] = str(bars)
    if qty is not None:
        os.environ["STRAT_QTY"] = str(qty)


def _inject_amo_flag(orders: Optional[List[dict]], amo: bool) -> List[dict]:
    if not orders:
        return []
    if not amo:
        return list(orders)
    out: List[dict] = []
    for od in orders:
        if not isinstance(od, dict):
            logger.warning(f"Skipping non-dict order: {od!r}")
            continue
        if "amo" not in od:
            od = {**od, "amo": "YES"}
        out.append(od)
    return out


def _kill_switch_blocked() -> bool:
    # If a flag file exists, block new entries for safety (created by risk.enforce_kill_switch)
    return os.path.exists(os.path.join("data", "kill_switch.flag"))


def _resolve_selected(names_csv: str) -> List[str]:
    """Split, trim, and map aliases (e.g., 'atm' → 'atm_straddle')."""
    out: List[str] = []
    for raw in (names_csv or "").split(","):
        k = raw.strip().lower()
        if not k:
            continue
        k = ALIASES.get(k, k)   # alias -> canonical
        out.append(k)
    return out


def _normalize_strategy_output(ret: Any) -> List[dict]:
    """
    Accepts None, dict, list[dict], or weird inputs and normalizes to list[dict].
    Prevents crashes from strategies that return a single dict or nothing.
    """
    if ret is None:
        return []
    if isinstance(ret, dict):
        return [ret]
    if isinstance(ret, list):
        out: List[dict] = []
        for item in ret:
            if isinstance(item, dict):
                out.append(item)
            else:
                logger.warning(f"Skipping non-dict item from strategy: {item!r}")
        return out
    logger.warning(f"Unexpected strategy return type: {type(ret).__name__}; ignoring.")
    return []

# -------------------------------
# Main run loop
# -------------------------------

def run_all(
    selected: List[str],
    amo: bool,
    preserve_amo: bool,
    *,
    cancel_dry_run: bool = False,
    skip_precancel: bool = False,
    route_signals: bool = False,
    route_ordertype: Optional[str] = None,
    route_producttype: Optional[str] = None,
    prefer_futures: bool = False,
) -> None:
    # Early block if kill-switch has tripped previously
    if _kill_switch_blocked():
        logger.critical(
            "Kill-switch flag present — blocking new entries. Delete data/kill_switch.flag to re-enable trading."
        )
        print("🚨 Kill-switch active — not executing strategies.")
        return

    smart = restore_or_login()

    # safety: ensure no stale open orders linger (unless user opts out)
    if not skip_precancel:
        try:
            cancel_all_open_before_trading(smart, preserve_amo=preserve_amo, dry_run=cancel_dry_run)
            if cancel_dry_run:
                logger.warning("Cancel dry-run mode: no orders were actually cancelled. Exiting before trades.")
                print("Cancel dry-run done. Exiting without placing strategies.")
                return
        except Exception as e:
            logger.warning(f"Cancel pre-check failed (continuing safely): {e}")
    else:
        logger.info("Skipping pre-cancel stage (--skip-precancel).")

    total_orders = 0
    for name in selected:
        run_fn = STRATEGY_REGISTRY.get(name)
        if not run_fn:
            logger.error(f"Unknown strategy: {name}. Skipping.")
            continue

        try:
            logger.info(f"▶ Running strategy: {name}")
            raw = run_fn(smart)  # may be List[dict] | dict | None
            payloads = _normalize_strategy_output(raw)
            payloads = _inject_amo_flag(payloads, amo=amo)

            if not payloads:
                logger.info(f"— {name}: no signals.")
                # Even if no signals, evaluate kill-switch (e.g., running short gamma elsewhere)
                try:
                    # prefer new guard; else fallback for older code
                    if hasattr(risk, "enforce_kill_switch"):
                        risk.enforce_kill_switch(smart)
                except Exception as e:
                    logger.warning(f"kill-switch check failed: {e}")
                continue

            # --- pre-trade risk guards (caps, funds, hours, etc.) ---
            try:
                if hasattr(risk, "pre_trade_guards"):
                    risk.pre_trade_guards(smart, payloads)
                elif hasattr(risk, "pre_trade_check"):
                    risk.pre_trade_check(smart)
            except Exception as e:
                logger.error(f"Risk guard blocked orders from {name}: {e}")
                continue

            # --- optional: convert BUY/SELL signals into broker orders ---
            orders_to_send: Any = payloads
            if route_signals and build_orders_from_signal is not None:
                built: List[dict] = []
                for pkt in payloads:
                    if isinstance(pkt, dict) and "signal" in pkt and "meta" in pkt:
                        try:
                            built += build_orders_from_signal(
                                smart,
                                pkt,
                                qty=int(os.getenv("STRAT_QTY", "1")),
                                ordertype=(route_ordertype or "MARKET").upper(),
                                producttype=(route_producttype or "INTRADAY").upper(),
                                prefer_futures=prefer_futures,
                                tag_prefix="auto",
                            )
                        except Exception as e:
                            logger.warning(f"signal routing failed for {name}: {e}")
                if built:
                    orders_to_send = built

            # --- place or route (pick external router if available) ---
            if USE_ROUTER and route_orders is not None and orders_to_send:
                route_orders(smart, orders_to_send)
            else:
                place_or_preview(smart, orders_to_send)

            count = len(orders_to_send) if isinstance(orders_to_send, list) else 1
            total_orders += count
            logger.success(f"✓ {name}: executed {count} order(s).")

            # --- post-trade kill-switch check ---
            try:
                if hasattr(risk, "enforce_kill_switch"):
                    risk.enforce_kill_switch(smart)
            except Exception as e:
                logger.warning(f"kill-switch check failed post-trade: {e}")

            # If kill-switch tripped within strategy loop, stop further entries
            if _kill_switch_blocked():
                logger.critical("Kill-switch tripped during run — stopping further strategy execution.")
                break

        except Exception as e:
            logger.exception(f"{name} failed: {e}")

    logger.info(f"All done. Total orders handled: {total_orders}")
    print(f"\nDone. Mode = {'DRY_RUN' if DRY_RUN else 'LIVE'}")

# -------------------------------
# CLI
# -------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Angel One multi-strategy engine")

    # Show dynamic list from registry in help (if available):
    strat_names = ", ".join(sorted(STRATEGY_REGISTRY.keys())) if STRATEGY_REGISTRY else "<none>"
    parser.add_argument(
        "-s",
        "--strategies",
        default="atm_straddle",
        help=f"Comma-separated strategy names or aliases (available: {strat_names})",
    )
    parser.add_argument("--symbols", help="Override: STRAT_SYMBOLS / ENGINE_SYMBOLS (e.g. RELIANCE,TCS,INFY)")
    parser.add_argument("--interval", help="Override: STRAT_INTERVAL / ENGINE_INTERVAL (e.g. 5m, 15m, 1d)")
    parser.add_argument("--bars", type=int, help="Override: STRAT_BARS (e.g. 300)")
    parser.add_argument("--qty", type=int, help="Override: STRAT_QTY per order")
    parser.add_argument("--amo", action="store_true", help="Tag orders for After-Market (amo=YES)")
    parser.add_argument(
        "--preserve-amo",
        action="store_true",
        help="Do NOT auto-cancel existing AMO orders before strategy execution",
    )
    parser.add_argument(
        "--cancel-dry-run",
        action="store_true",
        help="List open/pending orders that WOULD be cancelled, but do not call the API",
    )
    parser.add_argument(
        "--skip-precancel",
        action="store_true",
        help="Skip the pre-run cancellation of open/pending orders",
    )
    parser.add_argument(  # one-shot mode
        "--attach-exits-now",
        action="store_true",
        help="Scan recent filled shorts and attach STOP/TARGET exits immediately, then exit",
    )
    parser.add_argument(
        "--attach-lookback-mins",
        type=int,
        default=20,
        help="Look back this many minutes for filled shorts when attaching exits (default: 20)",
    )
    parser.add_argument(
        "--attach-exits-verbose",
        action="store_true",
        help="Print eligible rows and reasons before attaching exits",
    )
    parser.add_argument(
        "--attach-symbol-filter",
        help="Only attach exits for symbols containing this substring (e.g., NIFTY or BANKNIFTY)",
    )

    # NEW: signal routing flags (optional)
    parser.add_argument("--route-signals", action="store_true",
                        help="Convert strategy BUY/SELL signals into broker orders automatically")
    parser.add_argument("--ordertype", default=None,
                        help="Override order type when routing signals (e.g. MARKET, LIMIT)")
    parser.add_argument("--producttype", default=None,
                        help="Override product type when routing signals (e.g. INTRADAY, DELIVERY)")
    parser.add_argument("--prefer-futures", action="store_true",
                        help="When routing signals for indices, prefer NFO FUT instead of NSE index")

    args = parser.parse_args()

    _banner()

    try:
        market_ok = True if not ENFORCE_MARKET_HOURS else is_market_open()
    except Exception as e:
        logger.warning(f"Market-hours check failed, allowing run (set ENFORCE_MARKET_HOURS=False to silence): {e}")
        market_ok = True

    if ENFORCE_MARKET_HOURS and not market_ok:
        print("⏳ Market is closed (IST 09:15–15:30). Not executing strategies.")
        print("Tip: set ENFORCE_MARKET_HOURS=False in config.py or set BYPASS_MARKET_HOURS=true in .env for testing.")
        return

    # --- one-shot: attach exits and exit -------------------------------------
    if args.attach_exits_now:
        if attach_exits_for_recent_shorts is None:
            print("attach-exits is unavailable (core.attach_exits not found).")
            return
        smart = restore_or_login()
        n = attach_exits_for_recent_shorts(
            smart,
            lookback_minutes=args.attach_lookback_mins,
            symbol_filter=args.attach_symbol_filter,
            verbose=args.attach_exits_verbose,
        )
        print(f"Attached exits for {n} short entry/entries. Mode = {'DRY_RUN' if DRY_RUN else 'LIVE'}")
        return

    _apply_overrides(args.symbols, args.interval, args.bars, args.qty)
    selected = _resolve_selected(args.strategies)
    run_all(
        selected,
        amo=args.amo,
        preserve_amo=args.preserve_amo,
        cancel_dry_run=args.cancel_dry_run,
        skip_precancel=args.skip_precancel,
        route_signals=args.route_signals,
        route_ordertype=args.ordertype,
        route_producttype=args.producttype,
        prefer_futures=args.prefer_futures,
    )


if __name__ == "__main__":
    main()
