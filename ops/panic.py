# ops/panic.py
from __future__ import annotations
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Dict, Any, Tuple, List
from loguru import logger
import logging as _pylogging

from execution.order_manager import OrderManager

# --- Optional alerts (Telegram) ---
try:
    from ops.alerts import send as alert
except Exception:
    def alert(_msg: str) -> bool:  # type: ignore
        return False

# --- Tunables ---
CANCEL_RATE_LIMIT_SEC = 0.02   # faster between worker ops
SQUARE_RATE_LIMIT_SEC = 0.02
RETRY_TRIES_DEFAULT   = 4
RETRY_BACKOFF_SEC     = 0.3
CANCEL_WORKERS        = 8       # parallel cancels
SQUARE_WORKERS        = 4

OPEN_STATES = {
    "OPEN","PENDING","TRIGGER PENDING","AMO REQ RECEIVED","OPEN PENDING",
    "OPEN PENDING,MODIFY","MODIFY PENDING","OPEN PENDING,CANCEL",
}

# --- logging setup ---
def setup_logging(quiet: bool) -> None:
    """
    If quiet=True, only show INFO+ from our script and hide noisy DEBUG/TRACE
    from dependencies (requests/urllib3/SmartApi).
    """
    logger.remove()
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level="INFO" if quiet else "DEBUG",
        backtrace=not quiet,
        diagnose=not quiet,
        enqueue=False,
    )
    if quiet:
        for name in (
            "urllib3", "requests", "SmartApi", "websocket",
            "chardet", "charset_normalizer", "asyncio",
        ):
            _pylogging.getLogger(name).setLevel(_pylogging.WARNING)

# ---------------------- helpers ---------------------- #
def _fetch_orders(smart) -> Iterable[Dict[str, Any]]:
    try:
        res = smart.orderBook()  # type: ignore
    except Exception:
        try:
            res = smart.getOrderBook()  # type: ignore
        except Exception:
            res = None
    data = res.get("data") if isinstance(res, dict) else res
    return data or []

def _fetch_positions(smart) -> Iterable[Dict[str, Any]]:
    try:
        res = smart.position()  # type: ignore
    except Exception:
        try:
            res = smart.getPosition()  # type: ignore
        except Exception:
            res = None
    data = res.get("data") if isinstance(res, dict) else res
    return data or []

def _retry_call(fn, *, tries:int, backoff:float=RETRY_BACKOFF_SEC, rate_limit: float = 0.0):
    """
    Retry wrapper that understands OrderResult(success=...).
    Returns (ok: bool, payload_or_err: Any).
    """
    last_err = None
    for i in range(1, int(tries) + 1):
        try:
            resp = fn()
            ok = getattr(resp, "success", None)
            if ok is None:
                ok = True
            if ok:
                if rate_limit > 0:
                    time.sleep(rate_limit)
                return True, resp
            else:
                last_err = getattr(resp, "error", "non-success response")
                sleep_for = backoff * (2 ** (i - 1))
                logger.warning(f"[panic] op non-success (attempt {i}/{tries}): {last_err} — retry in {sleep_for:.2f}s")
                time.sleep(sleep_for)
        except Exception as e:
            last_err = e
            sleep_for = backoff * (2 ** (i - 1))
            logger.warning(f"[panic] op exception (attempt {i}/{tries}): {e} — retry in {sleep_for:.2f}s")
            time.sleep(sleep_for)
    return False, str(last_err)

# ---------------------- main ops ---------------------- #
def _cancel_one(om: OrderManager, o: Dict[str, Any], tries: int) -> Tuple[str, bool, str]:
    status = (o.get("status") or o.get("Status") or "").upper().strip()
    if status not in OPEN_STATES:
        return (str(o.get("orderid") or o.get("orderId") or o.get("order_id")), True, "skip-not-open")

    oid  = str(o.get("orderid") or o.get("orderId") or o.get("order_id"))
    exch = (o.get("exchange") or o.get("exch_seg") or "").upper()
    var  = (o.get("variety") or "NORMAL").upper()
    tsym = o.get("tradingsymbol")
    ptyp = (o.get("producttype") or o.get("productType") or "INTRADAY").upper()

    logger.info(f"[panic] cancel {oid} status={status} exch={exch} var={var} tsym={tsym} ptype={ptyp}")
    ok, resp = _retry_call(
        lambda: om.cancel(oid, variety=var, exchange=exch, tradingsymbol=tsym, producttype=ptyp),
        tries=tries, rate_limit=CANCEL_RATE_LIMIT_SEC
    )
    if not ok:
        logger.error(f"[panic] cancel failed {oid}: {resp}")
        return (oid, False, str(resp))
    return (oid, True, "ok")

def cancel_all_open(smart, *, dry_run: bool = False, fast: bool = False) -> int:
    """
    Cancel all OPEN/PENDING/TP orders. Returns count attempted.
    fast=True → only 1 try per order (relies on OrderManager self-verify), parallel workers.
    """
    if dry_run:
        cnt = sum(1 for o in _fetch_orders(smart) if (o.get("status") or "").upper() in OPEN_STATES)
        logger.info(f"[panic] DRY-RUN cancel_all_open count={cnt}")
        return cnt

    om = OrderManager(smart)
    orders = list(_fetch_orders(smart))
    targets = [o for o in orders if (o.get("status") or o.get("Status") or "").upper().strip() in OPEN_STATES]
    if not targets:
        logger.info("[panic] no OPEN orders")
        return 0

    tries = 1 if fast else RETRY_TRIES_DEFAULT
    errors: List[str] = []
    done = 0

    with ThreadPoolExecutor(max_workers=CANCEL_WORKERS) as pool:
        futs = [pool.submit(_cancel_one, om, o, tries) for o in targets]
        for f in as_completed(futs):
            oid, ok, msg = f.result()
            done += 1
            if not ok:
                errors.append(f"{oid}:{msg}")

    logger.info(f"[panic] cancel_all_open done: attempted={len(targets)} ok={done - len(errors)} fail={len(errors)}")
    if errors:
        logger.warning(f"[panic] cancel errors (first 10): {errors[:10]}")
    return done

def _square_one(om: OrderManager, p: Dict[str, Any], tries: int) -> Tuple[str, bool, str]:
    qty = int(float(p.get("netqty") or p.get("netQty") or 0))
    if qty == 0:
        return (p.get("tradingsymbol") or "?", True, "skip-zero")

    exch = p.get("exchange") or p.get("exch_seg")
    tsym = p.get("tradingsymbol")
    tok  = p.get("symboltoken") or p.get("token")
    side = "SELL" if qty > 0 else "BUY"
    order = {
        "exchange": exch,
        "tradingsymbol": tsym,
        "symboltoken": tok,
        "transactiontype": side,
        "ordertype": "MARKET",
        "producttype": p.get("producttype") or p.get("productType") or "INTRADAY",
        "duration": "DAY",
        "quantity": abs(qty),
        "variety": "NORMAL",
    }
    logger.info(f"[panic] square-off {tsym}:{tok} {side} {abs(qty)}")
    ok, resp = _retry_call(lambda: om.place(order), tries=tries, rate_limit=SQUARE_RATE_LIMIT_SEC)
    if not ok:
        logger.error(f"[panic] square-off failed {tsym}:{tok}: {resp}")
        return (tsym or "?", False, str(resp))
    return (tsym or "?", True, "ok")

def squareoff_all_positions(smart, *, dry_run: bool = False, fast: bool = False) -> int:
    if dry_run:
        cnt = sum(1 for p in _fetch_positions(smart) if int(float(p.get("netqty") or p.get("netQty") or 0)) != 0)
        logger.info(f"[panic] DRY-RUN squareoff count={cnt}")
        return cnt

    om = OrderManager(smart)
    positions = list(_fetch_positions(smart))
    targets = [p for p in positions if int(float(p.get("netqty") or p.get("netQty") or 0)) != 0]
    if not targets:
        logger.info("[panic] no open positions")
        return 0

    tries = 1 if fast else RETRY_TRIES_DEFAULT
    errors: List[str] = []
    done = 0

    with ThreadPoolExecutor(max_workers=SQUARE_WORKERS) as pool:
        futs = [pool.submit(_square_one, om, p, tries) for p in targets]
        for f in as_completed(futs):
            _, ok, msg = f.result()
            done += 1
            if not ok:
                errors.append(msg)

    logger.info(f"[panic] squareoff_all_positions done: attempted={len(targets)} ok={done - len(errors)} fail={len(errors)}")
    return done

# ---------------------- CLI ---------------------- #
def main():
    """
    Examples:
      python -m ops.panic --mode both
      python -m ops.panic --mode cancel --fast
      python -m ops.panic --mode squareoff --dry-run
      python -m ops.panic --mode both --fast --quiet
    """
    parser = argparse.ArgumentParser(description="Emergency kill-orders / square-off tool")
    parser.add_argument("--mode", choices=["cancel", "squareoff", "both"], default="both")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--fast", action="store_true", help="single try per order, parallel workers")
    parser.add_argument("--quiet", action="store_true", help="suppress DEBUG/TRACE logs from dependencies")
    args = parser.parse_args()

    # set up logging ASAP
    setup_logging(args.quiet)

    # Login
    from core.login import restore_or_login
    smart = restore_or_login()

    # Apply HTTP timeouts to SmartAPI (monkeypatch)
    try:
        from utils.angel_timeout import apply_http_timeouts
        apply_http_timeouts(smart)
    except Exception as e:
        logger.debug(f"[panic] timeout patch skipped: {e}")

    cancels = squares = 0
    if args.mode in {"cancel", "both"}:
        cancels = cancel_all_open(smart, dry_run=args.dry_run, fast=args.fast)
    if args.mode in {"squareoff", "both"}:
        squares = squareoff_all_positions(smart, dry_run=args.dry_run, fast=args.fast)

    msg = f"🛑 Panic complete • cancels={cancels} • squareoffs={squares} • dry_run={args.dry_run} • fast={args.fast}"
    logger.info(msg)
    alert(msg)

if __name__ == "__main__":
    main()
