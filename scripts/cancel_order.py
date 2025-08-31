# scripts/cancel_order.py
from __future__ import annotations
import argparse, json, sys, time, random
from typing import Any, Dict, List, Optional
from loguru import logger
from core.login import restore_or_login

CANCELABLE_STATUSES = {
    "open",
    "trigger pending",
    "put order req received",
    "put order received",
    "after market order req received",
    "after market order received",
}
ALLOWED_VARIETIES = {"NORMAL", "STOPLOSS", "AMO", "ROBO"}

# ---------- helpers: response normalization ----------
def _as_dict(resp: Any) -> Dict[str, Any]:
    if isinstance(resp, dict):
        return resp
    if isinstance(resp, str):
        try:
            return json.loads(resp)
        except Exception:
            return {"status": None, "raw": resp}
    return {"status": None, "raw": resp}

def _ok(resp: Dict[str, Any]) -> bool:
    s = resp.get("status")
    if isinstance(s, bool):
        return s
    if isinstance(s, str):
        if s.strip().lower() in {"true", "ok", "success"}:
            return True
    if str(resp.get("message", "")).strip().upper() == "SUCCESS":
        return True
    return False

def _row_status(row: Dict[str, Any]) -> str:
    return str(row.get("orderstatus") or row.get("status") or "").strip().lower()

def _looks_transient(err: Exception | str) -> bool:
    msg = str(err).lower()
    needles = (
        "timed out","timeout","read timeout","temporarily unavailable",
        "bad gateway","service unavailable","max retries exceeded",
        "connection aborted","502","503","504",
        "couldn't parse the json","jsondecode","expecting value","b''",
        "exceed"
    )
    return any(n in msg for n in needles)

def _sleep_backoff(i: int, base: float = 0.45) -> None:
    time.sleep(min(base * i, 2.5) + random.uniform(0.12, 0.4))

# ---------- SmartAPI wrappers ----------
def _fetch_orderbook(smart) -> List[Dict[str, Any]]:
    fn = getattr(smart, "orderBook", None)
    if not callable(fn):
        raise RuntimeError("SmartAPI client has no orderBook")
    try:
        ob = fn()
    except Exception as e:
        raise RuntimeError(f"orderBook error: {e}")
    # normalize
    if isinstance(ob, list):
        return ob
    obd = _as_dict(ob)
    if not _ok(obd):
        raise RuntimeError(f"Failed to fetch order book: {obd}")
    data = obd.get("data") or []
    return data if isinstance(data, list) else []

def _call_cancel_variants(smart, orderid: str, variety: str) -> Any:
    """
    Try cancelOrder with:
      - kwargs: cancelOrder(variety=..., orderid=...)
      - positional: cancelOrder(orderid, variety)
      - positional: cancelOrder(variety, orderid)
    """
    fn = getattr(smart, "cancelOrder", None)
    if not callable(fn):
        raise RuntimeError("SmartAPI client has no cancelOrder")
    # 1) kwargs
    try:
        return fn(variety=variety, orderid=orderid)
    except TypeError:
        pass
    except Exception as e:
        return {"status": False, "message": str(e), "orderid": orderid, "variety": variety}
    # 2) positional (orderid, variety)
    try:
        return fn(orderid, variety)
    except TypeError:
        pass
    except Exception as e:
        return {"status": False, "message": str(e), "orderid": orderid, "variety": variety}
    # 3) positional (variety, orderid)
    try:
        return fn(variety, orderid)
    except Exception as e:
        return {"status": False, "message": str(e), "orderid": orderid, "variety": variety}

def _cancel_once(smart, orderid: str, variety: str) -> Dict[str, Any]:
    resp = _call_cancel_variants(smart, orderid, variety)
    return _as_dict(resp)

# ---------- core cancel logic ----------
def cancel_one(
    smart,
    order_id: str,
    variety_hint: Optional[str] = None,
    *,
    use_orderbook: bool = True,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Cancel one order robustly.
      1) Determine variety: use hint else look up from orderBook (if enabled) else NORMAL.
      2) If order already final (cancelled/complete/rejected), skip.
      3) Try cancel; on AMO failure, retry NORMAL.
      4) Retry transient errors with backoff.
    Returns a normalized dict.
    """
    order_id = str(order_id).strip()
    if not order_id:
        return {"status": False, "message": "missing order_id"}

    # Learn from orderbook (best-effort)
    book_row = None
    if use_orderbook:
        try:
            rows = _fetch_orderbook(smart)
            book_row = next((r for r in rows if str(r.get("orderid")) == order_id), None)
        except Exception as e:
            logger.warning(f"[{order_id}] orderBook unavailable: {e}")

    if book_row:
        st = _row_status(book_row)
        if st in {"cancelled", "complete", "rejected"}:
            logger.info(f"[{order_id}] already {st}; skipping.")
            return {"status": True, "message": f"Already {st}", "data": {"orderid": order_id}}
        true_var = str(book_row.get("variety") or "NORMAL").upper()
        if true_var in ALLOWED_VARIETIES:
            variety = true_var
        else:
            variety = (variety_hint or "NORMAL").upper()
    else:
        variety = (variety_hint or "NORMAL").upper()

    if variety not in ALLOWED_VARIETIES:
        variety = "NORMAL"

    logger.info(f"[{order_id}] Cancelling (variety={variety}{' | from OB' if book_row else ''})")

    last = None
    for i in range(1, max_retries + 1):
        resp = _cancel_once(smart, order_id, variety)
        if _ok(resp):
            return resp
        last = resp

        # Transient? retry
        msg = str(resp.get("message") or resp.get("raw") or "")
        if _looks_transient(msg) and i < max_retries:
            logger.warning(f"[{order_id}] transient cancel error: {msg} (retry {i}/{max_retries})")
            _sleep_backoff(i)
            continue

        # AMO sometimes needs NORMAL
        if variety == "AMO":
            logger.info(f"[{order_id}] AMO cancel failed; retry as NORMAL")
            resp2 = _cancel_once(smart, order_id, "NORMAL")
            if _ok(resp2):
                return resp2
            last = {"status": False, "message": "AMO->NORMAL cancel failed", "first": resp, "second": resp2}
        break

    return last or {"status": False, "message": "cancel failed"}

# ---------- CLI ----------
def _build_cli():
    p = argparse.ArgumentParser(description="Cancel one or more SmartAPI orders robustly")
    p.add_argument("order_ids", nargs="+", help="Order IDs to cancel")
    p.add_argument("--variety", choices=["NORMAL","AMO","STOPLOSS","ROBO"], help="Hint variety (optional)")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    p.add_argument("--no-orderbook", action="store_true", help="Do not fetch order book to infer variety/status")
    p.add_argument("--retries", type=int, default=3, help="Max retries on transient errors (default 3)")
    p.add_argument("--sleep", type=float, default=0.25, help="Sleep between cancels (seconds)")
    return p

def main(argv: Optional[List[str]] = None):
    args = _build_cli().parse_args(argv)
    smart = restore_or_login()

    outputs: List[Dict[str, Any]] = []
    total = len(args.order_ids)
    for idx, oid in enumerate(args.order_ids, 1):
        try:
            resp = cancel_one(
                smart,
                oid,
                variety_hint=args.variety,
                use_orderbook=not args.no_orderbook,
                max_retries=max(1, args.retries),
            )
            ok = _ok(resp)
            (logger.success if ok else logger.warning)(f"[{idx}/{total}] {oid}: {resp}")
            outputs.append({"order_id": oid, "ok": ok, "response": resp})
        except Exception as e:
            logger.exception(f"[{idx}/{total}] {oid} cancel exception")
            outputs.append({"order_id": oid, "ok": False, "error": str(e)})
        time.sleep(max(0.0, args.sleep))

    if args.json:
        print(json.dumps(outputs, indent=2, ensure_ascii=False))
    else:
        for r in outputs:
            print(r)

if __name__ == "__main__":
    sys.exit(main())
