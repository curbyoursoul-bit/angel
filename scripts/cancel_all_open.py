# scripts/cancel_open_orders.py
from __future__ import annotations
from typing import Dict, Any, Optional, List, Iterable, Tuple
import time, random
from loguru import logger
from core.login import restore_or_login

# Varieties (Angel/SmartAPI sometimes sends AMO or mixed case)
ALLOWED_VARIETIES = {"NORMAL", "STOPLOSS", "AMO", "ROBO"}

# Statuses that still allow cancel (normalized to lowercase)
OPEN_STATUSES = {
    "open",
    "trigger pending",
    "put order req received",
    "put order received",
    "after market order req received",
    "after market order received",
}

def _looks_transient(err: Exception | str) -> bool:
    msg = str(err).lower()
    needles = (
        "timed out", "timeout", "read timeout",
        "temporarily unavailable", "bad gateway",
        "service unavailable", "502", "503", "504",
        "max retries exceeded", "connection aborted",
        "jsondecode", "couldn't parse the json", "expecting value", "b''",
        "exceed"  # rate/limit exceeded
    )
    return any(n in msg for n in needles)

def _retry_sleep(i: int) -> None:
    # gentle exponential backoff with jitter
    base = min(0.5 * i, 2.5)
    time.sleep(base + random.uniform(0.15, 0.5))

# ---------------- order book helpers ----------------

def _normalize_rows(resp: Any) -> List[Dict[str, Any]]:
    """Accept dict or list; extract list of order rows."""
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        if resp.get("status") is False:
            return []
        data = resp.get("data")
        if isinstance(data, list):
            return data
    # unknown shape
    return []

def _get_open_orders(smart, *, preserve_amo: bool = False,
                     include_statuses: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    """Return rows whose status is cancelable; optionally skip AMO."""
    try:
        ob = smart.orderBook()
    except Exception as e:
        logger.error(f"Failed to fetch order book: {e}")
        return []

    rows = _normalize_rows(ob)
    if not rows:
        msg = (ob.get("message") if isinstance(ob, dict) else str(ob)) if ob else "empty"
        logger.info(f"Order book empty/unavailable: {msg}")
        return []

    allowed = {s.strip().lower() for s in (include_statuses or OPEN_STATUSES)}
    out: List[Dict[str, Any]] = []
    for r in rows:
        status = str(r.get("status") or r.get("orderstatus") or "").strip().lower()
        if status not in allowed:
            continue
        variety = str(r.get("variety") or "").strip().upper()
        if preserve_amo and variety == "AMO":
            continue
        out.append(r)
    return out

# ---------------- cancel helpers ----------------

def _call_cancel_variants(smart, orderid: str, variety: str) -> Any:
    """
    Try cancelOrder with multiple signatures:
      - cancelOrder(variety=..., orderid=...)  (keywords)
      - cancelOrder(orderid, variety)          (positional #1)
      - cancelOrder(variety, orderid)          (positional #2)
    """
    fn = getattr(smart, "cancelOrder", None)
    if not callable(fn):
        raise RuntimeError("SmartAPI client has no cancelOrder method")

    # 1) keywords
    try:
        return fn(variety=variety, orderid=orderid)
    except TypeError:
        pass
    except Exception as e:
        # some SDKs raise immediately (non-retryable at this layer)
        return {"status": False, "message": str(e), "orderid": orderid, "variety": variety}

    # 2) positional: (orderid, variety)
    try:
        return fn(orderid, variety)
    except TypeError:
        pass
    except Exception as e:
        return {"status": False, "message": str(e), "orderid": orderid, "variety": variety}

    # 3) positional: (variety, orderid)
    try:
        return fn(variety, orderid)
    except Exception as e:
        return {"status": False, "message": str(e), "orderid": orderid, "variety": variety}

def _normalize_cancel_resp(resp: Any, orderid: str) -> Dict[str, Any]:
    if isinstance(resp, dict):
        ok = bool(resp.get("status"))
        return {"status": ok, "response": resp, "orderid": orderid}
    # some SDKs just return "success" or True
    if isinstance(resp, str):
        ok = resp.strip().lower() in {"success", "ok", "true"}
        return {"status": ok, "response": {"raw": resp}, "orderid": orderid}
    return {"status": bool(resp), "response": {"raw": resp}, "orderid": orderid}

def _cancel_one(smart, row: Dict[str, Any], *, max_retries: int = 3) -> Dict[str, Any]:
    orderid = str(row.get("orderid") or row.get("orderId") or "").strip()
    if not orderid:
        return {"status": False, "message": "missing orderid", "orderid": None}

    raw_var = (row.get("variety") or "").strip().upper()
    variety = raw_var if raw_var in ALLOWED_VARIETIES else "NORMAL"

    logger.info(f"Cancelling {orderid} (variety={variety})")

    last = None
    for i in range(1, max_retries + 1):
        resp = _call_cancel_variants(smart, orderid, variety)
        out = _normalize_cancel_resp(resp, orderid)
        if out["status"]:
            return out
        last = out

        # retry on transient conditions
        msg = ""
        try:
            if isinstance(resp, dict):
                msg = str(resp.get("message") or resp.get("error") or "")
            else:
                msg = str(resp)
        except Exception:
            msg = ""
        if _looks_transient(msg) and i < max_retries:
            logger.warning(f"Transient cancel error for {orderid}: {msg} (retry {i}/{max_retries})")
            _retry_sleep(i)
            continue

        # Fallback: if original variety present and different, try that once
        if raw_var and raw_var in ALLOWED_VARIETIES and raw_var != variety:
            resp2 = _call_cancel_variants(smart, orderid, raw_var)
            out2 = _normalize_cancel_resp(resp2, orderid)
            if out2["status"]:
                return out2

        break

    return last or {"status": False, "message": "cancel failed", "orderid": orderid}

# ---------------- public API ----------------

def cancel_all_open_orders(
    smart: Optional[Any] = None,
    *,
    preserve_amo: bool = False,
    include_statuses: Optional[Iterable[str]] = None,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Cancel all open/pending orders (optionally skip AMO).
    Returns: {status, message, cancelled:[], failed:[], totals:{found,cancelled,failed}}
    """
    own_session = False
    if smart is None:
        smart = restore_or_login()
        own_session = True

    try:
        open_rows = _get_open_orders(smart, preserve_amo=preserve_amo, include_statuses=include_statuses)
        if not open_rows:
            logger.info("No open orders to cancel.")
            return {"status": True, "message": "No open orders", "cancelled": [], "failed": [], "totals": {"found": 0, "cancelled": 0, "failed": 0}}

        cancelled, failed = [], []
        for r in open_rows:
            oid = r.get("orderid") or r.get("orderId")
            try:
                res = _cancel_one(smart, r, max_retries=max_retries)
                if res.get("status"):
                    cancelled.append({"orderid": oid, "response": res.get("response")})
                    logger.success(f"Cancelled {oid}")
                else:
                    failed.append({"orderid": oid, "response": res.get("response") or {"message": res.get("message")}})
                    logger.error(f"Failed to cancel {oid}: {res}")
            except Exception as e:
                logger.exception(f"Exception cancelling {oid}: {e}")
                failed.append({"orderid": oid, "error": str(e)})

        overall_ok = len(failed) == 0
        return {
            "status": overall_ok,
            "message": "Done" if overall_ok else "Some cancellations failed",
            "cancelled": cancelled,
            "failed": failed,
            "totals": {"found": len(open_rows), "cancelled": len(cancelled), "failed": len(failed)},
        }
    finally:
        # No explicit logout needed for SmartAPI (session token based)
        if own_session:
            pass

# ---------------- CLI smoke test ----------------
def main():
    out = cancel_all_open_orders()
    print(out)

if __name__ == "__main__":
    main()
