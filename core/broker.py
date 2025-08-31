# core/broker.py
from __future__ import annotations

import json, re, os
from typing import List, Dict, Any, Tuple
from loguru import logger

# ---- slippage config (robust) ----------------------------------------------
try:
    from config import SLIPPAGE_PCT  # e.g., 0.001 for 10 bps
    _SLIP = float(SLIPPAGE_PCT)
except Exception:
    _SLIP = float(os.getenv("SLIPPAGE_BPS", "10")) / 10000.0  # default 10 bps

from utils.ltp_fetcher import get_ltp


def _is_option_symbol(tsym: str) -> bool:
    ts = (tsym or "").upper()
    # crude but effective for NFO weekly/monthly symbols
    return any(x in ts for x in ("CE", "PE")) and any(c.isdigit() for c in ts)

def _guess_tick(exchange: str, tradingsymbol: str) -> float:
    ex = (exchange or "").upper()
    if ex == "NFO" or _is_option_symbol(tradingsymbol):
        return 0.05
    return 0.01

def _marketable_limit(side: str, ltp: float, slip: float) -> float:
    side = str(side).upper()
    # BUY must be >= LTP; SELL must be <= LTP
    return round(ltp * (1.0 + slip), 2) if side == "BUY" else round(ltp * (1.0 - slip), 2)

def _round_tick(px: float, tick: float = 0.05) -> float:
    steps = round(px / tick)
    return round(steps * tick, 2)

# ---------------- internals ----------------

_ALLOWED = {
    "variety","tradingsymbol","symboltoken","transactiontype","exchange",
    "ordertype","producttype","duration","price","squareoff","stoploss",
    "trailingstoploss","quantity","disclosedquantity","triggerprice","amo",
    "instrumenttype","strike","optiontype","expirydate","validity"
}
_ALIASES = {
    "qty": "quantity", "timeinforce": "duration", "tif": "duration",
    "product": "producttype", "type": "ordertype", "txn": "transactiontype",
    "symbol": "tradingsymbol", "token": "symboltoken",
    "exchange_type": "exchange", "trigger_price": "triggerprice",
}
_STR_FIELDS = {
    "variety","tradingsymbol","symboltoken","transactiontype","exchange",
    "ordertype","producttype","duration","amo","instrumenttype",
    "optiontype","expirydate","validity"
}
_NUM_FIELDS = {
    "squareoff","stoploss","trailingstoploss","quantity",
    "disclosedquantity","strike"
}
# NOTE: 'price' and 'triggerprice' handled specially.

_ORDER_ID_RE = re.compile(r"^[A-Za-z0-9]{10,}$")

def _normalize_order(order: dict) -> dict:
    """
    Keep only allowed keys, normalize types, and
    handle MARKET vs LIMIT price fields safely.
    """
    raw = dict(order or {})
    out: Dict[str, Any] = {}

    # keep + alias
    for k, v in raw.items():
        k2 = _ALIASES.get(k, k)
        if k2 in _ALLOWED:
            out[k2] = v

    # defaults
    out.setdefault("amo", "NO")

    # string fields
    for k in list(out.keys()):
        if k in _STR_FIELDS:
            out[k] = "" if out[k] is None else str(out[k]).strip()

    # numeric fields except price/triggerprice (special)
    for k in list(out.keys()):
        if k in _NUM_FIELDS:
            try:
                v = out[k]
                if v is None or v == "":
                    out[k] = 0
                elif isinstance(v, bool):
                    out[k] = int(v)
                else:
                    fv = float(v)
                    out[k] = int(fv) if fv.is_integer() else fv
            except Exception:
                out[k] = 0

    # ensure required fields are uppercased where relevant
    for key in ("exchange","ordertype","producttype","duration","transactiontype","variety"):
        if key in out and isinstance(out[key], str):
            out[key] = out[key].upper()

    # PRICE HANDLING:
    # - MARKET: price "0" and no triggerprice
    # - LIMIT: keep provided price if numeric; if blank, omit (we may auto-fill later)
    ot = out.get("ordertype", "").upper()
    price_raw = out.get("price", None)
    trig_raw  = out.get("triggerprice", None)

    def _coerce_price(x):
        if x is None or x == "":
            return None
        try:
            fx = float(x)
            return int(fx) if fx.is_integer() else fx
        except Exception:
            return None

    if ot == "MARKET":
        out["price"] = 0
        out.pop("triggerprice", None)
    else:
        p = _coerce_price(price_raw)
        t = _coerce_price(trig_raw)
        if p is None:
            out.pop("price", None)
        else:
            out["price"] = p
        if t is None:
            out.pop("triggerprice", None)
        else:
            out["triggerprice"] = t

    # sanity checks (keep strict; your infra expects token)
    if not str(out.get("tradingsymbol", "")).strip() or not str(out.get("symboltoken","")).strip():
        raise ValueError(f"Order missing tradingsymbol/symboltoken: {out}")

    return out

def _parse_response(resp: Any) -> Tuple[bool, Dict[str, Any], str | None]:
    """
    Normalize SmartAPI responses into (ok, dict, order_id).
    Accepts dicts or JSON strings or plain order-id strings.
    """
    order_id: str | None = None

    if isinstance(resp, str):
        rid = resp.strip()
        if _ORDER_ID_RE.match(rid):
            return True, {"status": True, "orderid": rid}, rid
        try:
            data = json.loads(resp)
        except Exception:
            data = {"status": None, "raw": resp}
    elif isinstance(resp, dict):
        data = resp
    else:
        data = {"status": None, "raw": resp}

    ok = False
    s = data.get("status")
    if isinstance(s, bool):
        ok = s
    elif isinstance(s, str):
        ok = s.strip().lower() in {"true", "success", "ok", "1", "yes"}

    msg = str(data.get("message", "")).strip().lower()
    if msg in {"success", "ok"}:
        ok = True

    # locate order id in common places
    for path in (("orderid",), ("data","orderid"), ("data","orderId"), ("orderId",)):
        cur = data
        try:
            for k in path:
                cur = cur[k]
            if isinstance(cur, str):
                order_id = cur.strip()
                break
        except Exception:
            continue

    if order_id and not ok:
        data.setdefault("status", True)
        ok = True

    return ok, data, order_id

def _call_place_order(smart, payload: dict) -> Any:
    """
    Call SmartAPI.placeOrder robustly:
      1) placeOrder(orderparams=payload)
      2) placeOrder(payload)
      3) placeOrder(**payload)
    """
    fn = getattr(smart, "placeOrder", None)
    if fn is None:
        raise RuntimeError("SmartAPI client has no placeOrder method")

    try:
        return fn(orderparams=payload)
    except TypeError as e1:
        err1 = str(e1)
    except Exception as e:
        raise

    try:
        return fn(payload)
    except TypeError as e2:
        err2 = str(e2)
    except Exception as e:
        raise

    try:
        return fn(**payload)
    except Exception as e3:
        raise TypeError(
            "All placeOrder invocation styles failed: "
            f"orderparams=payload -> {err1}; payload -> {err2}; kwargs -> {e3}"
        )

# ---------------- helpers for auto-pricing ----------------

def _ensure_limit_prices_for_sl(payload: dict, ltp: float, tick: float, slip: float) -> None:
    """
    For STOPLOSS_LIMIT: if price/triggerprice are missing, set reasonable values:
      BUY-SL-L: trigger ≤ price, both >= LTP
      SELL-SL-L: trigger ≥ price, both ≤ LTP
    We’ll place price slightly more marketable than trigger.
    """
    side = (payload.get("transactiontype") or "").upper()
    price = payload.get("price")
    trig  = payload.get("triggerprice")

    if side == "BUY":
        # place above LTP
        trig_val  = _round_tick(_marketable_limit("BUY", ltp, slip), tick) if trig in (None, "", 0, "0") else trig
        price_val = _round_tick(trig_val + max(tick, ltp * 0.0005), tick) if price in (None, "", 0, "0") else price
    else:
        # SELL
        trig_val  = _round_tick(_marketable_limit("SELL", ltp, slip), tick) if trig in (None, "", 0, "0") else trig
        price_val = _round_tick(trig_val - max(tick, ltp * 0.0005), tick) if price in (None, "", 0, "0") else price

    payload["triggerprice"] = trig_val
    payload["price"] = price_val

# ---------------- public API ----------------

def preview(order: dict) -> dict:
    """Return normalized payload without hitting the API."""
    try:
        return _normalize_order(order)
    except Exception as e:
        return {"error": str(e), "raw": order}

def place_order(smart, order: dict) -> Dict[str, Any]:
    payload = _normalize_order(order)

    # Auto-fill prices for LIMIT / STOPLOSS_LIMIT if missing
    ot = payload.get("ordertype", "MARKET")
    ex = payload.get("exchange", "")
    ts = payload.get("tradingsymbol", "")
    tick = _guess_tick(ex, ts)

    if ot in ("LIMIT", "STOPLOSS_LIMIT") and (("price" not in payload) or payload["price"] in ("", None, 0, "0") or (ot == "STOPLOSS_LIMIT" and (payload.get("triggerprice") in (None, "", 0, "0")))):
        try:
            ltp = float(get_ltp(smart, payload["exchange"], payload["tradingsymbol"], payload["symboltoken"]))
            if ot == "LIMIT":
                raw_px = _marketable_limit(payload.get("transactiontype", "SELL"), ltp, _SLIP)
                payload["price"] = _round_tick(raw_px, tick)
            else:  # STOPLOSS_LIMIT
                _ensure_limit_prices_for_sl(payload, ltp, tick, _SLIP)
            logger.info(f"Auto-priced {ot} {payload['tradingsymbol']}: LTP={ltp} → price={payload.get('price')} trig={payload.get('triggerprice')}")
        except Exception as e:
            logger.error(f"Auto-pricing failed for {payload.get('tradingsymbol')}: {e}")
            return {
                "ok": False,
                "request": payload,
                "response": {"status": False, "message": f"auto-pricing failed: {e}"},
                "order_id": None,
            }

    try:
        resp = _call_place_order(smart, payload)
    except Exception as e:
        logger.exception(f"placeOrder failed for {payload.get('tradingsymbol')}: {e}")
        return {"ok": False, "request": payload, "response": {"status": False, "message": str(e)}, "order_id": None}

    ok, parsed, order_id = _parse_response(resp)
    return {"ok": ok, "request": payload, "response": parsed, "order_id": order_id}

def place_batch(
    smart,
    orders: List[Dict[str, Any]],
    *,
    mode: str = "continue",   # or "rollback"
    dry_run: bool = False,
) -> Dict[str, Any]:
    results = []
    placed_indices: List[int] = []
    placed_order_ids: List[str] = []
    placed_varieties: List[str] = []

    if dry_run:
        for i, o in enumerate(orders, 1):
            results.append({
                "index": i,
                "request": o,
                "status": "success",
                "response": {"dry_run": True},
                "error": None,
                "normalized": preview(o)
            })
        return {"mode": mode, "overall": "success", "results": results}

    any_error = False
    for i, o in enumerate(orders, 1):
        try:
            res = place_order(smart, o)
            ok = bool(res.get("ok"))
            parsed = res.get("response") or {}
            order_id = res.get("order_id")
            norm = res.get("request") or {}

            results.append({
                "index": i,
                "request": o,
                "status": "success" if ok else "error",
                "response": parsed,
                "error": None if ok else json.dumps(parsed, ensure_ascii=False),
                "normalized": norm,
                "order_id": order_id
            })

            if ok:
                placed_indices.append(i)
                if order_id:
                    placed_order_ids.append(order_id)
                    placed_varieties.append(str(norm.get("variety") or "NORMAL"))
            else:
                any_error = True
                if mode == "rollback":
                    break
        except Exception as e:
            any_error = True
            results.append({
                "index": i,
                "request": o,
                "status": "error",
                "response": None,
                "error": str(e),
                "normalized": preview(o)
            })
            if mode == "rollback":
                break

    # Best-effort rollback
    rolled_back = False
    if any_error and mode == "rollback" and placed_order_ids:
        cancel_fn = getattr(smart, "cancelOrder", None)
        if cancel_fn:
            for oid, var in zip(placed_order_ids, placed_varieties):
                try:
                    try:
                        cancel_fn(orderid=oid, variety=var)
                    except TypeError:
                        cancel_fn(orderid=oid)
                except Exception as e:
                    logger.error(f"Rollback cancel failed for {oid}: {e}")
            rolled_back = True

    overall = (
        "success" if not any_error else
        ("rolled_back_due_to_failure" if rolled_back else
         ("partial" if placed_indices else "error"))
    )

    return {"mode": mode, "overall": overall, "results": results}
