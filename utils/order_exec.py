# utils/order_exec.py
from __future__ import annotations

import csv, time, json, hashlib, pathlib, os, random, string
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from loguru import logger

from utils.market_health import fetch_quote, illiquid_or_wide
from utils.oco_registry import new_group_id, record_primary, record_stop, record_target
from utils.market_hours import IST
from utils.auto_trail import spawn_trailer_for_short_leg
from utils.ltp_fetcher import get_ltp
from utils.order_adapter import to_smart_order

from config import (
    DRY_RUN,
    DEFAULT_ORDER_TYPE,
    SLIPPAGE_PCT,
    AUTO_STOPS_ENABLED,
    STOP_LOSS_PCT,
    STOP_LIMIT_BUFFER_PCT,  # used for SL limit & fallback buffer
    AUTO_TARGETS_ENABLED,
    TARGET_PCT,
    TRADE_LOG_CSV,
    TRAIL_ENABLE,
    _f as _cf, _i as _ci, _s as _cs,
)

# ==== ENV / KNOBS ============================================================
VOL_MAX_SPREAD_PCT       = _cf("VOL_MAX_SPREAD_PCT", 0.08)
ORDER_DEDUPE_WINDOW_SECS = _ci("ORDER_DEDUPE_WINDOW_SECS", 20)
ORDER_DEDUPE_FILE        = pathlib.Path(_cs("ORDER_DEDUPE_FILE", "data/order_dedupe.json"))
CANCEL_TRIES             = _ci("CANCEL_TRIES", 3)
CANCEL_BACKOFF_SECS      = _cf("CANCEL_BACKOFF_SECS", 0.4)
TICK_SIZE                = _cf("TICK_SIZE", 0.05)

# ==== HELPERS ================================================================
def _now_ist() -> datetime:
    return datetime.now(IST)

def _round_tick(x: float, step: float = TICK_SIZE) -> float:
    if step <= 0:
        return float(x)
    return round(round(float(x) / step) * step, 2)

def _as_float_str(x: float) -> str:
    return f"{float(x):.2f}"

def _ensure_trade_log(path: Path) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "ts","mode","symbol","side","ordertype","qty","price","triggerprice","orderid","note","ordertag"
            ])

def _log_trade_row(mode: str, order: dict, orderid: str | None, note: str) -> None:
    path = Path(TRADE_LOG_CSV)
    _ensure_trade_log(path)
    row = [
        _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        mode,
        order.get("tradingsymbol",""),
        order.get("transactiontype",""),
        order.get("ordertype",""),
        order.get("quantity",""),
        order.get("price",""),
        order.get("triggerprice",""),
        (orderid or ""),
        note,
        order.get("ordertag",""),
    ]
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

def _short_tag(tag: str | None) -> str:
    """AngelOne tag limit < 20 chars."""
    t = (tag or "")
    return t[:19]

def _normalize(order: dict) -> dict:
    """Pre-adapter normalization of a broker order dict."""
    o = dict(order)
    o["exchange"]        = str(o.get("exchange", "NFO")).upper()
    o["ordertype"]       = str(o.get("ordertype", DEFAULT_ORDER_TYPE)).upper()
    o["producttype"]     = str(o.get("producttype", "INTRADAY")).upper()
    o["duration"]        = str(o.get("duration", "DAY")).upper()
    o["transactiontype"] = str(o.get("transactiontype", o.get("side","BUY"))).upper()

    var = str(o.get("variety", "NORMAL")).upper()
    amo_flag = str(o.get("amo", "")).strip().lower() in {"yes","y","true","1","on"}
    if amo_flag:
        var = "AMO"
    o["variety"] = var

    if not o.get("tradingsymbol") or not o.get("symboltoken"):
        raise ValueError(f"Order missing tradingsymbol/token: {o}")

    if not o.get("ordertag"):
        o["ordertag"] = f"{o['tradingsymbol']}-{_now_ist().strftime('%H%M%S')}"
    o["ordertag"] = _short_tag(o["ordertag"])

    if "quantity" in o:
        try:
            o["quantity"] = int(o["quantity"])
        except Exception:
            pass

    if o["ordertype"] == "MARKET":
        o["price"] = "0"
        o.pop("triggerprice", None)
    else:
        if "price" in o and o["price"] not in (None, "", 0, "0"):
            try:
                o["price"] = _as_float_str(_round_tick(float(o["price"])))
            except Exception:
                pass
        if "triggerprice" in o and o["triggerprice"] not in (None, "", 0, "0"):
            try:
                o["triggerprice"] = _as_float_str(_round_tick(float(o["triggerprice"])))
            except Exception:
                pass

    return o

def _slippage_price(side: str, ltp: float, slip_pct: float) -> float:
    side_u = side.upper()
    if side_u == "BUY":
        return round(ltp * (1.0 - float(slip_pct)), 2)
    return round(ltp * (1.0 + float(slip_pct)), 2)

def _fake_order_id(prefix: str = "DRY") -> str:
    rand = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
    return f"{prefix}{rand}"

# ==== DE-DUPLICATION =========================================================
def _hash_order(o: dict) -> str:
    keys = ["tradingsymbol","symboltoken","transactiontype","exchange","ordertype","producttype","quantity","variety"]
    s = "|".join(str(o.get(k, "")) for k in keys)
    return hashlib.sha256(s.encode()).hexdigest()

def _dedupe_load() -> Dict[str, float]:
    try:
        if ORDER_DEDUPE_FILE.exists():
            return json.loads(ORDER_DEDUPE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _dedupe_save(d: Dict[str, float]) -> None:
    try:
        ORDER_DEDUPE_FILE.parent.mkdir(parents=True, exist_ok=True)
        ORDER_DEDUPE_FILE.write_text(json.dumps(d), encoding="utf-8")
    except Exception:
        pass

def _should_block_duplicate(o: dict) -> bool:
    now = time.time()
    db = _dedupe_load()
    h = _hash_order(o)
    last = db.get(h, 0.0)
    ttl = max(0, ORDER_DEDUPE_WINDOW_SECS)
    if ttl == 0:
        db[h] = now; _dedupe_save(db); return False
    if now - last <= ttl:
        logger.warning(f"[dedupe] Blocked duplicate within {ttl}s: {o.get('tradingsymbol')} {o.get('transactiontype')}")
        return True
    db[h] = now
    if ttl > 0:
        db = {k: v for k, v in db.items() if now - v <= ttl * 3}
    _dedupe_save(db)
    return False

# ==== DETECT SIGNAL PAYLOADS =================================================
def _is_signal_payload(obj: Any) -> bool:
    return isinstance(obj, dict) and "signal" in obj and "meta" in obj and "name" in obj

# ==== CORE BROKER CALL WITH AUTO-FIX =========================================
def _place(smart, order: dict) -> Tuple[bool, Optional[str], dict]:
    """Place order; on AB1020 Invalid Order Type, try one graceful fallback."""
    if DRY_RUN:
        oid = _fake_order_id()
        logger.info(f"[DRY-RUN] Would place → {order} (oid={oid})")
        return True, oid, {
            "status": True,
            "message": "DRY-RUN preview",
            "data": {"order_preview": order, "orderid": oid},
            "orderid": oid,
        }

    def _do_place(payload: dict):
        safe = to_smart_order(payload)
        try:
            try:
                resp = smart.placeOrder(orderparams=safe)  # newer SDK
            except TypeError:
                resp = smart.placeOrder(safe)              # older SDK
        except Exception as e:
            return False, None, {"status": False, "message": str(e), "data": None}
        # normalize response
        if isinstance(resp, str) and resp.strip():
            oid = resp.strip();  return True, oid, {"status": True, "message": "success", "orderid": oid}
        if not isinstance(resp, dict):
            return False, None, {"status": False, "message": f"Invalid broker response: {resp!r}", "data": None}
        ok = bool(resp.get("status") is True or str(resp.get("message","")).lower().startswith("success"))
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        oid = None
        for k in ("orderid","order_id"):
            if isinstance(resp.get(k), str) and resp.get(k).strip():
                oid = resp.get(k).strip(); break
        if not oid and isinstance(data.get("orderid"), str) and data.get("orderid").strip():
            oid = data.get("orderid").strip()
        return ok, oid, resp

    # 1) try as-is
    ok, oid, resp = _do_place(order)
    if ok:
        return ok, oid, resp

    msg = str(resp.get("message", "")).lower() if isinstance(resp, dict) else ""
    ab_code = (resp.get("errorcode") if isinstance(resp, dict) else None) or ""
    if "invalid order type" in msg or ab_code == "AB1020":
        # Fallback 1: if it's STOPLOSS_* try canonical "STOPLOSS"
        if order.get("ordertype","").upper() in {"STOPLOSS_MARKET", "STOPLOSS_LIMIT"}:
            o2 = dict(order)
            # Convert to STOPLOSS with both trigger + price
            trig = float(o2.get("triggerprice") or 0) or 0.0
            if trig <= 0:
                # try to salvage from price if present
                trig = float(o2.get("price") or 0) or 0.0
            if trig > 0:
                lim = _round_tick(trig * (1.0 + float(STOP_LIMIT_BUFFER_PCT)))
                o2["ordertype"] = "STOPLOSS"
                o2["triggerprice"] = _as_float_str(_round_tick(trig))
                o2["price"] = _as_float_str(lim)
                ok2, oid2, resp2 = _do_place(o2)
                if ok2:
                    return ok2, oid2, resp2
                # Fallback 2: try 'SL' alias (some variants)
                o3 = dict(o2)
                o3["ordertype"] = "SL"
                ok3, oid3, resp3 = _do_place(o3)
                if ok3:
                    return ok3, oid3, resp3
                # Fallback 3: plain LIMIT at a tiny buffer above trigger
                o4 = dict(o2)
                o4["ordertype"] = "LIMIT"
                o4.pop("triggerprice", None)
                ok4, oid4, resp4 = _do_place(o4)
                return ok4, oid4, resp4

    return ok, oid, resp

# ==== STOP/TARGET BUILDERS ===================================================
def _make_sl_buy_for_short(primary: dict, ref_price: float) -> dict:
    """
    Build a STOPLOSS/SL buy (Angel expects both triggerprice and price).
    """
    trig = _round_tick(max(ref_price, 0.05) * (1.0 + float(STOP_LOSS_PCT)))
    limit = _round_tick(trig * (1.0 + float(STOP_LIMIT_BUFFER_PCT)))
    return {
        "variety": "NORMAL",  # SLs go as NORMAL
        "tradingsymbol": primary["tradingsymbol"],
        "symboltoken": primary["symboltoken"],
        "transactiontype": "BUY",
        "exchange": primary["exchange"],
        "ordertype": "STOPLOSS",     # canonical
        "producttype": primary.get("producttype", "INTRADAY"),
        "duration": "DAY",
        "triggerprice": _as_float_str(trig),
        "price": _as_float_str(limit),
        "quantity": primary["quantity"],
        "ordertag": _short_tag(primary.get("ordertag")),
    }

def _make_limit_target_for_short(primary: dict, ref_price: float) -> Optional[dict]:
    price = _round_tick(ref_price * (1.0 - float(TARGET_PCT)))
    if price <= 0:
        return None
    return {
        "variety": "NORMAL",
        "tradingsymbol": primary["tradingsymbol"],
        "symboltoken": primary["symboltoken"],
        "transactiontype": "BUY",
        "exchange": primary["exchange"],
        "ordertype": "LIMIT",
        "producttype": primary.get("producttype", "INTRADAY"),
        "duration": "DAY",
        "price": _as_float_str(price),
        "quantity": primary["quantity"],
        "ordertag": _short_tag(primary.get("ordertag")),
    }

# ==== PUBLIC ENTRY ===========================================================
def place_or_preview(
    smart,
    orders: Any,  # dict | List[dict] | signal-payload
    *,
    rollback_on_failure: bool = True,
) -> List[Tuple[bool, Optional[str], dict]]:

    TRAIL_ON = bool(TRAIL_ENABLE)
    logger.info(
        "ORDER_EXEC DRY_RUN=%s AUTO_STOPS=%s AUTO_TARGETS=%s AUTO_TRAIL=%s",
        DRY_RUN, AUTO_STOPS_ENABLED, AUTO_TARGETS_ENABLED, TRAIL_ON
    )

    # Normalize to list
    if isinstance(orders, dict):
        items: List[Any] = [orders]
    elif isinstance(orders, (list, tuple)):
        items = list(orders)
    else:
        items = [orders]

    results: List[Tuple[bool, Optional[str], dict]] = []
    placed_primaries: List[Tuple[str, str]] = []

    for idx, raw in enumerate(items):
        # Strategy "signal" packets passthrough
        if _is_signal_payload(raw):
            logger.info(f"[signal] Strategy={raw.get('name')} signal={raw.get('signal')} meta={raw.get('meta')}")
            results.append((True, None, {"status": True, "message": "signal_only", "data": raw}))
            continue

        # Broker dict expected
        if not isinstance(raw, dict):
            logger.error(f"Skip non-dict orders[{idx}]: {type(raw)}")
            results.append((False, None, {"status": False, "message": "invalid_order_type"}))
            continue

        try:
            o = _normalize(raw)
        except Exception as e:
            logger.error(f"Skip malformed order[{idx}]: {e}")
            results.append((False, None, {"status": False, "message": str(e)}))
            continue

        if _should_block_duplicate(o):
            results.append((False, None, {"status": False, "message": "duplicate_blocked"}))
            continue

        # Liquidity/spread gate (best-effort)
        try:
            q = fetch_quote(smart, o["exchange"], o["tradingsymbol"], o["symboltoken"]) or {}
            if illiquid_or_wide(q, max_spread_pct=VOL_MAX_SPREAD_PCT):
                logger.warning(f"[spread] Wide/illiquid: {o['tradingsymbol']} — blocking order")
                results.append((False, None, {"status": False, "message": "wide_spread_block"}))
                continue
        except Exception:
            pass

        is_short = (o.get("transactiontype","").upper() == "SELL")
        is_amo   = (o.get("variety","").upper() == "AMO")

        # Auto-price LIMIT if blank
        if o["ordertype"] == "LIMIT" and (o.get("price") in (None,"",0,"0")):
            try:
                ltp = float(get_ltp(smart, o["exchange"], o["tradingsymbol"], o["symboltoken"]))
                o["price"] = _as_float_str(_round_tick(_slippage_price(o["transactiontype"], ltp, SLIPPAGE_PCT)))
            except Exception as e:
                results.append((False, None, {"status": False, "message": f"slippage/ltp error: {e}"}))
                continue

        # Group OCO
        oco_gid = None
        if (AUTO_STOPS_ENABLED or AUTO_TARGETS_ENABLED):
            try:
                oco_gid = new_group_id(o["tradingsymbol"])
                o["ordertag"] = _short_tag(oco_gid)
            except Exception:
                pass

        # DRY RUN path
        if DRY_RUN:
            oid_preview = _fake_order_id()
            logger.info(f"[DRY-RUN] Would place: {o} (oid={oid_preview})")
            _log_trade_row("DRY", o, oid_preview, "preview_primary")
            results.append((True, oid_preview, {"status": True, "message": "DRY-RUN preview", "orderid": oid_preview}))
            continue

        # LIVE: place primary
        ok, oid, resp = _place(smart, o)
        _log_trade_row("LIVE", o, oid, "primary" if ok else "primary_failed")
        results.append((ok, oid, resp))
        if not ok:
            if rollback_on_failure and placed_primaries:
                for poid, pvar in reversed(placed_primaries):
                    try:
                        smart.cancelOrder(variety=pvar, orderid=poid)
                    except Exception:
                        pass
            continue

        placed_primaries.append((oid or "", o.get("variety","NORMAL")))
        if oco_gid:
            try:
                record_primary(oco_gid, o)
            except Exception:
                pass

        # Auto attach for short legs only; skip for AMO primaries
        if is_short and not is_amo:
            # pick reference price (entry price preferred; else LTP)
            base_px = 0.0
            try:
                if o.get("price") not in (None, "", "0", 0):
                    base_px = float(o["price"])
            except Exception:
                base_px = 0.0
            if base_px <= 0:
                try:
                    base_px = float(get_ltp(smart, o["exchange"], o["tradingsymbol"], o["symboltoken"]))
                except Exception:
                    base_px = 0.0

            # Stop
            if AUTO_STOPS_ENABLED:
                if base_px > 0:
                    stop_o = _make_sl_buy_for_short(o, base_px)
                    if oco_gid: stop_o["ordertag"] = _short_tag(oco_gid)
                    ok_s, oid_s, res_s = _place(smart, stop_o)
                    results.append((ok_s, oid_s, res_s))
                    if ok_s:
                        try:
                            record_stop(oco_gid or "", oid_s or "", stop_o)
                        except Exception:
                            pass
                else:
                    results.append((False, None, {"status": False, "message": "stop_build_failed: no ref price"}))

            # Target
            if AUTO_TARGETS_ENABLED:
                if base_px > 0:
                    tp_o = _make_limit_target_for_short(o, base_px)
                    if tp_o:
                        if oco_gid: tp_o["ordertag"] = _short_tag(oco_gid)
                        ok_t, oid_t, res_t = _place(smart, tp_o)
                        results.append((ok_t, oid_t, res_t))
                        if ok_t:
                            try:
                                record_target(oco_gid or "", oid_t or "", tp_o)
                            except Exception:
                                pass
                    else:
                        results.append((False, None, {"status": False, "message": "target_build_failed: nonpositive price"}))
                else:
                    results.append((False, None, {"status": False, "message": "target_build_failed: no ref price"}))

            # Trailer
            if TRAIL_ON:
                try:
                    spawn_trailer_for_short_leg(smart, o, base_px)
                except Exception:
                    pass

    return results
