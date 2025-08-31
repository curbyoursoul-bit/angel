# execution/order_manager.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Iterable, Callable
import time, hashlib, os
import threading  # <<< NEW
from loguru import logger

ORDER_DEDUPE_MS = int(float(os.getenv("ORDER_DEDUPE_MS", "1200")))
DEFAULT_VARIETY = (os.getenv("ORDER_DEFAULT_VARIETY") or "NORMAL").upper()

REQUIRED = {
    "exchange", "tradingsymbol", "symboltoken", "transactiontype",
    "ordertype", "producttype", "duration", "quantity",
}
OPTIONAL = {
    "price", "triggerprice", "variety", "disclosedquantity",
    "squareoff", "stoploss", "trailingStopLoss", "client_order_id",
}

OPENISH = {
    "OPEN","PENDING","TRIGGER PENDING","AMO REQ RECEIVED","OPEN PENDING",
    "OPEN PENDING,MODIFY","MODIFY PENDING","OPEN PENDING,CANCEL",
}

def _ok(resp: Any) -> bool:
    if resp is True:
        return True
    if isinstance(resp, dict):
        st = resp.get("status")
        if isinstance(st, bool):
            return st
        msg = str(resp.get("message", "")).lower()
        if "success" in msg:
            return True
        if "data" in resp and resp["data"]:
            return True
    if isinstance(resp, str) and "success" in resp.lower():
        return True
    return bool(resp)

def _fetch_orders(smart) -> Iterable[Dict[str, Any]]:
    try:
        res = smart.orderBook()  # type: ignore
    except Exception:
        try:
            res = smart.getOrderBook()  # type: ignore
        except Exception:
            res = None
    return (res.get("data") if isinstance(res, dict) else res) or []

def _status_of_order(smart, order_id: str) -> Optional[str]:
    for o in _fetch_orders(smart):
        oid = o.get("orderid") or o.get("orderId") or o.get("order_id")
        if str(oid) == str(order_id):
            st = (o.get("status") or o.get("Status") or "").upper().strip()
            return st or "UNKNOWN"
    return None

def _looks_like_transient_comm_err(e: BaseException) -> bool:
    s = str(e)
    if "Couldn't parse the JSON response received from the server: b''" in s:
        return True
    if "Read timed out" in s or "ReadTimeout" in s:
        return True
    if "Max retries exceeded" in s:
        return True
    if "Failed to establish a new connection" in s:
        return True
    if "Connection aborted" in s or "RemoteDisconnected" in s:
        return True
    return False

@dataclass
class OrderResult:
    success: bool
    data: Dict[str, Any]
    error: Optional[str] = None

class OrderManager:
    """
    Adapter around SmartConnect with signature fallbacks and self-verifying cancel.
    Caches the first *core* working cancel signature (thread-safe) so subsequent
    cancels are cheap and quiet.
    """
    def __init__(self, smart: Any):
        self.smart = smart
        self._last_sig: Tuple[str, float] | None = None
        self._cancel_strategy: Optional[Callable[[str, str], Any]] = None
        self._lock = threading.Lock()  # <<< NEW

    # ---------- normalization ----------
    def _normalize(self, order_or_updates: Dict[str, Any]) -> Dict[str, Any]:
        o: Dict[str, Any] = {}
        for k in (REQUIRED | OPTIONAL):
            if k in order_or_updates:
                o[k] = order_or_updates[k]
        for k in ("transactiontype", "ordertype", "producttype", "duration", "variety", "exchange"):
            if k in o and isinstance(o[k], str):
                o[k] = o[k].strip().upper()
        if "quantity" in o and o.get("quantity") is not None:
            o["quantity"] = int(float(o["quantity"]))
        if "price" in o and o.get("price") is not None:
            o["price"] = float(o["price"])
        if "triggerprice" in o and o.get("triggerprice") is not None:
            o["triggerprice"] = float(o["triggerprice"])
        o.setdefault("duration", "DAY")
        o.setdefault("variety", DEFAULT_VARIETY)
        return o

    def _validate(self, o: Dict[str, Any]) -> None:
        miss = [k for k in REQUIRED if not (o.get(k) or o.get(k) == 0)]
        if miss:
            raise ValueError(f"Order missing: {miss} | got={o}")

    def _signature(self, o: Dict[str, Any]) -> str:
        key = "|".join(str(o.get(k)) for k in [
            "exchange","tradingsymbol","symboltoken","transactiontype",
            "ordertype","producttype","duration","price","triggerprice","quantity","variety"
        ])
        return hashlib.sha1(key.encode()).hexdigest()

    def _dedupe(self, sig: str) -> bool:
        now = time.time() * 1000.0
        if not self._last_sig:
            self._last_sig = (sig, now)
            return False
        last_sig, ts = self._last_sig
        if sig == last_sig and (now - ts) < ORDER_DEDUPE_MS:
            return True
        self._last_sig = (sig, now)
        return False

    # ---------- PLACE ----------
    def _call_place(self, o: Dict[str, Any]) -> Any:
        try:
            return self.smart.placeOrder(**o)  # kwargs
        except TypeError:
            pass
        except Exception as e:
            logger.debug(f"[om] place kwargs ex: {e}")
        try:
            return self.smart.placeOrder(o)    # positional dict
        except Exception as e:
            logger.debug(f"[om] place dict-pos ex: {e}")
            raise

    def place(self, order: Dict[str, Any]) -> OrderResult:
        o = self._normalize(order)
        self._validate(o)
        sig = self._signature(o)
        if self._dedupe(sig):
            msg = f"Deduped duplicate within {ORDER_DEDUPE_MS}ms"
            logger.warning(f"[om] {msg}: {o}")
            return OrderResult(False, {}, msg)
        try:
            resp = self._call_place(o)
            ok = _ok(resp)
            if ok:
                logger.info(f"[om] place OK: {resp}")
                return OrderResult(True, resp if isinstance(resp, dict) else {"raw": resp})
            logger.error(f"[om] place FAIL resp={resp}")
            return OrderResult(False, resp if isinstance(resp, dict) else {"raw": resp}, "place non-success")
        except Exception as e:
            logger.error(f"[om] place ex: {e} | payload={o}")
            return OrderResult(False, {}, str(e))

    # ---------- CANCEL (with verification + cached strategy) ----------
    def cancel(
        self,
        order_id: str,
        *,
        variety: Optional[str] = None,
        exchange: Optional[str] = None,
        tradingsymbol: Optional[str] = None,
        producttype: Optional[str] = None,
    ) -> OrderResult:
        v = (variety or DEFAULT_VARIETY or "NORMAL")
        ex = (exchange or "").upper() or None
        ts = tradingsymbol
        pt = (producttype or "").upper() or None

        # Try cached strategy first (thread-safe fetch)
        with self._lock:
            strat = self._cancel_strategy
        if strat is not None:
            try:
                resp = strat(v, order_id)
                if _ok(resp):
                    return OrderResult(True, resp if isinstance(resp, dict) else {"raw": resp})
                logger.debug("[om] cached cancel strategy returned non-success; re-probing")
            except Exception as ex_cached:
                logger.debug(f"[om] cached cancel strategy exception: {ex_cached}; re-probing")

        def _attempts():
            # Core shapes (only these will be cached)
            yield ("pos-2",   lambda: self.smart.cancelOrder(v, order_id))
            yield ("kw-2",    lambda: self.smart.cancelOrder(variety=v, orderid=order_id))
            yield ("kw-low",  lambda: self.smart.cancelOrder(orderid=order_id))
            yield ("kw-cam",  lambda: self.smart.cancelOrder(orderId=order_id))
            # Non-core probes (won’t be cached)
            if ex:
                yield ("pos-ex",  lambda: self.smart.cancelOrder(ex, order_id))
                yield ("pos-ex3", lambda: self.smart.cancelOrder(ex, v, order_id))
                yield ("kw-ex1",  lambda: self.smart.cancelOrder(exchange=ex, orderid=order_id))
                yield ("kw-ex2",  lambda: self.smart.cancelOrder(variety=v, exchange=ex, orderid=order_id))
                if ts or pt:
                    yield ("kw-ex-tsym",  lambda: self.smart.cancelOrder(exchange=ex, orderid=order_id,
                                                                          tradingsymbol=ts, producttype=pt))
                    yield ("kw-ex-tsym2", lambda: self.smart.cancelOrder(variety=v, exchange=ex, orderid=order_id,
                                                                          tradingsymbol=ts, producttype=pt))

        last_err: Optional[str] = None
        for tag, fn in _attempts():
            try:
                resp = fn()
                if _ok(resp):
                    # Cache only core shapes
                    core = None
                    if tag == "pos-2":
                        core = lambda vv, oid: self.smart.cancelOrder(vv, oid)
                    elif tag == "kw-2":
                        core = lambda vv, oid: self.smart.cancelOrder(variety=vv, orderid=oid)
                    elif tag == "kw-low":
                        core = lambda _vv, oid: self.smart.cancelOrder(orderid=oid)
                    elif tag == "kw-cam":
                        core = lambda _vv, oid: self.smart.cancelOrder(orderId=oid)
                    if core is not None:
                        with self._lock:
                            self._cancel_strategy = core
                    return OrderResult(True, resp if isinstance(resp, dict) else {"raw": resp})
                last_err = f"{tag}: non-success response"
                logger.trace(f"[om] cancel {tag} non-success resp={resp}")
            except Exception as e:
                if _looks_like_transient_comm_err(e):
                    logger.debug(f"[om] cancel {tag} transient comm err: {e} → verifying via orderBook")
                    time.sleep(0.25)
                    st = _status_of_order(self.smart, order_id)
                    if st is None:
                        logger.info(f"[om] cancel {tag} verify OK: order {order_id} not in book")
                        # opportunistically cache a core strategy when transient looked like success
                        if tag in {"pos-2", "kw-2", "kw-low", "kw-cam"}:
                            with self._lock:
                                if tag == "pos-2":
                                    self._cancel_strategy = lambda vv, oid: self.smart.cancelOrder(vv, oid)
                                elif tag == "kw-2":
                                    self._cancel_strategy = lambda vv, oid: self.smart.cancelOrder(variety=vv, orderid=oid)
                                elif tag == "kw-low":
                                    self._cancel_strategy = lambda _vv, oid: self.smart.cancelOrder(orderid=oid)
                                else:  # kw-cam
                                    self._cancel_strategy = lambda _vv, oid: self.smart.cancelOrder(orderId=oid)
                        return OrderResult(True, {"verified": True, "orderid": order_id})
                    if st and st not in OPENISH:
                        logger.info(f"[om] cancel {tag} verify OK: order {order_id} now status={st}")
                        if tag in {"pos-2", "kw-2", "kw-low", "kw-cam"}:
                            with self._lock:
                                if tag == "pos-2":
                                    self._cancel_strategy = lambda vv, oid: self.smart.cancelOrder(vv, oid)
                                elif tag == "kw-2":
                                    self._cancel_strategy = lambda vv, oid: self.smart.cancelOrder(variety=vv, orderid=oid)
                                elif tag == "kw-low":
                                    self._cancel_strategy = lambda _vv, oid: self.smart.cancelOrder(orderid=oid)
                                else:
                                    self._cancel_strategy = lambda _vv, oid: self.smart.cancelOrder(orderId=oid)
                        return OrderResult(True, {"verified": True, "orderid": order_id, "status": st})
                    last_err = f"{tag}: transient error + verify shows status={st}"
                else:
                    last_err = f"{tag} ex: {e}"
                    logger.trace(f"[om] cancel {tag} miss: {e}")

        st = _status_of_order(self.smart, order_id)
        if st is None or (st and st not in OPENISH):
            logger.info(f"[om] cancel final-verify OK for {order_id}: status={st}")
            return OrderResult(True, {"verified": True, "orderid": order_id, "status": st or "MISSING"})
        return OrderResult(False, {}, last_err or "cancelOrder: no compatible signature worked")

    # ---------- MODIFY ----------
    def modify(self, order_id: str, updates: Dict[str, Any], variety: Optional[str] = None) -> OrderResult:
        up = self._normalize(updates)
        v = (variety or up.get("variety") or DEFAULT_VARIETY or "NORMAL")
        kw_lower = {"orderid": order_id, "variety": v}
        kw_camel = {"orderId": order_id, "variety": v}
        for k in ("ordertype", "price", "triggerprice", "quantity", "producttype", "duration"):
            if k in up:
                kw_lower[k] = up[k]
        if "ordertype" in up:   kw_camel["orderType"] = up["ordertype"]
        if "price" in up:       kw_camel["price"] = up["price"]
        if "triggerprice" in up:kw_camel["triggerPrice"] = up["triggerprice"]
        if "quantity" in up:    kw_camel["quantity"] = up["quantity"]
        if "producttype" in up: kw_camel["productType"] = up["producttype"]
        if "duration" in up:    kw_camel["duration"] = up["duration"]

        try:
            try:
                resp = self.smart.modifyOrder(**kw_lower)  # type: ignore
                if _ok(resp): return OrderResult(True, resp if isinstance(resp, dict) else {"raw": resp})
            except TypeError:
                pass
            except Exception as e:
                logger.debug(f"[om] modify kw-lower ex: {e}")

            try:
                resp = self.smart.modifyOrder(**kw_camel)  # type: ignore
                if _ok(resp): return OrderResult(True, resp if isinstance(resp, dict) else {"raw": resp})
            except TypeError:
                pass
            except Exception as e:
                logger.debug(f"[om] modify kw-camel ex: {e}")

            if ("ordertype" in up) and (("price" in up) or ("triggerprice" in up)):
                pos_args = [v, order_id, up.get("ordertype"), up.get("price"), up.get("triggerprice")]
                try:
                    resp = self.smart.modifyOrder(*pos_args)  # type: ignore
                    if _ok(resp): return OrderResult(True, resp if isinstance(resp, dict) else {"raw": resp})
                except TypeError:
                    pass
                except Exception as e:
                    logger.debug(f"[om] modify pos-legacy ex: {e}")

            try:
                resp = self.smart.modifyOrder(order_id, **{k: v for k, v in kw_lower.items() if k not in {"orderid","variety"}})  # type: ignore
                if _ok(resp): return OrderResult(True, resp if isinstance(resp, dict) else {"raw": resp})
            except TypeError:
                pass
            except Exception as e:
                logger.debug(f"[om] modify pos-min ex: {e}")

            return OrderResult(False, {}, "modifyOrder: no compatible signature worked")
        except Exception as e:
            return OrderResult(False, {}, str(e))
