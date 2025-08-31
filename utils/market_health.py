# utils/market_health.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
from loguru import logger

from config import VOL_MAX_SPREAD_PCT
from utils.ltp_fetcher import get_ltp

# Normalized quote dict keys we care about
# We expect to extract: best_bid, best_ask, ltp, vol, oi (where available)

def _call_quote(smart, exchange: str, tradingsymbol: str, symboltoken: str) -> Dict[str, Any]:
    """
    Try SmartAPI quote variants:
      - smart.quoteData(exchange, tradingsymbol, symboltoken)
      - smart.quoteDataV2(...)
      - smart.getQuoteData(...)
    Returns dict (may be empty).
    """
    payload = {
        "exchange": str(exchange).upper(),
        "tradingsymbol": str(tradingsymbol),
        "symboltoken": str(symboltoken),
    }
    for name in ("quoteData", "quoteDataV2", "getQuoteData"):
        fn = getattr(smart, name, None)
        if not callable(fn):
            continue
        # kwargs first
        try:
            res = fn(**payload)  # type: ignore[misc]
            if isinstance(res, dict):
                return res
        except TypeError:
            pass
        except Exception:
            pass
        # single dict positional
        try:
            res = fn(payload)  # type: ignore[misc]
            if isinstance(res, dict):
                return res
        except TypeError:
            pass
        except Exception:
            continue
    return {}

def _first_num(d: Dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        if k in d:
            try:
                v = float(d[k])
                if v == v:  # not NaN
                    return v
            except Exception:
                continue
    return None

def _extract_primary(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Angel-ish quote payloads into a flat dict.
    Handles data=dict, data=list[dict], nested 'fetched' fields, etc.
    """
    data = resp.get("data")
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        data = {}

    # Some variants have nested book under 'fetched' / 'depth'
    book = data.get("fetched") or data.get("depth") or {}
    if isinstance(book, list) and book:
        # occasionally it's a list with a single dict
        book = book[0] if isinstance(book[0], dict) else {}

    # Best bid/ask
    bid = None
    ask = None

    # flat keys
    bid = bid or _first_num(data, "best_bid_price", "bestBid", "best_bid", "bidPrice", "bidprice", "bp")
    ask = ask or _first_num(data, "best_ask_price", "bestAsk", "best_ask", "askPrice", "askprice", "ap")

    # nested common structures
    if bid is None or ask is None:
        # depth -> buy/sell arrays
        for side_key, is_bid in (("buy", True), ("sell", False)):
            arr = book.get(side_key)
            if isinstance(arr, list) and arr:
                try:
                    px = float(arr[0].get("price"))
                    if is_bid and bid is None:
                        bid = px
                    if not is_bid and ask is None:
                        ask = px
                except Exception:
                    pass
        # single nested keys
        bid = bid or _first_num(book, "bp", "bestBid", "best_bid_price")
        ask = ask or _first_num(book, "ap", "bestAsk", "best_ask_price")

    ltp = _first_num(data, "ltp", "last_price", "lastPrice", "Ltp", "lp")
    vol = _first_num(data, "volume", "volume_traded", "VolumeTradedToday")
    oi  = _first_num(data, "oi", "open_interest", "OpenInterest")

    return {
        "best_bid": bid,
        "best_ask": ask,
        "ltp": ltp,
        "volume": vol,
        "oi": oi,
        "raw": data,
    }

def fetch_quote(smart, exchange: str, tradingsymbol: str, symboltoken: str) -> Dict[str, Any]:
    """
    Return a normalized quote dict with keys: best_bid, best_ask, ltp, volume, oi.
    Falls back to fetching LTP if quote lacks it.
    """
    resp = _call_quote(smart, exchange, tradingsymbol, symboltoken)
    if not resp:
        logger.debug("quote: empty response; attempting LTP fallback")
        ltp = None
        try:
            ltp = get_ltp(smart, exchange, tradingsymbol, symboltoken, use_cache=True)
        except Exception:
            pass
        return {"best_bid": None, "best_ask": None, "ltp": ltp, "volume": None, "oi": None, "raw": {}}

    out = _extract_primary(resp)
    if out.get("ltp") in (None,):
        try:
            out["ltp"] = get_ltp(smart, exchange, tradingsymbol, symboltoken, use_cache=True)
        except Exception:
            pass
    return out

def _spread_pct(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    try:
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
            return None
        mid = 0.5 * (bid + ask)
        if mid <= 0:
            return None
        return (ask - bid) / mid
    except Exception:
        return None

def illiquid_or_wide(q: Dict[str, Any], max_spread_pct: float = float(VOL_MAX_SPREAD_PCT)) -> bool:
    """
    Returns True if the quote looks illiquid or has too wide a spread.
    Heuristics:
      - missing bid/ask entirely → treat as illiquid
      - spread% > threshold
      - (optional) extremely low volume with missing bid/ask
    """
    bid = q.get("best_bid")
    ask = q.get("best_ask")
    sp  = _spread_pct(bid, ask)
    if bid is None or ask is None:
        # If we at least have a sane LTP, we can be generous; otherwise block
        ltp = q.get("ltp")
        return not (isinstance(ltp, (int, float)) and ltp > 0)
    if sp is None:
        return True
    return sp > float(max_spread_pct or 0.08)
