# utils/ltp_fetcher.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
from loguru import logger
import random
import time

# ------------------------------------------------------------------
# Config-driven knobs
# ------------------------------------------------------------------
try:
    from config import LTP_CACHE_TTL_S, QUOTE_RETRY, QUOTE_RETRY_DELAY_S
except Exception:
    LTP_CACHE_TTL_S = 1.0
    QUOTE_RETRY = 2
    QUOTE_RETRY_DELAY_S = 0.25

# Optional guardrails for obviously bogus prices
_MIN_PX = 0.05
_MAX_PX = 1_000_000.0

# Angel index tokens (consistent with rest of repo)
ANGEL_INDEX_TOKENS = {
    "NIFTY": "26000",
    "BANKNIFTY": "26009",
    # add FINNIFTY, MIDCPNIFTY here if you use them
}

# ------------------------------------------------------------------
# Tiny in-proc cache: key -> (price, ts)
# ------------------------------------------------------------------
_cache: dict[Tuple[str, str, str], Tuple[float, float]] = {}

def _cache_get(exchange: str, tradingsymbol: str, symboltoken: str) -> Optional[float]:
    key = (exchange, tradingsymbol, symboltoken)
    ent = _cache.get(key)
    if not ent:
        return None
    px, ts_ = ent
    if (time.time() - ts_) <= float(LTP_CACHE_TTL_S):
        return float(px)
    # stale -> drop
    _cache.pop(key, None)
    return None

def _cache_put(exchange: str, tradingsymbol: str, symboltoken: str, px: float) -> None:
    _cache[(exchange, tradingsymbol, symboltoken)] = (float(px), time.time())

# ------------------------------------------------------------------
# Raw SmartAPI calls (handle SDK variations)
# ------------------------------------------------------------------
def _call_ltp(smart, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try multiple SmartAPI variants:
      - ltpData(**payload) / ltpData(payload)
      - getLtpData(**payload) / getLtpData(payload)
      - ltpDataV2(**payload) / ltpDataV2(payload)
    Always returns a dict (may be empty on failure).
    """
    for fn_name in ("ltpData", "getLtpData", "ltpDataV2"):
        fn = getattr(smart, fn_name, None)
        if not callable(fn):
            continue
        # Try kwargs first
        try:
            r = fn(**payload)
            if isinstance(r, dict):
                return r
        except TypeError:
            pass
        except Exception:
            # fall through to positional try
            pass
        # Try single-dict positional
        try:
            r = fn(payload)
            if isinstance(r, dict):
                return r
        except TypeError:
            pass
        except Exception:
            # try next variant
            continue
    return {}

# ------------------------------------------------------------------
# Payload parsing
# ------------------------------------------------------------------
def _extract_ltp(resp: Dict[str, Any]) -> Optional[float]:
    """
    Normalize various Angel-ish payloads into a float LTP.
    Looks into resp['data'] (dict OR list[dict]) and common keys.
    Also checks some top-level fallbacks seen in the wild.
    """
    if not resp:
        return None

    # primary: data field (dict or list[dict])
    data = resp.get("data")
    if isinstance(data, list) and data:
        data = data[0]
    if isinstance(data, dict):
        for key in ("ltp", "last_price", "lastPrice", "last_traded_price", "Ltp", "lp"):
            if key in data:
                try:
                    v = float(data[key])
                    return v if _MIN_PX <= v <= _MAX_PX else None
                except Exception:
                    continue

    # some SDKs leak price at top-level (rare)
    for key in ("ltp", "last_price", "lastPrice", "Ltp"):
        if key in resp:
            try:
                v = float(resp[key])
                return v if _MIN_PX <= v <= _MAX_PX else None
            except Exception:
                continue

    return None

# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------
def get_ltp(
    smart,
    exchange: str,
    tradingsymbol: str,
    symboltoken: str | int | None,
    *,
    retries: Optional[int] = None,
    delay: Optional[float] = None,
    jitter: float = 0.25,
    use_cache: bool = True,
) -> float:
    """
    Resilient LTP fetch with small jittered retries + tiny TTL cache.
    Raises RuntimeError if unable to obtain a positive price.
    """
    exchange = str(exchange).upper()
    tradingsymbol = str(tradingsymbol).upper()
    symboltoken = str(symboltoken or "").strip()

    if use_cache:
        hit = _cache_get(exchange, tradingsymbol, symboltoken)
        if hit is not None:
            return hit

    payload = {
        "exchange": exchange,
        "tradingsymbol": tradingsymbol,
        "symboltoken": symboltoken,
    }

    max_tries = int(QUOTE_RETRY if retries is None else retries)
    base_delay = float(QUOTE_RETRY_DELAY_S if delay is None else delay)

    last_err = "unknown error"
    for attempt in range(max_tries + 1):
        try:
            resp = _call_ltp(smart, payload)
            if resp.get("status") is True or str(resp.get("message","")).lower().startswith("success"):
                px = _extract_ltp(resp)
                if px and _MIN_PX <= px <= _MAX_PX:
                    if use_cache:
                        _cache_put(exchange, tradingsymbol, symboltoken, px)
                    return px
                last_err = f"no usable price in response: {resp!r}"
            else:
                msg = str(resp.get("message") or resp)
                last_err = f"ltp status false: {msg}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        if attempt < max_tries:
            slp = max(0.05, base_delay + random.uniform(-jitter, jitter))
            time.sleep(slp)

    logger.error(
        f"LTP fetch failed for {exchange}:{tradingsymbol} (token={symboltoken}) → {last_err}"
    )
    raise RuntimeError(last_err)

def get_index_ltp(smart, index: str = "BANKNIFTY", **kwargs) -> Optional[float]:
    """
    Convenience for index LTP via Angel tokens. Returns None on failure.
    """
    idx = str(index).upper()
    token = ANGEL_INDEX_TOKENS.get(idx, "")
    try:
        return get_ltp(smart, "NSE", idx, token, **kwargs)
    except Exception:
        return None

# Back-compat alias used elsewhere in the repo
def get_banknifty_ltp(smart, **kwargs) -> Optional[float]:
    return get_index_ltp(smart, "BANKNIFTY", **kwargs)
