# utils/auto_trail.py
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Tuple, Any
from loguru import logger
from datetime import datetime, time as dtime

from utils.market_hours import IST
from execution.order_manager import OrderManager

# Optional: we’ll try both signatures of get_ltp
try:
    from utils.ltp_fetcher import get_ltp
except Exception:
    get_ltp = None  # type: ignore


# ------------------ ENV / knobs (robust parsing) ------------------

def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None:
        return float(default)
    try:
        return float(str(val).strip())
    except Exception:
        logger.warning(f"[trail] bad {name}={val!r}; using {default}")
        return float(default)

def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return int(default)
    try:
        return int(float(str(val).strip()))
    except Exception:
        logger.warning(f"[trail] bad {name}={val!r}; using {default}")
        return int(default)

def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return bool(default)
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

# Market/tick config
TICK_SIZE                  = _env_float("TICK_SIZE", 0.05)
LIMIT_EXTRA_TICKS          = _env_int("LIMIT_EXTRA_TICKS", 2)         # limit = trigger + N ticks
DESIRED_ABOVE_LTP_TICKS    = _env_int("DESIRED_ABOVE_LTP_TICKS", 2)   # keep trigger this many ticks above LTP

# Arming logic (when do we start trailing the short option)
TRAIL_TRIGGER_PCT          = _env_float("TRAIL_TRIGGER_PCT", 0.40)    # +40% gain vs entry *pair* credit
TRAIL_COOLDOWN_SECS        = _env_int("TRAIL_COOLDOWN_SECS", 300)     # ignore first N seconds after entry

# Trailing behavior
AUTO_TRAIL_PCT             = _env_float("AUTO_TRAIL_PCT", 0.50)       # lock 50% of further gains
TRAIL_THROTTLE_SECS        = _env_int("TRAIL_THROTTLE_SECS", 15)
TRAIL_MIN_DELTA_TICKS      = _env_int("TRAIL_MIN_DELTA_TICKS", 2)     # skip tiny modifies < N ticks

# Optional time-based cut off (e.g., 15:20 IST)
EXIT_ON_TIME_ENABLED       = _env_bool("EXIT_ON_TIME_ENABLED", False)
CUTOFF_HHMM: Tuple[int, int] = (
    _env_int("TRAIL_CUTOFF_HH", 15),
    _env_int("TRAIL_CUTOFF_MM", 20),
)


# ------------------ helpers ------------------

def _now_ist() -> datetime:
    return datetime.now(IST)

def _round_tick(x: float) -> float:
    # Always respect a sane tick size
    step = TICK_SIZE if TICK_SIZE > 0 else 0.05
    # Quantize, then keep 2dp (Angel accepts 2dp)
    return round(round(float(x) / step) * step, 2)

def _after_cutoff() -> bool:
    if not EXIT_ON_TIME_ENABLED:
        return False
    hh, mm = CUTOFF_HHMM
    return _now_ist().time() >= dtime(hh, mm)

def _ticks(n: int) -> float:
    step = TICK_SIZE if TICK_SIZE > 0 else 0.05
    return step * max(0, int(n))

def _sf(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return default

def _get_ltp_any(smart, exchange: str, tradingsymbol: Optional[str], token: str) -> float:
    """
    Try both get_ltp styles:
      1) get_ltp(smart, exchange, token)
      2) get_ltp(smart, exchange, tradingsymbol, token)
    """
    if get_ltp is None:
        raise RuntimeError("utils.ltp_fetcher.get_ltp unavailable")
    # Try token-only variant first
    try:
        px = get_ltp(smart, exchange, token)  # type: ignore
        v = _sf(px)
        if v is not None:
            return v
    except TypeError:
        pass
    except Exception as e:
        logger.debug(f"[trail] get_ltp token-only failed: {e}")
    # Try tradingsymbol + token variant
    try:
        if not tradingsymbol:
            raise ValueError("tradingsymbol required for this get_ltp signature")
        px = get_ltp(smart, exchange, tradingsymbol, token)  # type: ignore
        v = _sf(px)
        if v is not None:
            return v
    except Exception as e:
        logger.debug(f"[trail] get_ltp tradingsymbol+token failed: {e}")
    raise RuntimeError("get_ltp: no working signature")


# ------------------ trailer core ------------------

@dataclass
class ShortLegTrailer:
    smart: Any
    symbol: str                # tradingsymbol of short leg
    token: str                 # symbol token of short leg
    stop_order_id: str         # existing STOPLOSS order id to modify
    entry_price: float         # entry price of this short option
    entry_credit_pair: float   # combined CE+PE entry credit at/near entry

    other_leg_symbol: Optional[str] = None
    other_leg_token: Optional[str] = None
    exchange: str = "NFO"

    _armed: bool = False
    _last_adjust: float = 0.0         # last stop trigger we set (for monotonic rule)
    _last_run_ts: float = 0.0
    _stop_event: threading.Event = field(default_factory=threading.Event)

    # ---- readings ----
    def _leg_ltp(self) -> float:
        return _get_ltp_any(self.smart, self.exchange, self.symbol, self.token)

    def _pair_credit_now(self) -> float:
        """
        Combined premium (short call + short put) if both legs known,
        fallback: 2x current leg if the other isn't provided.
        """
        a = _get_ltp_any(self.smart, self.exchange, self.symbol, self.token)
        if self.other_leg_symbol and self.other_leg_token:
            b = _get_ltp_any(self.smart, self.exchange, self.other_leg_symbol, self.other_leg_token)
            return a + b
        return a * 2.0

    # ---- state ----
    def _throttled(self) -> bool:
        return (time.time() - self._last_run_ts) < max(1, TRAIL_THROTTLE_SECS)

    def _arm_if_ready(self, since_entry_ts: float) -> None:
        if self._armed:
            return
        if (time.time() - since_entry_ts) < max(0, TRAIL_COOLDOWN_SECS):
            return
        try:
            credit_now = self._pair_credit_now()
        except Exception as e:
            logger.debug(f"[trail] pair_credit_now error: {e}")
            return

        drop = self.entry_credit_pair - credit_now  # drop in credit == profit made
        if drop <= 0:
            return
        gain_frac = drop / max(1e-6, self.entry_credit_pair)
        if gain_frac >= max(0.0, TRAIL_TRIGGER_PCT):
            self._armed = True
            logger.success(f"[trail] ARMED {self.symbol} (pair gain {gain_frac:.0%}); next tick will move to >= entry")

    # ---- math ----
    def _target_trigger_px(self, ltp_now: float) -> Tuple[float, float]:
        """
        Compute new (trigger, limit) for STOPLOSS_LIMIT BUY on a short leg.

        Rules:
          • Once ARMED, first move pulls trigger to at least ENTRY (breakeven).
          • Then it trails DOWN as premium falls, keeping trigger >= (ltp + buffer ticks).
          • Monotonic: trigger never increases (never loosen the stop).
        """
        if not self._armed:
            return 0.0, 0.0

        entry = float(self.entry_price)
        # Lock AUTO_TRAIL_PCT of further gains after arming
        raw = entry - AUTO_TRAIL_PCT * max(0.0, entry - ltp_now)  # between [ltp_now, entry]

        desired = max(ltp_now + _ticks(DESIRED_ABOVE_LTP_TICKS), raw)

        # First move at least to entry; afterwards, never increase
        if self._last_adjust > 0:
            trig = min(self._last_adjust, desired)
        else:
            trig = max(entry, desired)

        trig = _round_tick(trig)
        limit = _round_tick(trig + _ticks(LIMIT_EXTRA_TICKS))  # a few ticks above trigger
        return trig, limit

    # ---- broker modify ----
    def _modify_stop(self, order_id: str, new_trigger: float, new_price: float) -> bool:
        """
        Route modifies via OrderManager for SDK compatibility.
        """
        try:
            om = OrderManager(self.smart)
            updates = {
                "ordertype": "STOPLOSS_LIMIT",
                "triggerprice": new_trigger,
                "price": new_price,
                # Do NOT send variety change on modify; keep original ("NORMAL"/"AMO")
            }
            res = om.modify(order_id, updates)
            ok = bool(res.success)
            if ok:
                logger.info(f"[trail] STOP modified {order_id} → trig={new_trigger:.2f} limit={new_price:.2f}")
            else:
                logger.warning(f"[trail] STOP modify failed {order_id}: {res.error}")
            return ok
        except Exception as e:
            logger.warning(f"[trail] modify exception {order_id}: {e}")
            return False

    # ---- main loop ----
    def run_forever(self, since_entry_ts: float) -> None:
        logger.info(f"[trail] start {self.symbol} stop={self.stop_order_id} entry={self.entry_price:.2f}")
        while not self._stop_event.is_set():
            if _after_cutoff():
                logger.info("[trail] cutoff reached — stopping trailer")
                break

            if self._throttled():
                time.sleep(0.25)
                continue
            self._last_run_ts = time.time()

            try:
                # 1) arming logic
                self._arm_if_ready(since_entry_ts)

                # 2) compute desired stop
                ltp = self._leg_ltp()
                trig, limit = self._target_trigger_px(ltp)

                if trig <= 0 or limit <= 0:
                    time.sleep(0.25)
                    continue

                # 3) only send modify if meaningful change (in ticks)
                if abs(trig - (self._last_adjust or 0.0)) >= _ticks(TRAIL_MIN_DELTA_TICKS):
                    ok = self._modify_stop(self.stop_order_id, trig, limit)
                    if ok:
                        self._last_adjust = trig

                time.sleep(max(0.25, TRAIL_THROTTLE_SECS))
            except Exception as e:
                logger.warning(f"[trail] loop error {self.symbol}: {e}")
                time.sleep(0.75)

        logger.info(f"[trail] exit {self.symbol} stop={self.stop_order_id}")

    # external control
    def stop(self) -> None:
        self._stop_event.set()


# ------------------ public API ------------------

def spawn_trailer_for_short_leg(
    smart: Any,
    *,
    symbol: str,
    token: str,
    stop_order_id: str,
    entry_price: float,
    entry_credit_pair: float,
    other_leg_symbol: Optional[str] = None,
    other_leg_token: Optional[str] = None,
    exchange: str = "NFO",
    daemon: bool = True,
) -> ShortLegTrailer:
    """
    Fire-and-forget trailing thread for ONE short leg (options).
    Requires an existing STOPLOSS_LIMIT order; we will only modify it.
    Returns the trailer object so callers can stop() it if needed.
    """
    trailer = ShortLegTrailer(
        smart=smart,
        symbol=symbol,
        token=token,
        stop_order_id=stop_order_id,
        entry_price=float(entry_price),
        entry_credit_pair=float(entry_credit_pair),
        other_leg_symbol=other_leg_symbol,
        other_leg_token=other_leg_token,
        exchange=exchange,
    )
    since_entry_ts = time.time()
    th = threading.Thread(
        target=trailer.run_forever,
        args=(since_entry_ts,),
        daemon=daemon,
        name=f"trail-{symbol}-{stop_order_id[-4:] if stop_order_id else 'xxxx'}",
    )
    th.start()
    logger.info(f"[trail] spawned {th.name}")
    return trailer
