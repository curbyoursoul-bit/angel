# utils/streaming.py
from __future__ import annotations

import json
import threading
import time
from typing import Callable, Iterable, Dict, Any, Optional, List
from loguru import logger

try:
    from SmartApi import SmartWebSocketV2 as SmartWS  # Angel's V2 WS
except Exception:
    SmartWS = None


class StreamingClient:
    """
    Robust wrapper around SmartWebSocketV2.

    - Auto-reconnect with jittered backoff
    - Graceful stop()
    - Heartbeat watchdog
    - Dynamic subscribe/unsubscribe
    - Thread-safe; user callbacks isolated

    Usage:
        sc = StreamingClient(api_key, client_code, jwt)
        sc.start([{"exchangeType":2,"tokens":["26009"]}], on_tick=my_handler)
        ...
        sc.stop()
    """

    def __init__(
        self,
        api_key: str,
        client_code: str,
        jwt: str,
        *,
        heartbeat_sec: float = 15.0,
        max_backoff_sec: float = 30.0,
    ):
        if SmartWS is None:
            raise RuntimeError("SmartWebSocketV2 not available in this environment")

        self.api_key = api_key
        self.client_code = client_code
        self.jwt = jwt

        self.heartbeat_sec = heartbeat_sec
        self.max_backoff_sec = max_backoff_sec

        self._sws: Optional[SmartWS] = None # type: ignore
        self._lock = threading.RLock()
        self._stop_evt = threading.Event()
        self._last_tick_ts = 0.0

        # keep desired subscriptions so we can re-apply after reconnect
        self._desired_subs: List[Dict[str, Any]] = []
        self._on_tick: Optional[Callable[[Dict[str, Any]], None]] = None

        # watchdog thread
        self._watchdog_th: Optional[threading.Thread] = None

    # ---------- public API ----------

    def start(
        self,
        subscriptions: Iterable[Dict[str, Any]],
        on_tick: Callable[[Dict[str, Any]], None],
    ):
        with self._lock:
            self._desired_subs = list(subscriptions or [])
            self._on_tick = on_tick
            self._stop_evt.clear()
            self._spawn_ws()
            if self.heartbeat_sec > 0 and (self._watchdog_th is None or not self._watchdog_th.is_alive()):
                self._watchdog_th = threading.Thread(target=self._watchdog_loop, daemon=True)
                self._watchdog_th.start()

    def stop(self):
        self._stop_evt.set()
        with self._lock:
            try:
                if self._sws:
                    self._sws.close()  # SmartWS has close(); safe even if already closed
            except Exception as e:
                logger.debug(f"WS close error: {e}")
            self._sws = None

    def subscribe(self, sub: Dict[str, Any]):
        """sub = {'exchangeType': 1|2, 'tokens': ['26009', ...]}"""
        with self._lock:
            self._desired_subs.append(dict(sub))
            if self._sws:
                try:
                    self._sws.subscribe(sub)
                    logger.info(f"Subscribed: {sub}")
                except Exception as e:
                    logger.warning(f"subscribe failed: {e}")

    def unsubscribe(self, sub: Dict[str, Any]):
        with self._lock:
            # remove first matching subscription intent
            for i, s in enumerate(self._desired_subs):
                if s == sub:
                    self._desired_subs.pop(i)
                    break
            if self._sws:
                try:
                    self._sws.unsubscribe(sub)
                    logger.info(f"Unsubscribed: {sub}")
                except Exception as e:
                    logger.warning(f"unsubscribe failed: {e}")

    # ---------- internals ----------

    def _spawn_ws(self):
        # Create a fresh WS instance and connect in background
        sws = SmartWS(self.api_key, self.client_code, self.jwt)

        def _on_data(wsapp, message):
            self._last_tick_ts = time.time()
            msg = self._parse_tick(message)
            if not msg:
                return
            cb = self._on_tick
            if cb:
                try:
                    cb(msg)
                except Exception as e:
                    logger.exception(f"tick handler failed: {e}")

        def _on_open(wsapp):
            logger.info("WS open")
            # re-apply desired subscriptions
            for sub in list(self._desired_subs):
                try:
                    sws.subscribe(sub)
                except Exception as e:
                    logger.warning(f"subscribe failed on open: {e}")

        def _on_error(wsapp, error):
            logger.error(f"WS error: {error}")

        def _on_close(wsapp):
            logger.warning("WS closed")
            # reconnect unless stopping
            self._schedule_reconnect()

        sws.on_data = _on_data
        sws.on_open = _on_open
        sws.on_error = _on_error
        sws.on_close = _on_close

        self._sws = sws
        threading.Thread(target=self._connect_blocking, args=(sws,), daemon=True).start()

    def _connect_blocking(self, sws):
        try:
            sws.connect()
        except Exception as e:
            logger.error(f"WS connect() failed: {e}")
            self._schedule_reconnect()

    def _schedule_reconnect(self):
        if self._stop_evt.is_set():
            return
        # backoff with jitter based on last tick age
        age = time.time() - self._last_tick_ts if self._last_tick_ts else self.max_backoff_sec
        delay = min(max(1.0, age), self.max_backoff_sec)
        logger.info(f"Reconnecting in ~{delay:.1f}s")
        threading.Timer(delay, self._reconnect).start()

    def _reconnect(self):
        if self._stop_evt.is_set():
            return
        with self._lock:
            try:
                if self._sws:
                    self._sws.close()
            except Exception:
                pass
            self._sws = None
            self._spawn_ws()

    def _watchdog_loop(self):
        # detects dead socket by missing ticks for ~2x heartbeat
        period = max(5.0, self.heartbeat_sec)
        while not self._stop_evt.wait(period):
            last = self._last_tick_ts
            if last and (time.time() - last) > (2.5 * self.heartbeat_sec):
                logger.warning("Heartbeat stale; forcing reconnect")
                self._reconnect()

    @staticmethod
    def _parse_tick(message: Any) -> Optional[Dict[str, Any]]:
        """
        Normalize tick payload to a dict.
        SmartWS sends dicts already; guard for string JSON.
        """
        try:
            if isinstance(message, (dict, list)):
                return message if isinstance(message, dict) else {"data": message}
            if isinstance(message, (bytes, bytearray)):
                message = message.decode("utf-8", errors="ignore")
            if isinstance(message, str):
                return json.loads(message)
        except Exception:
            logger.debug(f"Unparseable tick: {message!r}")
        return None


# ---------- convenience top-level API (keeps your old function working) ----------

def connect_ws(
    api_key: str,
    client_code: str,
    jwt: str,
    subscriptions: Iterable[Dict[str, Any]],
    on_tick: Callable[[Dict[str, Any]], None],
) -> Optional[StreamingClient]:
    """
    Backwards-compatible helper.
    Returns a StreamingClient you can stop() or add more subscriptions to.
    """
    if SmartWS is None:
        logger.warning("SmartWebSocketV2 not available; using pull-mode only.")
        return None
    sc = StreamingClient(api_key, client_code, jwt)
    sc.start(subscriptions, on_tick)
    return sc
