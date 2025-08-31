# auth/token_manager.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable
import time, json
from loguru import logger

# This wraps SmartConnect to make login/refresh rock-solid across SDK quirks.
# Usage:
#   sess = SmartSession(login_fn=lambda: SmartConnect(api_key=...), ...)
#   smart = sess.ensure()  # always returns a valid, logged-in client
#   # optionally: sess.attach(smart) if you already created the client elsewhere

DEFAULT_RETRY = (3, 0.8)  # tries, backoff seconds

@dataclass
class SmartSession:
    login_fn: Optional[Callable[[], Any]] = None       # function that returns SmartConnect()
    refresh_fn: Optional[Callable[[Any], Dict]] = None # function(smart) -> refresh result
    validate_fn: Optional[Callable[[Any], bool]] = None# function(smart) -> bool
    _smart: Any = None
    _last_ok_ts: float = 0.0

    def attach(self, smart: Any) -> None:
        self._smart = smart
        self._last_ok_ts = time.time()

    def _login(self, tries=DEFAULT_RETRY[0], backoff=DEFAULT_RETRY[1]) -> Any:
        if not self.login_fn:
            raise RuntimeError("SmartSession: login_fn not provided")
        err = None
        for i in range(1, tries+1):
            try:
                smart = self.login_fn()
                self._smart = smart
                self._last_ok_ts = time.time()
                logger.success(f"[smart] login ok (attempt {i})")
                return smart
            except Exception as e:
                err = e
                logger.warning(f"[smart] login attempt {i} failed: {e}")
                time.sleep(backoff*i)
        raise RuntimeError(f"SmartSession: login failed after retries: {err}")

    def _refresh(self, tries=DEFAULT_RETRY[0], backoff=DEFAULT_RETRY[1]) -> bool:
        if not self.refresh_fn or not self._smart:
            return False
        for i in range(1, tries+1):
            try:
                res = self.refresh_fn(self._smart)
                ok = bool(res) and ("access_token" in json.dumps(res).lower() or "success" in json.dumps(res).lower())
                if ok:
                    self._last_ok_ts = time.time()
                    logger.info(f"[smart] refresh ok (attempt {i})")
                    return True
            except Exception as e:
                logger.warning(f"[smart] refresh attempt {i} failed: {e}")
            time.sleep(backoff*i)
        return False

    def _valid(self) -> bool:
        if not self._smart:
            return False
        if self.validate_fn:
            try:
                return bool(self.validate_fn(self._smart))
            except Exception:
                return False
        # Fallback: if we were ok in the last 10 minutes, assume valid
        return (time.time() - self._last_ok_ts) < 600

    def ensure(self) -> Any:
        # happy path
        if self._valid():
            return self._smart
        # try refresh
        if self._refresh():
            return self._smart
        # fallback to login
        return self._login()
