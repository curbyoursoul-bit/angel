# utils/angel_timeout.py
from __future__ import annotations
from loguru import logger
import functools

# Adjust if you want different limits
CONNECT_TIMEOUT = 0.8
READ_TIMEOUT    = 1.6

def apply_http_timeouts(_smart=None) -> None:
    """
    Globally wraps requests.Session.request to enforce a default timeout
    whenever a SmartAPI call (or any call) doesn't pass one.
    Safe across SmartAPI versions—no reliance on SmartConnect internals.
    Idempotent: only wraps once.
    """
    try:
        import requests

        # already patched?
        if getattr(requests.Session.request, "_angel_timeout_wrapped", False):  # type: ignore[attr-defined]
            return

        orig_request = requests.Session.request

        @functools.wraps(orig_request)
        def wrapped(self, method, url, **kwargs):
            # If the caller didn't provide a timeout, inject ours
            if "timeout" not in kwargs or kwargs["timeout"] is None:
                kwargs["timeout"] = (CONNECT_TIMEOUT, READ_TIMEOUT)
            return orig_request(self, method, url, **kwargs)

        wrapped._angel_timeout_wrapped = True  # type: ignore[attr-defined]
        requests.Session.request = wrapped  # type: ignore[assignment]
        logger.info(f"[angel_timeout] HTTP timeouts enforced globally ({CONNECT_TIMEOUT}s connect, {READ_TIMEOUT}s read)")
    except Exception as e:
        logger.debug(f"[angel_timeout] patch skipped: {e}")
