# core/login.py
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Tuple, Optional

from loguru import logger

# ── Robust Loguru sink setup (no private internals) ───────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
if not getattr(logger, "_angel_sink_added", False):  # type: ignore[attr-defined]
    logger.add(
        LOG_DIR / "app.log",
        rotation="1 week",
        retention="4 weeks",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )
    setattr(logger, "_angel_sink_added", True)  # type: ignore[attr-defined]

import pyotp

# --- SmartApi imports (handle SDK path variants) -----------------------------
try:
    # Newer packaging
    from SmartApi.smartConnect import SmartConnect
except Exception:
    try:
        from SmartApi import SmartConnect  # type: ignore
    except Exception as e:
        # We'll still allow DRY mode with a dummy, but LIVE will fail to import here.
        SmartConnect = None  # type: ignore

# Exception shims
try:
    from SmartApi.smartExceptions import DataException as SmartDataException  # type: ignore
except Exception:
    class SmartDataException(Exception):  # type: ignore
        pass

try:
    from SmartApi.smartExceptions import NetworkException as SmartNetException  # type: ignore
except Exception:
    class SmartNetException(Exception):  # type: ignore
        pass

# --- Config (single source of truth) -----------------------------------------
from config import (
    API_KEY, CLIENT_CODE, PASSWORD, TOTP_SECRET,
    TOKEN_FILE,  # Path from config
    DRY_RUN,
    _i as _ci,   # int env
)

# Optional knobs
LOGIN_MAX_ATTEMPTS   = _ci("LOGIN_MAX_ATTEMPTS", 6)
REFRESH_MAX_ATTEMPTS = _ci("REFRESH_MAX_ATTEMPTS", 6)
FAST_FRESH_AFTER     = _ci("FAST_FRESH_AFTER", 2)  # early fallback to fresh login after N refresh errors
DISABLE_CLOCK_CHECK  = os.getenv("DISABLE_CLOCK_CHECK", "").strip().lower() in {"1","true","yes","on"}

# --- optional clock sanity check ---------------------------------------------
def _clock_drift_status(max_skew_seconds: float = 2.0) -> Tuple[bool, float, int]:
    """
    Best-effort: returns (ok, skew_seconds, samples_used).
    Will degrade gracefully if utils.clock is missing.
    """
    if DISABLE_CLOCK_CHECK:
        return True, 0.0, 0
    try:
        from utils.clock import check_clock_drift  # type: ignore
        ok, skew, samples = check_clock_drift(max_skew_seconds=max_skew_seconds)
        return bool(ok), float(skew), int(samples)
    except Exception:
        return True, 0.0, 0

# --- helpers -----------------------------------------------------------------
def _totp_now(secret: str) -> str:
    if not secret:
        raise RuntimeError("TOTP secret missing (env TOTP_SECRET).")
    totp = pyotp.TOTP(secret)
    now = int(time.time())
    # Avoid boundary to reduce AB1050 / invalid TOTP on slow networks
    if (now % 30) >= 28:
        time.sleep(2.2)
    return totp.now()

def _retry_sleep(attempt: int) -> None:
    # exponential-ish backoff with jitter
    import random
    base = min(0.6 * attempt, 3.5)
    jitter = random.uniform(0.20, 0.65)
    time.sleep(base + jitter)

def _looks_transient(err: Exception | str) -> bool:
    msg = str(err).lower()
    needles = (
        "couldn't parse the json",
        "jsondecode",
        "expecting value",
        "b''",
        "parse issue",
        "temporarily unavailable",
        "connection aborted",
        "timed out",
        "read timeout",
        "max retries exceeded",
        "bad gateway",
        "service unavailable",
        "502", "503", "504",
    )
    return any(s in msg for s in needles)

def _set_access_from_payload(smart, primary: dict, fallback: Optional[dict] = None) -> str:
    fallback = fallback or {}
    token = (
        primary.get("accessToken")
        or primary.get("jwtToken")
        or fallback.get("jwtToken")
        or fallback.get("accessToken")
    )
    if not token:
        raise RuntimeError(f"No usable access/jwt token in payloads. primary={primary}, fallback={fallback}")
    smart.setAccessToken(token)
    return token

def _write_token_file(refresh_token: str, access_token: str) -> None:
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = TOKEN_FILE.with_suffix(".json.tmp")
    payload = {
        "client_code": CLIENT_CODE,
        "api_key": API_KEY,
        "refresh_token": refresh_token,
        "access_token": access_token,
        "login_time": int(time.time()),
    }
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(TOKEN_FILE)

def _read_saved_refresh() -> Optional[str]:
    try:
        if not TOKEN_FILE.exists():
            return None
        saved = json.loads(TOKEN_FILE.read_text(encoding="utf-8") or "{}")
        rtoken = saved.get("refresh_token")
        return str(rtoken) if rtoken else None
    except Exception:
        return None

# --- DRY dummy client (lets you run engine/tests without creds) --------------
class _DummySmart:
    def __init__(self):
        self._token = "DUMMY"
    # methods the app might call:
    def setAccessToken(self, token: str) -> None:
        self._token = token
    def generateSession(self, *a, **kw):  # never used in DRY
        return {"status": True, "data": {"refreshToken": "DUMMY_REFRESH"}}
    def generateToken(self, *a, **kw):
        return {"status": True, "data": {"jwtToken": "DUMMY_JWT"}}
    def getProfile(self, *a, **kw):
        return {"status": True, "data": {"name": "DRY_USER"}}
    def ltpData(self, **payload):
        # return a deterministic-ish LTP
        import random
        random.seed(hash((payload.get("exchange"), payload.get("tradingsymbol"), payload.get("symboltoken"))) % (2**32))
        return {"status": True, "data": {"ltp": round(random.uniform(80, 220), 2)}}
    def orderBook(self):
        return {"status": True, "data": []}
    def placeOrder(self, *a, **kw):
        return {"status": True, "data": {"orderid": "OID-DUMMY-1"}}
    def cancelOrder(self, *a, **kw):
        return {"status": True}

# --- core flows --------------------------------------------------------------
def _fresh_login(smart) -> object:
    """
    LIVE login flow. In DRY mode, the callers will avoid invoking this.
    """
    assert not DRY_RUN, "Internal: _fresh_login should not be called in DRY mode."
    assert API_KEY and CLIENT_CODE and (PASSWORD) and TOTP_SECRET, \
        "Missing creds in .env/config (need API_KEY, CLIENT_CODE/CLIENT_ID, PASSWORD/MPIN, TOTP_SECRET)"

    max_attempts = LOGIN_MAX_ATTEMPTS
    sess, sess_data, last_err = None, {}, None

    # 1) Session (password + TOTP)
    for attempt in range(1, max_attempts + 1):
        totp = _totp_now(TOTP_SECRET)
        try:
            sess = smart.generateSession(CLIENT_CODE, PASSWORD, totp)
        except (SmartDataException, SmartNetException) as e:
            last_err = e
            if _looks_transient(e) and attempt < max_attempts:
                logger.info(f"generateSession transient (attempt {attempt}/{max_attempts}); retrying …")
                _retry_sleep(attempt)
                continue
            raise
        except Exception as e:
            last_err = e
            logger.warning(f"generateSession error (attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                _retry_sleep(attempt)
                continue
            raise

        if sess and sess.get("status"):
            sess_data = sess.get("data") or {}
            break

        # invalid TOTP boundary
        msg = str((sess or {}).get("message", "")).lower()
        errcode = (sess or {}).get("errorcode")
        if ("invalid totp" in msg or errcode == "AB1050") and attempt < max_attempts:
            logger.warning("Invalid TOTP (AB1050); waiting and retrying …")
            time.sleep(1.5)
            continue

        if attempt < max_attempts:
            logger.info(f"Login attempt {attempt} returned bad status; retrying … ({sess})")
            _retry_sleep(attempt)
            continue
        break

    if not (sess and sess.get("status")):
        raise RuntimeError(
            f"Login failed after {max_attempts} attempts: "
            f"{ {'status': False, 'message': (sess or {}).get('message'), 'errorcode': (sess or {}).get('errorcode'), 'last_err': repr(last_err)} }"
        )

    refresh_token = (sess_data or {}).get("refreshToken")
    if not refresh_token:
        raise RuntimeError(f"Login OK but no refreshToken in response: {sess}")

    # 2) Exchange refresh → access
    max_attempts = LOGIN_MAX_ATTEMPTS
    tok, tok_data, last_err = None, {}, None
    for attempt in range(1, max_attempts + 1):
        try:
            tok = smart.generateToken(refresh_token)
        except (SmartDataException, SmartNetException) as e:
            last_err = e
            if _looks_transient(e) and attempt < max_attempts:
                logger.info(f"generateToken transient (attempt {attempt}/{max_attempts}); retrying …")
                _retry_sleep(attempt)
                continue
            raise
        except Exception as e:
            last_err = e
            logger.warning(f"generateToken error (attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                _retry_sleep(attempt)
                continue
            raise

        if tok and tok.get("status"):
            tok_data = tok.get("data") or {}
            break

        if attempt < max_attempts:
            logger.info(f"generateToken bad status; retrying … ({tok})")
            _retry_sleep(attempt)
            continue

    if not (tok and tok.get("status")):
        raise RuntimeError(f"generateToken failed after {max_attempts} attempts: {tok} | last_err={last_err!r}")

    access_token = _set_access_from_payload(smart, tok_data, sess_data)

    # Best-effort profile fetch (verifies auth)
    try:
        smart.getProfile(refresh_token)
    except Exception as e:
        logger.warning(f"getProfile warning (ignored): {e}")

    _write_token_file(refresh_token, access_token)
    logger.success("✅ Logged in!")
    return smart

def _new_live_client() -> SmartConnect: # type: ignore
    assert SmartConnect is not None, "SmartApi SDK not installed"
    return SmartConnect(api_key=API_KEY)

def login():
    """
    Backwards-compat entrypoint: in DRY mode returns a dummy client.
    """
    if DRY_RUN:
        logger.info("DRY_RUN=True → returning dummy Smart client (no credentials needed).")
        return _DummySmart()

    ok_clock, skew, succ = _clock_drift_status(max_skew_seconds=2.0)
    if succ > 0 and not ok_clock:
        direction = "behind" if skew > 0 else "ahead"
        logger.warning(
            f"System clock is {abs(skew):.2f}s {direction} vs NTP (based on {succ} server(s)). "
            "TOTP may fail; please sync your system clock."
        )
    smart = _new_live_client()
    return _fresh_login(smart)

def restore_or_login():
    """
    Preferred: DRY returns dummy; LIVE tries refresh then full login.
    """
    if DRY_RUN:
        logger.info("DRY_RUN=True → returning dummy Smart client (no credentials needed).")
        return _DummySmart()

    ok_clock, skew, succ = _clock_drift_status(max_skew_seconds=2.0)
    if succ > 0 and not ok_clock:
        direction = "behind" if skew > 0 else "ahead"
        logger.warning(
            f"System clock is {abs(skew):.2f}s {direction} vs NTP (based on {succ} server(s)). "
            "TOTP/refresh may be flaky; consider syncing your system clock."
        )

    smart = _new_live_client()

    rtoken = _read_saved_refresh()
    if rtoken:
        last_err: Exception | None = None
        for attempt in range(1, REFRESH_MAX_ATTEMPTS + 1):
            try:
                tok = smart.generateToken(rtoken)
            except (SmartDataException, SmartNetException) as e:
                last_err = e
                if _looks_transient(e):
                    logger.info(f"refresh generateToken transient (attempt {attempt}/{REFRESH_MAX_ATTEMPTS})")
                    if attempt >= FAST_FRESH_AFTER:
                        logger.warning("Early fallback to fresh login due to repeated transient refresh errors.")
                        break
                    _retry_sleep(attempt)
                    continue
                logger.warning("Non-transient refresh error; switching to fresh login.")
                break
            except Exception as e:
                last_err = e
                logger.warning(f"refresh generateToken error (attempt {attempt}/{REFRESH_MAX_ATTEMPTS}): {e}")
                if attempt < REFRESH_MAX_ATTEMPTS:
                    _retry_sleep(attempt)
                    continue
                break

            if tok and tok.get("status"):
                tok_data = tok.get("data") or {}
                access_token = _set_access_from_payload(smart, tok_data, {})
                logger.success("🔄 Token refreshed")
                try:
                    smart.getProfile(rtoken)
                except Exception as e:
                    logger.warning(f"getProfile after refresh warning: {e}")
                _write_token_file(rtoken, access_token)
                return smart

            logger.info(f"refresh generateToken bad status; retrying … ({tok})")
            if attempt < REFRESH_MAX_ATTEMPTS:
                _retry_sleep(attempt)
                continue

        logger.warning(f"Refresh failed or skipped; falling back to fresh login. last_err={last_err!r}")

    return _fresh_login(smart)

# --- CLI smoke test ----------------------------------------------------------
if __name__ == "__main__":
    logger.info("Login smoke test starting …")
    try:
        c = restore_or_login()
        try:
            prof = c.getProfile("")  # dummy client also supports this
            logger.info(f"Profile (best-effort): {str(prof)[:200]} …")
        except Exception as e:
            logger.warning(f"Profile fetch warning (ignored): {e}")
        logger.success("Login smoke test OK.")
    except Exception as e:
        logger.exception(f"Login smoke test FAILED: {e}")
        raise
