# utils/alerts.py
from __future__ import annotations
import os, time, json, math
from typing import Optional, Dict, Any, Iterable
from loguru import logger

# --- env --------------------------------------------------------------------
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT    = os.getenv("TELEGRAM_CHAT_ID")
TG_PARSE   = os.getenv("TELEGRAM_PARSE_MODE", "").strip() or None  # "HTML" | "MarkdownV2"
TG_SILENT  = os.getenv("TELEGRAM_SILENT", "0").lower() in {"1", "true", "yes"}
TG_TIMEOUT = float(os.getenv("TELEGRAM_TIMEOUT", "10"))
TG_RETRIES = int(os.getenv("TELEGRAM_RETRIES", "3"))
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")  # optional: https://hooks.slack.com/services/...

# Telegram hard limit
_TG_MAX = 4096

def _redact_token(s: str) -> str:
    if not s:
        return s
    return s[:8] + "…" if len(s) > 12 else "****"

def _split_message(msg: str, lim: int) -> Iterable[str]:
    if len(msg) <= lim:
        yield msg
        return
    # try to split on line boundaries first
    lines, buf = msg.splitlines(True), ""
    for ln in lines:
        if len(buf) + len(ln) <= lim:
            buf += ln
        else:
            if buf:
                yield buf
            # if a single line is too big, chunk it hard
            if len(ln) > lim:
                for i in range(0, len(ln), lim):
                    yield ln[i : i + lim]
                buf = ""
            else:
                buf = ln
    if buf:
        yield buf

def _post_json(url: str, payload: Dict[str, Any], timeout: float) -> tuple[int, str]:
    try:
        import requests  # keep dependency local/optional
        r = requests.post(url, json=payload, timeout=timeout)
        return r.status_code, r.text
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"

def _send_telegram(text: str, *, silent: Optional[bool] = None, parse_mode: Optional[str] = TG_PARSE) -> bool:
    if not TG_TOKEN or not TG_CHAT:
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    ok_all = True
    for part in _split_message(text, _TG_MAX):
        payload = {"chat_id": TG_CHAT, "text": part, "disable_notification": bool(TG_SILENT if silent is None else silent)}
        if parse_mode in {"HTML", "MarkdownV2"}:
            payload["parse_mode"] = parse_mode

        # simple retries with decorrelated jitter
        attempt, ok = 0, False
        while attempt < max(1, TG_RETRIES) and not ok:
            attempt += 1
            code, body = _post_json(url, payload, TG_TIMEOUT)
            ok = (code == 200)
            if not ok and attempt < TG_RETRIES:
                sleep_s = min(6.0, 0.6 * attempt) + (0.2 * attempt)
                time.sleep(sleep_s)
        if not ok:
            logger.warning(f"Telegram notify failed (token={_redact_token(TG_TOKEN)}): {code} {body[:200]}")
            ok_all = False
    return ok_all

def _send_slack(text: str) -> bool:
    if not SLACK_WEBHOOK:
        return False
    code, body = _post_json(SLACK_WEBHOOK, {"text": text}, TG_TIMEOUT)
    if code != 200:
        logger.warning(f"Slack notify failed: {code} {body[:200]}")
        return False
    return True

def notify(message: str, *, silent: bool = False, parse_mode: Optional[str] = TG_PARSE) -> None:
    """
    Best-effort alert:
      - Telegram (if TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID present)
      - Slack webhook (if SLACK_WEBHOOK_URL present)
      - Always logs to INFO as a fallback
    """
    sent_any = False
    try:
        sent_any |= _send_telegram(message, silent=silent, parse_mode=parse_mode)
    except Exception as e:
        logger.warning(f"Telegram notify error: {e}")

    try:
        sent_any |= _send_slack(message)
    except Exception as e:
        logger.warning(f"Slack notify error: {e}")

    # Always log (so alerts show up in logs even if chat fails)
    (logger.info if sent_any else logger.warning)(f"[ALERT]{' (silent)' if silent else ''} {message}")

def notify_json(obj: Any, *, indent: int = 2, silent: bool = False) -> None:
    """Helper to pretty-print JSON in alerts."""
    try:
        txt = json.dumps(obj, indent=indent, ensure_ascii=False)
    except Exception:
        txt = str(obj)
    notify(txt, silent=silent)
