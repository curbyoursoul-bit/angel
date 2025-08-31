# ops/alerts.py
from __future__ import annotations
import os, json, urllib.request
from typing import Optional
from loguru import logger

def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

BOT  = _env("TELEGRAM_BOT_TOKEN")
CHAT = _env("TELEGRAM_CHAT_ID")

def send(msg: str, chat_id: Optional[str] = None) -> bool:
    """
    Send a Telegram message. Configure env:
      TELEGRAM_BOT_TOKEN=123:abc...
      TELEGRAM_CHAT_ID=123456789
    """
    token = BOT
    chat  = chat_id or CHAT
    if not token or not chat:
        logger.debug("[alerts] telegram not configured")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat, "text": msg, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            ok = r.getcode() == 200
            if not ok:
                logger.warning(f"[alerts] non-200 response: {r.getcode()}")
            return ok
    except Exception as e:
        logger.warning(f"[alerts] send failed: {e}")
        return False
