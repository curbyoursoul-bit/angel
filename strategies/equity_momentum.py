# strategies/equity_momentum.py
from __future__ import annotations
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from loguru import logger

from utils.ltp_fetcher import get_ltp

IST = timezone(timedelta(hours=5, minutes=30))

def _symbols_from_env() -> list[str]:
    raw = os.getenv("EQUITY_MOMO_SYMBOLS", "HDFCBANK,ICICIBANK,SBIN,RELIANCE,INFY,TCS")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]

def run(smart) -> List[Dict[str, Any]]:
    """
    Equity momentum — lightweight skeleton:
      - Between 09:20–10:15 IST, look for stocks up > X% intraday and breaking above last tick high
      - This version uses only LTP slope as proxy (safe skeleton). For production,
        wire 1/5/15m candles and volume filters.
    ENV:
      EQUITY_MOMO_SYMBOLS (comma list)
      MOMO_MIN_PCT (default 1.0)
      MOMO_QTY (default 20 shares)
    """
    now = datetime.now(IST)
    if not (now.hour == 9 and now.minute >= 20 or (now.hour == 10 and now.minute <= 15)):
        return []

    min_pct = float(os.getenv("MOMO_MIN_PCT", "1.0"))
    qty = int(os.getenv("MOMO_QTY", "20"))
    syms = _symbols_from_env()

    # NOTE: Without intraday open/volume, we keep this conservative → no orders by default.
    # It logs movers; extend with your candle utils to actually trade.
    movers: list[tuple[str,float]] = []
    for s in syms:
        try:
            ltp = float(get_ltp(smart, "NSE", s, None))
            # If you have a prev close helper, replace the baseline below.
            # This placeholder treats > min_pct instant move as "mover".
            # In practice you should compare to today's OPEN or a 5m baseline.
            # We do not place orders here to avoid naive entries.
            if ltp > 0:
                # You can stash a baseline LTP at 09:20 in a state file to compare.
                pass
        except Exception:
            continue

    logger.info("equity_momentum: skeleton active (no orders). Hook to candles/volume to enable entries.")
    return []
