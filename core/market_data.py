# core/market_data.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
from loguru import logger
from utils.ltp_fetcher import get_ltp
from utils.history import fetch_candles

class MarketData:
    """Unified access to LTP and candles."""

    def ltp(self, exchange: str, tradingsymbol: str, symboltoken: str) -> Optional[float]:
        try:
            return get_ltp(exchange=exchange, tradingsymbol=tradingsymbol, symboltoken=symboltoken)
        except Exception as e:
            logger.error(f"ltp error for {tradingsymbol}: {e}")
            return None

    def candles(self, exchange: str, tradingsymbol: str, interval: str, count: int) -> List[Dict[str, Any]]:
        try:
            return fetch_candles(exchange=exchange, tradingsymbol=tradingsymbol, interval=interval, count=count)
        except Exception as e:
            logger.error(f"candles error for {tradingsymbol}: {e}")
            return []
