# backtest/backtester.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Callable
from loguru import logger

@dataclass
class Trade:
    ts: str
    side: str
    qty: int
    price: float
    tag: str

class Backtester:
    """
    Minimal event loop:
      - iterate candles
      - call strategy(signal_fn) -> list[orders]
      - simulate fills via broker_sim
    """
    def __init__(self, data: List[Dict[str, Any]], signal_fn: Callable, broker):
        self.data = data
        self.signal_fn = signal_fn
        self.broker = broker
        self.trades: List[Trade] = []

    def run(self) -> List[Trade]:
        for bar in self.data:
            ts = bar["ts"]
            try:
                orders = self.signal_fn(bar) or []
                for o in orders:
                    fill = self.broker.place(o, bar)
                    if fill:
                        self.trades.append(Trade(ts, o["transactiontype"], int(o["quantity"]), float(fill["price"]), o.get("client_order_id","bt")))
            except Exception as e:
                logger.error(f"strategy error @ {ts}: {e}")
        return self.trades
