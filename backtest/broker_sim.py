# backtest/broker_sim.py
from __future__ import annotations
from typing import Dict, Any

class BrokerSim:
    """
    Super-simple fill model:
      - MARKET: fill at bar['close']
      - LIMIT:  fill if price crosses bar['low'..'high']
      - STOPLOSS_LIMIT: trigger if bar['high']>=trigger (for shorts) or bar['low']<=trigger (for longs)
    """
    def place(self, order: Dict[str, Any], bar: Dict[str, Any]) -> Dict[str, Any] | None:
        side = order["transactiontype"].upper()
        ot   = order.get("ordertype","MARKET").upper()
        px   = float(order.get("price", bar["close"]))
        trig = float(order.get("triggerprice", px))
        lo, hi, close = float(bar["low"]), float(bar["high"]), float(bar["close"])

        if ot == "MARKET":
            return {"price": close}
        if ot == "LIMIT":
            if (side == "BUY" and lo <= px) or (side == "SELL" and hi >= px):
                return {"price": px}
            return None
        if ot in {"STOPLOSS_LIMIT","SL","SL_LIMIT"}:
            # assume this is a BUY-to-cover for short exit
            if hi >= trig:
                return {"price": px}
            return None
        return None
