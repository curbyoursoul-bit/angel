# execution/portfolio.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from loguru import logger

@dataclass
class Position:
    exchange: str
    tradingsymbol: str
    symboltoken: str
    qty: int
    avg_price: float
    producttype: str = "INTRADAY"

@dataclass
class Portfolio:
    positions: Dict[str, Position] = field(default_factory=dict)  # key = symboltoken

    def _key(self, exch: str, token: str) -> str:
        return f"{exch}:{token}"

    def upsert_fill(self, exch: str, tsym: str, token: str, side: str, qty: int, price: float, product: str="INTRADAY") -> None:
        k = self._key(exch, token)
        p = self.positions.get(k)
        signed = qty if side.upper()=="BUY" else -qty
        if p is None:
            self.positions[k] = Position(exch, tsym, token, signed, float(price), product)
            return
        # moving average price on net add; if crosses through zero, reset AVG to new leg
        new_qty = p.qty + signed
        if p.qty == 0 or (p.qty>0 and signed>0) or (p.qty<0 and signed<0):
            p.avg_price = (abs(p.qty)*p.avg_price + abs(signed)*price) / max(1, abs(p.qty)+abs(signed))
        elif (p.qty>0 and signed<0) or (p.qty<0 and signed>0):
            # reducing; if flip sign, start new leg average at current price
            if (p.qty>0 and new_qty<0) or (p.qty<0 and new_qty>0):
                p.avg_price = float(price)
        p.qty = new_qty
        p.producttype = product

    def net_exposure(self) -> float:
        # simple placeholder; you can wire LTP to compute marked-to-market
        return sum(p.qty * p.avg_price for p in self.positions.values())

    def as_list(self) -> List[Dict[str, Any]]:
        out = []
        for p in self.positions.values():
            out.append(dict(exchange=p.exchange, tradingsymbol=p.tradingsymbol,
                            symboltoken=p.symboltoken, qty=p.qty, avg_price=p.avg_price,
                            producttype=p.producttype))
        return out
