# risk/risk_manager.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional
import json
from pathlib import Path
from datetime import datetime
from config import _i, _f
from loguru import logger

STATE_FILE = Path("data/risk_state.json")
MAX_DAILY_LOSS   = _f("MAX_DAILY_LOSS", 0.0)
MAX_ORDERS       = _i("MAX_ORDERS", 0)
MAX_QTY          = _i("MAX_QTY", 0)
MAX_DRAWDOWN_PCT = _f("MAX_DRAWDOWN_PCT", 0.0)

@dataclass
class RiskState:
    date: str; cum_pnl: float; peak_equity: float; trough_equity: float; orders_placed: int

class RiskManager:
    def __init__(self): self.state = self._load()
    def _today(self) -> str: return datetime.now().strftime("%Y-%m-%d")
    def _load(self) -> RiskState:
        try:
            if STATE_FILE.exists():
                d = json.loads(STATE_FILE.read_text())
                if d.get("date")==self._today(): return RiskState(**d)
        except Exception: logger.warning("risk state load failed")
        return RiskState(self._today(),0.0,0.0,0.0,0)
    def _save(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self.state.__dict__, indent=2))
    def record_pnl(self, realized_pnl: float, equity: Optional[float]=None)->None:
        self.state.cum_pnl += float(realized_pnl)
        if equity is not None:
            self.state.peak_equity = max(self.state.peak_equity, equity)
            self.state.trough_equity = min(self.state.trough_equity or equity, equity)
        self._save()
    def record_order(self)->None:
        self.state.orders_placed += 1; self._save()
    def _dd_breached(self)->bool:
        if MAX_DRAWDOWN_PCT<=0 or self.state.peak_equity<=0 or self.state.trough_equity<=0: return False
        dd = (self.state.peak_equity - self.state.trough_equity)/self.state.peak_equity*100
        return dd >= MAX_DRAWDOWN_PCT
    def allow_order(self, order: Dict[str,Any])->tuple[bool,str]:
        if MAX_DAILY_LOSS>0 and self.state.cum_pnl <= -abs(MAX_DAILY_LOSS):
            return False, f"daily loss hit: {self.state.cum_pnl:.2f}"
        if MAX_ORDERS>0 and self.state.orders_placed >= MAX_ORDERS:
            return False, "max orders cap reached"
        try:
            q=int(float(order.get("quantity",0)))
            if MAX_QTY>0 and q>MAX_QTY: return False, f"qty {q} > MAX_QTY {MAX_QTY}"
        except Exception: pass
        if self._dd_breached(): return False, f"drawdown >= {MAX_DRAWDOWN_PCT}%"
        return True,"OK"

RISK = RiskManager()
