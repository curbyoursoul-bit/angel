# risk/max_loss_guard.py
from __future__ import annotations
from config import _f
MAX_DAILY_LOSS = _f("MAX_DAILY_LOSS", 0.0)
def breach(cum_pnl: float) -> bool:
    return MAX_DAILY_LOSS>0 and cum_pnl <= -abs(MAX_DAILY_LOSS)
