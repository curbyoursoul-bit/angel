# utils/pnl_guard.py
from __future__ import annotations
import csv
from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Deque, Dict, List, Tuple

from loguru import logger

from config import TRADE_LOG_CSV
from utils.market_hours import IST  # <- use existing IST

# --- internals ---------------------------------------------------------------

def _today_ist_date_str() -> str:
    try:
        now = datetime.now(IST)
    except Exception:
        now = datetime.now()
    return now.strftime("%Y-%m-%d")

def _load_today_live_trades() -> List[dict]:
    path = Path(TRADE_LOG_CSV)
    if not path.exists():
        return []
    out: List[dict] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if not row:
                continue
            mode = (row.get("mode") or row.get("Mode") or "").upper()
            ts   = (row.get("ts") or row.get("timestamp") or row.get("time") or "")
            if mode != "LIVE":
                continue
            if not ts.startswith(_today_ist_date_str()):
                continue
            out.append(row)
    return out

# FIFO lot = (qty_remaining, price)
Lot = Tuple[int, float]

def _fifo_realized_for_symbol(trades: List[dict]) -> float:
    """
    Compute realized P&L for a single symbol using strict FIFO, supporting both long and short inventories.
    Each trade row must have fields: side, qty, price. (We tolerate casing/aliases.)
    """
    long_lots: Deque[Lot]  = deque()  # positive inventory
    short_lots: Deque[Lot] = deque()  # negative inventory
    realized = 0.0

    for row in trades:
        side = (row.get("side") or row.get("transactiontype") or "").upper()
        try:
            qty = int(float(row.get("qty") or row.get("quantity") or 0))
            px  = float(row.get("price") or 0)
        except Exception:
            continue
        if qty <= 0 or px <= 0:
            continue

        if side == "BUY":
            # First, cover shorts (realize P&L vs short entry prices)
            remaining = qty
            while remaining > 0 and short_lots:
                sqty, spx = short_lots[0]
                cover = min(remaining, sqty)
                # Short P&L: entry sell (spx) - buy cover (px)
                realized += (spx - px) * cover
                sqty -= cover
                remaining -= cover
                if sqty == 0:
                    short_lots.popleft()
                else:
                    short_lots[0] = (sqty, spx)
            # Any remainder increases long inventory
            if remaining > 0:
                long_lots.append((remaining, px))

        elif side == "SELL":
            # First, sell from long inventory (realize P&L vs long entry prices)
            remaining = qty
            while remaining > 0 and long_lots:
                lqty, lpx = long_lots[0]
                cover = min(remaining, lqty)
                # Long P&L: sell (px) - entry buy (lpx)
                realized += (px - lpx) * cover
                lqty -= cover
                remaining -= cover
                if lqty == 0:
                    long_lots.popleft()
                else:
                    long_lots[0] = (lqty, lpx)
            # Any remainder increases short inventory
            if remaining > 0:
                short_lots.append((remaining, px))

        else:
            # unknown side; skip
            continue

    return round(realized, 2)

# --- public API --------------------------------------------------------------

def estimate_realized_pnl_today() -> float:
    """
    Approximate realized P&L from today's LIVE trades in TRADE_LOG_CSV using strict FIFO per symbol.
    Broker slippage vs logged price is possible; this reflects your log, not broker statements.
    """
    rows = _load_today_live_trades()
    if not rows:
        return 0.0

    # group by symbol (tolerate different header names)
    by_sym: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        sym = row.get("symbol") or row.get("tradingsymbol") or row.get("Symbol") or "UNKNOWN"
        by_sym[sym].append(row)

    total = 0.0
    for sym, trs in by_sym.items():
        pnl = _fifo_realized_for_symbol(trs)
        total += pnl
    return round(total, 2)

def sum_live_quantities_today() -> int:
    """
    Sum quantities of today's LIVE trades marked as primary (if 'note' is present).
    Falls back to summing all LIVE trade qty if the note column is missing.
    """
    trades = _load_today_live_trades()
    s = 0
    for t in trades:
        note = (t.get("note") or "").lower()
        try:
            q = int(float(t.get("qty") or 0))
        except Exception:
            q = 0
        if note:
            if note.startswith("primary"):
                s += q
        else:
            # no note column — include all LIVE trades
            s += q
    return s
