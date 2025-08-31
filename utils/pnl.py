# utils/pnl.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import csv
import datetime

from utils.market_hours import IST  # same tz you already use

@dataclass
class Trade:
    ts: datetime.datetime   # timezone-aware IST
    mode: str               # "DRY" / "LIVE"
    symbol: str
    side: str               # BUY / SELL
    ordertype: str
    qty: int
    price: float
    orderid: str
    note: str
    ordertag: str

def _parse_row(row: List[str]) -> Optional[Trade]:
    try:
        ts_str, mode, symbol, side, ordertype, qty, price, trig, orderid, note, ordertag = row
        # ts like "YYYY-MM-DD HH:MM:SS" in IST (your order_exec.py)
        naive = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        ts = IST.localize(naive)
        q = int(str(qty or "0").strip() or 0)
        px = float(str(price or "0").strip() or 0.0)
        side = side.strip().upper()
        if side not in ("BUY", "SELL"):
            return None
        return Trade(ts, mode.strip().upper(), symbol.strip(), side, ordertype.strip().upper(),
                     q, px, (orderid or "").strip(), (note or "").strip(), (ordertag or "").strip())
    except Exception:
        return None

def load_trades(csv_path: Path, day: Optional[datetime.date] = None) -> List[Trade]:
    trades: List[Trade] = []
    if not csv_path.exists():
        return trades
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r, None)  # skip header
        for row in r:
            t = _parse_row(row)
            if not t:
                continue
            if day:
                if t.ts.astimezone(IST).date() != day:
                    continue
            trades.append(t)
    return trades

def realized_fifo_pnl(trades: List[Trade]) -> Tuple[float, Dict[str, float], Dict[str, float]]:
    """
    Returns (total_realized, by_symbol, by_tag).
    Uses FIFO; ignores unrealized PnL (open positions at end of day).
    Only trades with price>0 and qty>0 are considered.
    """
    # group per symbol
    from collections import defaultdict, deque
    by_sym: Dict[str, float] = defaultdict(float)
    by_tag: Dict[str, float] = defaultdict(float)

    # inventory per symbol: queue of (qty_remaining, price)
    inv: Dict[str, deque] = {}

    # sort by time to be safe
    trades_sorted = sorted(
        [t for t in trades if t.qty > 0 and t.price > 0.0],
        key=lambda x: x.ts
    )

    for t in trades_sorted:
        q = t.qty
        px = t.price
        sym = t.symbol
        tag = t.ordertag or "UNSPECIFIED"

        if sym not in inv:
            from collections import deque
            inv[sym] = deque()

        if t.side == "BUY":
            # Add inventory
            inv[sym].append([q, px])
        else:
            # SELL closes existing inventory (shorting handled by negative inventory via BUY on exit)
            qty_to_match = q
            realized_here = 0.0
            # If no inventory, treat as short open: negative inventory bucket
            if not inv[sym]:
                inv[sym].append([-qty_to_match, px])  # track short at sell price
                qty_to_match = 0
            while qty_to_match > 0 and inv[sym]:
                lot_qty, lot_px = inv[sym][0]
                if lot_qty <= 0:
                    # encountering short inventory while selling more: extend short
                    inv[sym][0][0] -= qty_to_match
                    qty_to_match = 0
                    break
                match = min(qty_to_match, lot_qty)
                realized_here += (px - lot_px) * match
                lot_qty -= match
                qty_to_match -= match
                if lot_qty == 0:
                    inv[sym].popleft()
                else:
                    inv[sym][0][0] = lot_qty
            # if still qty_to_match > 0, extend short for remaining
            if qty_to_match > 0:
                inv[sym].append([-qty_to_match, px])
            by_sym[sym] += realized_here
            by_tag[tag] += realized_here

        # If side == BUY and we had short inventory (negative), BUY closes shorts
        if t.side == "BUY":
            qty_to_match = q
            realized_here = 0.0
            # first, close shorts (negative lots) from front
            while qty_to_match > 0 and inv[sym]:
                lot_qty, lot_px = inv[sym][0]
                if lot_qty >= 0:
                    break
                # lot_qty negative → short opened at lot_px; closing BUY realizes (lot_px - px)
                match = min(qty_to_match, -lot_qty)
                realized_here += (lot_px - px) * match
                lot_qty += match  # toward zero
                qty_to_match -= match
                if lot_qty == 0:
                    inv[sym].popleft()
                else:
                    inv[sym][0][0] = lot_qty
            # any remaining qty_to_match was already added to long inventory above
            if realized_here != 0.0:
                by_sym[sym] += realized_here
                by_tag[tag] += realized_here

    total = float(sum(by_sym.values()))
    return total, dict(by_sym), dict(by_tag)
