# backtest/broker.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Literal

import pandas as pd


Side = Literal["buy", "sell", "short", "cover"]


@dataclass
class BrokerConfig:
    capital: float = 100_000.0
    allocation_pct: float = 1.0
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    allow_short: bool = True
    fill: Literal["next_open", "close"] = "next_open"
    fixed_qty: Optional[int] = None
    atr_sl_mult: float = 0.0
    atr_tp_mult: float = 0.0


@dataclass
class BrokerState:
    cash: float
    equity: float
    position: int = 0                # >0 long units, <0 short units
    avg_price: float = 0.0
    trades: List[Dict] = field(default_factory=list)

    # risk refs (set when position flips from 0 to non-zero)
    entry_price: Optional[float] = None
    stop: Optional[float] = None
    take: Optional[float] = None


class Broker:
    def __init__(self, cfg: BrokerConfig):
        self.cfg = cfg
        self.state = BrokerState(
            cash=float(cfg.capital),
            equity=float(cfg.capital),
        )
        # pending orders for next_open fills
        self._pending: List[Dict] = []

    # --- helpers -------------------------------------------------------------

    def _apply_slip(self, price: float, side: Side) -> float:
        bps = float(self.cfg.slippage_bps or 0.0) / 10_000.0
        if bps <= 0:
            return float(price)
        side = side.lower()
        if side in ("buy", "cover"):   # pay up
            return float(price) * (1.0 + bps)
        else:                          # sell/short worse fill
            return float(price) * (1.0 - bps)

    def _fee(self, notional: float) -> float:
        bps = float(self.cfg.fee_bps or 0.0) / 10_000.0
        return abs(float(notional)) * bps if bps > 0 else 0.0

    def _mkt_qty(self, px: float) -> int:
        if self.cfg.fixed_qty and self.cfg.fixed_qty > 0:
            return int(self.cfg.fixed_qty)
        alloc_cash = self.state.equity * float(self.cfg.allocation_pct or 0.0)
        qty = int(max(1, alloc_cash // max(1e-12, float(px))))
        return qty

    def _record(self, ts, side: Side, qty: int, px: float, fee: float, why: str):
        self.state.trades.append({
            "ts": ts,
            "side": side,
            "qty": int(qty),
            "price": float(px),
            "notional": float(qty) * float(px) * (1 if side in ("buy", "cover") else -1),
            "fee": float(fee),
            "reason": why,
        })

    def _enter_refs(self, entry_px: float, atr: Optional[float], long: bool):
        self.state.entry_price = float(entry_px)
        if atr is None:
            self.state.stop = None
            self.state.take = None
            return
        sl_k = float(self.cfg.atr_sl_mult or 0.0)
        tp_k = float(self.cfg.atr_tp_mult or 0.0)
        if long:
            self.state.stop = entry_px - sl_k * atr if sl_k > 0 else None
            self.state.take = entry_px + tp_k * atr if tp_k > 0 else None
        else:
            self.state.stop = entry_px + sl_k * atr if sl_k > 0 else None
            self.state.take = entry_px - tp_k * atr if tp_k > 0 else None

    # --- order execution -----------------------------------------------------

    def _exec(self, ts, side: Side, qty: int, px_raw: float, why: str):
        px = self._apply_slip(px_raw, side)
        fee = self._fee(qty * px)

        if side in ("buy", "cover"):
            self.state.cash -= qty * px
            self.state.cash -= fee
            self._record(ts, side, qty, px, fee, why)
            if side == "buy":
                # increase / flip to long
                new_pos = self.state.position + qty
                if self.state.position >= 0:
                    # average up
                    tot_cost = self.state.avg_price * self.state.position + qty * px
                    self.state.position = new_pos
                    self.state.avg_price = tot_cost / max(1, self.state.position)
                else:
                    # covering short partially/fully
                    self.state.position = new_pos
                    if self.state.position > 0:  # flipped
                        self.state.avg_price = px
                if self.state.position > 0 and self.state.entry_price is None:
                    self._enter_refs(self.state.avg_price, None, long=True)
            else:  # cover short
                self.state.position += qty
                if self.state.position == 0:
                    # reset refs after fully flat
                    self.state.avg_price = 0.0
                    self.state.entry_price = None
                    self.state.stop = None
                    self.state.take = None

        elif side in ("sell", "short"):
            self.state.cash += qty * px
            self.state.cash -= fee
            self._record(ts, side, qty, px, fee, why)
            if side == "sell":
                # decrease / close long
                self.state.position -= qty
                if self.state.position == 0:
                    self.state.avg_price = 0.0
                    self.state.entry_price = None
                    self.state.stop = None
                    self.state.take = None
            else:  # enter short (increase negative)
                new_pos = self.state.position - qty
                if self.state.position <= 0:
                    # average short
                    short_cost = abs(self.state.avg_price) * abs(self.state.position) + qty * px
                    self.state.position = new_pos
                    self.state.avg_price = short_cost / max(1, abs(self.state.position))
                else:
                    # exiting long before shorting
                    self.state.position = new_pos
                    if self.state.position < 0:
                        self.state.avg_price = px
                if self.state.position < 0 and self.state.entry_price is None:
                    self._enter_refs(self.state.avg_price, None, long=False)

    # --- public API ----------------------------------------------------------

    def step(self, ts, bar: Dict, sig: Dict[str, bool], atr: Optional[float] = None):
        """
        Process one bar.
        bar keys: open, high, low, close
        sig keys: enter_long, exit_long, enter_short, exit_short
        """
        o, h, l, c = [float(bar[k]) for k in ("open", "high", "low", "close")]

        # 1) execute any pending (for next_open fills)
        if self._pending:
            for order in self._pending:
                self._exec(ts, order["side"], order["qty"], o, why=order["why"])
            self._pending.clear()

        # 2) ATR-based risk exits (intrabar stop/take at boundary)
        if self.state.position != 0:
            # Ensure refs exist on first entry of a position
            if self.state.entry_price is None:
                self._enter_refs(self.state.avg_price if self.state.avg_price else c, atr, self.state.position > 0)
            # Evaluate hits
            if self.state.position > 0:
                # stop then take; prioritize stop if both touched
                if self.state.stop is not None and l <= self.state.stop <= h:
                    qty = abs(self.state.position)
                    self._exec(ts, "sell", qty, self.state.stop, why="atr_stop")
                elif self.state.take is not None and l <= self.state.take <= h:
                    qty = abs(self.state.position)
                    self._exec(ts, "sell", qty, self.state.take, why="atr_take")
            else:  # short
                if self.state.stop is not None and l <= self.state.stop <= h:
                    qty = abs(self.state.position)
                    self._exec(ts, "cover", qty, self.state.stop, why="atr_stop")
                elif self.state.take is not None and l <= self.state.take <= h:
                    qty = abs(self.state.position)
                    self._exec(ts, "cover", qty, self.state.take, why="atr_take")

        # 3) translate signals to orders
        enter_long = bool(sig.get("enter_long", False))
        exit_long = bool(sig.get("exit_long", False))
        enter_short = bool(sig.get("enter_short", False)) and bool(self.cfg.allow_short)
        exit_short = bool(sig.get("exit_short", False))

        def place(side: Side, qty: int, why: str):
            if qty <= 0:
                return
            if self.cfg.fill == "close":
                px = c
                self._exec(ts, side, qty, px, why=why)
            else:  # next_open
                self._pending.append({"side": side, "qty": qty, "why": why})

        # exits first
        if self.state.position > 0 and exit_long:
            place("sell", abs(self.state.position), "exit_long_signal")
        if self.state.position < 0 and exit_short:
            place("cover", abs(self.state.position), "exit_short_signal")

        # entries / flips
        if enter_long and self.state.position <= 0:
            # cover short if any, then buy
            if self.state.position < 0:
                place("cover", abs(self.state.position), "flip_to_long")
            qty = self._mkt_qty(c)
            place("buy", qty, "enter_long_signal")
            # set refs at the bar we expect to fill (using current ATR snapshot)
            if self.cfg.fill == "close":
                self._enter_refs(c, atr, long=True)
            else:
                self._enter_refs(o, atr, long=True)

        if enter_short and self.state.position >= 0:
            if self.state.position > 0:
                place("sell", abs(self.state.position), "flip_to_short")
            qty = self._mkt_qty(c)
            place("short", qty, "enter_short_signal")
            if self.cfg.fill == "close":
                self._enter_refs(c, atr, long=False)
            else:
                self._enter_refs(o, atr, long=False)

        # 4) mark to market
        self.state.equity = self.state.cash + self.state.position * c
