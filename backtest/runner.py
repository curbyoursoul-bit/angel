# backtest/runner.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd

from .data import load_ohlcv_csv, resample


# =========================
# Utilities & Metrics
# =========================

@dataclass
class RunResult:
    equity: pd.Series
    trades: int
    metrics: Dict[str, float]


def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """Average True Range."""
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat(
        [
            (h - l).abs(),
            (h - prev_c).abs(),
            (l - prev_c).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()


def _apply_session_mask(index: pd.DatetimeIndex, session: Optional[str]) -> pd.Series:
    """
    Build a boolean mask for bars inside the provided session (HH:MM-HH:MM, 24h).
    If session is falsy/None, return a mask of True.
    """
    if not session:
        return pd.Series(True, index=index)

    try:
        start_s, end_s = session.split("-")
        sh, sm = [int(x) for x in start_s.split(":")]
        eh, em = [int(x) for x in end_s.split(":")]
    except Exception:
        # On a bad format, allow all rather than erroring
        return pd.Series(True, index=index)

    times = index.time
    tmins = np.array([t.hour * 60 + t.minute for t in times])
    start_m = sh * 60 + sm
    end_m = eh * 60 + em
    if end_m >= start_m:
        mask = (tmins >= start_m) & (tmins <= end_m)
    else:
        # session crossing midnight (rare for intraday eq), allow either side
        mask = (tmins >= start_m) | (tmins <= end_m)
    return pd.Series(mask, index=index)


def _max_drawdown(equity: pd.Series) -> float:
    if len(equity) == 0:
        return float("nan")
    roll_max = equity.cummax()
    dd = (equity - roll_max) / roll_max
    return float(dd.min()) if len(dd) else float("nan")


def _daily_sharpe(equity: pd.Series) -> float:
    """
    Annualized Sharpe using daily equity changes (resampled to 1D),
    assuming risk-free ~0 for short backtests.
    """
    if len(equity) < 3:
        return float("nan")
    daily = equity.resample("1D").last().dropna()
    rets = daily.pct_change().dropna()
    if len(rets) == 0:
        return float("nan")
    mu = rets.mean()
    sd = rets.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return float("nan")
    return float(np.sqrt(252.0) * mu / sd)


def _cagr(equity: pd.Series) -> float:
    if len(equity) < 2:
        return float("nan")
    start_v = float(equity.iloc[0])
    end_v = float(equity.iloc[-1])
    if start_v <= 0 or end_v <= 0:
        return float("nan")
    # Use actual elapsed days (not trading days)
    dt_days = (equity.index[-1] - equity.index[0]).days
    years = dt_days / 365.25 if dt_days > 0 else len(equity) / 365.25
    if years <= 0:
        return float("nan")
    return float((end_v / start_v) ** (1.0 / years) - 1.0)


def _default_capital(capital: Optional[float]) -> float:
    return 100_000.0 if (capital is None or capital <= 0) else float(capital)


# =========================
# Strategy Signal Generators
# =========================

def _sig_ema_crossover(df: pd.DataFrame, fast: int, slow: int) -> pd.Series:
    c = df["close"]
    ema_f = c.ewm(span=fast, adjust=False).mean()
    ema_s = c.ewm(span=slow, adjust=False).mean()
    raw = np.sign(ema_f - ema_s)  # -1, 0, +1
    # Convert to position-style signal (-1/0/1), carry forward last non-zero
    pos = raw.replace(0, np.nan).ffill().fillna(0.0)
    return pos


def _sig_bollinger_breakout(df: pd.DataFrame, n: int, k: float) -> pd.Series:
    c = df["close"]
    ma = c.rolling(n, min_periods=n).mean()
    sd = c.rolling(n, min_periods=n).std(ddof=0)
    upper = ma + k * sd
    lower = ma - k * sd
    up_break = (c > upper).astype(float)
    dn_break = (c < lower).astype(float) * -1.0
    sig = up_break + dn_break
    sig = sig.replace(0, np.nan).ffill().fillna(0.0)
    return sig


def _sig_vwap_mean_reversion(df: pd.DataFrame, n: int, z: float) -> pd.Series:
    """
    Simple VWAP rolling mean-reversion:
      zscore = (close - vwap) / rolling_std(close, n)
      Long when z < -z
      Short when z > +z
      Flat when |z| < 0.2 (hysteresis to avoid flip-flop)
    """
    pv = (df["close"] * df["volume"]).rolling(n, min_periods=n).sum()
    vol = df["volume"].rolling(n, min_periods=n).sum()
    vwap = pv / vol.replace(0, np.nan)
    roll_sd = df["close"].rolling(n, min_periods=n).std(ddof=0)
    zscore = (df["close"] - vwap) / roll_sd.replace(0, np.nan)

    long_sig = (zscore < -abs(z)).astype(float)
    short_sig = (zscore > abs(z)).astype(float) * -1.0
    raw = long_sig + short_sig

    # hysteresis: flatten when in a small band near zero
    flat = (zscore.abs() < 0.2).astype(float) * 0.0
    raw = raw.where(~flat.astype(bool), 0.0)
    pos = raw.replace(0, np.nan).ffill().fillna(0.0)
    return pos


def _sig_orb_breakout(df: pd.DataFrame, orb_mins: int) -> pd.Series:
    """
    Opening Range Breakout:
      - Compute the first 'orb_mins' minutes' high/low per day
      - Long if price crosses above ORB high; short if below ORB low
    """
    ts = df.index
    days = ts.normalize()
    # define opening bucket per day: bars whose minutes within first orb_mins
    mins_into_day = (ts - days).total_seconds() / 60.0
    is_orb = mins_into_day <= float(orb_mins)

    # ORB high/low per day (using the bars flagged as ORB window)
    orb_mask = pd.Series(is_orb, index=ts)
    orb_high = df["high"].where(orb_mask).groupby(days).transform("max")
    orb_low = df["low"].where(orb_mask).groupby(days).transform("min")

    c = df["close"]
    up = (c > orb_high).astype(float)    # breakout up -> +1
    dn = (c < orb_low).astype(float) * -1.0  # breakout down -> -1
    raw = up + dn
    raw = raw.replace(0, np.nan).ffill().fillna(0.0)
    return raw


def _sig_volume_breakout(df: pd.DataFrame, n: int, k: float) -> pd.Series:
    """
    Volume breakout + price momentum:
      - Volume spike: vol > mean(vol, n) + k*std(vol, n)
      - Price momentum: close > rolling max(close, n) for long,
                        close < rolling min(close, n) for short
    """
    vol = df["volume"]
    vma = vol.rolling(n, min_periods=n).mean()
    vsd = vol.rolling(n, min_periods=n).std(ddof=0)
    spike = vol > (vma + k * vsd)

    c = df["close"]
    mom_up = c > c.rolling(n, min_periods=n).max().shift(1)
    mom_dn = c < c.rolling(n, min_periods=n).min().shift(1)

    up = (spike & mom_up).astype(float)
    dn = (spike & mom_dn).astype(float) * -1.0
    raw = up + dn
    raw = raw.replace(0, np.nan).ffill().fillna(0.0)
    return raw


def _build_signal(strategy: str, df: pd.DataFrame, kwargs: Dict[str, Any]) -> pd.Series:
    if strategy == "ema_crossover":
        return _sig_ema_crossover(df, int(kwargs["fast"]), int(kwargs["slow"]))
    if strategy == "bollinger_breakout":
        return _sig_bollinger_breakout(df, int(kwargs["bb_n"]), float(kwargs["bb_k"]))
    if strategy == "vwap_mean_reversion":
        return _sig_vwap_mean_reversion(df, int(kwargs["vwap_n"]), float(kwargs["vwap_z"]))
    if strategy == "orb_breakout":
        return _sig_orb_breakout(df, int(kwargs["orb_mins"]))
    if strategy == "volume_breakout":
        return _sig_volume_breakout(df, int(kwargs["vol_n"]), float(kwargs["vol_k"]))
    raise ValueError(f"Unknown strategy: {strategy}")


# =========================
# Backtest Engine
# =========================

def run_backtest(
    csv_path: str,
    strategy: str,
    strategy_kwargs: Optional[Dict[str, Any]] = None,
    timeframe: Optional[str] = None,
    capital: Optional[float] = None,
    allocation_pct: float = 1.0,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
    allow_short: bool = False,
    fill: str = "next_open",              # "next_open" or "close"
    fixed_qty: Optional[float] = 1.0,
    atr_n: int = 14,
    atr_sl_mult: float = 0.0,
    atr_tp_mult: float = 0.0,
    session: Optional[str] = None,        # "HH:MM-HH:MM"
    cooldown_bars: int = 0,
) -> Dict[str, Any] | RunResult:
    """
    Simple vector/semi-event backtester that supports:
      - five strategies (EMA crossover, Bollinger breakout, VWAP MR, ORB, Volume breakout)
      - optional ATR-based SL/TP exits
      - fees + slippage (bps)
      - fill on next open or on close
      - allow_short toggle
      - intraday session window for entries
      - cooldown bars after exits
    Returns a dict with equity Series, trades count, and metrics.
    """

    # -----------------
    # Load & prepare data
    # -----------------
    df = load_ohlcv_csv(csv_path)
    if timeframe:
        df = resample(df, timeframe)

    # Ensure tz-naive index for speed
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_convert(None)

    # Safety: drop any NaNs created by resampling/rolling
    df = df.dropna(subset=["open", "high", "low", "close"])

    # -----------------
    # Build signal & helpers
    # -----------------
    skw = strategy_kwargs or {}
    sig = _build_signal(strategy, df, skw).reindex(df.index).fillna(0.0)

    # Session gating (entries only)
    in_sess = _apply_session_mask(df.index, session)

    # ATR for SL/TP
    atr = _atr(df, n=atr_n) if (atr_sl_mult or atr_tp_mult) else pd.Series(index=df.index, dtype="float64")

    # -----------------
    # Backtest loop
    # -----------------
    cap0 = _default_capital(capital)
    fee_slip = (float(fee_bps) + float(slippage_bps)) / 10_000.0

    eq = []
    timestamps = []
    trades = 0

    pos = 0.0  # +qty for long, -qty for short, 0 flat
    entry_px = None
    cooldown_left = 0

    # Position sizing helper
    def _qty(bar_px: float) -> float:
        if fixed_qty is not None:
            return float(fixed_qty)
        notional = float(allocation_pct) * cap0
        q = notional / max(bar_px, 1e-9)
        return float(np.floor(q))  # round down to whole units

    # Price after fees/slippage given a side (+1 buy, -1 sell)
    def _price_after_costs(px: float, side: int) -> float:
        # Buy => pay higher, Sell => receive lower
        return float(px * (1.0 + fee_slip * side))

    # Evaluate intrabar SL/TP (if enabled) and return exit price or None
    def _check_intrabar_exit(i: int, is_long: bool, ep: float) -> Optional[float]:
        if atr.empty:
            return None
        a = atr.iloc[i]
        if np.isnan(a):
            return None

        hi = df["high"].iloc[i]
        lo = df["low"].iloc[i]

        if is_long:
            stop = ep - atr_sl_mult * a if atr_sl_mult > 0 else None
            tp = ep + atr_tp_mult * a if atr_tp_mult > 0 else None

            hit_stop = (stop is not None) and (lo <= stop)
            hit_tp = (tp is not None) and (hi >= tp)

            if hit_stop and hit_tp:
                # conservative: assume stop hit first
                return float(stop)
            if hit_stop:
                return float(stop)
            if hit_tp:
                return float(tp)
            return None
        else:
            stop = ep + atr_sl_mult * a if atr_sl_mult > 0 else None
            tp = ep - atr_tp_mult * a if atr_tp_mult > 0 else None

            hit_stop = (stop is not None) and (hi >= stop)
            hit_tp = (tp is not None) and (lo <= tp)

            if hit_stop and hit_tp:
                # conservative: assume stop hit first
                return float(stop)
            if hit_stop:
                return float(stop)
            if hit_tp:
                return float(tp)
            return None

    closes = df["close"].values
    opens = df["open"].values

    for i, ts in enumerate(df.index):
        px_ref = closes[i] if fill == "close" else opens[i]  # used for equity marking when flat

        # Maintain cooldown timer
        if cooldown_left > 0:
            cooldown_left -= 1

        # If we have a position, check ATR exits first (intrabar)
        exited_now = False
        if pos != 0 and (atr_sl_mult > 0 or atr_tp_mult > 0):
            # intrabar check uses same-bar extremes
            is_long = pos > 0
            intrabar_exit_px = _check_intrabar_exit(i, is_long, entry_px if entry_px is not None else px_ref)
            if intrabar_exit_px is not None:
                side = -1 if is_long else +1  # closing side
                fill_px = _price_after_costs(intrabar_exit_px, side)
                # realize PnL
                pnl = (fill_px - entry_px) * pos if is_long else (entry_px - fill_px) * (-pos)
                cap0 += pnl
                pos = 0.0
                entry_px = None
                trades += 1
                cooldown_left = max(cooldown_left, cooldown_bars)
                exited_now = True
                eq.append(cap0)
                timestamps.append(ts)
                continue  # skip signal evaluation this bar after intrabar exit

        # Evaluate entry/exit signals (at bar close) — fill per 'fill' setting
        target_pos = 0.0
        s = sig.iloc[i]

        if s > 0:
            target_pos = _qty(closes[i])
        elif s < 0 and allow_short:
            target_pos = -_qty(closes[i])
        else:
            target_pos = 0.0

        # Gate entries by session
        if not in_sess.iloc[i]:
            # allow exits but block new entries
            if pos == 0:
                target_pos = 0.0
            else:
                # keep current position until an exit signal (do nothing here)
                target_pos = pos

        # Enforce cooldown: no NEW entries while cooling
        if cooldown_left > 0 and pos == 0:
            target_pos = 0.0

        # Rebalance only on a *change* in desired position
        if target_pos != pos:
            # Exit if currently in a position
            if pos != 0:
                # Exit fill
                if fill == "close":
                    raw_px = closes[i]
                else:
                    # next open if possible; if we're on last bar, use current open
                    raw_px = opens[i] if i + 1 >= len(df) else opens[i + 1]
                side = -1 if pos > 0 else +1  # closing side
                fill_px = _price_after_costs(float(raw_px), side)

                # realize PnL
                is_long = pos > 0
                pnl = (fill_px - entry_px) * pos if is_long else (entry_px - fill_px) * (-pos)
                cap0 += pnl
                pos = 0.0
                entry_px = None
                trades += 1
                cooldown_left = max(cooldown_left, cooldown_bars)

            # Enter new position if target_pos != 0
            if target_pos != 0.0:
                if fill == "close":
                    raw_px = closes[i]
                else:
                    raw_px = opens[i] if i + 1 >= len(df) else opens[i + 1]
                side = +1 if target_pos > 0 else -1
                fill_px = _price_after_costs(float(raw_px), side)
                pos = float(target_pos)
                entry_px = float(fill_px)

        # Mark-to-market equity at close price
        mtm_px = closes[i]
        if pos != 0 and entry_px is not None:
            if pos > 0:
                unreal = (mtm_px - entry_px) * pos
            else:
                unreal = (entry_px - mtm_px) * (-pos)
            eq_val = cap0 + unreal
        else:
            eq_val = cap0

        eq.append(eq_val)
        timestamps.append(ts)

    equity = pd.Series(eq, index=pd.DatetimeIndex(timestamps, name="timestamp"))

    # -----------------
    # Metrics
    # -----------------
    total_return = float(equity.iloc[-1] / equity.iloc[0] - 1.0) if len(equity) > 1 else float("nan")
    metrics = dict(
        total_return=total_return,
        cagr=_cagr(equity),
        max_drawdown=_max_drawdown(equity),
        sharpe=_daily_sharpe(equity),
        trades=int(trades),
    )

    # Return as dict to match newer callers, but include compatibility dataclass
    return {
        "equity": equity,
        "trades": trades,
        "metrics": metrics,
    }
