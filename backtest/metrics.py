# backtest/metrics.py
from __future__ import annotations
import numpy as np
import pandas as pd


def _annual_factor(index: pd.DatetimeIndex) -> float:
    """Best-effort annualization factor based on inferred frequency."""
    if len(index) < 2:
        return 252.0
    # try infer freq
    freq = pd.infer_freq(index)
    if freq:
        f = freq.lower()
        if "t" in f or "min" in f:   # minutes
            # assume ~6.25 hours of trading per day -> 25 bars @ 15m; 75 @ 5m
            # Use exact mapping from rule length if possible
            try:
                n = int("".join(ch for ch in f if ch.isdigit()))
                bars_per_day = max(1, int(round(375 / n)))  # 375 minutes in NSE session
                return 252.0 * bars_per_day
            except Exception:
                return 252.0 * 25.0
        if "h" in f:
            try:
                n = int("".join(ch for ch in f if ch.isdigit()))
                bars_per_day = max(1, int(round(6.25 / n)))  # ~6.25 hours
                return 252.0 * bars_per_day
            except Exception:
                return 252.0 * 6.0
        if "d" in f:
            return 252.0
        if "w" in f:
            return 52.0
        if "m" in f:
            return 12.0

    # fallback: annualize by time span
    days = (index[-1] - index[0]).days
    periods = max(1, len(index))
    per_day = periods / max(1.0, days)
    return 252.0 * per_day


def equity_metrics(eq: pd.Series) -> dict:
    """Compute total_return, cagr, max_drawdown, sharpe from equity curve."""
    if not isinstance(eq, pd.Series) or eq.empty:
        return {"total_return": 0.0, "cagr": 0.0, "max_drawdown": 0.0, "sharpe": 0.0}

    eq = eq.astype(float).dropna()
    if eq.empty:
        return {"total_return": 0.0, "cagr": 0.0, "max_drawdown": 0.0, "sharpe": 0.0}

    start, end = float(eq.iloc[0]), float(eq.iloc[-1])
    total_return = (end / start - 1.0) if start != 0 else 0.0

    # CAGR based on elapsed years
    days = max(1.0, (eq.index[-1] - eq.index[0]).days / 365.25)
    cagr = (1.0 + total_return) ** (1.0 / days) - 1.0 if days > 0 else 0.0

    # Drawdown
    roll_max = eq.cummax()
    dd = eq / roll_max - 1.0
    max_dd = float(dd.min()) if len(dd) else 0.0

    # Sharpe
    rets = eq.pct_change().dropna()
    ann = _annual_factor(eq.index)
    mu = float(rets.mean())
    sig = float(rets.std(ddof=1))
    sharpe = (mu / sig * np.sqrt(ann)) if sig > 1e-12 else 0.0

    return {
        "total_return": float(total_return),
        "cagr": float(cagr),
        "max_drawdown": float(max_dd),
        "sharpe": float(sharpe),
    }
