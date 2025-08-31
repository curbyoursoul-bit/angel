# backtest/strategies.py
from __future__ import annotations
from typing import Dict
import pandas as pd
import numpy as np


def _blank(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "enter_long": False,
            "exit_long": False,
            "enter_short": False,
            "exit_short": False,
        },
        index=df.index,
    )


def ema_crossover(df: pd.DataFrame, fast: int = 9, slow: int = 21) -> pd.DataFrame:
    s = _blank(df)
    fast_ema = df["close"].ewm(span=int(fast), adjust=False).mean()
    slow_ema = df["close"].ewm(span=int(slow), adjust=False).mean()
    cross_up = (fast_ema > slow_ema) & (fast_ema.shift(1) <= slow_ema.shift(1))
    cross_dn = (fast_ema < slow_ema) & (fast_ema.shift(1) >= slow_ema.shift(1))
    s.loc[cross_up, "enter_long"] = True
    s.loc[cross_dn, "exit_long"] = True
    s.loc[cross_dn, "enter_short"] = True
    s.loc[cross_up, "exit_short"] = True
    return s


def bollinger_breakout(df: pd.DataFrame, bb_n: int = 20, bb_k: float = 2.0) -> pd.DataFrame:
    s = _blank(df)
    m = df["close"].rolling(int(bb_n)).mean()
    sd = df["close"].rolling(int(bb_n)).std(ddof=0)
    upper = m + float(bb_k) * sd
    lower = m - float(bb_k) * sd

    cross_up = (df["close"] > upper) & (df["close"].shift(1) <= upper.shift(1))
    cross_dn = (df["close"] < lower) & (df["close"].shift(1) >= lower.shift(1))
    back_below_mid = (df["close"] < m) & (df["close"].shift(1) >= m.shift(1))
    back_above_mid = (df["close"] > m) & (df["close"].shift(1) <= m.shift(1))

    s.loc[cross_up, "enter_long"] = True
    s.loc[back_below_mid, "exit_long"] = True
    s.loc[cross_dn, "enter_short"] = True
    s.loc[back_above_mid, "exit_short"] = True
    return s


def vwap_mean_reversion(df: pd.DataFrame, vwap_n: int = 30, vwap_z: float = 1.5) -> pd.DataFrame:
    s = _blank(df)
    # rolling VWAP
    vol = df["volume"].clip(lower=0.0)
    pv = df["close"] * vol
    vwap = pv.rolling(int(vwap_n)).sum() / vol.rolling(int(vwap_n)).sum()
    spread = df["close"] - vwap
    z = (spread - spread.rolling(int(vwap_n)).mean()) / (spread.rolling(int(vwap_n)).std(ddof=0) + 1e-12)

    enter_long = z <= -abs(float(vwap_z))
    exit_long = z >= 0
    enter_short = z >= abs(float(vwap_z))
    exit_short = z <= 0

    s.loc[enter_long, "enter_long"] = True
    s.loc[exit_long, "exit_long"] = True
    s.loc[enter_short, "enter_short"] = True
    s.loc[exit_short, "exit_short"] = True
    return s


def orb_breakout(df: pd.DataFrame, orb_mins: int = 30) -> pd.DataFrame:
    """Opening range breakout (per calendar day)."""
    s = _blank(df)
    idx = df.index

    if idx.tz is not None:
        local = idx.tz_convert("Asia/Kolkata")
    else:
        local = idx.tz_localize("Asia/Kolkata")

    dates = pd.Series(local.date, index=idx)

    for d, ix in dates.groupby(dates).groups.items():
        day_idx = df.index[ix]
        if day_idx.empty:
            continue
        start = day_idx[0]
        end = start + pd.Timedelta(minutes=int(orb_mins))
        orb_slice = df.loc[(df.index >= start) & (df.index <= end)]
        if orb_slice.empty:
            continue
        rh = float(orb_slice["high"].max())
        rl = float(orb_slice["low"].min())

        after = df.loc[df.index > end].index
        if len(after) == 0:
            continue

        # entries
        brk_up = (df["high"] > rh) & (df["high"].shift(1) <= rh)
        brk_dn = (df["low"] < rl) & (df["low"].shift(1) >= rl)

        # only consider after ORB window
        brk_up = brk_up & (df.index > end)
        brk_dn = brk_dn & (df.index > end)

        s.loc[brk_up.index[brk_up], "enter_long"] = True
        s.loc[brk_dn.index[brk_dn], "enter_short"] = True

        # exits: opposite side break or day end
        # mark day end exit at last bar of the day
        last_bar = day_idx[-1]
        s.loc[last_bar, "exit_long"] = True
        s.loc[last_bar, "exit_short"] = True

        # if opposite breakout occurs, that exit will also be set by entries above
        opp_dn = (df["low"] < rl) & (df["low"].shift(1) >= rl)
        opp_up = (df["high"] > rh) & (df["high"].shift(1) <= rh)
        s.loc[opp_dn.index[opp_dn], "exit_long"] = True
        s.loc[opp_up.index[opp_up], "exit_short"] = True

    return s


def volume_breakout(df: pd.DataFrame, vol_n: int = 20, vol_k: float = 2.0) -> pd.DataFrame:
    s = _blank(df)
    vma = df["volume"].rolling(int(vol_n)).mean()
    spike = df["volume"] > (float(vol_k) * vma)

    mom_up = df["close"] > df["close"].shift(1)
    mom_dn = df["close"] < df["close"].shift(1)

    s.loc[spike & mom_up, "enter_long"] = True
    s.loc[spike & mom_dn, "enter_short"] = True

    # simple 1-bar exit (momentum exhaust)
    s.loc[s.index[1:], "exit_long"] |= s["enter_long"].shift(1).fillna(False)
    s.loc[s.index[1:], "exit_short"] |= s["enter_short"].shift(1).fillna(False)
    return s


BUILT_INS: Dict[str, callable] = {
    "ema_crossover": ema_crossover,
    "bollinger_breakout": bollinger_breakout,
    "vwap_mean_reversion": vwap_mean_reversion,
    "orb_breakout": orb_breakout,
    "volume_breakout": volume_breakout,
}
