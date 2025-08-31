from __future__ import annotations
from datetime import date, timedelta
from typing import Tuple, Dict
import pandas as pd

from utils.instruments import _read_instruments_df

# ----------------------------------------------------------
# Expiry helpers
# ----------------------------------------------------------

def _nearest_weekday(base: date, weekday: int) -> date:
    """Return the next occurrence of weekday (0=Mon … 6=Sun)."""
    days_ahead = (weekday - base.weekday()) % 7
    if days_ahead == 0:  # if today is expiry, take today
        return base
    return base + timedelta(days=days_ahead)

def nearest_weekly_expiry(symbol: str, today: date | None = None) -> date:
    """
    Pick the correct weekly expiry date for index:
      - BANKNIFTY: Thursday
      - NIFTY: Thursday
      - FINNIFTY: Tuesday
      - SENSEX: Friday
    """
    today = today or date.today()
    sym = symbol.upper()
    if "BANKNIFTY" in sym or "NIFTY" in sym:
        return _nearest_weekday(today, 3)   # Thursday
    if "FINNIFTY" in sym:
        return _nearest_weekday(today, 1)   # Tuesday
    if "SENSEX" in sym:
        return _nearest_weekday(today, 4)   # Friday
    # fallback: Thursday
    return _nearest_weekday(today, 3)

# ----------------------------------------------------------
# ATM strike + tokens
# ----------------------------------------------------------

def round_strike(symbol: str, spot: float) -> int:
    """Round spot to nearest valid strike step based on symbol convention."""
    step = 50 if "NIFTY" in symbol.upper() else 100
    return int(round(spot / step) * step)

def pick_atm_tokens(symbol: str, spot: float) -> Tuple[Dict, Dict, int]:
    """
    Given symbol (NIFTY/BANKNIFTY/FINNIFTY/SENSEX) and spot,
    return (call_row, put_row, lotsize).
    """
    df = _read_instruments_df()
    expiry = nearest_weekly_expiry(symbol)
    strike = round_strike(symbol, spot)

    # Filter option chain
    opt_df = df[
        (df["name"].str.upper() == symbol.upper())
        & (df["expiry_dt"] == pd.to_datetime(expiry))
        & (df["strike_rupees"] == strike)
    ]

    if opt_df.empty:
        raise RuntimeError(f"No option rows found for {symbol} {expiry} strike={strike}")

    ce = opt_df[opt_df["symbol"].str.endswith("CE")].iloc[0].to_dict()
    pe = opt_df[opt_df["symbol"].str.endswith("PE")].iloc[0].to_dict()
    lotsize = int(opt_df["lotsize"].iloc[0])

    return ce, pe, lotsize
