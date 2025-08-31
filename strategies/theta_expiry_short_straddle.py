# strategies/theta_expiry_short_straddle.py
from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

# Use only your instruments utils (as requested)
from utils.instruments import (
    load_options,
    nearest_expiry,
    extract_symbol_fields,
    get_option_rows,
    get_lotsize,
)

IST = timezone(timedelta(hours=5, minutes=30))

# -------------------------------------------------------------------
# SmartAPI LTP helper (compatible with both dict and positional SDKs)
# -------------------------------------------------------------------
def _smartapi_ltp(smart, exchange: str, tradingsymbol: str, symboltoken: str) -> Optional[float]:
    """
    Works with:
      - smart.ltpData({"exchange": ..., "tradingsymbol": ..., "symboltoken": ...})
      - smart.ltpData(exchange, tradingsymbol, symboltoken)
    Returns float(ltp) or None.
    """
    try:
        # new-style dict
        res = smart.ltpData({"exchange": exchange, "tradingsymbol": tradingsymbol, "symboltoken": str(symboltoken)})
        if isinstance(res, dict):
            data = res.get("data") or {}
            if "ltp" in data and data["ltp"] is not None:
                return float(data["ltp"])
    except TypeError:
        # old-style positional
        try:
            res = smart.ltpData(exchange, tradingsymbol, str(symboltoken))
            if isinstance(res, dict):
                data = res.get("data") or {}
                if "ltp" in data and data["ltp"] is not None:
                    return float(data["ltp"])
        except Exception as e:
            logger.debug(f"positional ltpData failed for {exchange}:{tradingsymbol}/{symboltoken}: {e}")
    except Exception as e:
        logger.debug(f"ltpData failed for {exchange}:{tradingsymbol}/{symboltoken}: {e}")
    return None


# -------------------------------------------------------------------
# Spot fetch for index via ENV token or a quick discovery from options
# -------------------------------------------------------------------
INDEX_TOKEN_ENV_KEY = {
    "NIFTY": "NIFTY_INDEX_TOKEN",
    "BANKNIFTY": "BANKNIFTY_INDEX_TOKEN",
    "FINNIFTY": "FINNIFTY_INDEX_TOKEN",
    "MIDCPNIFTY": "MIDCPNIFTY_INDEX_TOKEN",
}

INDEX_NAME_ALIASES = {
    "NIFTY": ["NIFTY", "NIFTY 50", "NIFTY50"],
    "BANKNIFTY": ["BANKNIFTY", "NIFTY BANK", "NIFTYBANK", "BANK NIFTY"],
    "FINNIFTY": ["FINNIFTY"],
    "MIDCPNIFTY": ["MIDCPNIFTY", "NIFTY MIDCAP SELECT", "NIFTY MIDCAP"],
}


def _fetch_index_spot_from_env(smart, underlying: str) -> Optional[float]:
    key = INDEX_TOKEN_ENV_KEY.get(underlying.upper())
    if not key:
        return None
    tok = os.getenv(key)
    if not tok:
        return None
    for alias in INDEX_NAME_ALIASES.get(underlying.upper(), [underlying.upper()]):
        ltp = _smartapi_ltp(smart, "NSE", alias, tok)
        if ltp is not None:
            logger.info(f"theta_short: spot via ENV token NSE/{alias}/{tok} = {ltp}")
            return ltp
    return None


def _discover_spot_from_chain_midpoint(opts_same_expiry: pd.DataFrame) -> Optional[float]:
    """
    Chain-only fallback: derive a 'spot-like' level by taking the midpoint of
    available strikes in the same-expiry chain. This is robust when your LTP token
    maps to the wrong index scale (e.g., returning ~13k for BANKNIFTY ~55k).
    """
    strikes = opts_same_expiry["strike_int"].dropna().astype(int).unique()
    if len(strikes) == 0:
        return None
    strikes = np.array(sorted(strikes), dtype=float)
    mid = float(np.median(strikes))
    return mid


def _infer_step_from_strikes(strikes: np.ndarray) -> int:
    """
    Infer strike step from the chain itself. Handles common steps (50/100).
    """
    if strikes.size < 2:
        return 100
    diffs = np.diff(np.unique(np.sort(strikes)))
    # choose the most frequent positive diff
    diffs = diffs[diffs > 0]
    if diffs.size == 0:
        return 100
    vals, counts = np.unique(diffs, return_counts=True)
    step = int(vals[np.argmax(counts)])
    # normalize weird artifacts (e.g., 49/51 -> 50)
    if 45 <= step <= 55:
        return 50
    if 95 <= step <= 105:
        return 100
    return step


def _choose_atm_strike(spot: float, strikes: np.ndarray, step_hint: Optional[int] = None) -> int:
    """
    Choose ATM by nearest strike in the chain; fall back to rounding by step_hint.
    """
    strikes = np.array([int(s) for s in strikes if pd.notna(s)], dtype=int)
    if strikes.size == 0:
        raise RuntimeError("No strikes available to choose ATM.")
    # If spot wildly off-range (e.g., 13,800 vs 55,000 chain), clamp to chain median
    min_s, max_s = strikes.min(), strikes.max()
    if not (min_s * 0.5 <= spot <= max_s * 1.5):
        logger.warning(f"Spot {spot:.2f} looks off for chain range [{min_s}, {max_s}]. "
                       f"Using chain median instead.")
        spot = float(np.median(strikes))
    # nearest in-chain
    idx = int(np.argmin(np.abs(strikes - spot)))
    nearest = int(strikes[idx])
    if step_hint:
        # snap to step grid if helpful
        nearest = int(round(nearest / step_hint) * step_hint)
    return nearest


# -------------------------------------------------------------------
# Main strategy
# -------------------------------------------------------------------
def run(smart) -> List[Dict[str, Any]]:
    """
    Weekly-expiry theta harvest: SELL ATM CE + SELL ATM PE (short straddle).

    ENV (optional):
      THETA_UNDERLYING   default=BANKNIFTY
      THETA_LOTS         default=1
      THETA_START_HHMM   default=13:00 (IST)
      THETA_END_HHMM     default=14:45 (IST)
      THETA_FORCE        when "1", bypass expiry-day/time window (for testing)
      <UNDERLYING>_INDEX_TOKEN  (e.g., BANKNIFTY_INDEX_TOKEN, NIFTY_INDEX_TOKEN)
    """
    U = os.getenv("THETA_UNDERLYING", "BANKNIFTY").strip().upper()
    lots_req = int(os.getenv("THETA_LOTS", "1"))
    start_hhmm = os.getenv("THETA_START_HHMM", "13:00")
    end_hhmm = os.getenv("THETA_END_HHMM", "14:45")
    force = os.getenv("THETA_FORCE", "0") == "1"

    now = datetime.now(IST)
    hhmm = f"{now.hour:02d}:{now.minute:02d}"

    # Expiry-day (Thu) & window guard (unless THETA_FORCE=1)
    if not force:
        if now.weekday() != 3:
            logger.info("theta_short: Not weekly expiry day (Thu) — skipping.")
            return []
        if not (start_hhmm <= hhmm <= end_hhmm):
            logger.info(f"theta_short: Outside time window {start_hhmm}-{end_hhmm} — skipping.")
            return []

    # Options universe (strict) & nearest expiry (>= today)
    opts = load_options(U)
    exp_d: date = nearest_expiry(opts, now.date(), window_days=10)
    same_exp = opts.loc[opts["expiry_dt"].dt.date == exp_d].copy()
    if same_exp.empty:
        logger.warning(f"theta_short: no {U} rows for expiry {exp_d}.")
        return []

    # Infer the strike step from this expiry's strikes
    strikes = same_exp["strike_int"].dropna().astype(int).values
    if strikes.size == 0:
        logger.warning(f"theta_short: no strikes for expiry {exp_d}.")
        return []
    step = _infer_step_from_strikes(strikes)

    # Spot (ENV token best → chain fallback)
    spot = _fetch_index_spot_from_env(smart, U)
    if spot is None:
        # chain-only midpoint as robust fallback
        spot = _discover_spot_from_chain_midpoint(same_exp) or float(np.median(strikes))
        logger.info(f"theta_short: using chain-derived spot ≈ {spot:.2f}")

    # Choose ATM on the actual chain grid
    atm = _choose_atm_strike(spot, strikes, step_hint=step)

    # Pull CE/PE rows for the chosen strike (expands ±step if exact pair missing)
    try:
        ce_row, pe_row = get_option_rows(same_exp, exp_d, atm, step=step)
    except Exception as e:
        logger.warning(f"theta_short: Unable to resolve CE/PE pair at ~{atm} for {U} {exp_d}: {e}")
        # dump a quick preview to help diagnosing filters
        prev = same_exp[["token", "symbol", "name", "expiry", "strike_int"]].head(25)
        logger.debug(f"theta_short chain preview (top 25):\n{prev}")
        return []

    # Lot sizing (from universe)
    lotsize = get_lotsize(opts, U) or int(os.getenv("DEFAULT_LOTSIZE", "25"))
    qty = max(1, lots_req) * int(lotsize)

    # Extract tradingsymbol/token/exchange
    ce_tsym, ce_tok, ce_ex = extract_symbol_fields(ce_row)
    pe_tsym, pe_tok, pe_ex = extract_symbol_fields(pe_row)

    if not ce_tsym or not ce_tok or not pe_tsym or not pe_tok:
        logger.warning(f"theta_short: missing symbol fields — CE({ce_tsym}/{ce_tok}) PE({pe_tsym}/{pe_tok})")
        return []

    # Order tag for grouping
    tag = f"THETA-{U}-{exp_d}-{atm}"

    # Place LIMIT short orders; your global executor will convert to market/SL/oco if configured
    orders: List[Dict[str, Any]] = [
        {
            "variety": "NORMAL",
            "exchange": "NFO",
            "tradingsymbol": ce_tsym,
            "symboltoken": ce_tok,
            "transactiontype": "SELL",
            "ordertype": "LIMIT",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": qty,
            "ordertag": tag,
        },
        {
            "variety": "NORMAL",
            "exchange": "NFO",
            "tradingsymbol": pe_tsym,
            "symboltoken": pe_tok,
            "transactiontype": "SELL",
            "ordertype": "LIMIT",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": qty,
            "ordertag": tag,
        },
    ]

    logger.info(
        f"theta_short: SELL ATM straddle {U} {exp_d} @≈{atm} "
        f"(step={step}, lotsize={lotsize}, lots={lots_req}, spot≈{spot:.2f})"
    )
    return orders
