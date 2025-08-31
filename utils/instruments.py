# utils/instruments.py
from __future__ import annotations

from pathlib import Path
from datetime import date
from functools import lru_cache
from typing import Iterable, Dict, Optional, Tuple
import os
import re
import warnings

import pandas as pd
from loguru import logger

# ---------- Paths / config ----------
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CSV_DEFAULT = DATA_DIR / "OpenAPIScripMaster.csv"
JSON_FALLBACK = DATA_DIR / "OpenAPIScripMaster.json"

CEPE_RE = re.compile(r"(CE|PE)\b", re.IGNORECASE)


# ---------- Core loader (cached) ----------
@lru_cache(maxsize=1)
def _read_instruments_df() -> pd.DataFrame:
    """
    Load Angel instruments from CSV (preferred) or JSON fallback.
    Honors INSTRUMENTS_CSV env var if present.
    Normalizes common columns and adds:
      - expiry_dt (parsed)
      - strike_rupees (normalized)
      - lotsize (int)
    """
    csv_path = Path(os.getenv("INSTRUMENTS_CSV") or CSV_DEFAULT)

    if csv_path.exists():
        df = pd.read_csv(csv_path, low_memory=False)
        src = f"CSV:{csv_path}"
    elif JSON_FALLBACK.exists():
        df = pd.read_json(JSON_FALLBACK)
        src = f"JSON:{JSON_FALLBACK}"
    else:
        raise FileNotFoundError(
            "No instruments file found.\n"
            f"- Tried CSV:  {csv_path}\n"
            f"- Tried JSON: {JSON_FALLBACK}\n"
            "Run: python refresh_instruments.py"
        )

    # normalize columns/strings
    df.columns = [c.strip().lower() for c in df.columns]
    for col in (
        "name", "symbol", "tradingsymbol", "exchange", "exch_seg",
        "symboltoken", "token", "optiontype", "instrumenttype"
    ):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # parse expiry safely
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        if "expiry" in df.columns:
            df["expiry_dt"] = pd.to_datetime(df["expiry"], errors="coerce")
        else:
            df["expiry_dt"] = pd.NaT

    # strike normalization to rupees (Angel often stores *100)
    if "strike" in df.columns:
        s = pd.to_numeric(df["strike"], errors="coerce")
        med = s.dropna().median()
        df["strike_rupees"] = (s / 100.0).round(2) if pd.notna(med) and med > 100000 else s
    else:
        df["strike_rupees"] = pd.NA

    # lot size
    if "lotsize" in df.columns:
        df["lotsize"] = pd.to_numeric(df["lotsize"], errors="coerce").fillna(0).astype(int)
    else:
        df["lotsize"] = 0

    # option type standardization
    if "optiontype" in df.columns:
        df["optiontype"] = df["optiontype"].astype(str).str.upper().replace({
            "CALL": "CE",
            "PUT": "PE",
        })

    logger.info(f"Loaded instruments from {src}: rows={len(df)}")
    return df


def load_instruments() -> pd.DataFrame:
    """Public loader (returns a copy)."""
    return _read_instruments_df().copy()


# ---------- helpers ----------
def _series_upper(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype(str).str.strip().str.upper()
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def _infer_optiontype_from_symbol(row: pd.Series) -> str:
    """Infer CE/PE from tradingsymbol or symbol text."""
    for col in ("tradingsymbol", "symbol"):
        if col in row and pd.notna(row[col]):
            m = CEPE_RE.search(str(row[col]))
            if m:
                return m.group(1).upper()
    return ""


def _ensure_optiontype(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing/blank optiontype by inferring from tradingsymbol/symbol."""
    if "optiontype" not in df.columns:
        df = df.copy()
        df["optiontype"] = ""
    mask_missing = df["optiontype"].isna() | (df["optiontype"].astype(str).str.strip() == "")
    if mask_missing.any():
        inferred = df.loc[mask_missing].apply(_infer_optiontype_from_symbol, axis=1)
        df.loc[mask_missing, "optiontype"] = inferred
    df["optiontype"] = df["optiontype"].astype(str).str.upper()
    return df


# ---------- equities ----------
def _score_equity_row(row: pd.Series, sym: str) -> int:
    score = 0
    name = str(row.get("name", "")).upper()
    symbol = str(row.get("symbol", "")).upper()
    ts = str(row.get("tradingsymbol", "")).upper()
    it = str(row.get("instrumenttype", "")).upper()

    if name == sym:
        score += 100
    if symbol == f"{sym}-EQ":
        score += 95
    if ts == f"{sym}-EQ":
        score += 95
    if symbol.endswith("-EQ") and sym in symbol:
        score += 80
    if ts.endswith("-EQ") and sym in ts:
        score += 80
    if it in ("", "EQ", "EQUITY"):
        score += 10
    score += max(0, 10 - abs(len(symbol) - (len(sym) + 3)))  # prefer tight match
    return score


def pick_nse_equity_tokens(symbols: Iterable[str]) -> Dict[str, str]:
    """
    Resolve Angel 'token' for NSE cash equities, given human symbols like
    'RELIANCE', 'TCS', 'INFY'.
    """
    df = _read_instruments_df()
    if "exch_seg" not in df.columns or not {"token", "symboltoken"} & set(df.columns):
        raise RuntimeError("Instrument file missing required columns (exch_seg, token/symboltoken).")

    nse = df[df["exch_seg"].astype(str).str.upper().eq("NSE")].copy()

    # prefer equity-like rows (but don't require)
    if "instrumenttype" in nse.columns:
        it = nse["instrumenttype"].astype(str).str.upper()
        eq_like = (nse["instrumenttype"].isna()) | (it.eq("")) | (it.isin(["EQ", "EQUITY"]))
        nse_eq_pref = nse[eq_like].copy()
    else:
        nse_eq_pref = nse

    out: Dict[str, str] = {}

    for raw in symbols:
        sym = raw.strip().upper()
        if not sym:
            continue

        # vectorized OR over name/symbol/tradingsymbol
        mask_any = pd.Series(False, index=nse_eq_pref.index)
        for c in ("name", "symbol", "tradingsymbol"):
            if c in nse_eq_pref.columns:
                mask_any = mask_any | nse_eq_pref[c].astype(str).str.contains(rf"\b{re.escape(sym)}\b", case=False, na=False)

        cands = nse_eq_pref.loc[mask_any].copy()
        if cands.empty:
            mask_any = pd.Series(False, index=nse.index)
            for c in ("name", "symbol", "tradingsymbol"):
                if c in nse.columns:
                    mask_any = mask_any | nse[c].astype(str).str.contains(rf"\b{re.escape(sym)}\b", case=False, na=False)
            cands = nse.loc[mask_any].copy()

        if cands.empty:
            continue

        cands["_score"] = cands.apply(lambda r: _score_equity_row(r, sym), axis=1)
        best = cands.sort_values("_score", ascending=False).head(1)
        tok = str(best.iloc[0].get("token") or best.iloc[0].get("symboltoken") or "").strip()
        if tok:
            out[sym] = tok

    return out


def get_equity_token(symbol: str) -> Optional[str]:
    return pick_nse_equity_tokens([symbol]).get(symbol.strip().upper())


# ---------- option universe ----------
def load_options(underlying: str) -> pd.DataFrame:
    """
    Slice NFO options for the given underlying (e.g., 'BANKNIFTY', 'NIFTY', 'FINNIFTY').
    Strict rules to avoid cross-family mixups.
    """
    df = _read_instruments_df()

    ex = _series_upper(df, "exchange") if "exchange" in df.columns else _series_upper(df, "exch_seg")
    ex_ok = ex.str.contains("NFO", na=False)

    it = _series_upper(df, "instrumenttype")
    it_ok = (it == "") | it.str.contains("OPT", na=False)

    nm = _series_upper(df, "name")
    sy = _series_upper(df, "symbol")
    ts = _series_upper(df, "tradingsymbol")
    und = underlying.strip().upper()

    u_ok = (nm == und) | sy.str.startswith(und, na=False) | ts.str.startswith(und, na=False)

    # exclude other index families explicitly
    family_exclusions = {
        "NIFTY": ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"],
        "BANKNIFTY": ["NIFTY", "FINNIFTY", "MIDCPNIFTY"],
        "FINNIFTY": ["NIFTY", "BANKNIFTY", "MIDCPNIFTY"],
    }
    for bad in family_exclusions.get(und, []):
        u_ok &= ~(sy.str.startswith(bad, na=False) | ts.str.startswith(bad, na=False) | (nm == bad))

    mask = ex_ok & it_ok & u_ok
    opt = df.loc[mask].copy()

    # keep common fields; backfill missing
    keep = [
        "tradingsymbol", "symbol", "symboltoken", "token",
        "exchange", "exch_seg", "name", "optiontype",
        "expiry", "expiry_dt", "strike_rupees", "lotsize", "instrumenttype"
    ]
    for k in keep:
        if k not in opt.columns:
            opt[k] = pd.NA

    # numeric types + stable int strike
    opt["strike_rupees"] = pd.to_numeric(opt["strike_rupees"], errors="coerce")
    opt["lotsize"] = pd.to_numeric(opt["lotsize"], errors="coerce").fillna(0).astype(int)
    opt["strike_int"] = pd.to_numeric(opt["strike_rupees"], errors="coerce").round().astype("Int64")

    opt = _ensure_optiontype(opt)

    if opt.empty:
        logger.error(
            f"No {underlying} options found after strict filtering. "
            "Refresh instruments (python refresh_instruments.py) or check naming."
        )
    else:
        logger.info(f"Matched {len(opt)} {underlying} option rows (strict).")

    return opt


# ---------- convenience (ATM/expiry pair finder) ----------
def pick_atm_strike(spot: float, step: int = 100) -> int:
    if spot is None or spot != spot:
        raise ValueError("Invalid spot for ATM calculation")
    return int(round(spot / step) * step)


def round_banknifty_strike(spot: float) -> int:
    return pick_atm_strike(spot, step=100)


def round_nifty_strike(spot: float) -> int:
    return pick_atm_strike(spot, step=50)


def nearest_expiry(opts: pd.DataFrame, target: date, window_days: int = 10) -> date:
    exp_series = opts.get("expiry_dt")
    if exp_series is None or exp_series.isna().all():
        raise RuntimeError("No expiries found in options universe (missing expiry_dt).")

    exp_dates = exp_series.dropna().dt.date.unique()
    if len(exp_dates) == 0:
        raise RuntimeError("No expiries found in options universe.")

    future = sorted(d for d in exp_dates if d >= target)
    if future:
        best = future[0]
        if (best - target).days <= window_days:
            return best
        logger.warning(f"Nearest expiry {best} is beyond window ({window_days}d) from target {target}.")
        return best

    best = max(exp_dates)
    logger.warning(f"No future expiry >= {target}. Using latest available {best}.")
    return best


def get_option_rows(
    opts: pd.DataFrame,
    expiry_d: date,
    strike_rupees: int,
    step: int = 100,
) -> Tuple[pd.Series, pd.Series]:
    """
    Return (CE_row, PE_row) for expiry & strike.
    If exact pair isn't found, expands to nearest available strikes (±step, ±2*step, ...).
    """
    if opts.empty:
        raise RuntimeError("Options dataframe is empty. Refresh instruments?")

    if "strike_int" not in opts.columns:
        opts = opts.copy()
        opts["strike_int"] = pd.to_numeric(opts["strike_rupees"], errors="coerce").round().astype("Int64")

    target = int(round(strike_rupees))

    same_expiry = opts.loc[opts["expiry_dt"].dt.date == expiry_d].copy()
    if same_expiry.empty:
        avail_expiries = sorted({d for d in opts["expiry_dt"].dropna().dt.date.unique()})
        logger.error(f"No rows at expiry={expiry_d}. Available expiries: {avail_expiries}")
        raise RuntimeError(f"No options for expiry={expiry_d}.")

    same_expiry = _ensure_optiontype(same_expiry)

    avail_strikes = same_expiry["strike_int"].dropna().astype(int).unique()
    if len(avail_strikes) == 0:
        raise RuntimeError(f"No strikes for expiry={expiry_d}.")

    max_hops = 400
    for hops in range(0, max_hops + 1):
        dirs = [0] if hops == 0 else [-1, 1]
        for d in dirs:
            cand = target + d * hops * step
            if cand not in avail_strikes:
                continue
            slate = same_expiry.loc[same_expiry["strike_int"] == cand]
            ot_local = slate["optiontype"].astype(str).str.upper()
            ce = slate.loc[ot_local == "CE"]
            pe = slate.loc[ot_local == "PE"]
            if not ce.empty and not pe.empty:
                if cand != target:
                    logger.warning(f"Adjusted strike from {target} to nearest available {cand} for expiry {expiry_d}.")
                return ce.iloc[0], pe.iloc[0]

    sample = sorted(int(x) for x in avail_strikes)[:40]
    logger.error(f"No CE/PE pair for expiry={expiry_d} strike≈{target}. Available strikes (sample): {sample}")
    raise RuntimeError(f"Missing CE/PE leg for expiry={expiry_d} strike≈{target}.")


# ---------- extraction / tokens ----------
def extract_symbol_fields(row: pd.Series) -> tuple[str, str, str]:
    """
    Return (tradingsymbol, symboltoken, exchange) with robust fallbacks.
    """
    tsym = str(
        row.get("tradingsymbol")
        or row.get("symbol")
        or row.get("symbolname")
        or ""
    ).strip()

    token = str(
        row.get("symboltoken")
        or row.get("token")
        or row.get("symbol_token")
        or ""
    ).strip()

    exch = str(
        row.get("exchange")
        or row.get("exch_seg")
        or "NFO"
    ).strip().upper()

    # normalize Angel's exchange naming
    if exch == "NSECM":
        exch = "NSE"
    if exch == "NFO" or "FO" in exch:
        exch = "NFO"

    return tsym, token, exch


def find_option_token(
    underlying: str,
    expiry_d: date,
    strike_rupees: int,
    side: str,  # "CE" or "PE"
) -> Optional[str]:
    """
    Quick single-leg token finder (uses strict family filtering).
    """
    side = side.upper().strip()
    opts = load_options(underlying)
    same_exp = opts.loc[opts["expiry_dt"].dt.date == expiry_d]
    if same_exp.empty:
        return None
    same_exp = _ensure_optiontype(same_exp)
    strike_i = int(round(strike_rupees))
    leg = same_exp.loc[
        (same_exp["strike_int"] == strike_i) &
        (same_exp["optiontype"] == side)
    ]
    if leg.empty:
        return None
    token = str(leg.iloc[0].get("symboltoken") or leg.iloc[0].get("token") or "")
    return token or None


def get_lotsize(opts_or_df: pd.DataFrame, underlying: str) -> int:
    """
    Lotsize for the given underlying from either the options slice or the full df.
    Chooses a sensible mode/first non-null.
    """
    df = opts_or_df
    if "name" not in df.columns:
        df = _read_instruments_df()
    sub = df[df["name"].astype(str).str.upper().eq(underlying.strip().upper())]
    if sub.empty and "symbol" in df.columns:
        sub = df[df["symbol"].astype(str).str.upper().str.startswith(underlying.strip().upper(), na=False)]
    if sub.empty:
        return 0
    ls = sub["lotsize"]
    try:
        return int(ls.mode().iloc[0])
    except Exception:
        nz = ls.dropna()
        return int(nz.iloc[0]) if len(nz) else 0
