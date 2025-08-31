# utils/resolve.py
from __future__ import annotations
import re, os
from functools import lru_cache
from pathlib import Path
from typing import Tuple, List, Dict, Optional
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
CSV_PATH = DATA_DIR / "OpenAPIScripMaster.csv"

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Scrip master not found at {path}")
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="utf-8-sig")

@lru_cache(maxsize=1)
def _load_df_cached(path: str) -> pd.DataFrame:
    df = _read_csv(Path(path))
    df.columns = [c.strip().lower() for c in df.columns]
    want = ["symbol","tradingsymbol","exchange","exch_seg","series",
            "instrumenttype","name","symboltoken","token"]
    for c in want:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str).str.strip()
    ex = df["exchange"].where(df["exchange"] != "", df["exch_seg"])
    df["ex"] = ex.astype(str).str.upper()
    return df

def _load_df(*, refresh: bool = False) -> pd.DataFrame:
    # Cache key = path string; bust if refresh=True
    if refresh:
        _load_df_cached.cache_clear()
    return _load_df_cached(str(CSV_PATH))

def _score_candidates(df: pd.DataFrame, sym: str, *, exact_only: bool = False) -> pd.DataFrame:
    variants = {sym, f"{sym}-EQ"}
    pat = re.compile(rf"^{re.escape(sym)}(?:-EQ)?$", re.IGNORECASE)
    out = df.copy()

    out["_score_exact_ts"] = out["tradingsymbol"].str.upper().isin(variants).astype(int)
    out["_score_exact_sym"] = out["symbol"].str.upper().isin(variants).astype(int)
    out["_score_regex"]     = (out["tradingsymbol"].str.match(pat, na=False) |
                               out["symbol"].str.match(pat, na=False)).astype(int)
    out["_score_series_eq"] = (out["series"].str.upper() == "EQ").astype(int)
    out["_score_series_pen"] = (~out["series"].str.upper().isin(["", "EQ"])).astype(int) * -1  # down-weight non-EQ
    out["_score_instr_eq"]  = out["instrumenttype"].str.upper().isin(["EQUITY","EQ","STK"]).astype(int)
    out["_score_name"]      = out["name"].str.upper().str.contains(sym, na=False).astype(int)
    out["_score_total"] = (
        out["_score_exact_ts"]*7 +
        out["_score_exact_sym"]*6 +
        out["_score_regex"]*5 +
        out["_score_series_eq"]*3 +
        out["_score_instr_eq"]*2 +
        out["_score_name"] +
        out["_score_series_pen"]  # penalty
    )

    if exact_only:
        out = out[(out["_score_exact_ts"] + out["_score_exact_sym"] + out["_score_regex"]) > 0]
    return out

def resolve_nse_token(
    symbol: str,
    *,
    prefer_ex: str = "NSE",
    exact_only: bool = False,
    refresh: bool = False,
) -> Tuple[str, str]:
    """
    Return (tradingsymbol, symboltoken) for cash symbol like 'RELIANCE'.
    Options:
      - prefer_ex: exchange tag to prefer (default 'NSE')
      - exact_only: require exact/regex match (no fuzzy name-only hits)
      - refresh: bypass CSV cache (reload from disk)
    """
    sym = symbol.strip().upper()
    df = _load_df(refresh=refresh)

    pool = df[df["ex"].str.contains(prefer_ex.upper(), na=False)]
    if pool.empty:
        pool = df

    scored = _score_candidates(pool, sym, exact_only=exact_only).sort_values("_score_total", ascending=False)
    if scored.empty or scored["_score_total"].iloc[0] <= 0:
        # try global pool last
        scored = _score_candidates(df, sym, exact_only=exact_only).sort_values("_score_total", ascending=False)
        if scored.empty or scored["_score_total"].iloc[0] <= 0:
            raise ValueError(f"Token not found for {symbol} (prefer_ex={prefer_ex})")

    best = scored.iloc[0]
    ts = (best.get("tradingsymbol") or best.get("symbol") or sym).strip().upper()
    token = str(best.get("symboltoken") or best.get("token") or "").strip()
    if not token or token == "0":
        raise ValueError(f"symboltoken missing for {symbol} (picked {ts})")
    return ts, token

def debug_candidates(symbol: str, limit: int = 10, *, refresh: bool = False) -> List[Dict]:
    """Return top candidate rows for manual inspection."""
    sym = symbol.strip().upper()
    df = _load_df(refresh=refresh)
    scored = _score_candidates(df, sym).sort_values("_score_total", ascending=False)
    cols = ["symbol","tradingsymbol","ex","series","instrumenttype","name","symboltoken","token","_score_total"]
    return scored[cols].head(limit).to_dict(orient="records")
