# scripts/list_banknifty_expiries.py
from __future__ import annotations
import argparse
from typing import Optional, Tuple
import pandas as pd
from loguru import logger

from utils.instruments import load_instruments
from utils.expiry import next_thursday

# _normalize_strike_scale may not exist in all trees; import best-effort
try:
    from utils.instruments import _normalize_strike_scale  # type: ignore
except Exception:
    _normalize_strike_scale = None  # type: ignore


def _build_cli():
    p = argparse.ArgumentParser(description="List option expiries for an underlying from instruments master")
    p.add_argument("--underlying", default="BANKNIFTY", help="Underlying to match in tradingsymbol (default BANKNIFTY)")
    p.add_argument("--limit", type=int, default=15, help="Max expiries to show (default 15)")
    p.add_argument("--weekly", action="store_true", help="Show only weekly expiries (heuristic)")
    p.add_argument("--monthly", action="store_true", help="Show only monthly expiries (heuristic)")
    p.add_argument("--peek", action="store_true", help="Show a small sample of rows for the next Thursday expiry")
    p.add_argument("--csv", help="Optional path to save the expiry summary CSV")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    return p


def _cols(df: pd.DataFrame) -> dict:
    """Map loose column names -> canonical keys we use."""
    lower = {c.lower(): c for c in df.columns}
    return dict(
        instrumenttype=lower.get("instrumenttype") or lower.get("instrument") or "instrumenttype",
        tradingsymbol=lower.get("tradingsymbol") or lower.get("symbol") or "tradingsymbol",
        expiry=lower.get("expiry") or "expiry",
        strike=lower.get("strike") or "strike",
        optiontype=lower.get("optiontype") or lower.get("opttype") or "optiontype",
        symboltoken=lower.get("symboltoken") or lower.get("token") or "symboltoken",
        exch_seg=lower.get("exch_seg") or lower.get("exchange") or "exch_seg",
    )


def _is_option_like(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.upper()
    return t.str.contains("OPT", na=False) | t.isin(["CE", "PE", "OPTIDX", "OPTSTK"])


def _maybe_monthly(expiry: pd.Timestamp) -> bool:
    """
    Heuristic: Monthly if it's the last Thursday of the month.
    (Doesn't adjust for holiday-shifted Wednesdays; treat those as weekly.)
    """
    if pd.isna(expiry):
        return False
    # find next Thursday after expiry; if month changes within 7 days, current is "last Thursday"
    # (i.e., no more Thursdays remain in the month)
    day = expiry
    # Thursday = 3 (Mon=0)
    if day.weekday() != 3:
        return False
    next_week = day + pd.Timedelta(days=7)
    return next_week.month != day.month


def _summary_table(df: pd.DataFrame, cols: dict, underlying: str) -> pd.DataFrame:
    # Filter to options for the underlying
    mask_opt = _is_option_like(df[cols["instrumenttype"]]) | _is_option_like(df[cols["optiontype"]])
    mask_ul = df[cols["tradingsymbol"]].astype(str).str.upper().str.contains(underlying.upper(), na=False)
    d = df[mask_opt & mask_ul].copy()

    # Normalize strike scale if helper exists
    if _normalize_strike_scale is not None and cols["strike"] in d:
        try:
            d[cols["strike"]] = _normalize_strike_scale(d[cols["strike"]])
        except Exception:
            pass

    # Parse expiry
    d["__expiry"] = pd.to_datetime(d[cols["expiry"]], errors="coerce")
    d = d.dropna(subset=["__expiry"])

    # Count contracts per expiry
    grp = d.groupby("__expiry").size().reset_index(name="contracts")
    grp = grp.sort_values("__expiry").reset_index(drop=True)

    # Heuristic monthly flag
    grp["is_monthly"] = grp["__expiry"].apply(_maybe_monthly)

    # Friendly columns
    out = grp.rename(columns={"__expiry": "expiry"})
    out["expiry_date"] = out["expiry"].dt.date
    out = out[["expiry_date", "contracts", "is_monthly", "expiry"]]
    return out


def main(argv=None):
    args = _build_cli().parse_args(argv)
    logger.remove()
    logger.add(lambda m: print(m, end=""), level=("DEBUG" if args.verbose else "INFO"))

    df = load_instruments()
    c = _cols(df)

    table = _summary_table(df, c, args.underlying)
    if args.monthly and args.weekly:
        logger.warning("Both --monthly and --weekly specified; showing all.")
    elif args.monthly:
        table = table[table["is_monthly"]]
    elif args.weekly:
        table = table[~table["is_monthly"]]

    # Show top N
    head = table.head(max(1, int(args.limit)))

    print(f"Available {args.underlying.upper()} option expiries (first {len(head)}):")
    if head.empty:
        print(" (none found)")
    else:
        # neat print
        print(head[["expiry_date", "contracts", "is_monthly"]].to_string(index=False))

    # Optional peek around next Thursday
    if args.peek:
        target = next_thursday()
        dd = df.copy()
        dd["__expiry"] = pd.to_datetime(dd[c["expiry"]], errors="coerce")
        same = dd[dd["__expiry"].dt.date == pd.Timestamp(target).date()]
        mask_opt = _is_option_like(same[c["instrumenttype"]]) | _is_option_like(same[c["optiontype"]])
        mask_ul = same[c["tradingsymbol"]].astype(str).str.upper().str.contains(args.underlying.upper(), na=False)
        sample = same[mask_opt & mask_ul].copy()

        if _normalize_strike_scale is not None and c["strike"] in sample:
            try:
                sample[c["strike"]] = _normalize_strike_scale(sample[c["strike"]])
            except Exception:
                pass

        cols_to_show = [c["tradingsymbol"], c["expiry"], c["strike"], c["optiontype"], c["symboltoken"]]
        cols_to_show = [x for x in cols_to_show if x in sample.columns]
        print(f"\nRows for target expiry {target}: {len(sample)}")
        if cols_to_show:
            print(sample[cols_to_show].head(20).to_string(index=False))
        else:
            print("(columns missing for sample view)")

    # CSV export
    if args.csv:
        # save the full table (not just head), with ISO expiry
        out = table.copy()
        out["expiry"] = out["expiry"].dt.strftime("%Y-%m-%d")
        out.to_csv(args.csv, index=False)
        print(f"\nSaved summary to {args.csv}")


if __name__ == "__main__":
    main()
