# scripts/peek_instruments.py
from __future__ import annotations
from pathlib import Path
import sys, argparse
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_CSV = ROOT / "data" / "OpenAPIScripMaster.csv"

def _build_cli():
    p = argparse.ArgumentParser(description="Peek into instruments master and search symbols quickly")
    p.add_argument("--csv", default=str(DEFAULT_CSV), help="Path to OpenAPIScripMaster.csv")
    p.add_argument("--exchange", default="NSE", help="Exchange filter (e.g., NSE, NFO). Default NSE")
    p.add_argument("--query", default="RELIANCE", help="Search term (case-insensitive)")
    p.add_argument("--exact", action="store_true", help="Exact match (==) instead of substring")
    p.add_argument("--limit", type=int, default=10, help="Max rows to print (default 10)")
    p.add_argument("--show-cols", action="store_true", help="Print first 25 column names and exit")
    return p

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    # Read robustly; keep strings as strings
    df = pd.read_csv(path, low_memory=False, dtype=str, on_bad_lines="skip")
    # Canonicalize column names
    df.columns = [c.strip() for c in df.columns]
    lower_map = {c.lower(): c for c in df.columns}
    # Ensure common aliases exist for convenience
    def alias(dst: str, *cands: str):
        for c in cands:
            if c in lower_map:
                return lower_map[c]
        return None
    cols = {
        "exch_seg": alias("exch_seg", "exch_seg", "exchange", "exch"),
        "symbol": alias("symbol", "symbol"),
        "name": alias("name", "name"),
        "tradingsymbol": alias("tradingsymbol", "tradingsymbol", "tradingsym", "tsym"),
        "token": alias("token", "symboltoken", "token"),
        "instrumenttype": alias("instrumenttype", "instrumenttype", "instrument"),
        "description": alias("description", "description", "desc"),
        "expiry": alias("expiry", "expiry", "exp_date"),
        "strike": alias("strike", "strike", "strikprice", "strikeprice"),
        "optiontype": alias("optiontype", "optiontype", "opttype"),
    }
    return df, cols

def _select_exchange(df: pd.DataFrame, cols: dict, exch: str) -> pd.DataFrame:
    c = cols.get("exch_seg")
    if not c or c not in df.columns:
        # If exchange column missing, return as-is
        return df
    return df[df[c].astype(str).str.upper().eq(exch.upper())]

def _search(df: pd.DataFrame, cols: dict, query: str, exact: bool) -> pd.DataFrame:
    query = str(query or "").strip()
    if not query:
        return df.iloc[0:0]

    buckets = []
    for key in ("symbol", "name", "tradingsymbol", "description"):
        col = cols.get(key)
        if col and col in df.columns:
            s = df[col].astype(str)
            mask = (s.str.upper() == query.upper()) if exact else s.str.contains(query, case=False, na=False)
            if mask.any():
                buckets.append(df.loc[mask])

    if not buckets:
        return df.iloc[0:0]
    out = pd.concat(buckets, axis=0).drop_duplicates()
    return out

def main(argv=None):
    args = _build_cli().parse_args(argv)
    csv_path = Path(args.csv)

    df, cols = _read_csv(csv_path)

    if args.show_cols:
        print("Columns (first 25):", list(df.columns)[:25])
        return 0

    # Filter by exchange
    df_ex = _select_exchange(df, cols, args.exchange)

    # Quick summary by instrument type
    inst_col = cols.get("instrumenttype")
    if inst_col and inst_col in df_ex.columns:
        summary = (
            df_ex[inst_col]
            .astype(str)
            .str.upper()
            .value_counts()
            .head(12)
            .rename_axis("instrumenttype")
            .reset_index(name="rows")
        )
        print(f"{args.exchange} instrumenttype counts (top 12):")
        print(summary.to_string(index=False))
        print()

    # Search
    hits = _search(df_ex, cols, args.query, args.exact)

    # Decide which columns to show (only those that exist)
    want = ["symbol", "name", "tradingsymbol", "token", "instrumenttype", "expiry", "strike", "optiontype", "exch_seg"]
    show_cols = [cols[k] for k in want if cols.get(k) in df.columns]

    print(f"Query: {args.query!r} (exact={bool(args.exact)}) on {args.exchange}")
    if hits.empty:
        print("No matches found. (Tip: try without --exact or check --exchange)")
        return 1

    print(hits[show_cols].head(max(1, int(args.limit))).to_string(index=False))
    if len(hits) > args.limit:
        print(f"\n… and {len(hits) - args.limit} more rows (use --limit to see more)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
