# scripts/compare_sweeps.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

PARAM_CANDIDATES = [
    # common knobs
    "timeframe", "fill", "allow_short", "session", "cooldown_bars",
    "fixed_qty", "atr_n", "atr_sl_mult", "atr_tp_mult", "fee_bps", "slip_bps",
    # strat knobs
    "fast", "slow",
    "bb_n", "bb_k",
    "vwap_n", "vwap_z",
    "orb_mins",
    "vol_n", "vol_k",
]

def _split_keys(s: str | None) -> List[str]:
    if not s:
        return []
    return [k.strip() for k in s.split(",") if k.strip()]

def _coerce_bools(df: pd.DataFrame, cols=("allow_short",)) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            # handle 0/1/True/False/"True"/"False"
            out[c] = (
                out[c]
                .astype(str)
                .str.lower()
                .map({"true": True, "false": False, "1": True, "0": False})
                .fillna(out[c])
            )
            if out[c].dtype != bool:
                # for strings like "True"/"False"
                out[c] = out[c].replace({"True": True, "False": False})
                try:
                    out[c] = out[c].astype(bool)
                except Exception:
                    pass
    return out

def _collapse_duplicates(df: pd.DataFrame, keys: List[str], agg: str = "best") -> pd.DataFrame:
    """
    If there are duplicates on key columns, collapse them.
    agg = 'best'  -> keep row with highest sharpe
    agg = 'mean'  -> metric mean
    agg = 'median'-> metric median
    agg = 'error' -> raise
    """
    if not len(df) or not keys:
        return df

    dups = df.duplicated(subset=keys, keep=False)
    if not dups.any():
        return df

    if agg == "error":
        sample = df.loc[dups, keys].head(5)
        raise ValueError(f"Duplicate rows on {keys}.\n{sample}")

    if agg == "best":
        # sort by sharpe desc and keep first per group
        tmp = df.sort_values("sharpe", ascending=False, kind="mergesort")
        return tmp.drop_duplicates(subset=keys, keep="first")

    # mean/median over numeric metrics; take first parameter values
    metrics = [c for c in ["sharpe", "total_return", "cagr", "max_drawdown", "trades"] if c in df.columns]
    grouped = df.groupby(keys, as_index=False)
    if agg == "mean":
        g = grouped[metrics].mean()
    else:
        g = grouped[metrics].median()
    return g

def main():
    ap = argparse.ArgumentParser(description="Compare two sweep CSVs and show/emit a diff.")
    ap.add_argument("--csv_a", required=True)
    ap.add_argument("--csv_b", required=True)
    ap.add_argument("--label_a", default="A")
    ap.add_argument("--label_b", default="B")
    ap.add_argument("--strategy", default=None, help="Optional: filter by strategy if column exists")
    ap.add_argument("--on", default=None, help="Join keys CSV (e.g. 'fast,slow,atr_sl_mult,atr_tp_mult')")
    ap.add_argument("--agg", choices=("best", "mean", "median", "error"), default="best",
                    help="How to handle duplicates on join keys.")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--out_csv", default=None, help="Optional: write full diff table here")
    ap.add_argument("--strict_keys", action="store_true",
                    help="If set, error out when any requested --on keys are missing in either CSV.")
    args = ap.parse_args()

    df_a = pd.read_csv(args.csv_a)
    df_b = pd.read_csv(args.csv_b)

    # Optional strategy filter (if present)
    if args.strategy:
        if "strategy" in df_a.columns:
            df_a = df_a[df_a["strategy"] == args.strategy].copy()
        if "strategy" in df_b.columns:
            df_b = df_b[df_b["strategy"] == args.strategy].copy()

    # Coerce bool-like
    df_a = _coerce_bools(df_a)
    df_b = _coerce_bools(df_b)

    # Derive join keys
    user_keys = _split_keys(args.on)
    if user_keys:
        missing = [k for k in user_keys if k not in df_a.columns or k not in df_b.columns]
        if missing and args.strict_keys:
            raise ValueError(f"Join key(s) not in both CSVs: {missing}")
        # drop missing keys if not strict
        join_keys = [k for k in user_keys if (k in df_a.columns and k in df_b.columns)]
        if not join_keys:
            raise ValueError("No usable join keys after dropping missing ones. Check --on or CSV columns.")
    else:
        # infer from intersection of known parameter columns
        join_keys = [k for k in PARAM_CANDIDATES if k in df_a.columns and k in df_b.columns]
        if not join_keys:
            raise ValueError("Could not infer any join keys. Pass --on fast,slow,...")

    # Collapse duplicates on both sides if needed
    df_a = _collapse_duplicates(df_a, join_keys, agg=args.agg)
    df_b = _collapse_duplicates(df_b, join_keys, agg=args.agg)

    # Basic counts
    print("====== Compare Sweeps ======")
    print(f"Keys: {join_keys}")
    print(f"Rows in {args.label_a}: {len(df_a)} | Rows in {args.label_b}: {len(df_b)}")

    # Rank by Sharpe (desc; 1 is best)
    if "sharpe" not in df_a.columns or "sharpe" not in df_b.columns:
        raise ValueError("Both CSVs must have a 'sharpe' column.")
    df_a = df_a.copy()
    df_b = df_b.copy()
    df_a["rank"] = df_a["sharpe"].rank(method="min", ascending=False)
    df_b["rank"] = df_b["sharpe"].rank(method="min", ascending=False)

    # Select columns to merge
    cols_keep = join_keys + [c for c in ["sharpe", "total_return", "cagr", "max_drawdown", "trades", "rank"] if c in df_a.columns]
    a_small = df_a[cols_keep].copy()
    b_small = df_b[cols_keep].copy()

    merged = a_small.merge(
        b_small,
        on=join_keys,
        how="outer",
        suffixes=(f"_{args.label_a}", f"_{args.label_b}"),
        indicator=True
    )

    matched = (merged["_merge"] == "both").sum()
    only_a = (merged["_merge"] == "left_only").sum()
    only_b = (merged["_merge"] == "right_only").sum()
    print(f"Matched: {matched} | Only in {args.label_a}: {only_a} | Only in {args.label_b}: {only_b}")

    # helper to build suffixed names
    def col(name: str, label: str) -> str:
        return f"{name}_{label}"

    # deltas
    if col("sharpe", args.label_a) in merged.columns and col("sharpe", args.label_b) in merged.columns:
        merged["sharpe_delta"] = merged[col("sharpe", args.label_b)] - merged[col("sharpe", args.label_a)]
    if col("rank", args.label_a) in merged.columns and col("rank", args.label_b) in merged.columns:
        merged["rank_delta"] = merged[col("rank", args.label_b)] - merged[col("rank", args.label_a)]

    both = merged[merged["_merge"] == "both"].copy()

    def _show(title: str, df: pd.DataFrame, sort_col: str, asc: bool, cols: List[str]):
        print(f"\n-- {title} --")
        view = df.sort_values(sort_col, ascending=asc).head(args.top)
        if len(view):
            print(view[cols].to_string(index=False))
        else:
            print("(no rows)")

    # Worst / Best Sharpe change
    if "sharpe_delta" in both.columns:
        base_cols = join_keys + [col("sharpe", args.label_a), col("sharpe", args.label_b), "sharpe_delta"]
        extra_cols = [c for c in [col("rank", args.label_a), col("rank", args.label_b), "rank_delta"] if c in both.columns]
        _show(f"Worst Sharpe change ({args.label_b} - {args.label_a})",
              both, "sharpe_delta", asc=True, cols=base_cols + extra_cols)
        _show(f"Best Sharpe change ({args.label_b} - {args.label_a})",
              both, "sharpe_delta", asc=False, cols=base_cols + extra_cols)

    # Biggest ranking drops
    if "rank_delta" in both.columns:
        base_cols = join_keys + [col("rank", args.label_a), col("rank", args.label_b), "rank_delta"]
        extra_cols = [c for c in [col("sharpe", args.label_a), col("sharpe", args.label_b), "sharpe_delta"] if c in both.columns]
        _show("Biggest ranking drops (positive rank_delta = worse in comparison)",
              both, "rank_delta", asc=False, cols=base_cols + extra_cols)

    if args.out_csv:
        # put main comparison columns first
        ordered = join_keys + [
            col("sharpe", args.label_a), col("sharpe", args.label_b), "sharpe_delta",
            col("rank", args.label_a), col("rank", args.label_b), "rank_delta",
            col("total_return", args.label_a), col("total_return", args.label_b),
            col("cagr", args.label_a) if col("cagr", args.label_a) in both.columns else None,
            col("cagr", args.label_b) if col("cagr", args.label_b) in both.columns else None,
            col("max_drawdown", args.label_a), col("max_drawdown", args.label_b)
        ]
        ordered = [c for c in ordered if c and c in both.columns]
        rest = [c for c in both.columns if c not in ordered and c != "_merge"]
        out_df = both[ordered + rest].copy()
        Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(args.out_csv, index=False)
        print(f"\nWrote full diff table to {Path(args.out_csv).resolve()}")

if __name__ == "__main__":
    main()
