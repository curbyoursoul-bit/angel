# scripts/pick_top_from_sweep.py
from __future__ import annotations

import argparse
from pathlib import Path
import math
import re
import sys
from typing import Dict, List

import pandas as pd

STRATS = ("ema_crossover", "bollinger_breakout", "vwap_mean_reversion",
          "orb_breakout", "volume_breakout")


def _bool(x) -> bool:
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "y", "t")


def _num(x):
    try:
        f = float(x)
        return int(f) if f.is_integer() else f
    except Exception:
        return x


def _nan_to_posinf(x: float) -> float:
    try:
        return float("inf") if (x is None or math.isnan(float(x))) else float(x)
    except Exception:
        return float("inf")


def _nan_to_neginf(x: float) -> float:
    try:
        return float("-inf") if (x is None or math.isnan(float(x))) else float(x)
    except Exception:
        return float("-inf")


def _sanitize(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def _mk_out_png(row: pd.Series, idx: int, default_prefix: str = "combo") -> str:
    strat = row.get("strategy", "") or ""
    tf = row.get("timeframe", "") or ""
    fill = row.get("fill", "next_open")
    sh = row.get("sharpe", float("nan"))
    # include key strategy params in filename
    parts = [strat or default_prefix]
    for k in ("fast", "slow", "bb_n", "bb_k", "vwap_n", "vwap_z", "orb_mins", "vol_n", "vol_k"):
        if k in row and pd.notna(row[k]):
            parts.append(f"{k}{row[k]}")
    if "atr_sl_mult" in row and pd.notna(row["atr_sl_mult"]):
        parts.append(f"sl{row['atr_sl_mult']}")
    if "atr_tp_mult" in row and pd.notna(row["atr_tp_mult"]):
        parts.append(f"tp{row['atr_tp_mult']}")
    if tf:
        parts.append(tf)
    parts.append(fill)
    if not pd.isna(sh):
        parts.append(f"S{float(sh):.3f}")
    parts.append(f"r{idx+1}")
    base = _sanitize("_".join(str(p) for p in parts if str(p)))
    return f"data/{base}.png"


def _build_cli_command(row: pd.Series,
                       csv_path: str,
                       timeframe: str | None,
                       strategy: str) -> str:
    # common flags (match scripts/run_backtest.py)
    cmd = [
        "python -m scripts.run_backtest",
        f"--csv {csv_path}",
    ]
    if timeframe:
        cmd.append(f"--timeframe {timeframe}")

    # fill/session/cooldown/fixed qty
    fill = (row.get("fill") or "next_open")
    cmd.append(f"--fill {fill}")

    session = row.get("session")
    if isinstance(session, str) and session.strip():
        cmd.append(f"--session {session.strip()}")

    cd = row.get("cooldown_bars")
    if pd.notna(cd) and int(cd) > 0:
        cmd.append(f"--cooldown_bars {int(cd)}")

    fq = row.get("fixed_qty")
    if pd.notna(fq):
        cmd.append(f"--fixed_qty {int(float(fq))}")

    # ATR exits (may be 0.0)
    for k_cli, k_csv in (("--atr_sl_mult", "atr_sl_mult"), ("--atr_tp_mult", "atr_tp_mult")):
        if k_csv in row and pd.notna(row[k_csv]):
            cmd.append(f"{k_cli} {float(row[k_csv])}")

    # costs
    fee = row.get("fee_bps", 0.0)
    slip = row.get("slip_bps", row.get("slippage_bps", 0.0))
    if pd.notna(fee) and float(fee) != 0.0:
        cmd.append(f"--fee_bps {float(fee)}")
    if pd.notna(slip) and float(slip) != 0.0:
        cmd.append(f"--slip_bps {float(slip)}")

    # allow_short
    if _bool(row.get("allow_short", False)):
        cmd.append("--allow_short")

    # strategy & its params
    cmd.append(f"--strategy {strategy}")

    def add_if_present(flag: str, key: str, cast=int | float):
        if key in row and pd.notna(row[key]):
            v = cast(row[key])
            cmd.append(f"{flag} {v}")

    if strategy == "ema_crossover":
        add_if_present("--fast", "fast", int)
        add_if_present("--slow", "slow", int)
    elif strategy == "bollinger_breakout":
        add_if_present("--bb_n", "bb_n", int)
        add_if_present("--bb_k", "bb_k", float)
    elif strategy == "vwap_mean_reversion":
        add_if_present("--vwap_n", "vwap_n", int)
        add_if_present("--vwap_z", "vwap_z", float)
    elif strategy == "orb_breakout":
        add_if_present("--orb_mins", "orb_mins", int)
    elif strategy == "volume_breakout":
        add_if_present("--vol_n", "vol_n", int)
        add_if_present("--vol_k", "vol_k", float)

    # unique output file
    out_png = _mk_out_png(row, idx=int(row.name))
    cmd.append(f"--out_png {out_png}")

    return " ".join(cmd)


def main():
    ap = argparse.ArgumentParser(
        description="Pick the top parameter sets from a sweep CSV and write a .bat with commands."
    )
    ap.add_argument("--csv", required=True, help="Sweep CSV from run_param_sweep")
    ap.add_argument("--strategy", required=True, choices=STRATS)
    ap.add_argument("--csv_path", required=True, help="Path to OHLCV CSV used for the sweep")
    ap.add_argument("--timeframe", default=None, help="e.g., 15min")
    ap.add_argument("--top", type=int, default=5)

    # filters
    ap.add_argument("--min_trades", type=int, default=0)
    ap.add_argument("--max_dd", type=float, default=1.0)       # as fraction, e.g. 0.55
    ap.add_argument("--min_sharpe", type=float, default=float("-inf"))
    ap.add_argument("--min_return", type=float, default=float("-inf"))

    # override trade env (optional; if omitted, use the ones from CSV rows)
    ap.add_argument("--fill", choices=["next_open", "close"], default=None)
    ap.add_argument("--allow_short", action="store_true")
    ap.add_argument("--session", default=None)
    ap.add_argument("--cooldown_bars", type=int, default=None)
    ap.add_argument("--fixed_qty", type=int, default=None)
    ap.add_argument("--fee_bps", type=float, default=None)
    ap.add_argument("--slip_bps", type=float, default=None)

    ap.add_argument("--out_bat", required=True, help="Output .bat path")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)

    # carry strategy name into rows (for filename building)
    df["strategy"] = args.strategy

    # sort by sharpe desc (NaN last)
    df = df.sort_values(by="sharpe", ascending=False, na_position="last").reset_index(drop=True)

    # filters
    def _meets(r) -> bool:
        tr = int(r.get("trades", r.get("num_trades", 0)))
        dd = _nan_to_posinf(r.get("max_drawdown"))
        sh = _nan_to_neginf(r.get("sharpe"))
        ret = _nan_to_neginf(r.get("total_return"))
        return (tr >= args.min_trades and
                dd <= args.max_dd and
                sh >= args.min_sharpe and
                ret >= args.min_return)

    filt = df[df.apply(_meets, axis=1)].copy()

    if len(filt) == 0:
        print("No rows after filter; relaxing constraints...\n")
        # fall back to just top-k by sharpe
        filt = df.copy()

    # apply overrides if provided
    for col, val in (
        ("fill", args.fill),
        ("session", args.session),
        ("cooldown_bars", args.cooldown_bars),
        ("fixed_qty", args.fixed_qty),
        ("fee_bps", args.fee_bps),
        ("slip_bps", args.slip_bps),
    ):
        if val is not None:
            filt[col] = val

    if args.allow_short:
        filt["allow_short"] = True

    topk = filt.head(args.top).copy()

    # pretty print small summary
    print("=== Top candidates ===")
    cols_show = ["sharpe", "total_return", "max_drawdown", "trades"]
    print(topk[cols_show].to_string(index=False))
    print("\n=== Commands ===")

    cmds: List[str] = []
    for i, row in topk.iterrows():
        cmd = _build_cli_command(row, csv_path=args.csv_path,
                                 timeframe=args.timeframe or (row.get("timeframe") if pd.notna(row.get("timeframe")) else None),
                                 strategy=args.strategy)
        cmds.append(cmd)
        print(cmd)

    out = Path(args.out_bat)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="\n") as f:
        for c in cmds:
            f.write(c + "\n")

    print(f"\nWrote {out.resolve()} with {len(cmds)} commands.")
    print("Tip: run it with\n  " + out.name)


if __name__ == "__main__":
    main()
