# scripts/run_backtest.py
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, Tuple, Any
import math
import json

import pandas as pd
import matplotlib.pyplot as plt

from backtest.runner import run_backtest

STRAT_KEYS: Dict[str, Tuple[str, ...]] = {
    "ema_crossover": ("fast", "slow"),
    "bollinger_breakout": ("bb_n", "bb_k"),
    "vwap_mean_reversion": ("vwap_n", "vwap_z"),
    "orb_breakout": ("orb_mins",),
    "volume_breakout": ("vol_n", "vol_k"),
}
STRATS = tuple(STRAT_KEYS.keys())

REQUIRED_KEYS: Dict[str, Tuple[str, ...]] = {
    "ema_crossover": ("fast", "slow"),
    "bollinger_breakout": ("bb_n", "bb_k"),
    "vwap_mean_reversion": ("vwap_n", "vwap_z"),
    "orb_breakout": ("orb_mins",),
    "volume_breakout": ("vol_n", "vol_k"),
}

def _add_common_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--csv", required=True, help="Path to OHLCV CSV")
    ap.add_argument("--strategy", required=True, choices=STRATS)
    ap.add_argument("--timeframe", default=None, help="e.g. 15min, 1H, day (resample)")
    ap.add_argument("--capital", type=float, default=100000.0)

    # allocation aliases
    ap.add_argument("--allocation", type=float, default=None, help="Alias for allocation_pct")
    ap.add_argument("--allocation_pct", type=float, default=1.0)

    # costs & engine
    ap.add_argument("--fee_bps", type=float, default=0.0)
    ap.add_argument("--slip_bps", type=float, default=0.0, help="Slippage bps (mapped to slippage_bps)")
    ap.add_argument("--allow_short", action="store_true")
    ap.add_argument("--fill", choices=("next_open", "close"), default="next_open")
    ap.add_argument("--fixed_qty", type=float, default=None)
    ap.add_argument("--atr_n", type=int, default=14)
    ap.add_argument("--atr_sl_mult", type=float, default=0.0)
    ap.add_argument("--atr_tp_mult", type=float, default=0.0)
    ap.add_argument("--session", default=None, help="HH:MM-HH:MM")
    ap.add_argument("--cooldown_bars", type=int, default=0)
    ap.add_argument("--out_png", default=None)

    # outputs for automation
    ap.add_argument("--print_json", action="store_true", help="Print a JSON summary to stdout")
    ap.add_argument("--out_csv_metrics", default=None,
                    help="Write a one-row CSV with metrics + key params")

    # strategy-specific inputs (we'll route into strategy_kwargs)
    ap.add_argument("--fast", type=int, default=None)
    ap.add_argument("--slow", type=int, default=None)
    ap.add_argument("--bb_n", type=int, default=None)
    ap.add_argument("--bb_k", type=float, default=None)
    ap.add_argument("--vwap_n", type=int, default=None)
    ap.add_argument("--vwap_z", type=float, default=None)
    ap.add_argument("--orb_mins", type=int, default=None)
    ap.add_argument("--vol_n", type=int, default=None)
    ap.add_argument("--vol_k", type=float, default=None)

def _fmt_num(x: Any, default: str = "—", fmt: str = ".4f") -> str:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return default
        return f"{float(x):{fmt}}"
    except Exception:
        return default

def _fmt_pct(x: Any, default: str = "—") -> str:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return default
        return f"{float(x)*100:.2f}%"
    except Exception:
        return default

def _coerce_result(res):
    """Support dict (new) or dataclass-like (old). Return (eq, trades, metrics)."""
    if isinstance(res, dict):
        eq = res.get("equity", pd.Series(dtype="float64"))
        trades = int(res.get("trades", 0))
        metrics = dict(res.get("metrics", {}))
    else:
        eq = getattr(res, "equity", pd.Series(dtype="float64"))
        trades = int(getattr(res, "trades", 0))
        metrics = dict(getattr(res, "metrics", {}))

    # Normalize equity to a pd.Series
    if not isinstance(eq, pd.Series):
        try:
            eq = pd.Series(eq)
        except Exception:
            eq = pd.Series(dtype="float64")
    return eq, trades, metrics

def _build_strategy_kwargs(args: argparse.Namespace) -> dict:
    keys = STRAT_KEYS[args.strategy]
    return {k: getattr(args, k) for k in keys if getattr(args, k) is not None}

def _validate_required(args: argparse.Namespace) -> None:
    missing = [k for k in REQUIRED_KEYS[args.strategy] if getattr(args, k) is None]
    if missing:
        pretty = ", ".join(f"--{m}" for m in missing)
        raise SystemExit(f"[error] Missing required params for {args.strategy}: {pretty}")

def _summary_dict(args: argparse.Namespace, strat_kwargs: dict, trades: int, m: dict) -> dict:
    return {
        "strategy": args.strategy,
        "timeframe": args.timeframe,
        "fill": args.fill,
        "allow_short": bool(args.allow_short),
        "session": args.session or "",
        "cooldown_bars": int(args.cooldown_bars or 0),
        "fixed_qty": args.fixed_qty,
        "atr_n": args.atr_n,
        "atr_sl_mult": args.atr_sl_mult,
        "atr_tp_mult": args.atr_tp_mult,
        "fee_bps": args.fee_bps,
        "slip_bps": args.slip_bps,
        **strat_kwargs,
        # metrics
        "trades": int(m.get("trades", m.get("num_trades", trades) or trades)),
        "total_return": m.get("total_return"),
        "cagr": m.get("cagr"),
        "max_drawdown": m.get("max_drawdown"),
        "sharpe": m.get("sharpe"),
    }

def main():
    ap = argparse.ArgumentParser(description="Run a backtest and (optionally) save an equity plot.")
    _add_common_args(ap)
    args = ap.parse_args()

    _validate_required(args)
    strat_kwargs = _build_strategy_kwargs(args)

    # map allocation alias & slippage name
    allocation_pct = args.allocation if args.allocation is not None else args.allocation_pct
    slippage_bps = args.slip_bps

    runner_kwargs = dict(
        csv_path=args.csv,
        strategy=args.strategy,
        strategy_kwargs=strat_kwargs,    # only strategy-specific keys
        timeframe=args.timeframe,
        capital=args.capital,
        allocation_pct=allocation_pct,
        fee_bps=args.fee_bps,
        slippage_bps=slippage_bps,
        allow_short=args.allow_short,
        fill=args.fill,
        fixed_qty=args.fixed_qty,
        atr_n=args.atr_n,
        atr_sl_mult=args.atr_sl_mult,
        atr_tp_mult=args.atr_tp_mult,
        session=args.session,
        cooldown_bars=args.cooldown_bars,
    )

    res = run_backtest(**runner_kwargs)
    eq, trades, m = _coerce_result(res)

    print("====== Backtest Summary ======")
    print(f"Trades:          {int(m.get('trades', trades) or trades)}")
    print(f"total_return    : {_fmt_num(m.get('total_return'), '—', '.4f')}")
    print(f"cagr            : {_fmt_num(m.get('cagr'), '—', '.4f')}")
    print(f"max_drawdown    : {_fmt_pct(m.get('max_drawdown'))}")
    print(f"sharpe          : {_fmt_num(m.get('sharpe'), '—', '.4f')}")

    # Optional JSON summary to stdout (for automation/piping)
    if args.print_json:
        summary = _summary_dict(args, strat_kwargs, trades, m)
        print(json.dumps(summary, separators=(",", ":"), ensure_ascii=False))

    # Optional one-row CSV of metrics + key params
    if args.out_csv_metrics:
        summary = _summary_dict(args, strat_kwargs, trades, m)
        outp = Path(args.out_csv_metrics)
        outp.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([summary]).to_csv(outp, index=False)
        print(f"Saved metrics CSV: {outp.resolve()}")

    # Optional plot
    if args.out_png:
        try:
            out = Path(args.out_png)
            out.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(eq, pd.Series) and len(eq) > 0:
                # sort index for sanity; if not sortable, ignore
                try:
                    eq = eq.sort_index()
                except Exception:
                    pass
                plt.figure(figsize=(10, 4))
                eq.plot()
                plt.title(f"{args.strategy} | equity")
                plt.xlabel("timestamp")
                plt.ylabel("equity")
                plt.tight_layout()
                plt.savefig(out, dpi=120)
                plt.close()
                print(f"Saved plot: {out.resolve()}")
            else:
                print("[warn] No equity series to plot.")
        except Exception as e:
            print(f"[warn] plot failed: {e}")

if __name__ == "__main__":
    main()
