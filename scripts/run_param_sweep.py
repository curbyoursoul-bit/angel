# scripts/run_param_sweep.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple

import pandas as pd

from backtest.runner import run_backtest


def _num(x: float) -> float:
    """Normalize tiny float formatting."""
    if isinstance(x, (int,)) or (isinstance(x, float) and float(x).is_integer()):
        return int(x)
    return float(x)


def _parse_grid(spec: str, as_int: bool = True) -> List[float]:
    """
    Parse a grid spec into a list of numbers.
    Supports:
      - comma list: "5,7,9"
      - range with step: "5:15:2"  (5,7,9,11,13,15)
      - range default step=1: "5:10"
    """
    out: List[float] = []
    for part in (p.strip() for p in spec.split(",")):
        if not part:
            continue
        if ":" in part:
            bits = [b.strip() for b in part.split(":")]
            if len(bits) == 2:
                a, b = float(bits[0]), float(bits[1])
                step = 1.0
            elif len(bits) == 3:
                a, b, step = float(bits[0]), float(bits[1]), float(bits[2])
                if step == 0:
                    raise ValueError("step cannot be 0 in range spec")
            else:
                raise ValueError(f"Bad range: {part}")
            cur = a
            # inclusive upper bound
            while cur <= b + 1e-12:
                out.append(_num(cur))
                cur += step
        else:
            v = float(part)
            out.append(_num(v))
    # dedup while preserving order
    seen = set()
    dedup: List[float] = []
    for v in out:
        if v in seen:
            continue
        seen.add(v)
        dedup.append(int(v) if as_int and float(v).is_integer() else float(v))
    return dedup


def _coerce_result(res: Any) -> Tuple[Dict[str, float], int]:
    """
    Accept RunResult(dataclass) or dict.
    Return (metrics_dict, trades_int).
    """
    if isinstance(res, dict):
        metrics = dict(res.get("metrics", {}))
        trades = int(res.get("trades", metrics.get("trades", metrics.get("num_trades", 0))))
        return metrics, trades
    # dataclass fallback
    metrics = dict(getattr(res, "metrics", {}))
    trades = int(getattr(res, "trades", metrics.get("trades", metrics.get("num_trades", 0))))
    return metrics, trades


def _append_row(rows: List[Dict[str, Any]], params: Dict[str, Any], result: Any, strategy: str) -> None:
    m, trades_from_res = _coerce_result(result)
    rows.append({
        # metrics
        "sharpe": float(m.get("sharpe", float("nan"))),
        "total_return": float(m.get("total_return", float("nan"))),
        "cagr": float(m.get("cagr", float("nan"))),
        "max_drawdown": float(m.get("max_drawdown", float("nan"))),
        "trades": int(m.get("trades", m.get("num_trades", trades_from_res))),
        # always include strategy for downstream tools
        "strategy": strategy,
        # common params
        "timeframe": params.get("timeframe") or "",
        "fill": params.get("fill") or "next_open",
        "allow_short": bool(params.get("allow_short", False)),
        "session": params.get("session") or "",
        "cooldown_bars": int(params.get("cooldown_bars", 0)),
        "fixed_qty": _num(params.get("fixed_qty", 1)),
        "atr_n": _num(params.get("atr_n", 14)),
        "atr_sl_mult": float(params.get("atr_sl_mult", 1.0)),
        "atr_tp_mult": float(params.get("atr_tp_mult", 1.5)),
        "fee_bps": float(params.get("fee_bps", 0.0)),
        "slip_bps": float(params.get("slip_bps", 0.0)),
        # strategy params
        **{k: v for k, v in params.items() if k in (
            "fast", "slow", "bb_n", "bb_k", "vwap_n", "vwap_z", "orb_mins", "vol_n", "vol_k"
        )}
    })


def _count_combos(strategy: str, grids: Dict[str, List]) -> int:
    # product of relevant grid lengths
    if strategy == "ema_crossover":
        keys = ("ema_fast", "ema_slow", "atr_sl_mults", "atr_tp_mults")
    elif strategy == "bollinger_breakout":
        keys = ("bb_n", "bb_k", "atr_sl_mults", "atr_tp_mults")
    elif strategy == "vwap_mean_reversion":
        keys = ("vwap_n", "vwap_z", "atr_sl_mults", "atr_tp_mults")
    elif strategy == "orb_breakout":
        keys = ("orb_mins", "atr_sl_mults", "atr_tp_mults")
    elif strategy == "volume_breakout":
        keys = ("vol_n", "vol_k", "atr_sl_mults", "atr_tp_mults")
    else:
        return 0
    n = 1
    for k in keys:
        n *= max(1, len(grids.get(k, [])))
    return n


def main():
    ap = argparse.ArgumentParser(
        description="Sweep strategy hyperparameters and save a CSV ranked by Sharpe."
    )
    ap.add_argument("--csv_path", required=True, help="Input OHLCV CSV")
    ap.add_argument("--strategy",
                    required=True,
                    choices=["ema_crossover", "bollinger_breakout", "vwap_mean_reversion", "orb_breakout", "volume_breakout"])
    ap.add_argument("--timeframe", default=None, help="e.g. 15min (optional)")

    ap.add_argument("--out_csv", required=True, help="Output CSV path")
    ap.add_argument("--resume", action="store_true", help="If out_csv exists, skip already-evaluated combos.")
    ap.add_argument("--checkpoint_every", type=int, default=0,
                    help="Write/merge partial results every N new rows (0=off).")
    ap.add_argument("--progress_every", type=int, default=25,
                    help="Print a progress heartbeat every N evaluations.")

    # common trade args
    ap.add_argument("--fixed_qty", type=float, default=1)
    ap.add_argument("--atr_n", type=int, default=14)
    ap.add_argument("--atr_sl_mults", default="1.0", help="grid, e.g. 0.8,1.0,1.2")
    ap.add_argument("--atr_tp_mults", default="1.5", help="grid, e.g. 1.5,2.0")

    ap.add_argument("--allow_short", action="store_true")
    ap.add_argument("--fill", choices=["next_open", "close"], default="next_open")
    ap.add_argument("--session", default="", help="e.g. 10:00-14:45")
    ap.add_argument("--cooldown_bars", type=int, default=0)

    # transaction cost knobs (pass-through to runner)
    ap.add_argument("--fee_bps", type=float, default=0.0)
    ap.add_argument("--slip_bps", type=float, default=0.0)

    # strategy-specific grids
    ap.add_argument("--ema_fast", default="9", help="e.g. 5:15:2 or 5,7,9,11")
    ap.add_argument("--ema_slow", default="21", help="e.g. 21:55:2 or 21,34,55,89")

    ap.add_argument("--bb_n", default="20", help="e.g. 14,20,30")
    ap.add_argument("--bb_k", default="2.0", help="e.g. 1.6,2.0,2.4")

    ap.add_argument("--vwap_n", default="30", help="e.g. 20,30,40")
    ap.add_argument("--vwap_z", default="1.5", help="e.g. 1.2,1.5,2.0")

    ap.add_argument("--orb_mins", default="30", help="e.g. 15,30,45")

    ap.add_argument("--vol_n", default="20", help="e.g. 10,20,30")
    ap.add_argument("--vol_k", default="2.0", help="e.g. 1.5,2.0,2.5")

    args = ap.parse_args()

    rows: List[Dict[str, Any]] = []

    # Build common base params once
    base_params = dict(
        csv_path=args.csv_path,
        timeframe=args.timeframe,
        fill=args.fill,
        allow_short=args.allow_short,
        fixed_qty=args.fixed_qty,
        atr_n=args.atr_n,
        session=args.session or None,
        cooldown_bars=args.cooldown_bars,
        fee_bps=args.fee_bps,
        slip_bps=args.slip_bps,
    )

    # Build grids
    atr_sl_grid = [_num(x) for x in _parse_grid(args.atr_sl_mults, as_int=False)]
    atr_tp_grid = [_num(x) for x in _parse_grid(args.atr_tp_mults, as_int=False)]

    # Resume support: pre-load existing CSV to avoid re-running
    out_path = Path(args.out_csv)
    existing = pd.DataFrame()
    if args.resume and out_path.exists():
        existing = pd.read_csv(out_path)
        # SAFE coercion for allow_short (avoid astype(bool) on strings)
        if "allow_short" in existing.columns:
            existing["allow_short"] = (
                existing["allow_short"]
                .astype(str)
                .str.lower()
                .map({"true": True, "false": False, "1": True, "0": False})
                .fillna(False)
                .astype(bool)
            )
        print(f"[resume] loaded {len(existing)} rows from {out_path}")

    # keys that uniquely identify a sweep row
    common_keys = ["strategy", "timeframe", "fill", "allow_short", "session",
                   "cooldown_bars", "fixed_qty", "atr_n",
                   "atr_sl_mult", "atr_tp_mult", "fee_bps", "slip_bps"]
    strat_keys_map = {
        "ema_crossover": ["fast", "slow"],
        "bollinger_breakout": ["bb_n", "bb_k"],
        "vwap_mean_reversion": ["vwap_n", "vwap_z"],
        "orb_breakout": ["orb_mins"],
        "volume_breakout": ["vol_n", "vol_k"],
    }
    join_keys = common_keys + strat_keys_map[args.strategy]
    done: set[Tuple[Any, ...]] = set()
    if len(existing):
        for k in join_keys:
            if k not in existing.columns:
                existing[k] = None
        done = set(map(tuple, existing[join_keys].to_records(index=False)))

    # For progress info
    grids_for_count = {
        "ema_fast": _parse_grid(args.ema_fast),
        "ema_slow": _parse_grid(args.ema_slow),
        "bb_n": _parse_grid(args.bb_n),
        "bb_k": _parse_grid(args.bb_k, as_int=False),
        "vwap_n": _parse_grid(args.vwap_n),
        "vwap_z": _parse_grid(args.vwap_z, as_int=False),
        "orb_mins": _parse_grid(args.orb_mins),
        "vol_n": _parse_grid(args.vol_n),
        "vol_k": _parse_grid(args.vol_k, as_int=False),
        "atr_sl_mults": atr_sl_grid,
        "atr_tp_mults": atr_tp_grid,
    }
    total = _count_combos(args.strategy, grids_for_count)
    skipped = 0
    evaluated = 0

    def _maybe_checkpoint(force: bool = False):
        """Optionally merge & write partial results to disk."""
        nonlocal existing, done  # <-- important: declare before using

        if not rows:
            return
        if not force and args.checkpoint_every <= 0:
            return
        if not force and (evaluated % args.checkpoint_every != 0):
            return

        df_new = pd.DataFrame(rows)
        tmp = existing
        if len(tmp):
            all_rows = pd.concat([tmp, df_new], ignore_index=True)
        else:
            all_rows = df_new
        # ensure columns exist for dedup keys
        for k in join_keys:
            if k not in all_rows.columns:
                all_rows[k] = None
        # rank by sharpe (desc) and drop dups on keys
        if "sharpe" in all_rows.columns:
            all_rows = all_rows.sort_values("sharpe", ascending=False, kind="mergesort")
        all_rows = all_rows.drop_duplicates(subset=join_keys, keep="first")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        all_rows.to_csv(out_path, index=False)
        print(f"[checkpoint] wrote {len(all_rows)} rows to {out_path}")
        # update resume state & clear buffer
        existing = all_rows
        done = set(map(tuple, existing[join_keys].to_records(index=False)))
        rows.clear()

    try:
        if args.strategy == "ema_crossover":
            fast_grid = [int(x) for x in _parse_grid(args.ema_fast)]
            slow_grid = [int(x) for x in _parse_grid(args.ema_slow)]
            for fast in fast_grid:
                for slow in slow_grid:
                    if fast >= slow:
                        continue
                    for sl in atr_sl_grid:
                        for tp in atr_tp_grid:
                            p = dict(base_params,
                                     strategy=args.strategy,
                                     fast=fast, slow=slow,
                                     atr_sl_mult=sl, atr_tp_mult=tp)
                            key_tuple = tuple(p.get(k, None) for k in join_keys)
                            if key_tuple in done:
                                skipped += 1
                                continue
                            strat_kwargs = {"fast": fast, "slow": slow}
                            rk = dict(
                                csv_path=p["csv_path"],
                                strategy=args.strategy,
                                strategy_kwargs=strat_kwargs,
                                timeframe=p["timeframe"],
                                fee_bps=p["fee_bps"],
                                slippage_bps=p["slip_bps"],
                                allow_short=p["allow_short"],
                                fill=p["fill"],
                                fixed_qty=p["fixed_qty"],
                                atr_n=p["atr_n"],
                                atr_sl_mult=p["atr_sl_mult"],
                                atr_tp_mult=p["atr_tp_mult"],
                                session=p["session"],
                                cooldown_bars=p["cooldown_bars"],
                            )
                            res = run_backtest(**rk)
                            _append_row(rows, p, res, args.strategy)
                            evaluated += 1
                            if args.progress_every and evaluated % args.progress_every == 0:
                                print(f"[progress] evaluated={evaluated} skipped={skipped} total~{total}")
                            _maybe_checkpoint()

        elif args.strategy == "bollinger_breakout":
            n_grid = [int(x) for x in _parse_grid(args.bb_n)]
            k_grid = [float(x) for x in _parse_grid(args.bb_k, as_int=False)]
            for n in n_grid:
                for k in k_grid:
                    for sl in atr_sl_grid:
                        for tp in atr_tp_grid:
                            p = dict(base_params, strategy=args.strategy,
                                     bb_n=n, bb_k=k,
                                     atr_sl_mult=sl, atr_tp_mult=tp)
                            key_tuple = tuple(p.get(k2, None) for k2 in join_keys)
                            if key_tuple in done:
                                skipped += 1
                                continue
                            strat_kwargs = {"bb_n": n, "bb_k": k}
                            rk = dict(
                                csv_path=p["csv_path"],
                                strategy=args.strategy,
                                strategy_kwargs=strat_kwargs,
                                timeframe=p["timeframe"],
                                fee_bps=p["fee_bps"],
                                slippage_bps=p["slip_bps"],
                                allow_short=p["allow_short"],
                                fill=p["fill"],
                                fixed_qty=p["fixed_qty"],
                                atr_n=p["atr_n"],
                                atr_sl_mult=p["atr_sl_mult"],
                                atr_tp_mult=p["atr_tp_mult"],
                                session=p["session"],
                                cooldown_bars=p["cooldown_bars"],
                            )
                            res = run_backtest(**rk)
                            _append_row(rows, p, res, args.strategy)
                            evaluated += 1
                            if args.progress_every and evaluated % args.progress_every == 0:
                                print(f"[progress] evaluated={evaluated} skipped={skipped} total~{total}")
                            _maybe_checkpoint()

        elif args.strategy == "vwap_mean_reversion":
            n_grid = [int(x) for x in _parse_grid(args.vwap_n)]
            z_grid = [float(x) for x in _parse_grid(args.vwap_z, as_int=False)]
            for n in n_grid:
                for z in z_grid:
                    for sl in atr_sl_grid:
                        for tp in atr_tp_grid:
                            p = dict(base_params, strategy=args.strategy,
                                     vwap_n=n, vwap_z=z,
                                     atr_sl_mult=sl, atr_tp_mult=tp)
                            key_tuple = tuple(p.get(k2, None) for k2 in join_keys)
                            if key_tuple in done:
                                skipped += 1
                                continue
                            strat_kwargs = {"vwap_n": n, "vwap_z": z}
                            rk = dict(
                                csv_path=p["csv_path"],
                                strategy=args.strategy,
                                strategy_kwargs=strat_kwargs,
                                timeframe=p["timeframe"],
                                fee_bps=p["fee_bps"],
                                slippage_bps=p["slip_bps"],
                                allow_short=p["allow_short"],
                                fill=p["fill"],
                                fixed_qty=p["fixed_qty"],
                                atr_n=p["atr_n"],
                                atr_sl_mult=p["atr_sl_mult"],
                                atr_tp_mult=p["atr_tp_mult"],
                                session=p["session"],
                                cooldown_bars=p["cooldown_bars"],
                            )
                            res = run_backtest(**rk)
                            _append_row(rows, p, res, args.strategy)
                            evaluated += 1
                            if args.progress_every and evaluated % args.progress_every == 0:
                                print(f"[progress] evaluated={evaluated} skipped={skipped} total~{total}")
                            _maybe_checkpoint()

        elif args.strategy == "orb_breakout":
            o_grid = [int(x) for x in _parse_grid(args.orb_mins)]
            for om in o_grid:
                for sl in atr_sl_grid:
                    for tp in atr_tp_grid:
                        p = dict(base_params, strategy=args.strategy,
                                 orb_mins=om,
                                 atr_sl_mult=sl, atr_tp_mult=tp)
                        key_tuple = tuple(p.get(k2, None) for k2 in join_keys)
                        if key_tuple in done:
                            skipped += 1
                            continue
                        strat_kwargs = {"orb_mins": om}
                        rk = dict(
                            csv_path=p["csv_path"],
                            strategy=args.strategy,
                            strategy_kwargs=strat_kwargs,
                            timeframe=p["timeframe"],
                            fee_bps=p["fee_bps"],
                            slippage_bps=p["slip_bps"],
                            allow_short=p["allow_short"],
                            fill=p["fill"],
                            fixed_qty=p["fixed_qty"],
                            atr_n=p["atr_n"],
                            atr_sl_mult=p["atr_sl_mult"],
                            atr_tp_mult=p["atr_tp_mult"],
                            session=p["session"],
                            cooldown_bars=p["cooldown_bars"],
                        )
                        res = run_backtest(**rk)
                        _append_row(rows, p, res, args.strategy)
                        evaluated += 1
                        if args.progress_every and evaluated % args.progress_every == 0:
                            print(f"[progress] evaluated={evaluated} skipped={skipped} total~{total}")
                        _maybe_checkpoint()

        elif args.strategy == "volume_breakout":
            n_grid = [int(x) for x in _parse_grid(args.vol_n)]
            k_grid = [float(x) for x in _parse_grid(args.vol_k, as_int=False)]
            for n in n_grid:
                for k in k_grid:
                    for sl in atr_sl_grid:
                        for tp in atr_tp_grid:
                            p = dict(base_params, strategy=args.strategy,
                                     vol_n=n, vol_k=k,
                                     atr_sl_mult=sl, atr_tp_mult=tp)
                            key_tuple = tuple(p.get(k2, None) for k2 in join_keys)
                            if key_tuple in done:
                                skipped += 1
                                continue
                            strat_kwargs = {"vol_n": n, "vol_k": k}
                            rk = dict(
                                csv_path=p["csv_path"],
                                strategy=args.strategy,
                                strategy_kwargs=strat_kwargs,
                                timeframe=p["timeframe"],
                                fee_bps=p["fee_bps"],
                                slippage_bps=p["slip_bps"],
                                allow_short=p["allow_short"],
                                fill=p["fill"],
                                fixed_qty=p["fixed_qty"],
                                atr_n=p["atr_n"],
                                atr_sl_mult=p["atr_sl_mult"],
                                atr_tp_mult=p["atr_tp_mult"],
                                session=p["session"],
                                cooldown_bars=p["cooldown_bars"],
                            )
                            res = run_backtest(**rk)
                            _append_row(rows, p, res, args.strategy)
                            evaluated += 1
                            if args.progress_every and evaluated % args.progress_every == 0:
                                print(f"[progress] evaluated={evaluated} skipped={skipped} total~{total}")
                            _maybe_checkpoint()

    except KeyboardInterrupt:
        print("\n[abort] Stopped by user.", file=sys.stderr)

    # Final write (and sort by Sharpe desc)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_new = pd.DataFrame(rows)
    if len(df_new):
        df_new = df_new.sort_values("sharpe", ascending=False, kind="mergesort")

    if len(existing):
        all_rows = pd.concat([existing, df_new], ignore_index=True)
        # ensure columns exist for dedup keys
        for k in join_keys:
            if k not in all_rows.columns:
                all_rows[k] = None
        all_rows = all_rows.drop_duplicates(subset=join_keys, keep="first")
    else:
        all_rows = df_new

    if len(all_rows):
        col_order = [c for c in [
            "sharpe", "total_return", "cagr", "max_drawdown", "trades",
            "strategy", "timeframe", "fill", "allow_short", "session",
            "cooldown_bars", "fixed_qty", "atr_n", "atr_sl_mult", "atr_tp_mult",
            "fee_bps", "slip_bps",
            "fast", "slow", "bb_n", "bb_k", "vwap_n", "vwap_z", "orb_mins", "vol_n", "vol_k"
        ] if c in all_rows.columns] + [c for c in all_rows.columns if c not in {
            "sharpe","total_return","cagr","max_drawdown","trades","strategy",
            "timeframe","fill","allow_short","session","cooldown_bars","fixed_qty",
            "atr_n","atr_sl_mult","atr_tp_mult","fee_bps","slip_bps",
            "fast","slow","bb_n","bb_k","vwap_n","vwap_z","orb_mins","vol_n","vol_k"}]
        all_rows = all_rows[col_order]
        all_rows.to_csv(out_path, index=False)
        print(f"Wrote {out_path.resolve()} with {len(all_rows)} rows ranked by Sharpe.")
    else:
        print("No rows produced; check inputs.", file=sys.stderr)


if __name__ == "__main__":
    main()
