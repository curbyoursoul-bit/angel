# core/strategy_runner.py
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Any, List, Dict

from loguru import logger
from dotenv import load_dotenv

from core.login import restore_or_login
from core.strategy_registry import get_strategy_callable
# --- broker (fallbacks if module not present) ---
try:
    from core.broker import place_batch, preview
except Exception:
    def preview(o: dict) -> dict:
        # minimal normalizer used in dry-run fallback
        return dict(o)
    def place_batch(_s: Any, orders: List[dict], mode: str = "continue", dry_run: bool = False) -> dict:
        logger.warning("[fallback] core.broker.place_batch missing. Printing orders instead.")
        for od in orders:
            logger.info(f"[{mode}][{'DRY' if dry_run else 'LIVE'}] Would place: {od}")
        return {"overall": "dryrun" if dry_run else "success", "results": [{"status": "success", "normalized": od} for od in orders]}

# --- risk (fallback to no-ops if module not present) ---
try:
    from core.risk import pre_trade_guards, enforce_kill_switch
except Exception:
    def pre_trade_guards(_s: Any, _orders: List[dict]) -> None:
        logger.warning("[fallback] core.risk.pre_trade_guards missing; skipping risk caps.")
    def enforce_kill_switch(_s: Any) -> None:
        # no-op kill switch
        return

# --- market hours (defensive import) ---
try:
    from utils.market_hours import is_market_open, now_ist
except Exception:
    def is_market_open() -> bool:
        logger.warning("[fallback] is_market_open unavailable; assuming OPEN.")
        return True
    def now_ist():
        from datetime import datetime, timezone, timedelta
        return datetime.now(timezone(timedelta(hours=5, minutes=30)))

# Load .env for strategies that read config from env vars
load_dotenv()

# ensure logs directory exists to avoid logger crash on first run elsewhere
Path("logs").mkdir(parents=True, exist_ok=True)


def build_cli():
    p = argparse.ArgumentParser(description="All-in-one Strategy Runner")
    p.add_argument(
        "--strategies",
        "-s",
        required=True,
        help="Comma-separated list, e.g. atm_straddle,bollinger_breakout",
    )

    # execution mode
    p.add_argument("--live", action="store_true", help="Place live orders (default: dry-run)")
    p.add_argument("--rollback", action="store_true", help="Rollback earlier legs if any later leg fails")
    p.add_argument("--only-if-market-open", action="store_true", help="Skip if market is closed")
    p.add_argument("--force-open", action="store_true", help="Ignore market-hours check (useful off-hours)")
    p.add_argument("--amo", action="store_true", help="Place all orders as AMO (adds amo='YES')")
    p.add_argument("--max-strategies", type=int, default=50, help="Safety limit")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON result")
    p.add_argument("--tag", help="Custom order tag to stamp on all legs (default auto: RUN_YYYYMMDD_HHMMSS)")

    # ---- Global strategy tuning via CLI -> env (affects math strategies) ----
    p.add_argument("--symbols", help="Comma list of NSE symbols, e.g. RELIANCE,TCS,INFY")
    p.add_argument("--interval", help="Candle interval, e.g. FIVE_MINUTE,FIFTEEN_MINUTE")
    p.add_argument("--bars", type=int, help="How many candles to fetch")
    p.add_argument("--qty", type=int, help="Order quantity per signal")
    p.add_argument("--product", help="INTRADAY/CNC/MIS etc")
    p.add_argument("--ordertype", help="MARKET/LIMIT")

    # Specific strategy knobs
    p.add_argument("--ema-fast", type=int)
    p.add_argument("--ema-slow", type=int)
    p.add_argument("--bb-n", type=int)
    p.add_argument("--bb-k", type=float)
    p.add_argument("--z-lookback", type=int)
    p.add_argument("--z-entry", type=float)

    return p


def _apply_env_overrides(args):
    """Map CLI args to env vars that strategies read."""
    setenv = lambda k, v: os.environ.__setitem__(k, str(v))
    if args.symbols:
        setenv("STRAT_SYMBOLS", args.symbols)
    if args.interval:
        setenv("STRAT_INTERVAL", args.interval)
    if args.bars is not None:
        setenv("STRAT_BARS", args.bars)
    if args.qty is not None:
        setenv("STRAT_QTY", args.qty)
    if args.product:
        setenv("STRAT_PRODUCT", args.product)
    if args.ordertype:
        setenv("STRAT_ORDERTYPE", args.ordertype)

    if args.ema_fast is not None:
        setenv("STRAT_EMA_FAST", args.ema_fast)
    if args.ema_slow is not None:
        setenv("STRAT_EMA_SLOW", args.ema_slow)
    if args.bb_n is not None:
        setenv("STRAT_BB_N", args.bb_n)
    if args.bb_k is not None:
        setenv("STRAT_BB_K", args.bb_k)
    if args.z_lookback is not None:
        setenv("STRAT_Z_LOOKBACK", args.z_lookback)
    if args.z_entry is not None:
        setenv("STRAT_Z_ENTRY", args.z_entry)


def _normalize_strategy_output(ret: Any) -> List[dict]:
    """
    Accepts None, dict, list[dict], or weird inputs and normalizes to list[dict].
    Prevents crashes from strategies that return a single dict or nothing.
    """
    if ret is None:
        return []
    if isinstance(ret, dict):
        return [ret]
    if isinstance(ret, list):
        out: List[dict] = []
        for item in ret:
            if isinstance(item, dict):
                out.append(item)
        return out
    logger.warning(f"Unexpected strategy return type: {type(ret).__name__}; ignoring.")
    return []


def main(argv=None):
    args = build_cli().parse_args(argv)

    # market-hours gate (defensive)
    try:
        if args.only_if_market_open and not args.force_open and not is_market_open():
            logger.warning(f"Market closed at {now_ist().strftime('%Y-%m-%d %H:%M:%S')} IST. Skipping.")
            return 0
    except Exception as e:
        logger.warning(f"Market-hours check failed, continuing: {e}")

    # parse strategy names
    names = [x.strip() for x in args.strategies.split(",") if x.strip()]
    if not names:
        logger.error("No strategies specified.")
        return 2
    if len(names) > args.max_strategies:
        logger.error(f"Too many strategies ({len(names)}). Max allowed: {args.max_strategies}")
        return 2

    # env overrides for strategies
    _apply_env_overrides(args)

    # Run-wide tag (used for ordertag on every leg)
    run_tag = args.tag or f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    mode_bits = ["LIVE" if args.live else "DRY-RUN"]
    if args.amo:
        mode_bits.append("AMO")
    if args.force_open:
        mode_bits.append("FORCE-OPEN")
    mode_bits.append(f"TAG={run_tag}")
    logger.info(f"Running strategies: {', '.join(names)} | mode={' | '.join(mode_bits)}")

    s = restore_or_login()
    overall: Dict[str, Any] = {"mode": "LIVE" if args.live else "DRYRUN", "tag": run_tag, "runs": []}

    for name in names:
        try:
            fn = get_strategy_callable(name)
        except KeyError as e:
            logger.error(str(e))
            overall["runs"].append({"strategy": name, "status": "unknown"})
            continue

        logger.info(f"[{name}] building orders …")
        try:
            raw = fn(s)  # each returns List[Dict] flat Angel order dicts (or None/dict)
            orders = _normalize_strategy_output(raw)

            # No signal is not an error
            if len(orders) == 0:
                result = {"strategy": name, "status": "no_signal", "orders": []}
                overall["runs"].append(result)
                logger.success(f"[{name}] {result['status']}")
                continue

            # Optional AMO transform
            if args.amo:
                for o in orders:
                    if isinstance(o, dict) and "amo" not in o:
                        o["amo"] = "YES"

            # Stamp a common ordertag if not already present
            for o in orders:
                if isinstance(o, dict):
                    o.setdefault("ordertag", run_tag)

            # Pre-trade checks only when we actually have orders
            pre_trade_guards(s, orders)

            if not args.live:
                normalized = [preview(o) for o in orders]
                result = {"strategy": name, "status": "dryrun", "orders": normalized}
            else:
                batch = place_batch(
                    s,
                    orders,
                    mode="rollback" if args.rollback else "continue",
                    dry_run=False,
                )
                result = {"strategy": name, "status": batch.get("overall"), "batch": batch}

                # Persist successfully placed legs for this tag
                try:
                    placed = []
                    for r in (batch.get("results") or []):
                        if r.get("status") == "success":
                            norm = r.get("normalized") or {}
                            placed.append({
                                "tradingsymbol": norm.get("tradingsymbol"),
                                "symboltoken": norm.get("symboltoken"),
                                "exchange": (norm.get("exchange") or "NSE").upper(),
                                "producttype": (norm.get("producttype") or "INTRADAY").upper(),
                                "transactiontype": (norm.get("transactiontype") or "").upper(),
                                "quantity": int(norm.get("quantity") or 0),
                                "ordertype": (norm.get("ordertype") or "MARKET").upper(),
                                "duration": (norm.get("duration") or "DAY").upper(),
                                "ordertag": norm.get("ordertag") or run_tag,
                            })
                    if placed:
                        tag_dir = Path("data") / "tags"
                        tag_dir.mkdir(parents=True, exist_ok=True)
                        tag_file = tag_dir / f"{run_tag}.json"
                        # append or create
                        existing = []
                        if tag_file.exists():
                            try:
                                existing = json.loads(tag_file.read_text() or "[]")
                            except Exception:
                                existing = []
                        existing.extend(placed)
                        tag_file.write_text(json.dumps(existing, indent=2))
                        logger.info(f"Saved {len(placed)} legs under tag {run_tag} -> {tag_file}")
                except Exception as _e:
                    logger.warning(f"Could not save tag file for {run_tag}: {_e}")

            overall["runs"].append(result)
            logger.success(f"[{name}] {result['status']}")
        except Exception as e:
            msg = f"[{name}] ERROR: {e}"
            logger.exception(msg)
            overall["runs"].append({"strategy": name, "status": "error", "error": str(e)})

    if args.json:
        print(json.dumps(overall, indent=2))
    else:
        logger.info(f"Done. Summary: {overall}")

    # Non-zero exit if any error in live mode
    if args.live and any(r.get("status") in ("error", "partial", "rolled_back_due_to_failure") for r in overall["runs"]):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
