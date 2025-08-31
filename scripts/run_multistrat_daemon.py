# scripts/run_multistrat_daemon.py
from __future__ import annotations
import os
import time
from typing import List, Tuple, Optional
from loguru import logger

# --- login ---
try:
    from core.login import restore_or_login
except Exception:
    restore_or_login = None

# --- strategy registry (alias-aware resolver) ---
try:
    from core.strategy_registry import REGISTRY, ALIASES
except Exception:
    REGISTRY, ALIASES = {}, {}

def _resolve_strategy(name: str):
    k = (name or "").strip().lower()
    if k in REGISTRY:
        return REGISTRY[k]
    if k in ALIASES:
        return REGISTRY.get(ALIASES[k])
    return None

# --- risk manager ---
try:
    from core.risk import RiskManager   # preferred
except Exception:
    try:
        from core.risk_manager import RiskManager  # proxy to RiskManager
    except Exception:
        RiskManager = None  # type: ignore

# --- order executor ---
try:
    from utils.order_exec import place_or_preview
except Exception:
    def place_or_preview(*a, **k):
        logger.error("utils.order_exec.place_or_preview not found")
        return []

# --- helpers to estimate open qty & PnL for risk state ---
def _estimate_open_qty(smart) -> int:
    try:
        pb = smart.positionBook()
        total = 0
        for p in (pb.get("data") or []):
            try:
                total += int(float(p.get("netqty") or p.get("netQty") or 0))
            except Exception:
                pass
        return abs(total)
    except Exception:
        return 0

def _estimate_pnl(smart) -> float:
    try:
        pb = smart.positionBook()
        mtm = 0.0
        for p in (pb.get("data") or []):
            v = p.get("m2m") or p.get("pnl") or 0.0
            try:
                mtm += float(v)
            except Exception:
                pass
        return mtm
    except Exception:
        return 0.0

def _apply_env_overrides(risk: RiskManager) -> None: # type: ignore
    # Allow forcing overrides at runtime from the shell.
    env_q = os.getenv("RISK_MAX_QTY_OVERRIDE")
    if env_q is None:
        env_q = os.getenv("RISK_MAX_QTY")  # fallback to plain env
    try:
        env_q_int = int(env_q) if env_q else None
    except Exception:
        env_q_int = None

    logger.info(f"[debug] before override → ENV.RISK_MAX_QTY={env_q_int if env_q is not None else 'None'} | risk.cfg.max_qty_total={risk.cfg.max_qty_total}")
    if env_q_int is not None:
        risk.cfg.max_qty_total = env_q_int
        logger.info(f"[debug] applied RISK_MAX_QTY override → {risk.cfg.max_qty_total}")

    # Exit time + market-hours toggles
    exit_flag = os.getenv("EXIT_ON_TIME_ENABLED")
    if exit_flag is not None:
        risk.cfg.exit_on_time_enabled = str(exit_flag).strip().lower() in {"1", "true", "yes", "y", "on"}

    mkt_flag = os.getenv("ENFORCE_MARKET_HOURS")
    if mkt_flag is not None:
        risk.cfg.enforce_market_hours = str(mkt_flag).strip().lower() in {"1", "true", "yes", "y", "on"}

def run_loop(strategies: List[str], cycle_seconds: int = 60):
    if restore_or_login is None or RiskManager is None:
        logger.error("Missing required modules (login or risk_manager). Abort.")
        return

    smart = restore_or_login()
    risk  = RiskManager()
    _apply_env_overrides(risk)

    logger.info(
        f"⚙️  Effective risk config → max_loss={risk.cfg.max_loss}, "
        f"max_qty_total={risk.cfg.max_qty_total}, "
        f"exit_on_time_enabled={risk.cfg.exit_on_time_enabled}, "
        f"enforce_market_hours={risk.cfg.enforce_market_hours}"
    )
    logger.info(
        f"🚀 Multi-strategy daemon starting | strategies={strategies} | "
        f"cycle={cycle_seconds}s | DRY_RUN={os.getenv('DRY_RUN','false')}"
    )

    while True:
        try:
            # refresh risk state
            try:
                risk.set_open_qty(_estimate_open_qty(smart))
                risk.set_mtm(_estimate_pnl(smart))
            except Exception as e:
                logger.warning(f"Risk state update failed (continuing): {e}")

            # global kill-switch each cycle (needs smart)
            risk.enforce_kill_switch(smart)

            for name in strategies:
                strat = _resolve_strategy(name)
                if not strat:
                    logger.error(f"Strategy '{name}' not found in registry.")
                    continue

                logger.info(f"▶ Running strategy: {name}")
                try:
                    # strategy should return a list[dict] of Angel order dicts
                    orders = strat(smart)
                except SystemExit:
                    raise
                except Exception as e:
                    logger.exception(f"Strategy {name} failed: {e}")
                    continue

                if not orders:
                    logger.info(f"Strategy {name}: no orders this cycle.")
                    continue

                # ---- risk gate BEFORE sending to executor
                try:
                    risk.gate(smart, orders)
                except SystemExit:
                    raise
                except Exception as e:
                    logger.error(f"Risk gate blocked batch for {name}: {e}")
                    continue

                # ---- execute / preview
                try:
                    results: List[Tuple[bool, Optional[str], dict]] = place_or_preview(smart, orders)
                    ok_count = sum(1 for (ok, _, _) in results if ok)
                    fail_count = len(results) - ok_count
                    # helpful summary log
                    if results:
                        first = results[0]
                        logger.info(
                            f"Strategy {name}: placed {ok_count}/{len(results)} "
                            f"({'LIVE' if os.getenv('DRY_RUN','false').lower() in {'false','0','no'} else 'DRY'}) | "
                            f"first_result={first}"
                        )
                    else:
                        logger.info(f"Strategy {name}: executor returned no results.")
                except SystemExit:
                    raise
                except Exception as e:
                    logger.exception(f"Execution error for {name}: {e}")

        except SystemExit:
            logger.error("Kill-switch/fatal condition. Stopping daemon.")
            break
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt — exiting loop.")
            break
        except Exception as e:
            logger.exception(f"Top-level loop error: {e}")

        time.sleep(cycle_seconds)

if __name__ == "__main__":
    names = os.getenv("STRATEGIES", "atm_straddle").split(",")
    names = [n.strip() for n in names if n.strip()]
    cycle = int(os.getenv("CYCLE_SECS", "60"))
    run_loop(names, cycle_seconds=cycle)
