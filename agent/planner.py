# agent/planner.py
from __future__ import annotations
from typing import Dict, Any
from agent.types import Goal, Action

_STRAT_ALIASES = {
    "atm": "atm_straddle",
    "atm_straddle": "atm_straddle",
    "orb": "orb_breakout",
    "orb_breakout": "orb_breakout",
    "vwap": "vwap_mean_reversion",
    "vwap_mean_reversion": "vwap_mean_reversion",
    "bb": "bollinger_breakout",
    "bollinger_breakout": "bollinger_breakout",
    "ema": "ema_crossover",
    "ema_crossover": "ema_crossover",
    "zscore": "zscore_mean_reversion",
    "zscore_mean_reversion": "zscore_mean_reversion",
    "volume": "volume_breakout",
    "volume_breakout": "volume_breakout",
}

def _safe_params(p: Any) -> Dict[str, Any]:
    return p if isinstance(p, dict) else {}

def _resolve_strategy(name: str | None) -> str | None:
    if not name:
        return None
    k = str(name).strip().lower()
    # pattern: run_<strategy>
    if k.startswith("run_"):
        k = k[4:]
    # map aliases
    return _STRAT_ALIASES.get(k, k)

class Planner:
    def plan(self, goal: Goal, context: Dict[str, Any] | None = None) -> Action:
        ctx = context or {}
        params = _safe_params(getattr(goal, "params", {}))
        gtext = (getattr(goal, "text", "") or "").strip()

        # 1) explicit goals
        if gtext == "run_atm_straddle":
            return Action(tool="strategy", name="run", args={"strategy": "atm_straddle", "params": params})
        if gtext == "run_orb_breakout":
            return Action(tool="strategy", name="run", args={"strategy": "orb_breakout", "params": params})
        if gtext == "square_off_all":
            return Action(
                tool="angel",
                name="square_off_all",
                args={
                    "mode": params.get("mode", ctx.get("mode", "DRY_RUN")),
                    "include_products": params.get("include_products"),
                },
            )
        if gtext == "eod_report":
            return Action(tool="report", name="eod_report", args={"day": params.get("day")})

        # 2) generic strategy invocations
        #    a) goal.text == "strategy" with params={"name": "...", ...}
        if gtext == "strategy":
            strat = _resolve_strategy(params.get("name"))
            return Action(tool="strategy", name="run", args={"strategy": strat, "params": params})

        #    b) pattern: run_<strategyName>
        if gtext.startswith("run_"):
            strat = _resolve_strategy(gtext)
            return Action(tool="strategy", name="run", args={"strategy": strat, "params": params})

        # 3) fallback: quick LTP probe for a symbol (keeps your original behavior)
        symbol = params.get("symbol", "BANKNIFTY")
        exch = params.get("exchange", "NSE")
        return Action(tool="angel", name="ltp", args={"exchange": exch, "tradingsymbol": symbol})
