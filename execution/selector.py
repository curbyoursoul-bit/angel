# execution/selector.py
from __future__ import annotations
from typing import Dict, Callable, List
from intelligence.regime import regime as detect_regime

# Plug your strategies here; values are callables like run(smart, **kwargs)
REGISTRY: Dict[str, Callable] = {}  # you will import & populate in your engine

# Map regime -> list of strategy names to run
REGIME_MAP = {
    "trend":  ["ema_crossover", "vwap_breakout", "orb_breakout"],
    "range":  ["vwap_mean_reversion", "atm_iron_fly"],
    "volatile":["atm_straddle", "orb_breakout"],
}

def pick_strategies(df_ohlcv) -> List[str]:
    r = detect_regime(df_ohlcv)
    return REGIME_MAP.get(r, [])
