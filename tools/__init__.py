from __future__ import annotations
from typing import Dict, Callable, Any

def build_tool_factories() -> Dict[str, Callable[[], Any]]:
    from .strategy_tool import StrategyTool
    from .angel_tool import AngelTool
    return {
        "strategy": (lambda: StrategyTool()),
        "angel":    (lambda: AngelTool()),
    }
