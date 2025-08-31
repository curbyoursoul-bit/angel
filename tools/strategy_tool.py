# tools/strategy_tool.py
from __future__ import annotations
from typing import Any, Dict, Optional
from tools.base import Tool
from loguru import logger
import importlib
import inspect

def _normalize_result(result: Any) -> Dict[str, Any]:
    """
    Normalize strategy outputs to:
      {"orders": List[dict], "notes": str}
    Accepted inputs:
      - None
      - list[dict]
      - (list[dict], str)
      - dict (any)  → ensures 'orders' & 'notes'
      - anything else → treated as notes
    """
    if result is None:
        return {"orders": [], "notes": ""}
    if isinstance(result, list):
        return {"orders": result, "notes": ""}
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], list):
        orders, notes = result
        return {"orders": orders or [], "notes": str(notes or "")}
    if isinstance(result, dict):
        out = dict(result)
        out.setdefault("orders", [])
        out.setdefault("notes", "")
        # accept legacy keys
        if not out["orders"]:
            for k in ("signals", "trades"):
                if isinstance(out.get(k), list):
                    out["orders"] = out[k]
                    break
        return out
    return {"orders": [], "notes": str(result)}

def _resolve_strategy_callable(name: str):
    """
    Prefer registry (aliases, optional strategies), fallback to dynamic import.
    """
    try:
        from core.strategy_registry import get_strategy_callable  # central truth / aliases
        return get_strategy_callable(name)
    except Exception:
        # Fallback to dynamic import for ad-hoc strategies not in registry
        mod_name = f"strategies.{name}"
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:
            raise ImportError(f"Could not import {mod_name}: {e}")
        if not hasattr(mod, "run"):
            raise AttributeError(f"{mod_name} is missing run()")
        return getattr(mod, "run")

class StrategyTool(Tool):
    """
    Adapter around strategies.* modules.

    Features:
    - Resolves aliases via core.strategy_registry (fallback to importlib).
    - Filters kwargs to run() signature unless **kwargs is present.
    - Auto-injects SmartAPI client:
        * Prefer engine-provided ctx.smart (no repeated logins)
        * Else reuse cached self._smart
        * Else restore_or_login()
    - Normalizes legacy return shapes to {'orders': [], 'notes': ''}.
    """
    name = "strategy"

    def __init__(self) -> None:
        super().__init__()
        self._smart: Optional[Any] = None  # cached SmartConnect

    def _get_smart(self, ctx) -> Any:
        # Prefer engine context
        if ctx is not None and getattr(ctx, "smart", None):
            self._smart = ctx.smart
            return self._smart
        # Reuse cached
        if self._smart is not None:
            return self._smart
        # Last resort: login once and cache
        from core.login import restore_or_login
        self._smart = restore_or_login()
        return self._smart

    def _filtered_kwargs(self, fn, params: Dict[str, Any], ctx=None) -> Dict[str, Any]:
        sig = inspect.signature(fn)
        if any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
            filtered = dict(params or {})
        else:
            allowed = set(sig.parameters.keys())
            filtered = {k: v for k, v in (params or {}).items() if k in allowed}

        # Inject ctx if the strategy accepts it
        if "ctx" in sig.parameters and "ctx" not in filtered and ctx is not None:
            filtered["ctx"] = ctx

        # Lazy-inject SmartAPI client if required (prefer ctx.smart)
        if "smart" in sig.parameters and "smart" not in filtered:
            filtered["smart"] = self._get_smart(ctx)

        return filtered

    def run(self, fn: str, **kwargs) -> Dict[str, Any]:
        """
        Executor calls: StrategyTool.run('run', strategy='name', params={...}, ctx=...)
        """
        try:
            if fn != "run":
                return {"ok": False, "error": f"unknown fn {fn}"}

            # pull ctx (if any) that Executor passed
            ctx = kwargs.pop("ctx", None)

            strat = kwargs["strategy"]
            params = kwargs.get("params", {}) or {}

            run_fn = _resolve_strategy_callable(strat)
            call_kwargs = self._filtered_kwargs(run_fn, params, ctx=ctx)

            raw = run_fn(**call_kwargs)
            data = _normalize_result(raw)

            # Ensure final shape + a bit of context
            data.setdefault("orders", [])
            data.setdefault("notes", "")
            data.setdefault("_meta", {})
            data["_meta"].update({
                "strategy": strat,
                "passed_params": params,
                "used_params": call_kwargs,
            })

            return {"ok": True, "data": data}

        except Exception as e:
            logger.exception("StrategyTool error")
            # concise error for agent; full stack in logs
            return {"ok": False, "error": str(e)}
