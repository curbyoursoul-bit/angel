# tools/base.py
from __future__ import annotations
from typing import Any, Dict, Callable, List

class Tool:
    """
    Base tool:
      - Auto-discovers handlers named _<fn>(**kwargs)
      - Uniform return shape: {"ok": bool, "data"?: Any, "error"?: str}
      - Introspection: supported(), describe()
    """

    name: str = "tool"

    # ---- public API ---------------------------------------------------------
    def run(self, fn: str, **kwargs) -> Dict[str, Any]:
        """
        Default router. Subclasses may override, but usually not needed:
        define methods like `def _place_orders(...):` and call `run("place_orders", ...)`.
        """
        handler = self._get_handler(fn)
        if handler is None:
            return {"ok": False, "error": f"{self.name}.{fn} not supported"}
        try:
            result = handler(**kwargs)
        except Exception as e:
            return {"ok": False, "error": f"{self.name}.{fn} error: {e}"}
        if isinstance(result, dict) and "ok" in result:
            return result
        # Normalize plain returns into {"ok": True, "data": ...}
        return {"ok": True, "data": result}

    def supported(self) -> List[str]:
        """List of supported function names discovered from _<fn> handlers."""
        return sorted(self._discover_handlers().keys())

    def describe(self) -> str:
        """Short human-readable description."""
        return f"Tool<{self.name}>: {', '.join(self.supported()) or 'no actions'}"

    # ---- internals ----------------------------------------------------------
    def _get_handler(self, fn: str) -> Callable[..., Any] | None:
        return getattr(self, f"_{fn}", None) if fn else None

    def _discover_handlers(self) -> Dict[str, Callable[..., Any]]:
        """
        Find all callables starting with '_' that are intended as actions.
        Skips dunder/private/attrs.
        """
        out: Dict[str, Callable[..., Any]] = {}
        for attr in dir(self):
            if not attr.startswith("_") or attr.startswith("__"):
                continue
            if attr in {"_get_handler", "_discover_handlers"}:
                continue
            cand = getattr(self, attr)
            if callable(cand):
                out[attr[1:]] = cand
        return out
