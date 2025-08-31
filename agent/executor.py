# agent/executor.py
from __future__ import annotations
from typing import Dict, Any, Callable, Optional
from loguru import logger

class Executor:
    """
    Robust tool dispatcher used by the Agent.

    - Lazy-loads tools on first use (so missing modules don't crash init)
    - Normalizes all tool returns to {ok: bool, data: any, error: str|None}
    - Registry can be extended dynamically (register/unregister)
    - Engine can inject an execution context via set_context(...) so each tool
      automatically receives ctx=...
    """

    def __init__(self):
        self._ctx = None  # engine-injected execution context

        # factories return a module/instance exposing .run(name, **kwargs)
        self._factories: Dict[str, Callable[[], Any]] = {}
        try:
            # tools/__init__.py must define build_tool_factories()
            from tools import build_tool_factories
            self._factories.update(build_tool_factories())
        except Exception:
            # ok to run without; you can also register factories manually later
            logger.debug("Executor: no build_tool_factories found or failed to load.")

        # cache of instantiated tools {tool_name: tool_module_or_instance}
        self._tools: Dict[str, Any] = {}

    # ---- public API ----------------------------------------------------------

    def set_context(self, ctx) -> None:
        """Engine calls this once so every tool gets ctx automatically."""
        self._ctx = ctx

    def register(self, name: str, factory: Callable[[], Any]) -> None:
        """Register or override a tool factory."""
        self._factories[name] = factory
        # drop any previously loaded instance so it reloads on next use
        self._tools.pop(name, None)

    def unregister(self, name: str) -> None:
        self._factories.pop(name, None)
        self._tools.pop(name, None)

    def list_tools(self) -> Dict[str, bool]:
        """Return {tool_name: is_loaded} map."""
        return {k: (k in self._tools) for k in sorted(self._factories.keys())}

    def run(self, tool: str, name: str, **kwargs) -> Dict[str, Any]:
        """
        Dispatch a call: tool_module.run(name, **kwargs).
        Always returns a normalized dict: {ok, data, error}.
        """
        t = self._get_tool(tool)
        if t is None:
            msg = f"tool '{tool}' not registered/available"
            logger.error(msg)
            return {"ok": False, "data": None, "error": msg}

        run_fn = getattr(t, "run", None)
        if not callable(run_fn):
            msg = f"tool '{tool}' has no callable .run(name, **kwargs)"
            logger.error(msg)
            return {"ok": False, "data": None, "error": msg}

        # pass engine ctx to every tool unless caller already provided one
        call_kwargs = dict(kwargs)
        if "ctx" not in call_kwargs:
            call_kwargs["ctx"] = getattr(self, "_ctx", None)

        try:
            res = run_fn(name, **call_kwargs)
            return self._normalize_result(res)
        except TypeError as e:
            logger.exception(f"Type error in {tool}.run('{name}', **kwargs): {e}")
            return {"ok": False, "data": None, "error": f"type error: {e}"}
        except Exception as e:
            logger.exception(f"Unhandled error in {tool}.run('{name}'): {e}")
            return {"ok": False, "data": None, "error": str(e)}

    # ---- internals -----------------------------------------------------------

    def _get_tool(self, name: str) -> Optional[Any]:
        if name in self._tools:
            return self._tools[name]
        fac = self._factories.get(name)
        if not fac:
            return None
        try:
            tool_mod = fac()  # instance or module exposing .run
            if not callable(getattr(tool_mod, "run", None)):
                logger.error(f"tool '{name}' loaded but has no run(name, **kwargs)")
                return None
            self._tools[name] = tool_mod
            return tool_mod
        except Exception as e:
            logger.exception(f"Failed to instantiate tool '{name}': {e}")
            return None

    def _normalize_result(self, res: Any) -> Dict[str, Any]:
        """
        Accept common shapes and normalize to {ok, data, error}.
        - If a tool returns dict with 'ok', 'data', 'error', pass-through (with defaults).
        - If it returns Any other object, wrap as ok=True, data=res.
        """
        if isinstance(res, dict):
            ok = bool(res.get("ok", True))
            data = res.get("data", None)
            err = res.get("error", None)
            return {"ok": ok, "data": data, "error": err}
        return {"ok": True, "data": res, "error": None}
