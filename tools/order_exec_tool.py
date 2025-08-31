# tools/order_exec_tool.py
from __future__ import annotations

from typing import Any, Dict, List
from loguru import logger
from tools.base import Tool

class OrderExecTool(Tool):
    """
    Bridge to order execution utilities.

    Exposed fns:
      - place_or_preview(orders, rollback_on_failure=True)
        # orders: list[dict] or dict
        # DRY_RUN is respected from config/env inside utils.order_exec
    """
    name = "order_exec"

    def __init__(self) -> None:
        self.smart = None  # SmartConnect (lazy)

    # -------- internals ------------------------------------------------------
    def _ensure_smart(self, ctx) -> Any:
        if ctx and getattr(ctx, "smart", None):
            self.smart = ctx.smart
        if self.smart is None:
            from core.login import restore_or_login
            self.smart = restore_or_login()
        return self.smart

    # -------- dispatcher -----------------------------------------------------
    def run(self, fn: str, **kwargs) -> Dict[str, Any]:
        try:
            ctx = kwargs.pop("ctx", None)
            smart = self._ensure_smart(ctx)

            if fn in ("place_or_preview", "place", "place_orders"):
                return self._place_or_preview(smart, **kwargs)
            else:
                return {"ok": False, "data": None, "error": f"unknown fn {fn}"}

        except Exception as e:
            logger.exception("OrderExecTool error")
            return {"ok": False, "data": None, "error": str(e)}

    # -------- ops ------------------------------------------------------------
    def _place_or_preview(self, smart, **kwargs) -> Dict[str, Any]:
        from utils.order_exec import place_or_preview as _place_or_preview

        orders = kwargs.get("orders") or []
        if isinstance(orders, dict):
            orders = [orders]

        rollback_on_failure = bool(kwargs.get("rollback_on_failure", True))

        res = _place_or_preview(smart, orders, rollback_on_failure=rollback_on_failure)
        return {"ok": True, "data": res, "error": None}
