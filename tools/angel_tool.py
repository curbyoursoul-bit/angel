# tools/angel_tool.py
from __future__ import annotations
from typing import Any, Dict, List
from loguru import logger
from tools.base import Tool

class AngelTool(Tool):
    """
    Bridge to Angel One utilities.

    Exposed fns:
      - ltp(exchange, tradingsymbol, symboltoken)
      - place_orders(orders)  # orders can be dict or list[dict]; DRY_RUN comes from config/env
    """
    name = "angel"

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

            if fn == "ltp":
                return self._ltp(smart, **kwargs)
            elif fn in ("place_orders", "place"):
                return self._place_orders(smart, **kwargs)
            else:
                return {"ok": False, "data": None, "error": f"unknown fn {fn}"}

        except Exception as e:
            logger.exception("AngelTool error")
            return {"ok": False, "data": None, "error": str(e)}

    # -------- ops ------------------------------------------------------------
    def _ltp(self, smart, **kwargs) -> Dict[str, Any]:
        from utils.ltp_fetcher import get_ltp

        exch  = kwargs.get("exchange") or kwargs.get("exch") or "NSE"
        sym   = kwargs.get("tradingsymbol") or kwargs.get("symbol")
        token = str(kwargs.get("symboltoken") or kwargs.get("token") or "")

        if not sym:
            return {"ok": False, "data": None, "error": "tradingsymbol is required"}
        if not token:
            return {"ok": False, "data": None, "error": "Missing symboltoken; preview orders first or pass symboltoken."}

        px = float(get_ltp(smart, exch, sym, token))
        return {"ok": True, "data": {"ltp": px, "exchange": exch, "tradingsymbol": sym, "symboltoken": token}, "error": None}

    def _place_orders(self, smart, **kwargs) -> Dict[str, Any]:
        """
        Place one or many orders (dict or list[dict]).
        DRY vs LIVE is controlled inside utils/order_exec via config/env; no 'dry_run' arg here.
        """
        from utils.order_exec import place_or_preview

        orders: List[dict] | dict = kwargs.get("orders") or []
        if isinstance(orders, dict):
            orders = [orders]

        # Call the batch-friendly API
        try:
            result = place_or_preview(smart, orders)
            return {"ok": True, "data": result, "error": None}
        except Exception as e:
            logger.exception("AngelTool/_place_orders failed")
            return {"ok": False, "data": None, "error": str(e)}
