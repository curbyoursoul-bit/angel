# tools/instruments_tool.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd

from tools.base import Tool
from utils.instruments import (
    load_instruments,
    load_options,
    find_option_token,
    pick_atm_strike,
    nearest_expiry,
)
from utils.expiry import next_thursday


class InstrumentsTool(Tool):
    name = "instruments"

    # Let Tool.run auto-route to these _<fn> handlers

    def _nearest_expiry(
        self,
        *,
        underlying: str = "BANKNIFTY",
        target: Optional[str] = None,     # "YYYY-MM-DD"; defaults to next Thursday
        window_days: int = 10,
    ) -> Dict[str, Any]:
        """
        Return the nearest available options expiry date for the given underlying.
        """
        df = load_options(underlying.upper())
        tgt = pd.to_datetime(target).date() if target else next_thursday()
        exp = nearest_expiry(df, target=tgt, window_days=window_days)
        return {"ok": True, "data": str(exp)}

    def _find_option(
        self,
        *,
        underlying: str,
        expiry: str,
        strike: float | int,
        optiontype: str,                   # "CE" or "PE"
    ) -> Dict[str, Any]:
        """
        Locate a single option row (tradingsymbol + token) for underlying/expiry/strike/CE|PE.
        """
        df = load_instruments()  # broader master works with find_option_token
        row = find_option_token(
            df,
            underlying.upper(),
            pd.to_datetime(expiry).date(),
            int(strike),
            optiontype.upper(),
        )
        return {"ok": True, "data": row}

    def _pick_atm(
        self,
        *,
        spot: float,
        step: int = 100,
    ) -> Dict[str, Any]:
        """
        Round a spot to the nearest ATM strike given step.
        """
        return {"ok": True, "data": pick_atm_strike(float(spot), int(step))}

    # Optional helpers (handy in REPL/agent flows)

    def _list_expiries(self, *, underlying: str) -> Dict[str, Any]:
        """
        List upcoming expiries for an underlying (dates only).
        """
        df = load_options(underlying.upper())
        exps = (
            pd.to_datetime(df["expiry"], errors="coerce")
            .dropna()
            .sort_values()
            .dt.date
            .unique()
        )
        return {"ok": True, "data": [str(d) for d in exps]}

    def _resolve_equity(self, *, symbol: str) -> Dict[str, Any]:
        """
        Resolve an NSE equity tradingsymbol + token.
        """
        from utils.resolve import resolve_nse_token
        ts, token = resolve_nse_token(symbol)
        return {"ok": True, "data": {"tradingsymbol": ts, "symboltoken": token}}
