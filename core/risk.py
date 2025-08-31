# core/risk.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Iterable, Optional, List, Dict, Any
from datetime import datetime, time as dtime
from loguru import logger

# --- notify: safe fallback ---------------------------------------------------
try:
    from utils.alerts import notify  # type: ignore
except Exception:
    def notify(msg: str) -> None:  # type: ignore
        logger.warning(f"[notify-fallback] {msg}")

# Optional LTP helper (supports multiple signatures)
try:
    from utils.ltp_fetcher import get_ltp as _base_get_ltp  # type: ignore
except Exception:
    _base_get_ltp = None  # type: ignore


def _safe_get_ltp(smart, exchange: str, tradingsymbol: str, token: str) -> Optional[float]:
    """
    Call user's get_ltp with best-effort signature matching:
      get_ltp(smart, exch, tsym, token) OR get_ltp(smart, exch, token) OR get_ltp(smart, token)
    """
    if _base_get_ltp is None:
        return None
    try:
        return _base_get_ltp(smart, exchange, tradingsymbol, token)  # type: ignore
    except Exception:
        pass
    try:
        return _base_get_ltp(smart, exchange, token)  # type: ignore
    except Exception:
        pass
    try:
        return _base_get_ltp(smart, token)  # type: ignore
    except Exception:
        return None

# -------- SDK-variant helpers ------------------------------------------------
def _call_first(smart, names: Iterable[str]):
    """Call the first available callable on the SmartAPI object from a list of names."""
    for name in names:
        fn = getattr(smart, name, None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                continue
    return None


def _get_positions_data(smart) -> List[Dict[str, Any]]:
    """
    Return a list of position rows across SmartAPI variants:
    - {'status': True, 'data': [...]}
    - or a plain list
    """
    resp = _call_first(smart, ("positionBook", "positions", "position", "getPositions"))
    if resp is None:
        return []
    if isinstance(resp, dict):
        data = resp.get("data")
        return data if isinstance(data, list) else []
    if isinstance(resp, list):
        return resp
    return []


def _get_funds(smart):
    """Return funds/limits response across variants; may be dict or None."""
    return _call_first(smart, ("rmsLimits", "rmsLimit", "funds", "getFunds", "getRMS", "getRMSLimits"))

# -------- env helpers --------------------------------------------------------
def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# -------- config -------------------------------------------------------------
@dataclass
class RiskConfig:
    max_loss: float              # e.g., -2000  (negative number)
    max_qty_total: int           # e.g., 70
    enforce_market_hours: bool = True
    min_cash_warn: float = 0.0   # warn threshold only (won’t block)
    kill_switch_disabled: bool = False
    kill_flag_path: str = "data/kill_switch.flag"
    exit_on_time_enabled: bool = True
    exit_time_ist: str = "15:20"  # HH:MM


def load_risk_config() -> RiskConfig:
    """
    Build config with precedence:
      1) Shell env (wins)
      2) config.py (fallback defaults)
      3) Hardcoded safe defaults
    """
    try:
        import config as C  # type: ignore
    except Exception:
        C = None  # type: ignore

    # ENFORCE_MARKET_HOURS: env has priority, else config, else True
    if os.getenv("ENFORCE_MARKET_HOURS") is not None:
        enforce = _env_bool("ENFORCE_MARKET_HOURS", True)
    else:
        enforce = bool(getattr(C, "ENFORCE_MARKET_HOURS", True)) if C else True

    # Max daily loss: env wins, else config, else -2000
    cfg_max_loss = getattr(C, "RISK_MAX_LOSS", -2000) if C else -2000
    max_loss = _env_float("RISK_MAX_LOSS", float(cfg_max_loss))

    # Quantity cap: env wins, else config, else 70
    cfg_max_qty = getattr(C, "RISK_MAX_QTY", 70) if C else 70
    max_qty = _env_int("RISK_MAX_QTY", int(cfg_max_qty))

    # Optional warn threshold
    cfg_min_cash = getattr(C, "RISK_MIN_CASH", 0.0) if C else 0.0
    min_cash = _env_float("RISK_MIN_CASH", float(cfg_min_cash))

    # Kill switch disabled?
    cfg_kill_off = getattr(C, "KILL_SWITCH_DISABLED", False) if C else False
    kill_off = _env_bool("KILL_SWITCH_DISABLED", bool(cfg_kill_off))

    # Kill flag path
    cfg_flagpath = getattr(C, "KILL_FLAG_PATH", "data/kill_switch.flag") if C else "data/kill_switch.flag"
    flagpath = os.getenv("KILL_FLAG_PATH", cfg_flagpath)

    # Timed exit controls
    cfg_exit_en = getattr(C, "EXIT_ON_TIME_ENABLED", True) if C else True
    exit_en = _env_bool("EXIT_ON_TIME_ENABLED", bool(cfg_exit_en))
    cfg_exit_tm = getattr(C, "EXIT_TIME_IST", "15:20") if C else "15:20"
    exit_tm = os.getenv("EXIT_TIME_IST", cfg_exit_tm)

    return RiskConfig(
        max_loss=max_loss,
        max_qty_total=max_qty,
        enforce_market_hours=enforce,
        min_cash_warn=min_cash,
        kill_switch_disabled=kill_off,
        kill_flag_path=flagpath,
        exit_on_time_enabled=exit_en,
        exit_time_ist=exit_tm,
    )

# -------- qty / pnl ----------------------------------------------------------
def _sum_proposed_qty(orders: Iterable[dict]) -> int:
    total = 0
    for o in orders or []:
        try:
            total += int(o.get("quantity", 0) or o.get("qty", 0))
        except Exception:
            continue
    return total


def _extract_int(row: Dict[str, Any], *keys: str, default: int = 0) -> int:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            try:
                return int(row[k])
            except Exception:
                try:
                    return int(float(row[k]))
                except Exception:
                    continue
    return default


def _extract_float(row: Dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            try:
                return float(row[k])
            except Exception:
                continue
    return default


def _current_total_open_qty(smart) -> int:
    """Approximate current exposure using whatever 'positions' API exists."""
    try:
        rows = _get_positions_data(smart)
        total = 0
        for row in rows:
            q = _extract_int(row, "netqty", "netQty", "quantity", default=0)
            total += abs(q)
        return total
    except Exception as e:
        logger.warning(f"Qty check skipped: {e}")
        return 0


def _estimate_intraday_pnl(smart) -> float:
    """
    Compute intraday P&L.
    Prefer broker-provided 'pnl' field; else approximate via LTP vs avg price.
    """
    try:
        rows = _get_positions_data(smart)
        if not rows:
            return 0.0

        pnl_total = 0.0
        for row in rows:
            # Prefer broker-provided pnl if available
            if "pnl" in row:
                try:
                    pnl_total += float(row["pnl"])
                    continue
                except Exception:
                    pass

            # Fallback: compute rough M2M if fields exist
            exch = (row.get("exchange") or row.get("exch_seg") or "NSE")
            tsym = (row.get("tradingsymbol") or row.get("symbol") or "")
            token = (row.get("symboltoken") or row.get("token") or "")
            netqty = _extract_float(row, "netqty", "netQty", default=0.0)
            avg = _extract_float(row, "avgprice", "avgPrice", default=0.0)

            try:
                ltp = _safe_get_ltp(smart, exch, tsym, token) or avg
            except Exception:
                ltp = avg

            # If netqty>0 (long): PnL = (ltp - avg)*qty ; short -> inverse
            pnl_total += (ltp - avg) * netqty

        return float(pnl_total)
    except Exception as e:
        logger.warning(f"Could not compute P&L (positions failed): {e}")
        return 0.0


def _parse_available_cash(funds_resp: Any) -> float:
    """
    Try to extract available cash/margin from a variety of Angel responses.
    Returns 0.0 if unknown.
    """
    if not isinstance(funds_resp, dict):
        return 0.0

    # common shape: {"status": True, "data": {...}}
    root = funds_resp.get("data") if funds_resp.get("status") else funds_resp
    if not isinstance(root, dict):
        return 0.0

    # Try common keys
    for k in (
        "availablecash",
        "availableCash",
        "availablecashbalance",
        "availableFunds",
        "availablefunds",
        "avaiablesegmargin",
    ):
        if k in root:
            try:
                return float(root[k])
            except Exception:
                continue

    # Some variants nest cash under "cash" or "net" or segment keys
    for k in ("cash", "net", "equity", "derivatives"):
        if k in root and isinstance(root[k], dict):
            sub = root[k]
            for kk in ("available", "availableCash", "availablecash"):
                if kk in sub:
                    try:
                        return float(sub[kk])
                    except Exception:
                        continue
    return 0.0

# -------- RiskManager (class-based) ------------------------------------------
@dataclass
class RiskManager:
    cfg: RiskConfig = field(default_factory=load_risk_config)

    # state holders (engine should periodically update if possible)
    _open_qty_est: int = 0
    _mtm_estimate: float = 0.0

    def set_open_qty(self, qty: int) -> None:
        self._open_qty_est = int(qty)

    def set_mtm(self, pnl: float) -> None:
        self._mtm_estimate = float(pnl)

    # --- time guards ---
    def _within_market_hours(self, now: Optional[datetime] = None) -> bool:
        if not self.cfg.enforce_market_hours:
            return True
        now = now or datetime.now()
        start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        return start <= now <= end

    def _is_exit_window(self, now: Optional[datetime] = None) -> bool:
        if not self.cfg.exit_on_time_enabled:
            return False
        now = now or datetime.now()
        try:
            hh, mm = self.cfg.exit_time_ist.split(":")
            exit_t = dtime(hour=int(hh), minute=int(mm))
        except Exception:
            exit_t = dtime(hour=15, minute=20)
        return now.time() >= exit_t

    # --- guards ---
    def enforce_kill_switch(self, smart) -> None:
        """
        If P&L ≤ max_loss OR kill flag exists -> raise SystemExit after attempting square-off.
        """
        if os.path.exists(self.cfg.kill_flag_path):
            logger.error("Kill-switch flag present; blocking trading.")
            raise SystemExit("Kill-switch engaged (flag present).")

        if self.cfg.kill_switch_disabled:
            logger.info("Kill-switch disabled via env KILL_SWITCH_DISABLED=1")
            return

        # compute P&L (if engine hasn't set_mtm yet)
        pnl = self._mtm_estimate or _estimate_intraday_pnl(smart)
        logger.info(f"🧯 Intraday P&L check: ₹{pnl:.2f} (kill if ≤ {self.cfg.max_loss})")

        if pnl <= self.cfg.max_loss:
            msg = f"🚨 Kill-switch: P&L ₹{pnl:.2f} ≤ ₹{self.cfg.max_loss:.2f}. Squaring off & blocking new entries."
            logger.critical(msg)
            notify(msg)
            try:
                # lazy import to avoid hard dependency if user doesn't have script
                from scripts.panic_button import panic_squareoff  # type: ignore
                panic_squareoff(smart)
            except Exception as e:
                logger.error(f"panic_squareoff failed or missing: {e}")
            finally:
                os.makedirs(os.path.dirname(self.cfg.kill_flag_path) or ".", exist_ok=True)
                with open(self.cfg.kill_flag_path, "w", encoding="utf-8") as f:
                    f.write(f"Triggered at {datetime.now().isoformat()} PnL={pnl:.2f}\n")
                raise SystemExit("Kill-switch engaged (daily loss).")

    def pre_trade_check(self, smart, orders: List[dict]) -> None:
        """
        Raises RuntimeError if a pre-trade guard fails.
        - Market hours & timed-exit window
        - Quantity cap (existing + new must not exceed cap)
        - Margin availability (soft warning)
        """
        # market hours / timed exit
        if not self._within_market_hours():
            raise RuntimeError("market_closed")
        if self._is_exit_window():
            raise RuntimeError("timed_exit_window")

        # quantity cap (estimate current + proposed)
        current_qty = _current_total_open_qty(smart)
        proposed = _sum_proposed_qty(orders)

        # Which cap value are we using?
        env_cap_raw = os.getenv("RISK_MAX_QTY")
        try:
            env_cap_val = int(env_cap_raw) if env_cap_raw is not None else None
        except Exception:
            env_cap_val = None

        cap = env_cap_val if env_cap_val is not None else int(self.cfg.max_qty_total)

        logger.info(
            f"[risk-cfg] qty_cap check | env={env_cap_raw or 'none'} | "
            f"cfg={self.cfg.max_qty_total} | using_cap={cap} | "
            f"current={current_qty} | proposed={proposed}"
        )

        if current_qty + proposed > cap:
            msg = (f"⛔ Quantity cap exceeded: current {current_qty} + "
                   f"proposed {proposed} > cap {cap}")
            logger.error(msg)
            notify(msg)
            raise RuntimeError("quantity_cap_exceeded")

        # margin availability warn (best-effort)
        try:
            funds = _get_funds(smart)
            avail = _parse_available_cash(funds)
            if avail > 0:
                if self.cfg.min_cash_warn > 0 and avail < self.cfg.min_cash_warn:
                    logger.warning(
                        f"Low available cash (₹{avail:.0f} < ₹{self.cfg.min_cash_warn:.0f}). Orders may be rejected."
                    )
            else:
                logger.info("Funds API returned no usable available cash figure (skipping warn).")
        except Exception as e:
            logger.warning(f"Margin check skipped (API not available): {e}")

    def gate(self, smart, orders: List[dict]) -> None:
        """
        Full gate: kill-switch + pre-trade batch checks.
        """
        self.enforce_kill_switch(smart)
        self.pre_trade_check(smart, orders)


# -------- Legacy function API (kept for backward compatibility) --------------
def pre_trade_guards(smart, orders: List[dict], cfg: Optional[RiskConfig] = None) -> None:
    rm = RiskManager(cfg or load_risk_config())
    rm.pre_trade_check(smart, orders)


def enforce_kill_switch(smart, cfg: Optional[RiskConfig] = None) -> None:
    rm = RiskManager(cfg or load_risk_config())
    rm.enforce_kill_switch(smart)
