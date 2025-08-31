# strategies/atm_straddle.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple
from datetime import date
import math
import os
import pandas as pd
from loguru import logger

from utils.ltp_fetcher import get_ltp
from utils.instruments import (
    load_options, pick_atm_strike, get_option_rows, nearest_expiry
)
from utils.expiry import next_thursday
from config import DEFAULT_ORDER_TYPE

# ======= knobs =======
UNDERLYING = "NIFTY"      # NIFTY | BANKNIFTY (extendable)
LOTS       = 1

# Pricing
TICK_SIZE          = 0.05
MIN_PRICE          = 0.05
PRICE_PAD_PCT_SELL = 0.03  # place LIMIT slightly below LTP to get fills

# Black–Scholes fairness (toggleable)
USE_BS_FAIR_VALUE    = True
ALLOW_BS_FALLBACK    = True  # if BS check fails, proceed but warn
RISK_FREE            = 0.065
IV_GUESS             = 0.20
BS_MAX_MISPRICE_PCT  = 0.30  # allow ≤30% dev vs fair

# Credit/risk guards
MIN_LEG_PREMIUM        = 5.0     # ₹ per option
MIN_NET_CREDIT_PER_LOT = 250.0   # total CE+PE credit per lot
# =====================

EXCHANGE      = "NFO"
ORDER_VARIETY = "NORMAL"
PRODUCT_TYPE  = "INTRADAY"
DURATION      = "DAY"

STEP_MAP      = {"NIFTY": 50, "BANKNIFTY": 100}
SPOT_TOKEN    = {"NIFTY": "26000", "BANKNIFTY": "26009"}  # NSE index tokens

# ---------- optional BS import (fallback) ----------
try:
    from utils.black_scholes import BlackScholes as _BS
except Exception:
    class _BS:
        def __init__(self, spot, strike, expiry_days, rate=0.065, volatility=0.20):
            self.S = float(spot); self.K = float(strike)
            self.T = max(1e-6, expiry_days / 365.0)
            self.r = float(rate); self.sigma = max(1e-6, float(volatility))
        def _d1(self):
            import math
            return (math.log(self.S / self.K) + (self.r + 0.5*self.sigma**2)*self.T) / (self.sigma*math.sqrt(self.T))
        def _d2(self):
            import math
            return self._d1() - self.sigma*math.sqrt(self.T)
        def call_price(self):
            import math
            from math import erf
            d1=self._d1(); d2=self._d2()
            N=lambda x:0.5*(1.0+erf(x/math.sqrt(2)))
            return self.S*N(d1)-self.K*math.exp(-self.r*self.T)*N(d2)
        def put_price(self):
            import math
            from math import erf
            d1=self._d1(); d2=self._d2()
            N=lambda x:0.5*(1.0+erf(x/math.sqrt(2)))
            return self.K*math.exp(-self.r*self.T)*N(-d2)-self.S*N(-d1)
# --------------------------------------------------

# ---------- helpers ----------
def _cell(row: pd.Series, *keys: str) -> str:
    for k in keys:
        if k in row:
            v = row[k]
            if pd.notna(v):
                s = str(v).strip()
                if s and s.upper() not in {"NAN","NA","NONE"}:
                    return s
    return ""

def _row_to_pair(row: pd.Series) -> Tuple[str, str]:
    # Always return strings for Angel
    ts = _cell(row, "tradingsymbol", "symbol", "symbolname")
    tok = _cell(row, "symboltoken", "token")
    return ts, str(tok)

def _round_tick(px: float, tick: float = TICK_SIZE) -> float:
    if tick <= 0:
        return max(MIN_PRICE, float(px))
    # “nearest” tick (Angel accepts 2 decimals for options)
    return max(MIN_PRICE, round(round(float(px)/tick)*tick, 2))

def _sell_limit_from_ltp(ltp: float) -> str:
    px = _round_tick(float(ltp) * (1.0 - PRICE_PAD_PCT_SELL))
    return f"{px:.2f}"

def _safe_option_ltp(smart, ts: str, tok: str) -> float:
    last = None
    for _ in range(3):
        try:
            px = float(get_ltp(smart, exchange="NFO", tradingsymbol=ts, symboltoken=tok))
            if MIN_PRICE <= px < 10000:
                return px
            last = f"outlier {px}"
        except Exception as e:
            last = str(e)
    raise RuntimeError(f"LTP bad for {ts} ({tok}): {last}")

def _bs_okay(spot: float, strike: float, dte: int, ltp: float, is_call: bool) -> bool:
    if not USE_BS_FAIR_VALUE:
        return True
    bs = _BS(spot=spot, strike=strike, expiry_days=max(1, dte), rate=RISK_FREE, volatility=IV_GUESS)
    fair = max(MIN_PRICE, float(bs.call_price() if is_call else bs.put_price()))
    dev  = abs(ltp - fair) / max(fair, MIN_PRICE)
    ok   = dev <= BS_MAX_MISPRICE_PCT
    if not ok:
        kind = "CALL" if is_call else "PUT"
        logger.warning(f"BS: {kind} K={strike} LTP={ltp:.2f} fair≈{fair:.2f} dev={dev:.0%} > {BS_MAX_MISPRICE_PCT:.0%}")
    return ok

def _available_strikes_for_expiry(opts: pd.DataFrame, expiry_d: date) -> list[int]:
    df = opts
    if "expiry_dt" in df.columns:
        df = df[df["expiry_dt"].dt.date == expiry_d]
    col = "strike_int" if "strike_int" in df.columns else "strike"
    strikes: list[int] = []
    for v in df[col].tolist():
        try:
            strikes.append(int(float(v)))
        except Exception:
            pass
    return sorted(set(strikes))

def _snap_to_available_strike(desired: int, strikes: list[int]) -> int:
    if not strikes:
        return desired
    lo, hi = strikes[0], strikes[-1]
    if desired <= lo:
        return lo
    if desired >= hi:
        return hi
    return min(strikes, key=lambda k: abs(k - desired))
# ---------------------

def _pick_expiry(options_df: pd.DataFrame, underlying: str, target_hint: date) -> date:
    """
    Choose the nearest listed expiry around the target weekly date.
    Relaxed window so monthly-only chains are fine.
    """
    try:
        exp = nearest_expiry(options_df, target=target_hint, window_days=45)
        return exp
    except Exception:
        # fallback: simply take minimum future expiry in the sheet
        exps = sorted({d.date() for d in pd.to_datetime(options_df["expiry_dt"]).unique() if pd.notna(d)})
        today = date.today()
        fut = [d for d in exps if d >= today]
        if not fut:
            raise RuntimeError("No future expiries available in instruments CSV")
        return fut[0]

def run(smart) -> List[Dict[str, Any]]:
    und   = UNDERLYING.upper()
    step  = STEP_MAP.get(und, 50)
    token = SPOT_TOKEN[und]

    # 1) Spot (with optional DRY override)
    override_env = f"DRY_SPOT_OVERRIDE_{und}"
    override_val = os.getenv(override_env, "").strip()
    if override_val:
        try:
            spot = float(override_val)
            logger.warning(f"{und} spot overridden via {override_env}={override_val} → {spot:.2f}")
        except Exception:
            spot = float(get_ltp(smart, exchange="NSE", tradingsymbol=und, symboltoken=token))
    else:
        spot = float(get_ltp(smart, exchange="NSE", tradingsymbol=und, symboltoken=token))
    logger.info(f"{und} spot: {spot:.2f}")

    # 2) Expiry + instruments
    weekly_thu = next_thursday()
    opts = load_options(und)
    expiry_d = _pick_expiry(opts, und, weekly_thu)
    if expiry_d != weekly_thu:
        logger.warning(f"Adjusting expiry {weekly_thu} → nearest {expiry_d}")
    dte = (expiry_d - date.today()).days

    # 3) ATM strike & rows (snap to available if needed)
    atm_raw = pick_atm_strike(spot, step=step)
    strikes = _available_strikes_for_expiry(opts, expiry_d)
    atm = _snap_to_available_strike(atm_raw, strikes)
    if atm != atm_raw:
        logger.warning(f"Desired ATM {atm_raw} not listed. Snapped to nearest {atm}.")
    logger.info(f"ATM strike: {atm}")

    atm_ce, atm_pe = get_option_rows(opts, expiry_d=expiry_d, strike_rupees=atm, step=step)
    ce_ts, ce_tok = _row_to_pair(atm_ce)
    pe_ts, pe_tok = _row_to_pair(atm_pe)
    if not (ce_ts and ce_tok and pe_ts and pe_tok):
        raise RuntimeError("ATM CE/PE tokens missing")

    lot_size = int(atm_ce.get("lotsize") or atm_pe.get("lotsize") or 0) or 1
    qty = lot_size * LOTS
    logger.info(f"Lot size: {lot_size} | Lots: {LOTS} | Qty: {qty}")

    # 4) LTPs (sanitized)
    ce_ltp = _safe_option_ltp(smart, ce_ts, ce_tok)
    pe_ltp = _safe_option_ltp(smart, pe_ts, pe_tok)

    # 5) BS fairness checks
    ce_ok = _bs_okay(spot, atm, dte, ce_ltp, is_call=True)
    pe_ok = _bs_okay(spot, atm, dte, pe_ltp, is_call=False)
    if USE_BS_FAIR_VALUE and not (ce_ok and pe_ok):
        if ALLOW_BS_FALLBACK:
            logger.warning("BS fairness failed for ATM leg(s); proceeding due to ALLOW_BS_FALLBACK=True")
        else:
            raise RuntimeError("BS fairness failed; set ALLOW_BS_FALLBACK=True or disable USE_BS_FAIR_VALUE")

    # 6) Credit/risk guards
    if ce_ltp < MIN_LEG_PREMIUM or pe_ltp < MIN_LEG_PREMIUM:
        raise RuntimeError(f"Leg premium too small: CE={ce_ltp:.2f}, PE={pe_ltp:.2f}")
    net_credit_per_lot = (ce_ltp + pe_ltp) * lot_size
    if net_credit_per_lot < MIN_NET_CREDIT_PER_LOT:
        raise RuntimeError(f"Net credit/lot too low: ₹{net_credit_per_lot:.0f} < ₹{MIN_NET_CREDIT_PER_LOT:.0f}")

    logger.success(
        f"ATM Straddle OK: CE {ce_ltp:.2f} + PE {pe_ltp:.2f} → net≈₹{net_credit_per_lot:.0f}/lot (DTE={dte})"
    )

    # 7) Build SELL orders (LIMIT by default, tick-safe)
    ordertype = (DEFAULT_ORDER_TYPE or "LIMIT").upper()
    if ordertype not in ("MARKET", "LIMIT"):
        logger.warning(f"Unexpected DEFAULT_ORDER_TYPE={ordertype!r}; forcing LIMIT")
        ordertype = "LIMIT"

    ce_px = _sell_limit_from_ltp(ce_ltp) if ordertype == "LIMIT" else "0"
    pe_px = _sell_limit_from_ltp(pe_ltp) if ordertype == "LIMIT" else "0"

    ordertag = f"ATM-{und}-{expiry_d.strftime('%d%b').upper()}"[:19]  # keeps under Angel’s cap

    sell_ce = {
        "variety": ORDER_VARIETY,
        "tradingsymbol": ce_ts,
        "symboltoken": ce_tok,
        "transactiontype": "SELL",
        "exchange": EXCHANGE,
        "ordertype": ordertype,
        "producttype": PRODUCT_TYPE,
        "duration": DURATION,
        "price": ce_px,
        "quantity": int(qty),
        "ordertag": ordertag,
    }
    sell_pe = {
        "variety": ORDER_VARIETY,
        "tradingsymbol": pe_ts,
        "symboltoken": pe_tok,
        "transactiontype": "SELL",
        "exchange": EXCHANGE,
        "ordertype": ordertype,
        "producttype": PRODUCT_TYPE,
        "duration": DURATION,
        "price": pe_px,
        "quantity": int(qty),
        "ordertag": ordertag,
    }

    return [sell_ce, sell_pe]
