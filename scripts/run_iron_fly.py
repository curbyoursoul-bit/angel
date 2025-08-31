from __future__ import annotations
from typing import List, Dict, Any, Tuple
import math
from datetime import date
import pandas as pd
from loguru import logger

from utils.ltp_fetcher import get_ltp
from utils.instruments import (
    load_options, pick_atm_strike, get_option_rows, nearest_expiry
)
from utils.expiry import next_thursday
from config import DEFAULT_ORDER_TYPE

# ======= knobs you can tweak =======
UNDERLYING    = "NIFTY"     # NIFTY fits ~₹1L margin better than BANKNIFTY
LOTS          = 1
BASE_WING_DISTANCE = 150     # points for NIFTY; ~300 for BANKNIFTY
WING_STEP         = 50
MAX_WING_DISTANCE = 400
MIN_NET_CREDIT_PER_LOT = 400   # guard for minimum credit

# pricing/controls
TICK_SIZE            = 0.05    # Angel option tick
MIN_PRICE            = 0.05
PRICE_PAD_PCT_SELL   = 0.03    # 3% below LTP for sells
PRICE_PAD_PCT_BUY    = 0.03    # 3% above LTP for buys

# --- Black–Scholes integration (toggleable) ---
USE_BS_FAIR_VALUE     = True   # flip to False to bypass BS checks
RISK_FREE             = 0.065  # ~6.5% India
IV_GUESS              = 0.20   # 20% annual IV as starting point
BS_MAX_MISPRICE_PCT   = 0.30   # allow up to 30% deviation from BS fair value

# Risk-quality guard: ensure credit is a % of structural risk
MIN_CREDIT_OF_RISK_PCT = 0.20  # at least 20% of (distance * lot_size)
# ===================================

EXCHANGE      = "NFO"
ORDER_VARIETY = "NORMAL"
PRODUCT_TYPE  = "INTRADAY"
DURATION      = "DAY"
STEP_MAP      = {"NIFTY": 50, "BANKNIFTY": 100}
SPOT_TOKEN    = {"NIFTY": "26000", "BANKNIFTY": "26009"}  # NSE index tokens

# ---------- optional Black–Scholes import (fallback) ----------
try:
    from utils.black_scholes import BlackScholes as _BS
except Exception:
    class _BS:
        def __init__(self, spot, strike, expiry_days, rate=0.065, volatility=0.20):
            self.S = float(spot)
            self.K = float(strike)
            self.T = max(1e-6, expiry_days / 365.0)
            self.r = float(rate)
            self.sigma = max(1e-6, float(volatility))
        def _d1(self):
            import math
            return (math.log(self.S / self.K) + (self.r + 0.5 * self.sigma**2) * self.T) / (self.sigma * math.sqrt(self.T))
        def _d2(self):
            import math
            return self._d1() - self.sigma * math.sqrt(self.T)
        def call_price(self):
            import math
            from math import erf
            d1 = self._d1(); d2 = self._d2()
            N = lambda x: 0.5 * (1.0 + erf(x / math.sqrt(2)))
            return self.S * N(d1) - self.K * math.exp(-self.r * self.T) * N(d2)
        def put_price(self):
            import math
            from math import erf
            d1 = self._d1(); d2 = self._d2()
            N = lambda x: 0.5 * (1.0 + erf(x / math.sqrt(2)))
            return self.K * math.exp(-self.r * self.T) * N(-d2) - self.S * N(-d1)
# ---------------------------------------------------------------

# ---------- helpers ----------
def _cell(row: pd.Series, *keys: str) -> str:
    for k in keys:
        if k in row:
            v = row[k]
            if pd.notna(v):
                s = str(v).strip()
                if s and s.upper() not in ("NAN", "NA", "NONE"):
                    return s
    return ""

def _row_to_pair(row: pd.Series) -> Tuple[str, str]:
    return _cell(row, "tradingsymbol", "symbol", "symbolname"), _cell(row, "symboltoken", "token")

def _round_to_tick(px: float, tick: float = TICK_SIZE) -> float:
    return max(MIN_PRICE, round(px / tick) * tick)

def _limit_price(side: str, ltp: float) -> str:
    ltp = float(ltp)
    if side.upper() == "SELL":
        px = ltp * (1.0 - PRICE_PAD_PCT_SELL)
    else:
        px = ltp * (1.0 + PRICE_PAD_PCT_BUY)
    px = _round_to_tick(px)
    return f"{px:.2f}"

def _build(ts: str, tok: str, side: str, ordertype: str, qty: int, limit_price: float | None) -> Dict[str, Any]:
    if ordertype == "MARKET":
        price = "0"
    else:
        if limit_price is None:
            raise ValueError("LIMIT order requires limit_price")
        price = limit_price
    return {
        "variety": ORDER_VARIETY,
        "tradingsymbol": ts,
        "symboltoken": tok,
        "transactiontype": side,
        "exchange": EXCHANGE,
        "ordertype": ordertype,
        "producttype": PRODUCT_TYPE,
        "duration": DURATION,
        "price": price,
        "quantity": int(qty),
    }

def _safe_option_ltp(smart, exchange: str, ts: str, token: str, retries: int = 2) -> float:
    last_err = None
    for _ in range(retries + 1):
        try:
            px = float(get_ltp(smart, exchange=exchange, tradingsymbol=ts, symboltoken=token))
            if MIN_PRICE <= px < 1000:
                return px
            last_err = f"outlier ltp={px}"
        except Exception as e:
            last_err = str(e)
    raise RuntimeError(f"LTP fetch bad for {ts} ({token}): {last_err}")

def _bs_okay(spot: float, strike: float, dte: int, ltp: float, is_call: bool) -> bool:
    if not USE_BS_FAIR_VALUE:
        return True
    bs = _BS(spot=spot, strike=strike, expiry_days=max(1, dte), rate=RISK_FREE, volatility=IV_GUESS)
    fair = max(MIN_PRICE, float(bs.call_price() if is_call else bs.put_price()))
    dev  = abs(ltp - fair) / fair
    ok   = dev <= BS_MAX_MISPRICE_PCT
    if not ok:
        kind = "CALL" if is_call else "PUT"
        logger.warning(f"BS: {kind} K={strike} LTP={ltp:.2f} fair≈{fair:.2f} dev={dev:.0%} > {BS_MAX_MISPRICE_PCT:.0%}")
    return ok
# ---------------------------------------------------------------

def _try_wings(
    smart,
    opts: pd.DataFrame,
    expiry_d,
    atm: int,
    step: int,
    lot_size: int,
    ordertype: str,
    spot: float,
):
    """
    Pick ±distance for hedge wings with:
      1) BS fairness on all legs,
      2) net credit/lot ≥ MIN_NET_CREDIT_PER_LOT,
      3) credit ≥ MIN_CREDIT_OF_RISK_PCT * (dist * lot_size).
    """
    dte = (expiry_d - date.today()).days

    # Short ATM legs
    atm_ce, atm_pe = get_option_rows(opts, expiry_d=expiry_d, strike_rupees=atm, step=step)
    atm_ce_ts, atm_ce_tok = _row_to_pair(atm_ce)
    atm_pe_ts, atm_pe_tok = _row_to_pair(atm_pe)
    if not (atm_ce_ts and atm_ce_tok and atm_pe_ts and atm_pe_tok):
        raise RuntimeError("ATM option tokens missing")

    qty = lot_size * LOTS

    # Shorts LTP + BS check
    ce_ltp = _safe_option_ltp(smart, "NFO", atm_ce_ts, atm_ce_tok)
    pe_ltp = _safe_option_ltp(smart, "NFO", atm_pe_ts, atm_pe_tok)
    if not (_bs_okay(spot, atm, dte, ce_ltp, is_call=True) and _bs_okay(spot, atm, dte, pe_ltp, is_call=False)):
        raise RuntimeError("ATM legs failed BS fairness check; toggle USE_BS_FAIR_VALUE to bypass")

    short_credit_per_lot = (ce_ltp + pe_ltp) * lot_size
    logger.info(
        f"Short ATM credit (rough): CE {ce_ltp:.2f} + PE {pe_ltp:.2f} = "
        f"{short_credit_per_lot/lot_size:.2f} × {lot_size} = ₹{short_credit_per_lot:.0f}"
    )

    chosen = None
    for dist in range(BASE_WING_DISTANCE, MAX_WING_DISTANCE + WING_STEP, WING_STEP):
        lower = atm - dist
        upper = atm + dist

        up_ce,  _      = get_option_rows(opts, expiry_d=expiry_d, strike_rupees=upper, step=step)  # buy CE (upper)
        _,      low_pe = get_option_rows(opts, expiry_d=expiry_d, strike_rupees=lower, step=step)  # buy PE (lower)

        up_ce_ts,  up_ce_tok  = _row_to_pair(up_ce)
        low_pe_ts, low_pe_tok = _row_to_pair(low_pe)
        if not (up_ce_ts and up_ce_tok and low_pe_ts and low_pe_tok):
            continue

        up_ce_ltp  = _safe_option_ltp(smart, "NFO", up_ce_ts,  up_ce_tok)
        low_pe_ltp = _safe_option_ltp(smart, "NFO", low_pe_ts, low_pe_tok)

        # BS checks on wings
        if not (_bs_okay(spot, upper, dte, up_ce_ltp, is_call=True) and
                _bs_okay(spot, lower, dte, low_pe_ltp, is_call=False)):
            continue

        hedge_cost_per_lot = (up_ce_ltp + low_pe_ltp) * lot_size
        net_credit = short_credit_per_lot - hedge_cost_per_lot

        # Risk quality guard
        min_credit_req = MIN_CREDIT_OF_RISK_PCT * (dist * lot_size)
        logger.info(
            f"Wings ±{dist}: hedge≈₹{hedge_cost_per_lot:.0f} → credit≈₹{net_credit:.0f}/lot "
            f"(min_credit_req≈₹{min_credit_req:.0f})"
        )

        if net_credit >= MIN_NET_CREDIT_PER_LOT and net_credit >= min_credit_req:
            chosen = (dist, up_ce_ts, up_ce_tok, up_ce_ltp, low_pe_ts, low_pe_tok, low_pe_ltp, net_credit)
            break

    # Fallback distance
    if not chosen:
        logger.warning(
            f"No distance met guards up to ±{MAX_WING_DISTANCE}. "
            f"Proceeding with BASE_WING_DISTANCE=±{BASE_WING_DISTANCE}."
        )
        dist = BASE_WING_DISTANCE
        upper = atm + dist
        lower = atm - dist
        up_ce,  _       = get_option_rows(opts, expiry_d=expiry_d, strike_rupees=upper, step=step)
        _,      low_pe  = get_option_rows(opts, expiry_d=expiry_d, strike_rupees=lower, step=step)
        up_ce_ts,  up_ce_tok  = _row_to_pair(up_ce)
        low_pe_ts, low_pe_tok = _row_to_pair(low_pe)
        up_ce_ltp  = _safe_option_ltp(smart, "NFO", up_ce_ts,  up_ce_tok)
        low_pe_ltp = _safe_option_ltp(smart, "NFO", low_pe_ts, low_pe_tok)
        # still log BS fairness outcomes
        if USE_BS_FAIR_VALUE:
            if not _bs_okay(spot, upper, dte, up_ce_ltp, is_call=True):
                logger.warning("Fallback wing CE failed BS fairness.")
            if not _bs_okay(spot, lower, dte, low_pe_ltp, is_call=False):
                logger.warning("Fallback wing PE failed BS fairness.")
        net_credit = float("nan")
    else:
        dist, up_ce_ts, up_ce_tok, up_ce_ltp, low_pe_ts, low_pe_tok, low_pe_ltp, net_credit = chosen

    # 4 orders with prices
    qty = lot_size * LOTS
    ordertype = DEFAULT_ORDER_TYPE.upper() if DEFAULT_ORDER_TYPE else "LIMIT"
    if ordertype not in ("MARKET", "LIMIT"):
        logger.warning(f"Unexpected DEFAULT_ORDER_TYPE={ordertype!r}; forcing LIMIT")
        ordertype = "LIMIT"

    sell_ce_price = sell_pe_price = buy_up_price = buy_low_price = None
    if ordertype == "LIMIT":
        sell_ce_price = _limit_price("SELL", ce_ltp)
        sell_pe_price = _limit_price("SELL", pe_ltp)
        buy_up_price  = _limit_price("BUY",  up_ce_ltp)
        buy_low_price = _limit_price("BUY",  low_pe_ltp)

    sell_ce = _build(atm_ce_ts, atm_ce_tok, "SELL", ordertype, qty, sell_ce_price)
    sell_pe = _build(atm_pe_ts, atm_pe_tok, "SELL", ordertype, qty, sell_pe_price)
    buy_up  = _build(up_ce_ts,  up_ce_tok,  "BUY",  ordertype, qty, buy_up_price)   # upper CE
    buy_low = _build(low_pe_ts, low_pe_tok, "BUY",  ordertype, qty, buy_low_price)  # lower PE

    return [sell_ce, sell_pe, buy_up, buy_low], {
        "atm": atm,
        "dist": dist,
        "net_credit": net_credit,
        "dte": dte,
        "prices": {
            "sell_ce_ltp": ce_ltp,
            "sell_pe_ltp": pe_ltp,
            "buy_up_ltp": up_ce_ltp,
            "buy_low_ltp": low_pe_ltp,
        }
    }

def run(smart) -> List[Dict[str, Any]]:
    und = UNDERLYING.upper()
    step = STEP_MAP.get(und, 50)
    token = SPOT_TOKEN[und]

    # 1) Spot
    spot = float(get_ltp(smart, exchange="NSE", tradingsymbol=und, symboltoken=token))
    logger.info(f"{und} spot: {spot:.2f}")

    # 2) Options universe + expiry
    weekly_expiry = next_thursday()
    opts = load_options(und)
    expiry_d = nearest_expiry(opts, target=weekly_expiry, window_days=10)
    if expiry_d != weekly_expiry:
        logger.warning(f"Adjusting expiry {weekly_expiry} → nearest {expiry_d}")

    # 3) ATM
    atm = pick_atm_strike(spot, step=step)
    logger.info(f"ATM strike: {atm}")

    # 4) Qty
    atm_ce, atm_pe = get_option_rows(opts, expiry_d=expiry_d, strike_rupees=atm, step=step)
    lot_size = int(atm_ce.get("lotsize") or atm_pe.get("lotsize") or 0) or 1
    logger.info(f"Lot size: {lot_size} | Lots: {LOTS} | Qty: {lot_size*LOTS}")

    # 5) Build orders w/ BS checks + risk-quality filter
    ordertype = (DEFAULT_ORDER_TYPE or "LIMIT").upper()
    orders, meta = _try_wings(smart, opts, expiry_d, atm, step, lot_size, ordertype, spot)
    nc = meta["net_credit"]
    nc_txt = f"{nc:.0f}" if isinstance(nc, (int, float)) and not math.isnan(nc) else "n/a"
    logger.success(
        f"{und} iron fly prepared: SELL {atm} CE/PE, BUY {atm+meta['dist']} CE & {atm-meta['dist']} PE "
        f"(DTE={meta['dte']}; net credit≈₹{nc_txt}/lot)"
    )
    return orders
