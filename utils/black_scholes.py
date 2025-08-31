# utils/black_scholes.py
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Literal, Tuple, Optional

EPS_T = 1e-8           # floor for time to expiry (years)
EPS_SIGMA = 1e-8       # floor for volatility
MAX_IV = 6.0           # hard cap for implied vol search (600%)
MIN_IV = 1e-6

# ---- standard normal helpers (no SciPy) -------------------------------------
def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def _norm_cdf(x: float) -> float:
    # Abramowitz & Stegun approximation via erf
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

@dataclass
class BSResult:
    call: float
    put: float
    delta_call: float
    delta_put: float
    gamma: float
    vega_pct: float      # Vega for +1% IV change
    theta_call_day: float
    theta_put_day: float
    rho_call: float
    rho_put: float
    d1: float
    d2: float
    T: float
    r: float
    q: float
    sigma: float

class BlackScholes:
    """
    Black–Scholes–Merton with continuous dividend yield (q).
    - All greeks are "per option" (not multiplied by lot size).
    - Theta is per calendar day.
    - Vega is per +1% IV change.
    """
    def __init__(
        self,
        spot: float,
        strike: float,
        expiry_days: float,
        rate: float = 0.065,        # risk-free (annual)
        volatility: float = 0.20,   # annual IV
        dividend_yield: float = 0.0 # q; for indices often ~0
    ):
        self.S = float(spot)
        self.K = float(strike)
        self.T = max(float(expiry_days) / 365.0, EPS_T)  # days → years
        self.r = float(rate)
        self.sigma = max(float(volatility), EPS_SIGMA)
        self.q = float(dividend_yield)

    # ----- internals -----
    def _d1(self) -> float:
        S, K, T, r, q, s = self.S, self.K, self.T, self.r, self.q, self.sigma
        return (math.log(S / K) + (r - q + 0.5 * s * s) * T) / (s * math.sqrt(T))

    def _d2(self, d1: Optional[float] = None) -> float:
        if d1 is None:
            d1 = self._d1()
        return d1 - self.sigma * math.sqrt(self.T)

    # ----- prices -----
    def call_price(self) -> float:
        d1 = self._d1()
        d2 = self._d2(d1)
        S, K, T, r, q = self.S, self.K, self.T, self.r, self.q
        return S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)

    def put_price(self) -> float:
        d1 = self._d1()
        d2 = self._d2(d1)
        S, K, T, r, q = self.S, self.K, self.T, self.r, self.q
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)

    # ----- greeks & bundle -----
    def greeks(self) -> dict:
        d1 = self._d1()
        d2 = self._d2(d1)
        S, K, T, r, q, s = self.S, self.K, self.T, self.r, self.q, self.sigma
        sqrtT = math.sqrt(T)

        Nd1 = _norm_cdf(d1)
        Nnd1 = _norm_cdf(-d1)
        Nd2 = _norm_cdf(d2)
        Nnd2 = _norm_cdf(-d2)
        nd1 = _norm_pdf(d1)

        # Deltas (with dividend yield q)
        delta_call = math.exp(-q * T) * Nd1
        delta_put = math.exp(-q * T) * (Nd1 - 1)

        # Gamma
        gamma = (math.exp(-q * T) * nd1) / (S * s * sqrtT)

        # Vega (per +1% IV)
        vega_pct = (S * math.exp(-q * T) * nd1 * sqrtT) / 100.0

        # Theta (per day)
        theta_call = (
            - (S * math.exp(-q * T) * nd1 * s) / (2 * sqrtT)
            - r * K * math.exp(-r * T) * Nd2
            + q * S * math.exp(-q * T) * Nd1
        ) / 365.0

        theta_put = (
            - (S * math.exp(-q * T) * nd1 * s) / (2 * sqrtT)
            + r * K * math.exp(-r * T) * Nnd2
            - q * S * math.exp(-q * T) * Nnd1
        ) / 365.0

        # Rho
        rho_call = (K * T * math.exp(-r * T) * Nd2)
        rho_put  = (-K * T * math.exp(-r * T) * Nnd2)

        return {
            "delta_call": delta_call,
            "delta_put": delta_put,
            "gamma": gamma,
            "vega": vega_pct,
            "theta_call": theta_call,
            "theta_put": theta_put,
            "rho_call": rho_call,
            "rho_put": rho_put,
            "d1": d1,
            "d2": d2,
        }

    def price_and_greeks(self) -> BSResult:
        g = self.greeks()
        call = self.call_price()
        put = self.put_price()
        return BSResult(
            call=call,
            put=put,
            delta_call=g["delta_call"],
            delta_put=g["delta_put"],
            gamma=g["gamma"],
            vega_pct=g["vega"],
            theta_call_day=g["theta_call"],
            theta_put_day=g["theta_put"],
            rho_call=g["rho_call"],
            rho_put=g["rho_put"],
            d1=g["d1"],
            d2=g["d2"],
            T=self.T,
            r=self.r,
            q=self.q,
            sigma=self.sigma,
        )

    def parity_error(self) -> float:
        S, K, T, r, q = self.S, self.K, self.T, self.r, self.q
        return (self.call_price() - self.put_price()) - (S * math.exp(-q * T) - K * math.exp(-r * T))

    # ----- implied volatility -----
    def implied_vol(
        self,
        option_price: float,
        kind: Literal["C","P"] = "C",
        tol: float = 1e-6,
        max_iter: int = 100
    ) -> float:
        # Initial guess (Brenner–Subrahmanyam style)
        S, T = self.S, self.T
        guess = max(min(math.sqrt(2 * math.pi / max(T, EPS_T)) * option_price / max(S, 1e-12), 1.5), MIN_IV)
        sigma = guess

        def price_and_vega(sig: float) -> Tuple[float, float]:
            sig = max(sig, EPS_SIGMA)
            self.sigma = sig
            p = self.call_price() if kind == "C" else self.put_price()
            d1 = self._d1()
            v = self.S * math.exp(-self.q * self.T) * _norm_pdf(d1) * math.sqrt(self.T)  # vega per 1.0 σ
            return p, v

        # Newton–Raphson
        for _ in range(max_iter):
            p, v = price_and_vega(sigma)
            diff = p - option_price
            if abs(diff) < tol:
                return min(max(sigma, MIN_IV), MAX_IV)
            if v < 1e-10:
                break
            sigma -= diff / v
            if not (MIN_IV <= sigma <= MAX_IV):
                break

        # Bisection fallback
        lo, hi = MIN_IV, MAX_IV
        for _ in range(120):
            mid = 0.5 * (lo + hi)
            pmid, _ = price_and_vega(mid)
            if pmid > option_price:
                hi = mid
            else:
                lo = mid
            if abs(hi - lo) < tol:
                return 0.5 * (lo + hi)
        return 0.5 * (lo + hi)

# -------- convenience functional API --------
def bs_price(
    S: float, K: float, expiry_days: float,
    r: float = 0.065, sigma: float = 0.20, q: float = 0.0
) -> Tuple[float, float]:
    bs = BlackScholes(S, K, expiry_days, r, sigma, q)
    return bs.call_price(), bs.put_price()

def bs_greeks(
    S: float, K: float, expiry_days: float,
    r: float = 0.065, sigma: float = 0.20, q: float = 0.0
) -> dict:
    bs = BlackScholes(S, K, expiry_days, r, sigma, q)
    return bs.greeks()

def bs_implied_vol(
    option_price: float, kind: Literal["C","P"], S: float, K: float, expiry_days: float,
    r: float = 0.065, q: float = 0.0
) -> float:
    bs = BlackScholes(S, K, expiry_days, r, 0.20, q)
    # --- convenience helper used by scripts/banknifty_bs_demo.py -----------------
from dataclasses import dataclass
import datetime as _dt
try:
    import pytz as _pytz
    _IST = _pytz.timezone("Asia/Kolkata")
except Exception:
    _IST = None  # fallback to naive dates

@dataclass
class _TheoOut:
    price: float
    delta: float
    gamma: float
    vega: float           # per +1% IV change (matches vega_pct)
    theta: float          # per calendar day (matching theta_call_day/theta_put_day)

def _days_until(expiry: _dt.date) -> float:
    # Count whole/partial days from "now" (IST if available) to 23:59:59 of expiry date.
    if _IST:
        now = _dt.datetime.now(_IST)
        end = _IST.localize(_dt.datetime.combine(expiry, _dt.time(23, 59, 59)))
    else:
        now = _dt.datetime.now()
        end = _dt.datetime.combine(expiry, _dt.time(23, 59, 59))
    delta = (end - now).total_seconds() / 86400.0
    return max(delta, 0.0)

def bs_with_expiry_date(
    *,
    spot: float,
    strike: float,
    expiry: _dt.date,
    rate: float = 0.065,
    iv: float = 0.20,
    option: str = "CE",       # "CE"/"PE" or "C"/"P"
    q: float = 0.0
):
    """
    Convenience wrapper: compute price/greeks for a single option given a calendar expiry date.
    Returns an object with attributes: price, delta, gamma, vega (per +1% IV), theta (per day).
    """
    kind = option.strip().upper()
    if kind in ("CE", "C"):
        is_call = True
    elif kind in ("PE", "P"):
        is_call = False
    else:
        raise ValueError(f"option must be CE/PE or C/P, got {option!r}")

    days = max(_days_until(expiry), 1e-6)  # avoid zero
    bs = BlackScholes(spot=spot, strike=strike, expiry_days=days, rate=rate, volatility=iv, dividend_yield=q)
    g = bs.price_and_greeks()

    if is_call:
        return _TheoOut(price=g.call, delta=g.delta_call, gamma=g.gamma, vega=g.vega_pct, theta=g.theta_call_day)
    else:
        return _TheoOut(price=g.put,  delta=g.delta_put,  gamma=g.gamma, vega=g.vega_pct, theta=g.theta_put_day)

    return bs.implied_vol(option_price, kind=kind)
