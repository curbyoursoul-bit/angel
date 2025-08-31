# config.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Iterable, Optional

# -----------------------------------------------------------------------------
# dotenv (optional)
# -----------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(override=False)
except Exception:
    pass

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _intish(x: str, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default

def _i(name: str, default: int = 0) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    return _intish(v, default)

def _f(name: str, default: float = 0.0) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default

def _s(name: str, default: str = "") -> str:
    return os.getenv(name, default)

def _list(name: str, default: Optional[Iterable[str]] = None, sep: str = ",") -> list[str]:
    v = os.getenv(name)
    if not v:
        return list(default or [])
    return [p.strip() for p in v.split(sep) if p.strip()]

def _choice(name: str, choices: Iterable[str], default: str) -> str:
    v = _s(name, default).upper()
    up = {c.upper(): c for c in choices}
    return up.get(v, default)

def _p(name: str, default: Path) -> Path:
    """Read a path env with a safe Path default."""
    return Path(os.getenv(name, str(default)))

# -----------------------------------------------------------------------------
# Project paths
# -----------------------------------------------------------------------------
# Make REPO the *repo root* (folder containing this file)
REPO: Path        = Path(__file__).resolve().parent
ROOT: Path        = REPO  # back-compat alias

DATA_DIR: Path    = _p("DATA_DIR", REPO / "data")
LOG_DIR: Path     = _p("LOG_DIR",  REPO / "logs")
TMP_DIR: Path     = _p("TMP_DIR",  REPO / "tmp")
TOKEN_FILE: Path  = _p("TOKEN_FILE", REPO / "utils" / "token.json")

for d in (DATA_DIR, LOG_DIR, TMP_DIR, TOKEN_FILE.parent):
    d.mkdir(parents=True, exist_ok=True)

INSTRUMENTS_CSV: Path   = _p("INSTRUMENTS_CSV", DATA_DIR / "OpenAPIScripMaster.csv")
INSTRUMENTS_JSON: Path  = _p("INSTRUMENTS_JSON", DATA_DIR / "OpenAPIScripMaster.json")

# -----------------------------------------------------------------------------
# Credentials
# -----------------------------------------------------------------------------
API_KEY: str      = _s("API_KEY")
CLIENT_CODE: str  = _s("CLIENT_CODE") or _s("CLIENT_ID")
PASSWORD: str     = _s("PASSWORD")
MPIN: str         = _s("MPIN")
TOTP_SECRET: str  = _s("TOTP_SECRET")

# -----------------------------------------------------------------------------
# Runtime flags
# -----------------------------------------------------------------------------
DRY_RUN: bool         = _b("DRY_RUN", True)
CONFIRM_PLACE: bool   = _b("CONFIRM_PLACE", False)
PAPER_TRADE: bool     = _b("PAPER_TRADE", False)
TIMEZONE_IST: str     = "Asia/Kolkata"
ALLOW_AMO: bool       = _b("ALLOW_AMO", False)

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL: str        = _choice("LOG_LEVEL", ["DEBUG", "INFO", "WARNING", "ERROR"], "INFO")
APP_LOG_PATH: Path    = _p("APP_LOG_PATH", LOG_DIR / "app.log")
ROTATE_SIZE_MB: int   = _i("LOG_ROTATE_SIZE_MB", 10)
RETENTION_DAYS: int   = _i("LOG_RETENTION_DAYS", 14)

# -----------------------------------------------------------------------------
# Market hours
# -----------------------------------------------------------------------------
MARKET_OPEN_HH: int   = _i("MARKET_OPEN_HH", 9)
MARKET_OPEN_MM: int   = _i("MARKET_OPEN_MM", 15)
MARKET_CLOSE_HH: int  = _i("MARKET_CLOSE_HH", 15)
MARKET_CLOSE_MM: int  = _i("MARKET_CLOSE_MM", 30)

# -----------------------------------------------------------------------------
# Order defaults
# -----------------------------------------------------------------------------
DEFAULT_ORDER_TYPE: str = (
    _s("DEFAULT_ORDER_TYPE", _s("ORDER_TYPE_DEFAULT", "MARKET"))
).upper()
DEFAULT_PRODUCT: str    = _choice("DEFAULT_PRODUCT", ["INTRADAY", "DELIVERY", "CNC", "MARGIN"], "INTRADAY")
DEFAULT_DURATION: str   = _choice("DEFAULT_DURATION", ["DAY", "IOC"], "DAY")
DEFAULT_VARIETY: str    = _choice("ORDER_DEFAULT_VARIETY", ["NORMAL", "STOPLOSS", "AMO"], "NORMAL")

ORDER_DEDUPE_MS: int       = _i("ORDER_DEDUPE_MS", 1200)
ORDER_PLACE_COOLDOWN_S: int = _i("ORDER_PLACE_COOLDOWN_S", 1)
PRICE_PADDING_TICKS: int    = _i("PRICE_PADDING_TICKS", 1)
# NOTE: 0.25 here means 25% — if you intend 1%, set 0.01
STOP_LIMIT_BUFFER_PCT: float = _f("STOP_LIMIT_BUFFER_PCT", 0.25)

# -----------------------------------------------------------------------------
# Risk
# -----------------------------------------------------------------------------
RISK_MAX_LOSS: float       = _f("RISK_MAX_LOSS", 0.0)
RISK_MAX_QTY: int          = _i("RISK_MAX_QTY", 0)
RISK_MAX_ORDERS: int       = _i("RISK_MAX_ORDERS", 0)
SLIPPAGE_BPS: float        = _f("SLIPPAGE_BPS", 0.0)
SLIPPAGE_PCT: float        = _f("SLIPPAGE_PCT", 0.0)
VOL_MAX_SPREAD_PCT: float  = _f("VOL_MAX_SPREAD_PCT", 0.08)

# -----------------------------------------------------------------------------
# Exits / OCO
# -----------------------------------------------------------------------------
AUTO_STOPS_ENABLED: bool    = _b("AUTO_STOPS_ENABLED", True)
STOP_LOSS_PCT: float        = _f("STOP_LOSS_PCT", 1.0)
AUTO_TARGETS_ENABLED: bool  = _b("AUTO_TARGETS_ENABLED", False)
TARGET_PCT: float           = _f("TARGET_PCT", 1.0)
OCO_ENABLE: bool            = _b("OCO_ENABLE", True)

TRAIL_ENABLE: bool          = _b("TRAIL_ENABLE", True)
TRAIL_COOLDOWN_SECS: int    = _i("TRAIL_COOLDOWN_SECS", 300)
TRAIL_STEP_PCT: float       = _f("TRAIL_STEP_PCT", 0.50)
TRAIL_ARM_AFTER_PCT: float  = _f("TRAIL_ARM_AFTER_PCT", 0.25)

ATTACH_LOOKBACK_MINS: int   = _i("ATTACH_LOOKBACK_MINS", 90)
ATTACH_EXITS_VERBOSE: bool  = _b("ATTACH_EXITS_VERBOSE", False)

# -----------------------------------------------------------------------------
# Streaming / tick
# -----------------------------------------------------------------------------
WS_ENABLE: bool             = _b("WS_ENABLE", True)
PULL_TICK_INTERVAL_S: float = _f("PULL_TICK_INTERVAL_S", 1.0)

# -----------------------------------------------------------------------------
# LTP / quotes
# -----------------------------------------------------------------------------
LTP_CACHE_TTL_S: float      = _f("LTP_CACHE_TTL_S", 1.0)
QUOTE_RETRY: int            = _i("QUOTE_RETRY", 2)
QUOTE_RETRY_DELAY_S: float  = _f("QUOTE_RETRY_DELAY_S", 0.25)

# -----------------------------------------------------------------------------
# Strategy defaults
# -----------------------------------------------------------------------------
ATM_UNDERLYING: str         = _s("ATM_UNDERLYING", "BANKNIFTY")
ATM_STRIKE_STEP: int        = _i("ATM_STRIKE_STEP", 100)
ATM_LOTS: int               = _i("ATM_LOTS", 1)
ATM_ORDER_VARIETY: str      = _choice("ATM_ORDER_VARIETY", ["NORMAL", "AMO"], "NORMAL")

EMA_FAST: int               = _i("EMA_FAST", 5)
EMA_SLOW: int               = _i("EMA_SLOW", 20)
EMA_TIMEFRAME: str          = _s("EMA_TIMEFRAME", "5m")

VWAP_Z_ENTRY: float         = _f("VWAP_Z_ENTRY", 1.0)
VWAP_Z_EXIT: float          = _f("VWAP_Z_EXIT", 0.25)

VIX_HIGH: float             = _f("VIX_HIGH", 18.0)
VIX_LOW: float              = _f("VIX_LOW", 12.0)

ORB_WINDOW_MIN: int         = _i("ORB_WINDOW_MIN", 15)

ZS_LOOKBACK: int            = _i("ZS_LOOKBACK", 100)
ZS_ENTRY_Z: float           = _f("ZS_ENTRY_Z", 2.0)
ZS_EXIT_Z: float            = _f("ZS_EXIT_Z", 0.5)

# -----------------------------------------------------------------------------
# Files (paths used by tools/utils; self-contained & back-compat)
# -----------------------------------------------------------------------------
TRADE_LOG_CSV: Path = _p("TRADE_LOG_CSV", DATA_DIR / "trade_log.csv")
ORDERS_CSV: Path    = _p("ORDERS_CSV",    DATA_DIR / "orders.csv")
ORDERS_LOG_CSV: Path = ORDERS_CSV  # alias

# OCO registry path (needed by utils/oco_registry.py)
OCO_REGISTRY_JSON: Path = _p("OCO_REGISTRY_JSON", DATA_DIR / "oco_registry.json")

# -----------------------------------------------------------------------------
# Alerts
# -----------------------------------------------------------------------------
ALERTS_ENABLE: bool         = _b("ALERTS_ENABLE", False)
ALERTS_WEBHOOK_URL: str     = _s("ALERTS_WEBHOOK_URL", "")
ALERTS_EMAIL: str           = _s("ALERTS_EMAIL", "")

# -----------------------------------------------------------------------------
# Backtest
# -----------------------------------------------------------------------------
BACKTEST_DATA_DIR: Path     = _p("BACKTEST_DATA_DIR", DATA_DIR)
BACKTEST_CAPITAL: float     = _f("BACKTEST_CAPITAL", 100000.0)
BACKTEST_COMMISSION_PCT: float = _f("BACKTEST_COMMISSION_PCT", 0.0)

# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------
def validate() -> None:
    try:
        from loguru import logger  # type: ignore
    except Exception:
        class _L: 
            def warning(self, *a, **k): print(*a)
        logger = _L()  # fallback

    if not DRY_RUN and not PAPER_TRADE:
        missing = [k for k, v in {
            "API_KEY": API_KEY,
            "CLIENT_CODE": CLIENT_CODE,
            "PASSWORD": PASSWORD,
            "MPIN": MPIN,
            "TOTP_SECRET": TOTP_SECRET,
        }.items() if not v]
        if missing:
            logger.warning(f"[config] WARNING: missing credentials in LIVE mode: {', '.join(missing)}")

    if not TOKEN_FILE.parent.exists():
        logger.warning(f"[config] WARNING: TOKEN_FILE parent folder does not exist -> {TOKEN_FILE.parent}")

    if not INSTRUMENTS_CSV.exists() and not INSTRUMENTS_JSON.exists():
        logger.warning("[config] WARNING: Instruments file not found (CSV/JSON)")

try:
    validate()
except Exception:
    pass

# For back-compat only: this is a *string*, not tzinfo. Prefer utils.market_hours.IST
IST = TIMEZONE_IST
