# ---- SmartAPI credentials ----
API_KEY=U66ZQHt5 
CLIENT_CODE=R253564
MPIN=8652
PIN=8652
TOTP_SECRET=PSQRJUXSHJSQ57PYZCZOXCF6E4

# ---- Trading toggles ----
DRY_RUN=true                    # true = simulate only, false = send LIVE
CONFIRM_PLACE=yes               # must be 'yes' to allow live trades
DEFAULT_ORDER_TYPE=LIMIT        # or MARKET
SLIPPAGE_PCT=0.03               # 3% price padding for LIMIT orders

BYPASS_MARKET_HOURS=true
ALLOW_AFTER_HOURS_PREVIEW=true
ENFORCE_MARKET_HOURS=false
EXIT_ON_TIME_ENABLED=0          # 0 = no cutoff, or set minutes before 15:30

# ---- Risk ----
RISK_MAX_LOSS=-2000             # stop trading if PnL below this
RISK_MAX_QTY=200                # max total qty across all legs

# ---- Stops/targets ----
STOP_LOSS_PCT=0.20              # 20% SL vs entry
STOP_LIMIT_BUFFER_PCT=0.01      # buffer for SL-L orders
AUTO_STOPS_ENABLED=true
AUTO_TARGETS_ENABLED=true

AUTO_TRAIL_ENABLED=true
TRAIL_COOLDOWN_SECS=300
TRAIL_TRIGGER_PCT=0.40
TRAIL_THROTTLE_SECS=15
AUTO_TRAIL_PCT=0.50
TRAIL_MIN_DELTA=0.10


# ---- Instruments cache ----
INSTRUMENTS_CSV=data/OpenAPIScripMaster.csv

# ---- Strategy knobs ----
USE_BS_FAIR_VALUE=true
BS_MAX_MISPRICE_PCT=0.60

# ---- Logging ----
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_DIR=logs
LOG_RETENTION_DAYS=7
LOG_ROTATION_MB=500
LOG_FORMAT="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"

# ---- Misc ----
TIMEZONE=Asia/Kolkata
HTTP_TIMEOUT=10
STREAM_HEARTBEAT_SEC=30
STREAM_MAX_BACKOFF_SEC=300
USER_AGENT=AngelAutoTrader/1.0

# ============================
#  FUTURE FILTERS (not wired yet)
#  Keep here for reference, do not enable until coded
# ============================
# MIN_PREMIUM=20
# MIN_OPEN_INT=1000
# MIN_VOLUME=100
# MIN_DELTA=0.30
# MAX_DELTA=0.70
# MAX_DTE=45
# MIN_DTE=5
# MAX_STRANGLE_WIDTH=100
# MAX_STRANGLE_WIDTH_PCT=1.0
# MAX_ITM=0.10
# MAX_LOTS=5
# MAX_TRADES=3
# MAX_EXPOSURE=5000
# MIN_DAILY_VOLUME=5000
# MAX_DAILY_VOLUME=500000
# MIN_BID_ASK_SPREAD_PCT=0.10
# MAX_BID_ASK_SPREAD_PCT=3.0
# MAX_BID_ASK_SPREAD_PTS=5.0
# MIN_OPEN_PRICE=50.0
# MAX_OPEN_PRICE=500.0
# MAX_UNDERLYING_VOLATILITY=5.0
# MAX_UNDERLYING_DAILY_MOVE_PCT=3.0
# MIN_UNDERLYING_DAILY_MOVE_PCT=0.1
# MAX_UNDERLYING_PRICE=50000.0
# MIN_UNDERLYING_PRICE=1000.0


