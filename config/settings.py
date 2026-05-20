"""Central configuration — all settings loaded from environment / .env file."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")


def _bool(key: str, default: bool = False) -> bool:
    return os.getenv(key, str(default)).lower() in ("true", "1", "yes")


def _int(key: str, default: int = 0) -> int:
    return int(os.getenv(key, str(default)))


def _float(key: str, default: float = 0.0) -> float:
    return float(os.getenv(key, str(default)))


# ── Polymarket ────────────────────────────────────────────────────────────────
POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
POLY_WALLET_ADDRESS: str = os.getenv("POLY_WALLET_ADDRESS", "")
POLY_API_KEY: str = os.getenv("POLY_API_KEY", "")
POLY_API_SECRET: str = os.getenv("POLY_API_SECRET", "")
POLY_API_PASSPHRASE: str = os.getenv("POLY_API_PASSPHRASE", "")

CLOB_BASE_URL: str = "https://clob.polymarket.com"
GAMMA_API_URL: str = "https://gamma-api.polymarket.com"
DATA_API_URL: str = "https://data-api.polymarket.com"
WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Exchange APIs ─────────────────────────────────────────────────────────────
BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")

# ── AI / News ─────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
NEWS_API_KEY: str = os.getenv("NEWS_API_KEY", "")

# ── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_PORT: int = _int("DASHBOARD_PORT", 8083)
DASHBOARD_HOST: str = os.getenv("DASHBOARD_HOST", "0.0.0.0")

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

# ── Risk Management ───────────────────────────────────────────────────────────
DAILY_LOSS_CAP_USDC: float = _float("DAILY_LOSS_CAP_USDC", 500.0)
MAX_POSITION_SIZE_USDC: float = _float("MAX_POSITION_SIZE_USDC", 2000.0)
MAX_OPEN_ORDERS: int = _int("MAX_OPEN_ORDERS", 20)

# ── Strategy Toggle ───────────────────────────────────────────────────────────
LATENCY_ARB_ENABLED: bool = _bool("LATENCY_ARB_ENABLED", True)

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE: str = os.getenv("LOG_FILE", "logs/bot.log")

# ── Dry Run ───────────────────────────────────────────────────────────────────
DRY_RUN: bool = _bool("DRY_RUN", False)

# ── Trading Fees ──────────────────────────────────────────────────────────────
# Polymarket CLOB taker fee for FOK market orders (~2%).
TAKER_FEE_RATE: float = _float("TAKER_FEE_RATE", 0.020)

# ── Blockchain ────────────────────────────────────────────────────────────────
POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
CHAIN_ID: int = 137
USDC_ADDRESS: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# ── Profile / Runtime ─────────────────────────────────────────────────────────
ACTIVE_PROFILE: str = os.getenv("ACTIVE_PROFILE", "latency")
DB_PATH: str = os.getenv("DB_PATH", "data/polymarket.db")

# ── Telegram Alerting ────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

# ── SOL Market ────────────────────────────────────────────────────────────────
SOL_LAB_ENABLED: bool = _bool("SOL_LAB_ENABLED", True)

# SOL has ~1.7–1.8x BTC daily volatility. Filters are looser on mid-price windows
# (fewer market makers → contracts misprice more often) but stricter on OB floor
# (higher-vol OB imbalances flip faster → need stronger signal to avoid noise).
SOL_5M_MID_PRICE_MIN: float = _float("SOL_5M_MID_PRICE_MIN", 0.42)
SOL_5M_MID_PRICE_MAX: float = _float("SOL_5M_MID_PRICE_MAX", 0.58)
SOL_15M_MID_PRICE_MIN: float = _float("SOL_15M_MID_PRICE_MIN", 0.45)
SOL_15M_MID_PRICE_MAX: float = _float("SOL_15M_MID_PRICE_MAX", 0.62)
SOL_OB_MIN_IMBALANCE: float = _float("SOL_OB_MIN_IMBALANCE", 0.30)
SOL_OB_SIZE_STRONG: float = _float("SOL_OB_SIZE_STRONG", 1.50)
# SOL 5m momentum multiplier — SOL moves ~1.8x BTC/tick; apply 1.8x to the base threshold
SOL_5M_MOMENTUM_MULT: float = _float("SOL_5M_MOMENTUM_MULT", 1.8)
SOL_TREND_FILTER_MIN_SLOPE: float = _float("SOL_TREND_FILTER_MIN_SLOPE", 0.005)

# ── XRP Market ────────────────────────────────────────────────────────────────
XRP_LAB_ENABLED: bool = _bool("XRP_LAB_ENABLED", True)

# XRP has ~1.4x BTC daily volatility. OB is thinner and more susceptible to
# wash trading → apply stricter OB floor than BTC. Mid-price windows similar to BTC.
XRP_5M_MID_PRICE_MIN: float = _float("XRP_5M_MID_PRICE_MIN", 0.40)
XRP_5M_MID_PRICE_MAX: float = _float("XRP_5M_MID_PRICE_MAX", 0.58)
XRP_15M_MID_PRICE_MIN: float = _float("XRP_15M_MID_PRICE_MIN", 0.45)
XRP_15M_MID_PRICE_MAX: float = _float("XRP_15M_MID_PRICE_MAX", 0.62)
XRP_OB_MIN_IMBALANCE: float = _float("XRP_OB_MIN_IMBALANCE", 0.28)
XRP_OB_SIZE_STRONG: float = _float("XRP_OB_SIZE_STRONG", 1.50)
# XRP 5m momentum multiplier — XRP moves ~1.4x BTC/tick
XRP_5M_MOMENTUM_MULT: float = _float("XRP_5M_MOMENTUM_MULT", 1.4)
XRP_TREND_FILTER_MIN_SLOPE: float = _float("XRP_TREND_FILTER_MIN_SLOPE", 0.004)

# ── Latency Arb ───────────────────────────────────────────────────────────────
LAB_BASE_SIZE_USDC: float = _float("LAB_BASE_SIZE_USDC", 50.0)
# Base momentum threshold (0.10% per 10s window). Per-asset multipliers above
# scale this up: SOL uses 1.8x (0.18%), XRP uses 1.4x (0.14%).
LAB_MOMENTUM_THRESHOLD: float = _float("LAB_MOMENTUM_THRESHOLD", 0.0010)
LAB_POLL_INTERVAL: float = _float("LAB_POLL_INTERVAL", 0.5)  # 500ms
LAB_MAX_CONCURRENT_POSITIONS: int = _int("LAB_MAX_CONCURRENT_POSITIONS", 3)
LAB_MAX_CONCURRENT_POSITIONS_5M: int = _int("LAB_MAX_CONCURRENT_POSITIONS_5M", LAB_MAX_CONCURRENT_POSITIONS)
LAB_MAX_CONCURRENT_POSITIONS_15M: int = _int("LAB_MAX_CONCURRENT_POSITIONS_15M", LAB_MAX_CONCURRENT_POSITIONS)
LAB_WINDOWS: list[int] = [int(w) for w in os.getenv("LAB_WINDOWS", "5,15").split(",")]
LAB_15M_CONFIRM_SECONDS: int = _int("LAB_15M_CONFIRM_SECONDS", 45)
LAB_15M_CONFIRM_RETENTION: float = _float("LAB_15M_CONFIRM_RETENTION", 0.6)
LAB_15M_FASTTRACK_MULTIPLIER: float = _float("LAB_15M_FASTTRACK_MULTIPLIER", 2.0)
LAB_15M_CONFIRMATION_MARGIN: float = _float("LAB_15M_CONFIRMATION_MARGIN", 0.10)
LAB_OB_IMBALANCE_ENABLED: bool = _bool("LAB_OB_IMBALANCE_ENABLED", True)
LAB_OB_IMBALANCE_THRESHOLD: float = _float("LAB_OB_IMBALANCE_THRESHOLD", 0.10)
LAB_OB_SIZING_ENABLED: bool = _bool("LAB_OB_SIZING_ENABLED", True)
LAB_OB_SIZE_WEAK: float = _float("LAB_OB_SIZE_WEAK", 0.50)
LAB_OB_SIZE_STRONG: float = _float("LAB_OB_SIZE_STRONG", 1.50)
LAB_OB_STRONG_THRESHOLD: float = _float("LAB_OB_STRONG_THRESHOLD", 0.60)
OB_MIN_IMBALANCE: float = _float("OB_MIN_IMBALANCE", 0.20)       # fallback floor
EVENING_OB_MIN_IMBALANCE: float = _float("EVENING_OB_MIN_IMBALANCE", 0.30)
EVENING_HOURS_START: int = _int("EVENING_HOURS_START", 18)

# ── CVD — trade-based taker OFI (replaces resting OBI as the entry gate) ─────
# CVD = (buy_taker_qty - sell_taker_qty) / total_qty in the last CVD_WINDOW_SECS.
# Range [-1, +1]: +1 = all takers are buyers; -1 = all takers are sellers.
# Research (arXiv 2507.22712): trade-based OFI outperforms resting OBI for altcoins
# because it captures realized pressure, not spoofed/fleeting limit orders.
# OBI is kept for SIZING only (not as a hard entry gate).
LAB_CVD_ENABLED: bool = _bool("LAB_CVD_ENABLED", True)
LAB_CVD_WINDOW_SECS: float = _float("LAB_CVD_WINDOW_SECS", 10.0)
# CVD >= STRONG_THRESHOLD in signal direction → full/boosted size (confirmed move)
# CVD between 0 and STRONG → neutral size (move starting but not confirmed)
# CVD against signal direction → reduced size (counterflow present)
LAB_CVD_STRONG_THRESHOLD: float = _float("LAB_CVD_STRONG_THRESHOLD", 0.25)
# FAST_TRACK is blocked (not just sized down) when CVD opposes direction by this much.
# 0.35 = ~67.5% of taker volume is against the signal — indicates bull/bear trap.
LAB_CVD_FASTTRACK_BLOCK_THRESHOLD: float = _float("LAB_CVD_FASTTRACK_BLOCK_THRESHOLD", 0.35)
# Oracle lag gate — only enter when Binance is still ahead of Polymarket reprice.
# Negative default = shadow mode (logs only, never blocks).
LAB_MIN_ORACLE_LAG: float = _float("LAB_MIN_ORACLE_LAG", -999.0)
# Near-resolution arb — enter clearly-resolved windows with < N seconds remaining.
LAB_RESOLUTION_ARB_ENABLED: bool = _bool("LAB_RESOLUTION_ARB_ENABLED", True)
LAB_RESOLUTION_SECS_REMAINING: float = _float("LAB_RESOLUTION_SECS_REMAINING", 120.0)
LAB_RESOLUTION_MIN_BINANCE_MOVE: float = _float("LAB_RESOLUTION_MIN_BINANCE_MOVE", 0.003)
LAB_RESOLUTION_MIN_MID: float = _float("LAB_RESOLUTION_MIN_MID", 0.62)
LAB_RESOLUTION_MAX_MID: float = _float("LAB_RESOLUTION_MAX_MID", 0.85)
TREND_FILTER_ENABLED: bool = _bool("TREND_FILTER_ENABLED", True)
TREND_FILTER_TICKS: int = _int("TREND_FILTER_TICKS", 20)
TREND_FILTER_MIN_SLOPE: float = _float("TREND_FILTER_SLOPE", 0.003)
LAB_15M_MID_PRICE_MIN: float = _float("LAB_15M_MID_PRICE_MIN", 0.45)
LAB_15M_MID_PRICE_MAX: float = _float("LAB_15M_MID_PRICE_MAX", 0.62)
LAB_5M_MID_PRICE_MIN: float = _float("LAB_5M_MID_PRICE_MIN", 0.40)
LAB_5M_MID_PRICE_MAX: float = _float("LAB_5M_MID_PRICE_MAX", 0.58)
LAB_PRICE_MULT: float = _float("LAB_PRICE_MULT", 0.5)

# Cross-asset momentum validator.
# BTC is used as the reference for both SOL and XRP because its correlation with
# BTC is stronger (0.85–0.92 / 0.70–0.82) than SOL/XRP with each other (0.60–0.75).
# The BTC feed runs in the background for validation only — we never trade BTC.
# Set to "" to fall back to using the other trading asset (SOL↔XRP).
LAB_CROSS_ASSET_VALIDATOR: str = os.getenv("LAB_CROSS_ASSET_VALIDATOR", "BTC")

# ── Regime Gate — volatility ratio + liquidation cascade ─────────────────────
# Vol ratio = std(5-min returns) / std(30-min returns).
# Ratios above the thresholds below activate progressively stricter gates:
#   elevated (1.5–2.5): block CONFIRMED entries only
#   high     (2.5–3.5): block all entries (FAST_TRACK, CONFIRMED, 5M_DIRECT)
#   crash    (>3.5)   : same as high, plus alert
VOL_RATIO_ELEVATED: float = _float("VOL_RATIO_ELEVATED", 1.5)
VOL_RATIO_HIGH:     float = _float("VOL_RATIO_HIGH",     2.5)
VOL_RATIO_CRASH:    float = _float("VOL_RATIO_CRASH",    3.5)

# Liquidation cascade gate.
# When the rolling 5-minute liquidation volume on Binance futures crosses these
# thresholds, all entries for that asset (and all assets for BTC) are paused
# for LIQ_CASCADE_PAUSE_SECS seconds.
LIQ_CASCADE_PAUSE_SECS:     int   = _int("LIQ_CASCADE_PAUSE_SECS",   900)   # 15 min
LIQ_CASCADE_WINDOW_SECS:    float = _float("LIQ_CASCADE_WINDOW_SECS", 300.0) # 5 min
LIQ_CASCADE_BTC_USD:        float = _float("LIQ_CASCADE_BTC_USD",  20_000_000.0)  # $20M
LIQ_CASCADE_XRP_USD:        float = _float("LIQ_CASCADE_XRP_USD",   3_000_000.0)  # $3M
LIQ_CASCADE_SOL_USD:        float = _float("LIQ_CASCADE_SOL_USD",   2_000_000.0)  # $2M

# ── Latency Arb Active Exit ───────────────────────────────────────────────────
LAB_EXIT_ENABLED: bool = _bool("LAB_EXIT_ENABLED", True)
LAB_STOP_LOSS_PCT: float = _float("LAB_STOP_LOSS_PCT", 0.35)
LAB_TAKE_PROFIT_PCT: float = _float("LAB_TAKE_PROFIT_PCT", 0.50)

# ── Latency Arb Entry Guards ──────────────────────────────────────────────────
LAB_CONSEC_LOSS_PAUSE: int = _int("LAB_CONSEC_LOSS_PAUSE", 3)
LAB_CONSEC_LOSS_PAUSE_SECS: int = _int("LAB_CONSEC_LOSS_PAUSE_SECS", 900)
LAB_MIN_SECS_TREND_CONFIRMED: int = _int("LAB_MIN_SECS_TREND_CONFIRMED", 900)
LAB_STALE_FLAT_OVERNIGHT_SECS: int = _int("LAB_STALE_FLAT_OVERNIGHT_SECS", 3600)
