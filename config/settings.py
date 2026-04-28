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
COINBASE_API_KEY: str = os.getenv("COINBASE_API_KEY", "")
COINBASE_API_SECRET: str = os.getenv("COINBASE_API_SECRET", "")

# ── AI / News ─────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
NEWS_API_KEY: str = os.getenv("NEWS_API_KEY", "")
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
TWITTER_BEARER_TOKEN: str = os.getenv("TWITTER_BEARER_TOKEN", "")

# ── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_PORT: int = _int("DASHBOARD_PORT", 8080)
DASHBOARD_HOST: str = os.getenv("DASHBOARD_HOST", "0.0.0.0")

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

# ── Risk Management ───────────────────────────────────────────────────────────
DAILY_LOSS_CAP_USDC: float = _float("DAILY_LOSS_CAP_USDC", 500.0)
MAX_POSITION_SIZE_USDC: float = _float("MAX_POSITION_SIZE_USDC", 2000.0)
MAX_OPEN_ORDERS: int = _int("MAX_OPEN_ORDERS", 20)

# ── Strategy Toggles ──────────────────────────────────────────────────────────
LATENCY_ARB_ENABLED: bool = _bool("LATENCY_ARB_ENABLED", True)
MARKET_MAKER_ENABLED: bool = _bool("MARKET_MAKER_ENABLED", True)
AI_SENTIMENT_ENABLED: bool = _bool("AI_SENTIMENT_ENABLED", True)
COPY_TRADER_ENABLED: bool = _bool("COPY_TRADER_ENABLED", True)

# ── Copy Trader ───────────────────────────────────────────────────────────────
TARGET_WALLETS: list[str] = [
    w.strip()
    for w in os.getenv("TARGET_WALLETS", "").split(",")
    if w.strip()
]
COPY_RATIO: float = _float("COPY_RATIO", 0.10)

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE: str = os.getenv("LOG_FILE", "logs/bot.log")

# ── Dry Run ───────────────────────────────────────────────────────────────────
DRY_RUN: bool = _bool("DRY_RUN", False)

# ── Blockchain ────────────────────────────────────────────────────────────────
POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
CHAIN_ID: int = 137

# Contract addresses on Polygon
USDC_ADDRESS: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CLOB_EXCHANGE_ADDRESS: str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

# ── Market Maker ──────────────────────────────────────────────────────────────
MM_DEFAULT_SIZE_USDC: float = _float("MM_DEFAULT_SIZE_USDC", 200.0)
MM_MIN_SPREAD: float = _float("MM_MIN_SPREAD", 0.03)
MM_MAX_SPREAD: float = _float("MM_MAX_SPREAD", 0.06)
MM_REQUOTE_INTERVAL: int = _int("MM_REQUOTE_INTERVAL", 60)

# ── ETH Market ───────────────────────────────────────────────────────────────
ETH_LAB_ENABLED: bool = _bool("ETH_LAB_ENABLED", True)  # add ETH updown markets alongside BTC

# ETH-specific filters (ETH is more volatile than BTC — apply stricter gates)
ETH_5M_MID_PRICE_MIN: float = _float("ETH_5M_MID_PRICE_MIN", 0.45)   # tighter than BTC's 0.40
ETH_5M_MID_PRICE_MAX: float = _float("ETH_5M_MID_PRICE_MAX", 0.55)   # tighter than BTC's 0.58
ETH_15M_MID_PRICE_MIN: float = _float("ETH_15M_MID_PRICE_MIN", 0.47) # tighter than global 0.45
ETH_15M_MID_PRICE_MAX: float = _float("ETH_15M_MID_PRICE_MAX", 0.57) # tighter than global 0.62
ETH_OB_MIN_IMBALANCE: float = _float("ETH_OB_MIN_IMBALANCE", 0.75)   # stricter absolute OB floor for ETH
ETH_CONSEC_LOSS_PAUSE: int = _int("ETH_CONSEC_LOSS_PAUSE", 2)         # pause after N consecutive losses (same as BTC default)
LAB_CONSEC_LOSS_PAUSE: int = _int("LAB_CONSEC_LOSS_PAUSE", 3)         # pause after N consecutive BTC losses
ETH_LOSS_COOLDOWN_SECS: int = _int("ETH_LOSS_COOLDOWN_SECS", 600)     # cooldown duration after ETH loss streak
ETH_OB_SIZE_STRONG: float = _float("ETH_OB_SIZE_STRONG", 1.00)        # ETH STRONG OB size multiplier (flat — ETH is more volatile)

# ── Latency Arb ───────────────────────────────────────────────────────────────
LAB_BASE_SIZE_USDC: float = _float("LAB_BASE_SIZE_USDC", 50.0)  # base bet before multipliers
LAB_MOMENTUM_THRESHOLD: float = _float("LAB_MOMENTUM_THRESHOLD", 0.0010)  # 0.10% — actual trading threshold
BTC_5M_MOMENTUM_MULT: float = _float("BTC_5M_MOMENTUM_MULT", 1.3)  # extra strictness for BTC 5m vs ETH 5m
LAB_POLL_INTERVAL: float = _float("LAB_POLL_INTERVAL", 0.5)  # 500ms
LAB_MAX_CONCURRENT_POSITIONS: int = _int("LAB_MAX_CONCURRENT_POSITIONS", 3)
LAB_MAX_CONCURRENT_POSITIONS_5M: int = _int("LAB_MAX_CONCURRENT_POSITIONS_5M", LAB_MAX_CONCURRENT_POSITIONS)
LAB_MAX_CONCURRENT_POSITIONS_15M: int = _int("LAB_MAX_CONCURRENT_POSITIONS_15M", LAB_MAX_CONCURRENT_POSITIONS)
LAB_WINDOWS: list[int] = [int(w) for w in os.getenv("LAB_WINDOWS", "5,15").split(",")]
LAB_15M_CONFIRM_SECONDS: int = _int("LAB_15M_CONFIRM_SECONDS", 10)        # 10s: enter before crowd reprices
LAB_15M_CONFIRM_RETENTION: float = _float("LAB_15M_CONFIRM_RETENTION", 0.6)  # confirm at 60% of threshold
LAB_15M_FASTTRACK_MULTIPLIER: float = _float("LAB_15M_FASTTRACK_MULTIPLIER", 2.0)  # instant entry if momentum >= threshold * 2.0
LAB_15M_CONFIRMATION_MARGIN: float = _float("LAB_15M_CONFIRMATION_MARGIN", 0.10)
LAB_OB_IMBALANCE_ENABLED: bool = _bool("LAB_OB_IMBALANCE_ENABLED", True)
LAB_OB_IMBALANCE_THRESHOLD: float = _float("LAB_OB_IMBALANCE_THRESHOLD", 0.10)
LAB_OB_SIZING_ENABLED: bool = _bool("LAB_OB_SIZING_ENABLED", True)
LAB_OB_SIZE_WEAK: float = _float("LAB_OB_SIZE_WEAK", 0.50)       # 50% of size when OB is weak
LAB_OB_SIZE_STRONG: float = _float("LAB_OB_SIZE_STRONG", 1.50)   # 150% of size when OB is strong
LAB_OB_STRONG_THRESHOLD: float = _float("LAB_OB_STRONG_THRESHOLD", 0.60)  # imbalance >= this = strong
OB_MIN_IMBALANCE: float = _float("OB_MIN_IMBALANCE", 0.20)       # absolute floor — skip any trade if |imbalance| < this
EVENING_OB_MIN_IMBALANCE: float = _float("EVENING_OB_MIN_IMBALANCE", 0.30)  # stricter floor after EVENING_HOURS_START
EVENING_HOURS_START: int = _int("EVENING_HOURS_START", 18)       # local hour (24h) at which evening floor activates
TREND_FILTER_ENABLED: bool = _bool("TREND_FILTER_ENABLED", True)
TREND_FILTER_TICKS: int = _int("TREND_FILTER_TICKS", 20)
TREND_FILTER_MIN_SLOPE: float = _float("TREND_FILTER_SLOPE", 0.003)
LAB_15M_MID_PRICE_MIN: float = _float("LAB_15M_MID_PRICE_MIN", 0.45)  # 15m entry: discard if mid < this (below 0.45 = 43% win rate, below break-even)
LAB_15M_MID_PRICE_MAX: float = _float("LAB_15M_MID_PRICE_MAX", 0.62)  # 15m entry: discard if mid > this
LAB_5M_MID_PRICE_MIN: float = _float("LAB_5M_MID_PRICE_MIN", 0.40)   # 5m entry: discard if mid < this
LAB_5M_MID_PRICE_MAX: float = _float("LAB_5M_MID_PRICE_MAX", 0.58)   # 5m entry: discard if mid > this
LAB_PRICE_MULT: float = _float("LAB_PRICE_MULT", 0.5)            # flat position size multiplier (do not vary by mid-price)

# ── Latency Arb Active Exit ───────────────────────────────────────────────────
LAB_EXIT_ENABLED: bool = _bool("LAB_EXIT_ENABLED", True)          # enable stop-loss / take-profit for latency arb
LAB_STOP_LOSS_PCT: float = _float("LAB_STOP_LOSS_PCT", 0.35)      # exit if price falls 35% from entry
LAB_TAKE_PROFIT_PCT: float = _float("LAB_TAKE_PROFIT_PCT", 0.50)  # exit if price rises 50% from entry

# ── AI Sentiment ─────────────────────────────────────────────────────────────
AI_MAX_CONCURRENT_POSITIONS: int = _int("AI_MAX_CONCURRENT_POSITIONS", 5)
AI_EDGE_THRESHOLD: float = _float("AI_EDGE_THRESHOLD", 0.07)  # 7%
AI_POLL_INTERVAL: int = _int("AI_POLL_INTERVAL", 60)

# ── Copy Trader ───────────────────────────────────────────────────────────────
CT_POLL_INTERVAL: int = _int("CT_POLL_INTERVAL", 10)
CT_MIN_SECONDS_TO_CLOSE: int = _int("CT_MIN_SECONDS_TO_CLOSE", 300)  # 5 min

# ── Profile / Runtime ─────────────────────────────────────────────────────────
ACTIVE_PROFILE: str = os.getenv("ACTIVE_PROFILE", "latency")
DB_PATH: str = os.getenv("DB_PATH", "data/polymarket.db")

# ── Telegram Alerting ────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Synthetic Arbitrage ───────────────────────────────────────────────────────
SYNTH_ARB_ENABLED: bool = _bool("SYNTH_ARB_ENABLED", False)
SYNTH_MIN_GAP: float = _float("SYNTH_MIN_GAP", 0.030)          # minimum net gap after fees (3%)
SYNTH_POSITION_SIZE: float = _float("SYNTH_POSITION_SIZE", 50.0) # target payout P per opportunity (USDC)
SYNTH_MAX_OPEN: int = _int("SYNTH_MAX_OPEN", 5)                 # max simultaneous synth positions
SYNTH_MIN_LIQUIDITY: float = _float("SYNTH_MIN_LIQUIDITY", 300.0) # skip markets below this liquidity (USDC)
SYNTH_MAX_DAYS_TO_RESOLVE: int = _int("SYNTH_MAX_DAYS_TO_RESOLVE", 7)  # skip markets resolving >N days away (0 = no limit)
SYNTH_DAILY_LOSS_CAP: float = _float("SYNTH_DAILY_LOSS_CAP", 50.0)     # synth-only daily loss cap; independent of shared risk manager
SYNTH_MERGE_ENABLED: bool = _bool("SYNTH_MERGE_ENABLED", True)          # attempt on-chain CTF.mergePositions() after fills for fast capital recycling

# ── Sentiment Profile ─────────────────────────────────────────────────────────
SENTIMENT_POLL_INTERVAL: int = _int("SENTIMENT_POLL_INTERVAL", 180)
SENTIMENT_MAX_NEWS_AGE_HOURS: int = _int("SENTIMENT_MAX_NEWS_AGE_HOURS", 4)
SENTIMENT_VERBOSE: bool = _bool("SENTIMENT_VERBOSE", False)
SENTIMENT_MIN_CONFIDENCE: float = _float("SENTIMENT_MIN_CONFIDENCE", 0.65)
SENTIMENT_MIN_URGENCY: float = _float("SENTIMENT_MIN_URGENCY", 0.45)
SENTIMENT_MIN_EDGE: float = _float("SENTIMENT_MIN_EDGE", 0.10)
SENTIMENT_COOLDOWN_MINUTES: int = _int("SENTIMENT_COOLDOWN_MINUTES", 60)
SENTIMENT_MAX_CONCURRENT: int = _int("SENTIMENT_MAX_CONCURRENT", 3)
SENTIMENT_MAX_DAILY_LOSS: float = _float("SENTIMENT_MAX_DAILY_LOSS", 50.0)
SENTIMENT_ANALYZER: str = os.getenv("SENTIMENT_ANALYZER", "llm")
SENTIMENT_POSITION_SIZE: float = _float("SENTIMENT_POSITION_SIZE", 5.0)
SENTIMENT_MIN_LIQUIDITY: float = _float("SENTIMENT_MIN_LIQUIDITY", 1500.0)
SENTIMENT_MIN_VOLUME_24H: float = _float("SENTIMENT_MIN_VOLUME_24H", 1000.0)
SENTIMENT_MIN_RESOLUTION_MINUTES: int = _int("SENTIMENT_MIN_RESOLUTION_MINUTES", 30)
SENTIMENT_MAX_RESOLUTION_DAYS: int = _int("SENTIMENT_MAX_RESOLUTION_DAYS", 30)
SENTIMENT_MAX_MARKETS_PER_ITEM: int = _int("SENTIMENT_MAX_MARKETS_PER_ITEM", 2)
SENTIMENT_REPRICE_INTERVAL_SECONDS: int = _int("SENTIMENT_REPRICE_INTERVAL_SECONDS", 120)
SENTIMENT_PAPER_EXIT_ENABLED: bool = _bool("SENTIMENT_PAPER_EXIT_ENABLED", True)
SENTIMENT_TIME_STOP_MINUTES: int = _int("SENTIMENT_TIME_STOP_MINUTES", 240)
SENTIMENT_STOP_LOSS_PCT: float = _float("SENTIMENT_STOP_LOSS_PCT", 0.30)
SENTIMENT_TAKE_PROFIT_PCT: float = _float("SENTIMENT_TAKE_PROFIT_PCT", 0.40)
SENTIMENT_THESIS_INVALIDATION_PCT: float = _float("SENTIMENT_THESIS_INVALIDATION_PCT", 0.05)
SENTIMENT_SCAN_INTERVAL_SECONDS: int = _int("SENTIMENT_SCAN_INTERVAL_SECONDS", 300)
SENTIMENT_SCAN_ENABLED: bool = _bool("SENTIMENT_SCAN_ENABLED", True)
SENTIMENT_SCAN_MAX_HOURS: float = float(os.getenv("SENTIMENT_SCAN_MAX_HOURS", "48"))
SENTIMENT_SCAN_MIN_EDGE: float = _float("SENTIMENT_SCAN_MIN_EDGE", 0.08)
