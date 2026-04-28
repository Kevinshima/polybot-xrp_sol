"""SQLAlchemy models for trade log and PnL history."""
from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Trade(Base):
    __tablename__ = "trades"

    id = Column(String, primary_key=True)
    strategy = Column(String, nullable=False)
    market_id = Column(String, nullable=False)
    question = Column(Text, default="")
    side = Column(String, nullable=False)       # BUY / SELL
    size = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fill_price = Column(Float, default=0.0)
    pnl = Column(Float, default=0.0)
    status = Column(String, default="open")     # open / filled / cancelled
    exit_reason = Column(String, default="")
    dry_run = Column(Boolean, default=False)
    asset = Column(String, default="BTC")       # BTC / ETH / etc.
    timestamp = Column(Integer, nullable=False)
    # ML feature columns — captured at entry time for future model training
    momentum_at_entry = Column(Float, default=None)
    ob_imbalance_at_entry = Column(Float, default=None)
    trend_slope_at_entry = Column(Float, default=None)
    trend_direction_at_entry = Column(String, default=None)  # UP / DOWN / FLAT / WARMUP
    consec_losses_at_entry = Column(Integer, default=None)
    timeframe = Column(String, default=None)       # 5m / 15m
    ml_win_prob = Column(Float, default=None)      # shadow model prediction (0–1)


class PnLSnapshot(Base):
    __tablename__ = "pnl_snapshots"

    timestamp = Column(Integer, primary_key=True)
    daily_pnl = Column(Float, default=0.0)
    cumulative_pnl = Column(Float, default=0.0)
    open_positions_value = Column(Float, default=0.0)


class OpenPosition(Base):
    __tablename__ = "open_positions"

    market_id = Column(String, primary_key=True)
    token_id = Column(String, default="")
    question = Column(Text, default="")
    strategy = Column(String, default="")
    side = Column(String, default="")
    size = Column(Float, default=0.0)
    entry_price = Column(Float, default=0.0)
    current_price = Column(Float, default=0.0)
    unrealized_pnl = Column(Float, default=0.0)
    opened_at = Column(Integer, default=0)
    metadata_json = Column(Text, default="")


# ── Sentiment profile tables ───────────────────────────────────────────────────

class NewsItem(Base):
    __tablename__ = "news_items"

    id = Column(Integer, primary_key=True)
    fingerprint = Column(String, unique=True, nullable=False)
    source = Column(String)
    title = Column(String)
    url = Column(String)
    published_at = Column(DateTime)
    summary = Column(Text)
    raw_themes = Column(String)       # comma-separated matched theme names
    created_at = Column(DateTime, default=datetime.utcnow)


class NewsAnalysis(Base):
    __tablename__ = "news_analysis"

    id = Column(Integer, primary_key=True)
    news_item_id = Column(Integer, ForeignKey("news_items.id"))
    theme = Column(String)
    is_relevant = Column(Boolean)
    direction = Column(String)        # "increase_yes" | "decrease_yes" | "neutral"
    confidence = Column(Float)
    urgency = Column(Float)
    impact_strength = Column(Float)
    reasoning_short = Column(String)
    market_tags = Column(String)      # JSON list as string
    analyzer_name = Column(String)    # "llm_haiku" | "keyword"
    created_at = Column(DateTime, default=datetime.utcnow)


class SentimentDecision(Base):
    __tablename__ = "sentiment_decisions"

    id = Column(Integer, primary_key=True)
    news_item_id = Column(Integer, ForeignKey("news_items.id"))
    theme = Column(String, default="")
    market_id = Column(String)
    market_question = Column(String)
    market_price = Column(Float)
    estimated_probability = Column(Float)
    edge = Column(Float)
    decision = Column(String)         # "buy_yes" | "buy_no" | "skip"
    skip_reason = Column(String)      # null if traded
    trade_id = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class MarketCooldown(Base):
    __tablename__ = "market_cooldowns"

    market_id = Column(String, primary_key=True)
    strategy = Column(String, default="ai_sentiment")
    last_traded_at = Column(Float, nullable=False)  # unix timestamp
