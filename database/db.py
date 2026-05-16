"""SQLite database connection and query helpers."""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, func, desc
from sqlalchemy.orm import sessionmaker, Session

from database.models import Base, Trade, PnLSnapshot, OpenPosition, NewsItem, NewsAnalysis, SentimentDecision, MarketCooldown
from utils.logger import logger

# DB_PATH is profile-aware — set via DB_PATH env var for the sentiment profile
from config import settings as _settings
DB_PATH = Path(_settings.DB_PATH)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False},
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autoflush=True, autocommit=False)

Base.metadata.create_all(engine)

# Migrate existing databases that predate the dry_run column
with engine.connect() as _conn:
    from sqlalchemy import text as _text
    try:
        _conn.execute(_text("ALTER TABLE trades ADD COLUMN dry_run BOOLEAN DEFAULT 0"))
        _conn.commit()
    except Exception:
        pass  # column already exists
    try:
        _conn.execute(_text("ALTER TABLE open_positions ADD COLUMN token_id TEXT DEFAULT ''"))
        _conn.commit()
    except Exception:
        pass  # column already exists
    try:
        _conn.execute(_text("ALTER TABLE trades ADD COLUMN exit_reason TEXT DEFAULT ''"))
        _conn.commit()
    except Exception:
        pass  # column already exists
    try:
        _conn.execute(_text("ALTER TABLE open_positions ADD COLUMN metadata_json TEXT DEFAULT ''"))
        _conn.commit()
    except Exception:
        pass  # column already exists
    try:
        _conn.execute(_text("ALTER TABLE sentiment_decisions ADD COLUMN theme TEXT DEFAULT ''"))
        _conn.commit()
    except Exception:
        pass  # column already exists
    try:
        _conn.execute(_text("ALTER TABLE sentiment_decisions ADD COLUMN trade_id TEXT DEFAULT ''"))
        _conn.commit()
    except Exception:
        pass  # column already exists
    try:
        _conn.execute(_text("ALTER TABLE trades ADD COLUMN asset TEXT DEFAULT 'BTC'"))
        _conn.commit()
    except Exception:
        pass  # column already exists
    # ML feature columns
    for _col, _type in [
        ("momentum_at_entry", "REAL"),
        ("ob_imbalance_at_entry", "REAL"),
        ("cvd_at_entry", "REAL"),
        ("trend_slope_at_entry", "REAL"),
        ("trend_direction_at_entry", "TEXT"),
        ("consec_losses_at_entry", "INTEGER"),
        ("timeframe", "TEXT"),
        ("ml_win_prob", "REAL"),
        ("momentum_delta", "REAL"),
        ("secs_since_trend_change", "REAL"),
        ("prev_trend_direction", "TEXT"),
        ("entry_path", "TEXT"),
        ("consec_wins", "INTEGER"),
        ("ob_at_queue_time", "REAL"),
        ("cross_asset_agree", "INTEGER"),
        ("asset_range_15m", "REAL"),
    ]:
        try:
            _conn.execute(_text(f"ALTER TABLE trades ADD COLUMN {_col} {_type}"))
            _conn.commit()
        except Exception:
            pass  # column already exists


def get_session() -> Session:
    return SessionLocal()


# ── Trades ────────────────────────────────────────────────────────────────────

def insert_trade(
    trade_id: str,
    strategy: str,
    market_id: str,
    question: str,
    side: str,
    size: float,
    price: float,
    fill_price: float = 0.0,
    pnl: float = 0.0,
    status: str = "open",
    exit_reason: str = "",
    dry_run: bool = False,
    asset: str = "SOL",
    momentum_at_entry: Optional[float] = None,
    ob_imbalance_at_entry: Optional[float] = None,
    cvd_at_entry: Optional[float] = None,
    trend_slope_at_entry: Optional[float] = None,
    trend_direction_at_entry: Optional[str] = None,
    consec_losses_at_entry: Optional[int] = None,
    timeframe: Optional[str] = None,
    ml_win_prob: Optional[float] = None,
    momentum_delta: Optional[float] = None,
    secs_since_trend_change: Optional[float] = None,
    prev_trend_direction: Optional[str] = None,
    entry_path: Optional[str] = None,
    consec_wins: Optional[int] = None,
    ob_at_queue_time: Optional[float] = None,
    cross_asset_agree: Optional[int] = None,
    asset_range_15m: Optional[float] = None,
) -> None:
    with get_session() as s:
        trade = Trade(
            id=trade_id,
            strategy=strategy,
            market_id=market_id,
            question=question,
            side=side,
            size=size,
            price=price,
            fill_price=fill_price,
            pnl=pnl,
            status=status,
            exit_reason=exit_reason,
            dry_run=dry_run,
            asset=asset,
            timestamp=int(time.time()),
            momentum_at_entry=momentum_at_entry,
            ob_imbalance_at_entry=ob_imbalance_at_entry,
            cvd_at_entry=cvd_at_entry,
            trend_slope_at_entry=trend_slope_at_entry,
            trend_direction_at_entry=trend_direction_at_entry,
            consec_losses_at_entry=consec_losses_at_entry,
            timeframe=timeframe,
            ml_win_prob=ml_win_prob,
            momentum_delta=momentum_delta,
            secs_since_trend_change=secs_since_trend_change,
            prev_trend_direction=prev_trend_direction,
            entry_path=entry_path,
            consec_wins=consec_wins,
            ob_at_queue_time=ob_at_queue_time,
            cross_asset_agree=cross_asset_agree,
            asset_range_15m=asset_range_15m,
        )
        s.merge(trade)
        s.commit()


def update_trade_ml_prob(trade_id: str, ml_win_prob: float) -> None:
    """Write the shadow model's win probability to an already-inserted trade row."""
    with get_session() as s:
        trade = s.get(Trade, trade_id)
        if trade:
            trade.ml_win_prob = ml_win_prob
            s.commit()


def update_trade(
    trade_id: str,
    fill_price: float,
    pnl: float,
    status: str = "filled",
    exit_reason: Optional[str] = None,
) -> None:
    with get_session() as s:
        trade = s.get(Trade, trade_id)
        if trade:
            trade.fill_price = fill_price
            trade.pnl = pnl
            trade.status = status
            if exit_reason is not None:
                trade.exit_reason = exit_reason
            s.commit()


def close_trade_by_market_id(
    market_id: str,
    strategy: str,
    question: str,
    side: str,
    size: float,
    entry_price: float,
    exit_price: float,
    pnl: float,
    exit_reason: str = "",
    dry_run: bool = False,
) -> None:
    """Update the most recent non-terminal Trade row for market_id to closed, or insert a synthetic one.

    Guards against double-writes: if the heartbeat resolver already closed the trade via
    update_trades_for_market(), this call will detect the existing closed row and skip it.
    """
    from utils.logger import logger as _logger
    with get_session() as s:
        # Guard: if already closed by another code path (e.g. heartbeat resolver), skip
        already_closed = (
            s.query(Trade)
            .filter(Trade.market_id == market_id, Trade.status == "closed")
            .first()
        )
        if already_closed:
            _logger.warning(f"PnL double-write prevented for {market_id[:20]}")
            return

        trade = (
            s.query(Trade)
            .filter(
                Trade.market_id == market_id,
                Trade.status.notin_(["cancelled", "closed"]),
            )
            .order_by(desc(Trade.timestamp))
            .first()
        )
        if trade:
            trade.fill_price = exit_price
            trade.pnl = pnl
            trade.status = "closed"
            trade.exit_reason = exit_reason
            s.commit()
        else:
            trade_id = f"synthetic_{market_id[:20]}_{int(time.time())}"
            s.add(Trade(
                id=trade_id,
                strategy=strategy,
                market_id=market_id,
                question=question,
                side=side,
                size=size,
                price=entry_price,
                fill_price=exit_price,
                pnl=pnl,
                status="closed",
                exit_reason=exit_reason,
                dry_run=dry_run,
                timestamp=int(time.time()),
            ))
            s.commit()


def get_recent_trades(limit: int = 100) -> list[dict]:
    with get_session() as s:
        rows = (
            s.query(Trade)
            .order_by(desc(Trade.timestamp))
            .limit(limit)
            .all()
        )
        return [_trade_to_dict(r) for r in rows]


def get_daily_pnl() -> float:
    now = datetime.now()
    local_midnight = datetime(now.year, now.month, now.day).timestamp()
    with get_session() as s:
        result = s.query(func.sum(Trade.pnl)).filter(
            Trade.timestamp >= local_midnight,
            Trade.status.in_(["filled", "closed"]),
        ).scalar()
        return result or 0.0


def get_cumulative_pnl() -> float:
    with get_session() as s:
        result = s.query(func.sum(Trade.pnl)).filter(
            Trade.status.in_(["filled", "closed"])
        ).scalar()
        return result or 0.0


def get_strategy_daily_pnl(strategy: str) -> float:
    now = datetime.now()
    local_midnight = datetime(now.year, now.month, now.day).timestamp()
    with get_session() as s:
        result = s.query(func.sum(Trade.pnl)).filter(
            Trade.strategy == strategy,
            Trade.timestamp >= local_midnight,
            Trade.status.in_(["filled", "closed"]),
        ).scalar()
        return result or 0.0


def get_strategy_cumulative_pnl(strategy: str) -> float:
    with get_session() as s:
        result = s.query(func.sum(Trade.pnl)).filter(
            Trade.strategy == strategy,
            Trade.status.in_(["filled", "closed"]),
        ).scalar()
        return result or 0.0


def _trade_to_dict(t: Trade) -> dict:
    return {
        "id": t.id,
        "strategy": t.strategy,
        "market_id": t.market_id,
        "question": t.question,
        "side": t.side,
        "size": t.size,
        "price": t.price,
        "fill_price": t.fill_price,
        "pnl": t.pnl,
        "status": t.status,
        "exit_reason": t.exit_reason,
        "dry_run": bool(t.dry_run),
        "asset": t.asset or "SOL",
        "timestamp": t.timestamp,
        "momentum_at_entry": t.momentum_at_entry,
        "ob_imbalance_at_entry": t.ob_imbalance_at_entry,
        "cvd_at_entry": t.cvd_at_entry,
        "trend_slope_at_entry": t.trend_slope_at_entry,
        "trend_direction_at_entry": t.trend_direction_at_entry,
        "consec_losses_at_entry": t.consec_losses_at_entry,
        "timeframe": t.timeframe,
        "ml_win_prob": t.ml_win_prob,
        "momentum_delta": t.momentum_delta,
        "secs_since_trend_change": t.secs_since_trend_change,
        "prev_trend_direction": t.prev_trend_direction,
        "entry_path": t.entry_path,
        "consec_wins": t.consec_wins,
        "ob_at_queue_time": t.ob_at_queue_time,
        "cross_asset_agree": t.cross_asset_agree,
        "asset_range_15m": t.asset_range_15m,
    }


# ── PnL Snapshots ─────────────────────────────────────────────────────────────

def insert_pnl_snapshot(daily_pnl: float, cumulative_pnl: float, open_value: float) -> None:
    with get_session() as s:
        snap = PnLSnapshot(
            timestamp=int(time.time()),
            daily_pnl=daily_pnl,
            cumulative_pnl=cumulative_pnl,
            open_positions_value=open_value,
        )
        s.merge(snap)
        s.commit()


def get_all_closed_trades_asc() -> list[dict]:
    """Return all closed/filled trades sorted oldest-first, for analytics charts."""
    with get_session() as s:
        rows = (
            s.query(Trade)
            .filter(Trade.status.in_(["filled", "closed"]))
            .order_by(Trade.timestamp.asc())
            .all()
        )
        return [_trade_to_dict(r) for r in rows]


def get_pnl_history(limit: int = 288) -> list[dict]:
    """Return last `limit` snapshots (288 = 24h at 5-min intervals)."""
    with get_session() as s:
        rows = (
            s.query(PnLSnapshot)
            .order_by(desc(PnLSnapshot.timestamp))
            .limit(limit)
            .all()
        )
        return [
            {
                "timestamp": r.timestamp,
                "daily_pnl": r.daily_pnl,
                "cumulative_pnl": r.cumulative_pnl,
                "open_positions_value": r.open_positions_value,
            }
            for r in reversed(rows)
        ]


# ── Open Positions ────────────────────────────────────────────────────────────

def upsert_position(
    market_id: str,
    token_id: str,
    question: str,
    strategy: str,
    side: str,
    size: float,
    entry_price: float,
    current_price: float,
    metadata_json: str = "",
) -> None:
    with get_session() as s:
        pos = s.get(OpenPosition, market_id)
        if pos is None:
            pos = OpenPosition(market_id=market_id, opened_at=int(time.time()))
        pos.token_id = token_id
        pos.question = question
        pos.strategy = strategy
        pos.side = side
        pos.size = size
        pos.entry_price = entry_price
        pos.current_price = current_price
        pos.unrealized_pnl = (
            (current_price - entry_price) * size
            if side == "BUY"
            else (entry_price - current_price) * size
        )
        pos.metadata_json = metadata_json or ""
        s.merge(pos)
        s.commit()


def remove_position(market_id: str) -> None:
    with get_session() as s:
        pos = s.get(OpenPosition, market_id)
        if pos:
            s.delete(pos)
            s.commit()


def get_open_positions() -> list[dict]:
    with get_session() as s:
        rows = s.query(OpenPosition).all()
        return [
            {
                "market_id": r.market_id,
                "token_id": r.token_id,
                "question": r.question,
                "strategy": r.strategy,
                "side": r.side,
                "size": r.size,
                "entry_price": r.entry_price,
                "current_price": r.current_price,
                "unrealized_pnl": r.unrealized_pnl,
                "opened_at": r.opened_at,
                "metadata_json": r.metadata_json or "",
            }
            for r in rows
        ]


def update_trades_for_market(
    market_id: str,
    fill_price: float,
    pnl: float,
    exit_reason: str = "",
) -> None:
    """Close all non-terminal trades for a market (used by position resolver)."""
    with get_session() as s:
        trades = (
            s.query(Trade)
            .filter(
                Trade.market_id == market_id,
                Trade.status.notin_(["cancelled", "closed"]),
            )
            .all()
        )
        for trade in trades:
            trade.fill_price = fill_price
            trade.pnl = pnl
            trade.status = "closed"
            if exit_reason:
                trade.exit_reason = exit_reason
        s.commit()


# ── Sentiment helpers — called only when running sentiment profile ─────────────

def insert_news_item(
    fingerprint: str,
    source: str,
    title: str,
    url: str,
    published_at,
    summary: str,
    raw_themes: str,
) -> int:
    """Insert a new news item and return its auto-generated id."""
    with get_session() as s:
        item = NewsItem(
            fingerprint=fingerprint,
            source=source,
            title=title,
            url=url,
            published_at=published_at,
            summary=summary,
            raw_themes=raw_themes,
        )
        s.add(item)
        s.commit()
        s.refresh(item)
        return item.id


def news_item_exists(fingerprint: str) -> bool:
    with get_session() as s:
        return s.query(NewsItem).filter(NewsItem.fingerprint == fingerprint).first() is not None


def insert_news_analysis(
    news_item_id: int,
    theme: str,
    is_relevant: bool,
    direction: str,
    confidence: float,
    urgency: float,
    impact_strength: float,
    reasoning_short: str,
    market_tags: str,
    analyzer_name: str,
) -> int:
    with get_session() as s:
        row = NewsAnalysis(
            news_item_id=news_item_id,
            theme=theme,
            is_relevant=is_relevant,
            direction=direction,
            confidence=confidence,
            urgency=urgency,
            impact_strength=impact_strength,
            reasoning_short=reasoning_short,
            market_tags=market_tags,
            analyzer_name=analyzer_name,
        )
        s.add(row)
        s.commit()
        s.refresh(row)
        return row.id


def insert_sentiment_decision(
    news_item_id: int,
    theme: str,
    market_id: str,
    market_question: str,
    market_price: float,
    estimated_probability: float,
    edge: float,
    decision: str,
    skip_reason: Optional[str] = None,
    trade_id: str = "",
) -> int:
    with get_session() as s:
        row = SentimentDecision(
            news_item_id=news_item_id,
            theme=theme,
            market_id=market_id,
            market_question=market_question,
            market_price=market_price,
            estimated_probability=estimated_probability,
            edge=edge,
            decision=decision,
            skip_reason=skip_reason,
            trade_id=trade_id,
        )
        s.add(row)
        s.commit()
        s.refresh(row)
        return row.id


def update_sentiment_decision_trade(decision_id: int, trade_id: str) -> None:
    with get_session() as s:
        row = s.get(SentimentDecision, decision_id)
        if row:
            row.trade_id = trade_id
            s.commit()


def get_recent_news(limit: int = 50) -> list[dict]:
    with get_session() as s:
        rows = (
            s.query(NewsItem)
            .order_by(desc(NewsItem.id))
            .limit(limit)
            .all()
        )
        return [
            {
                "id": r.id,
                "fingerprint": r.fingerprint,
                "source": r.source,
                "title": r.title,
                "url": r.url,
                "published_at": r.published_at.isoformat() if r.published_at else None,
                "summary": r.summary,
                "raw_themes": r.raw_themes,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]


def record_trade_outcome(theme: str, market_id: str, pnl: float) -> None:
    """Record the outcome of a closed trade for win-rate tracking."""
    with get_session() as s:
        # Update the most recent trade for this market with the pnl outcome
        row = (
            s.query(SentimentDecision)
            .filter(SentimentDecision.market_id == market_id)
            .filter(SentimentDecision.decision.in_(["buy_yes", "buy_no"]))
            .order_by(desc(SentimentDecision.id))
            .first()
        )
        if row and row.trade_id:
            row.skip_reason = f"outcome:pnl={pnl:.4f}"
            s.commit()


def get_theme_win_rate(theme: str, min_trades: int = 5) -> Optional[float]:
    """
    Return win rate (0.0–1.0) for a theme based on recent closed trades.
    Returns None if fewer than min_trades closed trades exist for the theme.
    Positive PnL = win. Zero PnL = neutral (excluded).
    """
    with get_session() as s:
        rows = (
            s.query(SentimentDecision)
            .filter(SentimentDecision.theme == theme)
            .filter(SentimentDecision.decision.in_(["buy_yes", "buy_no"]))
            .filter(SentimentDecision.skip_reason.like("outcome:pnl=%"))
            .order_by(desc(SentimentDecision.id))
            .limit(50)
            .all()
        )
        outcomes = []
        for row in rows:
            try:
                pnl = float(str(row.skip_reason).split("pnl=")[1])
                if pnl != 0.0:
                    outcomes.append(1 if pnl > 0 else 0)
            except Exception:
                pass
        if len(outcomes) < min_trades:
            return None
        return sum(outcomes) / len(outcomes)


def get_recent_decisions(limit: int = 50) -> list[dict]:
    with get_session() as s:
        rows = (
            s.query(SentimentDecision)
            .order_by(desc(SentimentDecision.id))
            .limit(limit)
            .all()
        )
        return [
            {
                "id": r.id,
                "news_item_id": r.news_item_id,
                "theme": r.theme,
                "market_id": r.market_id,
                "market_question": r.market_question,
                "market_price": r.market_price,
                "estimated_probability": r.estimated_probability,
                "edge": r.edge,
                "decision": r.decision,
                "skip_reason": r.skip_reason,
                "trade_id": r.trade_id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]


def _backfill_sentiment_trade_outcomes() -> None:
    """Repair historical sentiment decisions created before outcome tracking was fixed."""
    with get_session() as s:
        rows = (
            s.query(SentimentDecision, Trade)
            .join(Trade, Trade.id == SentimentDecision.trade_id)
            .filter(SentimentDecision.decision.in_(["buy_yes", "buy_no"]))
            .filter((SentimentDecision.skip_reason.is_(None)) | (SentimentDecision.skip_reason == ""))
            .filter(SentimentDecision.trade_id != "")
            .filter(Trade.status == "closed")
            .all()
        )
        if not rows:
            return

        for decision, trade in rows:
            decision.skip_reason = f"outcome:pnl={float(trade.pnl or 0.0):.4f}"

        s.commit()


_backfill_sentiment_trade_outcomes()


def set_market_cooldown(market_id: str, timestamp: float, strategy: str = "latency_arb") -> None:
    """Persist a market cooldown so it survives restarts."""
    with get_session() as s:
        row = s.get(MarketCooldown, market_id)
        if row:
            row.last_traded_at = timestamp
            row.strategy = strategy
        else:
            s.add(MarketCooldown(market_id=market_id, strategy=strategy, last_traded_at=timestamp))
        s.commit()


def get_market_cooldowns(strategy: str = "latency_arb") -> dict[str, float]:
    """Load all persisted market cooldowns for a strategy."""
    with get_session() as s:
        rows = s.query(MarketCooldown).filter(MarketCooldown.strategy == strategy).all()
        return {r.market_id: r.last_traded_at for r in rows}
