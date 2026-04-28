"""REST API routes for the dashboard."""
from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from datetime import datetime
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import func, or_

from sqlalchemy import distinct
from core.risk_manager import get_risk_manager
from database import db
from database.models import NewsAnalysis, NewsItem, OpenPosition, SentimentDecision, Trade

router = APIRouter(prefix="/api")


@router.get("/pnl")
async def get_pnl():
    daily = db.get_daily_pnl()
    cumulative = db.get_cumulative_pnl()
    history = db.get_pnl_history(limit=288)
    trades = db.get_recent_trades(limit=500)
    open_market_ids = {p["market_id"] for p in db.get_open_positions()}
    closed = [
        t for t in trades
        if t["status"] == "closed"
        or (t["status"] == "filled" and t["market_id"] not in open_market_ids)
    ]
    wins = [t for t in closed if float(t["pnl"]) > 0]
    win_rate = len(wins) / len(closed) if closed else 0.0
    return {
        "daily_pnl": round(daily, 4),
        "cumulative_pnl": round(cumulative, 4),
        "win_rate": round(win_rate, 4),
        "total_trades": len(closed),
        "history": history,
    }


@router.get("/pnl/by_asset")
async def get_pnl_by_asset():
    """Per-asset PnL breakdown for the dashboard chart (one series per asset)."""
    with db.get_session() as s:
        # Find all assets that have closed trades
        asset_rows = (
            s.query(Trade.asset, func.count(Trade.id), func.sum(Trade.pnl))
            .filter(Trade.status.in_(["filled", "closed"]))
            .group_by(Trade.asset)
            .all()
        )
    result = []
    for asset, trade_count, total_pnl in asset_rows:
        asset_name = asset or "BTC"
        with db.get_session() as s:
            closed = (
                s.query(Trade)
                .filter(
                    Trade.asset == asset,
                    Trade.status.in_(["filled", "closed"]),
                )
                .all()
            )
        wins = [t for t in closed if float(t.pnl or 0) > 0]
        result.append({
            "asset": asset_name,
            "total_trades": len(closed),
            "wins": len(wins),
            "losses": len(closed) - len(wins),
            "win_rate": round(len(wins) / len(closed), 4) if closed else 0.0,
            "total_pnl": round(float(total_pnl or 0), 4),
            "avg_pnl": round(float(total_pnl or 0) / len(closed), 4) if closed else 0.0,
        })
    return sorted(result, key=lambda r: r["asset"])


@router.get("/positions")
async def get_positions():
    return db.get_open_positions()


@router.get("/orders")
async def get_orders():
    return db.get_recent_trades(limit=100)


@router.get("/risk")
async def get_risk():
    return get_risk_manager().stats()


@router.get("/logs")
async def get_logs():
    from config import settings
    from pathlib import Path
    log_path = Path(settings.LOG_FILE)
    if not log_path.exists():
        return []
    lines = log_path.read_text().splitlines()
    return lines[-50:]


@router.post("/kill")
async def kill_switch():
    from core.client import get_client
    risk = get_risk_manager()
    risk.kill_all("dashboard kill switch")
    try:
        get_client().cancel_all_orders()
    except Exception as exc:
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=500)
    return {"status": "ok", "message": "Kill switch activated — all orders cancelled"}


@router.post("/resume")
async def resume():
    get_risk_manager().resume()
    return {"status": "ok", "message": "Bot resumed"}


@router.get("/health")
async def health():
    """Watchdog endpoint — returns healthy=true if bot is actively logging."""
    from config import settings
    from pathlib import Path

    now = time.time()
    log_path = Path(settings.LOG_FILE)

    last_activity: float | None = None
    if log_path.exists():
        last_activity = log_path.stat().st_mtime

    age = (now - last_activity) if last_activity is not None else 999999
    healthy = age < 120  # healthy if log written in last 2 minutes

    return {
        "status": "ok" if healthy else "frozen",
        "healthy": healthy,
        "last_activity_seconds_ago": round(age, 1),
        "timestamp": int(now),
    }


# ── Analytics ─────────────────────────────────────────────────────────────────

def _detect_timeframe(question: str) -> str:
    """Detect 5m or 15m from question text like '6:00PM-6:05PM' or '7:00PM-7:15PM'."""
    m = re.search(r'\d+:(\d+)[AP]M-\d+:(\d+)[AP]M', question or "")
    if m:
        diff = (int(m.group(2)) - int(m.group(1))) % 60
        if diff == 5:
            return "5m"
        if diff == 15:
            return "15m"
    return "?"


@router.get("/analytics")
async def get_analytics():
    """Full analytics: equity curve per trade, daily bars, per-asset and per-timeframe breakdown."""
    trades = db.get_all_closed_trades_asc()

    # Equity curve — one point per closed trade, running cumulative
    cumulative = 0.0
    equity_curve = []
    for t in trades:
        pnl = float(t.get("pnl") or 0)
        cumulative += pnl
        equity_curve.append({
            "timestamp": t["timestamp"],
            "pnl": round(pnl, 4),
            "cumulative_pnl": round(cumulative, 4),
            "question": (t.get("question") or "")[:60],
            "asset": t.get("asset") or "BTC",
            "timeframe": _detect_timeframe(t.get("question") or ""),
            "side": t.get("side") or "",
        })

    # Daily bars — group by calendar day
    daily: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "_ts": 0})
    for t in trades:
        ts = t["timestamp"]
        day_key = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        pnl = float(t.get("pnl") or 0)
        daily[day_key]["pnl"] = round(daily[day_key]["pnl"] + pnl, 4)
        daily[day_key]["_ts"] = max(daily[day_key]["_ts"], ts)
        if pnl > 0:
            daily[day_key]["wins"] += 1
        else:
            daily[day_key]["losses"] += 1
    daily_bars = [
        {"date": k, "label": datetime.strptime(k, "%Y-%m-%d").strftime("%b %d"),
         "wins": v["wins"], "losses": v["losses"], "pnl": v["pnl"]}
        for k, v in sorted(daily.items())
    ]

    # By asset
    asset_map: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        a = t.get("asset") or "BTC"
        pnl = float(t.get("pnl") or 0)
        asset_map[a]["trades"] += 1
        asset_map[a]["pnl"] = round(asset_map[a]["pnl"] + pnl, 4)
        if pnl > 0:
            asset_map[a]["wins"] += 1
        else:
            asset_map[a]["losses"] += 1
    by_asset = [{"asset": k, **v} for k, v in sorted(asset_map.items())]

    # By timeframe
    tf_map: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        tf = _detect_timeframe(t.get("question") or "")
        pnl = float(t.get("pnl") or 0)
        tf_map[tf]["trades"] += 1
        tf_map[tf]["pnl"] = round(tf_map[tf]["pnl"] + pnl, 4)
        if pnl > 0:
            tf_map[tf]["wins"] += 1
        else:
            tf_map[tf]["losses"] += 1
    by_timeframe = [{"timeframe": k, **v} for k, v in sorted(tf_map.items())]

    return {
        "equity_curve": equity_curve,
        "daily_bars": daily_bars,
        "by_asset": by_asset,
        "by_timeframe": by_timeframe,
    }


# ── Sentiment profile routes ───────────────────────────────────────────────────

@router.get("/sentiment/news")
async def get_sentiment_news():
    try:
        return db.get_recent_news(limit=50)
    except Exception:
        return []


@router.get("/sentiment/decisions")
async def get_sentiment_decisions():
    try:
        return db.get_recent_decisions(limit=50)
    except Exception:
        return []


@router.get("/sentiment/status")
async def get_sentiment_status():
    from config import settings
    daily_pnl = db.get_strategy_daily_pnl("ai_sentiment")
    return {
        "profile": getattr(settings, "ACTIVE_PROFILE", "latency"),
        "analyzer": getattr(settings, "SENTIMENT_ANALYZER", "keyword"),
        "anthropic_key_set": bool(
            getattr(settings, "ANTHROPIC_API_KEY", "")
            and settings.ANTHROPIC_API_KEY not in ("your-anthropic-key", "")
        ),
        "newsapi_key_set": bool(
            getattr(settings, "NEWS_API_KEY", "")
            and settings.NEWS_API_KEY not in ("your-newsapi-key", "")
        ),
        "strategy_daily_pnl": round(daily_pnl, 4),
        "strategy_loss_cap": getattr(settings, "SENTIMENT_MAX_DAILY_LOSS", 0.0),
        "strategy_halted": daily_pnl <= -getattr(settings, "SENTIMENT_MAX_DAILY_LOSS", 0.0),
    }


@router.get("/synth_arb/stats")
async def get_synth_arb_stats():
    """Synth arb open positions + lifetime stats for the dashboard panel."""
    from config import settings as _s
    enabled = getattr(_s, "SYNTH_ARB_ENABLED", False)

    # Live open positions from the running strategy instance
    positions: list[dict] = []
    try:
        from strategies.synth_arb import get_synth_arb
        strat = get_synth_arb()
        if strat is not None:
            positions = strat.open_positions_summary()
    except Exception:
        pass

    # Historical stats from DB
    daily_pnl = db.get_strategy_daily_pnl("synth_arb")
    cumulative_pnl = db.get_strategy_cumulative_pnl("synth_arb")

    with db.get_session() as s:
        closed = (
            s.query(Trade)
            .filter(Trade.strategy == "synth_arb", Trade.status == "closed")
            .all()
        )
    total_trades = len(closed)
    wins = [t for t in closed if float(t.pnl or 0) > 0]
    win_rate = len(wins) / total_trades if total_trades else 0.0
    avg_gap = (
        sum(float(t.pnl or 0) / float(t.size or 1) for t in wins) / len(wins)
        if wins else 0.0
    )

    return {
        "enabled": enabled,
        "open_count": len(positions),
        "positions": positions,
        "daily_pnl": round(daily_pnl, 4),
        "cumulative_pnl": round(cumulative_pnl, 4),
        "total_trades": total_trades,
        "win_rate": round(win_rate, 4),
        "avg_gap_pct": round(avg_gap * 100, 2),
    }


def _trade_mark_pnl(trade: Trade, open_position: OpenPosition | None) -> float:
    if trade.status == "closed":
        return float(trade.pnl or 0.0)
    mark_price = None
    entry_price = float(trade.price or 0.0)
    if open_position is not None:
        mark_price = float(open_position.current_price or 0.0)
        entry_price = float(open_position.entry_price or entry_price)
    elif trade.fill_price:
        mark_price = float(trade.fill_price)
    else:
        mark_price = float(trade.price or 0.0)

    size = float(trade.size or 0.0)
    if trade.side == "SELL":
        return size * (entry_price - mark_price)
    return size * (mark_price - entry_price)


@router.get("/sentiment/metrics")
async def get_sentiment_metrics():
    now = int(time.time())
    horizons = {
        "15m": 15 * 60,
        "1h": 60 * 60,
        "4h": 4 * 60 * 60,
        "24h": 24 * 60 * 60,
    }

    with db.get_session() as s:
        headlines_seen = s.query(func.count(NewsItem.id)).scalar() or 0
        relevant_headlines = (
            s.query(func.count(func.distinct(NewsAnalysis.news_item_id)))
            .filter(NewsAnalysis.is_relevant.is_(True))
            .scalar()
            or 0
        )
        mapped_headlines = (
            s.query(func.count(func.distinct(SentimentDecision.news_item_id)))
            .filter(
                or_(
                    SentimentDecision.skip_reason.is_(None),
                    SentimentDecision.skip_reason != "no_market_mapping",
                )
            )
            .scalar()
            or 0
        )
        sentiment_trades = (
            s.query(Trade)
            .filter(Trade.strategy == "ai_sentiment")
            .order_by(Trade.timestamp.desc())
            .all()
        )
        open_positions = {
            row.market_id: row
            for row in s.query(OpenPosition).filter(OpenPosition.strategy == "ai_sentiment").all()
        }
        decisions = (
            s.query(SentimentDecision, NewsItem)
            .join(NewsItem, NewsItem.id == SentimentDecision.news_item_id)
            .filter(SentimentDecision.decision.in_(("buy_yes", "buy_no")))
            .all()
        )

    trade_lookup = {trade.id: trade for trade in sentiment_trades}
    trade_by_market: dict[str, Trade] = {}
    for trade in sentiment_trades:
        trade_by_market.setdefault(trade.market_id, trade)
    horizon_metrics = {
        key: {"eligible_trades": 0, "total_pnl": 0.0, "avg_pnl": 0.0, "win_rate": 0.0}
        for key in horizons
    }
    by_source: dict[str, dict] = {}
    by_theme: dict[str, dict] = {}
    traded_headline_ids: set[int] = set()

    for key, seconds in horizons.items():
        eligible = [trade for trade in sentiment_trades if now - int(trade.timestamp or 0) >= seconds]
        pnls = [_trade_mark_pnl(trade, open_positions.get(trade.market_id)) for trade in eligible]
        wins = [pnl for pnl in pnls if pnl > 0]
        horizon_metrics[key] = {
            "eligible_trades": len(eligible),
            "total_pnl": round(sum(pnls), 4),
            "avg_pnl": round(sum(pnls) / len(pnls), 4) if pnls else 0.0,
            "win_rate": round(len(wins) / len(pnls), 4) if pnls else 0.0,
        }

    for decision, news_item in decisions:
        trade = trade_lookup.get(decision.trade_id) or trade_by_market.get(decision.market_id)
        if trade is None:
            continue
        traded_headline_ids.add(int(decision.news_item_id))
        pnl = _trade_mark_pnl(trade, open_positions.get(trade.market_id))
        source_key = (news_item.source or "unknown").strip() or "unknown"
        theme_key = (decision.theme or "unknown").strip() or "unknown"

        source_bucket = by_source.setdefault(source_key, {"source": source_key, "trades": 0, "total_pnl": 0.0, "wins": 0})
        source_bucket["trades"] += 1
        source_bucket["total_pnl"] += pnl
        if pnl > 0:
            source_bucket["wins"] += 1

        theme_bucket = by_theme.setdefault(theme_key, {"theme": theme_key, "trades": 0, "total_pnl": 0.0, "wins": 0})
        theme_bucket["trades"] += 1
        theme_bucket["total_pnl"] += pnl
        if pnl > 0:
            theme_bucket["wins"] += 1

    def finalize(rows: dict[str, dict], key_name: str) -> list[dict]:
        result = []
        for _, row in rows.items():
            trades = int(row["trades"])
            total_pnl = float(row["total_pnl"])
            wins = int(row["wins"])
            result.append(
                {
                    key_name: row[key_name],
                    "trades": trades,
                    "total_pnl": round(total_pnl, 4),
                    "avg_pnl": round(total_pnl / trades, 4) if trades else 0.0,
                    "win_rate": round(wins / trades, 4) if trades else 0.0,
                }
            )
        return sorted(result, key=lambda row: (row["total_pnl"], row["trades"]), reverse=True)

    return {
        "funnel": {
            "headlines_seen": headlines_seen,
            "relevant_headlines": relevant_headlines,
            "mapped_headlines": mapped_headlines,
            "traded_headlines": len(traded_headline_ids),
            "traded_markets": len(sentiment_trades),
            "open_positions": len(open_positions),
            "daily_pnl": round(db.get_strategy_daily_pnl("ai_sentiment"), 4),
            "cumulative_pnl": round(db.get_strategy_cumulative_pnl("ai_sentiment"), 4),
        },
        "horizons": horizon_metrics,
        "by_source": finalize(by_source, "source")[:8],
        "by_theme": finalize(by_theme, "theme")[:8],
    }
