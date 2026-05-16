"""REST API routes for the dashboard."""
from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from datetime import datetime
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import func

from core.risk_manager import get_risk_manager
from database import db
from database.models import OpenPosition, Trade

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
        asset_name = asset or "SOL"
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
    """Full analytics: equity curve per trade, daily bars, per-asset and entry-path breakdown."""
    all_trades = db.get_all_closed_trades_asc()
    # Only show latency_arb trades in analytics — other strategies distort the charts
    trades = [t for t in all_trades if (t.get("strategy") or "") == "latency_arb"]

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
            "asset": t.get("asset") or "SOL",
            "timeframe": _detect_timeframe(t.get("question") or ""),
            "side": t.get("side") or "",
            "entry_path": t.get("entry_path") or "UNKNOWN",
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
        a = t.get("asset") or "SOL"
        pnl = float(t.get("pnl") or 0)
        asset_map[a]["trades"] += 1
        asset_map[a]["pnl"] = round(asset_map[a]["pnl"] + pnl, 4)
        if pnl > 0:
            asset_map[a]["wins"] += 1
        else:
            asset_map[a]["losses"] += 1
    by_asset = [{"asset": k, **v} for k, v in sorted(asset_map.items())]

    # By entry path
    path_map: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
    for t in trades:
        path = t.get("entry_path") or "UNKNOWN"
        pnl = float(t.get("pnl") or 0)
        path_map[path]["trades"] += 1
        path_map[path]["pnl"] = round(path_map[path]["pnl"] + pnl, 4)
        if pnl > 0:
            path_map[path]["wins"] += 1
        else:
            path_map[path]["losses"] += 1
    by_entry_path = [{"entry_path": k, **v} for k, v in sorted(path_map.items()) if k != "UNKNOWN"]

    return {
        "equity_curve": equity_curve,
        "daily_bars": daily_bars,
        "by_asset": by_asset,
        "by_entry_path": by_entry_path,
    }



@router.get("/latency_arb/stats")
async def get_latency_arb_stats():
    """Filter rejection counts, ML shadow info, and regime for the dashboard."""
    import re
    from datetime import datetime, timezone, timedelta
    from pathlib import Path
    from config import settings as _s

    # Parse last 24h of log for filter rejections
    rejections: dict[str, int] = defaultdict(int)
    log_path = Path(_s.LOG_FILE)
    if log_path.exists():
        cutoff_ts = time.time() - 86400
        for line in log_path.read_text().splitlines():
            if "LatencyArb reject" not in line:
                continue
            m_reason = re.search(r"reason=(\w+)", line)
            if not m_reason:
                continue
            rejections[m_reason.group(1)] += 1

    # ML model metadata
    ml_info: dict = {}
    _pkl_err = None
    try:
        import joblib
        from pathlib import Path as _Path
        pkl_path = _Path(_s.DB_PATH).parent / "ml_model.pkl"
        model_data = joblib.load(pkl_path)
        ml_info = {
            "n_trades": model_data.get("n_trades", "?"),
            "cv_roc_auc": model_data.get("cv_roc_auc", "?"),
            "win_rate": model_data.get("win_rate", "?"),
            "trained_at": str(model_data.get("trained_at", "?"))[:19],
            "gate_enabled": getattr(_s, "GATE_ENABLED", False),
            "afternoon_auc": model_data.get("afternoon_cv_roc_auc", "?"),
        }
    except Exception as _e:
        _pkl_err = str(_e)

    # Hours since last latency_arb trade
    hours_since: float | None = None
    with db.get_session() as s:
        last = (
            s.query(Trade)
            .filter(Trade.strategy == "latency_arb")
            .order_by(Trade.timestamp.desc())
            .first()
        )
        if last:
            hours_since = round((time.time() - float(last.timestamp)) / 3600, 1)

    # Current regime
    utc_hour = datetime.now(timezone.utc).hour
    if utc_hour >= 21 or utc_hour < 7:
        regime = "Overnight (filtered)"
    elif 7 <= utc_hour < 9:
        regime = "EU Open"
    elif 9 <= utc_hour < 14:
        regime = "Mid-Session"
    elif 14 <= utc_hour < 21:
        regime = "Afternoon (active)"
    else:
        regime = "Unknown"

    return {
        "filter_rejections": dict(rejections),
        "total_rejected": sum(rejections.values()),
        "ml": ml_info,
        "hours_since_last_trade": hours_since,
        "regime": regime,
        "utc_hour": utc_hour,
    }


