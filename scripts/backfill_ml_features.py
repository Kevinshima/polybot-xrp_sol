"""
Backfill ML feature columns for the 355 historical latency_arb trades.

Recovers 5 features from the log file and DB trade sequence:
  - momentum_at_entry       : from "LatencyArb: BUY … momentum=X" log line
  - ob_imbalance_at_entry   : from "LatencyArb OB sizing … imbalance=X" log line (fires ~33ms before BUY)
  - trend_direction_at_entry: from nearest "LatencyArb tick: … trend=X" log line before trade
  - trend_slope_at_entry    : from same tick line (slope_pct=X%)
  - consec_losses_at_entry  : replayed from DB trade sequence (deterministic)

Run once:
    python3 scripts/backfill_ml_features.py

Prints a summary of what was recovered vs left NULL.
"""
from __future__ import annotations

import re
import sys
import sqlite3
from datetime import datetime
from pathlib import Path
from bisect import bisect_right

# ── Config ────────────────────────────────────────────────────────────────────

LOG_FILE = Path(__file__).parent.parent / "logs" / "bot.log"
DB_FILE  = Path(__file__).parent.parent / "data" / "polymarket.db"

# How far before a BUY log to look for its paired OB sizing line (seconds)
OB_WINDOW_SECS = 1.0
# How far back to look for a tick log to recover trend state (seconds)
TICK_WINDOW_SECS = 120.0

# ── Regex patterns ────────────────────────────────────────────────────────────

_DT   = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.(\d+)"

BUY_PAT = re.compile(
    _DT + r".*LatencyArb: BUY.*"
    r"\[((?:btc|eth)-updown-(5m|15m)-\d+)\]"
    r"\s+momentum=([\-+\d.]+)"
)
OB_PAT = re.compile(
    _DT + r".*LatencyArb OB sizing.*?imbalance=([\-+\d.]+)"
)
# Two tick formats:
#   old: "LatencyArb tick: BTC=… trend=FLAT slope_pct=+0.04%"
#   new: "LatencyArb tick: ETH=… trend=UP   slope_pct=-0.12%"
TICK_PAT = re.compile(
    _DT + r".*LatencyArb tick:\s*(BTC|ETH)=.*?\s+trend=(\w+)\s+slope_pct=([\-+\d.]+)%"
)


def _ts(date_str: str, frac_str: str) -> float:
    """Convert loguru datetime parts to unix timestamp."""
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp() + int(frac_str) / 1000


# ── Step 1: Parse log file ────────────────────────────────────────────────────

def parse_log(log_path: Path) -> tuple[list, list, dict]:
    """
    Returns:
        buy_events  : [(ts, momentum, asset, timeframe), ...]  sorted by ts
        ob_events   : [(ts, imbalance), ...]                    sorted by ts
        tick_events : {"BTC": [(ts, direction, slope), ...], "ETH": [...]}  sorted by ts
    """
    buy_events: list[tuple[float, float, str, str]] = []
    ob_events:  list[tuple[float, float]] = []
    tick_events: dict[str, list[tuple[float, str, float]]] = {"BTC": [], "ETH": []}

    print(f"Parsing {log_path} …", flush=True)
    with open(log_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = BUY_PAT.search(line)
            if m:
                ts  = _ts(m.group(1), m.group(2))
                asset = "ETH" if m.group(3).startswith("eth") else "BTC"
                tf  = m.group(4)           # "5m" or "15m"
                mom = float(m.group(5))
                buy_events.append((ts, mom, asset, tf))
                continue

            m = OB_PAT.search(line)
            if m:
                ts  = _ts(m.group(1), m.group(2))
                imb = float(m.group(3))
                ob_events.append((ts, imb))
                continue

            m = TICK_PAT.search(line)
            if m:
                ts    = _ts(m.group(1), m.group(2))
                asset = m.group(3)
                direction = m.group(4)
                slope = float(m.group(5))
                tick_events[asset].append((ts, direction, slope))

    buy_events.sort(key=lambda x: x[0])
    ob_events.sort(key=lambda x: x[0])
    for asset in tick_events:
        tick_events[asset].sort(key=lambda x: x[0])

    print(f"  BUY entries:  {len(buy_events)}")
    print(f"  OB entries:   {len(ob_events)}")
    print(f"  Tick BTC:     {len(tick_events['BTC'])}")
    print(f"  Tick ETH:     {len(tick_events['ETH'])}")
    return buy_events, ob_events, tick_events


# ── Step 2: Build per-trade feature lookup keyed by unix timestamp ─────────────

def build_feature_lookup(
    buy_events: list,
    ob_events: list,
    tick_events: dict,
) -> dict[float, dict]:
    """
    For each BUY event, find its matching OB event (within OB_WINDOW_SECS before it)
    and its most recent tick event (within TICK_WINDOW_SECS before it).
    Returns {buy_ts: {momentum, ob_imbalance, trend_direction, trend_slope}}.
    """
    ob_ts_list = [ts for ts, _ in ob_events]

    tick_ts: dict[str, list[float]] = {
        asset: [e[0] for e in evts] for asset, evts in tick_events.items()
    }

    lookup: dict[float, dict] = {}

    for buy_ts, momentum, asset, timeframe in buy_events:
        features: dict = {
            "momentum": momentum,
            "ob_imbalance": None,
            "trend_direction": None,
            "trend_slope": None,
            "asset": asset,
            "timeframe": timeframe,
        }

        # ── OB match: most recent OB line within OB_WINDOW_SECS before BUY ──
        # bisect to find insertion point of buy_ts in ob_ts_list
        idx = bisect_right(ob_ts_list, buy_ts) - 1
        if idx >= 0:
            ob_ts_val, ob_imb = ob_events[idx]
            if 0 <= buy_ts - ob_ts_val <= OB_WINDOW_SECS:
                features["ob_imbalance"] = ob_imb

        # ── Tick match: most recent tick for this asset within TICK_WINDOW_SECS ──
        asset_ticks = tick_events[asset]
        asset_ts    = tick_ts[asset]
        idx2 = bisect_right(asset_ts, buy_ts) - 1
        if idx2 >= 0:
            tick_ts_val, direction, slope = asset_ticks[idx2]
            if 0 <= buy_ts - tick_ts_val <= TICK_WINDOW_SECS:
                features["trend_direction"] = direction
                features["trend_slope"]     = slope / 100.0  # convert pct → ratio

        lookup[buy_ts] = features

    return lookup


# ── Step 3: Replay DB for consec_losses ──────────────────────────────────────

def compute_consec_losses(db_path: Path, feature_lookup: dict) -> dict[float, int]:
    """
    Replay trades chronologically and return {trade_ts: consec_losses_at_entry}.
    Uses the same logic as on_loss / on_win / on_stop_loss in latency_arb.py.
    """
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT timestamp, asset, pnl, exit_reason
        FROM trades
        WHERE strategy = 'latency_arb'
        ORDER BY timestamp ASC
        """
    ).fetchall()
    conn.close()

    # We need the timeframe per trade — derive it from feature_lookup where available,
    # otherwise fall back to inferring from pnl size (not reliable, so leave as "5m").
    ts_to_tf: dict[float, str] = {}
    for ts, features in feature_lookup.items():
        ts_to_tf[ts] = features["timeframe"]

    consec: dict[str, int] = {}  # key = "{asset}_{timeframe}"
    result: dict[float, int] = {}

    for db_ts, asset, pnl, exit_reason in rows:
        asset = asset or "BTC"
        tf    = ts_to_tf.get(float(db_ts), "5m")  # default 5m when unknown
        key   = f"{asset}_{tf}"

        if key not in consec:
            consec[key] = 0

        result[float(db_ts)] = consec[key]  # record BEFORE updating

        if pnl is not None:
            if pnl > 0:
                consec[key] = 0
            elif pnl < 0:
                consec[key] += 1
            # pnl == 0 → no change (open or cancelled)

    return result


# ── Step 4: Match log features to DB trades and UPDATE ───────────────────────

def match_and_update(
    db_path: Path,
    feature_lookup: dict,
    consec_lookup: dict,
    dry_run: bool = False,
) -> None:
    """
    For each DB trade, find the closest log BUY entry within 2 seconds
    and UPDATE the trade row with recovered features.
    """
    conn = sqlite3.connect(db_path)
    trades = conn.execute(
        """
        SELECT id, timestamp, asset
        FROM trades
        WHERE strategy = 'latency_arb'
          AND status IN ('filled', 'closed')
        ORDER BY timestamp ASC
        """
    ).fetchall()

    buy_ts_list = sorted(feature_lookup.keys())
    MATCH_WINDOW = 2.0

    updated = 0
    no_log_match = 0
    partial = 0

    updates: list[tuple] = []

    for trade_id, db_ts, asset in trades:
        db_ts_f = float(db_ts)

        # Find nearest BUY log entry within 2 seconds
        idx = bisect_right(buy_ts_list, db_ts_f + MATCH_WINDOW) - 1
        feat = None
        while idx >= 0:
            candidate_ts = buy_ts_list[idx]
            if abs(db_ts_f - candidate_ts) <= MATCH_WINDOW:
                feat = feature_lookup[candidate_ts]
                break
            idx -= 1

        consec = consec_lookup.get(db_ts_f)

        if feat is None:
            no_log_match += 1
            # Still update consec_losses if we have it
            if consec is not None:
                updates.append((None, None, None, None, consec, None, trade_id))
            continue

        if feat["ob_imbalance"] is None or feat["trend_direction"] is None:
            partial += 1

        updates.append((
            feat["momentum"],
            feat["ob_imbalance"],
            feat["trend_slope"],
            feat["trend_direction"],
            consec,
            feat["timeframe"],
            trade_id,
        ))
        updated += 1

    print(f"\nMatch results:")
    print(f"  Fully matched (log + consec): {updated}")
    print(f"  Partially matched (log only, no OB or trend): {partial}")
    print(f"  No log match (consec only):  {no_log_match}")
    print(f"  Total UPDATE statements:     {len(updates)}")

    if dry_run:
        print("\n[DRY RUN] — no DB changes written. Pass --apply to commit.")
        print("Sample updates:")
        for u in updates[:5]:
            print(" ", u)
        conn.close()
        return

    cur = conn.cursor()
    cur.executemany(
        """
        UPDATE trades SET
            momentum_at_entry        = COALESCE(?, momentum_at_entry),
            ob_imbalance_at_entry    = COALESCE(?, ob_imbalance_at_entry),
            trend_slope_at_entry     = COALESCE(?, trend_slope_at_entry),
            trend_direction_at_entry = COALESCE(?, trend_direction_at_entry),
            consec_losses_at_entry   = COALESCE(?, consec_losses_at_entry),
            timeframe                = COALESCE(?, timeframe)
        WHERE id = ?
        """,
        updates,
    )
    conn.commit()
    print(f"\nCommitted {cur.rowcount} rows updated.")

    # Verify
    row = conn.execute(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN momentum_at_entry IS NOT NULL THEN 1 ELSE 0 END) as has_mom,
            SUM(CASE WHEN ob_imbalance_at_entry IS NOT NULL THEN 1 ELSE 0 END) as has_ob,
            SUM(CASE WHEN trend_direction_at_entry IS NOT NULL THEN 1 ELSE 0 END) as has_trend,
            SUM(CASE WHEN consec_losses_at_entry IS NOT NULL THEN 1 ELSE 0 END) as has_consec,
            SUM(CASE WHEN timeframe IS NOT NULL THEN 1 ELSE 0 END) as has_tf
        FROM trades
        WHERE strategy = 'latency_arb' AND status IN ('filled', 'closed')
        """
    ).fetchone()
    print(f"\nVerification (of {row[0]} closed/filled trades):")
    print(f"  momentum:        {row[1]} / {row[0]}")
    print(f"  ob_imbalance:    {row[2]} / {row[0]}")
    print(f"  trend_direction: {row[3]} / {row[0]}")
    print(f"  consec_losses:   {row[4]} / {row[0]}")
    print(f"  timeframe:       {row[5]} / {row[0]}")

    conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    apply = "--apply" in sys.argv
    if not apply:
        print("Running in DRY RUN mode. Pass --apply to write to DB.\n")

    if not LOG_FILE.exists():
        print(f"ERROR: log file not found at {LOG_FILE}")
        sys.exit(1)

    buy_events, ob_events, tick_events = parse_log(LOG_FILE)
    feature_lookup = build_feature_lookup(buy_events, ob_events, tick_events)
    consec_lookup  = compute_consec_losses(DB_FILE, feature_lookup)

    print(f"\nFeature lookup built: {len(feature_lookup)} entries")
    print(f"Consec lookup built:  {len(consec_lookup)} entries")

    match_and_update(DB_FILE, feature_lookup, consec_lookup, dry_run=not apply)


if __name__ == "__main__":
    main()
