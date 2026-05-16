"""
Backfill ML feature columns for historical latency_arb trades.

Recovers features from the log file and DB trade sequence:

  Original features (already backfilled on first run):
  - momentum_at_entry        : from "LatencyArb: BUY … momentum=X"
  - ob_imbalance_at_entry    : from "LatencyArb OB sizing … imbalance=X"
  - trend_direction_at_entry : from nearest "LatencyArb tick: … trend=X"
  - trend_slope_at_entry     : from same tick line
  - consec_losses_at_entry   : replayed from DB trade sequence

  Extended features (added 2026-05-05):
  - secs_remaining_in_window : derived from slug timestamp + trade timestamp
  - entry_path               : FAST_TRACK / CONFIRMED / 5M_DIRECT from log
  - consec_wins              : replayed from DB trade sequence

Run:
    python3 scripts/backfill_ml_features.py           # dry run — prints plan
    python3 scripts/backfill_ml_features.py --apply   # writes to DB
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

OB_WINDOW_SECS   = 1.0    # seconds before BUY to look for OB sizing line
TICK_WINDOW_SECS = 120.0  # seconds before BUY to look for tick line
PATH_WINDOW_SECS = 3.0    # seconds before BUY to look for CONFIRMED/FAST-TRACK

# ── Regex patterns ────────────────────────────────────────────────────────────

_DT = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.(\d+)"

BUY_PAT = re.compile(
    _DT + r".*LatencyArb: BUY.*"
    r"\[((?:btc|eth)-updown-(5m|15m)-(\d+))\]"
    r"\s+momentum=([\-+\d.]+)"
)
OB_PAT = re.compile(
    _DT + r".*LatencyArb OB sizing.*?imbalance=([\-+\d.]+)"
)
TICK_PAT = re.compile(
    _DT + r".*LatencyArb tick:\s*(BTC|ETH)=.*?\s+trend=(\w+)\s+slope_pct=([\-+\d.]+)%"
)
CONFIRMED_PAT = re.compile(
    _DT + r".*15m CONFIRMED\s+\[(?:BTC|ETH)\]"
)
FASTTRACK_PAT = re.compile(
    _DT + r".*15m FAST-TRACK entry\s+\[(?:BTC|ETH)\]"
)


def _ts(date_str: str, frac_str: str) -> float:
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp() + int(frac_str) / 1000


# ── Step 1: Parse log file ────────────────────────────────────────────────────

def parse_log(log_path: Path) -> tuple[list, list, dict, list]:
    """
    Returns:
        buy_events   : [(ts, momentum, asset, timeframe, slug, window_ts), ...]
        ob_events    : [(ts, imbalance), ...]
        tick_events  : {"SOL": [(ts, direction, slope), ...], "XRP": [...]}
        path_events  : [(ts, path_str), ...]  — CONFIRMED or FAST_TRACK markers
    """
    buy_events:  list = []
    ob_events:   list = []
    tick_events: dict = {"SOL": [], "XRP": []}
    path_events: list = []

    print(f"Parsing {log_path} …", flush=True)
    with open(log_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = BUY_PAT.search(line)
            if m:
                ts         = _ts(m.group(1), m.group(2))
                slug       = m.group(3)                           # e.g. sol-updown-15m-1777900500
                timeframe  = m.group(4)                           # "5m" or "15m"
                window_ts  = int(m.group(5))                      # unix timestamp of window start
                asset      = "XRP" if slug.startswith("xrp") else "SOL"
                momentum   = float(m.group(6))
                buy_events.append((ts, momentum, asset, timeframe, slug, window_ts))
                continue

            m = OB_PAT.search(line)
            if m:
                ts  = _ts(m.group(1), m.group(2))
                imb = float(m.group(3))
                ob_events.append((ts, imb))
                continue

            m = TICK_PAT.search(line)
            if m:
                ts        = _ts(m.group(1), m.group(2))
                asset     = m.group(3)
                direction = m.group(4)
                slope     = float(m.group(5))
                tick_events[asset].append((ts, direction, slope))
                continue

            m = CONFIRMED_PAT.search(line)
            if m:
                path_events.append((_ts(m.group(1), m.group(2)), "CONFIRMED"))
                continue

            m = FASTTRACK_PAT.search(line)
            if m:
                path_events.append((_ts(m.group(1), m.group(2)), "FAST_TRACK"))

    buy_events.sort(key=lambda x: x[0])
    ob_events.sort(key=lambda x: x[0])
    path_events.sort(key=lambda x: x[0])
    for asset in tick_events:
        tick_events[asset].sort(key=lambda x: x[0])

    print(f"  BUY entries:      {len(buy_events)}")
    print(f"  OB entries:       {len(ob_events)}")
    print(f"  Tick BTC:         {len(tick_events['BTC'])}")
    print(f"  Tick ETH:         {len(tick_events['ETH'])}")
    print(f"  Path markers:     {len(path_events)}")
    return buy_events, ob_events, tick_events, path_events


# ── Step 2: Build per-trade feature lookup ────────────────────────────────────

def build_feature_lookup(
    buy_events: list,
    ob_events: list,
    tick_events: dict,
    path_events: list,
) -> dict[float, dict]:
    ob_ts_list   = [ts for ts, _ in ob_events]
    path_ts_list = [ts for ts, _ in path_events]
    tick_ts: dict[str, list[float]] = {
        asset: [e[0] for e in evts] for asset, evts in tick_events.items()
    }

    lookup: dict[float, dict] = {}

    for buy_ts, momentum, asset, timeframe, slug, window_ts in buy_events:
        features: dict = {
            "momentum":              momentum,
            "ob_imbalance":          None,
            "trend_direction":       None,
            "trend_slope":           None,
            "asset":                 asset,
            "timeframe":             timeframe,
            "entry_path":            "5M_DIRECT",   # default unless CONFIRMED/FAST_TRACK found
            "secs_remaining":        None,
        }

        # ── OB: most recent OB line within 1s before BUY ──────────────────────
        idx = bisect_right(ob_ts_list, buy_ts) - 1
        if idx >= 0:
            ob_ts_val, ob_imb = ob_events[idx]
            if 0 <= buy_ts - ob_ts_val <= OB_WINDOW_SECS:
                features["ob_imbalance"] = ob_imb

        # ── Tick: most recent tick for this asset within 120s before BUY ──────
        asset_ticks = tick_events[asset]
        asset_ts    = tick_ts[asset]
        idx2 = bisect_right(asset_ts, buy_ts) - 1
        if idx2 >= 0:
            tick_ts_val, direction, slope = asset_ticks[idx2]
            if 0 <= buy_ts - tick_ts_val <= TICK_WINDOW_SECS:
                features["trend_direction"] = direction
                features["trend_slope"]     = slope / 100.0

        # ── entry_path: look for CONFIRMED or FAST_TRACK within 3s before BUY ─
        idx3 = bisect_right(path_ts_list, buy_ts) - 1
        if idx3 >= 0:
            path_ts_val, path_str = path_events[idx3]
            if 0 <= buy_ts - path_ts_val <= PATH_WINDOW_SECS:
                features["entry_path"] = path_str

        # ── secs_remaining_in_window: derived from slug ────────────────────────
        window_secs = 900 if timeframe == "15m" else 300
        remaining = (window_ts + window_secs) - buy_ts
        features["secs_remaining"] = max(0.0, remaining)

        lookup[buy_ts] = features

    return lookup


# ── Step 3: Replay DB for consec_losses + consec_wins ────────────────────────

def compute_consec_stats(
    db_path: Path,
    feature_lookup: dict,
) -> tuple[dict[float, int], dict[float, int]]:
    """
    Replay trades chronologically.
    Returns ({trade_ts: consec_losses_at_entry}, {trade_ts: consec_wins_at_entry}).
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

    ts_to_tf: dict[float, str] = {
        ts: feat["timeframe"] for ts, feat in feature_lookup.items()
    }

    consec_losses: dict[str, int] = {}
    consec_wins:   dict[str, int] = {}
    loss_result:   dict[float, int] = {}
    win_result:    dict[float, int] = {}

    for db_ts, asset, pnl, exit_reason in rows:
        asset  = asset or "SOL"
        tf     = ts_to_tf.get(float(db_ts), "5m")
        key    = f"{asset}_{tf}"

        consec_losses.setdefault(key, 0)
        consec_wins.setdefault(key, 0)

        loss_result[float(db_ts)] = consec_losses[key]
        win_result[float(db_ts)]  = consec_wins[key]

        if pnl is not None and pnl > 0:
            consec_losses[key] = 0
            consec_wins[key]  += 1
        elif pnl is not None and pnl < 0:
            consec_losses[key] += 1
            consec_wins[key]   = 0

    return loss_result, win_result


# ── Step 4: Match log features to DB trades and UPDATE ───────────────────────

def match_and_update(
    db_path: Path,
    feature_lookup: dict,
    consec_loss_lookup: dict,
    consec_win_lookup: dict,
    dry_run: bool = False,
) -> None:
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

    updated = no_log_match = partial = 0
    updates: list[tuple] = []

    for trade_id, db_ts, asset in trades:
        db_ts_f = float(db_ts)

        # Find nearest BUY log entry within 2 seconds
        idx = bisect_right(buy_ts_list, db_ts_f + MATCH_WINDOW) - 1
        feat = None
        while idx >= 0:
            if abs(db_ts_f - buy_ts_list[idx]) <= MATCH_WINDOW:
                feat = feature_lookup[buy_ts_list[idx]]
                break
            idx -= 1

        cl = consec_loss_lookup.get(db_ts_f)
        cw = consec_win_lookup.get(db_ts_f)

        if feat is None:
            no_log_match += 1
            if cl is not None or cw is not None:
                updates.append((None, None, None, None, cl, None, None, None, cw, trade_id))
            continue

        if feat["ob_imbalance"] is None or feat["trend_direction"] is None:
            partial += 1

        updates.append((
            feat["momentum"],
            feat["ob_imbalance"],
            feat["trend_slope"],
            feat["trend_direction"],
            cl,
            feat["timeframe"],
            feat["entry_path"],
            feat["secs_remaining"],
            cw,
            trade_id,
        ))
        updated += 1

    print(f"\nMatch results:")
    print(f"  Fully matched:   {updated}")
    print(f"  Partial match:   {partial}")
    print(f"  No log match:    {no_log_match}")
    print(f"  Total updates:   {len(updates)}")

    if dry_run:
        print("\n[DRY RUN] — pass --apply to write to DB.")
        print("Sample updates (first 5):")
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
            timeframe                = COALESCE(?, timeframe),
            entry_path               = COALESCE(?, entry_path),
            secs_since_trend_change  = COALESCE(?, secs_since_trend_change),
            consec_wins              = COALESCE(?, consec_wins)
        WHERE id = ?
        """,
        updates,
    )
    conn.commit()
    print(f"\nCommitted {cur.rowcount} rows updated.")

    row = conn.execute(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN momentum_at_entry IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN ob_imbalance_at_entry IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN trend_direction_at_entry IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN consec_losses_at_entry IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN timeframe IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN entry_path IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN secs_since_trend_change IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN consec_wins IS NOT NULL THEN 1 ELSE 0 END)
        FROM trades
        WHERE strategy = 'latency_arb' AND status IN ('filled', 'closed')
        """
    ).fetchone()
    total = row[0]
    labels = ["momentum", "ob_imbalance", "trend_direction", "consec_losses",
              "timeframe", "entry_path", "secs_remaining", "consec_wins"]
    print(f"\nVerification ({total} closed trades):")
    for i, label in enumerate(labels):
        print(f"  {label:30s}: {row[i+1]} / {total}")

    conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    apply = "--apply" in sys.argv
    if not apply:
        print("DRY RUN — pass --apply to write to DB.\n")

    if not LOG_FILE.exists():
        print(f"ERROR: log file not found at {LOG_FILE}")
        sys.exit(1)

    buy_events, ob_events, tick_events, path_events = parse_log(LOG_FILE)
    feature_lookup = build_feature_lookup(buy_events, ob_events, tick_events, path_events)
    consec_loss_lookup, consec_win_lookup = compute_consec_stats(DB_FILE, feature_lookup)

    print(f"\nFeature lookup: {len(feature_lookup)} entries")
    print(f"Consec lookup:  {len(consec_loss_lookup)} entries")

    match_and_update(
        DB_FILE, feature_lookup,
        consec_loss_lookup, consec_win_lookup,
        dry_run=not apply,
    )


if __name__ == "__main__":
    main()
