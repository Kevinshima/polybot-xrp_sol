"""
Train the latency-arb win-probability classifier.

Usage:
    python3 scripts/train_ml_model.py          # train + evaluate, print report
    python3 scripts/train_ml_model.py --save   # also save model to data/ml_model.pkl

The saved model is a dict:
    {
        "model":    CalibratedClassifierCV (LightGBM + Platt scaling),
        "features": list[str],        # ordered feature names
        "trained_at": ISO timestamp,
        "n_trades":   int,
        "cv_roc_auc": float,
    }

Nightly retrain cron:
    0 4 * * * cd /root/polymarket-bot && /root/polymarket-bot/.venv/bin/python3 scripts/train_ml_model.py --save >> /root/polymarket-bot/logs/ml_train.log 2>&1
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")

DB_FILE    = Path(__file__).parent.parent / "data" / "polymarket.db"
MODEL_FILE = Path(__file__).parent.parent / "data" / "ml_model.pkl"

# Exponential time-decay: trades from WEIGHT_HALF_LIFE_DAYS ago are weighted 0.5×.
# Trades from today = 1.0×. This makes the model adapt to current market regime
# without discarding old data entirely.
WEIGHT_HALF_LIFE_DAYS: float = 30.0

FEATURES = [
    # Original features
    "entry_price",
    "abs_momentum",
    "ob_imbalance",
    "trend_slope",
    "trend_encoded",            # FLAT=0, UP=1, DOWN=2, WARMUP=3
    "hour_of_day",
    "day_of_week",
    "consec_losses",
    "asset_encoded",            # SOL=0, XRP=1
    "cvd_at_entry",             # taker CVD ratio [-1,+1] at entry (trade-based OFI)
    "timeframe_encoded",        # 5m=0, 15m=1
    # Extended features — Phase 2
    "secs_remaining_in_window", # derived from slug + opened_at
    "momentum_delta",           # current - prev tick momentum
    "secs_since_trend_change",  # seconds since last FLAT/UP/DOWN transition
    "prev_trend_encoded",       # prev trend state (FLAT=0, UP=1, DOWN=2, WARMUP=3)
    "entry_path_encoded",       # 5M_DIRECT=0, CONFIRMED=1, FAST_TRACK=2
    "consec_wins",              # current winning streak
    "ob_at_queue_time",         # OB imbalance when 15m signal was first queued
    "cross_asset_agree",        # 1 if other asset momentum aligns with signal direction
    "asset_range_15m",          # normalized price range over last 15m
    # Regime features — Fix 4
    "is_afternoon",             # 1 if 14:00-21:00 UTC (highest-edge regime)
]

TREND_MAP      = {"FLAT": 0, "UP": 1, "DOWN": 2, "WARMUP": 3, None: 0}
ASSET_MAP      = {"SOL": 0, "XRP": 1}
TIMEFRAME_MAP  = {"5m": 0, "15m": 1, None: 0}
ENTRY_PATH_MAP = {"5M_DIRECT": 0, "CONFIRMED": 1, "FAST_TRACK": 2, None: 0}


# ── Data loading ──────────────────────────────────────────────────────────────

def _secs_remaining_from_market_id(market_id: str, opened_at: int) -> float:
    try:
        parts = str(market_id).split("-")
        window_ts = int(parts[-1])
        tf_part = [p for p in parts if p in ("5m", "15m")]
        window_secs = 900 if tf_part and tf_part[0] == "15m" else 300
        remaining = (window_ts + window_secs) - opened_at
        return max(0.0, float(remaining))
    except Exception:
        return 0.0


def load_dataset(db_path: Path) -> tuple[pd.DataFrame, np.ndarray, list, np.ndarray, np.ndarray]:
    """
    Load all closed latency_arb trades that have ML features.
    Returns X, y, trade_ids, timestamps (unix), pnl_values (for magnitude weighting).
    Sorted by timestamp ascending (critical for TimeSeriesSplit).

    Fix 2: Stale-FLAT overnight trades are excluded from training.
    These are the no-edge regime (secs_trend >= 3600, hour outside 06-21 UTC) where
    WR=31% and the model cannot distinguish them from good trades — they pollute learning.
    """
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT id, timestamp, price, pnl,
               momentum_at_entry, ob_imbalance_at_entry,
               trend_slope_at_entry, trend_direction_at_entry,
               consec_losses_at_entry, asset, timeframe, market_id,
               momentum_delta, secs_since_trend_change, prev_trend_direction,
               entry_path, consec_wins, ob_at_queue_time,
               cross_asset_agree, asset_range_15m, cvd_at_entry
        FROM trades
        WHERE strategy = 'latency_arb'
          AND status IN ('filled', 'closed')
          AND momentum_at_entry IS NOT NULL
          AND ob_imbalance_at_entry IS NOT NULL
          AND consec_losses_at_entry IS NOT NULL
        ORDER BY timestamp ASC
        """
    ).fetchall()
    conn.close()

    if not rows:
        raise ValueError("No feature-complete trades found in DB.")

    records, y_list, ids, timestamps, pnl_values = [], [], [], [], []
    skipped_stale = 0
    skipped_delta = 0
    skipped_ca = 0

    for row in rows:
        (trade_id, ts, price, pnl,
         momentum, ob_imbalance, trend_slope, trend_dir,
         consec_losses, asset, timeframe, market_id,
         momentum_delta, secs_since_trend_change, prev_trend_dir,
         entry_path, consec_wins, ob_at_queue_time,
         cross_asset_agree, asset_range_15m, cvd_at_entry) = row

        hour = (ts % 86400) / 3600.0

        # Fix 2: exclude stale-FLAT overnight — noise regime, actively misleads the model.
        # Data: 13 trades, WR=31%, -$160 PnL, ML gave them avg p=0.53 (couldn't detect them).
        # Window matches live filter: 21:00-07:00 UTC.
        is_overnight = hour >= 21 or hour < 7
        is_stale_flat = (secs_since_trend_change or 0) >= 3600
        if is_overnight and is_stale_flat:
            skipped_stale += 1
            continue

        # Fix 6: exclude CONFIRMED/5M_DIRECT with delta=0 or delta opposing momentum.
        # These are stalling or fading signals at entry — delta=0 WR=26.9%, opposed WR=42.9%.
        # FAST_TRACK exempt: extreme absolute momentum overrides deceleration (53% WR even when opposed).
        if entry_path in ("CONFIRMED", "5M_DIRECT") and momentum_delta is not None:
            _delta_aligned = (
                (momentum > 0 and momentum_delta > 0) or
                (momentum < 0 and momentum_delta < 0)
            )
            if momentum_delta == 0 or not _delta_aligned:
                skipped_delta += 1
                continue

        # Fix 7: exclude CONFIRMED/5M_DIRECT with ca=0 (cross-asset not confirming).
        # These are now blocked live — training on them teaches patterns we'll never see again.
        # ca=0 WR on these paths: 41-49% (-$1,060 all-time). Pure noise for the model.
        if entry_path in ("CONFIRMED", "5M_DIRECT") and (cross_asset_agree == 0 or cross_asset_agree is None):
            skipped_ca += 1
            continue

        # Fix 4: explicit regime binary feature — afternoon is the high-edge window (56% WR).
        # Giving the model this flag lets it learn conditional patterns within each regime.
        is_afternoon = float(1 if 14 <= hour < 21 else 0)

        records.append({
            "entry_price":              float(price or 0.5),
            "abs_momentum":             abs(float(momentum or 0)),
            "ob_imbalance":             float(ob_imbalance or 0),
            "trend_slope":              float(trend_slope or 0),
            "trend_encoded":            float(TREND_MAP.get(trend_dir, 0)),
            "hour_of_day":              float(hour),
            "day_of_week":              float((ts // 86400) % 7),
            "consec_losses":            float(consec_losses or 0),
            "asset_encoded":            float(ASSET_MAP.get(asset, 0)),
            "timeframe_encoded":        float(TIMEFRAME_MAP.get(timeframe, 0)),
            "secs_remaining_in_window": _secs_remaining_from_market_id(market_id, ts),
            "momentum_delta":           float(momentum_delta or 0),
            "secs_since_trend_change":  float(secs_since_trend_change or 0),
            "prev_trend_encoded":       float(TREND_MAP.get(prev_trend_dir, 0)),
            "entry_path_encoded":       float(ENTRY_PATH_MAP.get(entry_path, 0)),
            "consec_wins":              float(consec_wins or 0),
            "ob_at_queue_time":         float(ob_at_queue_time or 0),
            "cross_asset_agree":        float(cross_asset_agree if cross_asset_agree is not None else 0),
            "asset_range_15m":          float(asset_range_15m or 0),
            "cvd_at_entry":             float(cvd_at_entry if cvd_at_entry is not None else 0),
            "is_afternoon":             is_afternoon,
        })
        y_list.append(1 if (pnl or 0) > 0 else 0)
        ids.append(trade_id)
        timestamps.append(float(ts))
        pnl_values.append(float(pnl or 0))

    if skipped_stale:
        print(f"  [Fix 2] Excluded {skipped_stale} stale-FLAT overnight trades from training")
    if skipped_delta:
        print(f"  [Fix 6] Excluded {skipped_delta} delta-zero/opposed CONFIRMED/5M_DIRECT from training")
    if skipped_ca:
        print(f"  [Fix 7] Excluded {skipped_ca} ca=0 CONFIRMED/5M_DIRECT from training")

    X = pd.DataFrame(records, columns=FEATURES)
    return (
        X,
        np.array(y_list, dtype=np.int32),
        ids,
        np.array(timestamps, dtype=np.float64),
        np.array(pnl_values, dtype=np.float64),
    )


# ── Sample weights ────────────────────────────────────────────────────────────

def compute_combined_weights(
    timestamps: np.ndarray,
    pnl_values: np.ndarray,
    half_life_days: float = WEIGHT_HALF_LIFE_DAYS,
) -> np.ndarray:
    """
    Combined temporal-decay × |pnl|-magnitude weights.

    Fix 3: Two components:
    - Temporal: recent trades weighted more (half-life 30 days).
      A trade from 30 days ago has weight 0.5 vs today's 1.0.
    - PnL magnitude: larger-outcome trades weighted more (capped at 3× median).
      A +$40 outcome teaches the model more than a ±$2 near-zero near fee noise.

    Both components normalize to mean=1.0 so LightGBM treats the sum correctly.
    """
    # Temporal component
    now_ts = timestamps.max()
    days_ago = (now_ts - timestamps) / 86400.0
    temporal = np.exp(-np.log(2) / half_life_days * days_ago)

    # PnL magnitude component — cap at 3× median to prevent outliers dominating
    abs_pnl = np.abs(pnl_values)
    nonzero = abs_pnl[abs_pnl > 0]
    median_abs = float(np.median(nonzero)) if len(nonzero) > 0 else 1.0
    pnl_w = np.clip(abs_pnl / max(median_abs, 0.01), 0.5, 3.0)

    combined = temporal * pnl_w
    combined = combined / combined.mean()  # normalize to mean=1.0
    return combined


# ── Model definition ──────────────────────────────────────────────────────────

def build_model():
    from lightgbm import LGBMClassifier
    from sklearn.calibration import CalibratedClassifierCV

    base = LGBMClassifier(
        n_estimators=100,
        max_depth=4,            # shallow — prevents overfit on small dataset
        min_child_samples=15,   # each leaf requires 15+ trades
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        class_weight="balanced",
        random_state=42,
        verbose=-1,
    )
    # Sigmoid (Platt) calibration: correct choice for < ~1000 calibration samples.
    # Isotonic overfits badly on small datasets (needs 1000+ to beat Platt).
    return CalibratedClassifierCV(base, method="sigmoid", cv=3)


# ── Time-series cross-validation ─────────────────────────────────────────────

def evaluate(X: pd.DataFrame, y: np.ndarray, weights: np.ndarray | None = None) -> dict:
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss

    tscv = TimeSeriesSplit(n_splits=5)
    auc_scores, ll_scores, brier_scores = [], [], []
    fold_details = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        if len(train_idx) < 40 or len(test_idx) < 10:
            continue

        X_tr, X_te = X.iloc[train_idx], X.iloc[test_idx]
        y_tr, y_te = y[train_idx], y[test_idx]
        w_tr = weights[train_idx] if weights is not None else None

        model = build_model()
        model.fit(X_tr, y_tr, sample_weight=w_tr)
        probs = model.predict_proba(X_te)[:, 1]

        auc   = roc_auc_score(y_te, probs)
        ll    = log_loss(y_te, probs)
        brier = brier_score_loss(y_te, probs)

        auc_scores.append(auc)
        ll_scores.append(ll)
        brier_scores.append(brier)

        fold_details.append({
            "fold": fold + 1,
            "train": len(train_idx),
            "test": len(test_idx),
            "auc": round(auc, 3),
            "log_loss": round(ll, 3),
            "brier": round(brier, 3),
            "actual_wr": round(float(y_te.mean()), 3),
            "predicted_wr": round(float(probs.mean()), 3),
        })

    return {
        "cv_roc_auc": round(float(np.mean(auc_scores)), 3) if auc_scores else 0.0,
        "cv_log_loss": round(float(np.mean(ll_scores)), 3) if ll_scores else 0.0,
        "cv_brier":   round(float(np.mean(brier_scores)), 3) if brier_scores else 0.0,
        "folds": fold_details,
    }


# ── Feature importance ────────────────────────────────────────────────────────

def feature_importance(X: pd.DataFrame, y: np.ndarray, weights: np.ndarray | None = None) -> list[tuple[str, float]]:
    from lightgbm import LGBMClassifier
    base = LGBMClassifier(
        n_estimators=100, max_depth=4, min_child_samples=15,
        learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
        reg_lambda=1.0, class_weight="balanced", random_state=42, verbose=-1,
    )
    base.fit(X, y, sample_weight=weights)
    importances = base.feature_importances_
    pairs = sorted(zip(FEATURES, importances), key=lambda x: x[1], reverse=True)
    return pairs


# ── Gate simulation ───────────────────────────────────────────────────────────

def simulate_gate(X: pd.DataFrame, y: np.ndarray, thresholds: list[float], weights: np.ndarray | None = None) -> None:
    from sklearn.metrics import roc_auc_score

    split = int(len(X) * 0.6)
    X_tr, X_te = X.iloc[:split], X.iloc[split:]
    y_tr, y_te = y[:split], y[split:]
    w_tr = weights[:split] if weights is not None else None

    model = build_model()
    model.fit(X_tr, y_tr, sample_weight=w_tr)
    probs = model.predict_proba(X_te)[:, 1]
    auc   = roc_auc_score(y_te, probs)

    print(f"\n  Walk-forward gate simulation (train={split}, test={len(X_te)}, AUC={auc:.3f}):")
    print(f"  {'Threshold':>10} {'Trades kept':>12} {'WR':>8} {'Skip rate':>10}")
    print(f"  {'(no gate)':>10} {len(y_te):>12} {y_te.mean():.1%} {'0.0%':>10}")

    for thr in thresholds:
        mask = probs >= thr
        if mask.sum() == 0:
            continue
        kept_wr   = y_te[mask].mean()
        skip_rate = 1 - mask.mean()
        print(f"  {thr:>10.2f} {mask.sum():>12} {kept_wr:.1%} {skip_rate:.1%}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    save = "--save" in sys.argv

    print("Loading dataset …")
    X, y, ids, timestamps, pnl_values = load_dataset(DB_FILE)
    print(f"  {len(X)} trades loaded, {int(y.sum())} wins ({y.mean():.1%} WR)\n")

    weights = compute_combined_weights(timestamps, pnl_values)
    oldest_days = (timestamps.max() - timestamps.min()) / 86400
    min_weight  = float(weights.min())
    print(f"  Combined weights: half-life={WEIGHT_HALF_LIFE_DAYS}d  "
          f"span={oldest_days:.0f}d  oldest_weight={min_weight:.3f}  "
          f"(temporal × |pnl| magnitude)\n")

    print("Time-series cross-validation (all filtered trades) …")
    cv_results = evaluate(X, y, weights=weights)
    print(f"  CV ROC-AUC : {cv_results['cv_roc_auc']:.3f}  "
          f"(0.5=random, 0.6=useful, 0.7=strong)")
    print(f"  CV Log-Loss: {cv_results['cv_log_loss']:.3f}")
    print(f"  CV Brier   : {cv_results['cv_brier']:.3f}\n")

    print("  Fold breakdown:")
    for fd in cv_results["folds"]:
        print(f"    Fold {fd['fold']}: train={fd['train']} test={fd['test']} "
              f"AUC={fd['auc']} actual_wr={fd['actual_wr']:.1%} pred_wr={fd['predicted_wr']:.1%}")

    # Fix 5: Afternoon-only cross-validation — the regime where the strategy has actual edge.
    # This is the honest signal the model needs to learn from.
    print("\nAfternoon-only cross-validation (14:00–21:00 UTC) …")
    hour_arr = X["hour_of_day"].values
    aft_mask = (hour_arr >= 14) & (hour_arr < 21)
    X_aft = X[aft_mask].reset_index(drop=True)
    y_aft = y[aft_mask]
    w_aft = weights[aft_mask]
    cv_aft_auc: float | None = None
    if len(X_aft) >= 60:
        cv_aft = evaluate(X_aft, y_aft, weights=w_aft)
        cv_aft_auc = cv_aft["cv_roc_auc"]
        print(f"  Afternoon CV AUC : {cv_aft_auc:.3f}  n={len(X_aft)}  WR={y_aft.mean():.1%}")
        print(f"  Afternoon Log-Loss: {cv_aft['cv_log_loss']:.3f}  Brier: {cv_aft['cv_brier']:.3f}")
        print("  Afternoon fold breakdown:")
        for fd in cv_aft["folds"]:
            print(f"    Fold {fd['fold']}: train={fd['train']} test={fd['test']} "
                  f"AUC={fd['auc']} actual_wr={fd['actual_wr']:.1%}")
    else:
        print(f"  Insufficient afternoon trades for CV (n={len(X_aft)}, need ≥60)")

    print("\nFeature importance (trained on full filtered dataset):")
    importances = feature_importance(X, y, weights=weights)
    for name, score in importances:
        bar = "█" * int(score / max(s for _, s in importances) * 30)
        print(f"  {name:>25}: {score:>6.0f}  {bar}")

    simulate_gate(X, y, thresholds=[0.50, 0.52, 0.55, 0.58, 0.60], weights=weights)

    if save:
        print("\nTraining final model on full filtered dataset …")
        model = build_model()
        model.fit(X, y, sample_weight=weights)

        artifact = {
            "model":                model,
            "features":             FEATURES,
            "trained_at":           datetime.now(timezone.utc).isoformat(),
            "n_trades":             len(X),
            "win_rate":             float(y.mean()),
            "cv_roc_auc":           cv_results["cv_roc_auc"],
            "afternoon_cv_roc_auc": cv_aft_auc,
        }
        joblib.dump(artifact, MODEL_FILE)
        print(f"  Saved → {MODEL_FILE}")
    else:
        print("\n[DRY RUN] — pass --save to write model to disk.")


if __name__ == "__main__":
    main()
