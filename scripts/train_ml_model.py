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
    0 4 * * * cd /home/kevi/polymarket-bot && python3 scripts/train_ml_model.py --save
"""
from __future__ import annotations

import json
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

FEATURES = [
    "entry_price",
    "abs_momentum",
    "ob_imbalance",
    "trend_slope",
    "trend_encoded",      # FLAT=0, UP=1, DOWN=2, WARMUP=3
    "hour_of_day",
    "day_of_week",
    "consec_losses",
    "asset_encoded",      # BTC=0, ETH=1
    "timeframe_encoded",  # 5m=0, 15m=1
]

TREND_MAP    = {"FLAT": 0, "UP": 1, "DOWN": 2, "WARMUP": 3, None: 0}
ASSET_MAP    = {"BTC": 0, "ETH": 1}
TIMEFRAME_MAP = {"5m": 0, "15m": 1, None: 0}


# ── Data loading ──────────────────────────────────────────────────────────────

def load_dataset(db_path: Path) -> tuple[pd.DataFrame, np.ndarray, list]:
    """
    Load all closed latency_arb trades that have ML features.
    Returns X (DataFrame), y (ndarray), trade_ids.
    Sorted by timestamp ascending (critical for TimeSeriesSplit).
    """
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT id, timestamp, price, pnl,
               momentum_at_entry, ob_imbalance_at_entry,
               trend_slope_at_entry, trend_direction_at_entry,
               consec_losses_at_entry, asset, timeframe
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

    records, y_list, ids = [], [], []

    for row in rows:
        (trade_id, ts, price, pnl,
         momentum, ob_imbalance, trend_slope, trend_dir,
         consec_losses, asset, timeframe) = row

        records.append({
            "entry_price":       float(price or 0.5),
            "abs_momentum":      abs(float(momentum or 0)),
            "ob_imbalance":      float(ob_imbalance or 0),
            "trend_slope":       float(trend_slope or 0),
            "trend_encoded":     float(TREND_MAP.get(trend_dir, 0)),
            "hour_of_day":       float((ts // 3600) % 24),
            "day_of_week":       float((ts // 86400) % 7),
            "consec_losses":     float(consec_losses or 0),
            "asset_encoded":     float(ASSET_MAP.get(asset, 0)),
            "timeframe_encoded": float(TIMEFRAME_MAP.get(timeframe, 0)),
        })
        y_list.append(1 if (pnl or 0) > 0 else 0)
        ids.append(trade_id)

    X = pd.DataFrame(records, columns=FEATURES)
    return X, np.array(y_list, dtype=np.int32), ids


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
    # Isotonic calibration converts raw LightGBM scores → reliable probabilities
    return CalibratedClassifierCV(base, method="isotonic", cv=3)


# ── Time-series cross-validation ─────────────────────────────────────────────

def evaluate(X: np.ndarray, y: np.ndarray) -> dict:
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss

    tscv = TimeSeriesSplit(n_splits=5)
    auc_scores, ll_scores, brier_scores = [], [], []
    fold_details = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        # Need enough data in both train and test
        if len(train_idx) < 40 or len(test_idx) < 10:
            continue

        X_tr, X_te = X.iloc[train_idx], X.iloc[test_idx]
        y_tr, y_te = y[train_idx], y[test_idx]

        model = build_model()
        model.fit(X_tr, y_tr)
        probs = model.predict_proba(X_te)[:, 1]

        auc  = roc_auc_score(y_te, probs)
        ll   = log_loss(y_te, probs)
        brier = brier_score_loss(y_te, probs)

        wr_actual = y_te.mean()
        wr_pred   = probs.mean()

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
            "actual_wr": round(wr_actual, 3),
            "predicted_wr": round(wr_pred, 3),
        })

    return {
        "cv_roc_auc": round(np.mean(auc_scores), 3),
        "cv_log_loss": round(np.mean(ll_scores), 3),
        "cv_brier":   round(np.mean(brier_scores), 3),
        "folds": fold_details,
    }


# ── Feature importance ────────────────────────────────────────────────────────

def feature_importance(X: np.ndarray, y: np.ndarray) -> list[tuple[str, float]]:
    """Train one full model and extract feature importances."""
    from lightgbm import LGBMClassifier
    base = LGBMClassifier(
        n_estimators=100, max_depth=4, min_child_samples=15,
        learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
        reg_lambda=1.0, class_weight="balanced", random_state=42, verbose=-1,
    )
    base.fit(X, y)
    importances = base.feature_importances_
    pairs = sorted(zip(FEATURES, importances), key=lambda x: x[1], reverse=True)
    return pairs


# ── Gate simulation ───────────────────────────────────────────────────────────

def simulate_gate(X: np.ndarray, y: np.ndarray, thresholds: list[float]) -> None:
    """
    Walk-forward simulation: train on first 60% of data, predict on remaining 40%.
    Show how each threshold would have performed vs no gate.
    """
    from sklearn.metrics import roc_auc_score

    split = int(len(X) * 0.6)
    X_tr, X_te = X.iloc[:split], X.iloc[split:]
    y_tr, y_te = y[:split], y[split:]

    model = build_model()
    model.fit(X_tr, y_tr)
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
    X, y, ids = load_dataset(DB_FILE)
    print(f"  {len(X)} trades loaded, {int(y.sum())} wins ({y.mean():.1%} WR)\n")

    print("Time-series cross-validation …")
    cv_results = evaluate(X, y)
    print(f"  CV ROC-AUC : {cv_results['cv_roc_auc']:.3f}  "
          f"(0.5=random, 0.6=useful, 0.7=strong)")
    print(f"  CV Log-Loss: {cv_results['cv_log_loss']:.3f}")
    print(f"  CV Brier   : {cv_results['cv_brier']:.3f}\n")

    print("  Fold breakdown:")
    for fd in cv_results["folds"]:
        print(f"    Fold {fd['fold']}: train={fd['train']} test={fd['test']} "
              f"AUC={fd['auc']} actual_wr={fd['actual_wr']:.1%} pred_wr={fd['predicted_wr']:.1%}")

    print("\nFeature importance (trained on full dataset):")
    importances = feature_importance(X, y)
    for name, score in importances:
        bar = "█" * int(score / max(s for _, s in importances) * 30)
        print(f"  {name:>25}: {score:>6.0f}  {bar}")

    simulate_gate(X, y, thresholds=[0.50, 0.52, 0.55, 0.58, 0.60])

    if save:
        print("\nTraining final model on full dataset …")
        model = build_model()
        model.fit(X, y)

        artifact = {
            "model":       model,
            "features":    FEATURES,
            "trained_at":  datetime.now(timezone.utc).isoformat(),
            "n_trades":    len(X),
            "win_rate":    float(y.mean()),
            "cv_roc_auc":  cv_results["cv_roc_auc"],
        }
        joblib.dump(artifact, MODEL_FILE)
        print(f"  Saved → {MODEL_FILE}")
    else:
        print("\n[DRY RUN] — pass --save to write model to disk.")


if __name__ == "__main__":
    main()
