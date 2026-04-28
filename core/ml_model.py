"""
Shadow ML model — scores every entry signal without affecting trading decisions.

Phase 1 (current): pure shadow mode.
  - Loads the saved LightGBM model from data/ml_model.pkl at startup.
  - Scores each signal at entry time and stores the probability in the DB.
  - Does NOT gate or size anything — predictions are observations only.

Phase 2 (future, when CV AUC > 0.60 consistently):
  - Enable GATE_ENABLED to skip signals below ML_GATE_THRESHOLD.
  - Enable KELLY_SIZING to scale position sizes by predicted win probability.

The model is refreshed from disk every ML_REFRESH_SECS seconds so a nightly
retrain (scripts/train_ml_model.py --save) takes effect without a bot restart.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

from utils.logger import logger

MODEL_FILE         = Path(__file__).parent.parent / "data" / "ml_model.pkl"
ML_REFRESH_SECS    = 3600.0   # reload model from disk every hour
MIN_AUC_FOR_GATE   = 0.60     # don't enable gating until model proves itself

# Phase 2 toggles — both False until model is validated
GATE_ENABLED       = False
KELLY_SIZING       = False
ML_GATE_THRESHOLD  = 0.52     # skip if predicted P(win) < this

TREND_MAP     = {"FLAT": 0, "UP": 1, "DOWN": 2, "WARMUP": 3}
ASSET_MAP     = {"BTC": 0, "ETH": 1}
TIMEFRAME_MAP = {"5m": 0, "15m": 1}


class MLModel:
    """
    Wraps the saved joblib artifact.  Thread-safe for reads; reload is
    guarded by a simple timestamp check (single writer: nightly cron).
    """

    def __init__(self):
        self._model = None
        self._features: list[str] = []
        self._cv_auc: float = 0.0
        self._n_trades: int = 0
        self._loaded_at: float = 0.0
        self._load_attempted_at: float = 0.0
        self._load()

    # ── Public API ────────────────────────────────────────────────────────────

    def predict(
        self,
        entry_price: float,
        momentum: float,
        ob_imbalance: float | None,
        trend_slope: float | None,
        trend_direction: str | None,
        consec_losses: int,
        asset: str,
        timeframe: str,
        hour: int,
        dow: int,
    ) -> float | None:
        """
        Return predicted P(win) in [0, 1], or None if model unavailable.
        Always returns a value — never raises.
        """
        self._maybe_reload()
        if self._model is None:
            return None

        try:
            import pandas as pd
            row = pd.DataFrame([{
                "entry_price":       float(entry_price),
                "abs_momentum":      abs(float(momentum)),
                "ob_imbalance":      float(ob_imbalance or 0),
                "trend_slope":       float(trend_slope or 0),
                "trend_encoded":     float(TREND_MAP.get(trend_direction, 0)),
                "hour_of_day":       float(hour),
                "day_of_week":       float(dow),
                "consec_losses":     float(consec_losses),
                "asset_encoded":     float(ASSET_MAP.get(asset, 0)),
                "timeframe_encoded": float(TIMEFRAME_MAP.get(timeframe, 0)),
            }], columns=self._features)

            prob = float(self._model.predict_proba(row)[0][1])
            return prob
        except Exception as exc:
            logger.debug(f"MLModel.predict failed: {exc}")
            return None

    @property
    def is_ready(self) -> bool:
        return self._model is not None

    @property
    def cv_auc(self) -> float:
        return self._cv_auc

    @property
    def n_trades(self) -> int:
        return self._n_trades

    def should_gate(self, prob: float | None) -> tuple[bool, str]:
        """
        Returns (skip: bool, reason: str).
        Currently always returns (False, '') since GATE_ENABLED=False.
        When Phase 2 is activated, this becomes the entry gate.
        """
        if not GATE_ENABLED or prob is None:
            return False, ""
        if self._cv_auc < MIN_AUC_FOR_GATE:
            return False, ""
        if prob < ML_GATE_THRESHOLD:
            return True, f"ml_gate(p={prob:.2f}<{ML_GATE_THRESHOLD})"
        return False, ""

    def kelly_size_mult(self, prob: float | None, avg_win: float, avg_loss: float) -> float:
        """
        Returns a size multiplier in [0.25, 2.0].
        Currently returns 1.0 (flat) since KELLY_SIZING=False.
        """
        if not KELLY_SIZING or prob is None or self._cv_auc < MIN_AUC_FOR_GATE:
            return 1.0
        if avg_loss <= 0:
            return 1.0
        b = avg_win / avg_loss
        f_full = (prob * (b + 1) - 1) / b
        f_frac = f_full * 0.25          # fractional Kelly — 25% of full
        mult   = f_frac / (avg_win / (avg_win + avg_loss))  # normalise to 1.0 at current WR
        return max(0.25, min(2.0, mult))

    # ── Internal ──────────────────────────────────────────────────────────────

    def _load(self) -> None:
        self._load_attempted_at = time.monotonic()
        if not MODEL_FILE.exists():
            logger.info("MLModel: no model file found — shadow mode inactive until first train")
            return
        try:
            import joblib
            artifact = joblib.load(MODEL_FILE)
            self._model    = artifact["model"]
            self._features = artifact["features"]
            self._cv_auc   = artifact.get("cv_roc_auc", 0.0)
            self._n_trades = artifact.get("n_trades", 0)
            self._loaded_at = time.monotonic()
            logger.info(
                f"MLModel loaded: n_trades={self._n_trades} "
                f"cv_auc={self._cv_auc:.3f} "
                f"({'SHADOW ONLY' if not GATE_ENABLED else 'GATE ACTIVE'})"
            )
        except Exception as exc:
            logger.warning(f"MLModel load failed: {exc}")
            self._model = None

    def _maybe_reload(self) -> None:
        """Reload from disk if the file is newer than our in-memory version."""
        if time.monotonic() - self._load_attempted_at < ML_REFRESH_SECS:
            return
        try:
            mtime = MODEL_FILE.stat().st_mtime if MODEL_FILE.exists() else 0
            if mtime > (time.time() - (time.monotonic() - self._loaded_at)):
                self._load()
            else:
                self._load_attempted_at = time.monotonic()
        except Exception:
            self._load_attempted_at = time.monotonic()


# ── Singleton ─────────────────────────────────────────────────────────────────

_ml_model: Optional[MLModel] = None


def get_ml_model() -> MLModel:
    global _ml_model
    if _ml_model is None:
        _ml_model = MLModel()
    return _ml_model
