"""Strategy 1: Price-lag arbitrage vs. crypto exchanges.

Supports SOL and XRP. Each asset uses its own Binance feed, order-book stream,
trend filter, and momentum signal. They share the same portfolio capacity limits
(MAX_CONCURRENT) and database. BTC is tracked as a cross-asset momentum validator
but is never traded directly.
"""
from __future__ import annotations

import asyncio
import json
import math
import time
from collections import deque
from datetime import datetime

from config import settings
from core.ml_model import get_ml_model
from data.exchange_feed import get_exchange_feed
from data.liquidation_feed import get_liquidation_feed
from data.market_scanner import get_scanner
from data.polymarket_feed import get_polymarket_feed
from data.rtds_feed import get_rtds_feed
from monitoring.alerter import get_alerter
from strategies.base import BaseStrategy
from utils.logger import logger


class LatencyArb(BaseStrategy):
    """
    Compares Binance spot momentum against Polymarket updown-5m and updown-15m
    markets for SOL and XRP. Buys the UP token on positive momentum and the DOWN
    token on negative momentum when divergence exceeds threshold.
    BTC momentum is also tracked for cross-asset validation (not traded).

    Runs every 500ms.
    """

    name = "latency_arb"

    MAX_ENTRY_MID_PRICE = 0.65
    BASE_MOMENTUM_THRESHOLD = settings.LAB_MOMENTUM_THRESHOLD
    FIFTEEN_MIN_CONFIRMATION_MULTIPLIER = 1.25
    # Lowered from 2.0 to 1.0 for data collection phase: at 2.0 the threshold fired so late
    # (0.24%) that Polymarket contracts were already priced >$0.85 — no edge window remaining.
    # At 1.0, signals fire earlier (0.12%/0.15%) when contracts are still at $0.45–$0.60.
    FLAT_MOMENTUM_MULTIPLIER = 1.0
    REJECTION_SUMMARY_INTERVAL_SECS = 60.0
    TREND_SAMPLE_INTERVAL_SECS = 10.0
    TREND_MIN_VALID_SAMPLES = 8
    MARKET_REFRESH_INTERVAL_SECS = 20.0
    SIGNAL_SAMPLE_INTERVAL_SECS = 1.0
    SIGNAL_HISTORY_RETENTION_SECS = 900.0
    FIFTEEN_MIN_SIGNAL_WINDOW_SECS = 600.0
    FIFTEEN_MIN_SIGNAL_MIN_WINDOW_SECS = 300.0

    def __init__(self):
        super().__init__()
        self._exchange_feed = get_exchange_feed()
        self._liq_feed = get_liquidation_feed()
        self._scanner = get_scanner()
        self._pm_feed = get_polymarket_feed()
        self._rtds_feed = get_rtds_feed()
        self.MAX_CONCURRENT = settings.LAB_MAX_CONCURRENT_POSITIONS
        self._branch_limits = {
            "5m": settings.LAB_MAX_CONCURRENT_POSITIONS_5M,
            "15m": settings.LAB_MAX_CONCURRENT_POSITIONS_15M,
        }
        self._trend_slope_threshold_pct = self._normalize_trend_slope_threshold_pct(
            settings.TREND_FILTER_MIN_SLOPE
        )

        # Active trading assets: SOL and/or XRP (enabled via settings).
        # BTC is tracked as a cross-asset validator only — not in this list.
        self._assets: list[str] = (
            (["SOL"] if settings.SOL_LAB_ENABLED else []) +
            (["XRP"] if settings.XRP_LAB_ENABLED else [])
        )

        # ── Per-asset market cache ────────────────────────────────────────────
        self._current_updowns: dict[str, dict | None] = {
            f"{a}_{w}": None for a in self._assets for w in ["5m", "15m"]
        }
        self._last_market_fetch: dict[str, float] = {
            f"{a}_{w}": 0.0 for a in self._assets for w in ["5m", "15m"]
        }
        # Tracks when market data was last *successfully* received (not just checked)
        self._market_data_ts: dict[str, float] = {
            f"{a}_{w}": 0.0 for a in self._assets for w in ["5m", "15m"]
        }
        self._all_markets_none_logged_at: float = 0.0

        # ── Per-asset tick logging ────────────────────────────────────────────
        self._last_momentum_log: dict[str, float] = {a: 0.0 for a in self._assets}

        # ── Per-asset trend filter ────────────────────────────────────────────
        self._price_history: dict[str, deque] = {
            a: deque(maxlen=settings.TREND_FILTER_TICKS) for a in self._assets
        }
        self._signal_history: dict[str, deque] = {a: deque() for a in self._assets}
        self._trend_direction: dict[str, str | None] = {a: None for a in self._assets}
        self._last_slope: dict[str, float] = {a: 0.0 for a in self._assets}
        self._last_warmup_log: dict[str, float] = {a: 0.0 for a in self._assets}
        self._last_warmup_sample_count: dict[str, int] = {a: -1 for a in self._assets}
        self._last_signal_sample_ts: dict[str, float] = {a: 0.0 for a in self._assets}
        self._last_trend_sample_ts: dict[str, float] = {a: 0.0 for a in self._assets}
        self._last_logged_trend: dict[str, str] = {a: "WARMUP" for a in self._assets}

        # ── Per-asset 15m context gate ────────────────────────────────────────
        self._last_15m_direction: dict[str, str | None] = {a: None for a in self._assets}
        self._last_15m_direction_ts: dict[str, float | None] = {a: None for a in self._assets}
        self._last_15m_slug: dict[str, str] = {f"{a}_15m": "" for a in self._assets}

        # ── Per-asset loss/win counters (ML features) ────────────────────────
        self._consecutive_losses: dict[str, int] = {
            f"{a}_{tf}": 0 for a in self._assets for tf in ["5m", "15m"]
        }
        self._consecutive_wins: dict[str, int] = {
            f"{a}_{tf}": 0 for a in self._assets for tf in ["5m", "15m"]
        }

        # ── Consecutive loss pause gate (time-keyed per asset+timeframe) ────────
        self._consec_loss_pause_until: dict[str, float] = {}

        # ── Strategy-level rolling circuit breaker ────────────────────────────
        # Fires when 3+ stop-losses hit in any 90-minute window → pauses ALL
        # entries for 2 hours. Resets automatically. Prevents May-11-style runs.
        self._recent_stop_loss_ts: list[float] = []
        self._circuit_breaker_until: float = 0.0

        # ── Per-asset momentum delta tracking ────────────────────────────────
        self._prev_momentum: dict[str, float] = {a: 0.0 for a in self._assets}

        # ── Per-asset trend change tracking (ML features) ─────────────────────
        self._trend_change_ts: dict[str, float] = {a: 0.0 for a in self._assets}
        self._prev_trend_direction: dict[str, str] = {a: "WARMUP" for a in self._assets}

        # ── Per-asset log state ───────────────────────────────────────────────
        self._rejection_log_state: dict[str, dict] = {
            f"{a}_{tf}": {"signature": None, "suppressed": 0, "last_log": 0.0}
            for a in self._assets for tf in ["5m", "15m"]
        }
        self._weak_15m_log_state: dict[str, dict] = {
            a: {"signature": None, "suppressed": 0, "last_log": 0.0}
            for a in self._assets
        }

        # ── Global slug-keyed state (slugs include asset name, no collision) ──
        self._traded_this_cycle: set[str] = set()
        self._cooldown: dict[str, dict[str, float]] = {"5m": {}, "15m": {}}
        self._pending_15m: dict[str, dict] = {}
        self._entered_15m_slugs: set[str] = set()
        # Binance price recorded when each 15m slug is first seen — used for oracle lag
        self._window_open_snapshots: dict[str, dict] = {}
        self._holding_logged: set[str] = set()
        self._last_mid_discard_ts: dict[str, float] = {}

        # ── Price-skip throttle: don't hammer get_midpoint when market is already priced ──
        # When _trade_updown returns False due to price checks, wait 15s before retrying
        self._price_skip_until: dict[str, float] = {}

        # ── Post-restart lockout ─────────────────────────────────────────────
        # Block all new entries for 15 minutes after startup so that the momentum
        # window (600s), trend slope (20 ticks × 10s), and whipsaw guard all have
        # enough data to reflect actual market conditions before we commit capital.
        self._startup_ts: float = time.time()
        self.STARTUP_LOCKOUT_SECS: float = 300.0  # 5 minutes

        logger.info(
            "LatencyArb config: "
            f"assets={self._assets} "
            f"global_cap={self.MAX_CONCURRENT} "
            f"cap_5m={self._branch_limits['5m']} cap_15m={self._branch_limits['15m']} "
            f"trend_slope_threshold={self._trend_slope_threshold_pct:.3f}% "
            f"trend_warmup={self._required_trend_samples()} samples "
            f"base_threshold_5m={self.BASE_MOMENTUM_THRESHOLD:.4%} "
            f"base_threshold_15m={self.BASE_MOMENTUM_THRESHOLD * self.FIFTEEN_MIN_CONFIRMATION_MULTIPLIER:.4%} "
            f"market_refresh={self.MARKET_REFRESH_INTERVAL_SECS:.0f}s "
            f"signal_window_15m={self.FIFTEEN_MIN_SIGNAL_WINDOW_SECS:.0f}s"
        )

    async def run(self) -> None:
        logger.info("LatencyArb starting")

        while self._running:
            if self._check_halted():
                await asyncio.sleep(5)
                continue

            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"LatencyArb tick error: {exc}")

            await asyncio.sleep(settings.LAB_POLL_INTERVAL)

    async def _tick(self) -> None:
        self._traded_this_cycle = set()

        await self._refresh_all_markets()

        for asset in self._assets:
            self._cleanup_15m_entered_slugs(asset)

        branch_open_count, total_open_count = self._latency_open_counts()
        now = time.time()

        for asset in self._assets:
            await self._tick_asset(asset, branch_open_count, total_open_count, now)

    async def _tick_asset(
        self,
        asset: str,
        branch_open_count: dict[str, int],
        total_open_count: int,
        now: float,
    ) -> None:
        """Process one asset's momentum signal and potentially enter a trade."""
        symbol = f"{asset}/USDT"
        raw_price = self._exchange_feed.get_price(symbol)
        price = raw_price or 0.0
        self._sample_asset_price(asset, raw_price, now)  # always run — fills momentum window

        # Post-restart lockout: refuse all entries until signal history, trend slope,
        # and whipsaw guard have had time to observe current market conditions.
        elapsed_since_start = now - self._startup_ts
        if elapsed_since_start < self.STARTUP_LOCKOUT_SECS:
            remaining = self.STARTUP_LOCKOUT_SECS - elapsed_since_start
            if now - self._last_momentum_log.get(asset, 0) >= 60:
                self._last_momentum_log[asset] = now
                logger.info(
                    f"LatencyArb: post-restart lockout [{asset}] — "
                    f"{remaining:.0f}s remaining before trading allowed"
                )
            return
        fast_momentum = self._exchange_feed.get_momentum(symbol)
        _momentum_delta = fast_momentum - self._prev_momentum.get(asset, fast_momentum)
        self._prev_momentum[asset] = fast_momentum

        if now - self._last_trend_sample_ts[asset] >= self.TREND_SAMPLE_INTERVAL_SECS:
            self._update_trend(asset, price)
            self._last_trend_sample_ts[asset] = now

        momentum_15m = self._momentum_for_timeframe(asset, "15m", fast_momentum)
        required_momentum_5m = self._momentum_threshold_for_current_trend(asset)

        if now - self._last_momentum_log[asset] >= 15:
            ob_imbalance = self._exchange_feed.get_order_book_imbalance(symbol)
            ob_str = f"{ob_imbalance:+.3f}" if ob_imbalance is not None else "None"
            required_momentum_15m = self._15m_required_momentum(asset)
            notable = (
                self._trend_direction[asset] is not None
                or abs(fast_momentum) > required_momentum_5m * 0.5
                or abs(momentum_15m) > required_momentum_15m * 0.5
            )
            slope_display = (
                f"{self._last_slope[asset]:+.4f}%"
                if self._trend_state_label(asset) != "WARMUP"
                else "n/a"
            )
            logger.debug(
                f"LatencyArb tick: {asset}={price:.2f} momentum_5m={fast_momentum:+.4%} "
                f"momentum_15m={momentum_15m:+.4%} "
                f"OB={ob_str} threshold_5m={required_momentum_5m:.3%} "
                f"threshold_15m={required_momentum_15m:.3%} "
                f"trend={self._trend_state_label(asset)} slope_pct={slope_display} "
                f"trend_gate={self._trend_slope_threshold_pct:.2f}% "
                f"open_5m={branch_open_count['5m']} open_15m={branch_open_count['15m']}"
            )
            self._last_momentum_log[asset] = now

        if self._trend_state_label(asset) == "WARMUP":
            self._log_trend_warmup(asset)
            return

        can_trade_15m, reason_15m, cooldown_state_15m = self._branch_trade_availability(
            asset, "15m", branch_open_count, total_open_count
        )
        await self._check_pending_15m(
            asset,
            momentum_15m,
            can_trade=can_trade_15m,
            blocked_reason=reason_15m,
            branch_open_count=branch_open_count["15m"],
            cooldown_state=cooldown_state_15m,
        )

        market_15m = self._get_market_if_fresh(f"{asset}_15m")
        if market_15m is not None:
            await self._queue_15m_pending(
                asset,
                market_15m,
                momentum_15m,
                can_trade=can_trade_15m,
                blocked_reason=reason_15m,
                branch_open_count=branch_open_count["15m"],
                total_open_count=total_open_count,
                cooldown_state=cooldown_state_15m,
                momentum_delta=_momentum_delta,
            )

        # Resolution arb: near-window-end entries on clearly-resolved markets
        await self._resolution_arb_check(asset, can_trade_15m, branch_open_count, total_open_count)

        can_trade_5m, reason_5m, cooldown_state_5m = self._branch_trade_availability(
            asset, "5m", branch_open_count, total_open_count
        )
        # Per-asset 5m momentum multiplier — higher-vol assets need a stricter floor.
        # SOL: 1.8x (moves ~1.7–1.8x BTC per 10s); XRP: 1.4x (moves ~1.4x BTC).
        _5m_mult = {
            "SOL": settings.SOL_5M_MOMENTUM_MULT,
            "XRP": settings.XRP_5M_MOMENTUM_MULT,
        }.get(asset, 1.0)
        required_momentum_5m *= _5m_mult
        if abs(fast_momentum) < required_momentum_5m:
            self._log_rejection(
                asset, "5m",
                "warmup" if self._trend_state_label(asset) == "WARMUP" else "weak_momentum",
                momentum=fast_momentum,
                threshold=required_momentum_5m,
                cooldown_state=cooldown_state_5m,
                branch_open_count=branch_open_count["5m"],
            )
            return

        if not can_trade_5m:
            self._log_rejection(
                asset, "5m",
                reason_5m,
                momentum=fast_momentum,
                threshold=required_momentum_5m,
                cooldown_state=cooldown_state_5m,
                branch_open_count=branch_open_count["5m"],
            )
            return

        market_5m = self._get_market_if_fresh(f"{asset}_5m")
        if market_5m is not None:
            slug_5m = market_5m["slug"]
            cooldown_map = self._cooldown["5m"]
            if (
                time.time() - cooldown_map.get(slug_5m, 0) >= 300
                and slug_5m not in self._traded_this_cycle
                and time.time() >= self._price_skip_until.get(slug_5m, 0)
            ):
                traded = await self._trade_updown(
                    market_5m, asset, fast_momentum,
                    entry_path="5M_DIRECT", momentum_delta=_momentum_delta,
                )
                if traded:
                    self._traded_this_cycle.add(slug_5m)
                    cooldown_map[slug_5m] = time.time()
                else:
                    # Price check failed — throttle retries for 15s to avoid hammering API
                    self._price_skip_until[slug_5m] = time.time() + 15.0
            else:
                remaining = max(0.0, 300 - (time.time() - cooldown_map.get(slug_5m, 0)))
                if remaining > 0:
                    _reason, _cd = "cooldown_active", f"{remaining:.0f}s"
                elif slug_5m in self._traded_this_cycle:
                    _reason, _cd = "already_traded_this_cycle", "inactive"
                else:
                    price_skip_rem = max(0.0, self._price_skip_until.get(slug_5m, 0) - time.time())
                    _reason, _cd = "price_skip_active", f"{price_skip_rem:.0f}s"
                self._log_rejection(
                    asset, "5m",
                    _reason,
                    momentum=fast_momentum,
                    threshold=required_momentum_5m,
                    cooldown_state=_cd,
                    branch_open_count=branch_open_count["5m"],
                )

    def _get_market_if_fresh(self, key: str) -> "dict | None":
        """Return cached market only if it's not too stale to be useful.

        5m markets expire every 5 minutes — allow up to 2 windows (10 min) of stale data.
        15m markets expire every 15 minutes — allow up to 2 windows (30 min).
        Beyond that, the market slug is certainly expired; discard to avoid phantom positions.
        """
        market = self._current_updowns.get(key)
        if market is None:
            return None
        window_secs = 900 if key.endswith("_15m") else 300
        age = time.time() - self._market_data_ts.get(key, 0.0)
        if age > window_secs * 2:
            return None  # stale enough that the market is definitely expired
        return market

    async def _refresh_all_markets(self) -> None:
        now = time.time()
        configs = [
            (asset, window, f"{asset}_{window}m")
            for asset in self._assets
            for window in [5, 15]
        ]
        for asset, window, key in configs:
            if now - self._last_market_fetch[key] > self.MARKET_REFRESH_INTERVAL_SECS:
                market = await self._scanner.get_updown_market_for(asset, window)
                self._last_market_fetch[key] = now
                if market is not None:
                    self._current_updowns[key] = market
                    self._market_data_ts[key] = now
                    # Subscribe new token IDs to the Polymarket feed for real-time prices
                    self._pm_feed.subscribe([
                        market["up_token_id"],
                        market["down_token_id"],
                    ])
                # else: keep stale data — circuit breaker or API hiccup;
                # do NOT overwrite with None so ticks can still attempt trades

        # Warn when all markets are unavailable (circuit breaker likely open)
        if all(v is None for v in self._current_updowns.values()):
            if now - self._all_markets_none_logged_at > 300:  # at most once per 5 min
                self._all_markets_none_logged_at = now
                logger.warning(
                    "LatencyArb: ALL updown markets are None — Gamma API circuit breaker "
                    "may be open. Trading suspended until API recovers."
                )

    async def _trade_updown(
        self,
        updown: dict,
        asset: str,
        momentum: float,
        entry_path: str = "5M_DIRECT",
        ob_at_queue_time: float | None = None,
        momentum_delta: float | None = None,
    ) -> bool:
        """Trade an updown market based on exchange momentum."""
        symbol = f"{asset}/USDT"
        market = updown["market"]
        market_id = market.get("conditionId") or market.get("id", "")
        question = market.get("question", updown["slug"])
        slug = updown["slug"]
        timeframe = self._timeframe_from_slug(slug)
        required_momentum = (
            self._15m_required_momentum(asset)
            if timeframe == "15m"
            else self._momentum_threshold_for_current_trend(asset)
        )

        if not market.get("acceptingOrders", True):
            logger.debug(f"LatencyArb: SKIPPED {slug} — market not accepting orders")
            return False
        if self._portfolio.has_position(market_id):
            logger.debug(f"LatencyArb: SKIPPED {slug} — already have position")
            return False

        # ── Strategy-level circuit breaker ────────────────────────────────────
        # Blocks ALL new entries when 3+ stop-losses have hit in the last 90 min.
        if time.time() < self._circuit_breaker_until:
            _cb_rem = int(self._circuit_breaker_until - time.time())
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=circuit_breaker "
                f"{_cb_rem}s remaining"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────

        # ── Resolution arb fast-path ─────────────────────────────────────────
        # Near-window-end entries: direction from net Binance move, not momentum threshold.
        # Skips all quality filters — certainty comes from time + price convergence.
        if entry_path == "RESOLUTION_ARB":
            direction = "UP" if momentum > 0 else "DOWN"
            _res_token = updown["up_token_id"] if direction == "UP" else updown["down_token_id"]
            if any(p.token_id == _res_token for p in self._portfolio.all_positions()):
                return False
            try:
                _res_mid = await asyncio.get_running_loop().run_in_executor(
                    None, self._client.get_midpoint, _res_token
                )
            except Exception:
                return False
            if _res_mid < settings.LAB_RESOLUTION_MIN_MID or _res_mid > settings.LAB_RESOLUTION_MAX_MID:
                return False
            _res_size = min(settings.LAB_BASE_SIZE_USDC * 0.75, settings.MAX_POSITION_SIZE_USDC)
            _res_market = updown["market"]
            _res_market_id = _res_market.get("conditionId") or _res_market.get("id", "")
            logger.info(
                f"LatencyArb: RESOLUTION ARB [{asset}] {timeframe} direction={direction} "
                f"mid={_res_mid:.3f} size=${_res_size:.2f} net_move={momentum:+.4%}"
            )
            self._execute_signal(
                _res_market_id, _res_token, updown.get("question", slug),
                "BUY", _res_mid, _res_size,
                momentum, f"{symbol} ({direction}) [{slug}]",
                timeframe=timeframe, window_slug=slug, asset=asset,
                entry_path="RESOLUTION_ARB",
            )
            return True
        # ─────────────────────────────────────────────────────────────────────

        # ── Regime gate (vol ratio + liquidation cascade) ─────────────────────
        # Approach 2: realized-vol ratio gate.
        #   elevated (1.5–2.5×): CONFIRMED only blocked — weaker signal, loses money
        #                        in choppy high-vol markets.
        #   high/crash (>2.5×):  all paths blocked — momentum signals are unreliable
        #                        dead-cat-bounce territory.
        # Approach 4: liquidation cascade gate.
        #   When Binance futures show $20M BTC / $3M XRP / $2M SOL liquidated in 5 min,
        #   a cascade is in progress. Every UP signal during a cascade is a bounce in
        #   an active waterfall. Pause for LIQ_CASCADE_PAUSE_SECS (15 min default).
        # RESOLUTION_ARB is always exempt — near-expiry certainty overrides regime.
        _vol_regime   = self._exchange_feed.get_vol_regime(symbol)
        _cascade      = self._liq_feed.is_cascade_active(asset)
        _vol_ratio    = self._exchange_feed.get_vol_ratio(symbol)

        if _cascade or _vol_regime in ("high", "crash"):
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=regime_gate "
                f"regime={_vol_regime} cascade={_cascade} vol_ratio={_vol_ratio:.2f}x "
                f"path={entry_path}"
            )
            return False

        if _vol_regime == "elevated" and entry_path == "CONFIRMED":
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=regime_gate_elevated "
                f"vol_ratio={_vol_ratio:.2f}x path=CONFIRMED"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────

        direction = self._direction_from_momentum(momentum, required_momentum)
        if direction is None:
            self._log_rejection(
                asset, timeframe,
                "weak_momentum",
                momentum=momentum,
                threshold=required_momentum,
                branch_open_count=self._latency_open_counts()[0].get(timeframe, 0),
            )
            return False

        # Skip if too little time remains in the current window.
        # Raised from 90s/60s to 240s/120s: binary token prices are extremely sensitive
        # near expiry — a 35% stop-loss can trigger in under 30s during the final 90s.
        # With 240s (4 min) floor: the position has time to breathe before the token
        # converges aggressively toward 0 or 1.
        min_remaining = 240 if timeframe == "15m" else 120
        window_secs = 900 if timeframe == "15m" else 300
        ts = (int(time.time()) // window_secs) * window_secs
        seconds_remaining = ts + window_secs - time.time()
        if seconds_remaining < min_remaining:
            logger.debug(
                f"LatencyArb: skipping {slug} — only {seconds_remaining:.0f}s remaining in window"
            )
            return False

        token_id = updown["up_token_id"] if direction == "UP" else updown["down_token_id"]

        # Dedup guard — prevent entering the same token twice when 5m and 15m paths
        # fire in the same tick (e.g. at 15m boundaries where both slugs share a window
        # start timestamp and may resolve to the same underlying token)
        if any(p.token_id == token_id for p in self._portfolio.all_positions()):
            logger.debug(
                f"LatencyArb: SKIPPED {slug} — already holding token {token_id[:16]}…"
            )
            return False

        # Prefer real-time ask price from PM WebSocket feed (sub-ms, no HTTP call).
        # We pay the ask when buying — using ask gives an accurate effective entry.
        # Falls back to HTTP get_midpoint if feed data is stale or not yet received.
        mid = self._pm_feed.get_best_ask(token_id)
        if mid is None or not (0 < mid < 1):
            try:
                mid = await asyncio.get_running_loop().run_in_executor(
                    None, self._client.get_midpoint, token_id
                )
            except Exception:
                return False
        else:
            logger.debug(f"PM feed: using live ask={mid:.4f} for {token_id[:16]}… (skipping HTTP)")

        # ── Oracle freshness — seconds since last trade on this binary token ─────
        # A quiet Polymarket token (long trade silence) while Binance is moving
        # means the oracle gap is unexploited: ideal entry timing.
        # "HOT" = actively repricing (<10s), "ACTIVE" = in-progress (10-30s), "FRESH" = untouched (30+s)
        _trade_age = self._pm_feed.get_last_trade_age(token_id)
        _freshness = (
            "FRESH" if (_trade_age is None or _trade_age > 30)
            else "ACTIVE" if _trade_age > 10
            else "HOT"
        )
        # ─────────────────────────────────────────────────────────────────────────

        # ── Oracle lag computation ────────────────────────────────────────────
        # How much of the Binance move has Polymarket already priced?
        # lag_proxy > 0 = Binance still ahead, edge open.
        # lag_proxy < 0 = Polymarket caught up, edge closed.
        # LAB_MIN_ORACLE_LAG = -999 (shadow/log mode) — set > 0 to activate gate.
        _binance_move = None
        _pm_from_neutral = None
        _open_snap = self._window_open_snapshots.get(slug)
        if _open_snap and _open_snap.get("binance_price", 0) > 0:
            _current_binance = self._exchange_feed.get_price(symbol) or 0.0
            if _current_binance > 0:
                _binance_move = (
                    abs(_current_binance - _open_snap["binance_price"])
                    / _open_snap["binance_price"]
                )
                _pm_from_neutral = abs(mid - 0.50)
                _lag_proxy = _binance_move - (_pm_from_neutral * 2.0)
                logger.info(
                    f"Oracle lag [{asset}]: binance_move={_binance_move:.4%} "
                    f"pm_dist={_pm_from_neutral:.3f} lag={_lag_proxy:+.4f} path={entry_path}"
                )
                if _lag_proxy < settings.LAB_MIN_ORACLE_LAG:
                    logger.info(
                        f"LatencyArb reject [{asset}]: {timeframe} reason=oracle_lag_closed "
                        f"lag={_lag_proxy:.4f} < {settings.LAB_MIN_ORACLE_LAG} path={entry_path}"
                    )
                    return False
        # ─────────────────────────────────────────────────────────────────────

        if mid <= 0 or mid >= 1:
            return False
        if mid < 0.20:
            return False
        if mid > self.MAX_ENTRY_MID_PRICE:
            required_win_rate = mid * 100
            logger.debug(
                f"LatencyArb: SKIPPED {slug} — entry price {mid:.3f} exceeds max "
                f"{self.MAX_ENTRY_MID_PRICE} (break-even would require {required_win_rate:.0f}% win rate)"
            )
            return False
        if timeframe == "5m":
            _5m_min, _5m_max = self._mid_price_window(asset, "5m")
            if mid < _5m_min or mid > _5m_max:
                logger.debug(
                    f"LatencyArb: SKIPPED {slug} — 5m mid-price {mid:.3f} outside window "
                    f"[{_5m_min:.3f}, {_5m_max:.3f}]"
                )
                return False
        elif timeframe == "15m":
            _15m_min, _15m_max = self._mid_price_window(asset, "15m")
            if mid < _15m_min or mid > _15m_max:
                logger.debug(
                    f"LatencyArb: SKIPPED {slug} — 15m mid-price {mid:.3f} outside window "
                    f"[{_15m_min:.3f}, {_15m_max:.3f}]"
                )
                return False

        base_size = settings.LAB_BASE_SIZE_USDC

        # Fix 1: Inverted price multiplier — give MORE size to high-probability entries.
        # Data showed 0.45-0.50 zone is the golden zone (59% WR), while 0.40-0.45
        # loses money (38% WR) and was previously getting the LARGEST positions.
        # New formula: mid=0.45 → 0.80x, mid=0.50 → 1.00x (neutral), mid=0.55 → 1.20x,
        # mid=0.60 → 1.40x, capped at 1.50x. Cheap entries (low mid) get reduced size.
        # For 5m: same inversion but tighter ceiling (1.20x) since 5m signals have less time.
        if timeframe == "5m":
            price_mult = max(0.50, min(1.20, mid / 0.50))
        else:
            price_mult = max(0.50, min(1.50, mid / 0.50))

        # Fix 2: Remove momentum multiplier for 5m — strong short-term momentum is noise.
        # Data: momentum_mult=2.0 on 5m produced 48% WR on large-cost trades (-$185 USDC).
        # 15m keeps the multiplier — longer confirmation window earns the extra size.
        abs_momentum = abs(momentum)
        if timeframe == "5m":
            momentum_mult = 1.0  # flat — size is driven entirely by price_mult and OB tier
        else:
            if abs_momentum >= 0.004:
                momentum_mult = 2.0
            elif abs_momentum >= 0.003:
                momentum_mult = 1.5
            else:
                momentum_mult = 1.0

        size_usdc = min(base_size * price_mult * momentum_mult, base_size * 2)

        # Fetch both signals now — used for sizing and ML recording.
        # OBI is NO LONGER a hard entry gate (research: resting book is spoofed on altcoins).
        # CVD (trade-based OFI) is the primary sizing signal.
        imbalance: float | None = self._exchange_feed.get_order_book_imbalance(symbol)
        _cvd: float | None = (
            self._exchange_feed.get_cvd(symbol, settings.LAB_CVD_WINDOW_SECS)
            if settings.LAB_CVD_ENABLED else None
        )

        # Trend filter — skip entries that contradict the current asset trend
        if settings.TREND_FILTER_ENABLED and self._trend_direction[asset] is not None:
            if direction != self._trend_direction[asset]:
                self._log_rejection(
                    asset, timeframe,
                    "trend_blocked",
                    momentum=momentum,
                    threshold=required_momentum,
                    branch_open_count=self._latency_open_counts()[0].get(timeframe, 0),
                    extra=(
                        f" slug={slug} signal={direction} trend={self._trend_direction[asset]} "
                        f"slope={self._last_slope[asset]:+.5f}"
                    ),
                )
                return False

        # ── CVD + OBI combined sizing ─────────────────────────────────────────
        # CVD (trade-based OFI) is primary — more predictive than resting OBI for altcoins.
        # OBI is fallback when CVD buffer hasn't filled yet (first ~10s after startup).
        # Neither is a hard gate: entries are allowed regardless, size is adjusted.
        ob_mult = 1.0
        ob_tier = "NORMAL"
        cvd_str = f"{_cvd:+.4f}" if _cvd is not None else "n/a"

        if _cvd is not None:
            cvd_strong = settings.LAB_CVD_STRONG_THRESHOLD
            if direction == "UP":
                if _cvd >= cvd_strong:
                    ob_mult = settings.LAB_OB_SIZE_STRONG    # strong buy flow confirms UP
                    ob_tier = "CVD_STRONG"
                elif _cvd >= 0:
                    ob_mult = 1.0                             # neutral-positive: normal size
                    ob_tier = "CVD_NEUTRAL"
                else:
                    ob_mult = settings.LAB_OB_SIZE_WEAK      # sell flow present: reduce size
                    ob_tier = "CVD_WEAK"
            else:  # DOWN
                if _cvd <= -cvd_strong:
                    ob_mult = settings.LAB_OB_SIZE_STRONG
                    ob_tier = "CVD_STRONG"
                elif _cvd <= 0:
                    ob_mult = 1.0
                    ob_tier = "CVD_NEUTRAL"
                else:
                    ob_mult = settings.LAB_OB_SIZE_WEAK
                    ob_tier = "CVD_WEAK"
            size_usdc = size_usdc * ob_mult
            logger.info(
                f"LatencyArb CVD sizing ({asset}): cvd={cvd_str} "
                f"tier={ob_tier} mult={ob_mult:.2f}x → size=${size_usdc:.2f} "
                f"(direction={direction} obi={f'{imbalance:+.3f}' if imbalance is not None else 'n/a'})"
            )
        elif settings.LAB_OB_SIZING_ENABLED and imbalance is not None:
            # Fallback: OBI sizing when CVD buffer not yet ready
            abs_imbalance = abs(imbalance)
            if abs_imbalance >= settings.LAB_OB_STRONG_THRESHOLD:
                ob_mult = self._ob_strong_size_mult(asset)
                ob_tier = "OBI_STRONG"
            elif abs_imbalance >= settings.LAB_OB_IMBALANCE_THRESHOLD:
                ob_mult = 1.0
                ob_tier = "OBI_NORMAL"
            else:
                ob_mult = settings.LAB_OB_SIZE_WEAK
                ob_tier = "OBI_WEAK"
            size_usdc = size_usdc * ob_mult
            logger.info(
                f"LatencyArb OBI sizing fallback ({asset}): obi={imbalance:+.3f} "
                f"tier={ob_tier} mult={ob_mult:.2f}x → size=${size_usdc:.2f}"
            )

        # 2.0x momentum multiplier requires CVD_STRONG confirmation.
        # Without confirmed taker flow, the extra size is not earned — cap at 1.5x.
        # T14/T15 both used 2.0x on CVD_WEAK/NEUTRAL and hit max position on losing signals.
        if momentum_mult == 2.0 and ob_tier not in ("CVD_STRONG", "OBI_STRONG"):
            capped_size = min(base_size * price_mult * 1.5 * ob_mult, base_size * 2)
            logger.info(
                f"LatencyArb: momentum_mult 2.0x→1.5x [{asset}]: {ob_tier} not strong, "
                f"size ${size_usdc:.2f}→${capped_size:.2f}"
            )
            size_usdc = capped_size

        # Hard ceiling — never exceed MAX_POSITION_SIZE_USDC regardless of multipliers
        size_usdc = min(size_usdc, settings.MAX_POSITION_SIZE_USDC)

        # Oracle freshness boost removed: bot always enters in the first ~10s of each
        # 15-minute window when the token is brand-new and has zero trades by definition.
        # FRESH was always true — the boost was a permanent unconditional size increase.

        # ── Chainlink oracle confirmation (logging only — data collection phase) ─
        _cl_price = self._rtds_feed.get_chainlink_price(asset.lower())
        _cl_trend = self._rtds_feed.get_chainlink_trend(asset.lower())
        _binance_price = self._exchange_feed.get_price(symbol) or 0.0
        logger.info(
            f"LatencyArb signals [{asset}]: "
            f"binance={_binance_price:.4f} chainlink={f'{_cl_price:.4f}' if _cl_price else 'n/a'} "
            f"cl_trend={_cl_trend or 'n/a'} pm_freshness={_freshness} "
            f"({f'{_trade_age:.0f}s' if _trade_age is not None else 'no_trades'})"
        )
        # ─────────────────────────────────────────────────────────────────────────

        logger.info(
            f"LatencyArb size ({asset}): base=${base_size:.0f} "
            f"price_mult={price_mult:.2f} momentum_mult={momentum_mult} {ob_tier}_mult={ob_mult:.2f}x "
            f"→ size=${size_usdc:.2f} (mid={mid:.3f} momentum={momentum:+.4%} cvd={cvd_str})"
        )

        # ── CVD hard block for FAST_TRACK ─────────────────────────────────────
        # Sizing down to 0.5x still loses money when CVD strongly opposes direction.
        # A price spike against dominant taker flow is a bull/bear trap — block it.
        # Threshold 0.35 = ~67.5% of taker volume is against the signal direction.
        if (entry_path == "FAST_TRACK"
                and ob_tier == "CVD_WEAK"
                and _cvd is not None
                and abs(_cvd) >= settings.LAB_CVD_FASTTRACK_BLOCK_THRESHOLD):
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=cvd_opposes_fasttrack "
                f"cvd={_cvd:.3f} threshold=±{settings.LAB_CVD_FASTTRACK_BLOCK_THRESHOLD} path=FAST_TRACK"
            )
            return False

        # ── CVD neutral zone block for FAST_TRACK ────────────────────────────
        # FAST_TRACK relies on Binance momentum + Polymarket taker flow agreement.
        # When |cvd| < 0.20 there is almost no taker directional conviction —
        # the move has no flow backing. Historical: 17 trades at 35% WR, -$76 net.
        # Cutoff 0.20 is the exact point where WR flips positive (50%+ above it).
        # NOT applied to CONFIRMED — flat CVD is fine there (57% WR historically).
        if (entry_path == "FAST_TRACK"
                and _cvd is not None
                and abs(_cvd) < 0.20):
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=ft_flat_cvd "
                f"cvd={_cvd:.3f} |cvd|<0.20 path=FAST_TRACK"
            )
            return False

        # ── Zero oracle lag block for FAST_TRACK ─────────────────────────────
        # FAST_TRACK fires when Binance moves ≥1.7× threshold. When binance_move=0%
        # AND pm_dist<0.010 simultaneously, there is no Binance move (threshold met
        # by accumulated drift, not a real spike) and the token is already at fair
        # value — no oracle lag to exploit. Historical: 10 such trades, 30% WR,
        # -$113 net. All 3 "wins" were random; all 7 losses were predictable.
        if (entry_path == "FAST_TRACK"
                and _binance_move is not None
                and _binance_move == 0.0
                and _pm_from_neutral is not None
                and _pm_from_neutral < 0.010):
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=ft_zero_lag "
                f"binance_move=0.0% pm_dist={_pm_from_neutral:.3f} path=FAST_TRACK"
            )
            return False

        # ── CVD hard block for CONFIRMED ──────────────────────────────────────
        # CONFIRMED has no Binance momentum guarantee — it relies on a 5-second
        # persistence of a Polymarket drift. When CVD strongly opposes direction
        # (|cvd| > 0.65), taker flow is actively fighting the trade: 8 losses,
        # 1 win observed (16-05-18). Uses a higher threshold than FAST_TRACK (0.35)
        # because CONFIRMED already has a weaker signal and some CVD opposition
        # is normal in choppy markets.
        if (entry_path == "CONFIRMED"
                and ob_tier == "CVD_WEAK"
                and _cvd is not None
                and abs(_cvd) > 0.65):
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=cvd_opposes_confirmed "
                f"cvd={_cvd:.3f} threshold=±0.65 path=CONFIRMED"
            )
            return False

        # ── Zero oracle lag block for CONFIRMED ───────────────────────────────
        # When pm_dist < 0.010 the binary token is essentially at fair value —
        # there is no oracle lag to exploit. CONFIRMED entries here are pure
        # directional bets with no edge. Exception: |cvd| > 0.95 allows entry
        # because extreme taker consensus is a separate signal from oracle lag.
        _pm_dist_now = abs(mid - 0.50)
        if (entry_path == "CONFIRMED"
                and _pm_dist_now < 0.010
                and (_cvd is None or abs(_cvd) <= 0.95)):
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=zero_oracle_lag "
                f"pm_dist={_pm_dist_now:.3f} cvd={f'{_cvd:.3f}' if _cvd is not None else 'n/a'} path=CONFIRMED"
            )
            return False

        # ── SOL UP: require positive CVD ──────────────────────────────────────
        # SOL UP + negative CVD = pump not backed by taker flow → mean-reverts.
        # XRP UP runs at 87% WR; SOL UP at 38%. Filtering to CVD>0 removes non-confirmed pumps.
        if asset == "SOL" and direction == "UP" and _cvd is not None and _cvd <= 0:
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=sol_up_no_cvd "
                f"cvd={_cvd:.3f} <= 0 direction=UP path={entry_path}"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────

        _consec_key = f"{asset}_{timeframe}"
        _consec_losses = self._consecutive_losses.get(_consec_key, 0)
        _consec_wins = self._consecutive_wins.get(_consec_key, 0)

        # ── Consecutive loss gate ─────────────────────────────────────────────
        _gate_until = self._consec_loss_pause_until.get(_consec_key, 0.0)
        if time.time() < _gate_until:
            _gate_remaining = _gate_until - time.time()
            logger.info(
                f"LatencyArb: CONSEC-LOSS GATE BLOCKING [{_consec_key}] — "
                f"{_gate_remaining:.0f}s remaining (losses={_consec_losses}) path={entry_path}"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────
        _secs_since_trend = time.time() - self._trend_change_ts.get(asset, 0)

        # ── Fresh-trend filter ────────────────────────────────────────────────
        # CONFIRMED entries require the trend to be at least LAB_MIN_SECS_TREND_CONFIRMED
        # seconds old. The 60-300s window has WR=33% — trend hasn't established itself.
        # FAST_TRACK is exempt: extreme momentum breakouts are valid on fresh trends.
        if entry_path == "CONFIRMED" and _secs_since_trend < settings.LAB_MIN_SECS_TREND_CONFIRMED:
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=fresh_trend "
                f"secs={_secs_since_trend:.0f}s < {settings.LAB_MIN_SECS_TREND_CONFIRMED}s "
                f"path={entry_path} momentum={momentum:+.4%}"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────

        # ── Stale-FLAT overnight filter ───────────────────────────────────────
        # CONFIRMED entries are blocked when the trend has been unchanged for
        # >= LAB_STALE_FLAT_OVERNIGHT_SECS during overnight hours.
        # SOL: 21:00–06:00 UTC (EU open at 06:00 injects flow)
        # XRP: no overnight block — Asian session (00:00–08:00 UTC) is prime volume
        # FAST_TRACK is exempt — extreme momentum is valid even overnight.
        _utc_hour = int((time.time() % 86400) / 3600)
        if asset == "XRP":
            _is_overnight = False
        else:  # SOL
            _is_overnight = _utc_hour >= 21 or _utc_hour < 6
        if (entry_path == "CONFIRMED"
                and _is_overnight
                and _secs_since_trend >= settings.LAB_STALE_FLAT_OVERNIGHT_SECS):
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=stale_flat_overnight "
                f"secs={_secs_since_trend:.0f}s >= {settings.LAB_STALE_FLAT_OVERNIGHT_SECS}s "
                f"utc_hour={_utc_hour} path={entry_path} momentum={momentum:+.4%}"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────

        # ── Momentum delta direction filter (5M_DIRECT only) ────────────────
        # CONFIRMED path: delta removed — a 5s confirmation period already validates
        # the signal. Sustained momentum has delta≈0 by definition (plateauing, not
        # accelerating), so the delta check was structurally killing all CONFIRMED entries.
        # 5M_DIRECT: keep delta filter (fast, unconfirmed signals need more validation).
        # CVD override: strong taker flow in signal direction bypasses a zero/flat delta.
        if entry_path == "5M_DIRECT" and momentum_delta is not None:
            _delta_aligned = (
                (momentum > 0 and momentum_delta > 0) or
                (momentum < 0 and momentum_delta < 0)
            )
            _cvd_confirms = (
                _cvd is not None and (
                    (momentum > 0 and _cvd >= settings.LAB_CVD_STRONG_THRESHOLD) or
                    (momentum < 0 and _cvd <= -settings.LAB_CVD_STRONG_THRESHOLD)
                )
            )
            if (momentum_delta == 0 or not _delta_aligned) and not _cvd_confirms:
                logger.info(
                    f"LatencyArb reject [{asset}]: {timeframe} reason=delta_not_aligned "
                    f"momentum={momentum:+.4%} delta={momentum_delta:+.4%} "
                    f"cvd={f'{_cvd:.3f}' if _cvd is not None else 'n/a'} path={entry_path}"
                )
                return False
        # ─────────────────────────────────────────────────────────────────────

        # ── FAST_TRACK freshness guards ───────────────────────────────────────
        # Too fresh (<120s): trend just declared after bot warmup — only 8 price
        # samples (80s). Post-restart analysis shows T03/T06/T14/T15 all fired
        # within 14–107s of a brand-new FLAT trend and all lost. The trend label
        # at this age is noise, not signal.
        if entry_path == "FAST_TRACK" and _secs_since_trend < 120:
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=ft_too_fresh "
                f"secs={_secs_since_trend:.0f}s < 120s path=FAST_TRACK"
            )
            return False
        # Danger zone (3–7 min): initial move partially priced, trend going stale.
        # Historical WR: 34% (n=32). Oracle lag reopens after 7min.
        if entry_path == "FAST_TRACK" and 180 < _secs_since_trend < 420:
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=ft_danger_zone "
                f"secs={_secs_since_trend:.0f}s in 3-7min bucket (WR=34%) path=FAST_TRACK"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────

        _prev_trend = self._prev_trend_direction.get(asset)
        # Use _last_logged_trend (stores "FLAT"/"UP"/"DOWN"/"WARMUP") — not _trend_direction
        # which stores None for FLAT, making FLAT indistinguishable from missing data.
        _trend_label = self._last_logged_trend.get(asset)

        # cross_asset_agree: smart two-tier validation.
        # Independent move (only SOL or only XRP moving): peer validates the other.
        #   e.g. SOL spikes while XRP is flat → XRP agreement = genuine crypto move.
        # Correlated move (both SOL and XRP moving same direction simultaneously):
        #   peer validation is useless (they co-validate noise). Require BTC as an
        #   independent third-party check to filter correlated flash pumps.
        _other_threshold = settings.LAB_MOMENTUM_THRESHOLD
        _peer_asset = "XRP" if asset == "SOL" else "SOL"
        _peer_momentum = self._exchange_feed.get_momentum(f"{_peer_asset}/USDT") or 0.0
        _peer_moving_same = (
            self._direction_from_momentum(_peer_momentum, _other_threshold) == direction
        )
        if _peer_moving_same:
            # Both assets moving together — use BTC as independent validator
            _validator = "BTC"
            _validator_momentum = self._exchange_feed.get_momentum("BTC/USDT") or 0.0
        else:
            # Independent move — peer is the validator
            _validator = _peer_asset
            _validator_momentum = _peer_momentum
        _cross_asset_agree = int(
            self._direction_from_momentum(_validator_momentum, _other_threshold) == direction
        )

        # asset_range_15m: normalized price range over the last 15m of signal history
        _now_ts = time.time()
        _prices_15m = [p for ts, p in self._signal_history[asset] if _now_ts - ts <= 900.0 and p > 0]
        _asset_range_15m = (
            (max(_prices_15m) - min(_prices_15m)) / (sum(_prices_15m) / len(_prices_15m))
            if len(_prices_15m) >= 2 else 0.0
        )

        # ── Cross-asset agreement gate ────────────────────────────────────────
        # FAST_TRACK: exempt for independent moves. When both SOL+XRP move together
        # BTC must agree (correlated flash pumps fool peer validation).
        # CONFIRMED: only requires CA when the peer is also moving the same direction.
        #   Independent CONFIRMED moves (SOL up, XRP flat) don't need CA — the 5s
        #   confirmation window already validates the signal on its own.
        # 5M_DIRECT: no CA requirement — short 5m signals rely on delta + CVD only.
        _ft_needs_ca = entry_path == "FAST_TRACK" and _peer_moving_same
        _confirmed_needs_ca = entry_path == "CONFIRMED" and _peer_moving_same
        if (_ft_needs_ca or _confirmed_needs_ca) and _cross_asset_agree == 0:
            logger.info(
                f"LatencyArb reject [{asset}]: {timeframe} reason=ca_not_aligned "
                f"momentum={momentum:+.4%} cross_asset=0 validator={_validator} "
                f"peer_same_dir={_peer_moving_same} path={entry_path}"
            )
            return False
        # ─────────────────────────────────────────────────────────────────────

        # ── Shadow ML scoring ─────────────────────────────────────────────────
        _now = datetime.now()
        _ml_prob = get_ml_model().predict(
            entry_price=mid,
            momentum=momentum,
            ob_imbalance=imbalance,
            trend_slope=self._last_slope.get(asset),
            trend_direction=_trend_label,
            consec_losses=_consec_losses,
            asset=asset,
            timeframe=timeframe,
            hour=_now.hour,
            dow=_now.weekday(),
            momentum_delta=momentum_delta,
            secs_since_trend_change=_secs_since_trend,
            prev_trend_direction=_prev_trend,
            entry_path=entry_path,
            consec_wins=_consec_wins,
            ob_at_queue_time=ob_at_queue_time,
            cross_asset_agree=_cross_asset_agree,
            asset_range_15m=_asset_range_15m,
            cvd_at_entry=_cvd,
            window_slug=slug,
        )
        if _ml_prob is not None:
            logger.info(
                f"MLShadow [{asset}/{timeframe}]: p_win={_ml_prob:.3f} "
                f"mid={mid:.3f} mom={momentum:+.4%} ob={f'{imbalance:+.3f}' if imbalance is not None else 'N/A'} "
                f"cvd={f'{_cvd:+.3f}' if _cvd is not None else 'N/A'} "
                f"trend={_trend_label} cross={_cross_asset_agree} path={entry_path}"
            )
        # ─────────────────────────────────────────────────────────────────────

        self._execute_signal(
            market_id, token_id, question,
            "BUY", mid, size_usdc,
            momentum, f"{symbol} ({direction}) [{updown['slug']}]",
            timeframe=timeframe,
            window_slug=slug,
            asset=asset,
            ob_imbalance=imbalance,
            cvd_at_entry=_cvd,
            trend_slope=self._last_slope.get(asset),
            trend_direction=_trend_label,
            consec_losses=_consec_losses,
            ml_win_prob=_ml_prob,
            momentum_delta=momentum_delta,
            secs_since_trend_change=_secs_since_trend,
            prev_trend_direction=_prev_trend,
            entry_path=entry_path,
            consec_wins=_consec_wins,
            ob_at_queue_time=ob_at_queue_time,
            cross_asset_agree=_cross_asset_agree,
            asset_range_15m=_asset_range_15m,
        )

        return True

    def _cleanup_15m_entered_slugs(self, asset: str) -> None:
        """Discard the previous 15m window's slug from _entered_15m_slugs when window rolls over."""
        key = f"{asset}_15m"
        market_15m = self._current_updowns.get(key)
        if market_15m is None:
            return
        current_slug = market_15m["slug"]
        if current_slug != self._last_15m_slug.get(key, ""):
            old_slug = self._last_15m_slug.get(key, "")
            if old_slug:
                self._entered_15m_slugs.discard(old_slug)
            self._last_15m_slug[key] = current_slug

    async def _resolution_arb_check(
        self,
        asset: str,
        can_trade: bool,
        branch_open_count: dict,
        total_open_count: int,
    ) -> None:
        """
        Near-resolution arb: when a 15m window has < LAB_RESOLUTION_SECS_REMAINING left
        AND Binance has clearly moved since window open, buy the winning token if it's
        still not fully priced by Polymarket.
        """
        if not settings.LAB_RESOLUTION_ARB_ENABLED:
            return
        if not can_trade:
            return

        market_15m = self._get_market_if_fresh(f"{asset}_15m")
        if market_15m is None:
            return

        slug = market_15m["slug"]
        if slug in self._entered_15m_slugs:
            return

        window_secs = 900
        ts = (int(time.time()) // window_secs) * window_secs
        seconds_remaining = ts + window_secs - time.time()

        if seconds_remaining > settings.LAB_RESOLUTION_SECS_REMAINING:
            return
        if seconds_remaining < 30:
            return

        open_snap = self._window_open_snapshots.get(slug)
        if not open_snap or not open_snap.get("binance_price", 0):
            return

        symbol = f"{asset}/USDT"
        current_price = self._exchange_feed.get_price(symbol)
        if not current_price or current_price <= 0:
            return

        net_move = (current_price - open_snap["binance_price"]) / open_snap["binance_price"]
        if abs(net_move) < settings.LAB_RESOLUTION_MIN_BINANCE_MOVE:
            return

        await self._trade_updown(
            market_15m, asset,
            momentum=net_move,
            entry_path="RESOLUTION_ARB",
        )
        self._entered_15m_slugs.add(slug)

    async def _queue_15m_pending(
        self,
        asset: str,
        updown: dict,
        momentum: float,
        can_trade: bool = True,
        blocked_reason: str = "ready",
        branch_open_count: int = 0,
        total_open_count: int = 0,
        cooldown_state: str = "inactive",
        momentum_delta: float | None = None,
    ) -> None:
        """Queue, strengthen, or fast-track a 15m signal."""
        symbol = f"{asset}/USDT"
        slug = updown["slug"]
        required_momentum = self._15m_required_momentum(asset)
        direction = self._direction_from_momentum(momentum, required_momentum)

        if slug in self._entered_15m_slugs:
            return

        window_secs = 900
        ts = (int(time.time()) // window_secs) * window_secs
        seconds_remaining = ts + window_secs - time.time()
        if seconds_remaining < 240:
            return

        if direction is None:
            self._log_rejection(
                asset, "15m",
                "weak_momentum",
                momentum=momentum,
                threshold=required_momentum,
                cooldown_state=cooldown_state,
                branch_open_count=branch_open_count,
            )
            return

        # Capture Binance price the first time this slug is seen (both FAST-TRACK and QUEUED)
        if slug not in self._window_open_snapshots:
            _snap_price = self._exchange_feed.get_price(symbol)
            if _snap_price and _snap_price > 0:
                self._window_open_snapshots[slug] = {
                    "binance_price": _snap_price,
                    "ts": time.time(),
                }

        fasttrack_threshold = required_momentum * settings.LAB_15M_FASTTRACK_MULTIPLIER

        if abs(momentum) >= fasttrack_threshold:
            if not can_trade:
                self._log_rejection(
                    asset, "15m",
                    blocked_reason,
                    momentum=momentum,
                    threshold=required_momentum,
                    cooldown_state=cooldown_state,
                    branch_open_count=branch_open_count,
                )
                return
            multiplier = settings.LAB_15M_FASTTRACK_MULTIPLIER
            _15m_min, _15m_max = self._mid_price_window(asset, "15m")
            _ft_token = updown["up_token_id"] if direction == "UP" else updown["down_token_id"]
            _ft_mid = self._pm_feed.get_best_ask(_ft_token)
            if _ft_mid is None or not (0 < _ft_mid < 1):
                try:
                    _ft_mid = await asyncio.get_running_loop().run_in_executor(
                        None, self._client.get_midpoint, _ft_token
                    )
                except Exception:
                    _ft_mid = 0.5
            if _ft_mid < _15m_min or _ft_mid > _15m_max:
                _now = time.time()
                if _now - self._last_mid_discard_ts.get(slug, 0) >= 30.0:
                    self._last_mid_discard_ts[slug] = _now
                    logger.debug(
                        f"LatencyArb: 15m DISCARDED [{asset}] — mid-price {_ft_mid:.3f} outside edge window "
                        f"[{_15m_min:.3f}, {_15m_max:.3f}]"
                    )
                return
            logger.info(
                f"LatencyArb: 15m FAST-TRACK entry [{asset}] — {slug} direction={direction} "
                f"momentum={momentum:+.4%} (>= {multiplier}x threshold)"
            )
            self._cooldown["15m"][slug] = time.time()
            await self._trade_updown(
                updown, asset, momentum,
                entry_path="FAST_TRACK",
                ob_at_queue_time=self._exchange_feed.get_order_book_imbalance(symbol),
                momentum_delta=momentum_delta,
            )
            self._entered_15m_slugs.add(slug)
            return

        if slug in self._pending_15m:
            existing = self._pending_15m[slug]
            if (
                existing["direction"] == direction
                and abs(momentum) > abs(existing["momentum"])
                and abs(momentum) - abs(existing["momentum"]) >= 0.00005
            ):
                old_momentum = existing["momentum"]
                reset = abs(momentum) >= abs(existing["momentum"]) * 1.5
                existing["momentum"] = momentum
                if reset:
                    existing["triggered_at"] = time.time()
                logger.debug(
                    f"LatencyArb: 15m pending STRENGTHENED [{asset}] — {slug} momentum updated "
                    f"{old_momentum:+.4%} → {momentum:+.4%} "
                    f"{'(timer reset)' if reset else ''}"
                )
                if abs(momentum) >= required_momentum * 2:
                    logger.info(
                        f"LatencyArb: 15m pending approaching fast-track [{asset}] — {slug} "
                        f"momentum={momentum:+.4%} (2x threshold)"
                    )
            return

        # Capture pm_dist at queue time so confirmation can verify the oracle lag
        # hasn't closed during the wait window (Fix 2).
        _queue_token = updown["up_token_id"] if direction == "UP" else updown["down_token_id"]
        _queue_ask = self._pm_feed.get_best_ask(_queue_token)
        _pm_dist_at_queue = abs((_queue_ask - 0.50) if _queue_ask is not None else 0.0)

        self._pending_15m[slug] = {
            "momentum": momentum,
            "direction": direction,
            "triggered_at": time.time(),
            "window_slug": slug,
            "asset": asset,
            "ob_at_queue": self._exchange_feed.get_order_book_imbalance(symbol),
            "momentum_delta": momentum_delta,
            "pm_dist_at_queue": _pm_dist_at_queue,
        }
        self._last_15m_direction[asset] = direction
        self._last_15m_direction_ts[asset] = time.time()
        logger.info(
            f"LatencyArb: 15m pending QUEUED [{asset}] — {slug} "
            f"direction={direction} momentum={momentum:+.4%} "
            f"({seconds_remaining:.0f}s remaining in window)"
        )

    async def _check_pending_15m(
        self,
        asset: str,
        momentum: float,
        can_trade: bool = True,
        blocked_reason: str = "ready",
        branch_open_count: int = 0,
        cooldown_state: str = "inactive",
    ) -> None:
        """Confirm or discard pending 15m entries for this asset on every tick."""
        if not self._pending_15m:
            return

        symbol = f"{asset}/USDT"
        market_15m = self._current_updowns.get(f"{asset}_15m")
        current_slug = market_15m["slug"] if market_15m else None
        now = time.time()
        to_remove: list[str] = []

        # Only process pending entries for this asset (slugs include asset name)
        asset_prefix = asset.lower() + "-"

        for slug, pending in self._pending_15m.items():
            if not slug.startswith(asset_prefix):
                continue  # belongs to a different asset

            if current_slug != slug:
                logger.info(
                    f"LatencyArb: 15m pending expired [{asset}] — window changed "
                    f"({slug} → {current_slug})"
                )
                to_remove.append(slug)
                continue

            elapsed = now - pending["triggered_at"]

            if elapsed >= 60 and slug not in self._holding_logged:
                self._holding_logged.add(slug)
                window_secs = 900
                ts = (int(now) // window_secs) * window_secs
                _remaining = ts + window_secs - now
                logger.info(
                    f"LatencyArb: 15m pending HOLDING [{asset}] — {slug} "
                    f"direction={pending['direction']} momentum={pending['momentum']:+.4%} "
                    f"({elapsed:.0f}s old, {_remaining:.0f}s remaining)"
                )

            # Wait for confirmation period before entering
            if elapsed < settings.LAB_15M_CONFIRM_SECONDS:
                continue

            confirm_threshold = max(
                self._15m_required_momentum(asset),
                self._momentum_threshold_for_current_trend(asset) * settings.LAB_15M_CONFIRM_RETENTION,
            )
            current_direction = self._direction_from_momentum(momentum, confirm_threshold)

            if current_direction is None:
                self._log_15m_confirmation_skip(asset, momentum, confirm_threshold)
                self._log_rejection(
                    asset, "15m",
                    "no_signal",
                    momentum=momentum,
                    threshold=confirm_threshold,
                    cooldown_state=cooldown_state,
                    branch_open_count=branch_open_count,
                )
                to_remove.append(slug)
                continue

            if current_direction != pending["direction"]:
                self._log_rejection(
                    asset, "15m",
                    "confirmation_failed",
                    momentum=momentum,
                    threshold=confirm_threshold,
                    cooldown_state=cooldown_state,
                    branch_open_count=branch_open_count,
                    extra=(
                        f" slug={slug} expected={pending['direction']} current={current_direction}"
                    ),
                )
                to_remove.append(slug)
                continue

            if abs(momentum) < confirm_threshold:
                self._log_rejection(
                    asset, "15m",
                    "weak_momentum",
                    momentum=momentum,
                    threshold=confirm_threshold,
                    cooldown_state=cooldown_state,
                    branch_open_count=branch_open_count,
                    extra=f" slug={slug}",
                )
                to_remove.append(slug)
                continue

            if slug in self._entered_15m_slugs:
                to_remove.append(slug)
                continue

            window_secs = 900
            ts = (int(now) // window_secs) * window_secs
            seconds_remaining = ts + window_secs - now
            if seconds_remaining < 240:
                logger.info(
                    f"LatencyArb: 15m pending discarded [{asset}] — only {seconds_remaining:.0f}s remaining in window"
                )
                to_remove.append(slug)
                continue

            if not can_trade:
                self._log_rejection(
                    asset, "15m",
                    blocked_reason,
                    momentum=momentum,
                    threshold=confirm_threshold,
                    cooldown_state=cooldown_state,
                    branch_open_count=branch_open_count,
                )
                continue

            # ── Oracle lag closure check ───────────────────────────────────────
            # If pm_dist was meaningful at queue time (>= 0.020) but has now
            # dropped below 0.010, the Polymarket token repriced during the
            # confirmation wait — the lag we were waiting to exploit is gone.
            # Skip rather than enter a trade with no oracle lag premise.
            if market_15m is not None:
                _dir_chk = pending["direction"]
                _tok_chk = market_15m["up_token_id"] if _dir_chk == "UP" else market_15m["down_token_id"]
                _ask_now = self._pm_feed.get_best_ask(_tok_chk)
                _pm_dist_queued = pending.get("pm_dist_at_queue", 0.0)
                if _ask_now is not None and _pm_dist_queued >= 0.020:
                    _pm_dist_now = abs(_ask_now - 0.50)
                    if _pm_dist_now < 0.010:
                        logger.info(
                            f"LatencyArb reject [{asset}]: 15m reason=lag_closed_during_wait "
                            f"pm_dist_at_queue={_pm_dist_queued:.3f} pm_dist_now={_pm_dist_now:.3f} "
                            f"path=CONFIRMED"
                        )
                        to_remove.append(slug)
                        continue

            # Final mid-price check (use PM feed ask if available)
            if market_15m is not None:
                _dir = pending["direction"]
                _tok = market_15m["up_token_id"] if _dir == "UP" else market_15m["down_token_id"]
                _mid = self._pm_feed.get_best_ask(_tok)
                if _mid is None or not (0 < _mid < 1):
                    try:
                        _mid = await asyncio.get_running_loop().run_in_executor(
                            None, self._client.get_midpoint, _tok
                        )
                    except Exception:
                        _mid = 0.5
                _15m_min, _15m_max = self._mid_price_window(asset, "15m")
                if _mid < _15m_min or _mid > _15m_max:
                    _now = time.time()
                    if _now - self._last_mid_discard_ts.get(slug, 0) >= 30.0:
                        self._last_mid_discard_ts[slug] = _now
                        logger.debug(
                            f"LatencyArb: 15m DISCARDED [{asset}] — mid-price {_mid:.3f} outside edge window "
                            f"[{_15m_min:.3f}, {_15m_max:.3f}]"
                        )
                    to_remove.append(slug)
                    continue

            logger.info(
                f"LatencyArb: 15m CONFIRMED [{asset}] after {elapsed:.0f}s — entering {slug} "
                f"direction={pending['direction']} momentum={momentum:+.4%}"
            )
            if market_15m is not None:
                self._cooldown["15m"][slug] = time.time()
                await self._trade_updown(
                    market_15m, asset, momentum,
                    entry_path="CONFIRMED",
                    ob_at_queue_time=pending.get("ob_at_queue"),
                    momentum_delta=pending.get("momentum_delta"),
                )
                self._entered_15m_slugs.add(slug)
            to_remove.append(slug)

        for slug in to_remove:
            self._pending_15m.pop(slug, None)
            self._holding_logged.discard(slug)

    def _update_trend(self, asset: str, price: float) -> None:
        """Update rolling trend direction for the given asset using linear regression."""
        if not self._is_valid_price(price):
            self._log_trend_warmup(asset)
            self._trend_direction[asset] = None
            self._last_slope[asset] = 0.0
            return

        self._price_history[asset].append(price)
        required_samples = self._required_trend_samples()
        if len(self._price_history[asset]) < required_samples:
            self._trend_direction[asset] = None
            self._last_slope[asset] = 0.0
            self._log_trend_warmup(asset)
            return

        prices = list(self._price_history[asset])
        n = len(prices)
        x_mean = (n - 1) / 2
        y_mean = sum(prices) / n
        numerator = sum((i - x_mean) * (prices[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator else 0.0
        slope_pct = (slope * n) / y_mean * 100 if y_mean else 0.0
        self._last_slope[asset] = slope_pct

        _slope_threshold = self._trend_slope_threshold_for_asset(asset)
        if slope_pct > _slope_threshold:
            self._trend_direction[asset] = "UP"
            new_trend = "UP"
        elif slope_pct < -_slope_threshold:
            self._trend_direction[asset] = "DOWN"
            new_trend = "DOWN"
        else:
            self._trend_direction[asset] = None
            new_trend = "FLAT"

        if new_trend != self._last_logged_trend[asset]:
            price_range = max(prices) - min(prices)
            logger.info(
                f"Trend state changed [{asset}]: {self._last_logged_trend[asset]} → {new_trend} "
                f"(slope_pct={slope_pct:.4f}% range={price_range:.2f} n_ticks={n})"
            )
            self._prev_trend_direction[asset] = self._last_logged_trend[asset]
            self._trend_change_ts[asset] = time.time()
            self._last_logged_trend[asset] = new_trend

    def _is_flat_trend(self, asset: str) -> bool:
        return self._trend_direction[asset] is None

    def _momentum_threshold_for_current_trend(self, asset: str) -> float:
        threshold = self.BASE_MOMENTUM_THRESHOLD
        if self._is_flat_trend(asset):
            return threshold * self.FLAT_MOMENTUM_MULTIPLIER
        return threshold

    def _15m_required_momentum(self, asset: str) -> float:
        return self._momentum_threshold_for_current_trend(asset) * self.FIFTEEN_MIN_CONFIRMATION_MULTIPLIER

    def _get_ob_floor(self, asset: str = "SOL") -> float:
        """Minimum absolute OB imbalance required to enter, per asset."""
        return {
            "SOL": settings.SOL_OB_MIN_IMBALANCE,
            "XRP": settings.XRP_OB_MIN_IMBALANCE,
        }.get(asset, settings.OB_MIN_IMBALANCE)

    def _ob_strong_size_mult(self, asset: str) -> float:
        """Position size multiplier when OB imbalance is in the STRONG tier."""
        return {
            "SOL": settings.SOL_OB_SIZE_STRONG,
            "XRP": settings.XRP_OB_SIZE_STRONG,
        }.get(asset, settings.LAB_OB_SIZE_STRONG)

    def _mid_price_window(self, asset: str, timeframe: str) -> tuple[float, float]:
        """Returns (min, max) mid-price entry window for the given asset and timeframe."""
        if timeframe == "5m":
            return {
                "SOL": (settings.SOL_5M_MID_PRICE_MIN, settings.SOL_5M_MID_PRICE_MAX),
                "XRP": (settings.XRP_5M_MID_PRICE_MIN, settings.XRP_5M_MID_PRICE_MAX),
            }.get(asset, (settings.LAB_5M_MID_PRICE_MIN, settings.LAB_5M_MID_PRICE_MAX))
        return {
            "SOL": (settings.SOL_15M_MID_PRICE_MIN, settings.SOL_15M_MID_PRICE_MAX),
            "XRP": (settings.XRP_15M_MID_PRICE_MIN, settings.XRP_15M_MID_PRICE_MAX),
        }.get(asset, (settings.LAB_15M_MID_PRICE_MIN, settings.LAB_15M_MID_PRICE_MAX))

    def _trend_slope_threshold_for_asset(self, asset: str) -> float:
        """Per-asset trend slope threshold (scaled to each asset's volatility)."""
        raw = {
            "SOL": settings.SOL_TREND_FILTER_MIN_SLOPE,
            "XRP": settings.XRP_TREND_FILTER_MIN_SLOPE,
        }.get(asset, settings.TREND_FILTER_MIN_SLOPE)
        return self._normalize_trend_slope_threshold_pct(raw)

    def _execute_signal(
        self,
        market_id: str,
        token_id: str,
        question: str,
        side: str,
        price: float,
        size_usdc: float,
        momentum: float,
        symbol: str,
        timeframe: str,
        window_slug: str,
        asset: str = "SOL",
        ob_imbalance: float | None = None,
        cvd_at_entry: float | None = None,
        trend_slope: float | None = None,
        trend_direction: str | None = None,
        consec_losses: int | None = None,
        ml_win_prob: float | None = None,
        momentum_delta: float | None = None,
        secs_since_trend_change: float | None = None,
        prev_trend_direction: str | None = None,
        entry_path: str | None = None,
        consec_wins: int | None = None,
        ob_at_queue_time: float | None = None,
        cross_asset_agree: int | None = None,
        asset_range_15m: float | None = None,
    ) -> None:
        try:
            result = self._orders.place_market_order(
                strategy=self.name,
                market_id=market_id,
                token_id=token_id,
                question=question,
                side=side,
                size_usdc=size_usdc,
                price=price,
                asset=asset,
                momentum_at_entry=momentum,
                ob_imbalance_at_entry=ob_imbalance,
                cvd_at_entry=cvd_at_entry,
                trend_slope_at_entry=trend_slope,
                trend_direction_at_entry=trend_direction,
                consec_losses_at_entry=consec_losses,
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
            if result:
                # Use fee-adjusted position sizing so heartbeat PnL is accurate.
                # Polymarket fee formula: fee = size × feeRate × p × (1-p)
                # At p=0.455 with feeRate=0.10: ~2.5% of collateral (not flat 10%)
                _fee_rate = settings.TAKER_FEE_RATE
                _fee_usdc = size_usdc * _fee_rate * price * (1 - price) if price > 0 else 0.0
                _shares = ((size_usdc - _fee_usdc) / price) if price > 0 else 0
                _eff_entry = (size_usdc / _shares) if _shares > 0 else price
                self._portfolio.add_position(
                    market_id=market_id,
                    token_id=token_id,
                    question=question,
                    strategy=self.name,
                    side=side,
                    size=_shares,
                    entry_price=_eff_entry,
                    metadata_json=json.dumps({
                        "timeframe": timeframe,
                        "window_slug": window_slug,
                        "asset": asset,
                        "direction": "UP" if momentum > 0 else "DOWN",
                        "entry_path": entry_path or "UNKNOWN",
                    }),
                )
                logger.info(
                    f"LatencyArb: {side} {size_usdc:.2f} USDC @ {price:.3f} "
                    f"(eff={_eff_entry:.4f} after {_fee_rate:.1%} fee) "
                    f"on {question[:40]}… {symbol} momentum={momentum:.4f}"
                )
                asyncio.ensure_future(get_alerter().trade_opened(
                    asset=asset,
                    direction="UP" if momentum > 0 else "DOWN",
                    timeframe=timeframe,
                    entry_path=entry_path or "UNKNOWN",
                    size_usdc=size_usdc,
                    mid=price,
                    momentum=momentum,
                    ml_prob=ml_win_prob,
                    dry_run=settings.DRY_RUN,
                ))
        except AssertionError as exc:
            logger.warning(f"LatencyArb order rejected: {exc}")

    def on_stop_loss(self, timeframe: str | None = None, asset: str = "SOL") -> None:
        """Increment consecutive loss counter; engage gate if threshold reached."""
        key = f"{asset}_{timeframe}" if timeframe in ("5m", "15m") else f"{asset}_5m"
        if key not in self._consecutive_losses:
            key = "SOL_5m"
        self._consecutive_losses[key] += 1
        self._consecutive_wins[key] = 0
        losses = self._consecutive_losses[key]
        logger.warning(
            f"LatencyArb: stop-loss exit on {key} "
            f"(consecutive_losses={losses})"
        )
        if losses >= settings.LAB_CONSEC_LOSS_PAUSE:
            self._consec_loss_pause_until[key] = time.time() + settings.LAB_CONSEC_LOSS_PAUSE_SECS
            logger.warning(
                f"LatencyArb: CONSEC-LOSS GATE ON [{key}] — "
                f"{settings.LAB_CONSEC_LOSS_PAUSE_SECS}s pause after {losses} consecutive losses"
            )

        # ── Rolling circuit breaker ───────────────────────────────────────────
        _now = time.time()
        self._recent_stop_loss_ts.append(_now)
        self._recent_stop_loss_ts = [t for t in self._recent_stop_loss_ts if t > _now - 5400]
        if len(self._recent_stop_loss_ts) >= 3 and _now >= self._circuit_breaker_until:
            self._circuit_breaker_until = _now + 2700  # 45-min pause
            logger.warning(
                f"LatencyArb: CIRCUIT BREAKER — "
                f"{len(self._recent_stop_loss_ts)} stop-losses in 90min, "
                f"all entries paused 45min"
            )
            asyncio.ensure_future(get_alerter().send(
                "🚨 Circuit breaker: 3 stop-losses in 90min — entries paused 45min",
                category="circuit_breaker",
                cooldown=3600,
            ))
        # ─────────────────────────────────────────────────────────────────────

    def on_win(self, timeframe: str | None = None, asset: str = "SOL") -> None:
        key = f"{asset}_{timeframe}" if timeframe in ("5m", "15m") else f"{asset}_5m"
        if key not in self._consecutive_losses:
            key = "SOL_5m"
        self._consecutive_losses[key] = 0
        self._consecutive_wins[key] = self._consecutive_wins.get(key, 0) + 1
        if time.time() < self._consec_loss_pause_until.get(key, 0):
            self._consec_loss_pause_until[key] = 0.0
            logger.info(f"LatencyArb: CONSEC-LOSS GATE CLEARED [{key}] — win recorded")
        logger.info(
            f"LatencyArb: win recorded for {key}, "
            f"consecutive_wins={self._consecutive_wins[key]}"
        )

    def on_loss(self, timeframe: str | None = None, asset: str = "SOL") -> None:
        key = f"{asset}_{timeframe}" if timeframe in ("5m", "15m") else f"{asset}_5m"
        if key not in self._consecutive_losses:
            key = "SOL_5m"
        self._consecutive_losses[key] += 1
        self._consecutive_wins[key] = 0
        losses = self._consecutive_losses[key]
        logger.info(
            f"LatencyArb: loss recorded for {key}, consecutive losses = {losses}"
        )
        if losses >= settings.LAB_CONSEC_LOSS_PAUSE:
            self._consec_loss_pause_until[key] = time.time() + settings.LAB_CONSEC_LOSS_PAUSE_SECS
            logger.warning(
                f"LatencyArb: CONSEC-LOSS GATE ON [{key}] — "
                f"{settings.LAB_CONSEC_LOSS_PAUSE_SECS}s pause after {losses} consecutive losses"
            )

    def is_signal_reversed(
        self,
        asset: str,
        direction: str,
        entry_path: str,
        token_id: str,
    ) -> bool:
        """
        Returns True when a CONFIRMED position should exit early because the
        Binance signal that triggered entry has reversed AND the oracle lag
        has closed — meaning both the edge and the momentum premise are gone.

        Conditions (all must hold):
          - entry_path == CONFIRMED (FAST_TRACK has different risk profile)
          - Binance momentum has flipped: opposite direction at ≥ 2× threshold
          - pm_dist < 0.010 (token repriced, no remaining lag to capture)
        """
        if entry_path != "CONFIRMED":
            return False
        symbol = f"{asset}/USDT"
        momentum = self._exchange_feed.get_momentum(symbol)
        if momentum is None:
            return False
        threshold = self._15m_required_momentum(asset)
        ask = self._pm_feed.get_best_ask(token_id)
        pm_dist = abs((ask - 0.50) if ask is not None else 0.50)
        if pm_dist >= 0.010:
            return False  # lag still open — hold
        reversed_up = (direction == "UP" and momentum < -(threshold * 2.0))
        reversed_down = (direction == "DOWN" and momentum > (threshold * 2.0))
        return reversed_up or reversed_down

    # ── Utility helpers ───────────────────────────────────────────────────────

    def _normalize_trend_slope_threshold_pct(self, raw_value: float) -> float:
        raw = abs(float(raw_value))
        if raw <= 0.05:
            return raw * 100
        return raw

    def _timeframe_from_slug(self, slug: str) -> str:
        return "15m" if "15m" in (slug or "") else "5m"

    def _trend_state_label(self, asset: str) -> str:
        if len(self._price_history[asset]) < self._required_trend_samples():
            return "WARMUP"
        return self._trend_direction[asset] or "FLAT"

    def _required_trend_samples(self) -> int:
        return max(2, min(settings.TREND_FILTER_TICKS, self.TREND_MIN_VALID_SAMPLES))

    def _is_valid_price(self, price: float | None) -> bool:
        if price is None:
            return False
        try:
            value = float(price)
        except (TypeError, ValueError):
            return False
        return math.isfinite(value) and value > 0

    def _log_trend_warmup(self, asset: str) -> None:
        now = time.time()
        sample_count = len(self._price_history[asset])
        if (
            sample_count == self._last_warmup_sample_count[asset]
            and now - self._last_warmup_log[asset] < 60
        ):
            return
        self._last_warmup_log[asset] = now
        self._last_warmup_sample_count[asset] = sample_count
        logger.info(
            f"LatencyArb: trend warmup [{asset}] — insufficient valid samples "
            f"({sample_count}/{self._required_trend_samples()})"
        )

    def _sample_asset_price(self, asset: str, price: float | None, now: float) -> None:
        if not self._is_valid_price(price):
            return
        if now - self._last_signal_sample_ts[asset] < self.SIGNAL_SAMPLE_INTERVAL_SECS:
            return
        self._last_signal_sample_ts[asset] = now
        self._signal_history[asset].append((now, float(price)))
        cutoff = now - self.SIGNAL_HISTORY_RETENTION_SECS
        history = self._signal_history[asset]
        while history and history[0][0] < cutoff:
            history.popleft()

    def _window_momentum(self, asset: str, window_secs: float, min_window_secs: float = 0.0) -> float:
        history = self._signal_history[asset]
        if len(history) < 2:
            return 0.0
        newest_ts, newest_price = history[-1]
        if newest_price <= 0:
            return 0.0
        oldest_price = None
        for ts, price in history:
            age = newest_ts - ts
            if age < min_window_secs:
                continue
            if age <= window_secs:
                oldest_price = price
                break
        if not oldest_price:
            return 0.0
        return (newest_price - oldest_price) / oldest_price

    def _momentum_for_timeframe(self, asset: str, timeframe: str, fast_momentum: float) -> float:
        if timeframe == "15m":
            return self._window_momentum(
                asset,
                self.FIFTEEN_MIN_SIGNAL_WINDOW_SECS,
                self.FIFTEEN_MIN_SIGNAL_MIN_WINDOW_SECS,
            )
        return fast_momentum

    def _direction_from_momentum(self, momentum: float, threshold: float) -> str | None:
        if momentum >= threshold:
            return "UP"
        if momentum <= -threshold:
            return "DOWN"
        return None

    def _bucket_metric(self, value: float | None) -> float | None:
        if value is None:
            return None
        return round(float(value), 4)

    def _momentum_bucket(self, value: float | None) -> str:
        if value is None:
            return "n/a"
        if value <= -0.0010:
            return "<=-0.10%"
        if value <= -0.0006:
            return "(-0.10%,-0.06%]"
        if value <= -0.0003:
            return "(-0.06%,-0.03%]"
        if value < 0.0003:
            return "(-0.03%,+0.03%)"
        if value < 0.0006:
            return "[+0.03%,+0.06%)"
        if value < 0.0010:
            return "[+0.06%,+0.10%)"
        return ">=+0.10%"

    def _threshold_bucket(self, value: float | None) -> str:
        if value is None:
            return "n/a"
        return f"{round(float(value) * 100, 3):.3f}%"

    def _load_position_metadata(self, metadata_json: str) -> dict:
        if not metadata_json:
            return {}
        try:
            data = json.loads(metadata_json)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _latency_open_counts(self) -> tuple[dict[str, int], int]:
        branch_counts = {"5m": 0, "15m": 0}
        total_latency = 0
        for pos in self._portfolio.all_positions():
            if pos.strategy != self.name:
                continue
            total_latency += 1
            timeframe = str(self._load_position_metadata(pos.metadata_json).get("timeframe") or "")
            if timeframe in branch_counts:
                branch_counts[timeframe] += 1
        return branch_counts, total_latency

    def _branch_trade_availability(
        self,
        asset: str,
        timeframe: str,
        branch_open_count: dict[str, int],
        total_open_count: int,
    ) -> tuple[bool, str, str]:
        if total_open_count >= self.MAX_CONCURRENT:
            return False, "overall_capacity_full", "inactive"
        if branch_open_count.get(timeframe, 0) >= self._branch_limits[timeframe]:
            return False, "branch_capacity_full", "inactive"
        return True, "ready", "inactive"

    def _log_15m_confirmation_skip(self, asset: str, momentum: float, threshold: float) -> None:
        trend_state = self._trend_state_label(asset)
        momentum_bucket = self._momentum_bucket(momentum)
        threshold_bucket = self._threshold_bucket(threshold)
        signature = (trend_state, momentum_bucket, threshold_bucket)
        now = time.time()
        state = self._weak_15m_log_state[asset]

        if state["signature"] != signature:
            state["signature"] = signature
            state["suppressed"] = 0
            state["last_log"] = now
            logger.debug(
                f"LatencyArb: 15m confirmation skipped [{asset}] — weak momentum "
                f"(momentum={momentum:+.4%} threshold={threshold:.4%})"
            )
            return

        state["suppressed"] = int(state["suppressed"]) + 1
        if now - float(state["last_log"]) < self.REJECTION_SUMMARY_INTERVAL_SECS:
            return

        suppressed = int(state["suppressed"])
        state["suppressed"] = 0
        state["last_log"] = now
        logger.debug(
            f"LatencyArb: 15m confirmation skipped [{asset}] x{suppressed} "
            f"(momentum_band={momentum_bucket} threshold_band={threshold_bucket} "
            f"trend={trend_state})"
        )

    def _log_rejection(
        self,
        asset: str,
        timeframe: str,
        reason: str,
        momentum: float | None = None,
        threshold: float | None = None,
        ob_signal: float | None = None,
        cooldown_state: str = "inactive",
        branch_open_count: int = 0,
        extra: str = "",
    ) -> None:
        trend_state = self._trend_state_label(asset)
        if ob_signal is None:
            ob_signal = self._exchange_feed.get_order_book_imbalance(f"{asset}/USDT")
        momentum_str = f"{momentum:+.4%}" if momentum is not None else "n/a"
        threshold_str = f"{threshold:.4%}" if threshold is not None else "n/a"
        ob_str = f"{ob_signal:+.3f}" if ob_signal is not None else "None"
        momentum_bucket = self._momentum_bucket(momentum)
        threshold_bucket = self._threshold_bucket(threshold)
        signature = (
            reason,
            trend_state,
            branch_open_count,
            momentum_bucket,
            threshold_bucket,
        )
        state_key = f"{asset}_{timeframe}"
        state = self._rejection_log_state.get(
            state_key,
            {"signature": None, "suppressed": 0, "last_log": 0.0},
        )
        now = time.time()

        # Noisy background reasons stay at DEBUG — they fire hundreds of times per hour
        # and carry no actionable signal. Interesting reasons (OB mismatch, trend block,
        # cooldown, warmup) stay at INFO so they're visible after a CONFIRMED fires.
        _noisy_reasons = {"weak_momentum", "quiet_hours", "no_signal", "warmup", "price_skip_active"}
        _log = logger.debug if reason in _noisy_reasons else logger.info

        if state["signature"] != signature:
            state["signature"] = signature
            state["suppressed"] = 0
            state["last_log"] = now
            self._rejection_log_state[state_key] = state
            _log(
                f"LatencyArb reject [{asset}]: timeframe={timeframe} reason={reason} "
                f"momentum={momentum_str} threshold={threshold_str} "
                f"ob={ob_str} trend={trend_state} "
                f"cooldown={cooldown_state} branch_open_count={branch_open_count}{extra}"
            )
            return

        state["suppressed"] = int(state["suppressed"]) + 1
        if now - float(state["last_log"]) < self.REJECTION_SUMMARY_INTERVAL_SECS:
            return

        suppressed = int(state["suppressed"])
        state["suppressed"] = 0
        state["last_log"] = now
        _log(
            f"LatencyArb reject [{asset}]: timeframe={timeframe} reason={reason} repeated={suppressed} "
            f"momentum_band={momentum_bucket} threshold_band={threshold_bucket} "
            f"trend={trend_state} cooldown={cooldown_state} "
            f"branch_open_count={branch_open_count}{extra}"
        )
