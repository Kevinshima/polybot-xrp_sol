"""Strategy 1: Price-lag arbitrage vs. crypto exchanges.

Supports multiple assets (BTC, ETH). Each asset uses its own Binance feed,
order-book stream, trend filter, and momentum signal. They share the same
portfolio capacity limits (MAX_CONCURRENT) and database.
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
from data.market_scanner import get_scanner
from strategies.base import BaseStrategy
from utils.logger import logger


class LatencyArb(BaseStrategy):
    """
    Compares Binance spot momentum against Polymarket updown-5m and updown-15m
    markets for BTC (and optionally ETH). Buys the UP token on positive momentum
    and the DOWN token on negative momentum when divergence exceeds threshold.

    Runs every 500ms.
    """

    name = "latency_arb"

    CONTEXT_GATE_WINDOW_SECS = 60
    TREND_SLOPE_THRESHOLD_PCT = 0.75
    MAX_ENTRY_MID_PRICE = 0.65
    BASE_MOMENTUM_THRESHOLD = settings.LAB_MOMENTUM_THRESHOLD
    FIFTEEN_MIN_CONFIRMATION_MULTIPLIER = 1.25
    FLAT_MOMENTUM_MULTIPLIER = 1.0
    FLAT_OB_IMBALANCE_MULTIPLIER = 1.25
    REJECTION_SUMMARY_INTERVAL_SECS = 12.0
    WHIPSAW_WINDOW_SECS = 1200  # 20-minute lookback for trend flip counting
    WHIPSAW_MAX_FLIPS = 2       # block entries if trend reversed >= this many times
    FIFTEEN_M_FLIP_WINDOW_SECS = 1200  # 20-minute lookback for 15m trade direction flips
    FIFTEEN_M_MAX_FLIPS = 2            # block 15m entries after this many UP↔DOWN alternations
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
        self._scanner = get_scanner()
        self.MAX_CONCURRENT = settings.LAB_MAX_CONCURRENT_POSITIONS
        self._branch_limits = {
            "5m": settings.LAB_MAX_CONCURRENT_POSITIONS_5M,
            "15m": settings.LAB_MAX_CONCURRENT_POSITIONS_15M,
        }
        self._trend_slope_threshold_pct = self._normalize_trend_slope_threshold_pct(
            settings.TREND_FILTER_MIN_SLOPE
        )

        # Active assets: always BTC, optionally ETH
        self._assets: list[str] = ["BTC"] + (["ETH"] if settings.ETH_LAB_ENABLED else [])

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

        # ── Per-asset loss cooldowns (keyed: "{ASSET}_{timeframe}") ───────────
        self._consecutive_losses: dict[str, int] = {
            f"{a}_{tf}": 0 for a in self._assets for tf in ["5m", "15m"]
        }
        self._loss_cooldown_until: dict[str, float] = {
            f"{a}_{tf}": 0.0 for a in self._assets for tf in ["5m", "15m"]
        }
        self._last_cooldown_log: dict[str, float] = {
            f"{a}_{tf}": 0.0 for a in self._assets for tf in ["5m", "15m"]
        }

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
        self._holding_logged: set[str] = set()
        self._last_mid_discard_ts: dict[str, float] = {}

        # ── Anti-correlation gate: prevent simultaneous BTC+ETH 5m entries ──
        self._last_5m_entry_ts: float = 0.0
        self._5m_anti_corr_window: float = 30.0  # seconds

        # ── Per-asset 15m momentum cache (used by 5m adverse-context gate) ──
        self._current_momentum_15m: dict[str, float] = {a: 0.0 for a in self._assets}

        # ── Per-asset whipsaw guard ──────────────────────────────────────────
        # Records timestamps of UP↔DOWN trend reversals (ignores FLAT transitions)
        self._trend_flip_ts: dict[str, deque] = {a: deque() for a in self._assets}
        self._last_directional_trend: dict[str, str | None] = {a: None for a in self._assets}

        # ── Per-asset 15m trade flip counter ────────────────────────────────
        # Tracks actual 15m trade direction alternations (UP→DOWN→UP = 2 flips).
        # Separate from the Binance-slope whipsaw guard — this one watches the bot's
        # own 15m trade decisions, catching the April-20 pattern where 15m signals
        # kept alternating direction every 15-30 min while Binance slope stayed FLAT.
        self._15m_trade_flip_ts: dict[str, deque] = {a: deque() for a in self._assets}
        self._last_15m_trade_direction: dict[str, str | None] = {a: None for a in self._assets}

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

        if now - self._last_trend_sample_ts[asset] >= self.TREND_SAMPLE_INTERVAL_SECS:
            self._update_trend(asset, price)
            self._last_trend_sample_ts[asset] = now

        momentum_15m = self._momentum_for_timeframe(asset, "15m", fast_momentum)
        self._current_momentum_15m[asset] = momentum_15m
        required_momentum_5m = self._momentum_threshold_for_current_trend(asset)

        if now - self._last_momentum_log[asset] >= 60:
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
            _log = logger.info if notable else logger.debug
            _log(
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
            )

        can_trade_5m, reason_5m, cooldown_state_5m = self._branch_trade_availability(
            asset, "5m", branch_open_count, total_open_count
        )
        # BTC 5m gets a stricter momentum floor to filter out noise entries
        if asset == "BTC":
            required_momentum_5m *= settings.BTC_5M_MOMENTUM_MULT
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
            ):
                self._traded_this_cycle.add(slug_5m)
                cooldown_map[slug_5m] = time.time()
                await self._trade_updown(market_5m, asset, fast_momentum)
            else:
                remaining = max(0.0, 300 - (time.time() - cooldown_map.get(slug_5m, 0)))
                self._log_rejection(
                    asset, "5m",
                    "cooldown_active",
                    momentum=fast_momentum,
                    threshold=required_momentum_5m,
                    cooldown_state=f"{remaining:.0f}s",
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

    async def _trade_updown(self, updown: dict, asset: str, momentum: float) -> None:
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
            return
        if self._portfolio.has_position(market_id):
            logger.debug(f"LatencyArb: SKIPPED {slug} — already have position")
            return
        direction = self._direction_from_momentum(momentum, required_momentum)
        if direction is None:
            self._log_rejection(
                asset, timeframe,
                "weak_momentum",
                momentum=momentum,
                threshold=required_momentum,
                branch_open_count=self._latency_open_counts()[0].get(timeframe, 0),
            )
            return

        # Skip if too little time remains in the current window
        min_remaining = 90 if timeframe == "15m" else 60
        window_secs = 900 if timeframe == "15m" else 300
        ts = (int(time.time()) // window_secs) * window_secs
        seconds_remaining = ts + window_secs - time.time()
        if seconds_remaining < min_remaining:
            logger.info(
                f"LatencyArb: skipping {slug} — only {seconds_remaining:.0f}s remaining in window"
            )
            return

        token_id = updown["up_token_id"] if direction == "UP" else updown["down_token_id"]

        # Dedup guard — prevent entering the same token twice when 5m and 15m paths
        # fire in the same tick (e.g. at 15m boundaries where both slugs share a window
        # start timestamp and may resolve to the same underlying token)
        if any(p.token_id == token_id for p in self._portfolio.all_positions()):
            logger.debug(
                f"LatencyArb: SKIPPED {slug} — already holding token {token_id[:16]}…"
            )
            return

        try:
            mid = await asyncio.get_running_loop().run_in_executor(
                None, self._client.get_midpoint, token_id
            )
        except Exception:
            return

        if mid <= 0 or mid >= 1:
            return
        if mid < 0.20:
            return
        if mid > self.MAX_ENTRY_MID_PRICE:
            required_win_rate = mid * 100
            logger.info(
                f"LatencyArb: SKIPPED {slug} — entry price {mid:.3f} exceeds max "
                f"{self.MAX_ENTRY_MID_PRICE} (break-even would require {required_win_rate:.0f}% win rate)"
            )
            return
        if timeframe == "5m":
            _5m_min = settings.ETH_5M_MID_PRICE_MIN if asset == "ETH" else settings.LAB_5M_MID_PRICE_MIN
            _5m_max = settings.ETH_5M_MID_PRICE_MAX if asset == "ETH" else settings.LAB_5M_MID_PRICE_MAX
            if mid < _5m_min or mid > _5m_max:
                logger.info(
                    f"LatencyArb: SKIPPED {slug} — 5m mid-price {mid:.3f} outside window "
                    f"[{_5m_min:.3f}, {_5m_max:.3f}]"
                )
                return

        # Anti-correlation gate: if another 5m position opened in the last 30s, skip
        if timeframe == "5m":
            since_last_5m = time.time() - self._last_5m_entry_ts
            if since_last_5m < self._5m_anti_corr_window:
                logger.info(
                    f"LatencyArb: SKIPPED {slug} — anti-correlation gate "
                    f"(another 5m opened {since_last_5m:.0f}s ago)"
                )
                return

        # Adverse 15m context gate: block 5m entries where the 15m momentum
        # is working against the trade direction (UP into downtrend, DOWN into uptrend)
        if timeframe == "5m":
            mom_15m = self._current_momentum_15m.get(asset, 0.0)
            adverse_threshold = 0.0005  # 0.05%
            if direction == "UP" and mom_15m < -adverse_threshold:
                logger.info(
                    f"LatencyArb: SKIPPED {slug} — adverse 15m context "
                    f"(5m=UP but momentum_15m={mom_15m:+.4%})"
                )
                return
            if direction == "DOWN" and mom_15m > adverse_threshold:
                logger.info(
                    f"LatencyArb: SKIPPED {slug} — adverse 15m context "
                    f"(5m=DOWN but momentum_15m={mom_15m:+.4%})"
                )
                return

        # Whipsaw guard — skip all entries when trend has reversed direction too many times
        # recently. This catches volatile BTC/ETH periods where signals have no edge.
        flip_count = self._count_recent_flips(asset)
        if flip_count >= self.WHIPSAW_MAX_FLIPS:
            logger.info(
                f"LatencyArb: SKIPPED {slug} — whipsaw guard "
                f"({flip_count} trend reversals in last {self.WHIPSAW_WINDOW_SECS // 60}min)"
            )
            return

        # 15m flip counter gate — block 15m entries when the bot's own 15m trade
        # direction has alternated too many times in the recent window. This specifically
        # catches the April-20 pattern where 15m momentum flipped UP↔DOWN every 15-30 min
        # while the Binance slope stayed FLAT (so the whipsaw guard above didn't fire).
        if timeframe == "15m":
            flip_count_15m = self._count_recent_15m_flips(asset)
            if flip_count_15m >= self.FIFTEEN_M_MAX_FLIPS:
                logger.info(
                    f"LatencyArb: SKIPPED {slug} — 15m flip guard "
                    f"({flip_count_15m} trade direction flips in last "
                    f"{self.FIFTEEN_M_FLIP_WINDOW_SECS // 60}min)"
                )
                return

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

        # Track imbalance at None so it's always defined at the call site below
        imbalance: float | None = None

        # Order book imbalance filter — use the correct asset's OB
        if settings.LAB_OB_IMBALANCE_ENABLED:
            imbalance = self._exchange_feed.get_order_book_imbalance(symbol)
            if imbalance is not None:
                directional_ob_threshold = settings.LAB_OB_IMBALANCE_THRESHOLD
                ob_floor = self._get_ob_floor(asset)
                if self._is_flat_trend(asset):
                    directional_ob_threshold *= self.FLAT_OB_IMBALANCE_MULTIPLIER
                    ob_floor = max(ob_floor, directional_ob_threshold)
                if abs(imbalance) < ob_floor:
                    evening = datetime.now().hour >= settings.EVENING_HOURS_START
                    label = " [evening floor]" if evening else ""
                    if self._is_flat_trend(asset):
                        label += " [flat filter]"
                    self._log_rejection(
                        asset, timeframe,
                        "weak_ob",
                        momentum=momentum,
                        threshold=required_momentum,
                        ob_signal=imbalance,
                        branch_open_count=self._latency_open_counts()[0].get(timeframe, 0),
                        extra=f" slug={slug} needed>={ob_floor:.3f}{label}",
                    )
                    return
                if direction == "UP" and imbalance < directional_ob_threshold:
                    self._log_rejection(
                        asset, timeframe,
                        "weak_ob",
                        momentum=momentum,
                        threshold=required_momentum,
                        ob_signal=imbalance,
                        branch_open_count=self._latency_open_counts()[0].get(timeframe, 0),
                        extra=f" slug={slug} needed>={directional_ob_threshold:.3f} direction=UP",
                    )
                    return
                if direction == "DOWN" and imbalance > -directional_ob_threshold:
                    self._log_rejection(
                        asset, timeframe,
                        "weak_ob",
                        momentum=momentum,
                        threshold=required_momentum,
                        ob_signal=imbalance,
                        branch_open_count=self._latency_open_counts()[0].get(timeframe, 0),
                        extra=f" slug={slug} needed<={-directional_ob_threshold:.3f} direction=DOWN",
                    )
                    return

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
                return

        # Order book sizing
        ob_mult = 1.0
        ob_tier = "NORMAL"
        if settings.LAB_OB_SIZING_ENABLED:
            imbalance = self._exchange_feed.get_order_book_imbalance(symbol)
            if imbalance is not None:
                abs_imbalance = abs(imbalance)
                if abs_imbalance >= settings.LAB_OB_STRONG_THRESHOLD:
                    ob_mult = settings.ETH_OB_SIZE_STRONG if asset == "ETH" else settings.LAB_OB_SIZE_STRONG
                    ob_tier = "STRONG"
                elif abs_imbalance >= settings.LAB_OB_IMBALANCE_THRESHOLD:
                    ob_mult = 1.0
                    ob_tier = "NORMAL"
                else:
                    ob_mult = settings.LAB_OB_SIZE_WEAK
                    ob_tier = "WEAK"
                size_usdc = size_usdc * ob_mult
                logger.info(
                    f"LatencyArb OB sizing ({asset}): imbalance={imbalance:+.3f} "
                    f"tier={ob_tier} mult={ob_mult:.2f}x → size=${size_usdc:.2f}"
                )

        # Hard ceiling — never exceed MAX_POSITION_SIZE_USDC regardless of multipliers
        size_usdc = min(size_usdc, settings.MAX_POSITION_SIZE_USDC)

        logger.info(
            f"LatencyArb size ({asset}): base=${base_size:.0f} "
            f"price_mult={price_mult:.2f} momentum_mult={momentum_mult} ob_mult={ob_mult:.2f}x "
            f"→ size=${size_usdc:.2f} (mid={mid:.3f} momentum={momentum:+.4%})"
        )

        # Record 5m entry timestamp for anti-correlation gate
        if timeframe == "5m":
            self._last_5m_entry_ts = time.time()

        # Record 15m trade direction for flip counter
        if timeframe == "15m":
            prev_15m_dir = self._last_15m_trade_direction[asset]
            if prev_15m_dir is not None and prev_15m_dir != direction:
                self._15m_trade_flip_ts[asset].append(time.time())
                logger.info(
                    f"15m flip guard [{asset}]: trade direction {prev_15m_dir}→{direction} "
                    f"(flips in last {self.FIFTEEN_M_FLIP_WINDOW_SECS // 60}min: "
                    f"{self._count_recent_15m_flips(asset)})"
                )
            self._last_15m_trade_direction[asset] = direction
            # Prune stale flip timestamps
            cutoff = time.time() - self.FIFTEEN_M_FLIP_WINDOW_SECS
            while self._15m_trade_flip_ts[asset] and self._15m_trade_flip_ts[asset][0] < cutoff:
                self._15m_trade_flip_ts[asset].popleft()

        _consec_key = f"{asset}_{timeframe}"
        _consec_losses = self._consecutive_losses.get(_consec_key, 0)

        # ── Shadow ML scoring ─────────────────────────────────────────────────
        # Score every signal that passes all rule-based filters.
        # Phase 1: log only — does NOT affect trading decisions.
        _now = datetime.now()
        _ml_prob = get_ml_model().predict(
            entry_price=mid,
            momentum=momentum,
            ob_imbalance=imbalance,
            trend_slope=self._last_slope.get(asset),
            trend_direction=self._trend_direction.get(asset),
            consec_losses=_consec_losses,
            asset=asset,
            timeframe=timeframe,
            hour=_now.hour,
            dow=_now.weekday(),
        )
        if _ml_prob is not None:
            logger.info(
                f"MLShadow [{asset}/{timeframe}]: p_win={_ml_prob:.3f} "
                f"mid={mid:.3f} mom={momentum:+.4%} ob={imbalance:+.3f if imbalance is not None else 'N/A'} "
                f"trend={self._trend_direction.get(asset)} consec={_consec_losses}"
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
            trend_slope=self._last_slope.get(asset),
            trend_direction=self._trend_direction.get(asset),
            consec_losses=_consec_losses,
            ml_win_prob=_ml_prob,
        )

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
        if seconds_remaining < 90:
            return

        if self._is_15m_quiet_hours():
            self._log_rejection(
                asset, "15m",
                "quiet_hours",
                momentum=momentum,
                threshold=required_momentum,
                cooldown_state=cooldown_state,
                branch_open_count=branch_open_count,
            )
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

        # Pre-check mid-price before queuing
        _pre_token = updown["up_token_id"] if direction == "UP" else updown["down_token_id"]
        try:
            _pre_mid = await asyncio.get_running_loop().run_in_executor(
                None, self._client.get_midpoint, _pre_token
            )
        except Exception:
            _pre_mid = 0.5
        _15m_min = settings.ETH_15M_MID_PRICE_MIN if asset == "ETH" else settings.LAB_15M_MID_PRICE_MIN
        _15m_max = settings.ETH_15M_MID_PRICE_MAX if asset == "ETH" else settings.LAB_15M_MID_PRICE_MAX
        if _pre_mid < _15m_min or _pre_mid > _15m_max:
            _now = time.time()
            if _now - self._last_mid_discard_ts.get(slug, 0) >= 3.0:
                self._last_mid_discard_ts[slug] = _now
                logger.debug(
                    f"LatencyArb: 15m SKIPPED (pre-queue) [{asset}] — mid-price {_pre_mid:.3f} already outside "
                    f"edge window [{_15m_min:.3f}, {_15m_max:.3f}]"
                )
            return

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
            _ft_token = updown["up_token_id"] if direction == "UP" else updown["down_token_id"]
            try:
                _ft_mid = await asyncio.get_running_loop().run_in_executor(
                    None, self._client.get_midpoint, _ft_token
                )
            except Exception:
                _ft_mid = 0.5
            if _ft_mid < _15m_min or _ft_mid > _15m_max:
                _now = time.time()
                if _now - self._last_mid_discard_ts.get(slug, 0) >= 3.0:
                    self._last_mid_discard_ts[slug] = _now
                    logger.info(
                        f"LatencyArb: 15m DISCARDED [{asset}] — mid-price {_ft_mid:.3f} outside edge window "
                        f"[{_15m_min:.3f}, {_15m_max:.3f}]"
                    )
                return
            logger.info(
                f"LatencyArb: 15m FAST-TRACK entry [{asset}] — {slug} direction={direction} "
                f"momentum={momentum:+.4%} (>= {multiplier}x threshold)"
            )
            self._cooldown["15m"][slug] = time.time()
            await self._trade_updown(updown, asset, momentum)
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

        self._pending_15m[slug] = {
            "momentum": momentum,
            "direction": direction,
            "triggered_at": time.time(),
            "window_slug": slug,
            "asset": asset,
            "ob_at_queue": self._exchange_feed.get_order_book_imbalance(symbol),
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

        if self._is_15m_quiet_hours():
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
            if seconds_remaining < 90:
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

            # Final mid-price check
            if market_15m is not None:
                _dir = pending["direction"]
                _tok = market_15m["up_token_id"] if _dir == "UP" else market_15m["down_token_id"]
                try:
                    _mid = await asyncio.get_running_loop().run_in_executor(
                        None, self._client.get_midpoint, _tok
                    )
                except Exception:
                    _mid = 0.5
                _15m_min = settings.ETH_15M_MID_PRICE_MIN if asset == "ETH" else settings.LAB_15M_MID_PRICE_MIN
                _15m_max = settings.ETH_15M_MID_PRICE_MAX if asset == "ETH" else settings.LAB_15M_MID_PRICE_MAX
                if _mid < _15m_min or _mid > _15m_max:
                    _now = time.time()
                    if _now - self._last_mid_discard_ts.get(slug, 0) >= 3.0:
                        self._last_mid_discard_ts[slug] = _now
                        logger.info(
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
                await self._trade_updown(market_15m, asset, momentum)
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

        if slope_pct > self._trend_slope_threshold_pct:
            self._trend_direction[asset] = "UP"
            new_trend = "UP"
        elif slope_pct < -self._trend_slope_threshold_pct:
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
            self._last_logged_trend[asset] = new_trend

        # Whipsaw guard: record when direction genuinely reverses (UP↔DOWN, ignoring FLAT)
        if new_trend in ("UP", "DOWN"):
            prev_dir = self._last_directional_trend[asset]
            if prev_dir is not None and prev_dir != new_trend:
                self._trend_flip_ts[asset].append(time.time())
                logger.info(
                    f"Whipsaw guard [{asset}]: direction flip {prev_dir}→{new_trend} recorded "
                    f"(total flips in last {self.WHIPSAW_WINDOW_SECS//60}min: "
                    f"{self._count_recent_flips(asset)})"
                )
            self._last_directional_trend[asset] = new_trend
        # Prune stale flip timestamps
        cutoff = time.time() - self.WHIPSAW_WINDOW_SECS
        while self._trend_flip_ts[asset] and self._trend_flip_ts[asset][0] < cutoff:
            self._trend_flip_ts[asset].popleft()

    def _count_recent_flips(self, asset: str) -> int:
        """Count UP↔DOWN direction reversals in the last WHIPSAW_WINDOW_SECS."""
        cutoff = time.time() - self.WHIPSAW_WINDOW_SECS
        return sum(1 for ts in self._trend_flip_ts[asset] if ts >= cutoff)

    def _count_recent_15m_flips(self, asset: str) -> int:
        """Count 15m trade direction alternations (UP↔DOWN) in the last FIFTEEN_M_FLIP_WINDOW_SECS."""
        cutoff = time.time() - self.FIFTEEN_M_FLIP_WINDOW_SECS
        return sum(1 for ts in self._15m_trade_flip_ts[asset] if ts >= cutoff)

    def _is_15m_quiet_hours(self) -> bool:
        """Return True during 22:00-07:00 local time — 15m trades are blocked."""
        h = datetime.now().hour
        return h >= 22 or h < 7

    def _is_flat_trend(self, asset: str) -> bool:
        return self._trend_direction[asset] is None

    def _momentum_threshold_for_current_trend(self, asset: str) -> float:
        threshold = self.BASE_MOMENTUM_THRESHOLD
        if self._is_flat_trend(asset):
            return threshold * self.FLAT_MOMENTUM_MULTIPLIER
        return threshold

    def _15m_required_momentum(self, asset: str) -> float:
        return self._momentum_threshold_for_current_trend(asset) * self.FIFTEEN_MIN_CONFIRMATION_MULTIPLIER

    def _get_ob_floor(self, asset: str = "BTC") -> float:
        evening = datetime.now().hour >= settings.EVENING_HOURS_START
        base_floor = settings.EVENING_OB_MIN_IMBALANCE if evening else settings.OB_MIN_IMBALANCE
        if asset == "ETH":
            return max(base_floor, settings.ETH_OB_MIN_IMBALANCE)
        return base_floor

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
        asset: str = "BTC",
        ob_imbalance: float | None = None,
        trend_slope: float | None = None,
        trend_direction: str | None = None,
        consec_losses: int | None = None,
        ml_win_prob: float | None = None,
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
                trend_slope_at_entry=trend_slope,
                trend_direction_at_entry=trend_direction,
                consec_losses_at_entry=consec_losses,
                timeframe=timeframe,
                ml_win_prob=ml_win_prob,
            )
            if result:
                self._portfolio.add_position(
                    market_id=market_id,
                    token_id=token_id,
                    question=question,
                    strategy=self.name,
                    side=side,
                    size=size_usdc / price if price else 0,
                    entry_price=price,
                    metadata_json=json.dumps({
                        "timeframe": timeframe,
                        "window_slug": window_slug,
                        "asset": asset,
                    }),
                )
                logger.info(
                    f"LatencyArb: {side} {size_usdc:.2f} USDC @ {price:.3f} "
                    f"on {question[:40]}… {symbol} momentum={momentum:.4f}"
                )
        except AssertionError as exc:
            logger.warning(f"LatencyArb order rejected: {exc}")

    def on_stop_loss(self, timeframe: str | None = None, asset: str = "BTC") -> None:
        """Apply a short mandatory cooldown immediately after a stop-loss exit.

        This fires regardless of the consecutive-loss count — a single stop-loss
        is enough to warrant a 120-second pause for that asset+timeframe.
        The consecutive-loss counter is also incremented so 3 stop-losses in a
        row still trigger the full 600s pause.
        """
        key = f"{asset}_{timeframe}" if timeframe in ("5m", "15m") else f"{asset}_5m"
        if key not in self._consecutive_losses:
            key = "BTC_5m"
        STOP_LOSS_COOLDOWN_SECS = 120
        current_until = self._loss_cooldown_until.get(key, 0.0)
        new_until = time.time() + STOP_LOSS_COOLDOWN_SECS
        if new_until > current_until:
            self._loss_cooldown_until[key] = new_until
        self._consecutive_losses[key] += 1
        logger.warning(
            f"LatencyArb: stop-loss exit on {key} — applying {STOP_LOSS_COOLDOWN_SECS}s cooldown "
            f"(consecutive_losses={self._consecutive_losses[key]})"
        )
        pause_after = settings.ETH_CONSEC_LOSS_PAUSE if asset == "ETH" else settings.LAB_CONSEC_LOSS_PAUSE
        cooldown_secs = settings.ETH_LOSS_COOLDOWN_SECS if asset == "ETH" else 600
        if self._consecutive_losses[key] >= pause_after:
            self._loss_cooldown_until[key] = time.time() + cooldown_secs
            self._consecutive_losses[key] = 0
            logger.warning(
                f"LatencyArb: {pause_after} consecutive stop-losses on {key} — extending pause to {cooldown_secs}s"
            )

    def on_win(self, timeframe: str | None = None, asset: str = "BTC") -> None:
        key = f"{asset}_{timeframe}" if timeframe in ("5m", "15m") else f"{asset}_5m"
        if key not in self._consecutive_losses:
            key = f"BTC_5m"  # fallback for old trades without asset in metadata
        self._consecutive_losses[key] = 0
        self._loss_cooldown_until[key] = 0.0
        logger.info(f"LatencyArb: win recorded for {key}, consecutive loss counter reset")

    def on_loss(self, timeframe: str | None = None, asset: str = "BTC") -> None:
        key = f"{asset}_{timeframe}" if timeframe in ("5m", "15m") else f"{asset}_5m"
        if key not in self._consecutive_losses:
            key = f"BTC_5m"
        self._consecutive_losses[key] += 1
        logger.info(
            f"LatencyArb: loss recorded for {key}, consecutive losses = "
            f"{self._consecutive_losses[key]}"
        )
        pause_after = settings.ETH_CONSEC_LOSS_PAUSE if asset == "ETH" else settings.LAB_CONSEC_LOSS_PAUSE
        cooldown_secs = settings.ETH_LOSS_COOLDOWN_SECS if asset == "ETH" else 600
        if self._consecutive_losses[key] >= pause_after:
            self._loss_cooldown_until[key] = time.time() + cooldown_secs
            self._consecutive_losses[key] = 0
            logger.warning(
                f"LatencyArb: {pause_after} consecutive losses on {key} — pausing for {cooldown_secs}s"
            )

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
        now = time.time()
        key = f"{asset}_{timeframe}"
        cooldown_until = self._loss_cooldown_until.get(key, 0.0)
        cooldown_state = "inactive"
        if now < cooldown_until:
            remaining = max(0.0, cooldown_until - now)
            cooldown_state = f"{remaining:.0f}s"
            if now - self._last_cooldown_log.get(key, 0.0) >= 60:
                self._last_cooldown_log[key] = now
                logger.debug(
                    f"LatencyArb: loss cooldown active for {key}, "
                    f"resuming in {remaining:.0f}s"
                )
            return False, "cooldown_active", cooldown_state
        if total_open_count >= self.MAX_CONCURRENT:
            return False, "overall_capacity_full", cooldown_state
        if branch_open_count.get(timeframe, 0) >= self._branch_limits[timeframe]:
            return False, "branch_capacity_full", cooldown_state
        return True, "ready", cooldown_state

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
        _noisy_reasons = {"weak_momentum", "quiet_hours", "no_signal", "warmup"}
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
