"""Strategy 3: LLM news scoring → trade signal via Claude."""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional, Any

import aiohttp

from config import settings
from data.news_feed import NewsFeed, NewsItem
from data.news_analyzer import get_analyzer
from data.market_scanner import get_scanner
from data.crypto_prices import get_crypto_prices, format_prices_for_prompt
from database import db
from strategies.base import BaseStrategy
from utils.logger import logger
from utils.helpers import round_price, extract_clob_token_ids


def _clamp01(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def calibrate_probability_shift(analysis: dict, theme_weight: float = 1.0) -> float:
    """
    Convert the analyzer's raw probability shift into a more selective, quality-
    weighted shift so weak signals shrink materially and only stronger news
    keeps a large edge.
    """
    if not analysis.get("is_relevant"):
        return 0.0

    raw_shift = float(
        analysis.get("raw_implied_probability_shift", analysis.get("implied_probability_shift", 0.0))
    )
    raw_shift = max(-0.25, min(0.25, raw_shift))
    confidence = _clamp01(analysis.get("confidence", 0.0))
    urgency = _clamp01(analysis.get("urgency", 0.0))
    impact_strength = _clamp01(analysis.get("impact_strength", 0.0))
    signal_quality = min(
        1.0,
        0.45 * impact_strength
        + 0.35 * confidence
        + 0.20 * urgency,
    )
    adjusted_shift = raw_shift * signal_quality * max(0.0, theme_weight)
    return max(-0.25, min(0.25, adjusted_shift))


class AISentiment(BaseStrategy):
    """
    Polls news sources on a configurable interval.
    For each relevant headline, finds matching Polymarket markets and asks the
    analyzer (Claude Haiku or keyword fallback) for a probability estimate.
    Trades when there is sufficient edge (>= SENTIMENT_MIN_EDGE + 2% fee buffer).

    Runs as an isolated profile — separate DB and log from latency arb.
    """

    name = "ai_sentiment"
    _GAMMA_BASE = "https://gamma-api.polymarket.com"

    # FIX 1: Peace market direction flip keyword sets
    _PEACE_MARKET_KEYWORDS = frozenset([
        "ceasefire", "peace deal", "peace talks", "truce",
        "armistice", "withdrawal", "negotiate",
    ])
    _CONFLICT_NEWS_KEYWORDS = frozenset([
        "strike", "attack", "bomb", "missile", "invasion", "assault",
        "casualt", "kill", "destroy", "forces enter", "troops",
    ])
    _NEGOTIATION_NEWS_KEYWORDS = frozenset([
        "mediator", "negotiat", "prisoner exchange", "hostage deal",
        "talks", "diplomat",
    ])
    _ALLOWED_CATALYST_CLASSES = frozenset({
        "official_catalyst",
        "confirmed_event",
        "policy_action",
    })
    _MARKET_KIND_KEYWORDS = {
        "conflict": frozenset({
            "strike", "attack", "war", "missile", "troops", "forces", "enter",
            "invasion", "bomb", "military", "assault", "airstrike",
            "invade", "occupy", "offensive", "shelling", "barrage", "intercept",
            "blockade", "naval", "escort", "warship", "convoy",
        }),
        "peace": frozenset({
            "ceasefire", "truce", "peace", "hostage", "deal", "withdrawal",
        }),
        "diplomacy": frozenset({
            "meeting", "talks", "negot", "diplomat", "agreement", "nuclear deal",
            "strait", "hormuz", "shipping",
        }),
        "politics": frozenset({
            "court", "tariff", "fed", "congress", "parliament", "election", "approval",
            "rating", "poll", "president", "senate", "white house", "executive",
            "trump", "biden", "democrat", "republican", "vote", "legislation",
            "pope", "rift", "courage", "meloni", "approve", "disapprove",
        }),
        "policy": frozenset({
            "sanction", "sanctions", "embargo", "restriction", "penalty", "ban",
            "tariff", "regulation", "policy",
        }),
        "crypto": frozenset({
            "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency", "solana",
            "sol", "price", "above", "below", "reach", "etf", "coinbase", "binance",
            "halving", "blockchain", "defi", "stablecoin", "altcoin", "token",
        }),
        "economics": frozenset({
            "rate", "cut", "hike", "inflation", "cpi", "gdp", "recession", "jobs",
            "unemployment", "fed", "fomc", "powell", "treasury", "yield", "interest",
            "deficit", "debt", "stagflation", "nonfarm", "payroll",
        }),
    }
    _GENERIC_MATCH_TOKENS = frozenset({
        "iran", "israel", "middle", "east", "war", "conflict", "politics",
        "world", "news", "update", "breaking", "future", "impact", "against",
        "allies", "minister", "officials", "government",
    })
    _TIME_TOKENS = frozenset({
        "today", "tomorrow", "tonight", "week", "month", "months", "quarter",
        "april", "may", "june", "july", "august", "september", "october",
        "november", "december", "2026",
    })

    def __init__(self):
        super().__init__()
        self._scanner = get_scanner()
        self._news_queue: asyncio.Queue[NewsItem] = asyncio.Queue(maxsize=100)
        # Load persisted cooldowns from DB so they survive restarts
        try:
            self._market_cooldowns: dict[str, float] = db.get_market_cooldowns("ai_sentiment")
            if self._market_cooldowns:
                logger.info(f"AISentiment: loaded {len(self._market_cooldowns)} cooldowns from DB")
        except Exception:
            self._market_cooldowns = {}
        self._processed_fingerprints: set[str] = set()
        self._skip_log_counts: dict[tuple[str, str], int] = {}
        self._strategy_halted = False
        self._db = db

        # Theme-group cooldowns — block an entire correlated group after any entry/exit
        # Prevents iran_conflict + middle_east_war + ceasefire_deal + iran_diplomacy
        # from each being used to re-enter the same bet under a different theme name.
        self._theme_group_cooldowns: dict[str, float] = {}  # group_name → last_entry_ts

        # FIX 1: Scanner token budget tracking
        # Max LLM calls per scan cycle (10 per 5-min cycle = 2/min — far below rate limit)
        self._SCANNER_MAX_LLM_PER_CYCLE: int = 10
        # Max scanner LLM calls per day — reserves tokens for news classification
        self._SCANNER_DAILY_LLM_BUDGET: int = 500
        self._scanner_daily_calls: int = 0
        self._scanner_budget_reset_day: int = -1

        # FIX 9: Kelly progressive sizing cache (refresh every 30 min from DB)
        self._kelly_cap_usdc: float = 8.0  # conservative default until proven
        self._kelly_cap_last_refresh: float = 0.0
        self._kelly_cap_refresh_interval: float = 1800.0  # 30 minutes

        # FIX 2 & 3: Strategy parameter defaults (overridden by sentiment_config.yaml)
        self._min_token_price: float = 0.10
        self._max_token_price: float = 0.90
        self._dead_zone_low: float = 0.35   # skip near-50/50 entries (no LLM edge)
        self._dead_zone_high: float = 0.50
        self._thesis_invalidation_pct: float = 0.12
        self._thesis_invalid_min_age_secs: int = 900

        # Load theme config from YAML
        self._theme_config = {}
        self._news_config = {}
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "runs", "sentiment", "sentiment_config.yaml"
        )
        if os.path.exists(config_path):
            try:
                import yaml
                with open(config_path) as f:
                    loaded = yaml.safe_load(f)
                    self._theme_config = loaded.get("themes", {})
                    self._news_config = loaded.get("news_sources", {})
                    sp = loaded.get("strategy_params", {})
                    self._min_token_price = float(sp.get("min_token_price", self._min_token_price))
                    self._max_token_price = float(sp.get("max_token_price", self._max_token_price))
                    self._dead_zone_low = float(sp.get("dead_zone_low", self._dead_zone_low))
                    self._dead_zone_high = float(sp.get("dead_zone_high", self._dead_zone_high))
                    self._thesis_invalidation_pct = float(sp.get("thesis_invalidation_pct", self._thesis_invalidation_pct))
                    self._thesis_invalid_min_age_secs = int(sp.get("thesis_invalid_min_age_secs", self._thesis_invalid_min_age_secs))
                logger.info(f"AISentiment: loaded {len(self._theme_config)} themes from config")
            except Exception as e:
                logger.warning(f"AISentiment: failed to load theme config: {e}")
        else:
            logger.warning(
                f"AISentiment: no theme config found at {config_path} — using empty theme map"
            )

        self._analyzer = get_analyzer()
        self._news_feed = NewsFeed(
            on_item=self._enqueue_news,
            theme_config=self._theme_config,
            news_config=self._news_config,
        )

    def _enqueue_news(self, item: NewsItem) -> None:
        try:
            self._news_queue.put_nowait(item)
        except asyncio.QueueFull:
            pass  # drop old news

    def _log_repeated_skip(self, reason: str, scope: str, message: str, every: int = 10) -> None:
        """
        Keep the first skip visible at INFO, then reduce noise for repeated
        identical skips by downgrading them to DEBUG and surfacing a summary
        pulse every N repeats.
        """
        key = (reason, scope)
        count = self._skip_log_counts.get(key, 0) + 1
        self._skip_log_counts[key] = count

        if count == 1:
            logger.info(message)
            return

        if count % every == 0:
            logger.info(f"{message} x{count}")
            return

        logger.debug(message)

    async def run(self) -> None:
        if not getattr(settings, "AI_SENTIMENT_ENABLED", False):
            logger.warning("AISentiment: AI_SENTIMENT_ENABLED=false — strategy disabled")
            return
        logger.info("AISentiment starting")
        tasks = [
            self._news_feed.run(),
            self._process_queue(),
            self._paper_exit_loop(),
        ]
        if getattr(settings, "SENTIMENT_SCAN_ENABLED", True):
            tasks.append(self._proactive_scan_loop())
        await asyncio.gather(*tasks)

    async def _paper_exit_loop(self) -> None:
        """Runs every SENTIMENT_REPRICE_INTERVAL_SECONDS and checks open positions for exit conditions."""
        while self._running:
            await asyncio.sleep(settings.SENTIMENT_REPRICE_INTERVAL_SECONDS)
            if not getattr(settings, "SENTIMENT_PAPER_EXIT_ENABLED", True):
                continue
            n_positions = sum(1 for p in self._portfolio.all_positions() if p.strategy == self.name)
            if n_positions > 0:
                logger.info(f"AISentiment paper exit loop: checking {n_positions} positions")
            try:
                await self._check_paper_exits()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                import traceback
                logger.error(f"AISentiment paper exit check error: {exc}\n{traceback.format_exc()}")

    async def _check_paper_exits(self) -> None:
        """Evaluate each open ai_sentiment position against time/PnL stops and close if triggered."""
        now = time.time()
        positions = [p for p in self._portfolio.all_positions() if p.strategy == self.name]
        for pos in positions:
            meta: dict = {}
            if pos.metadata_json:
                try:
                    meta = json.loads(pos.metadata_json)
                except Exception:
                    pass

            time_stop_minutes = float(settings.SENTIMENT_TIME_STOP_MINUTES)
            take_profit_pct = float(meta.get("take_profit_pct", settings.SENTIMENT_TAKE_PROFIT_PCT))
            thesis_invalidation_pct = self._thesis_invalidation_pct
            token_label = meta.get("token_label", "YES")

            # FIX 1 (analysis): Disable stop-loss entirely for low-priced tokens (< 0.30).
            # At entry price 0.14, a 20% stop = $0.028 move — within normal market noise.
            # These tokens are long-shot bets: hold to time-stop, not noise-stop.
            base_stop_loss_pct = float(meta.get("stop_loss_pct", settings.SENTIMENT_STOP_LOSS_PCT))
            entry_price_for_tier = pos.entry_price
            if entry_price_for_tier < 0.30:
                stop_loss_pct = None  # disabled — noise >> signal at low prices
            elif entry_price_for_tier > 0.75:
                stop_loss_pct = base_stop_loss_pct  # tight for high-priced tokens
            else:
                stop_loss_pct = base_stop_loss_pct

            age_minutes = (now - pos.opened_at) / 60

            try:
                current_price = self._client.get_midpoint(pos.token_id)
            except Exception as e:
                logger.warning(f"AISentiment paper exit: price fetch exception for {pos.market_id[:12]}…: {e}")
                current_price = None

            if not current_price or current_price <= 0:
                logger.warning(f"AISentiment paper exit: no price for {pos.market_id[:12]}… (got {current_price!r}), age={age_minutes:.1f}m")
                if age_minutes > time_stop_minutes:
                    pnl = self._portfolio.close_position(pos.market_id, exit_price=pos.entry_price)
                    realized = f"{pnl:+.4f}" if pnl is not None else "?"
                    if pnl is not None:
                        self._db.update_trades_for_market(
                            pos.market_id,
                            fill_price=pos.entry_price,
                            pnl=pnl,
                            exit_reason="time_stop_no_price",
                        )
                        self._risk.record_fill(pnl)
                        theme_from_meta = meta.get("theme", "unknown")
                        try:
                            self._db.record_trade_outcome(theme_from_meta, pos.market_id, pnl)
                        except Exception:
                            pass
                    logger.warning(
                        f"AISentiment EXIT [time_stop_no_price] {pos.market_id[:12]}… "
                        f"entry={pos.entry_price:.3f} age={age_minutes:.0f}m PnL={realized} USDC"
                    )
                continue

            entry = pos.entry_price
            logger.debug(
                f"AISentiment exit eval: age={age_minutes:.1f}m time_stop={time_stop_minutes}m "
                f"price={current_price} entry={entry:.3f} "
                f"tp_threshold={entry * (1 + take_profit_pct):.3f} sl_threshold={entry * (1 - stop_loss_pct):.3f}"
            )
            reason: str | None = None
            if age_minutes > time_stop_minutes:
                reason = "time_stop"
            elif current_price >= entry * (1 + take_profit_pct):
                reason = "take_profit"
            elif stop_loss_pct is not None and current_price <= entry * (1 - stop_loss_pct):
                reason = "stop_loss"
            elif token_label == "YES" and current_price < entry * (1 - thesis_invalidation_pct):
                age_secs = now - pos.opened_at
                if age_secs < self._thesis_invalid_min_age_secs:
                    logger.debug(
                        f"AISentiment: thesis_invalid check skipped — position only "
                        f"{age_secs:.0f}s old (min {self._thesis_invalid_min_age_secs}s)"
                    )
                else:
                    reason = "thesis_invalid"

            if reason:
                pnl = self._portfolio.close_position(pos.market_id, exit_price=current_price)
                realized = f"{pnl:+.4f}" if pnl is not None else "?"
                logger.info(
                    f"AISentiment EXIT [{reason}] {pos.market_id[:12]}… "
                    f"entry={entry:.3f} current={current_price:.3f} "
                    f"age={age_minutes:.0f}m PnL={realized} USDC"
                )
                # FIX 2: Enforce minimum 6h cooldown after any exit (8h after stop-loss).
                # This prevents the bot from re-entering the same market after a time-stop
                # and oscillating on the same bet repeatedly.
                # Formula: stored_ts = now + (desired_block - normal_cooldown)
                # Check: time.time() - stored_ts < normal_cooldown → blocked for desired_block total
                _now_ts = time.time()
                _normal_cd = settings.SENTIMENT_COOLDOWN_MINUTES * 60
                if reason == "stop_loss":
                    _block_secs = 8 * 3600  # 8 hours after stop-loss
                else:
                    _block_secs = 6 * 3600  # 6 hours after any other exit (was: normal_cooldown only)
                _cd_ts = _now_ts + (_block_secs - _normal_cd)
                self._market_cooldowns[pos.market_id] = _cd_ts
                try:
                    self._db.set_market_cooldown(pos.market_id, _cd_ts, "ai_sentiment")
                except Exception:
                    pass
                # Record outcome for win-rate calibration
                if pnl is not None:
                    self._db.update_trades_for_market(
                        pos.market_id,
                        fill_price=current_price,
                        pnl=pnl,
                        exit_reason=reason,
                    )
                    self._risk.record_fill(pnl)
                    theme_from_meta = meta.get("theme", "unknown")
                    try:
                        self._db.record_trade_outcome(theme_from_meta, pos.market_id, pnl)
                    except Exception:
                        pass

    async def _proactive_scan_loop(self) -> None:
        """
        Every SENTIMENT_SCAN_INTERVAL_SECONDS, independently fetch all markets
        closing within SENTIMENT_SCAN_MAX_HOURS hours, score each against current
        LLM-generated sentiment, and enter if edge is found.

        This is the self-directed mode: no news item required. The bot watches
        expiring markets for mispricing opportunities across all categories.
        """
        scan_interval = getattr(settings, "SENTIMENT_SCAN_INTERVAL_SECONDS", 300)
        max_hours = getattr(settings, "SENTIMENT_SCAN_MAX_HOURS", 48.0)
        min_edge = getattr(settings, "SENTIMENT_SCAN_MIN_EDGE", 0.08)

        await asyncio.sleep(30)  # stagger startup vs news feed
        while self._running:
            try:
                await self._run_proactive_scan(max_hours=max_hours, min_edge=min_edge)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                import traceback
                logger.error(f"AISentiment proactive scan error: {exc}\n{traceback.format_exc()}")
            await asyncio.sleep(scan_interval)

    async def _run_proactive_scan(self, max_hours: float, min_edge: float) -> None:
        """Fetch markets expiring soon, score them, trade if edge found."""
        if self._check_halted() or self._strategy_loss_cap_breached():
            return

        max_conc = getattr(settings, "SENTIMENT_MAX_CONCURRENT", 10)
        current_positions = sum(1 for p in self._portfolio.all_positions() if p.strategy == self.name)
        if current_positions >= max_conc:
            logger.debug(f"AISentiment scan: max_positions reached ({current_positions}/{max_conc}), skipping scan")
            return

        # FIX 1: Reset daily counter at midnight UTC
        today_day = datetime.now(timezone.utc).day
        if today_day != self._scanner_budget_reset_day:
            self._scanner_daily_calls = 0
            self._scanner_budget_reset_day = today_day
            logger.debug("AISentiment scan: daily LLM budget counter reset")

        # FIX 1: Guard against exhausting all tokens in the scanner — reserve some for news
        if self._scanner_daily_calls >= self._SCANNER_DAILY_LLM_BUDGET:
            logger.info(
                f"AISentiment scan: daily LLM budget exhausted "
                f"({self._scanner_daily_calls}/{self._SCANNER_DAILY_LLM_BUDGET} calls) — skipping"
            )
            return

        # Fetch live crypto prices once per scan — injected into Groq prompts so
        # it can evaluate "Will BTC be above $X?" without guessing.
        try:
            crypto_prices = await get_crypto_prices()
            if crypto_prices:
                logger.debug(
                    f"AISentiment scan: live prices — {format_prices_for_prompt(crypto_prices)}"
                )
        except Exception:
            crypto_prices = {}

        # Fetch short-term markets from Gamma
        markets_found = 0
        scored_candidates: list[tuple[float, dict, str]] = []  # (hours_remaining, market, category)

        scan_queries = [
            # ── Crypto ────────────────────────────────────────────────────────
            ("bitcoin price", "crypto_markets"),
            ("ethereum price", "crypto_markets"),
            ("bitcoin above", "crypto_markets"),
            ("ethereum above", "crypto_markets"),
            ("solana above", "crypto_markets"),
            ("bitcoin ETF", "crypto_markets"),
            ("crypto regulation", "crypto_markets"),
            ("BTC end of week", "crypto_markets"),
            ("will the price", "crypto_markets"),
            # Current BTC price range targets (~$93k as of Apr 2026)
            ("bitcoin above $90000", "crypto_markets"),
            ("bitcoin above $93000", "crypto_markets"),
            ("bitcoin above $95000", "crypto_markets"),
            ("bitcoin above $100000", "crypto_markets"),
            ("bitcoin end of month", "crypto_markets"),
            ("ethereum above $2500", "crypto_markets"),
            ("ethereum above $3000", "crypto_markets"),
            ("crypto market cap", "crypto_markets"),

            # ── US Politics ───────────────────────────────────────────────────
            ("trump announce", "us_politics"),
            ("trump sign", "us_politics"),
            ("trump tariff", "us_politics"),
            ("trump executive order", "us_politics"),
            ("trump approval", "us_politics"),
            ("approval rating", "us_politics"),
            ("senate vote", "us_politics"),
            ("supreme court", "us_politics"),
            ("election winner", "us_politics"),
            ("congress pass", "us_politics"),
            ("trump fire", "us_politics"),
            # Tariff / trade legislation targets
            ("tariff legislation", "us_politics"),
            ("tariff bill", "us_politics"),
            ("congress tariff", "us_politics"),
            ("trade deal", "us_politics"),
            ("trump impeach", "us_politics"),
            ("senate confirm", "us_politics"),
            ("federal reserve chair", "us_politics"),

            # ── Economics ─────────────────────────────────────────────────────
            ("fed rate", "us_economy"),
            ("interest rate", "us_economy"),
            ("inflation", "us_economy"),
            ("S&P 500", "us_economy"),
            ("recession", "us_economy"),
            ("GDP growth", "us_economy"),
            ("unemployment", "us_economy"),
            ("stock market", "us_economy"),
            ("oil price", "us_economy"),
            ("gold price", "us_economy"),

            # ── Sports: NBA ───────────────────────────────────────────────────
            ("NBA playoffs", "sports_events"),
            ("NBA championship", "sports_events"),
            ("NBA winner", "sports_events"),
            ("NBA series", "sports_events"),

            # ── Sports: Soccer ────────────────────────────────────────────────
            ("Champions League", "sports_events"),
            ("Premier League", "sports_events"),
            ("La Liga", "sports_events"),
            ("Serie A", "sports_events"),
            ("World Cup", "sports_events"),
            ("Europa League", "sports_events"),

            # ── Sports: American ──────────────────────────────────────────────
            ("NFL draft", "sports_events"),
            ("MLB game", "sports_events"),
            ("NHL playoffs", "sports_events"),
            ("Super Bowl", "sports_events"),

            # ── Sports: Combat / Individual ───────────────────────────────────
            ("UFC fight", "sports_events"),
            ("boxing fight", "sports_events"),
            ("MMA fight", "sports_events"),

            # ── Sports: Tennis / Golf / F1 ────────────────────────────────────
            ("tennis tournament", "sports_events"),
            ("Wimbledon", "sports_events"),
            ("Roland Garros", "sports_events"),
            ("Masters golf", "sports_events"),
            ("Formula 1", "sports_events"),
            ("F1 race", "sports_events"),

            # ── Tech companies ────────────────────────────────────────────────
            ("Apple earnings", "tech_companies"),
            ("Tesla stock", "tech_companies"),
            ("nvidia earnings", "tech_companies"),
            ("Microsoft earnings", "tech_companies"),
            ("SpaceX launch", "tech_companies"),
            ("AI regulation", "tech_companies"),
            ("OpenAI", "tech_companies"),
            ("Meta earnings", "tech_companies"),
            ("Google antitrust", "tech_companies"),
            ("Amazon earnings", "tech_companies"),

            # ── Entertainment / Awards ────────────────────────────────────────
            ("Oscar winner", "entertainment"),
            ("Grammy winner", "entertainment"),
            ("Emmy award", "entertainment"),
            ("box office", "entertainment"),
            ("Netflix show", "entertainment"),

            # ── Global elections & politics ───────────────────────────────────
            ("UK election", "global_elections"),
            ("EU election", "global_elections"),
            ("Germany election", "global_elections"),
            ("France election", "global_elections"),
            ("India election", "global_elections"),
            ("Canada election", "global_elections"),
            ("Japan election", "global_elections"),
            ("Brazil election", "global_elections"),

            # ── Geopolitics ───────────────────────────────────────────────────
            ("Russia Ukraine", "geopolitics"),
            ("Ukraine war", "geopolitics"),
            ("NATO", "geopolitics"),
            ("iran nuclear", "iran_diplomacy"),
            ("US Iran deal", "iran_diplomacy"),
            ("US China trade", "geopolitics_china"),
            ("china tariff", "geopolitics_china"),
            ("Taiwan strait", "geopolitics_china"),
            ("North Korea", "geopolitics"),

            # ── General time-bounded ──────────────────────────────────────────
            ("by april 30", "general"),
            ("by may", "general"),
            ("by june", "general"),
            ("this week", "general"),
            ("will there be", "general"),
            ("first to", "general"),
        ]

        seen_market_ids: set[str] = set()
        for query, category in scan_queries:
            try:
                results = await self._search_live_markets(query, max_results=5)
                for market in results:
                    market_key = self._market_identifier(market)
                    if not market_key or market_key in seen_market_ids:
                        continue
                    seen_market_ids.add(market_key)
                    markets_found += 1

                    # Only consider short-resolution markets
                    end_dt = self._parse_market_end(market)
                    if end_dt is None:
                        continue
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                    hours_remaining = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                    if hours_remaining <= 0 or hours_remaining > max_hours:
                        continue

                    liquidity = float(market.get("liquidityNum") or market.get("liquidity") or 0.0)
                    volume = float(market.get("volumeNum") or market.get("volume24hr") or market.get("volume") or 0.0)
                    if liquidity < settings.SENTIMENT_MIN_LIQUIDITY:
                        continue
                    if volume < settings.SENTIMENT_MIN_VOLUME_24H:
                        continue

                    scored_candidates.append((hours_remaining, market, category))
            except Exception as e:
                logger.debug(f"AISentiment scan query '{query}' failed: {e}")

        if not scored_candidates:
            logger.debug(f"AISentiment proactive scan: no short-term candidates found (searched {markets_found} markets)")
            return

        # Sort by soonest expiry first — most time-sensitive first
        scored_candidates.sort(key=lambda x: x[0])
        logger.info(f"AISentiment proactive scan: {len(scored_candidates)} short-term candidates (max {max_hours:.0f}h) from {markets_found} markets")

        # FIX 1: Cap LLM calls per cycle to preserve daily token budget
        llm_calls_this_cycle = 0

        for hours_remaining, market, category in scored_candidates:
            # FIX 1: Stop if per-cycle LLM budget used
            if llm_calls_this_cycle >= self._SCANNER_MAX_LLM_PER_CYCLE:
                logger.debug(
                    f"AISentiment scan: per-cycle LLM cap reached ({llm_calls_this_cycle}), stopping"
                )
                break
            # FIX 1: Stop if daily LLM budget used
            if self._scanner_daily_calls >= self._SCANNER_DAILY_LLM_BUDGET:
                logger.info(
                    f"AISentiment scan: daily LLM budget reached mid-cycle "
                    f"({self._scanner_daily_calls}/{self._SCANNER_DAILY_LLM_BUDGET}), stopping"
                )
                break

            if self._portfolio.has_position(self._market_identifier(market) or ""):
                continue
            market_id = self._market_identifier(market) or ""
            if not market_id:
                continue
            cooldown_secs = settings.SENTIMENT_COOLDOWN_MINUTES * 60
            if time.time() - self._market_cooldowns.get(market_id, 0) < cooldown_secs:
                continue

            # FIX 3: Skip scanner-disabled categories (geopolitical themes with no LLM edge)
            theme_cfg = self._theme_config.get(category, {})
            if theme_cfg.get("scanner_disabled", False):
                logger.debug(f"AISentiment proactive SKIP scanner_disabled: {category} | {(market.get('question',''))[:50]}")
                continue

            # Use LLM to evaluate if this expiring market is mispriced
            question = market.get("question") or market.get("title") or ""
            try:
                # Extract YES price from Gamma outcomePrices field
                # Gamma API returns outcomePrices as a JSON-encoded string e.g. '["0.62","0.38"]'
                current_price = 0.5
                outcome_prices = market.get("outcomePrices")
                if outcome_prices:
                    try:
                        if isinstance(outcome_prices, str):
                            import json as _json
                            outcome_prices = _json.loads(outcome_prices)
                        if outcome_prices and len(outcome_prices) >= 1:
                            current_price = float(outcome_prices[0])
                    except (TypeError, ValueError, Exception):
                        pass
                if current_price == 0.5:
                    # fallback: tokens array (older API shape)
                    tokens = market.get("tokens") or []
                    for tok in tokens:
                        if str(tok.get("outcome", "")).upper() == "YES":
                            current_price = float(tok.get("price") or tok.get("lastTradePrice") or 0.5)
                            break

                # Skip near-resolved markets before calling LLM — saves API quota.
                # Direct-math categories (crypto, sports, weather, elections, politics, geo)
                # use a lower floor (0.10) because the stop-loss fix already protects cheap
                # tokens and these evals don't rely on LLM at all.
                _direct_math_cats = {
                    "crypto_markets", "crypto", "sports_events", "weather",
                    "us_economy", "global_elections", "us_politics",
                    "geopolitics", "ceasefire_deal",
                }
                effective_min = 0.10 if category in _direct_math_cats else self._min_token_price
                if current_price < effective_min or current_price > self._max_token_price:
                    logger.debug(
                        f"AISentiment proactive SKIP price_range: {question[:55]} price={current_price:.3f}"
                    )
                    continue

                # FIX 5: Direct crypto math evaluation — no LLM token used
                analysis = None
                if category in ("crypto_markets", "crypto") and crypto_prices:
                    analysis = self._try_direct_crypto_eval(question, current_price, crypto_prices)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_crypto: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # FIX 6: Direct sports result evaluation — no LLM token used
                if analysis is None and category == "sports_events":
                    analysis = await self._try_direct_sports_eval(question, current_price)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_sports: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # FIX 7: Direct weather forecast evaluation — no LLM token used
                if analysis is None and category == "weather":
                    analysis = await self._try_direct_weather_eval(question, current_price)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_weather: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # FIX 8: Direct Fed rate futures evaluation — no LLM token used
                if analysis is None and category == "us_economy":
                    analysis = await self._try_direct_econ_eval(question, current_price)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_econ: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # FIX 11: FRED economic indicator evaluation — no LLM token used
                # Covers CPI/inflation, unemployment, 10-year yield, payrolls
                if analysis is None and category == "us_economy":
                    analysis = await self._try_direct_fred_eval(question, current_price)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_fred: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # FIX 10: Kalshi real-money crowd evaluation — no LLM token used
                if analysis is None and category == "us_politics":
                    analysis = await self._try_direct_kalshi_eval(question, current_price)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_kalshi: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # FIX 9: Direct election polling evaluation — no LLM token used
                if analysis is None and category == "global_elections":
                    analysis = await self._try_direct_polling_eval(question, current_price)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_polling: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # FIX 10: Direct geopolitical crowd evaluation — no LLM token used
                # Only fires for geopolitics/ceasefire_deal — other geo themes remain disabled.
                # If Manifold has no matching market (< 30 bettors or < 35% overlap), skips silently.
                _geo_crowd_categories = {"geopolitics", "ceasefire_deal"}
                if analysis is None and category in _geo_crowd_categories:
                    analysis = await self._try_direct_geo_eval(question, current_price)
                    if analysis:
                        logger.info(
                            f"AISentiment proactive direct_geo: {question[:55]} "
                            f"price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                            f"edge={analysis.get('implied_probability_shift', 0):+.3f}"
                        )

                # Fall back to LLM evaluation — only for themes where LLM adds value.
                # Sports/weather/econ/elections/geopolitics skip LLM if direct math found nothing.
                # Crypto keeps LLM fallback: "Will BTC ETF get approved?" has no price target
                # but LLM can reason about it.
                _data_math_only = {
                    "sports_events",
                    "weather", "us_economy", "global_elections",
                    "geopolitics", "ceasefire_deal",
                    # us_politics: uses Kalshi first, falls back to LLM if no match
                }
                if analysis is None and category not in _data_math_only:
                    analysis = await self._analyzer.evaluate_market_pricing(
                        question=question,
                        current_price=current_price,
                        hours_remaining=hours_remaining,
                        category=category,
                        crypto_prices=crypto_prices,
                    )
                    llm_calls_this_cycle += 1
                    self._scanner_daily_calls += 1
                elif analysis is None:
                    # Data-math category but no signal found — skip quietly
                    logger.debug(
                        f"AISentiment proactive SKIP no_data_signal [{category}]: {question[:55]}"
                    )
                    continue

                if not analysis.get("is_relevant"):
                    logger.info(
                        f"AISentiment proactive SKIP not_mispriced: {question[:55]} "
                        f"price={current_price:.3f} | {analysis.get('reasoning_short','no reason')}"
                    )
                    continue

                implied_shift = float(analysis.get("implied_probability_shift", 0.0))
                if abs(implied_shift) < min_edge:
                    logger.info(
                        f"AISentiment proactive SKIP low_edge {abs(implied_shift):.3f} < {min_edge:.3f}: "
                        f"{question[:55]}"
                    )
                    continue

                # Build a synthetic news item for _evaluate_market compatibility
                from data.news_feed import NewsItem as _NewsItem
                synthetic_item = _NewsItem(
                    title=question,
                    summary=f"Market pricing analysis: {analysis.get('reasoning_short', '')}",
                    url="",
                    source="proactive_scan",
                    published_at=datetime.now(timezone.utc),
                    raw_themes=[category],
                )
                analysis["raw_implied_probability_shift"] = implied_shift
                # Mark as proactive scan so _evaluate_market skips mapping check
                market["_proactive_scan"] = True
                market["_sentiment_query_entry"] = {
                    "query": question[:40],
                    "priority": 0.80,
                    "trade_type": "reaction" if hours_remaining < 6 else "event",
                    "action_tags": [],
                    "catalyst_types": [],
                }
                logger.info(
                    f"AISentiment proactive: {hours_remaining:.1f}h market — "
                    f"{question[:55]} | price={current_price:.3f} fair={analysis.get('fair_probability', 0):.3f} "
                    f"edge={implied_shift:+.3f} conf={analysis.get('confidence', 0):.2f}"
                )
                await self._evaluate_market(None, category, analysis, market, synthetic_item)

                current_positions = sum(1 for p in self._portfolio.all_positions() if p.strategy == self.name)
                if current_positions >= max_conc:
                    break
            except Exception as e:
                logger.debug(f"AISentiment proactive eval error for {question[:40]}: {e}")

    async def _process_queue(self) -> None:
        while self._running:
            if self._check_halted():
                await asyncio.sleep(30)
                continue
            if self._strategy_loss_cap_breached():
                await asyncio.sleep(30)
                continue

            try:
                item = await asyncio.wait_for(self._news_queue.get(), timeout=10)
                await self._process_news_item(item)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"AISentiment process error: {exc}")

    async def _process_news_item(self, item: NewsItem) -> None:
        if self._is_stale_news(item):
            logger.debug(f"AISentiment SKIP stale_news | {item.title[:60]}")
            return

        # Session-level dedupe (DB is authoritative, this is a fast in-memory check)
        if item.fingerprint in self._processed_fingerprints:
            return
        if self._db.news_item_exists(item.fingerprint):
            self._processed_fingerprints.add(item.fingerprint)
            logger.debug(f"AISentiment: duplicate fingerprint skipped — {item.title[:50]}")
            return
        self._processed_fingerprints.add(item.fingerprint)

        # Save to DB
        news_id: Optional[int] = None
        try:
            news_id = self._db.insert_news_item(
                fingerprint=item.fingerprint,
                source=item.source,
                title=item.title,
                url=item.url,
                published_at=item.published_at,
                summary=item.summary,
                raw_themes=",".join(item.raw_themes),
            )
        except Exception:
            # Already in DB from a previous run — that's fine, still process
            pass

        themes = item.raw_themes if item.raw_themes else list(self._theme_config.keys())

        for theme in themes:
            theme_config = self._theme_config.get(theme, {})
            if not theme_config.get("enabled", True):
                logger.debug(f"AISentiment: theme disabled — {theme}")
                continue

            # Classify with analyzer
            analysis = await self._analyzer.analyze(item, theme, theme_config)
            raw_shift = float(analysis.get("implied_probability_shift", 0.0))
            analysis["raw_implied_probability_shift"] = raw_shift
            analysis["implied_probability_shift"] = calibrate_probability_shift(
                analysis,
                theme_weight=float(theme_config.get("weight", 1.0)),
            )

            # Save analysis to DB
            if news_id:
                try:
                    self._db.insert_news_analysis(
                        news_item_id=news_id,
                        theme=theme,
                        is_relevant=analysis["is_relevant"],
                        direction=analysis["direction"],
                        confidence=analysis["confidence"],
                        urgency=analysis["urgency"],
                        impact_strength=analysis["impact_strength"],
                        reasoning_short=analysis["reasoning_short"],
                        market_tags=str(analysis["market_tags"]),
                        analyzer_name=analysis["analyzer_name"],
                    )
                except Exception as e:
                    logger.debug(f"AISentiment: DB analysis insert error: {e}")

            if not analysis["is_relevant"]:
                logger.debug(f"AISentiment: irrelevant for {theme} — {item.title[:50]}")
                continue

            # Confidence + urgency gates
            min_conf = settings.SENTIMENT_MIN_CONFIDENCE
            min_urg = settings.SENTIMENT_MIN_URGENCY
            if analysis["confidence"] < min_conf:
                logger.info(
                    f"AISentiment SKIP low_confidence {analysis['confidence']:.2f} < {min_conf} "
                    f"| {item.title[:50]}"
                )
                continue
            if analysis["urgency"] < min_urg:
                logger.info(
                    f"AISentiment SKIP low_urgency {analysis['urgency']:.2f} < {min_urg} "
                    f"| {item.title[:50]}"
                )
                continue
            if not self._is_tradeable_catalyst(theme_config, analysis):
                logger.info(
                    f"AISentiment SKIP weak_catalyst {analysis.get('catalyst_class', 'noise')} "
                    f"| {item.title[:50]}"
                )
                continue

            logger.info(
                f"AISentiment: RELEVANT [{theme}] dir={analysis['direction']} "
                f"cat={analysis.get('catalyst_class', 'noise')} "
                f"conf={analysis['confidence']:.2f} urg={analysis['urgency']:.2f} "
                f"impact={analysis['impact_strength']:.2f} "
                f"shift={raw_shift:+.3f}->{analysis['implied_probability_shift']:+.3f} | "
                f"{item.title[:60]}"
            )

            # Skip news-driven trading for themes marked news_trading_disabled.
            # These themes still run via the proactive scanner (which has real price data).
            # Geopolitical speculation belongs here — Groq has no edge over market consensus
            # on "Will Iran ceasefire hold?" based on public news articles.
            if theme_config.get("news_trading_disabled", False):
                logger.debug(
                    f"AISentiment SKIP news_trading_disabled [{theme}] — "
                    f"proactive scanner only | {item.title[:50]}"
                )
                continue

            # Find markets for this theme
            market_candidates = await self._discover_market_candidates(
                theme,
                theme_config,
                item,
                analysis,
            )

            if not market_candidates:
                self._log_repeated_skip(
                    "no_market_mapping",
                    f"theme:{theme}",
                    f"AISentiment SKIP no_market_mapping for theme={theme}",
                )
                continue

            # Max concurrent positions check
            max_conc = min(
                getattr(
                    settings,
                    "AI_MAX_CONCURRENT_POSITIONS",
                    getattr(settings, "SENTIMENT_MAX_CONCURRENT", 3),
                ),
                getattr(settings, "SENTIMENT_MAX_CONCURRENT", 3),
            )
            current_positions = sum(
                1 for pos in self._portfolio.all_positions() if pos.strategy == self.name
            )
            if current_positions >= max_conc:
                self._log_repeated_skip(
                    "max_positions",
                    f"theme:{theme}",
                    f"AISentiment SKIP max_positions ({current_positions}/{max_conc}) "
                    f"for theme={theme}"
                )
                continue

            for market in market_candidates[: settings.SENTIMENT_MAX_MARKETS_PER_ITEM]:
                if market.get("_llm_discovered"):
                    logger.info(
                        f"AISentiment: LLM-discovered market → {market.get('question', '')[:60]}"
                    )
                await self._evaluate_market(news_id, theme, analysis, market, item)

    async def _maybe_await(self, value: Any) -> Any:
        if asyncio.iscoroutine(value):
            return await value
        return value

    def _market_identifier(self, market: dict) -> Optional[str]:
        """Stable per-market key for positions, cooldowns, and DB rows."""
        if not isinstance(market, dict):
            return None

        value = market.get("conditionId") or market.get("id")
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None:
            return str(value)

        return None

    def _gamma_market_id(self, market: dict) -> Optional[str]:
        value = market.get("id")
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None:
            return str(value)
        return None

    def _is_hex_identifier(self, value: str) -> bool:
        return bool(re.fullmatch(r"0x[0-9a-fA-F]{16,}", value or ""))

    def _is_viable_market(self, market: dict) -> bool:
        if not isinstance(market, dict):
            return False

        if market.get("archived") is True:
            return False
        if market.get("closed") is True:
            return False
        if market.get("active") is False:
            return False
        if market.get("acceptingOrders") is False:
            return False

        if not (market.get("question") or market.get("title")):
            return False
        if len(extract_clob_token_ids(market)) < 2:
            return False

        return self._market_identifier(market) is not None

    async def _fetch_json(self, url: str, params: Optional[dict] = None) -> Any:
        timeout = aiohttp.ClientTimeout(total=10)
        headers = {"Accept": "application/json"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url, params=params) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status} for {url}: {body[:200]}")
                return await resp.json()

    def _extract_markets_from_payload(self, payload: Any) -> list[dict]:
        """Pull only actual market dicts from Gamma search responses."""
        found: list[dict] = []
        seen: set[str] = set()

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                maybe_id = self._market_identifier(node)
                if maybe_id and maybe_id not in seen:
                    if (
                        ("question" in node or "title" in node)
                        and ("conditionId" in node or "id" in node)
                        and ("clobTokenIds" in node or "outcomes" in node)
                    ):
                        seen.add(maybe_id)
                        found.append(node)
                for value in node.values():
                    visit(value)
            elif isinstance(node, list):
                for item in node:
                    visit(item)

        visit(payload)
        return found

    async def _search_live_markets(self, query: str, max_results: int = 5) -> list[dict]:
        """Discover currently live markets from Gamma public search."""
        if not query or not str(query).strip():
            return []
        try:
            payload = await self._fetch_json(
                f"{self._GAMMA_BASE}/public-search",
                params={"q": query},
            )
        except Exception as e:
            logger.debug(f"AISentiment: live market search failed for '{query}': {e}")
            return []

        candidates = self._extract_markets_from_payload(payload)
        viable = [m for m in candidates if self._is_viable_market(m)]
        query_l = query.lower()

        def score(m: dict) -> tuple[int, float, float]:
            text = " ".join(
                str(m.get(k, "")) for k in ("question", "title", "description", "category", "slug")
            ).lower()
            contains = 1 if query_l in text else 0
            liquidity = float(m.get("liquidityNum") or m.get("liquidity") or 0.0)
            volume = float(m.get("volumeNum") or m.get("volume") or 0.0)
            return (contains, liquidity, volume)

        viable.sort(key=score, reverse=True)
        return viable[:max_results]

    async def _get_market_by_ref(self, market_ref: str) -> Optional[dict]:
        """Resolve a market by numeric Gamma id or conditionId without false 422s."""
        active_markets = await self._scanner.get_active_markets()
        for market in active_markets:
            if str(market.get("conditionId") or "") == market_ref:
                return market
            if str(market.get("id") or "") == market_ref:
                return market

        if market_ref and not self._is_hex_identifier(market_ref):
            try:
                market = await self._fetch_json(f"{self._GAMMA_BASE}/markets/{market_ref}")
                if isinstance(market, dict) and self._is_viable_market(market):
                    return market
            except Exception as e:
                logger.debug(f"AISentiment: direct Gamma market lookup failed for {market_ref}: {e}")

        try:
            getter = getattr(self._scanner, "get_market_by_id", None)
            if callable(getter):
                market = await self._maybe_await(getter(market_ref))
                if isinstance(market, dict) and self._is_viable_market(market):
                    return market
        except Exception as e:
            logger.debug(f"AISentiment: scanner get_market_by_id failed for {market_ref}: {e}")

        try:
            getter_many = getattr(self._scanner, "get_markets_by_ids", None)
            if callable(getter_many):
                result = await self._maybe_await(getter_many([market_ref]))
                if isinstance(result, dict):
                    market = result.get(market_ref)
                    if isinstance(market, dict) and self._is_viable_market(market):
                        return market
                elif isinstance(result, list):
                    for market in result:
                        if not isinstance(market, dict):
                            continue
                        if (
                            str(market.get("id") or "") == market_ref
                            or str(market.get("conditionId") or "") == market_ref
                        ) and self._is_viable_market(market):
                            return market
        except Exception as e:
            logger.debug(f"AISentiment: scanner get_markets_by_ids failed for {market_ref}: {e}")

        try:
            found = await self._scanner.find_markets_for_keyword(market_ref, max_results=10)
            for market in found or []:
                if not isinstance(market, dict):
                    continue
                if (
                    str(market.get("id") or "") == market_ref
                    or str(market.get("conditionId") or "") == market_ref
                ) and self._is_viable_market(market):
                    return market
        except Exception as e:
            logger.debug(f"AISentiment: scanner fallback exact search failed for {market_ref}: {e}")

        return None

    def _is_stale_news(self, item: NewsItem) -> bool:
        published_at = item.published_at or datetime.now(timezone.utc)
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - published_at).total_seconds()
        return age_seconds > settings.SENTIMENT_MAX_NEWS_AGE_HOURS * 3600

    def _strategy_loss_cap_breached(self) -> bool:
        daily_pnl = self._db.get_strategy_daily_pnl(self.name)
        cap = settings.SENTIMENT_MAX_DAILY_LOSS
        if daily_pnl <= -cap:
            if not self._strategy_halted:
                self._strategy_halted = True
                logger.critical(
                    f"AISentiment halted — strategy daily loss cap breached "
                    f"({daily_pnl:.2f} <= -{cap:.2f})"
                )
            return True
        if self._strategy_halted and daily_pnl > -cap:
            self._strategy_halted = False
            logger.warning("AISentiment resumed — strategy daily loss back within limit")
        return False

    def _parse_market_end(self, market: dict) -> Optional[datetime]:
        end_str = market.get("endDate") or market.get("endDateIso") or ""
        if not end_str:
            return None
        try:
            normalized = end_str.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except Exception:
            return None

    def _market_in_resolution_window(self, market: dict) -> bool:
        end_dt = self._parse_market_end(market)
        if end_dt is None:
            return False
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        remaining = (end_dt - datetime.now(timezone.utc)).total_seconds()
        min_remaining = settings.SENTIMENT_MIN_RESOLUTION_MINUTES * 60
        max_remaining = settings.SENTIMENT_MAX_RESOLUTION_DAYS * 86400
        return min_remaining <= remaining <= max_remaining

    def _tokenize(self, text: str) -> set[str]:
        return {token for token in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(token) > 2}

    def _query_entries_for_item(
        self,
        theme_config: dict,
        item: NewsItem,
        analysis: dict,
    ) -> list[dict]:
        strict_queries = theme_config.get("strict_market_queries")
        if isinstance(strict_queries, list):
            raw_entries = strict_queries
        else:
            raw_entries = [{"query": query} for query in theme_config.get("market_queries", [])]

        headline_text = f"{item.title} {item.summary}".lower()
        analyzer_tags_text = " ".join(str(tag or "") for tag in analysis.get("market_tags", [])).lower()
        catalyst_class = str(analysis.get("catalyst_class", "noise")).strip().lower()
        allowed_defaults = self._allowed_catalysts(theme_config)

        entries: list[dict] = []
        seen_queries: set[str] = set()
        for raw in raw_entries:
            if isinstance(raw, str):
                entry = {"query": raw}
            elif isinstance(raw, dict):
                entry = dict(raw)
            else:
                continue

            query = str(entry.get("query") or "").strip()
            if not query:
                continue

            query_key = query.lower()
            if query_key in seen_queries:
                continue

            catalyst_types = entry.get("catalyst_types") or list(allowed_defaults)
            catalyst_types = {
                str(value).strip().lower() for value in catalyst_types if str(value).strip()
            }
            if catalyst_types and catalyst_class not in catalyst_types:
                continue

            action_tags = entry.get("action_tags") or []
            action_tags = [str(value).strip().lower() for value in action_tags if str(value).strip()]
            if action_tags:
                if not any(tag in headline_text or tag in analyzer_tags_text for tag in action_tags):
                    continue

            priority = float(entry.get("priority", 1.0))
            seen_queries.add(query_key)
            entries.append(
                {
                    "query": query,
                    "priority": priority,
                    "trade_type": str(entry.get("trade_type") or "").strip().lower() or None,
                    "action_tags": action_tags,
                    "catalyst_types": sorted(catalyst_types),
                }
            )

        entries.sort(key=lambda entry: entry["priority"], reverse=True)
        return entries[:5]

    def _score_market_candidate(
        self,
        market: dict,
        query: str,
        theme_config: dict,
        item: NewsItem,
    ) -> tuple:
        text = " ".join(
            str(market.get(k, "")) for k in ("question", "title", "description", "slug", "category")
        ).lower()
        query_l = query.lower()
        title_tokens = self._tokenize(item.title)
        theme_tokens = self._tokenize(" ".join(theme_config.get("keywords", [])))
        market_tokens = self._tokenize(text)
        exact_query = 1 if query_l and query_l in text else 0
        title_overlap = len(title_tokens & market_tokens)
        theme_overlap = len(theme_tokens & market_tokens)
        liquidity = float(market.get("liquidityNum") or market.get("liquidity") or 0.0)
        volume = float(market.get("volumeNum") or market.get("volume24hr") or market.get("volume") or 0.0)
        end_dt = self._parse_market_end(market)
        hours_to_resolve = 9999.0
        if end_dt is not None:
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            hours_to_resolve = max(
                0.0, (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            )
        resolution_score = -hours_to_resolve
        return (
            exact_query,
            title_overlap,
            theme_overlap,
            self._market_in_resolution_window(market),
            liquidity,
            volume,
            resolution_score,
        )

    async def _discover_market_candidates(
        self,
        theme: str,
        theme_config: dict,
        item: NewsItem,
        analysis: dict,
    ) -> list[dict]:
        explicit_ids = [str(value).strip() for value in theme_config.get("market_ids", []) if str(value).strip()]
        resolved: list[dict] = []
        seen: set[str] = set()

        for market_ref in explicit_ids:
            market = await self._get_market_by_ref(market_ref)
            market_key = self._market_identifier(market) if market else None
            if market and market_key and market_key not in seen and self._is_viable_market(market):
                seen.add(market_key)
                resolved.append(market)

        if resolved:
            return resolved

        scored: dict[str, tuple[tuple, dict]] = {}
        for query_entry in self._query_entries_for_item(theme_config, item, analysis):
            query = query_entry["query"]
            for market in await self._search_live_markets(query, max_results=8):
                market_key = self._market_identifier(market)
                if not market_key:
                    continue
                if not self._market_in_resolution_window(market):
                    continue

                liquidity = float(market.get("liquidityNum") or market.get("liquidity") or 0.0)
                volume = float(market.get("volumeNum") or market.get("volume24hr") or market.get("volume") or 0.0)
                if liquidity < settings.SENTIMENT_MIN_LIQUIDITY:
                    continue
                if volume < settings.SENTIMENT_MIN_VOLUME_24H:
                    continue

                candidate_score = (
                    query_entry["priority"],
                    *self._score_market_candidate(market, query, theme_config, item),
                )
                existing = scored.get(market_key)
                if existing is None or candidate_score > existing[0]:
                    enriched = dict(market)
                    enriched["_sentiment_query_entry"] = query_entry
                    scored[market_key] = (candidate_score, enriched)

        # LLM-generated queries: ask Claude to derive additional search terms from the
        # raw headline, independent of theme config. This catches markets the YAML
        # queries would never find (new topics, unexpected framings, crypto/econ angles).
        try:
            llm_queries = await self._analyzer.generate_market_queries(item)
            if llm_queries:
                logger.debug(f"AISentiment LLM queries for [{theme}]: {llm_queries}")
            for query in llm_queries:
                for market in await self._search_live_markets(query, max_results=5):
                    market_key = self._market_identifier(market)
                    if not market_key:
                        continue
                    if not self._market_in_resolution_window(market):
                        continue
                    liquidity = float(market.get("liquidityNum") or market.get("liquidity") or 0.0)
                    volume = float(market.get("volumeNum") or market.get("volume24hr") or market.get("volume") or 0.0)
                    if liquidity < settings.SENTIMENT_MIN_LIQUIDITY:
                        continue
                    if volume < settings.SENTIMENT_MIN_VOLUME_24H:
                        continue
                    # LLM queries get a slightly lower base priority (0.70) so they rank
                    # below explicit config queries but above nothing.
                    llm_entry = {
                        "query": query,
                        "priority": 0.70,
                        "trade_type": None,
                        "action_tags": [],
                        "catalyst_types": [],
                    }
                    candidate_score = (
                        llm_entry["priority"],
                        *self._score_market_candidate(market, query, theme_config, item),
                    )
                    existing = scored.get(market_key)
                    if existing is None or candidate_score > existing[0]:
                        enriched = dict(market)
                        enriched["_sentiment_query_entry"] = llm_entry
                        enriched["_llm_discovered"] = True
                        scored[market_key] = (candidate_score, enriched)
        except Exception as exc:
            logger.debug(f"AISentiment: LLM query generation error for [{theme}]: {exc}")

        ranked = sorted(scored.values(), key=lambda item: item[0], reverse=True)
        return [market for _, market in ranked]

    def _allowed_catalysts(self, theme_config: dict) -> set[str]:
        values = theme_config.get("allowed_catalyst_classes") or list(self._ALLOWED_CATALYST_CLASSES)
        return {str(value).strip().lower() for value in values if str(value).strip()}

    def _is_tradeable_catalyst(self, theme_config: dict, analysis: dict) -> bool:
        catalyst_class = str(analysis.get("catalyst_class", "noise")).strip().lower()
        # In dry-run data-collection mode allow commentary/analysis — we want signal data
        # even from soft catalysts. Noise and speculation are still blocked.
        if settings.DRY_RUN and catalyst_class in {"commentary", "analysis"}:
            return True
        return catalyst_class in self._allowed_catalysts(theme_config)

    def _classify_market_kinds(self, text: str) -> set[str]:
        text_l = (text or "").lower()
        matched = {
            kind for kind, keywords in self._MARKET_KIND_KEYWORDS.items()
            if any(keyword in text_l for keyword in keywords)
        }
        return matched or {"general"}

    def _extract_time_tokens(self, text: str) -> set[str]:
        tokens = self._tokenize(text)
        return {token for token in tokens if token in self._TIME_TOKENS or token.isdigit()}

    def _market_mapping_details(
        self,
        item: NewsItem,
        analysis: dict,
        market: dict,
        theme_config: dict,
        query_entry: Optional[dict] = None,
    ) -> dict:
        headline_text = f"{item.title} {item.summary}".lower()
        market_text = " ".join(
            str(market.get(key, "")) for key in ("question", "title", "description", "slug")
        ).lower()
        headline_tokens = self._tokenize(headline_text) - self._GENERIC_MATCH_TOKENS
        market_tokens = self._tokenize(market_text)
        specific_overlap = len(headline_tokens & market_tokens)

        query_phrase_match = 0
        for query in theme_config.get("market_queries", []):
            query_tokens = self._tokenize(str(query or ""))
            if query_tokens and len(query_tokens & headline_tokens) >= 2:
                query_phrase_match = 1
                break

        tag_overlap = 0
        for tag in analysis.get("market_tags", []):
            tag_tokens = self._tokenize(str(tag or ""))
            if tag_tokens and tag_tokens & market_tokens:
                tag_overlap += 1

        headline_kinds = self._classify_market_kinds(headline_text)
        market_kinds = self._classify_market_kinds(market_text)
        action_match = 1 if headline_kinds & market_kinds else 0

        headline_times = self._extract_time_tokens(headline_text)
        market_times = self._extract_time_tokens(market_text)
        if headline_times and market_times:
            timing_match = 1 if headline_times & market_times else -1
        else:
            timing_match = 0

        action_tag_match = 0
        if query_entry:
            action_tags = [str(value).strip().lower() for value in query_entry.get("action_tags", []) if str(value).strip()]
            if action_tags:
                headline_has_action = any(tag in headline_text for tag in action_tags)
                market_has_action = any(tag in market_text for tag in action_tags)
                if headline_has_action and market_has_action:
                    action_tag_match = 1
                elif headline_has_action and not market_has_action:
                    action_tag_match = -1

        score = (
            (query_phrase_match * 2)
            + specific_overlap
            + tag_overlap
            + action_match
            + max(timing_match, 0)
            + max(action_tag_match, 0)
        )
        is_strong = query_phrase_match == 1 and action_match == 1 and specific_overlap >= 1 and timing_match >= 0
        if not is_strong and action_match == 1 and specific_overlap >= 2 and timing_match >= 0:
            is_strong = True
        # Dry-run: accept any action-matched market with overlap >= 1 regardless of timing,
        # so mismatched time tokens don't silently kill valid candidates.
        if not is_strong and settings.DRY_RUN and action_match == 1 and specific_overlap >= 1:
            is_strong = True
        # action_tag_match == -1 means the headline has action keywords but the market text
        # doesn't — that's a genuine mismatch; enforce it in live mode only.
        if query_entry and query_entry.get("action_tags") and not settings.DRY_RUN:
            is_strong = is_strong and action_tag_match >= 0

        return {
            "score": score,
            "query_phrase_match": query_phrase_match,
            "specific_overlap": specific_overlap,
            "tag_overlap": tag_overlap,
            "action_match": action_match,
            "action_tag_match": action_tag_match,
            "timing_match": timing_match,
            "headline_kinds": sorted(headline_kinds),
            "market_kinds": sorted(market_kinds),
            "is_strong": is_strong,
        }

    def _determine_trade_profile(
        self,
        market: dict,
        analysis: dict,
        query_entry: Optional[dict] = None,
    ) -> dict:
        base_time_stop = float(settings.SENTIMENT_TIME_STOP_MINUTES)
        end_dt = self._parse_market_end(market)
        hours_to_resolve = 0.0
        if end_dt is not None:
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            hours_to_resolve = max(
                0.0,
                (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600,
            )

        forced_trade_type = str((query_entry or {}).get("trade_type") or "").strip().lower()
        if forced_trade_type == "event":
            return {
                "trade_type": "event",
                "time_stop_minutes": max(base_time_stop, 180.0),
            }
        if forced_trade_type == "reaction":
            return {
                "trade_type": "reaction",
                "time_stop_minutes": min(base_time_stop, 60.0),
            }

        catalyst_class = str(analysis.get("catalyst_class", "noise")).lower()
        if catalyst_class == "policy_action" or hours_to_resolve > 72:
            return {
                "trade_type": "event",
                "time_stop_minutes": max(base_time_stop, 180.0),
            }
        return {
            "trade_type": "reaction",
            "time_stop_minutes": min(base_time_stop, 60.0),
        }

    def _compute_position_size(
        self,
        analysis: dict,
        market: dict,
        raw_edge: float,
        min_edge: float,
    ) -> tuple[float, dict]:
        base_size = settings.SENTIMENT_POSITION_SIZE
        liquidity = float(market.get("liquidityNum") or market.get("liquidity") or 0.0)
        volume = float(market.get("volumeNum") or market.get("volume24hr") or market.get("volume") or 0.0)
        conf = max(0.0, min(1.0, float(analysis.get("confidence", 0.0))))
        urg = max(0.0, min(1.0, float(analysis.get("urgency", 0.0))))
        edge_score = min(raw_edge / max(min_edge, 1e-6), 3.0) / 3.0
        liq_score = min(liquidity / max(settings.SENTIMENT_MIN_LIQUIDITY, 1.0), 2.0) / 2.0
        vol_score = min(volume / max(settings.SENTIMENT_MIN_VOLUME_24H, 1.0), 2.0) / 2.0

        quality = (
            0.40 * edge_score
            + 0.25 * conf
            + 0.20 * urg
            + 0.10 * liq_score
            + 0.05 * vol_score
        )
        # Scale from 50% to 100% of SENTIMENT_POSITION_SIZE — never exceed it
        multiplier = 0.50 + (quality * 0.50)
        size_usdc = min(base_size * multiplier, base_size, settings.MAX_POSITION_SIZE_USDC)

        # FIX 9: Kelly progressive sizing — cap based on proven track record.
        # Until the strategy demonstrates positive expected value (PF > 1.2 over 50+ trades),
        # cap positions conservatively. This prevents large losses during the calibration period.
        kelly_cap = self._get_kelly_cap()
        if size_usdc > kelly_cap:
            logger.debug(
                f"AISentiment sizing: Kelly cap ${kelly_cap:.0f} < computed ${size_usdc:.2f} — capping"
            )
            size_usdc = kelly_cap
            multiplier = size_usdc / base_size if base_size > 0 else multiplier

        details = {
            "base_size": base_size,
            "multiplier": multiplier,
            "quality": quality,
            "liquidity": liquidity,
            "volume": volume,
            "kelly_cap": kelly_cap,
        }
        return size_usdc, details

    # ── Change 1: Price trend filter ─────────────────────────────────────────────
    async def _get_yes_price_trend(self, yes_token_id: str, window_hours: float = 2.0) -> str:
        """
        Fetch the last `window_hours` of price history for a YES token and return:
          "up"   — price has risen >= 6% over the window (bullish momentum)
          "down" — price has fallen >= 6% over the window (bearish momentum)
          "flat" — no significant move
        Returns "flat" on any API error so a bad fetch never blocks a trade.
        """
        try:
            url = f"{settings.CLOB_BASE_URL}/prices-history"
            data = await self._fetch_json(url, params={
                "market": yes_token_id,
                "interval": "1h",
                "fidelity": 60,
            })
            history = data.get("history", []) if isinstance(data, dict) else []
            if len(history) < 2:
                return "flat"
            # Keep only ticks within the window
            now_ts = time.time()
            cutoff = now_ts - window_hours * 3600
            window = [p for p in history if isinstance(p, dict) and float(p.get("t", 0)) >= cutoff]
            if len(window) < 2:
                return "flat"
            start_price = float(window[0].get("p", 0.5))
            end_price = float(window[-1].get("p", 0.5))
            change = end_price - start_price
            threshold = 0.06  # 6% move counts as a significant trend
            if change >= threshold:
                return "up"
            if change <= -threshold:
                return "down"
            return "flat"
        except Exception as e:
            logger.debug(f"AISentiment: price trend fetch failed for {yes_token_id[:12]}: {e}")
            return "flat"

    # ── FIX 9: Kelly progressive sizing helpers ───────────────────────────────────

    def _get_current_pf(self) -> Optional[float]:
        """Compute current profit factor from strategy's closed trades (last 20). Returns None if <10 trades."""
        try:
            all_trades = self._db.get_recent_trades(limit=20)
            my_trades = [t for t in all_trades if t.get("strategy") == self.name and t.get("pnl") is not None]
            if len(my_trades) < 10:
                return None
            wins = sum(t["pnl"] for t in my_trades if t["pnl"] > 0)
            losses = abs(sum(t["pnl"] for t in my_trades if t["pnl"] < 0))
            return wins / losses if losses > 0 else (1.5 if wins > 0 else None)
        except Exception:
            return None

    def _get_kelly_cap(self) -> float:
        """
        Return the Kelly-based position size cap for current trade.
        Refreshes from DB every 30 min; uses cached value between refreshes.
        - < 20 closed trades: $8 max (calibration phase)
        - 20-50 trades, PF > 1.1: $15 max
        - 50+ trades, PF > 1.2: full SENTIMENT_POSITION_SIZE
        - PF <= 1.0: stay at $8 regardless of trade count
        """
        now = time.time()
        if now - self._kelly_cap_last_refresh < self._kelly_cap_refresh_interval:
            return self._kelly_cap_usdc

        self._kelly_cap_last_refresh = now
        try:
            all_trades = self._db.get_recent_trades(limit=20)
            my_trades = [t for t in all_trades if t.get("strategy") == self.name and t.get("pnl") is not None]
            n = len(my_trades)
            wins = sum(t["pnl"] for t in my_trades if t["pnl"] > 0)
            losses = abs(sum(t["pnl"] for t in my_trades if t["pnl"] < 0))
            pf = wins / losses if losses > 0 else (1.5 if wins > 0 else 0.0)

            if n < 20:
                cap = 8.0
            elif n < 50:
                cap = 15.0 if pf > 1.1 else 8.0
            else:
                if pf > 1.2:
                    cap = float(settings.SENTIMENT_POSITION_SIZE)
                elif pf > 1.0:
                    cap = 15.0
                else:
                    cap = 8.0

            if cap != self._kelly_cap_usdc:
                logger.info(
                    f"AISentiment Kelly cap updated: ${self._kelly_cap_usdc:.0f} → ${cap:.0f} "
                    f"(n={n} trades, PF={pf:.2f})"
                )
            self._kelly_cap_usdc = cap
        except Exception:
            pass  # keep prior cached value on error
        return self._kelly_cap_usdc

    # ── FIX 5: Direct crypto math evaluation ──────────────────────────────────────

    def _try_direct_crypto_eval(
        self,
        question: str,
        current_price: float,
        crypto_prices: dict,
    ) -> Optional[dict]:
        """
        Evaluate "Will X be above/below $Y on date Z?" markets mathematically using
        live Binance prices — no LLM call, no hallucination.
        Returns an analysis dict (same shape as evaluate_market_pricing) or None if
        the question doesn't match a pattern we can evaluate directly.
        """
        import re as _re
        q_lower = question.lower()

        asset_map = [
            ("BTC", ["bitcoin", "btc"]),
            ("ETH", ["ethereum", "eth"]),
            ("SOL", ["solana", "sol"]),
        ]

        for asset, aliases in asset_map:
            live_price = crypto_prices.get(asset, 0)
            if not live_price:
                continue
            if not any(a in q_lower for a in aliases):
                continue

            # ── "between $X and $Y" range markets ────────────────────────────
            is_between = "between" in q_lower
            if is_between:
                prices_found = _re.findall(r'\$([\d,]+(?:\.\d+)?)', question)
                if len(prices_found) >= 2:
                    try:
                        lo = float(prices_found[0].replace(",", ""))
                        hi = float(prices_found[1].replace(",", ""))
                        if lo > hi:
                            lo, hi = hi, lo
                        in_range = lo <= live_price <= hi
                        # Edge only when live price is clearly outside range (>5% from boundary)
                        if live_price < lo * 0.95:
                            fair_prob = max(0.05, 0.10 - (lo - live_price) / lo)
                        elif live_price > hi * 1.05:
                            fair_prob = max(0.05, 0.10 - (live_price - hi) / hi)
                        elif in_range:
                            fair_prob = min(0.90, 0.70 + min(live_price - lo, hi - live_price) / (hi - lo) * 0.2)
                        else:
                            continue  # too close to boundary — no edge
                        implied_shift = fair_prob - current_price
                        if abs(implied_shift) < 0.08:
                            continue
                        direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
                        return {
                            "is_relevant": True,
                            "direction": direction,
                            "implied_probability_shift": implied_shift,
                            "fair_probability": fair_prob,
                            "confidence": 0.82,
                            "urgency": 0.75,
                            "impact_strength": 0.85,
                            "reasoning_short": f"{asset}=${live_price:,.0f} range=[${lo:,.0f},${hi:,.0f}] in_range={in_range}",
                            "analyzer_name": "direct_crypto_math",
                            "catalyst_class": "confirmed_event",
                            "market_tags": [asset.lower()],
                        }
                    except (ValueError, ZeroDivisionError):
                        continue

            # ── "above/below $X" simple threshold markets ─────────────────────
            # Extract target price — match $80,000 or $80000 patterns
            target_match = _re.search(r'\$[\d,]+(?:\.\d+)?', question)
            if not target_match:
                continue
            try:
                target = float(target_match.group(0).replace("$", "").replace(",", ""))
            except ValueError:
                continue
            if target <= 0:
                continue

            is_above = any(w in q_lower for w in ("above", "over", "exceed", "higher than", "reach"))
            is_below = any(w in q_lower for w in ("below", "under", "less than", "lower than"))
            if not is_above and not is_below:
                continue

            dist = (live_price - target) / target  # positive = live price is above target

            if is_above:
                if dist > 0.08:   # >8% above → very likely YES
                    fair_prob = min(0.95, 0.75 + dist * 0.8)
                elif dist < -0.08:  # >8% below → very likely NO
                    fair_prob = max(0.05, 0.25 + dist * 0.8)
                else:
                    continue  # too close to target — LLM can handle
            else:  # is_below
                if dist < -0.08:  # >8% below → very likely YES (below target)
                    fair_prob = min(0.95, 0.75 + (-dist) * 0.8)
                elif dist > 0.08:   # >8% above → very likely NO
                    fair_prob = max(0.05, 0.25 + (-dist) * 0.8)
                else:
                    continue

            implied_shift = fair_prob - current_price
            if abs(implied_shift) < 0.05:
                continue

            direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
            return {
                "is_relevant": True,
                "direction": direction,
                "implied_probability_shift": implied_shift,
                "fair_probability": fair_prob,
                "confidence": 0.90,
                "urgency": min(1.0, 0.8),
                "impact_strength": 0.9,
                "reasoning_short": f"{asset}=${live_price:,.0f} vs target=${target:,.0f} ({'above' if is_above else 'below'})",
                "analyzer_name": "direct_crypto_math",
                "catalyst_class": "confirmed_event",
                "market_tags": [asset.lower()],
            }
        return None

    # ── FIX 6: Direct sports score evaluation ────────────────────────────────────

    async def _try_direct_sports_eval(
        self,
        question: str,
        current_price: float,
    ) -> Optional[dict]:
        """
        Check live + finished sports scores via ESPN free API.

        Path A — Finished games: game just ended, trade near-certainty (conf=0.92).
        Path B — Live games:     decisive lead in final minutes, trade before market
                                 fully prices in the outcome (conf=0.85–0.93).

        Returns analysis dict or None.
        """
        from data.sports_scores import get_finished_game_winners, get_live_game_leaders

        q_lower = question.lower()
        is_winner_market = any(w in q_lower for w in (
            "win", "advance", "beat", "defeat", "champion", "winner"
        ))
        is_loser_market = any(w in q_lower for w in (
            "lose", "eliminated", "out", "fail"
        ))
        if not is_winner_market and not is_loser_market:
            return None

        try:
            # ── Path A: finished games (highest confidence) ───────────────────────
            winners = await get_finished_game_winners()
            for team, outcome in winners.items():
                if team.lower() not in q_lower:
                    continue
                if outcome == "won":
                    fair_prob = 0.95 if is_winner_market else 0.05
                else:
                    fair_prob = 0.05 if is_winner_market else 0.95

                implied_shift = fair_prob - current_price
                if abs(implied_shift) < 0.08:
                    continue

                direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
                logger.info(
                    f"AISentiment direct_sports (finished): {team} {outcome} → "
                    f"fair={fair_prob:.2f} on '{question[:50]}'"
                )
                return {
                    "is_relevant": True,
                    "direction": direction,
                    "implied_probability_shift": implied_shift,
                    "fair_probability": fair_prob,
                    "confidence": 0.92,
                    "urgency": 1.0,
                    "impact_strength": 1.0,
                    "reasoning_short": f"{team} {outcome} — game finished",
                    "analyzer_name": "direct_sports_score",
                    "catalyst_class": "confirmed_event",
                    "market_tags": ["sports", team.lower()],
                }
        except Exception as e:
            logger.debug(f"AISentiment direct sports eval (finished) error: {e}")

        try:
            # ── Path B: live games with decisive lead ─────────────────────────────
            leaders = await get_live_game_leaders()
            for team, info in leaders.items():
                if team.lower() not in q_lower:
                    continue

                conf = info["confidence"]
                detail = info["detail"]
                is_leading = info["status"] == "likely_winning"

                # Map leading/trailing → YES/NO probability
                if is_leading:
                    fair_prob = conf if is_winner_market else (1.0 - conf)
                else:
                    fair_prob = (1.0 - conf) if is_winner_market else conf

                implied_shift = fair_prob - current_price
                # Require larger edge for live games (not yet confirmed)
                if abs(implied_shift) < 0.10:
                    continue

                direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
                verb = "leading" if is_leading else "trailing"
                logger.info(
                    f"AISentiment direct_sports (live): {team} {verb} [{detail}] → "
                    f"fair={fair_prob:.2f} on '{question[:50]}'"
                )
                return {
                    "is_relevant": True,
                    "direction": direction,
                    "implied_probability_shift": implied_shift,
                    "fair_probability": fair_prob,
                    "confidence": conf,
                    "urgency": 0.9,
                    "impact_strength": 0.9,
                    "reasoning_short": f"{team} {verb} — {detail}",
                    "analyzer_name": "direct_sports_live",
                    "catalyst_class": "live_game_state",
                    "market_tags": ["sports", team.lower()],
                }
        except Exception as e:
            logger.debug(f"AISentiment direct sports eval (live) error: {e}")

        return None

    # ── FIX 7: Direct weather forecast evaluation ─────────────────────────────

    async def _try_direct_weather_eval(
        self,
        question: str,
        current_price: float,
    ) -> Optional[dict]:
        """
        Evaluate weather markets using Open-Meteo free API (no key).
        Parses city + date + condition from the question, fetches the forecast,
        and computes a fair probability — no LLM call.
        """
        import math
        import re
        from data.weather import find_city, parse_date_from_text, get_city_forecast

        q_lower = question.lower()

        city_key = find_city(q_lower)
        if not city_key:
            return None

        is_rain = any(w in q_lower for w in ("rain", "precipitation", "wet", "downpour", "shower"))
        is_snow = any(w in q_lower for w in ("snow", "snowfall", "blizzard"))
        is_temp = any(w in q_lower for w in (
            "°f", "°c", "temperature", "degrees", "high", "heat",
            "freeze", "freezing", "cold", "warm",
        ))

        if not (is_rain or is_snow or is_temp):
            return None

        target_date = parse_date_from_text(question)
        if not target_date:
            return None

        forecast = await get_city_forecast(city_key, target_date)
        if forecast is None:
            return None

        fair_prob: Optional[float] = None
        detail = ""

        if is_rain and not is_temp:
            pct = forecast["precip_prob_pct"]
            fair_prob = pct / 100.0
            detail = f"{city_key.title()} {target_date}: {pct:.0f}% rain forecast"

        elif is_snow and not is_temp:
            snow_mm = forecast["snowfall_mm"]
            if snow_mm >= 5:
                fair_prob = 0.88
            elif snow_mm >= 1:
                fair_prob = 0.72
            else:
                fair_prob = 0.07
            detail = f"{city_key.title()} {target_date}: {snow_mm:.1f}mm snow forecast"

        elif is_temp:
            # Extract temperature threshold (e.g. "95°F", "35 degrees")
            m = re.search(r'(\d+)\s*°?\s*([fc])\b', q_lower)
            if not m:
                m2 = re.search(r'(\d+)\s*degrees', q_lower)
                if not m2:
                    return None
                threshold_c = (float(m2.group(1)) - 32) * 5 / 9  # assume °F
            else:
                threshold = float(m.group(1))
                threshold_c = (threshold - 32) * 5 / 9 if m.group(2) == 'f' else threshold

            is_above = any(w in q_lower for w in ("exceed", "above", "over", "reach", "high", "at least"))
            is_below = any(w in q_lower for w in ("below", "under", "freezing", "freeze", "low", "cold"))

            if is_above:
                fc = forecast["temp_max_c"]
                # Sigmoid: +5°C above threshold → ~87%, -5°C → ~13%
                fair_prob = 1.0 / (1.0 + math.exp(-(fc - threshold_c) / 2.5))
                detail = f"{city_key.title()} {target_date}: forecast max {fc:.1f}°C vs threshold {threshold_c:.1f}°C"
            elif is_below:
                fc = forecast["temp_min_c"]
                fair_prob = 1.0 / (1.0 + math.exp(-(threshold_c - fc) / 2.5))
                detail = f"{city_key.title()} {target_date}: forecast min {fc:.1f}°C vs threshold {threshold_c:.1f}°C"
            else:
                return None

        if fair_prob is None:
            return None

        # Skip near-50/50 forecasts — weather is uncertain in that band
        if 0.38 <= fair_prob <= 0.62:
            return None

        implied_shift = fair_prob - current_price
        if abs(implied_shift) < 0.08:
            return None

        conf = min(0.82, abs(fair_prob - 0.5) * 1.8)
        direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
        logger.info(
            f"AISentiment direct_weather: {city_key} {target_date} "
            f"fair={fair_prob:.2f} on '{question[:50]}'"
        )
        return {
            "is_relevant": True,
            "direction": direction,
            "implied_probability_shift": implied_shift,
            "fair_probability": fair_prob,
            "confidence": conf,
            "urgency": 0.80,
            "impact_strength": 0.80,
            "reasoning_short": detail,
            "analyzer_name": "direct_weather",
            "catalyst_class": "data_forecast",
            "market_tags": ["weather", city_key.replace(" ", "_")],
        }

    # ── FIX 8: Direct Fed rate futures evaluation ─────────────────────────────

    async def _try_direct_econ_eval(
        self,
        question: str,
        current_price: float,
    ) -> Optional[dict]:
        """
        Evaluate Fed rate cut/hike markets using Yahoo Finance 30-Day Fed Funds
        futures prices — no LLM call, no API key.
        """
        from data.econ_calendar import find_fomc_meeting, get_fed_cut_probability

        q_lower = question.lower()

        is_fed = any(w in q_lower for w in (
            "fed", "federal reserve", "fomc", "interest rate", "basis point",
        ))
        is_cut = any(w in q_lower for w in (
            "cut", "lower", "reduce", "decrease", "rate cut", "rate reduction",
        ))
        is_hike = any(w in q_lower for w in (
            "hike", "raise", "increase", "rate hike", "rate increase",
        ))

        if not is_fed or not (is_cut or is_hike):
            return None

        meeting_date = find_fomc_meeting(q_lower)
        if not meeting_date:
            return None

        cut_prob = await get_fed_cut_probability(meeting_date)
        if cut_prob is None:
            return None

        fair_prob = cut_prob if is_cut else (1.0 - cut_prob)

        if 0.38 <= fair_prob <= 0.62:
            return None

        implied_shift = fair_prob - current_price
        if abs(implied_shift) < 0.08:
            return None

        conf = min(0.82, abs(fair_prob - 0.5) * 1.8)
        direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
        action = "cut" if is_cut else "hike"
        logger.info(
            f"AISentiment direct_econ: Fed {action} @ {meeting_date} "
            f"P={fair_prob:.2f} on '{question[:50]}'"
        )
        return {
            "is_relevant": True,
            "direction": direction,
            "implied_probability_shift": implied_shift,
            "fair_probability": fair_prob,
            "confidence": conf,
            "urgency": 0.75,
            "impact_strength": 0.75,
            "reasoning_short": f"Fed Funds futures: P({action} @ {meeting_date})={fair_prob:.2f}",
            "analyzer_name": "direct_fed_futures",
            "catalyst_class": "data_forecast",
            "market_tags": ["fed", "interest_rate", "fomc"],
        }

    # ── FIX 9: Direct election polling evaluation ─────────────────────────────

    async def _try_direct_polling_eval(
        self,
        question: str,
        current_price: float,
    ) -> Optional[dict]:
        """
        Evaluate election markets by comparing Polymarket price against Metaculus
        crowd forecasts (free public API, no key).
        Only fires when Metaculus has ≥20 forecasters and ≥40% word overlap.
        """
        import re
        from data.polling import get_election_probability

        q_lower = question.lower()

        is_election_market = any(w in q_lower for w in (
            "election", "win", "wins", "won", "seats", "majority",
            "prime minister", "president", "chancellor", "parliament",
            "vote", "ballot", "party", "candidate",
        ))
        if not is_election_market:
            return None

        manifold_prob = await get_election_probability(question)
        if manifold_prob is None:
            return None

        # Wider dead-zone for polling — uncertainty is higher than direct math
        if 0.35 <= manifold_prob <= 0.65:
            return None

        implied_shift = manifold_prob - current_price
        if abs(implied_shift) < 0.10:
            return None

        conf = min(0.78, abs(manifold_prob - 0.5) * 1.6)
        direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
        logger.info(
            f"AISentiment direct_polling: Manifold P={manifold_prob:.2f} "
            f"vs market {current_price:.2f} on '{question[:50]}'"
        )
        return {
            "is_relevant": True,
            "direction": direction,
            "implied_probability_shift": implied_shift,
            "fair_probability": manifold_prob,
            "confidence": conf,
            "urgency": 0.70,
            "impact_strength": 0.70,
            "reasoning_short": f"Manifold Markets crowd: P={manifold_prob:.2f}",
            "analyzer_name": "direct_polling",
            "catalyst_class": "data_forecast",
            "market_tags": ["election", "politics", "polling"],
        }

    # ── FIX 10: Kalshi real-money crowd evaluation (US politics) ─────────────────

    async def _try_direct_kalshi_eval(
        self,
        question: str,
        current_price: float,
    ) -> Optional[dict]:
        """
        Evaluate US politics markets using Kalshi real-money crowd prices.
        Kalshi is a CFTC-regulated real-money US prediction exchange — highest
        signal quality available for free without auth. Covers Trump approval,
        tariffs, Congress votes, Fed decisions, elections, macro events.

        Requirements:
          - ≥ 35% word overlap between question and Kalshi title
          - ≥ 0.10 edge vs Polymarket price
          - Dead zone 0.38–0.62 — skip toss-up markets
          - Max confidence 0.78
        """
        from data.kalshi import get_kalshi_probability

        q_lower = question.lower()

        is_politics_market = any(w in q_lower for w in (
            "trump", "president", "congress", "senate", "house", "republican",
            "democrat", "approval", "impeach", "veto", "legislation", "bill",
            "tariff", "executive order", "cabinet", "nominee", "fed chair",
            "supreme court", "election", "vote", "ballot", "primary", "speaker",
            "shutdown", "debt ceiling", "regulation",
        ))
        if not is_politics_market:
            return None

        crowd_prob = await get_kalshi_probability(question)
        if crowd_prob is None:
            return None

        if 0.38 <= crowd_prob <= 0.62:
            return None

        implied_shift = crowd_prob - current_price
        if abs(implied_shift) < 0.10:
            return None

        conf = min(0.78, abs(crowd_prob - 0.5) * 1.56)
        direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
        logger.info(
            f"AISentiment direct_kalshi: P={crowd_prob:.2f} "
            f"vs market {current_price:.2f} on '{question[:50]}'"
        )
        return {
            "is_relevant": True,
            "direction": direction,
            "implied_probability_shift": implied_shift,
            "fair_probability": crowd_prob,
            "confidence": conf,
            "urgency": 0.70,
            "impact_strength": 0.72,
            "reasoning_short": f"Kalshi real-money crowd: P={crowd_prob:.2f}",
            "analyzer_name": "direct_kalshi",
            "catalyst_class": "data_forecast",
            "market_tags": ["politics", "trump", "congress", "kalshi"],
        }

    # ── FIX 11: FRED economic data evaluation ─────────────────────────────────────

    async def _try_direct_fred_eval(
        self,
        question: str,
        current_price: float,
    ) -> Optional[dict]:
        """
        Evaluate US economic indicator markets using real FRED data.
        Covers CPI/inflation, unemployment, 10-year yield, nonfarm payrolls.

        "Will CPI exceed 3.5%?" → fetches actual CPI YoY from FRED → math answer.
        "Will unemployment rise above 4.5%?" → fetches actual UNRATE → math answer.

        This upgrades us_economy beyond Fed rate futures — now covers any economic
        indicator market with a numeric threshold.

        Requirements:
          - ≥ 0.08 edge
          - Dead zone 0.38–0.62
          - Max confidence 0.80
        """
        from data.fred import evaluate_econ_question

        q_lower = question.lower()

        # Must look like an economic indicator question with a numeric threshold
        is_econ_indicator = any(w in q_lower for w in (
            "cpi", "inflation", "consumer price",
            "unemployment", "jobless",
            "10-year", "10 year", "treasury yield",
            "nonfarm", "payroll", "jobs added",
        ))
        if not is_econ_indicator:
            return None

        fair_prob, reasoning = await evaluate_econ_question(question, current_price)
        if fair_prob is None:
            return None

        if 0.38 <= fair_prob <= 0.62:
            return None

        implied_shift = fair_prob - current_price
        if abs(implied_shift) < 0.08:
            return None

        conf = min(0.80, abs(fair_prob - 0.5) * 1.7)
        direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
        logger.info(
            f"AISentiment direct_fred: P={fair_prob:.2f} "
            f"vs market {current_price:.2f} — {reasoning}"
        )
        return {
            "is_relevant": True,
            "direction": direction,
            "implied_probability_shift": implied_shift,
            "fair_probability": fair_prob,
            "confidence": conf,
            "urgency": 0.70,
            "impact_strength": 0.75,
            "reasoning_short": reasoning or f"FRED data: P={fair_prob:.2f}",
            "analyzer_name": "direct_fred",
            "catalyst_class": "data_forecast",
            "market_tags": ["economics", "fed", "inflation", "unemployment"],
        }

    # ── FIX 12: Direct geopolitical crowd evaluation (Manifold) ──────────────────

    async def _try_direct_geo_eval(
        self,
        question: str,
        current_price: float,
    ) -> Optional[dict]:
        """
        Evaluate geopolitical markets (Russia-Ukraine, ceasefire, nuclear deals, etc.)
        using Manifold Markets crowd probability — same mechanism as elections but with
        stricter thresholds because conflict markets are harder to predict.

        Requirements (stricter than elections):
          - ≥ 30 Manifold bettors (vs 15 for elections) — needs larger crowd
          - ≥ 35% word overlap — conflict questions use different wording
          - ≥ 0.12 edge (vs 0.10 for elections)
          - Dead zone 0.35–0.65 — skip murky 50/50 markets
          - Max confidence 0.70 (vs 0.78 for elections)

        If Manifold has no matching market, returns None — no LLM fallback.
        """
        from data.polling import get_manifold_probability

        q_lower = question.lower()

        # Must look like a geopolitical outcome market
        is_geo_market = any(w in q_lower for w in (
            "ceasefire", "peace", "deal", "war", "conflict", "nuclear",
            "treaty", "agreement", "invasion", "withdraw", "troops",
            "sanctions", "diplomatic", "summit", "talks", "negotiate",
            "occupation", "offensive", "liberation", "annexation",
        ))
        if not is_geo_market:
            return None

        # Stricter thresholds: 30 bettors, 35% overlap
        crowd_prob = await get_manifold_probability(
            question, min_bettors=30, min_overlap=0.35
        )
        if crowd_prob is None:
            return None

        # Wider dead-zone than elections — conflict outcomes are inherently uncertain
        if 0.35 <= crowd_prob <= 0.65:
            return None

        implied_shift = crowd_prob - current_price
        if abs(implied_shift) < 0.12:  # stricter edge requirement
            return None

        # Lower confidence cap than elections — geopolitical events are less predictable
        conf = min(0.70, abs(crowd_prob - 0.5) * 1.4)
        direction = "increase_yes" if implied_shift > 0 else "decrease_yes"
        logger.info(
            f"AISentiment direct_geo: Manifold P={crowd_prob:.2f} "
            f"vs market {current_price:.2f} on '{question[:50]}'"
        )
        return {
            "is_relevant": True,
            "direction": direction,
            "implied_probability_shift": implied_shift,
            "fair_probability": crowd_prob,
            "confidence": conf,
            "urgency": 0.65,
            "impact_strength": 0.65,
            "reasoning_short": f"Manifold crowd ({30}+ bettors): P={crowd_prob:.2f}",
            "analyzer_name": "direct_geo_crowd",
            "catalyst_class": "data_forecast",
            "market_tags": ["geopolitics", "conflict", "crowd_forecast"],
        }

    # ── Theme group map — themes in the same group share a cooldown ──────────────
    _THEME_GROUPS: dict[str, str] = {
        # All four Iran/ceasefire themes → same group
        "iran_conflict":    "middle_east",
        "middle_east_war":  "middle_east",
        "ceasefire_deal":   "middle_east",
        "iran_diplomacy":   "middle_east",
        "regional_escalation": "middle_east",
        # US politics sub-themes → same group
        "us_politics":      "us_politics",
        "global_elections": "us_politics",
        # Geopolitics group
        "geopolitics":      "geopolitics",
        "geopolitics_china": "geopolitics",
        # Economy group
        "us_economy":       "economics",
        # Crypto standalone
        "crypto_markets":   "crypto",
        # Sports standalone
        "sports_events":    "sports",
        # Tech standalone
        "tech_companies":   "tech",
        # Weather standalone
        "weather":          "weather",
        # Entertainment standalone
        "entertainment":    "entertainment",
    }
    # Seconds to block the entire group after any trade in it
    _THEME_GROUP_COOLDOWN_SECS: int = 3 * 3600  # 3 hours

    def _theme_group_of(self, theme: str) -> str:
        return self._THEME_GROUPS.get(theme, theme)

    def _theme_group_is_cooling(self, theme: str) -> bool:
        group = self._theme_group_of(theme)
        last_ts = self._theme_group_cooldowns.get(group, 0.0)
        return (time.time() - last_ts) < self._THEME_GROUP_COOLDOWN_SECS

    def _theme_group_touch(self, theme: str) -> None:
        group = self._theme_group_of(theme)
        self._theme_group_cooldowns[group] = time.time()

    # ── Change 2: Theme correlation cap ──────────────────────────────────────────
    def _theme_exposure(self, theme: str) -> tuple[int, float]:
        """
        Return (open_position_count, total_deployed_usdc) for positions in this theme.
        Parses `metadata_json` of each open sentiment position to extract the theme tag.
        """
        count = 0
        total_usdc = 0.0
        for pos in self._portfolio.all_positions():
            if pos.strategy != self.name:
                continue
            try:
                meta = json.loads(pos.metadata_json) if pos.metadata_json else {}
            except Exception:
                meta = {}
            if meta.get("theme") == theme:
                count += 1
                total_usdc += pos.current_price * pos.size
        return count, total_usdc

    async def _evaluate_market(
        self,
        news_id: Optional[int],
        theme: str,
        analysis: dict,
        market: dict,
        item: Optional[NewsItem] = None,
    ) -> None:
        """Evaluate a single market against the news analysis and decide whether to trade."""
        market_id = self._market_identifier(market) or ""
        question = market.get("question", "")
        if not market_id:
            return
        theme_config = self._theme_config.get(theme, {})
        if self._portfolio.has_position(market_id):
            logger.info(f"AISentiment SKIP already_in_position {market_id[:12]}…")
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "already_in_position"
            )
            return

        if self._strategy_loss_cap_breached():
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "strategy_loss_cap"
            )
            return

        # Market cooldown check
        cooldown_secs = settings.SENTIMENT_COOLDOWN_MINUTES * 60
        last_trade = self._market_cooldowns.get(market_id, 0)
        if time.time() - last_trade < cooldown_secs:
            remaining_mins = (cooldown_secs - (time.time() - last_trade)) / 60
            logger.info(
                f"AISentiment SKIP market_cooldown {market_id[:12]}… ({remaining_mins:.0f}m remaining)"
            )
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "market_cooldown"
            )
            return

        # ── Theme group cooldown ──────────────────────────────────────────────────
        # Blocks ALL themes in the same group for 3h after any entry.
        # Stops iran_conflict / middle_east_war / ceasefire_deal / iran_diplomacy
        # from being used interchangeably to re-enter the same correlated bet.
        if self._theme_group_is_cooling(theme):
            group = self._theme_group_of(theme)
            elapsed = time.time() - self._theme_group_cooldowns.get(group, 0.0)
            remaining_h = (self._THEME_GROUP_COOLDOWN_SECS - elapsed) / 3600
            logger.info(
                f"AISentiment SKIP group_cooldown [{group}] — "
                f"{remaining_h:.1f}h remaining (theme={theme} blocked)"
            )
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "group_cooldown"
            )
            return

        # ── Theme correlation cap ─────────────────────────────────────────────────
        # Limit exposure within a single theme: max 1 open position per theme.
        _theme_count, _theme_value = self._theme_exposure(theme)
        if _theme_count >= 1:
            logger.info(
                f"AISentiment SKIP theme_cap [{theme}] — "
                f"{_theme_count} position already open in this theme (max 1)"
            )
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "theme_cap"
            )
            return
        if _theme_value >= 50.0:
            logger.info(
                f"AISentiment SKIP theme_cap [{theme}] — "
                f"${_theme_value:.0f} already deployed in this theme (max $50)"
            )
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "theme_cap"
            )
            return

        if not market.get("acceptingOrders", True):
            logger.info(f"AISentiment SKIP market_closed {market_id[:12]}…")
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "market_closed"
            )
            return

        token_ids = extract_clob_token_ids(market)
        if len(token_ids) < 2:
            logger.info(f"AISentiment SKIP no_token_pair {market_id[:12]}…")
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "no_token_pair"
            )
            return
        yes_token_id, no_token_id = token_ids[0], token_ids[1]

        try:
            yes_price = self._client.get_midpoint(yes_token_id)
            no_price = self._client.get_midpoint(no_token_id)
        except Exception as e:
            logger.debug(f"AISentiment: market fetch error for {market_id}: {e}")
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "market_fetch_error"
            )
            return

        # Dead market filter: YES + NO midpoints should sum to ~1.0 for a healthy market.
        # A wide spread (sum > 1.10 or < 0.90) means low liquidity / dead orderbook.
        if yes_price and no_price:
            spread_sum = yes_price + no_price
            if spread_sum > 1.10 or spread_sum < 0.90:
                logger.info(
                    f"AISentiment SKIP dead_market spread={spread_sum:.3f} "
                    f"(yes={yes_price:.3f}+no={no_price:.3f}) {question[:40]}"
                )
                self._record_decision(
                    news_id, theme, market_id, question, yes_price, yes_price, 0,
                    "skip", "dead_market_spread"
                )
                return

        query_entry = market.get("_sentiment_query_entry") if isinstance(market, dict) else None
        is_proactive = bool(market.get("_proactive_scan")) if isinstance(market, dict) else False
        # Themes that use direct search queries don't need keyword-overlap validation —
        # the query string already guarantees relevance (e.g. "Apple earnings" → earnings markets)
        bypass_mapping = bool(theme_config.get("bypass_mapping", False))
        if item is not None and not is_proactive and not bypass_mapping:
            mapping = self._market_mapping_details(item, analysis, market, theme_config, query_entry)
            if not mapping["is_strong"]:
                logger.info(
                    f"AISentiment SKIP weak_mapping score={mapping['score']} "
                    f"overlap={mapping['specific_overlap']} action={mapping['action_match']} "
                    f"tag={mapping['action_tag_match']} "
                    f"for {question[:40]}"
                )
                self._record_decision(
                    news_id, theme, market_id, question, 0, 0, 0, "skip", "weak_mapping"
                )
                return

        # === FIX 1: Peace market direction flip ===
        effective_direction = analysis["direction"]
        effective_shift = analysis["implied_probability_shift"]
        if item is not None and effective_direction in ("increase_yes", "decrease_yes"):
            market_text_l = (question + " " + market.get("slug", "")).lower()
            news_text_l = (item.title + " " + item.summary).lower()
            is_peace_market = any(kw in market_text_l for kw in self._PEACE_MARKET_KEYWORDS)
            has_conflict_news = any(kw in news_text_l for kw in self._CONFLICT_NEWS_KEYWORDS)
            has_negotiation_news = any(kw in news_text_l for kw in self._NEGOTIATION_NEWS_KEYWORDS)
            if is_peace_market and has_conflict_news and not has_negotiation_news:
                flipped = "decrease_yes" if effective_direction == "increase_yes" else "increase_yes"
                logger.info(
                    f"AISentiment: peace market direction flipped — conflict news implies "
                    f"{flipped} on {market.get('slug', question[:30])}"
                )
                effective_direction = flipped
                effective_shift = -effective_shift

        if effective_direction == "increase_yes":
            token_id = yes_token_id
            token_label = "YES"
            current_price = yes_price
            fair_prob = max(0.05, min(0.95, current_price + abs(effective_shift)))
            decision_label = "buy_yes"
        elif effective_direction == "decrease_yes":
            token_id = no_token_id
            token_label = "NO"
            current_price = no_price
            fair_prob = max(0.05, min(0.95, current_price + abs(effective_shift)))
            decision_label = "buy_no"
        else:
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "neutral_direction"
            )
            return

        if current_price is None:
            logger.info(f"AISentiment SKIP no_midpoint {market_id[:12]}…")
            self._record_decision(
                news_id, theme, market_id, question, 0, 0, 0, "skip", "no_midpoint"
            )
            return

        # Extreme price filter — near-certain outcome, terrible risk/reward
        # Buying YES at 0.10 needs 90% win rate to break even; not worth it on speculative markets
        if current_price < 0.12 or current_price > 0.88:
            self._record_decision(
                news_id, theme, market_id, question, current_price, current_price, 0,
                "skip", "market_price_extreme"
            )
            return

        # === FIX 2: Token price range filter ===
        if current_price < self._min_token_price or current_price > self._max_token_price:
            logger.info(
                f"AISentiment SKIP {question[:40]} — token price {current_price:.3f} "
                f"outside range [{self._min_token_price}, {self._max_token_price}]"
            )
            self._record_decision(
                news_id, theme, market_id, question, current_price, current_price, 0,
                "skip", "token_price_range"
            )
            return

        # Dead zone filter: 0.35–0.50 is near-50/50 — news-driven LLM has no consistent edge here.
        # Data: 33% win rate, -$3.74 total in this bucket. Best bucket is 0.50–0.65 (67% WR).
        if self._dead_zone_low <= current_price < self._dead_zone_high:
            logger.info(
                f"AISentiment SKIP dead_zone {current_price:.3f} in "
                f"[{self._dead_zone_low}, {self._dead_zone_high}) for {question[:40]}"
            )
            self._record_decision(
                news_id, theme, market_id, question, current_price, current_price, 0,
                "skip", "dead_zone"
            )
            return

        # ── Change 1: Market price trend filter ──────────────────────────────────
        # Check the last 2h of the YES token's price history. If the market is
        # already moving AGAINST the proposed trade direction (e.g. YES falling
        # while we want to BUY YES), skip — we'd be catching a falling knife.
        # A flat or confirming trend is allowed through.
        _yes_trend = await self._get_yes_price_trend(yes_token_id, window_hours=2.0)
        _trend_adverse = (
            (token_label == "YES" and _yes_trend == "down") or
            (token_label == "NO" and _yes_trend == "up")
        )
        if _trend_adverse:
            logger.info(
                f"AISentiment SKIP trend_adverse {token_label} {question[:40]} — "
                f"YES price trend={_yes_trend} last 2h (would be chasing against market)"
            )
            self._record_decision(
                news_id, theme, market_id, question, current_price, fair_prob, 0,
                "skip", "trend_adverse"
            )
            return

        # FIX 4: Edge calculation with multi-tier win-rate calibration
        # The LLM systematically overestimates its edge — calibrate against real outcomes.
        raw_edge = fair_prob - current_price
        base_min_edge = settings.SENTIMENT_MIN_EDGE
        try:
            win_rate = self._db.get_theme_win_rate(theme, min_trades=5)
            if win_rate is not None:
                if win_rate < 0.30:
                    # Severely losing theme: require 2× edge
                    min_edge = base_min_edge * 2.0
                    logger.debug(f"AISentiment edge adj: theme={theme} win_rate={win_rate:.0%} → min_edge={min_edge:.3f} (2x penalty)")
                elif win_rate < 0.40:
                    # Losing theme: require 1.5× edge
                    min_edge = base_min_edge * 1.50
                    logger.debug(f"AISentiment edge adj: theme={theme} win_rate={win_rate:.0%} → min_edge={min_edge:.3f} (1.5x penalty)")
                elif win_rate < 0.50:
                    # Below-random theme: require 1.25× edge
                    min_edge = base_min_edge * 1.25
                    logger.debug(f"AISentiment edge adj: theme={theme} win_rate={win_rate:.0%} → min_edge={min_edge:.3f} (1.25x penalty)")
                elif win_rate > 0.65:
                    # Consistently winning theme: small reward
                    min_edge = base_min_edge * 0.85
                    logger.debug(f"AISentiment edge adj: theme={theme} win_rate={win_rate:.0%} → min_edge={min_edge:.3f} (0.85x reward)")
                else:
                    min_edge = base_min_edge
            else:
                min_edge = base_min_edge
            # Global calibration: if overall strategy PF < 1.0 over 20+ trades,
            # apply a universal 1.25× multiplier as a skepticism floor.
            try:
                _kelly_pf = self._get_current_pf()
                if _kelly_pf is not None and _kelly_pf < 1.0:
                    _global_mult = 1.25
                    min_edge = max(min_edge, base_min_edge * _global_mult)
                    logger.debug(f"AISentiment edge adj: global PF={_kelly_pf:.2f} < 1.0 → min_edge floor={min_edge:.3f}")
            except Exception:
                pass
        except Exception:
            min_edge = base_min_edge
        if raw_edge < min_edge:
            logger.info(
                f"AISentiment SKIP low_edge {raw_edge:.3f} < {min_edge:.3f} for {question[:40]}"
            )
            self._record_decision(
                news_id, theme, market_id, question, current_price, fair_prob, raw_edge,
                "skip", "low_edge"
            )
            return

        size_usdc, size_details = self._compute_position_size(analysis, market, raw_edge, min_edge)
        side = "BUY"
        trade_profile = self._determine_trade_profile(market, analysis, query_entry)
        query_label = str((query_entry or {}).get("query") or "").strip()

        logger.info(
            f"AISentiment TRADE: BUY {token_label} {question[:40]}… "
            f"market={current_price:.3f} fair={fair_prob:.3f} edge={raw_edge:.3f} "
            f"type={trade_profile['trade_type']} "
            f"query={query_label[:28] or 'n/a'} "
            f"size=${size_usdc:.2f} mult={size_details['multiplier']:.2f}"
        )
        # Stamp group cooldown so all correlated themes are blocked for 3h
        self._theme_group_touch(theme)

        decision_id = self._record_decision(
            news_id, theme, market_id, question, current_price, fair_prob, raw_edge,
            decision_label, None
        )

        # Execute — dry_run goes through order_manager which saves to DB internally
        try:
            price = round_price(current_price)
            metadata_json = json.dumps(
                {
                    "entry_fair_prob": fair_prob,
                    "entry_edge": raw_edge,
                    "theme": theme,
                    "news_item_id": news_id,
                    "catalyst_class": analysis.get("catalyst_class"),
                    "trade_type": trade_profile["trade_type"],
                    "query_pattern": query_label,
                    "token_label": token_label,
                    "stop_loss_pct": settings.SENTIMENT_STOP_LOSS_PCT,
                    "take_profit_pct": settings.SENTIMENT_TAKE_PROFIT_PCT,
                    "time_stop_minutes": trade_profile["time_stop_minutes"],
                    "thesis_invalidation_pct": settings.SENTIMENT_THESIS_INVALIDATION_PCT,
                }
            )
            result = self._orders.place_market_order(
                strategy=self.name,
                market_id=market_id,
                token_id=token_id,
                question=question,
                side=side,
                size_usdc=size_usdc,
                price=price,
            )
            if result:
                trade_id = str(result.get("id", ""))
                if decision_id is not None and trade_id:
                    self._db.update_sentiment_decision_trade(decision_id, trade_id)
                self._portfolio.add_position(
                    market_id=market_id,
                    token_id=token_id,
                    question=question,
                    strategy=self.name,
                    side=side,
                    size=size_usdc / price if price else 0,
                    entry_price=price,
                    metadata_json=metadata_json,
                )
                now_ts = time.time()
                self._market_cooldowns[market_id] = now_ts
                try:
                    self._db.set_market_cooldown(market_id, now_ts, "ai_sentiment")
                except Exception as _e:
                    logger.debug(f"AISentiment: cooldown DB persist failed: {_e}")
        except AssertionError as exc:
            logger.warning(f"AISentiment order rejected: {exc}")

    def _record_decision(
        self,
        news_id: Optional[int],
        theme: str,
        market_id: str,
        question: str,
        market_price: float,
        estimated_prob: float,
        edge: float,
        decision: str,
        skip_reason: Optional[str],
    ) -> Optional[int]:
        """Save a sentiment decision to DB. Silently skips if news_id unavailable."""
        if news_id is None:
            return None
        try:
            return self._db.insert_sentiment_decision(
                news_item_id=news_id,
                theme=theme,
                market_id=market_id,
                market_question=question,
                market_price=market_price,
                estimated_probability=estimated_prob,
                edge=edge,
                decision=decision,
                skip_reason=skip_reason,
            )
        except Exception as e:
            logger.debug(f"AISentiment: decision DB insert error: {e}")
            return None


def estimate_fair_probability(
    analysis: dict, current_market_price: float, theme_weight: float = 1.0
) -> float:
    """Compute fair probability from analyzer output. Used in smoke tests."""
    if not analysis.get("is_relevant"):
        return current_market_price
    shift = calibrate_probability_shift(analysis, theme_weight=theme_weight)
    return max(0.05, min(0.95, current_market_price + shift))
