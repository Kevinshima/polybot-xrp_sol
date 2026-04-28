"""Heartbeat task — keeps API connection alive, auto-cancels orders on disconnect."""
from __future__ import annotations

import asyncio
import json
import time
from typing import Optional, Tuple

import aiohttp

from config import settings
from database import db
from monitoring.alerter import get_alerter
from utils.logger import logger
from utils.helpers import extract_clob_token_ids

_RESOLVED_THRESHOLD = 0.01  # mid ≤ this → lost; mid ≥ (1 - this) → won


class Heartbeat:
    """
    - Pings the CLOB API every 30 seconds
    - Snapshots PnL to SQLite every 5 minutes
    - Cancels all orders on exit
    """

    PING_INTERVAL = 30
    SNAPSHOT_INTERVAL = 300
    RESOLVER_INTERVAL = 60

    def __init__(self, client, portfolio, risk_manager, latency_arb=None):
        self._client = client
        self._portfolio = portfolio
        self._risk = risk_manager
        self._latency_arb = latency_arb
        self._alerter = get_alerter()
        self._running = False
        self._last_snapshot = 0.0
        self._last_resolve = 0.0
        self._last_reprice = 0.0
        self._consec_losses = 0
        self._consec_loss_total = 0.0
        self._last_trade_ts: float = time.time()

    async def run(self) -> None:
        self._running = True
        logger.info("Heartbeat started")

        while self._running:
            try:
                await self._ping()
                await self._maybe_reprice_positions()
                await self._maybe_manage_latency_positions()
                await self._maybe_snapshot()
                await self._maybe_resolve_positions()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"Heartbeat error: {exc}")

            await asyncio.sleep(self.PING_INTERVAL)

    async def _ping(self) -> None:
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._client.get_balance)
        except Exception as exc:
            logger.debug(f"Heartbeat ping failed: {exc}")

    async def _maybe_snapshot(self) -> None:
        now = time.time()
        if now - self._last_snapshot < self.SNAPSHOT_INTERVAL:
            return
        self._last_snapshot = now

        daily_pnl = self._risk.get_daily_pnl()
        cumulative_pnl = self._risk.get_cumulative_pnl()
        open_value = self._portfolio.total_value()

        db.insert_pnl_snapshot(
            daily_pnl=daily_pnl,
            cumulative_pnl=cumulative_pnl,
            open_value=open_value,
        )
        # Only surface at INFO when there's something to see; silence idle noise
        if open_value > 0 or daily_pnl != 0.0 or cumulative_pnl != 0.0:
            logger.info(
                f"PnL snapshot: daily={daily_pnl:.2f} cumulative={cumulative_pnl:.2f} "
                f"open_value={open_value:.2f}"
            )
        else:
            logger.debug(
                f"PnL snapshot: daily={daily_pnl:.2f} cumulative={cumulative_pnl:.2f} "
                f"open_value={open_value:.2f}"
            )

        # ── Alerts ────────────────────────────────────────────────────────────
        cap = settings.DAILY_LOSS_CAP_USDC
        if daily_pnl < 0:
            loss_pct = abs(daily_pnl) / cap if cap > 0 else 0
            if loss_pct >= 1.0:
                await self._alerter.daily_loss_cap_hit(daily_pnl, cap)
            elif loss_pct >= 0.8:
                await self._alerter.daily_loss_warning(daily_pnl, cap)

        # No-trade warning: if it's been 48h since last trade, something is wrong
        hours_silent = (now - self._last_trade_ts) / 3600
        if hours_silent >= 48:
            await self._alerter.no_trades_warning(hours_silent)

    async def _maybe_reprice_positions(self) -> None:
        interval = max(30, int(settings.SENTIMENT_REPRICE_INTERVAL_SECONDS))
        now = time.time()
        if now - self._last_reprice < interval:
            return
        self._last_reprice = now

        if not self._portfolio.all_positions():
            return

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._portfolio.update_prices)

    async def _maybe_manage_latency_positions(self) -> None:
        """Stop-loss and take-profit exits for open latency_arb positions."""
        if not settings.LAB_EXIT_ENABLED:
            return

        loop = asyncio.get_event_loop()
        now = time.time()
        for pos in list(self._portfolio.all_positions()):
            if pos.strategy != "latency_arb":
                continue

            # Let position breathe for 30s before checking exits
            if now - pos.opened_at < 30:
                continue

            # Fetch live midpoint from CLOB to avoid stop-loss slippage caused
            # by stale prices (pos.current_price is only updated every ~120s by
            # _maybe_reprice_positions, which is far too slow for 5m markets).
            current_price = pos.current_price  # fallback if CLOB unavailable
            if pos.token_id:
                try:
                    live_mid = await loop.run_in_executor(
                        None, self._client.get_midpoint, pos.token_id
                    )
                    if live_mid is not None and float(live_mid) > 0:
                        current_price = float(live_mid)
                        pos.current_price = current_price  # keep in sync
                except Exception:
                    pass  # 404 = expired market; let _maybe_resolve_positions handle it

            if not current_price or current_price <= 0:
                continue

            entry = pos.entry_price
            meta = self._load_position_meta(pos.metadata_json)
            timeframe = str(meta.get("timeframe") or "5m")
            asset = str(meta.get("asset") or "BTC")

            exit_reason: Optional[str] = None
            if timeframe != "5m" and current_price <= entry * (1 - settings.LAB_STOP_LOSS_PCT):
                exit_reason = "stop_loss"
            elif timeframe != "5m" and current_price >= entry * (1 + settings.LAB_TAKE_PROFIT_PCT):
                exit_reason = "take_profit"

            if not exit_reason:
                continue

            pnl = self._portfolio.close_position(pos.market_id, current_price)
            if pnl is None:
                continue

            db.update_trades_for_market(
                pos.market_id,
                fill_price=current_price,
                pnl=pnl,
                exit_reason=exit_reason,
            )
            self._risk.record_fill(pnl)
            logger.info(
                f"LatencyArb EXIT [{exit_reason}] {pos.question[:40]}… "
                f"entry={entry:.3f} current={current_price:.3f} "
                f"PnL={pnl:+.4f} USDC ({asset} {timeframe})"
            )

            if self._latency_arb is not None:
                if pnl > 0:
                    self._latency_arb.on_win(timeframe, asset)
                elif exit_reason == "stop_loss":
                    self._latency_arb.on_stop_loss(timeframe, asset)
                else:
                    self._latency_arb.on_loss(timeframe, asset)

    async def _maybe_manage_sentiment_positions(self) -> None:
        if not settings.SENTIMENT_PAPER_EXIT_ENABLED:
            return

        now = time.time()
        for pos in list(self._portfolio.all_positions()):
            if pos.strategy != "ai_sentiment":
                continue

            meta = self._load_position_meta(pos.metadata_json)
            trade_type = str(meta.get("trade_type", "reaction"))
            age_minutes = (now - pos.opened_at) / 60
            current_return = self._position_return(pos, pos.current_price)
            entry_edge = float(meta.get("entry_edge") or 0.0)
            invalidation_pct = float(
                meta.get("thesis_invalidation_pct", settings.SENTIMENT_THESIS_INVALIDATION_PCT)
            )
            stop_loss_pct = float(meta.get("stop_loss_pct", settings.SENTIMENT_STOP_LOSS_PCT))
            take_profit_pct = float(meta.get("take_profit_pct", settings.SENTIMENT_TAKE_PROFIT_PCT))
            time_stop_minutes = float(meta.get("time_stop_minutes", settings.SENTIMENT_TIME_STOP_MINUTES))

            exit_reason: Optional[str] = None
            if current_return >= take_profit_pct:
                exit_reason = "take_profit"
            elif current_return <= -stop_loss_pct:
                exit_reason = "stop_loss"
            else:
                wrong_way_move = max(entry_edge * 0.5, invalidation_pct)
                if pos.side == "BUY" and pos.current_price <= max(0.01, pos.entry_price - wrong_way_move):
                    exit_reason = "thesis_invalidation"
                elif pos.side == "SELL" and pos.current_price >= min(0.99, pos.entry_price + wrong_way_move):
                    exit_reason = "thesis_invalidation"
                elif age_minutes >= time_stop_minutes:
                    exit_reason = "time_stop"

            if not exit_reason:
                continue

            pnl = self._portfolio.close_position(pos.market_id, pos.current_price)
            if pnl is None:
                continue

            db.update_trades_for_market(
                pos.market_id,
                fill_price=pos.current_price,
                pnl=pnl,
                exit_reason=exit_reason,
            )
            self._risk.record_fill(pnl)
            logger.info(
                f"SentimentExit: {pos.question[:50]}… type={trade_type} reason={exit_reason} "
                f"price={pos.current_price:.4f} pnl={pnl:+.4f}"
            )
            # Persist cooldown so the sentiment strategy cannot immediately re-enter.
            # stop_loss → 4-hour block; all others → normal cooldown restarts from exit.
            _cd_secs = settings.SENTIMENT_COOLDOWN_MINUTES * 60
            if exit_reason == "stop_loss":
                _block = 4 * 3600
                _cd_ts = time.time() + (_block - _cd_secs)
            else:
                _cd_ts = time.time()
            try:
                db.set_market_cooldown(pos.market_id, _cd_ts, "ai_sentiment")
            except Exception:
                pass

    async def _maybe_resolve_positions(self) -> None:
        now = time.time()
        if now - self._last_resolve < self.RESOLVER_INTERVAL:
            return
        self._last_resolve = now

        positions = self._portfolio.all_positions()
        if not positions:
            return

        loop = asyncio.get_event_loop()
        async with aiohttp.ClientSession() as session:
            for pos in positions:
                if not pos.token_id:
                    continue

                fill_price: Optional[float] = None
                pnl: Optional[float] = None
                outcome: Optional[str] = None

                # Positions older than 10 minutes are from expired markets —
                # get_midpoint() returns 404 for expired btc-updown-5m markets.
                # Skip the CLOB check entirely and go straight to Gamma.
                if time.time() - pos.opened_at <= 600:
                    try:
                        mid = await loop.run_in_executor(
                            None, self._client.get_midpoint, pos.token_id
                        )
                        if mid >= 1.0 - _RESOLVED_THRESHOLD:
                            fill_price = 1.0
                            pnl = self._position_pnl(pos, fill_price)
                            outcome = "WON"
                        elif mid <= _RESOLVED_THRESHOLD:
                            fill_price = 0.0
                            pnl = self._position_pnl(pos, fill_price)
                            outcome = "LOST"
                        else:
                            continue  # still live
                    except Exception:
                        pass  # fall through to Gamma below

                if fill_price is None:
                    # CLOB unavailable or position too old — use Gamma API
                    fill_price, pnl, outcome = await self._resolve_via_gamma(session, pos)
                    if fill_price is None:
                        continue  # genuinely undetermined — skip for now

                logger.info(
                    f"PositionResolver: {pos.question[:40]}… → {outcome} "
                    f"PnL={pnl:+.4f} USDC "
                    f"(size={pos.size:.4f} entry={pos.entry_price:.3f})"
                )
                db.update_trades_for_market(
                    pos.market_id,
                    fill_price=fill_price,
                    pnl=pnl,
                    exit_reason="resolved",
                )
                self._portfolio.close_position(pos.market_id, fill_price)
                self._risk.record_fill(pnl)
                self._last_trade_ts = time.time()

                # Track consecutive losses for alerts
                if pnl < 0:
                    self._consec_losses += 1
                    self._consec_loss_total += pnl
                    if self._consec_losses >= 5:
                        await self._alerter.consecutive_losses(
                            self._consec_losses, self._consec_loss_total
                        )
                else:
                    self._consec_losses = 0
                    self._consec_loss_total = 0.0

                if self._latency_arb is not None:
                    meta = self._load_position_meta(pos.metadata_json)
                    timeframe = str(meta.get("timeframe") or "")
                    asset = str(meta.get("asset") or "BTC")
                    if pnl > 0:
                        self._latency_arb.on_win(timeframe, asset)
                    else:
                        self._latency_arb.on_loss(timeframe, asset)

    async def _resolve_via_gamma(
        self, session: aiohttp.ClientSession, pos
    ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        """
        Fallback resolver using Gamma API when the CLOB midpoint is unavailable
        (expired btc-updown-5m markets return 404 from get_midpoint).

        Checks outcomePrices by matching the held token_id to clobTokenIds.
        If the market is closed but outcomePrices is missing, marks LOST conservatively.

        Returns (fill_price, pnl, outcome) or (None, None, None) if undetermined.
        """
        url = "https://gamma-api.polymarket.com/markets"
        try:
            async with session.get(
                url,
                params={"clob_token_ids": pos.token_id},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"PositionResolver: Gamma API returned {resp.status} for "
                        f"{pos.market_id[:20]}…"
                    )
                    return None, None, None
                data = await resp.json()
                markets = data if isinstance(data, list) else data.get("data", [])
                if not markets:
                    return None, None, None
                market = markets[0]
        except Exception as exc:
            logger.warning(f"PositionResolver: Gamma API fetch failed: {exc}")
            return None, None, None

        outcome_prices = market.get("outcomePrices")
        closed = market.get("closed", False)
        token_ids = extract_clob_token_ids(market)
        token_index = token_ids.index(pos.token_id) if pos.token_id in token_ids else None

        if outcome_prices:
            try:
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)
                prices = [float(p) for p in outcome_prices]
                if token_index is not None and token_index < len(prices):
                    token_price = prices[token_index]
                    if token_price >= 0.99:
                        return 1.0, self._position_pnl(pos, 1.0), "WON"
                    elif token_price <= 0.01:
                        return 0.0, self._position_pnl(pos, 0.0), "LOST"
            except (ValueError, IndexError):
                pass

        if closed:
            # Outcome indeterminate — mark LOST conservatively
            logger.warning(
                f"PositionResolver: {pos.question[:40]}… closed with no outcome "
                f"prices — marking LOST conservatively"
            )
            return 0.0, self._position_pnl(pos, 0.0), "LOST"

        return None, None, None

    def _position_pnl(self, pos, fill_price: float) -> float:
        if pos.side == "SELL":
            return pos.size * (pos.entry_price - fill_price)
        return pos.size * (fill_price - pos.entry_price)

    def _position_return(self, pos, mark_price: float) -> float:
        if pos.entry_price <= 0:
            return 0.0
        if pos.side == "SELL":
            return (pos.entry_price - mark_price) / pos.entry_price
        return (mark_price - pos.entry_price) / pos.entry_price

    def _load_position_meta(self, metadata_json: str) -> dict:
        if not metadata_json:
            return {}
        try:
            return json.loads(metadata_json)
        except Exception:
            return {}

    async def stop(self) -> None:
        self._running = False
