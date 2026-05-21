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

    PING_INTERVAL      = 30   # API balance check — keep at 30s to avoid rate-limits
    POSITION_INTERVAL  = 10   # stop-loss / take-profit check — 10s cuts slippage ~50%
    SNAPSHOT_INTERVAL  = 300
    RESOLVER_INTERVAL  = 60

    def __init__(self, client, portfolio, risk_manager, latency_arb=None):
        self._client = client
        self._portfolio = portfolio
        self._risk = risk_manager
        self._latency_arb = latency_arb
        self._alerter = get_alerter()
        self._running = False
        self._last_ping     = 0.0
        self._last_snapshot = 0.0
        self._last_resolve  = 0.0
        self._last_reprice  = 0.0
        self._consec_losses = 0
        self._consec_loss_total = 0.0
        self._last_trade_ts: float = time.time()
        # Trailing exit state: highest price seen since entry, keyed by market_id.
        # Initialised to entry_price on first check; updated upward on every tick.
        self._peak_prices: dict[str, float] = {}

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

            await asyncio.sleep(self.POSITION_INTERVAL)

    async def _ping(self) -> None:
        now = time.time()
        if now - self._last_ping < self.PING_INTERVAL:
            return
        self._last_ping = now
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
        interval = 30
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
            asset = str(meta.get("asset") or "SOL")

            entry_path = meta.get("entry_path", "")
            direction  = meta.get("direction", "")

            # ── Trailing exit ─────────────────────────────────────────────────
            # Track the highest price seen since entry (all positions are BUY —
            # both UP and DOWN tokens increase toward 1.0 when correct).
            market_id = pos.market_id
            peak = max(self._peak_prices.get(market_id, entry), current_price)
            self._peak_prices[market_id] = peak

            # Trail distance tightens as peak climbs:
            #   normal  (peak < 0.85) → 15¢ wide — leaves room for noise
            #   high    (peak ≥ 0.85) →  8¢ tight — lock in significant gain
            #   hold    (peak ≥ 0.93) →  5¢ — near resolution, resolver takes over
            if peak >= settings.LAB_TRAIL_HOLD_THRESHOLD:
                trail_dist = settings.LAB_TRAIL_HOLD
            elif peak >= settings.LAB_TRAIL_HIGH_THRESHOLD:
                trail_dist = settings.LAB_TRAIL_HIGH
            else:
                trail_dist = settings.LAB_TRAIL_NORMAL

            trail_level = peak - trail_dist
            hard_floor  = entry * (1.0 - settings.LAB_TRAIL_FLOOR_PCT)

            exit_reason: Optional[str] = None
            if current_price <= hard_floor:
                # Flash-crash blew through trail — safety net exit
                exit_reason = "stop_loss"
            elif current_price <= trail_level:
                # Trailing stop fired: price fell trail_dist from peak
                exit_reason = "trail_stop"
            elif (
                entry_path == "CONFIRMED"
                and direction
                and pos.token_id
                and self._latency_arb is not None
                and self._latency_arb.is_signal_reversed(asset, direction, entry_path, pos.token_id)
            ):
                exit_reason = "signal_reversed"
            # ─────────────────────────────────────────────────────────────────

            if not exit_reason:
                continue

            pnl = self._portfolio.close_position(market_id, current_price)
            if pnl is None:
                continue

            # Clean up trailing state for this position
            self._peak_prices.pop(market_id, None)

            db.update_trades_for_market(
                market_id,
                fill_price=current_price,
                pnl=pnl,
                exit_reason=exit_reason,
            )
            self._risk.record_fill(pnl)
            logger.info(
                f"LatencyArb EXIT [{exit_reason}] {pos.question[:40]}… "
                f"entry={entry:.3f} peak={peak:.3f} trail_lvl={trail_level:.3f} "
                f"current={current_price:.3f} PnL={pnl:+.4f} USDC ({asset} {timeframe})"
            )
            await self._alerter.trade_closed(
                asset=asset, timeframe=timeframe,
                outcome="WON" if pnl > 0 else "LOST",
                pnl=pnl,
                fill_price=current_price, entry_price=entry,
                exit_reason=exit_reason,
                daily_pnl=self._risk.get_daily_pnl(),
                cumulative_pnl=self._risk.get_cumulative_pnl(),
            )

            if self._latency_arb is not None:
                if pnl > 0:
                    self._latency_arb.on_win(timeframe, asset)
                elif exit_reason in ("stop_loss", "trail_stop", "signal_reversed"):
                    self._latency_arb.on_stop_loss(timeframe, asset)
                else:
                    self._latency_arb.on_loss(timeframe, asset)

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
                    # CLOB unavailable or position too old — try Gamma first
                    fill_price, pnl, outcome = await self._resolve_via_gamma(session, pos)

                if fill_price is None:
                    # Gamma doesn't index updown markets — fall back to CLOB market endpoint
                    fill_price, pnl, outcome = await self._resolve_via_clob_market(session, pos)
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

                # Parse metadata once — used for alert + latency_arb callbacks
                _meta = self._load_position_meta(pos.metadata_json)
                _tf = str(_meta.get("timeframe") or "?")
                _asset = str(_meta.get("asset") or "SOL")

                if pos.strategy == "latency_arb":
                    await self._alerter.trade_closed(
                        asset=_asset, timeframe=_tf,
                        outcome=outcome, pnl=pnl,
                        fill_price=fill_price, entry_price=pos.entry_price,
                        exit_reason="resolved",
                        daily_pnl=self._risk.get_daily_pnl(),
                        cumulative_pnl=self._risk.get_cumulative_pnl(),
                    )

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
                    if pnl > 0:
                        self._latency_arb.on_win(_tf, _asset)
                    else:
                        self._latency_arb.on_loss(_tf, _asset)

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

    async def _resolve_via_clob_market(
        self, session: aiohttp.ClientSession, pos
    ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        """
        Second fallback resolver using the CLOB /markets/{condition_id} endpoint.
        The btc/eth-updown-5m markets are NOT indexed by Gamma, so this is the
        only reliable way to get final outcome data for these automated markets.

        Checks tokens[].winner by matching pos.token_id.
        Returns (fill_price, pnl, outcome) or (None, None, None) if undetermined.
        """
        url = f"https://clob.polymarket.com/markets/{pos.market_id}"
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    logger.debug(
                        f"PositionResolver: CLOB market endpoint returned {resp.status} "
                        f"for {pos.market_id[:20]}…"
                    )
                    return None, None, None
                market = await resp.json()
        except Exception as exc:
            logger.warning(f"PositionResolver: CLOB market fetch failed: {exc}")
            return None, None, None

        if not market.get("closed", False):
            return None, None, None  # market still live

        tokens = market.get("tokens", [])
        for token in tokens:
            if str(token.get("token_id", "")) != str(pos.token_id):
                continue
            winner = token.get("winner")
            if winner is True:
                return 1.0, self._position_pnl(pos, 1.0), "WON"
            elif winner is False:
                return 0.0, self._position_pnl(pos, 0.0), "LOST"

        # Market closed but our token not found — mark LOST conservatively
        if tokens:
            logger.warning(
                f"PositionResolver: {pos.question[:40]}… CLOB closed, token not found "
                f"— marking LOST conservatively"
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
