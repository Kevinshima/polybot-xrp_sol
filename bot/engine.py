"""Main async engine — orchestrates all strategies concurrently."""
from __future__ import annotations

import asyncio
import signal
import sys
from typing import Optional

from config import settings
from core.client import get_client
from core.order_manager import get_order_manager
from core.portfolio import get_portfolio
from core.risk_manager import get_risk_manager
from bot.heartbeat import Heartbeat
from data.exchange_feed import get_exchange_feed
from data.liquidation_feed import get_liquidation_feed
from data.polymarket_feed import get_polymarket_feed
from data.rtds_feed import get_rtds_feed
from database import db
from monitoring.alerter import get_alerter
from utils.logger import logger


async def _supervised(strat, shutdown_event: asyncio.Event, max_restarts: int = 10) -> None:
    """
    Runs a strategy with automatic restarts on crash.

    Backoff: 5s → 10s → 20s … up to 5 minutes per attempt.
    After max_restarts failures the strategy is abandoned and an alert is sent.
    CancelledError propagates immediately (clean shutdown).
    """
    alerter = get_alerter()
    for attempt in range(max_restarts):
        if shutdown_event.is_set():
            return
        try:
            await strat.start()
            return  # clean exit (shutdown_event triggered cancel inside start())
        except asyncio.CancelledError:
            raise  # propagate shutdown
        except Exception as exc:
            backoff = min(5 * (2 ** attempt), 300)
            logger.error(
                f"Strategy {strat.name} crashed (attempt {attempt + 1}/{max_restarts}): "
                f"{exc} — restarting in {backoff}s"
            )
            asyncio.ensure_future(alerter.strategy_crashed(strat.name, str(exc), attempt + 1))
            try:
                await asyncio.wait_for(
                    asyncio.shield(asyncio.ensure_future(shutdown_event.wait())),
                    timeout=backoff,
                )
                return  # shutdown fired during backoff
            except asyncio.TimeoutError:
                pass  # backoff elapsed, loop to next attempt

    logger.critical(f"Strategy {strat.name} exceeded max restarts — giving up")
    asyncio.ensure_future(alerter.strategy_dead(strat.name))


def _build_strategies() -> list:
    """Instantiate enabled strategies."""
    strategies = []

    if settings.LATENCY_ARB_ENABLED:
        from strategies.latency_arb import LatencyArb
        strategies.append(LatencyArb())

    return strategies


async def _startup_checks() -> bool:
    """Verify credentials, balance and allowances before trading."""
    client = get_client()

    try:
        balance = await asyncio.get_event_loop().run_in_executor(None, client.get_balance)
        logger.info(f"Wallet balance: {balance:.2f} USDC")
        if balance < 10:
            logger.warning("Very low balance — are allowances set? Run scripts/setup_allowances.py")
    except Exception as exc:
        logger.error(f"Startup check failed: {exc}")
        return False

    logger.info(
        f"DRY_RUN={settings.DRY_RUN}  "
        f"Loss cap={settings.DAILY_LOSS_CAP_USDC} USDC  "
        f"Max position={settings.MAX_POSITION_SIZE_USDC} USDC"
    )
    return True


async def run() -> None:
    """Entry point — starts all subsystems and runs until SIGINT/SIGTERM."""
    logger.info("=" * 60)
    logger.info("Polymarket Bot starting")
    if settings.DRY_RUN:
        logger.warning("DRY RUN MODE — no real orders will be placed")

    # ── Startup checks ────────────────────────────────────────────────────────
    ok = await _startup_checks()
    if not ok:
        logger.error("Startup checks failed — aborting")
        sys.exit(1)

    # ── Core singletons ───────────────────────────────────────────────────────
    client = get_client()
    portfolio = get_portfolio()
    risk = get_risk_manager()
    order_manager = get_order_manager()
    exchange_feed = get_exchange_feed()
    liquidation_feed = get_liquidation_feed()
    polymarket_feed = get_polymarket_feed()
    rtds_feed = get_rtds_feed()

    # ── Restore open positions from DB ────────────────────────────────────────
    saved_positions = db.get_open_positions()
    for p in saved_positions:
        portfolio.add_position(
            market_id=p["market_id"],
            token_id=p["token_id"],
            question=p["question"],
            strategy=p["strategy"],
            side=p["side"],
            size=p["size"],
            entry_price=p["entry_price"],
            metadata_json=p.get("metadata_json", ""),
            opened_at=p.get("opened_at"),
        )
    if saved_positions:
        logger.info(f"Restored {len(saved_positions)} open position(s) from database")
    logger.warning(f"Engine portfolio id={id(portfolio)}")

    # ── Build strategy list ───────────────────────────────────────────────────
    strategies = _build_strategies()
    if not strategies:
        logger.warning("No strategies enabled! Check settings.")

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    latency_arb = next((s for s in strategies if s.name == "latency_arb"), None)
    heartbeat = Heartbeat(client, portfolio, risk, latency_arb)

    # ── Dashboard ─────────────────────────────────────────────────────────────
    import uvicorn
    from dashboard.server import app

    dashboard_config = uvicorn.Config(
        app,
        host=settings.DASHBOARD_HOST,
        port=settings.DASHBOARD_PORT,
        log_level="warning",
    )
    dashboard_server = uvicorn.Server(dashboard_config)

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    shutdown_event = asyncio.Event()
    alerter = get_alerter()

    def _signal_handler(sig, frame):
        logger.info(f"Signal {sig} received — initiating shutdown")
        # Cancel all orders immediately from signal handler
        try:
            client.cancel_all_orders()
        except Exception as exc:
            logger.error(f"Emergency cancel_all failed: {exc}")
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # ── Launch all tasks ──────────────────────────────────────────────────────
    tasks: list[asyncio.Task] = []

    # Exchange data feed (needed for latency arb)
    tasks.append(asyncio.create_task(exchange_feed.run(), name="exchange_feed"))
    # Binance futures liquidation stream — cascade regime detector
    tasks.append(asyncio.create_task(liquidation_feed.run(), name="liquidation_feed"))
    # Polymarket real-time market feed — orderbook + last_trade_price events
    tasks.append(asyncio.create_task(polymarket_feed.run(), name="polymarket_feed"))
    # RTDS feed — Chainlink oracle prices for independent confirmation
    tasks.append(asyncio.create_task(rtds_feed.run(), name="rtds_feed"))

    # Strategy tasks — wrapped in supervisor for auto-restart on crash
    for strat in strategies:
        tasks.append(
            asyncio.create_task(
                _supervised(strat, shutdown_event), name=strat.name
            )
        )

    # Heartbeat
    tasks.append(asyncio.create_task(heartbeat.run(), name="heartbeat"))

    # Dashboard (serves on separate port, non-blocking)
    tasks.append(asyncio.create_task(dashboard_server.serve(), name="dashboard"))

    strategy_names = [s.name for s in strategies]
    logger.info(
        f"Running {len(strategies)} strategies: "
        + ", ".join(strategy_names)
    )
    logger.info(f"Dashboard: http://{settings.DASHBOARD_HOST}:{settings.DASHBOARD_PORT}")

    # Notify Telegram that we're up
    asyncio.ensure_future(alerter.bot_started(settings.DRY_RUN, strategy_names))

    # ── Wait for shutdown ─────────────────────────────────────────────────────
    await shutdown_event.wait()

    logger.info("Shutting down…")
    await alerter.bot_stopped()

    # Cancel all open orders (belt-and-suspenders)
    try:
        client.cancel_all_orders()
    except Exception as exc:
        logger.error(f"Final cancel_all failed: {exc}")

    # Close exchange feed cleanly (prevents unclosed aiohttp/ccxt connector warnings)
    try:
        await exchange_feed.stop()
    except Exception as exc:
        logger.error(f"exchange_feed stop failed: {exc}")

    try:
        await liquidation_feed.stop()
    except Exception as exc:
        logger.error(f"liquidation_feed stop failed: {exc}")

    try:
        await polymarket_feed.stop()
    except Exception as exc:
        logger.error(f"polymarket_feed stop failed: {exc}")

    try:
        await rtds_feed.stop()
    except Exception as exc:
        logger.error(f"rtds_feed stop failed: {exc}")

    # Cancel async tasks
    for task in tasks:
        if not task.done():
            task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run())
