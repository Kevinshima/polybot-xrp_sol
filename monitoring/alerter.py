"""
Telegram alerter — sends critical bot events to a Telegram chat.

Setup:
  1. Create a bot via @BotFather → get TELEGRAM_BOT_TOKEN
  2. Start a chat with your bot, then get your chat ID:
       curl https://api.telegram.org/bot<TOKEN>/getUpdates
  3. Add to .env:
       TELEGRAM_BOT_TOKEN=123456:ABCdef...
       TELEGRAM_CHAT_ID=987654321

If TELEGRAM_BOT_TOKEN is empty, all alerts are logged only (no-op).
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp

from config import settings
from utils.logger import logger


class Alerter:
    """
    Async Telegram alerter with:
    - Rate limiting (max 1 message per 30s per category)
    - Dedup (won't repeat the exact same message within 5 minutes)
    - Non-blocking (fire-and-forget, never crashes the caller)
    """

    _COOLDOWN_DEFAULT = 30      # seconds between alerts of the same category
    _DEDUP_WINDOW = 300         # seconds before same message text can fire again

    def __init__(self):
        self._token: str = getattr(settings, "TELEGRAM_BOT_TOKEN", "")
        self._chat_id: str = getattr(settings, "TELEGRAM_CHAT_ID", "")
        self._enabled = bool(self._token and self._chat_id)
        self._last_sent: dict[str, float] = {}   # category → timestamp
        self._last_text: dict[str, str] = {}     # category → last message text
        self._lock = asyncio.Lock()

        if self._enabled:
            logger.info("Alerter: Telegram enabled")
        else:
            logger.info(
                "Alerter: Telegram disabled (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set)"
            )

    # ── Public API ────────────────────────────────────────────────────────────

    async def send(
        self,
        message: str,
        category: str = "general",
        cooldown: Optional[float] = None,
    ) -> None:
        """
        Send a Telegram message.

        Args:
            message: Text to send (Markdown supported).
            category: Used for rate-limiting; messages in the same category
                      share the same cooldown bucket.
            cooldown: Override the default cooldown (seconds) for this category.
        """
        if cooldown is None:
            cooldown = self._COOLDOWN_DEFAULT

        async with self._lock:
            now = time.monotonic()
            # Rate limit
            if now - self._last_sent.get(category, 0) < cooldown:
                return
            # Dedup
            if (
                self._last_text.get(category) == message
                and now - self._last_sent.get(category, 0) < self._DEDUP_WINDOW
            ):
                return
            self._last_sent[category] = now
            self._last_text[category] = message

        # Log regardless of Telegram availability
        logger.info(f"ALERT [{category}]: {message}")

        if not self._enabled:
            return

        asyncio.ensure_future(self._send_telegram(message))

    # ── Convenience helpers ───────────────────────────────────────────────────

    async def strategy_crashed(self, strategy_name: str, error: str, attempt: int) -> None:
        await self.send(
            f"🔴 *Strategy crashed*: `{strategy_name}`\n"
            f"Attempt #{attempt} — restarting with backoff\n"
            f"`{error[:200]}`",
            category=f"crash_{strategy_name}",
            cooldown=120,
        )

    async def strategy_dead(self, strategy_name: str) -> None:
        await self.send(
            f"💀 *Strategy DEAD*: `{strategy_name}` exceeded max restarts — manual intervention needed",
            category=f"dead_{strategy_name}",
            cooldown=3600,
        )

    async def daily_loss_warning(self, daily_pnl: float, cap: float) -> None:
        pct = abs(daily_pnl) / cap * 100
        await self.send(
            f"⚠️ *Daily loss warning*: {daily_pnl:+.2f} USDC ({pct:.0f}% of {cap:.0f} cap)",
            category="daily_loss_warn",
            cooldown=1800,
        )

    async def daily_loss_cap_hit(self, daily_pnl: float, cap: float) -> None:
        await self.send(
            f"🛑 *Daily loss cap HIT*: {daily_pnl:+.2f} USDC — trading halted for today",
            category="daily_loss_cap",
            cooldown=3600,
        )

    async def consecutive_losses(self, count: int, cumulative_loss: float) -> None:
        await self.send(
            f"🔴 *{count} consecutive losses* — cumulative: {cumulative_loss:+.2f} USDC",
            category="consec_loss",
            cooldown=600,
        )

    async def trade_opened(
        self,
        asset: str,
        direction: str,
        timeframe: str,
        entry_path: str,
        size_usdc: float,
        mid: float,
        momentum: float,
        ml_prob,
        dry_run: bool = True,
    ) -> None:
        tag = "[DRY RUN]" if dry_run else "[LIVE]"
        ml_str = f" | ML {ml_prob:.3f}" if ml_prob is not None else ""
        await self.send(
            f"Trade OPENED {tag}\n"
            f"{asset} {direction} {timeframe} | {entry_path}\n"
            f"${size_usdc:.2f} @ {mid:.3f}\n"
            f"Mom: {momentum:+.2%}{ml_str}",
            category=f"trade_open_{asset}",
            cooldown=5,
        )

    async def trade_closed(
        self,
        asset: str,
        timeframe: str,
        outcome: str,
        pnl: float,
        fill_price: float,
        entry_price: float,
        exit_reason: str,
        daily_pnl: float,
        cumulative_pnl: float,
    ) -> None:
        icon = "✅" if pnl > 0 else "❌"
        result = "WIN" if pnl > 0 else "LOSS"
        await self.send(
            f"{icon} {result}: {pnl:+.2f} USDC\n"
            f"{asset} {timeframe} | {exit_reason}\n"
            f"{entry_price:.3f} -> {fill_price:.3f}\n"
            f"Daily: {daily_pnl:+.2f} | Total: {cumulative_pnl:+.2f}",
            category=f"trade_close_{asset}",
            cooldown=5,
        )

    async def no_trades_warning(self, hours: float) -> None:
        await self.send(
            f"😴 No trades in {hours:.0f}h — check if bot is filtering too aggressively or signals stopped",
            category="no_trades",
            cooldown=3600,
        )

    async def api_circuit_open(self, circuit_name: str) -> None:
        await self.send(
            f"🔌 *API circuit OPEN*: `{circuit_name}` — external API unreachable, using cached data",
            category=f"circuit_{circuit_name}",
            cooldown=300,
        )

    async def bot_started(self, dry_run: bool, strategies: list[str]) -> None:
        mode = "DRY RUN" if dry_run else "LIVE"
        await self.send(
            f"✅ *Bot started* [{mode}]\nStrategies: {', '.join(strategies)}",
            category="startup",
            cooldown=60,
        )

    async def bot_stopped(self, reason: str = "SIGINT") -> None:
        await self.send(
            f"⏹ *Bot stopped* — reason: {reason}",
            category="shutdown",
            cooldown=60,
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _send_telegram(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        # Strip Markdown symbols so strategy names with underscores don't break the parser
        plain = text.replace("*", "").replace("`", "").replace("_", " ")
        payload = {
            "chat_id": self._chat_id,
            "text": plain,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"Alerter: Telegram returned {resp.status}: {body[:100]}")
        except Exception as exc:
            logger.warning(f"Alerter: Telegram send failed: {exc}")


# ── Singleton ─────────────────────────────────────────────────────────────────

_alerter: Optional[Alerter] = None


def get_alerter() -> Alerter:
    global _alerter
    if _alerter is None:
        _alerter = Alerter()
    return _alerter
