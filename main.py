"""Entry point — starts the latency arb bot."""
from __future__ import annotations

import asyncio
from bot.engine import run

if __name__ == "__main__":
    asyncio.run(run())
