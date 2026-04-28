"""
Profile-aware entry point.

Usage:
    python main.py                      # latency arb (default)
    python main.py --profile latency    # latency arb (explicit)
    python main.py --profile sentiment  # AI sentiment mode
    PROFILE=sentiment python main.py    # via env var
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path


def _parse_profile() -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", default=os.getenv("PROFILE", "latency"))
    args, _ = parser.parse_known_args()
    return args.profile


def _create_sentiment_env(sent_env_path: Path) -> None:
    """Create runs/sentiment/.env by copying root .env and overriding profile-specific keys."""
    root_env = Path(".env")
    sent_env_path.parent.mkdir(parents=True, exist_ok=True)

    content = root_env.read_text() if root_env.exists() else ""

    overrides = {
        "LATENCY_ARB_ENABLED": "false",
        "MARKET_MAKER_ENABLED": "false",
        "AI_SENTIMENT_ENABLED": "true",
        "COPY_TRADER_ENABLED": "false",
        "DASHBOARD_PORT": "8081",
        "DB_PATH": "runs/sentiment/bot.db",
        "LOG_FILE": "runs/sentiment/logs/sentiment.log",
        "LOG_LEVEL": "INFO",
        "SENTIMENT_VERBOSE": "false",
        "DRY_RUN": "true",
    }

    lines = content.splitlines()
    seen_keys: set[str] = set()
    new_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if "=" in stripped and not stripped.startswith("#"):
            key = stripped.split("=")[0].strip()
            if key in overrides:
                new_lines.append(f"{key}={overrides[key]}")
                seen_keys.add(key)
                continue
        new_lines.append(line)

    # Append any override keys not found in the original
    for key, val in overrides.items():
        if key not in seen_keys:
            new_lines.append(f"{key}={val}")

    sent_env_path.write_text("\n".join(new_lines) + "\n")
    print(f"Created {sent_env_path}")


def _load_sentiment_profile() -> None:
    """Load runs/sentiment/.env (creating it first if absent) and set env vars."""
    from dotenv import load_dotenv

    sent_env = Path("runs/sentiment/.env")
    if not sent_env.exists():
        print("runs/sentiment/.env not found — creating from root .env")
        _create_sentiment_env(sent_env)

    # Create log directory
    Path("runs/sentiment/logs").mkdir(parents=True, exist_ok=True)

    # Override env vars from sentiment .env BEFORE settings is imported anywhere
    load_dotenv(sent_env, override=True)
    os.environ["ACTIVE_PROFILE"] = "sentiment"


def main() -> None:
    profile = _parse_profile()

    if profile == "sentiment":
        _load_sentiment_profile()
    # For 'latency' (default) — do nothing, settings loads root .env normally

    # Import after env is set so all singletons pick up the right config
    from bot.engine import run
    asyncio.run(run())


if __name__ == "__main__":
    main()
