# Polymarket Trading Bot

Production-grade automated trading bot for [Polymarket](https://polymarket.com) — the on-chain prediction market on Polygon.

Runs four concurrent strategies with a real-time monitoring dashboard.

---

## Architecture

```
Data ingestion → Signal engine → Risk manager → Execution (py-clob-client)
                                                        ↓
                                              Dashboard (FastAPI + WS)
```

**Strategies:**
- **Latency Arb** — fades price-lag between Binance momentum and Polymarket midpoints
- **Market Maker** — two-sided quotes on rewards-eligible markets
- **AI Sentiment** — Claude scores news headlines → probability estimate → limit order
- **Copy Trader** — mirrors top leaderboard wallets at configurable ratio

---

## Prerequisites

- Python 3.11+
- Polygon mainnet wallet with MATIC (for gas) + USDC
- Redis (local or Docker)
- Anthropic API key (for AI Sentiment strategy)
- NewsAPI key (optional, for richer news feed)

---

## Installation

```bash
git clone <repo>
cd polymarket-bot
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## One-Time Setup

### 1. Configure `.env`

```bash
cp .env.example .env
# Edit .env — set POLY_PRIVATE_KEY, POLY_WALLET_ADDRESS, ANTHROPIC_API_KEY, etc.
```

### 2. Approve contracts

This grants Polymarket permission to move your USDC. Only needed once per wallet.

```bash
python scripts/setup_allowances.py
```

### 3. Generate API credentials

Derives your Polymarket API key from your private key.

```bash
python scripts/generate_api_keys.py
```

---

## Configuration

All settings are in `.env`. Key parameters:

| Variable | Default | Description |
|---|---|---|
| `POLY_PRIVATE_KEY` | — | Wallet private key (never commit!) |
| `DAILY_LOSS_CAP_USDC` | 500 | Bot halts if daily loss exceeds this |
| `MAX_POSITION_SIZE_USDC` | 2000 | Max exposure per market |
| `DRY_RUN` | false | Log orders without placing them |
| `LATENCY_ARB_ENABLED` | true | Enable/disable each strategy |
| `COPY_RATIO` | 0.10 | Mirror 10% of copied wallet's size |
| `TARGET_WALLETS` | — | Comma-separated wallet addresses to copy |
| `ANTHROPIC_API_KEY` | — | Required for AI Sentiment strategy |

---

## Running the Bot

### Direct

```bash
python -m bot.engine
```

### With Docker Compose

```bash
docker-compose up -d
docker-compose logs -f bot
```

### As systemd service (VPS)

```bash
sudo cp deploy/polymarket-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable polymarket-bot
sudo systemctl start polymarket-bot
journalctl -u polymarket-bot -f
```

---

## Dashboard

Open [http://localhost:8080](http://localhost:8080) after starting the bot.

Features:
- Live PnL chart (updates every 2s via WebSocket)
- Strategy status indicators
- Open positions with mark-to-market values
- Recent trades feed
- Kill switch + resume buttons
- Last 50 log lines

---

## Dry Run Mode

Set `DRY_RUN=true` in `.env`. All orders are logged but never sent to the API.
Useful for verifying strategy logic without capital risk.

---

## Risk Management

The `RiskManager` gatekeeps every order:

- **Daily loss cap**: bot halts if losses exceed `DAILY_LOSS_CAP_USDC`
- **Position size limit**: no single market exposure exceeds `MAX_POSITION_SIZE_USDC`
- **Max open orders**: caps concurrent open orders
- **Kill switch**: `/api/kill` or dashboard button — cancels all orders immediately
- **Graceful shutdown**: `Ctrl+C` / `SIGTERM` cancels all orders before exiting

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Backtesting

```bash
python scripts/backtest.py
```

Fetches 30 days of resolved market data and simulates market-making PnL.

---

## Troubleshooting

**`insufficient balance`**: Run `scripts/setup_allowances.py` — USDC not approved.

**`not accepting orders`**: Market is paused or resolved. The bot auto-skips these.

**`429 rate limit`**: Already handled with exponential backoff. If persistent, reduce strategy intervals in `.env`.

**WebSocket disconnects**: Auto-reconnect with 1s → 2s → 4s … 30s backoff.

**Dashboard shows no data**: The dashboard reads from SQLite — start the bot first to generate trades.

**`POLY_API_KEY not set`**: Run `scripts/generate_api_keys.py`.

---

## Security Notes

- Never commit `.env` or private keys
- Run on a VPS with firewall — expose port 8080 only to your IP
- Consider SSH tunnel: `ssh -L 8080:localhost:8080 user@vps`
- The bot signs orders with your private key — keep it secure
