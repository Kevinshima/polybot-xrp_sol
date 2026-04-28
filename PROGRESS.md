# Polymarket Bot — Development Progress

## Project Status: RUNNING (dry-run mode, market_maker strategy active)

---

## Session 1 — Initial Setup & Audit

### What was built (pre-existing at session start)
Full project scaffold was already generated matching the spec:
- 44/44 required files present across all modules
- 4 strategies: `latency_arb`, `market_maker`, `ai_sentiment`, `copy_trader`
- Dashboard at `http://localhost:8080` (FastAPI + vanilla JS)
- SQLite database, Redis cache, structured JSON logging via loguru
- Dockerfile + docker-compose with Redis service
- systemd unit file + nginx config in `deploy/`
- 3 test files in `tests/`

### Credentials added to `.env`
User filled in:
- `POLY_PRIVATE_KEY` — real value
- `POLY_WALLET_ADDRESS` — `0x86eF4D40cCE1C7BCd117F46c5cAc5416c4B665e3`
- `POLY_API_KEY` — real value
- `POLY_API_SECRET` — real value
- `POLY_API_PASSPHRASE` — real value

Still missing (as of this session):
- `ANTHROPIC_API_KEY` — placeholder (`your-anthropic-key`)
- `NEWS_API_KEY` — placeholder (`your-newsapi-key`)
- `BINANCE_API_KEY` / `BINANCE_API_SECRET` — empty
- `TARGET_WALLETS` — placeholder addresses

### Current `.env` strategy toggles
```
LATENCY_ARB_ENABLED=false    ← disabled (no Binance key / IP blocked)
MARKET_MAKER_ENABLED=true
AI_SENTIMENT_ENABLED=false   ← disabled (no Anthropic key)
COPY_TRADER_ENABLED=false    ← disabled (no target wallets)
DRY_RUN=true
```

---

## Session 2 — Bug Fixes (Round 1)

### Bug: `ClobClient has no attribute 'get_balance'`
**File:** `core/client.py` — `get_balance()` method
**Root cause:** `py-clob-client` SDK exposes no balance method.
**Fix:** Replaced with web3 call to USDC contract on Polygon (same pattern as `setup_allowances.py`). Queries `balanceOf(wallet)` on `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`.

### Bug: `MM candidates: 0 markets`
**File:** `data/market_scanner.py` — `get_mm_candidates()`
**Root cause:** Filter required `rewards_daily_rate > 0` but field name was wrong AND Polymarket's rewards program is inactive/zero on most markets.
**Fix:** Removed hard rewards gate. Now accepts all `acceptingOrders=True` markets and logs rewarded count as info. (Field name fix came in Round 2.)

### Bug: Unclosed aiohttp/ccxt connectors on shutdown
**File:** `bot/engine.py` — shutdown sequence
**Root cause:** `exchange_feed.stop()` was never awaited during graceful shutdown.
**Fix:** Added `await exchange_feed.stop()` before task cancellation in the shutdown block.

---

## Session 3 — Bug Fixes (Round 2)

### Bug: `cannot import name 'geth_poa_middleware' from 'web3.middleware'`
**Files:** `core/client.py`, `scripts/setup_allowances.py`
**Root cause:** web3 v7 (installed: `7.14.1`) removed `geth_poa_middleware`.
**Fix:** Replaced with `ExtraDataToPOAMiddleware` (the v7 equivalent) in both files.

### Bug: `MM candidates: 0 markets (0 with active rewards)` — still 0
**File:** `data/market_scanner.py`
**Root cause:** Filter used `accepting_orders` (snake_case) but Gamma API returns `acceptingOrders` (camelCase). Every market evaluated as False.
**Fix:** Changed to `acceptingOrders`. Also fixed rewards field: `clobRewards[].rewardsDailyRate` (not `rewards_daily_rate`).
**Also fixed same field name in:**
- `strategies/ai_sentiment.py` line 82
- `strategies/latency_arb.py` line 77

---

## Session 4 — Bug Fixes (Round 3)

### Bug: `401 Unauthorized for url: https://polygon-rpc.com/`
**Files:** `config/settings.py`, `.env`
**Root cause:** `polygon-rpc.com` now requires auth / rate-limits unauthenticated requests.
**Fix:** Switched default RPC to `https://polygon-bor-rpc.publicnode.com` (tested working, returned block 84612570). Updated both `settings.py` default and `.env`.

### Bug: `No orderbook exists for the requested token id` (all markets)
**Files:** `strategies/market_maker.py`, `strategies/ai_sentiment.py`, `strategies/latency_arb.py`
**Root cause:** All three strategies read `market.get("tokens", [])` then `tokens[0].get("token_id")`. But the Gamma API has **no `tokens` field** — it has `clobTokenIds`, a JSON-encoded string like `'["1234...", "5678..."]'`. Every market fell back to using `conditionId` as the token ID, which the CLOB API rejects.
**Fix:** Added `extract_clob_token_id(market)` helper to `utils/helpers.py` that parses the JSON string and returns the YES (index 0) token ID with a `conditionId` fallback. All three strategies now use this helper.

### Bug: Rewards sort in market_maker used wrong field
**File:** `strategies/market_maker.py` — `_scan_markets()`
**Root cause:** Sort key used `m.get("rewards_daily_rate")` (old field name, doesn't exist).
**Fix:** Replaced sort key with a lambda that reads `clobRewards[].rewardsDailyRate`.

### Info: Binance blocked
**File:** `data/exchange_feed.py`
**Cause:** Binance API blocks direct connections from certain IPs (residential/EU).
**Status:** Not a bug — `LATENCY_ARB_ENABLED=false` so no impact. Exchange feed still starts (needed as shared infrastructure) but fetch errors are non-fatal warnings. Will need VPS in supported region or Binance API key to enable latency arb.

---

## Session 5 — Market Filter + Shutdown Fix

### Bug: Unclosed ccxt/aiohttp resources on shutdown
**File:** `data/exchange_feed.py` — `_connect_binance()`
**Root cause:** No `try/finally` block around the exchange instance. When the asyncio task was cancelled, ccxt's `__del__` emitted a warning because `close()` was never called *from within the coroutine*. The `stop()` method called it externally but ccxt tracks per-coroutine.
**Fix:** Restructured `_connect_binance` to create the exchange outside the try block, added `try/finally` that always calls `await exchange.close()` regardless of how the loop exits (normal, exception, or `CancelledError`).

### Feature: Short-term high-volume market filter
**File:** `data/market_scanner.py` — `get_mm_candidates()`
**Change:** Added two new filters:
1. **End date ≤ 7 days** — parses `endDate` / `endDateIso` field, skips markets resolving more than 7 days out or already past
2. **24h volume ≥ $10,000** — reads `volume24hr` (falls back to `volume`), skips low-liquidity markets
**Result:** No more 2028 presidential election markets — only near-term, liquid markets selected.

### Feature: BTC/ETH/SOL market prioritization
**File:** `strategies/market_maker.py` — `_scan_markets()`
**Change:** Replaced simple rewards-rate sort with a 3-key sort:
1. Crypto markets (BTC/bitcoin/ETH/ethereum/SOL/solana in question) ranked first
2. Within crypto: sorted by volume descending
3. Within non-crypto: sorted by rewards rate descending

## Session 6 — Calibration + Crypto Market Refocus

### Fix: 7-day window too narrow → 0 markets
**File:** `data/market_scanner.py` — `get_mm_candidates()`
**Root cause:** Probing the live API revealed only 1 market resolves within 7 days on all of Polymarket. The `volume24hr` field confirmed vol=$8,532 for the Russia/Ukraine market (below $10k threshold), but total `volume` was $187k — the `or` fallback caused it to pass.
**Fix:** Changed `timedelta(days=7)` → `timedelta(days=30)`. At $10k/day volume this gives 34 markets. Also fixed an indentation bug on the `continue` statement introduced in Session 5.

### Fix: Crypto keyword false positives
**File:** `strategies/market_maker.py` — `_is_crypto_market()`
**Root cause:** Bare substring `"eth" in question` matched `"Spi**eth**"` (Jordan Spieth, golf). `"sol"` matched `"resolu**tion**"` etc. Probe data confirmed "Jordan Spieth" was being flagged as a crypto market.
**Fix:** Replaced with `re.search(r'\b(BTC|ETH|SOL)\b|bitcoin|ethereum|solana', ...)` using word boundaries for uppercase symbols.

### Feature: Crypto price market scanner + latency arb refocus
**Files:** `data/market_scanner.py`, `strategies/latency_arb.py`, `strategies/market_maker.py`, `.env`

**Context:** User requested refocusing on `updown-15m` / `up-or-down-15` slug markets. Live API probe confirmed these slugs **do not exist on Polymarket** (0 matches across 1000 markets). Available crypto price markets: 4 total, top being "Will bitcoin hit $1m before GTA VI?" ($11k/day vol).

**What was implemented:**
- Added `MarketScanner.get_crypto_price_candidates(min_volume)` — searches slug+question for BTC/bitcoin, ETH price keywords (multi-word to avoid false positives), SOL price keywords. Returns `{asset: [markets]}` dict. `_ASSET_PATTERNS` list is the single place to add `updown-15m` slugs when Polymarket lists them.
- Rewrote `LatencyArb._scan_crypto_markets()` to call `get_crypto_price_candidates()` instead of per-keyword fuzzy search. Maps `BTC→BTC/USDT`, `ETH→ETH/USDT`, `SOL→SOL/USDT` for exchange feed momentum lookup.
- Updated `MarketMaker._scan_markets()` to call `get_crypto_price_candidates()` first; falls back to `get_mm_candidates()` if no crypto markets found.
- Set `LATENCY_ARB_ENABLED=true` in `.env`.

**Latency arb signal logic** (unchanged, already correct):
- Binance momentum > 0.3% AND poly_price < 0.60 → BUY YES (rising price, market underpriced)
- Binance momentum < -0.3% AND poly_price > 0.40 → SELL YES / BUY NO (falling price, overpriced)

**Known blocker:** Binance is still blocked on current IP (no `BINANCE_API_KEY`). Exchange feed connects but `fetch_tickers` returns warnings. `get_momentum()` returns 0.0 until real prices load — latency arb runs but fires no signals. Fix: add Binance API key or run on VPS outside blocked region.

---

## Session 7 — btc-updown-5m Markets + Coinbase Feed

### Feature: `btc-updown-5m` market discovery
**File:** `data/market_scanner.py` — added `get_updown_market()`
**Context:** `btc-updown-5m-{unix_timestamp}` markets exist on Polymarket and are accessible via Gamma API. Slug pattern uses UTC time rounded down to the nearest 5-minute interval.
**What was implemented:**
- Calculates current 5-min interval: `(int(time.time()) // 300) * 300`
- Queries Gamma API by slug — tries current interval then next interval (in case window is expiring)
- Parses `clobTokenIds` JSON string → `ids[0]` = UP token, `ids[1]` = DOWN token
- Returns `{slug, market, up_token_id, down_token_id}` or `None`

### Feature: Replace Binance with Coinbase + Kraken price feed
**File:** `data/exchange_feed.py` — full rewrite
**Root cause:** Binance is geo-blocked in Albania. `get_momentum()` returned 0.0 permanently, so latency arb never fired.
**Fix:** Removed all ccxt/Binance code. New feed:
- **Primary:** `https://api.coinbase.com/v2/prices/{BTC,ETH,SOL}-USD/spot` — no API key needed
- **Fallback per symbol:** `https://api.kraken.com/0/public/Ticker?pair=XBTUSD` (result key `XXBTZUSD`), `ETHUSD`→`XETHZUSD`, `SOLUSD`→`SOLUSD`
- Coinbase called first; Kraken only called if Coinbase returns `None` for that symbol
- Coinbase failures logged at DEBUG (quiet), Kraken failures at WARNING
- Same `get_price()` / `get_momentum()` interface — rest of codebase unchanged

### Bug: `_prev_prices` initialized to same value as `_prices` → momentum always 0
**File:** `data/exchange_feed.py` — `_fetch_all()`
**Root cause:** On the very first fetch, `self._prices.get(symbol, price)` returns `price` (dict is empty), so `_prev_prices[symbol] = price` and `_prices[symbol] = price` were identical. Momentum = 0.0 until the second 60-second rotation.
**Fix:** Skip rotation when `symbol not in self._prices` (first fetch). Rotation now only fires when there is already a real "old" price to capture:
```python
if symbol in self._prices and now - self._last_rotate.get(symbol, 0) >= 60:
    self._prev_prices[symbol] = self._prices[symbol]
```

### Feature: Latency arb refocused on updown-5m markets
**File:** `strategies/latency_arb.py` — restructured `_tick()`
- `_refresh_updown_market()` re-fetches when the 5-min slug changes or cache is >60s old
- Positive Coinbase BTC momentum → BUY UP token; negative → BUY DOWN token (both side=`BUY`, different token IDs)
- Falls back to BTC/ETH/SOL milestone markets (`get_crypto_price_candidates()`) if no updown market is active
- `_scan_crypto_markets()` retained as fallback, logs as `LatencyArb fallback: N markets for BTC/USDT`

### Feature: Momentum + price diagnostic logging
**Files:** `data/exchange_feed.py`, `strategies/latency_arb.py`
- Exchange feed logs BTC/ETH/SOL price + momentum at every 60s rotation: `ExchangeFeed BTC/USDT: price=X prev=Y momentum=+Z%`
- Latency arb logs momentum every 30s: `LatencyArb tick: BTC=X momentum=+Y% threshold=Z% market=btc-updown-5m-...`

### Config: Lowered momentum threshold for testing
**File:** `.env` — added `LAB_MOMENTUM_THRESHOLD=0.001` (0.1% instead of default 0.3%)
**Reason:** BTC rarely moves >0.3% in a 60-second window during low-volatility periods; 0.1% allows confirming end-to-end signal flow before raising back to 0.3%.

---

## Session 8 — Order Flood Guards

### Bug: Latency arb floods 20+ orders on same market per second
**File:** `strategies/latency_arb.py`
**Root cause:** In DRY RUN mode `_execute_signal` returns early without calling `_portfolio.add_position()`. So `has_position(market_id)` always returns False, and every 500ms tick re-fires on the same market as long as momentum stays above threshold.
**Confirmed in logs:** 20+ `DRY RUN LatencyArb: BUY` lines firing ~500ms apart on the same `btc-updown-5m` market.

### Fix: Per-tick dedup set (`_traded_this_cycle`)
**File:** `strategies/latency_arb.py`
- Added `_traded_this_cycle: set[str]` to `__init__`
- Reset to `set()` at top of every `_tick()` call
- Both `_trade_updown` and `_trade_milestone_markets` check `market_id in self._traded_this_cycle` before trading
- `self._traded_this_cycle.add(market_id)` called immediately before `_execute_signal` (so it takes effect even if the call raises)

### Fix: 5-minute per-market cooldown (`_cooldown`)
**File:** `strategies/latency_arb.py`
- Added `_cooldown: dict[str, float]` to `__init__`
- Both trade paths check `time.time() - self._cooldown.get(market_id, 0) < 300` before trading
- `self._cooldown[market_id] = time.time()` set before `_execute_signal` in both paths
- Prevents re-entering the same `btc-updown-5m` market across ticks for 5 minutes — matches the natural 5-minute market window

---

## Session 9 — Price Filter + Dry-Run Database Persistence

### Feature: Near-certain market price filter in `_compute_signal()`
**File:** `strategies/latency_arb.py` — `_compute_signal()`
**Reason:** Markets already priced above 0.85 or below 0.15 are near-certain outcomes with no edge — the momentum signal doesn't move them meaningfully.
**Change:** Added early return `None` before any directional check when `poly_price < 0.15 or poly_price > 0.85`. Only trades milestone markets where `0.15 ≤ mid ≤ 0.85`.

### Feature: Dry-run trades saved to SQLite database
**Files:** `database/models.py`, `database/db.py`, `core/order_manager.py`
**Reason:** The dashboard showed no trade history or simulated PnL during dry-run mode because `_execute_signal` returned early before calling `insert_trade()`.

**Changes:**
- `database/models.py` — added `dry_run = Column(Boolean, default=False)` to `Trade` model
- `database/db.py`:
  - `insert_trade()` accepts `dry_run: bool = False` and stores it on the row
  - `_trade_to_dict()` includes `"dry_run": bool(t.dry_run)` in the returned dict
  - Auto-migration on startup: `ALTER TABLE trades ADD COLUMN dry_run BOOLEAN DEFAULT 0` (silently skipped if column already exists — safe for existing databases)
- `core/order_manager.py` — `place_limit_order()`:
  - Added dry-run early return (previously had no DRY_RUN guard at all — would have attempted a real CLOB API call)
  - Saves to DB with `status="open"`, `dry_run=True`, returns `{"id": "dry_lmt_...", "status": "dry_run"}`
- `core/order_manager.py` — `place_market_order()`:
  - Dry-run path now calls `insert_trade(..., status="filled", dry_run=True)` before returning
  - Market orders saved as `status="filled"` (simulated instant fill)
  - Cleaned up duplicate `from config import settings` import that was in the original code

**Dashboard impact:** `/api/trades` endpoint now returns dry-run trades with `"dry_run": true` flag — can be filtered or labeled in the UI.

---

## Session 10 — Dry-Run DB Save Fix + Order Size Unification

### Bug: Dry-run trades not reaching `insert_trade()` — dashboard still showed "No trades"
**File:** `strategies/latency_arb.py` — `_execute_signal()`
**Root cause:** `_execute_signal` had its own `if self.dry_run: logger.info(...); return` guard that short-circuited before ever calling `self._orders.place_market_order()`. The `order_manager` dry-run code was correct — it just wasn't being called. The "DRY RUN LatencyArb: BUY" log came from `_execute_signal` itself, not from order_manager.
**Fix:** Removed the early-return guard from `_execute_signal`. Dry-run calls now flow through to `place_market_order`, which handles `settings.DRY_RUN` and calls `insert_trade`.
**Also added:** `logger.debug(f"DB insert OK — dry run trade {order_id} saved")` in `place_market_order` after the insert, confirming DB write happened.

### Feature: Actual fill price stored for dry-run market orders
**File:** `core/order_manager.py` — `place_market_order()`
**Problem:** Dry-run trades were saved with `price=0.0` and `fill_price=0.0` instead of the actual mid price.
**Fix:** Added `price: float = 0.0` optional parameter to `place_market_order()`. Dry-run `insert_trade` now saves `price=price`, `fill_price=price`, and `size=size_usdc/price` (shares). `latency_arb._execute_signal` passes `price=price` through to the call.

### Feature: Unified order size across all strategies to `MAX_POSITION_SIZE_USDC`
**Files:** `strategies/latency_arb.py`, `strategies/market_maker.py`, `strategies/ai_sentiment.py`
**Reason:** All three strategies used `settings.MM_DEFAULT_SIZE_USDC` (default 200 USDC). `MAX_POSITION_SIZE_USDC` in `.env` (default 2000 USDC) is now the single knob controlling trade size.
**Changes:**
- `latency_arb._compute_signal()` — `size = settings.MAX_POSITION_SIZE_USDC`
- `latency_arb._trade_updown()` — `settings.MAX_POSITION_SIZE_USDC` passed to `_execute_signal`
- `market_maker._place_quotes()` — `size = settings.MAX_POSITION_SIZE_USDC`
- `market_maker._cancel_quotes()` — `size = settings.MAX_POSITION_SIZE_USDC`
- `ai_sentiment._process_news_item()` — `size_usdc = settings.MAX_POSITION_SIZE_USDC`
- `copy_trader.py` — no change; uses `size_usdc * COPY_RATIO` (scales target wallet size, not a fixed amount)

---

## Session 11 — Position Resolver (PnL Graph)

### Feature: Automatic position resolution with PnL calculation
**Problem:** The dashboard PnL graph never drew because positions were never closed — no `status="closed"` trades with non-zero PnL existed in the DB.
**Root cause:** Nothing was checking whether markets had resolved. Positions stayed in the portfolio forever.

### Schema: `token_id` added to `OpenPosition`
**Files:** `database/models.py`, `database/db.py`
- Added `token_id = Column(String, default="")` to `OpenPosition` model
- `upsert_position()` and `get_open_positions()` now include `token_id`
- Startup migration: `ALTER TABLE open_positions ADD COLUMN token_id TEXT DEFAULT ''`
- New DB helper `update_trades_for_market(market_id, fill_price, pnl)` — sets all non-terminal trades for a market to `status="closed"` with the final fill price and PnL

### Portfolio: `token_id` threaded through
**File:** `core/portfolio.py`
- `Position` dataclass gains `token_id: str` field
- `add_position()` requires `token_id` and passes it to `upsert_position()`
- `update_prices()` passes `pos.token_id` to `upsert_position()`

### Strategy call sites updated
**Files:** `strategies/latency_arb.py`, `strategies/ai_sentiment.py`, `strategies/copy_trader.py`
- All three `add_position()` calls now pass `token_id=token_id`

### Resolver: `_maybe_resolve_positions()` added to Heartbeat
**File:** `bot/heartbeat.py`
- Runs every 5 minutes (reuses `SNAPSHOT_INTERVAL = 300`)
- For each open position with a `token_id`, fetches `get_midpoint(token_id)` via executor
- `mid ≥ 0.99` → **WON**: `fill_price=1.0`, `pnl = size × (1 - entry_price)`
- `mid ≤ 0.01` → **LOST**: `fill_price=0.0`, `pnl = -(size × entry_price)`
- Calls `db.update_trades_for_market(...)`, `portfolio.close_position(...)`, `risk.record_fill(pnl)`
- Next 5-min PnL snapshot picks up the closed PnL → dashboard graph draws

---

## Session 12 — Price Sanity Check + PnL Snapshot Fix

### Feature: Near-zero price sanity check in `_compute_signal()`
**File:** `strategies/latency_arb.py` — `_compute_signal()`
**Change:** Added a second guard before the existing 0.15/0.85 edge filter:
```python
if poly_price < 0.05 or poly_price > 0.95:
    return None
```
**Reason:** Prices below 0.05 or above 0.95 indicate a market that is already fully decided — the outcome is virtually certain and trading would be buying a resolved token at near-zero (or near-one) price. The existing 0.15/0.85 filter handles "no edge" cases; this new check handles "invalid/decided market" cases.

### Fix: PnL snapshot now uses risk manager's live daily PnL
**Files:** `core/risk_manager.py`, `bot/heartbeat.py`
**Problem:** `_maybe_snapshot()` called `db.get_daily_pnl()` and `db.get_cumulative_pnl()`, which read from the database. But the position resolver records PnL via `risk.record_fill(pnl)`, which updates the risk manager's in-memory `_daily_pnl` counter. If `db.get_daily_pnl()` calculates differently or lags, the graph would not reflect the resolver's fills.
**Fix:**
- Added `get_daily_pnl() -> float` to `RiskManager` — returns `self._daily_pnl` (always current, in-memory)
- Added `get_cumulative_pnl() -> float` to `RiskManager` — delegates to `db.get_cumulative_pnl()` (DB is authoritative for cumulative since it persists across restarts)
- `_maybe_snapshot()` in heartbeat now calls `self._risk.get_daily_pnl()` and `self._risk.get_cumulative_pnl()` instead of reading from DB directly

---

## Session 13 — Milestone Fallback Removed + PnL Graph Fix

### Refactor: Removed milestone market fallback from LatencyArb
**File:** `strategies/latency_arb.py`
**Reason:** Latency arb should only trade `btc-updown-5m` markets. The milestone fallback (BTC/ETH/SOL price target markets) was a different strategy type and was adding noise.
**Removed:** `_trade_milestone_markets()`, `_compute_signal()`, `_scan_crypto_markets()`, `_ASSET_TO_SYMBOL` constant, `_crypto_markets` and `_last_market_scan` state, initial `_scan_crypto_markets()` call in `run()`, 10-min market scan refresh in `_tick()`, `extract_clob_token_id` and `pct_change` imports.
**Changed:** `_tick()` now returns early (skips the tick) when `self._current_updown is None` instead of falling back. The 30s momentum diagnostic log still runs even when no updown market is active (so the feed remains observable).
**Docstring** updated to reflect updown-5m only behavior.

### Bug: PnL graph always showing 0
**File:** `database/db.py` — `get_daily_pnl()` and `get_cumulative_pnl()`
**Root cause:** Both functions filtered `Trade.status == "filled"`. The position resolver closes trades with `status="closed"` (not "filled"). So every resolved trade's PnL was invisible to both queries — `get_cumulative_pnl()` returned 0, `risk.get_cumulative_pnl()` delegated to it and also returned 0, and `_maybe_snapshot()` saved `cumulative_pnl=0` to the snapshot table. The graph draws from snapshots, so it was flat at 0 regardless of actual PnL.
**Fix:** Changed both filters to `Trade.status.in_(["filled", "closed"])` so resolved trades are included in all PnL calculations. This also fixes the "daily_pnl" and "cumulative_pnl" values returned by the `/api/pnl` dashboard endpoint.

---

## Session 14 — Gamma API Fallback for Expired Markets

### Bug: Position resolver fails with 404 for all 66 stuck positions
**File:** `bot/heartbeat.py` — `_maybe_resolve_positions()`
**Root cause:** `btc-updown-5m` markets expire after 5 minutes. Once expired, the CLOB API's `get_midpoint()` returns a 404 / "No orderbook exists" error. The resolver caught all exceptions with a `continue`, so every expired position was silently skipped — positions never closed, PnL never recorded.

### Fix: Gamma API fallback resolver
**File:** `bot/heartbeat.py` — new `_resolve_via_gamma()` method
**What it does:** When `get_midpoint()` raises any exception, instead of skipping, the resolver falls back to the Gamma API:
```
GET https://gamma-api.polymarket.com/markets?clob_token_ids={token_id}
```
Reads `outcomePrices` from the response:
- `prices[0] >= 0.99` → YES/UP token resolved to 1 → **WON** (`pnl = size × (1 - entry_price)`)
- `prices[0] <= 0.01` → YES/UP token resolved to 0 → **LOST** (`pnl = -(size × entry_price)`)
- `closed=true` with no readable `outcomePrices` → **LOST conservatively** (with a warning log)
- Gamma API error / market not found / still active → returns `(None, None, None)` → position skipped for this cycle

**Structural change:** `_maybe_resolve_positions()` now opens an `aiohttp.ClientSession` once per resolve cycle and passes it to `_resolve_via_gamma()` for each position (avoids creating a new session per position).

**Imports added:** `aiohttp`, `Optional`, `Tuple` from typing.

---

## Session 18 — Multi-Market Trading, Loss Cooldown, Config Tightening

### Change 1: Edge filter tightened (strategies/latency_arb.py)
Old: `mid < 0.15 or mid > 0.85` → New: `mid < 0.20 or mid > 0.65`
Rationale: at 0.80 entry, max win ~$4 vs $20 loss. At 0.65 entry: ~$14 win vs $20 loss.

### Change 2: Position size scaled by entry price (strategies/latency_arb.py)
- `mid > 0.55` → half size (`base_size * 0.5`)
- `mid ≤ 0.55` → full size (`base_size`)
Trades above 0.65 are blocked by the edge filter so half-size applies only to 0.55–0.65.

### Change 3: Generalised updown market fetch (data/market_scanner.py)
- New `get_updown_market_for(asset, window_minutes)` — parameterised version:
  calculates `ts = (now // (window*60)) * (window*60)`, builds slug
  `{asset}-updown-{window}m-{ts}`, tries current + next interval, same token parse logic
- `get_updown_market()` now delegates: `return await self.get_updown_market_for("btc", 5)`

### Change 4: Trade BTC 5m and BTC 15m in parallel (strategies/latency_arb.py)
- Replaced `self._current_updown` (single) with `self._current_updowns: dict[str, dict|None]` keyed `"BTC_5m"` / `"BTC_15m"`
- Added `self._last_market_fetch: dict[str, float]` per key
- `self.MAX_CONCURRENT = 3` cap — tick exits early if `len(portfolio.all_positions()) >= 3`
- `_refresh_updown_market()` replaced by `_refresh_all_markets()` — iterates configs, refreshes each key when >60s stale
- `_tick()` loops over both markets, using `slug` as the cooldown/dedup key
- `_trade_updown(updown, asset, momentum)` — new signature; `updown` is the market dict

### Change 5: Consecutive loss cooldown (strategies/latency_arb.py + bot/heartbeat.py + bot/engine.py)
- `LatencyArb` gains `_consecutive_losses: int`, `_loss_cooldown_until: float`
- `_tick()` returns early if `time.time() < self._loss_cooldown_until`
- `on_win()` resets counter; `on_loss()` increments — at 2 consecutive losses, sets 10-min pause
- `Heartbeat.__init__` accepts optional `latency_arb` param; after each `risk.record_fill(pnl)`, calls `on_win()` or `on_loss()` if not None
- `engine.py` finds the latency_arb instance from the strategies list and passes it to Heartbeat

### Change 6: New settings (config/settings.py)
- `LAB_MAX_CONCURRENT_POSITIONS: int` (default 3)
- `LAB_WINDOWS: list[int]` (default [5, 15])

### Change 7: .env updates
- `LAB_MOMENTUM_THRESHOLD`: 0.001 → 0.002 (0.2%)
- `DAILY_LOSS_CAP_USDC`: 100 → 60
- Added `LAB_MAX_CONCURRENT_POSITIONS=3`
- Added `LAB_WINDOWS=5,15`
- `DRY_RUN=true` unchanged

---

## Session 17 — Gamma API outcomePrices Parse Fix

### Bug: Every position marked LOST due to silent parse failure
**File:** `bot/heartbeat.py` — `_resolve_via_gamma()`
**Root cause:** The Gamma API returns `outcomePrices` as a JSON-encoded string (e.g. `'["1", "0"]'`), not a Python list. The code did `[float(p) for p in outcome_prices]` directly on the string, which iterates characters — failing the `prices[0] >= 0.99` and `prices[0] <= 0.01` checks silently. Execution fell through to the `if closed:` conservative-LOST branch, marking every expired position as LOST regardless of actual outcome.

**Fix:** Added `json.loads()` before iterating:
```python
if isinstance(outcome_prices, str):
    outcome_prices = json.loads(outcome_prices)
prices = [float(p) for p in outcome_prices]
```
The existing `(ValueError, IndexError)` catch already handles malformed JSON strings.

**Also:** Added `import json` to imports. Removed the temporary `_gamma_debug_done` debug flag and its associated `logger.info` call that were added to diagnose this issue.

---

## Session 16 — Restore Open Positions on Startup

### Fix: In-memory portfolio empty after restart
**File:** `bot/engine.py` — `run()`
**Problem:** On restart, `portfolio.all_positions()` returned an empty list because positions are only tracked in memory. The resolver loop had nothing to iterate over, so 66+ stuck positions from previous runs would never resolve.
**Fix:** After the singletons are created, call `db.get_open_positions()` and restore each row into the in-memory portfolio via `portfolio.add_position()`. Logs the count of restored positions at startup.

```python
saved_positions = db.get_open_positions()
for p in saved_positions:
    portfolio.add_position(
        market_id=p["market_id"], token_id=p["token_id"],
        question=p["question"], strategy=p["strategy"],
        side=p["side"], size=p["size"], entry_price=p["entry_price"],
    )
if saved_positions:
    logger.info(f"Restored {len(saved_positions)} open position(s) from database")
```

**Note:** `add_position()` calls `db.upsert_position()` internally — this is an idempotent merge, not a duplicate insert.

---

## Session 15 — Fast-Path Resolver for Expired Positions

### Fix: Skip CLOB for positions older than 10 minutes
**File:** `bot/heartbeat.py` — `_maybe_resolve_positions()`
**Problem:** For positions from expired markets, the resolver was calling `get_midpoint()` (which returns 404), waiting for the exception, then falling back to Gamma — incurring a redundant failed CLOB call for every historical position on every resolver cycle.
**Fix:** Added an age check using `pos.opened_at`. If the position is more than 10 minutes old (`time.time() - pos.opened_at > 600`), the CLOB block is skipped entirely. `fill_price is None` after the conditional block acts as the shared fallthrough to Gamma for both the "position too old" and "CLOB raised" cases.

**Before:**
```python
try:
    mid = await loop.run_in_executor(None, self._client.get_midpoint, pos.token_id)
    # ... resolve from mid ...
except Exception:
    # Fall back to Gamma
    fill_price, pnl, outcome = await self._resolve_via_gamma(session, pos)
    if fill_price is None:
        continue
```

**After:**
```python
if time.time() - pos.opened_at <= 600:
    try:
        mid = await loop.run_in_executor(None, self._client.get_midpoint, pos.token_id)
        # ... resolve from mid ...
    except Exception:
        pass  # fall through to Gamma below

if fill_price is None:
    fill_price, pnl, outcome = await self._resolve_via_gamma(session, pos)
    if fill_price is None:
        continue
```

**Effect:** All 66+ stuck historical positions now go straight to Gamma on the next resolver tick instead of hammering the CLOB API with 404s first.

---

## Session 19 — Dashboard Win Rate Fix

### Bug: Win Rate showing 0.0% despite closed trades
**File:** `dashboard/api/routes.py` — win rate calculation
**Root cause:** The closed-trade filter used `t["status"] == "filled"`. The position resolver closes trades with `status="closed"`, not `"filled"`. So every resolved trade was excluded from the closed-trade list, making `len(closed) == 0` and win rate = 0.0%.
**Fix:**
- Changed filter to `t["status"] in ("filled", "closed")` — both statuses count as resolved
- Added `float()` cast around `t["pnl"]` for robustness (DB can return string if read raw)
- Renamed intermediate variable to `closed` for clarity
- Result: a trade with PnL=+10.08 and a trade with PnL=-20.00 now correctly shows 50.0% win rate

---

## Session 20 — btc_price None Guard

### Bug: Potential format crash when price feed not yet populated
**File:** `strategies/latency_arb.py` — `_tick()`
**Root cause:** `self._exchange_feed.get_price("BTC/USDT")` returns `None` before the Coinbase feed has fetched its first price (can take 1-2 seconds after startup). The log line `f"BTC={btc_price:.2f}"` would raise `TypeError: unsupported format character` when `btc_price` is `None`.
**Fix:** Added `or 0.0` guard on the assignment:
```python
btc_price = self._exchange_feed.get_price("BTC/USDT") or 0.0
```
The log line is unchanged; the guard is on the variable, not in the f-string.

**Also:** User manually set `MARKET_MAKER_ENABLED=false` in `.env` (was `true`). Market Maker strategy is now disabled.

---

## Session 21 — Momentum-Scaled Position Sizing

### Feature: Position size scaled by both price AND momentum strength
**File:** `strategies/latency_arb.py` — `_trade_updown()`
**Replaced:** Simple one-liner `size_usdc = base_size * 0.5 if mid > 0.55 else base_size`
**With:** Three-step combined logic:

```python
# Step 1 — price multiplier (existing logic, unchanged)
if mid > 0.55:
    price_mult = 0.5   # borderline entry, reduce risk
else:
    price_mult = 1.0   # good entry, full size

# Step 2 — momentum multiplier (new)
abs_momentum = abs(momentum)
if abs_momentum >= 0.004:
    momentum_mult = 2.0   # very strong signal → double
elif abs_momentum >= 0.003:
    momentum_mult = 1.5   # strong signal → 1.5x
else:
    momentum_mult = 1.0   # normal signal → base

# Step 3 — combine and cap at 2x base_size
size_usdc = min(base_size * price_mult * momentum_mult, base_size * 2)
```

**Effective sizes at `MAX_POSITION_SIZE_USDC=$20`:**
| Entry | Momentum | Size |
|---|---|---|
| Good (0.20–0.55) | normal (0.002–0.003) | $20 |
| Good (0.20–0.55) | strong (0.003–0.004) | $30 |
| Good (0.20–0.55) | very strong (0.004+) | $40 |
| Thin (0.55–0.65) | normal | $10 |
| Thin (0.55–0.65) | strong | $15 |
| Thin (0.55–0.65) | very strong | $20 (capped) |

**Also added:** `logger.info()` line after size calc so every trade logs `base, price_mult, momentum_mult → size` for auditability.

---

## Session 22 — Loss Cooldown Log Throttle

### Fix: Loss cooldown log was firing every 500ms tick
**File:** `strategies/latency_arb.py` — `_tick()`
**Problem:** While the loss cooldown was active, the log line `"LatencyArb: loss cooldown active..."` fired on every 500ms tick — 120+ log lines per minute of cooldown.
**Fix:**
- Added `self._last_cooldown_log: float = 0.0` to `__init__`
- Wrapped the log line in a 60-second guard:
```python
if time.time() < self._loss_cooldown_until:
    if time.time() - self._last_cooldown_log >= 60:
        self._last_cooldown_log = time.time()
        logger.info(f"LatencyArb: loss cooldown active, resuming at {self._loss_cooldown_until:.0f}")
    return
```
Cooldown is still enforced on every tick; the log is now at most once per minute.

---

## Session 23 — Time-Remaining Window Check + Resolver Interval Split

### Feature: Skip trades when window is nearly expired
**File:** `strategies/latency_arb.py` — `_trade_updown()`
**Motivation:** Entering a 5m updown market with only 30 seconds left gives no time for the thesis to play out. Entering a 15m market with 2 minutes left is similarly pointless.
**Added:** Before the midpoint fetch, compute time remaining in the current window and bail if below the minimum:
```python
slug = updown["slug"]
min_remaining = 300 if "15m" in slug else 60
window_secs = 900 if "15m" in slug else 300
ts = (int(time.time()) // window_secs) * window_secs
seconds_remaining = ts + window_secs - time.time()
if seconds_remaining < min_remaining:
    logger.info(f"LatencyArb: skipping {slug} — only {seconds_remaining:.0f}s remaining in window")
    return
```
- **5m markets:** skip if <60 seconds remaining (last 20% of window)
- **15m markets:** skip if <300 seconds remaining (last 33% of window)
- Window size detected from slug name (`"15m" in slug`)

### Feature: Resolver runs every 60s, separate from 5-min PnL snapshot
**File:** `bot/heartbeat.py`
**Problem:** Both the PnL snapshot and position resolver shared `SNAPSHOT_INTERVAL = 300`. A 5m updown market that resolves at minute 4 might not be detected for up to 5 minutes.
**Fix:**
- Added `RESOLVER_INTERVAL = 60` class constant
- `_maybe_resolve_positions()` now checks `now - self._last_resolve < self.RESOLVER_INTERVAL` (60s) instead of `SNAPSHOT_INTERVAL` (300s)
- PnL snapshots unchanged at 5 minutes; resolver now runs independently every 60 seconds
- A resolved 5m market is now detected within 1 minute of resolution instead of up to 5 minutes

---

## Session 24 — Log Noise Reduction

### Change: ExchangeFeed price rotation log demoted to DEBUG
**File:** `data/exchange_feed.py` — `_fetch_all()`
**Before:** `logger.info(f"ExchangeFeed {symbol}: price=...")` — logged every 60s per symbol (3 symbols = 3 lines/min)
**After:** `logger.debug(f"ExchangeFeed {symbol}: price=...")` — only visible when `LOG_LEVEL=DEBUG`
**Reason:** With `LOG_LEVEL=INFO` (default), these lines added ~180 log lines/hour of pure noise. The feed being alive is already observable from LatencyArb's 30s momentum log.

### Change: MarketScanner slug log deduplication
**File:** `data/market_scanner.py` — `get_updown_market_for()`
**Before:** `logger.info(f"Updown market found: {slug}")` — logged every 60s when the same slug was re-fetched (same market, same window)
**After:** Only logs when the slug has changed from the last seen slug for that key:
```python
if self._last_logged_slug.get(slug) != slug:
    self._last_logged_slug[slug] = slug
    logger.info(f"Updown market found: {slug}")
```
**Added:** `self._last_logged_slug: dict[str, str] = {}` to `MarketScanner.__init__`
**Effect:** Logs once when a new window opens (slug changes), silent on repeated fetches of the same slug. When the window rolls over (e.g. `btc-updown-5m-1743012300` → `btc-updown-5m-1743012600`), it logs once again.

---

## Session 25 — Staggered 15m Entry (60-Second Confirmation)

### Feature: 15m updown market now requires 60-second momentum confirmation before entry
**File:** `strategies/latency_arb.py`
**Motivation:** 5m markets benefit from fast entry (signal → trade immediately). 15m markets have more time but also more noise — requiring momentum to hold for 60 seconds filters false spikes before committing capital.

### New state variables in `__init__`
- `_pending_15m: dict[str, dict]` — keyed by slug; value is `{momentum, direction, triggered_at, window_slug}`
- `_entered_15m_slugs: set[str]` — slugs where a 15m trade was actually executed; prevents re-entry in the same window
- `_last_15m_slug: str` — tracks the current 15m window slug to detect rollover

### `_tick()` restructured
- 5m market: identical to before — immediate entry on signal
- 15m market: calls `_queue_15m_pending()` instead of `_trade_updown()` directly
- `_check_pending_15m()` now runs every tick, placed **before** the open-count early return so stale entries are cleaned up even when at `MAX_CONCURRENT`

### New method: `_queue_15m_pending(updown, momentum)`
Records a 15m signal. Silently skips if:
- Slug already in `_entered_15m_slugs` (already traded this window)
- Slug already in `_pending_15m` (already queued — let confirmation handle it)
- < 90s remaining in the window (too late to confirm + trade meaningfully)

Logs: `"LatencyArb: 15m pending queued — {slug} direction=UP/DOWN momentum=+X.XX% (Ns remaining)"`

### New method: `_check_pending_15m(momentum, can_trade)`
Runs every tick. For each pending entry:
- **Window changed** → log `"15m pending expired — window changed"`, discard
- **< 60s elapsed** → keep waiting
- **≥ 60s, momentum failed or direction reversed** → log `"15m pending discarded — momentum failed"`, discard. Slug is NOT added to `_entered_15m_slugs` — a fresh strong signal later in the same window can start a new confirmation attempt from zero
- **< 90s remaining** → log `"15m pending discarded — only Xs remaining"`, discard
- **At MAX_CONCURRENT** → keep entry in pending, retry next tick (does not discard)
- **All checks pass** → log `"15m confirmed after Xs — entering {slug}"`, call `_trade_updown()`, add slug to `_entered_15m_slugs`, remove from pending

### New method: `_cleanup_15m_entered_slugs()`
Runs every tick. When the 15m window rolls over (current slug ≠ `_last_15m_slug`), discards the old slug from `_entered_15m_slugs`. Keeps the set bounded to at most 1 entry in steady state.

### `_trade_updown()` — min_remaining for 15m changed
- Before: `min_remaining = 300 if "15m" in slug else 60`
- After: `min_remaining = 90 if "15m" in slug else 60`
- Rationale: confirmed 15m entries enter after 60s of validation; 90s remaining is sufficient time for the trade to resolve meaningfully. The old 300s guard was for immediate (unconfirmed) entry and is no longer appropriate.

---

## Session 26 — Configurable 15m Confirmation + Fast-Track + Strengthen

### Feature: Configurable 15m confirmation parameters
**Files:** `config/settings.py`, `.env`
Three new settings replacing hardcoded values:
- `LAB_15M_CONFIRM_SECONDS=30` — confirmation wait window (was hardcoded 60s)
- `LAB_15M_CONFIRM_RETENTION=0.6` — confirmed if momentum >= threshold × 0.6 (allows slight fade)
- `LAB_15M_FASTTRACK_MULTIPLIER=2.5` — skip queue entirely if momentum >= threshold × 2.5

### Feature: Fast-track path in `_queue_15m_pending()`
**File:** `strategies/latency_arb.py`
If `abs(momentum) >= LAB_MOMENTUM_THRESHOLD * LAB_15M_FASTTRACK_MULTIPLIER`, skips the confirmation queue entirely and calls `_trade_updown()` immediately. This required converting `_queue_15m_pending()` to `async` and updating the call site in `_tick()` to `await self._queue_15m_pending(...)`.
Logs: `"LatencyArb: 15m FAST-TRACK entry — {slug} direction={dir} momentum={val} (>= {mult}x threshold)"`

### Feature: Strengthen logic in `_queue_15m_pending()`
**File:** `strategies/latency_arb.py`
If a slug is already pending in the same direction and new momentum is stronger:
- Updates the stored momentum value
- Resets `triggered_at` only if new `abs(momentum) >= old * 1.5` (significant jump, not minor noise)
Logs: `"LatencyArb: 15m pending STRENGTHENED — {slug} {old} → {new} (timer reset)"` — only if delta ≥ 0.00005 (see Session 28)
Queue log changed to uppercase: `"15m pending QUEUED"`

### Change: `_check_pending_15m()` uses configurable thresholds
**File:** `strategies/latency_arb.py`
- `elapsed < 60` → `elapsed < settings.LAB_15M_CONFIRM_SECONDS`
- Full-threshold confirmation → `threshold * LAB_15M_CONFIRM_RETENTION` (allows slight momentum fade)
- Split discard log into two distinct cases:
  - `"15m pending DISCARDED — direction reversed for {slug} (was X, now Y)"`
  - `"15m pending DISCARDED — momentum too weak for {slug} (needed {X}, got {Y})"`
- Confirmation log changed to uppercase: `"15m CONFIRMED after Xs"`

---

## Session 27 — Binance WebSocket Feed + Order Book Imbalance Filter

### Refactor: Exchange feed replaced Coinbase REST with Binance WebSocket
**File:** `data/exchange_feed.py` — full rewrite
**Reason:** Coinbase REST polling had 1-second granularity and no order book data. Binance public WebSocket provides sub-100ms trade updates with no API key.
- **Primary:** `wss://stream.binance.com:9443/ws/btcusdt@aggTrade` — real-time trades, field `p` = price string
- **Fallback:** Kraken HTTP (`https://api.kraken.com/0/public/Ticker?pair=XBTUSD`) — activates if WebSocket produces no prices within 10 seconds
- Auto-reconnect on disconnect with 5-second delay
- Same `get_price()` / `get_momentum()` public interface — rest of codebase unchanged
- `_MOMENTUM_WINDOW = 30.0` seconds of trade history for momentum

### Feature: Binance order book depth stream
**File:** `data/exchange_feed.py`
- New constant: `_BINANCE_OB_STREAM = "wss://stream.binance.com:9443/ws/btcusdt@depth20@1000ms"` — top-20 levels, 1s updates
- New `__init__` fields: `_ob_task: Optional[asyncio.Task]`, `_ob_imbalance: dict[str, float | None] = {}`
- New public method: `get_order_book_imbalance(symbol) → float | None` — returns `None` until first message arrives (startup safety)
- `run()` now launches `_run_orderbook_loop()` via `asyncio.ensure_future()` before entering `_main_loop()`
- `stop()` now iterates `(self._task, self._ob_task)` and cancels both
- Formula: `(bid_vol - ask_vol) / (bid_vol + ask_vol)` — range [-1.0, +1.0], positive = bid-heavy

### New methods: `_run_orderbook_loop()`, `_listen_orderbook()`, `_handle_orderbook_message()`
Same reconnect pattern as aggTrade loop. OB has no HTTP fallback — `_ob_imbalance` stays `None` if stream fails (filter skips gracefully).

### Feature: OB imbalance filter in `_trade_updown()`
**Files:** `config/settings.py`, `.env`, `strategies/latency_arb.py`
- New settings: `LAB_OB_IMBALANCE_ENABLED=true`, `LAB_OB_IMBALANCE_THRESHOLD=0.10`
- Filter inserted in `_trade_updown()` just before `_execute_signal()` — covers all 3 entry paths (5m immediate, 15m confirmed, 15m fast-track) without duplication
- Filter skipped during startup when `imbalance is None`
- `direction == "UP"`: requires `imbalance >= +0.10`; `direction == "DOWN"`: requires `imbalance <= -0.10`
- On skip: `"LatencyArb: SKIPPED {slug} — OB imbalance too weak for {dir} (imbalance={val} needed{op}{threshold})"`

### Change: Tick log updated
**File:** `strategies/latency_arb.py` — `_tick()`
Added `OB={value}` to the 30s momentum log (shows "None" during startup):
```
LatencyArb tick: BTC=95432.00 momentum=+0.2100% OB=+0.143 threshold=0.100%
```

---

## Session 28 — STRENGTHENED Log Delta Filter

### Fix: STRENGTHENED log firing on noise (sub-threshold momentum changes)
**File:** `strategies/latency_arb.py` — `_queue_15m_pending()`
**Problem:** The STRENGTHENED condition fired whenever `abs(new) > abs(old)` — even a 0.0001% change qualified, producing dozens of noisy duplicate log lines per minute while momentum fluctuated near-threshold.
**Fix:** Added minimum delta guard to the condition:
```python
if (
    existing["direction"] == direction
    and abs(momentum) > abs(existing["momentum"])
    and abs(momentum) - abs(existing["momentum"]) >= 0.00005
):
```
STRENGTHENED log (and momentum update) only fires when the new value differs by at least 0.005% (0.00005 in decimal). Sub-noise fluctuations are silently ignored; the pending entry's stored momentum and timer are unchanged.

---

## Current State

### Bot starts cleanly and:
- Connects to Polymarket CLOB API
- Reads real USDC balance from Polygon via web3
- Fetches 500 active markets from Gamma API
- Finds live `btc-updown-5m` and `btc-updown-15m` markets every 60s; logs only when slug changes
- Reads real BTC price via **Binance aggTrade WebSocket** (`btcusdt@aggTrade`) — no API key needed; Kraken HTTP fallback activates after 10s if WS unavailable
- Reads real-time **order book imbalance** via Binance depth20 WebSocket (`btcusdt@depth20@1000ms`) — separate concurrent task, same reconnect pattern
- Latency Arb fires DRY RUN BUY/SELL signals when BTC moves >0.1% in 30s
  - **5m market:** enters immediately on signal
  - **15m market:** three paths:
    - **Fast-track** (`momentum >= threshold × 2.5`): skips queue, enters immediately
    - **Confirmed** (`momentum >= threshold × 0.6` sustained for 30s): enters after confirmation
    - **Discarded**: if direction reverses, momentum fades below 60% of threshold, window expires, or <90s remaining
  - **OB imbalance filter:** before every entry, requires `imbalance >= +0.10` for UP trades, `<= -0.10` for DOWN trades; skipped during startup (imbalance=None)
  - Max 3 concurrent positions; `_entered_15m_slugs` prevents re-entry in the same 15m window
  - Edge filter: only trades when `0.20 ≤ mid ≤ 0.65`
  - Skips entries if <60s remaining in 5m window, <90s in 15m window
  - Position size = `base × price_mult × momentum_mult`, capped at 2×base ($40 max)
  - Every trade logs the size calculation breakdown
- 2 consecutive losses trigger a 10-minute trading pause (cooldown log throttled to once/min)
- Per-tick dedup + 5-minute cooldown (keyed by slug) prevent 5m order flooding
- Dry-run trades saved to SQLite with `dry_run=true` flag and actual fill price — dashboard shows simulated trade history
- Dashboard win rate correctly counts both `status="filled"` and `status="closed"` trades
- All strategies use `MAX_POSITION_SIZE_USDC` from `.env` as the single order-size control
- Position resolver runs every **60 seconds** (independent of 5-min PnL snapshot): tries CLOB midpoint first (skipped for positions >10 min old), falls back to Gamma API `outcomePrices` for expired markets, marks LOST conservatively if closed with no outcome data
- PnL graph draws correctly: `db.get_daily_pnl()` / `get_cumulative_pnl()` include both `status="filled"` and `status="closed"` trades
- Dashboard live at `http://127.0.0.1:8080`
- Shuts down cleanly on Ctrl+C

### Strategy toggles (current `.env`)
```
LATENCY_ARB_ENABLED=true
MARKET_MAKER_ENABLED=false      ← disabled by user
AI_SENTIMENT_ENABLED=false      ← disabled (no Anthropic key)
COPY_TRADER_ENABLED=false       ← disabled (no target wallets)
DRY_RUN=true
LAB_MOMENTUM_THRESHOLD=0.001    ← 0.1%
LAB_MAX_CONCURRENT_POSITIONS=3
LAB_WINDOWS=5,15
LAB_15M_CONFIRM_SECONDS=30
LAB_15M_CONFIRM_RETENTION=0.6
LAB_15M_FASTTRACK_MULTIPLIER=2.5
LAB_OB_IMBALANCE_ENABLED=true
LAB_OB_IMBALANCE_THRESHOLD=0.10
DAILY_LOSS_CAP_USDC=100
MAX_POSITION_SIZE_USDC=5
```

### Known remaining items
- Allowances not yet run (`setup_allowances.py`) — wallet shows 0 USDC
- No Anthropic API key → AI Sentiment disabled
- No target wallets → Copy Trader disabled
- `DRY_RUN=true` — no real orders placed yet

### One-time setup still needed before live trading
```bash
python scripts/setup_allowances.py   # approve USDC + CTF contracts on-chain
# then in .env:
#   DRY_RUN=false
```

---

## Files Modified (cumulative)

| File | Changes |
|------|---------|
| `core/client.py` | Fixed `get_balance()` to use web3; fixed `ExtraDataToPOAMiddleware` |
| `data/market_scanner.py` | Fixed `acceptingOrders`; fixed rewards field; removed hard rewards gate; added 30-day+volume filter; added `get_crypto_price_candidates()`; fixed indentation bug; added `get_updown_market()`; added `get_updown_market_for(asset, window_minutes)`; `get_updown_market()` now delegates to it; added `_last_logged_slug` dict — slug log only fires when slug changes |
| `data/exchange_feed.py` | Full rewrite: replaced ccxt/Binance with Coinbase REST + Kraken fallback; fixed first-fetch momentum bug; price rotation log demoted to DEBUG; **Session 27:** replaced Coinbase REST with Binance aggTrade WebSocket + Kraken HTTP fallback; added depth20 OB WebSocket as concurrent task; `_ob_imbalance` dict; `get_order_book_imbalance()` public method; `_run_orderbook_loop()`, `_listen_orderbook()`, `_handle_orderbook_message()`; `stop()` cancels both tasks |
| `bot/engine.py` | Added `await exchange_feed.stop()` in shutdown; DB position restore on startup; passes `latency_arb` instance to Heartbeat |
| `strategies/market_maker.py` | Fixed token_id extraction; fixed rewards sort; fixed crypto keyword regex; switched to `get_crypto_price_candidates()` with general fallback |
| `strategies/ai_sentiment.py` | Fixed `acceptingOrders`; fixed token_id extraction |
| `strategies/latency_arb.py` | Fixed `acceptingOrders`; fixed token_id extraction; added updown-5m as sole market source; added Coinbase momentum signal; added `_traded_this_cycle` dedup set; added `_cooldown` 5-min guard; added 30s momentum diagnostic log; removed dry-run early-return from `_execute_signal`; aligned order size to `MAX_POSITION_SIZE_USDC`; passes `token_id` to `add_position()`; removed milestone fallback entirely; edge filter tightened to 0.20/0.65; momentum-scaled sizing (price_mult × momentum_mult capped at 2×base); size log line; multi-market dict (`BTC_5m`/`BTC_15m`); `MAX_CONCURRENT=3` cap; `_refresh_all_markets()`; `on_win()`/`on_loss()` with 10-min pause after 2 consecutive losses; `_last_cooldown_log` throttle (cooldown log once/min); `btc_price or 0.0` None guard; time-remaining window check (5m: skip if <60s, 15m: skip if <90s); staggered 15m entry: `_pending_15m` dict, `_entered_15m_slugs` set, `_last_15m_slug` tracker, `_queue_15m_pending()`, `_check_pending_15m()`, `_cleanup_15m_entered_slugs()` — 15m enters only after 60s confirmed momentum; **Session 26:** `_queue_15m_pending()` converted to async — fast-track path (FASTTRACK_MULTIPLIER), strengthen logic (STRENGTHENED log, timer reset on 1.5× jump), uppercase log labels (QUEUED/STRENGTHENED); `_check_pending_15m()` uses `LAB_15M_CONFIRM_SECONDS` and `LAB_15M_CONFIRM_RETENTION`; split discard logs; **Session 27:** tick log includes OB imbalance; OB imbalance filter in `_trade_updown()` before `_execute_signal()`; **Session 28:** STRENGTHENED condition requires delta ≥ 0.00005 |
| `strategies/market_maker.py` | Fixed token_id extraction; fixed rewards sort; fixed crypto keyword regex; switched to `get_crypto_price_candidates()` with general fallback; aligned order size to `MAX_POSITION_SIZE_USDC` in `_place_quotes` and `_cancel_quotes` |
| `strategies/ai_sentiment.py` | Fixed `acceptingOrders`; fixed token_id extraction; aligned order size to `MAX_POSITION_SIZE_USDC`; passes `token_id` to `add_position()` |
| `strategies/copy_trader.py` | Passes `token_id` to `add_position()` |
| `utils/helpers.py` | Added `extract_clob_token_id()` helper |
| `config/settings.py` | Changed default RPC to `polygon-bor-rpc.publicnode.com`; added `LAB_MAX_CONCURRENT_POSITIONS`, `LAB_WINDOWS`; **Session 26:** added `LAB_15M_CONFIRM_SECONDS`, `LAB_15M_CONFIRM_RETENTION`, `LAB_15M_FASTTRACK_MULTIPLIER`; **Session 27:** added `LAB_OB_IMBALANCE_ENABLED`, `LAB_OB_IMBALANCE_THRESHOLD` |
| `scripts/setup_allowances.py` | Fixed `ExtraDataToPOAMiddleware` import |
| `database/models.py` | Added `dry_run` Boolean to `Trade`; added `token_id` String to `OpenPosition` |
| `database/db.py` | Added `dry_run` to `insert_trade()` and `_trade_to_dict()`; added `token_id` to `upsert_position()` and `get_open_positions()`; added `update_trades_for_market()`; startup migrations for both new columns; fixed `get_daily_pnl()` and `get_cumulative_pnl()` to include `status="closed"` trades |
| `core/order_manager.py` | Added dry-run branch to `place_limit_order()`; dry-run `place_market_order()` saves to DB with actual fill price; added `price` param to `place_market_order()` |
| `core/portfolio.py` | Added `token_id` to `Position` dataclass and `add_position()`; passes `token_id` through to `upsert_position()` |
| `bot/heartbeat.py` | Added `_maybe_resolve_positions()` — position resolver using midpoint threshold; `_maybe_snapshot()` now uses `risk.get_daily_pnl()` / `risk.get_cumulative_pnl()`; added `_resolve_via_gamma()` Gamma API fallback for expired markets; positions >10 min old skip CLOB and go straight to Gamma; fixed `outcomePrices` string→list parse via `json.loads()`; accepts optional `latency_arb` param; calls `on_win()`/`on_loss()` after each resolved position; added `RESOLVER_INTERVAL=60` separate from `SNAPSHOT_INTERVAL=300` — resolver now runs every 60s |
| `bot/engine.py` | Added DB position restore on startup: loads `db.get_open_positions()` and calls `portfolio.add_position()` for each before strategies start |
| `core/risk_manager.py` | Added `get_daily_pnl()` and `get_cumulative_pnl()` public methods |
| `dashboard/api/routes.py` | Fixed win rate: `status == "filled"` → `status in ("filled", "closed")`; added `float()` cast on `pnl` |
| `.env` | `LAB_MOMENTUM_THRESHOLD` 0.001→0.002; `DAILY_LOSS_CAP_USDC` 100→60; added `LAB_MAX_CONCURRENT_POSITIONS=3`, `LAB_WINDOWS=5,15`; `MARKET_MAKER_ENABLED` true→false; **Session 26:** added `LAB_15M_CONFIRM_SECONDS=30`, `LAB_15M_CONFIRM_RETENTION=0.6`, `LAB_15M_FASTTRACK_MULTIPLIER=2.5`; **Session 27:** added `LAB_OB_IMBALANCE_ENABLED=true`, `LAB_OB_IMBALANCE_THRESHOLD=0.10` |
