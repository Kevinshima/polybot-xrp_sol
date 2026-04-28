"""
Synthetic arbitrage strategy — FAST CAPITAL RECYCLING version.

When YES_ask + NO_ask < $1.00 (after fees), buy both legs for a
guaranteed profit regardless of outcome — zero directional risk.

Two exit paths (in order of preference):
  1. FAST  — After both FOK fills confirm, call CTF.mergePositions() on-chain
             to immediately convert YES+NO tokens back into USDC collateral.
             Capital recycled in seconds, not weeks.
  2. SLOW  — If merge fails, hold and poll Gamma API. Close only when the
             market is officially marked closed=true. Never close on a
             price-threshold heuristic.

Key changes vs. previous version:
  FIX 2 — Rejects markets with missing or unparseable end dates (were silently
           allowed before; the 7-day filter was effectively disabled for any
           market whose date field failed to parse).
  FIX 3 — Gap detection uses executable ask-side order-book depth, NOT midpoints.
           Midpoints are still used as a cheap pre-filter to avoid fetching 500
           order books; the actual entry decision uses real ask prices.
  FIX 4 — Orders use FOK (Fill or Kill) instead of GTC. If a leg cannot fill
           immediately, the order cancels — no passive resting orders that could
           fill hours later when the other side has moved.
  FIX 5 — Independent SynthRiskGuard tracks synth-only daily PnL and halts
           only synth scanning. Losses from latency_arb can no longer prevent
           synth from scanning.
  FIX 6 — Order book depth is checked per leg: if the available size at
           reasonable prices is less than our target, we skip the market.
  FIX 7 — Resolution uses Gamma API closed=true only. The previous code closed
           positions when a token's midpoint crossed 0.99, which is NOT the same
           as official oracle resolution and produced phantom "profits".
  FIX 1 — After both FOK legs fill, immediately calls CTF.mergePositions()
           on-chain to convert P YES + P NO → P USDC without waiting for
           market resolution. If merge fails, the position stays open and is
           retried every RESOLVE_INTERVAL until MAX_MERGE_ATTEMPTS is reached,
           after which we fall through to slow resolution.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from config import settings
from database import db
from strategies.base import BaseStrategy
from utils.helpers import extract_clob_token_ids, round_price
from utils.logger import logger


# ── Module-level singleton reference (used by dashboard API) ──────────────────
_instance: Optional["SynthArb"] = None


def get_synth_arb() -> Optional["SynthArb"]:
    return _instance


# ── FIX 5: Independent risk guard ────────────────────────────────────────────

class SynthRiskGuard:
    """
    Synth-specific daily loss cap and halt flag.

    Completely independent from the shared RiskManager so that losses from
    latency_arb (or any other strategy) cannot halt synth scanning.
    Resets automatically at local midnight.
    """

    def __init__(self, daily_loss_cap: float):
        self._cap = daily_loss_cap
        self._daily_pnl: float = 0.0
        self._halted: bool = False
        self._last_day: int = self._today()

    @staticmethod
    def _today() -> int:
        now = datetime.now()
        return now.year * 10000 + now.month * 100 + now.day

    def _maybe_reset(self) -> None:
        today = self._today()
        if today != self._last_day:
            self._daily_pnl = 0.0
            self._halted = False
            self._last_day = today

    def is_halted(self) -> bool:
        self._maybe_reset()
        return self._halted

    def record_pnl(self, delta: float) -> None:
        self._maybe_reset()
        self._daily_pnl += delta
        if self._daily_pnl <= -self._cap:
            self._halted = True
            logger.critical(
                f"SynthArb: own daily loss cap ${self._cap:.0f} breached "
                f"(${self._daily_pnl:.2f}) — halting synth scans only"
            )

    @property
    def daily_pnl(self) -> float:
        self._maybe_reset()
        return self._daily_pnl


# ── Position dataclass ────────────────────────────────────────────────────────

@dataclass
class SynthPosition:
    trade_id: str
    condition_id: str
    question: str
    yes_token_id: str
    no_token_id: str
    yes_price: float        # average fill price for YES leg (ask-side)
    no_price: float         # average fill price for NO leg (ask-side)
    payout_target: float    # P — shares bought; payout if held to resolution
    total_cost: float       # yes_price * P + no_price * P
    gap_pct: float          # net gap after fees at entry (fraction of P)
    opened_at: float = field(default_factory=time.time)
    merge_attempts: int = 0  # how many on-chain merge attempts have been made


# ── Strategy ──────────────────────────────────────────────────────────────────

class SynthArb(BaseStrategy):
    """
    Scans active Polymarket markets every SCAN_INTERVAL seconds.
    Gap detection: midpoint pre-filter → order-book ask depth check.
    Execution: FOK batch order (both legs simultaneously).
    After fills: attempt on-chain CTF.mergePositions() for instant recycling.
    Fallback: hold to official market close per Gamma API.
    """

    name = "synth_arb"

    SCAN_INTERVAL = 30           # seconds between full market scans
    RESOLVE_INTERVAL = 60        # seconds between resolution + merge-retry checks
    BATCH_SIZE = 50              # token IDs per batch midpoint pre-filter call
    MAX_MARKETS_PER_SCAN = 500   # cap total markets scanned to protect rate limits
    MAX_BOOK_CANDIDATES = 20     # max markets to fetch order books for per scan cycle
    MIDPOINT_PREFILTER = 0.96    # only fetch order books if midpoint sum < this
    MERGE_WAIT_SECS = 6          # seconds after fill confirmation before merge attempt
    MAX_MERGE_ATTEMPTS = 5       # fall back to hold-to-resolution after this many failures

    MAX_FEE_RATE = 0.0315        # Polymarket taker fee peaks at mid=0.50

    # FIX 1: CTF mergePositions ABI (Gnosis Conditional Token Framework)
    _CTF_MERGE_ABI = [
        {
            "name": "mergePositions",
            "type": "function",
            "inputs": [
                {"name": "collateralToken",    "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId",        "type": "bytes32"},
                {"name": "partition",          "type": "uint256[]"},
                {"name": "amount",             "type": "uint256"},
            ],
            "outputs": [],
            "stateMutability": "nonpayable",
        }
    ]

    def __init__(self):
        super().__init__()
        global _instance
        _instance = self

        self._open: dict[str, SynthPosition] = {}
        self._entered_ids: set[str] = set()
        # FIX 5: synth-specific risk guard, independent of the shared RiskManager
        self._synth_risk = SynthRiskGuard(
            daily_loss_cap=settings.SYNTH_DAILY_LOSS_CAP
        )

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info(
            "SynthArb: starting "
            "(ask-based gap detection | FOK orders | merge-on-fill | independent risk)"
        )
        await self._restore_open_positions()

        last_scan = 0.0
        last_resolve = 0.0

        while self._running:
            try:
                # FIX 5: check synth-specific guard first; never check shared is_halted
                if self._synth_risk.is_halted():
                    logger.warning("SynthArb: own daily loss cap hit — skipping scan")
                    await asyncio.sleep(10)
                    continue

                now = time.time()

                if now - last_scan >= self.SCAN_INTERVAL:
                    await self._scan()
                    last_scan = time.time()

                if now - last_resolve >= self.RESOLVE_INTERVAL:
                    await self._check_resolutions_and_retry_merges()
                    last_resolve = time.time()

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"SynthArb loop error: {exc}")

            await asyncio.sleep(2)

    # ── Startup restore ───────────────────────────────────────────────────────

    async def _restore_open_positions(self) -> None:
        """Rebuild in-memory state from DB open trades on startup."""
        try:
            trades = db.get_recent_trades(limit=500)
            open_synth = [
                t for t in trades
                if t.get("strategy") == self.name
                and t.get("status") in ("open", "filled")
            ]
            if not open_synth:
                return

            logger.info(f"SynthArb: restoring {len(open_synth)} open position(s) from DB")

            async with aiohttp.ClientSession() as session:
                for trade in open_synth:
                    cid = trade["market_id"]
                    token_ids = await self._fetch_token_ids_for(session, cid)
                    if len(token_ids) < 2:
                        logger.warning(
                            f"SynthArb: could not restore token IDs for {cid[:20]} — skipping"
                        )
                        continue

                    yes_tok, no_tok = token_ids[0], token_ids[1]
                    entry_sum = float(trade.get("price") or 0.9)
                    P = float(trade.get("size") or settings.SYNTH_POSITION_SIZE)

                    pos = SynthPosition(
                        trade_id=trade["id"],
                        condition_id=cid,
                        question=trade.get("question", ""),
                        yes_token_id=yes_tok,
                        no_token_id=no_tok,
                        yes_price=entry_sum / 2,   # approximate; good enough for resolution
                        no_price=entry_sum / 2,
                        payout_target=P,
                        total_cost=entry_sum * P,
                        gap_pct=1.0 - entry_sum,
                        opened_at=float(trade.get("timestamp") or time.time()),
                    )
                    self._open[cid] = pos
                    self._entered_ids.add(cid)

            logger.info(f"SynthArb: restored {len(self._open)} open position(s)")
        except Exception as exc:
            logger.warning(f"SynthArb: restore failed (non-fatal): {exc}")

    async def _fetch_token_ids_for(
        self, session: aiohttp.ClientSession, condition_id: str
    ) -> list[str]:
        """Fetch YES/NO token IDs for a condition_id via Gamma API."""
        try:
            async with session.get(
                "https://gamma-api.polymarket.com/markets",
                params={"condition_ids": condition_id},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                markets = data if isinstance(data, list) else data.get("data", [])
                if not markets:
                    return []
                return extract_clob_token_ids(markets[0])
        except Exception:
            return []

    # ── Market scan ───────────────────────────────────────────────────────────

    async def _scan(self) -> None:
        """Two-stage scan: midpoint pre-filter → ask-side depth check."""
        slots_available = settings.SYNTH_MAX_OPEN - len(self._open)
        if slots_available <= 0:
            logger.debug(f"SynthArb: max open ({settings.SYNTH_MAX_OPEN}) reached — skipping scan")
            return

        markets = await self._fetch_active_markets()
        if not markets:
            logger.debug("SynthArb: no markets returned from Gamma API")
            return

        max_days = settings.SYNTH_MAX_DAYS_TO_RESOLVE
        now_ts = time.time()
        pairs: list[tuple[dict, str, str]] = []

        for m in markets:
            cid = m.get("conditionId") or m.get("id", "")
            if not cid or cid in self._entered_ids:
                continue

            token_ids = extract_clob_token_ids(m)
            if len(token_ids) < 2:
                continue

            liq = float(m.get("liquidity") or m.get("volume24hr") or 0)
            if liq < settings.SYNTH_MIN_LIQUIDITY:
                continue

            # Always reject markets with missing or unparseable end dates (Fix 2).
            # This guard runs regardless of max_days so that setting max_days=0
            # (no window cap) does not re-introduce the silent-allow bug.
            end_raw = m.get("endDate") or m.get("end_date") or m.get("closeTime")
            if not end_raw:
                continue  # no date at all — unknown resolution window = skip
            try:
                end_ts = datetime.fromisoformat(
                    str(end_raw).replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                continue  # unparseable date → skip

            # Window cap: only applied when max_days > 0 (0 = no limit).
            # With the merge path, resolution date only matters if merge fails,
            # so no-limit is safe as long as merge is enabled.
            if max_days > 0 and end_ts - now_ts > max_days * 86400:
                continue

            pairs.append((m, token_ids[0], token_ids[1]))

        if not pairs:
            logger.debug(
                f"SynthArb: {len(markets)} markets scanned — "
                "none passed date/liquidity filter"
            )
            return

        # ── Stage 1: cheap midpoint pre-filter ───────────────────────────────
        # Fetches midpoints for all candidate tokens in batch. Markets whose
        # midpoint sum >= MIDPOINT_PREFILTER cannot have a real ask-side gap
        # large enough to cover fees, so we skip them without fetching books.
        all_tokens = list({tok for _, y, n in pairs for tok in (y, n)})
        mids = await self._batch_midpoints(all_tokens)

        candidates: list[tuple[dict, str, str]] = []
        for market, yes_tok, no_tok in pairs:
            yes_mid = mids.get(yes_tok)
            no_mid = mids.get(no_tok)
            if yes_mid is None or no_mid is None:
                continue
            if yes_mid <= 0.01 or no_mid <= 0.01:
                continue
            # FIX 3: loose pre-filter only; actual decision made on asks below
            if yes_mid + no_mid >= self.MIDPOINT_PREFILTER:
                continue
            candidates.append((market, yes_tok, no_tok))

        if not candidates:
            logger.debug(
                f"SynthArb: {len(pairs)} liquid markets — "
                f"none have midpoint sum < {self.MIDPOINT_PREFILTER:.2f}"
            )
            return

        logger.debug(
            f"SynthArb: {len(candidates)} midpoint candidate(s) — "
            "fetching order books for ask-side check"
        )

        # ── Stage 2: FIX 3 + FIX 6 — fetch order books, compute executable cost ──
        P = settings.SYNTH_POSITION_SIZE
        opportunities: list[tuple[float, dict, str, str, float, float]] = []
        loop = asyncio.get_event_loop()

        for market, yes_tok, no_tok in candidates[:self.MAX_BOOK_CANDIDATES]:
            try:
                yes_book, no_book = await asyncio.gather(
                    loop.run_in_executor(None, self._client.get_order_book, yes_tok),
                    loop.run_in_executor(None, self._client.get_order_book, no_tok),
                )
            except Exception as exc:
                logger.debug(f"SynthArb: order book fetch failed: {exc}")
                continue

            # FIX 6: returns None when depth is insufficient for our target size
            cost_yes = self._get_executable_cost(yes_book, P)
            cost_no  = self._get_executable_cost(no_book,  P)

            if cost_yes is None or cost_no is None:
                logger.debug(
                    f"SynthArb: insufficient depth for "
                    f"{market.get('question', '')[:50]}"
                )
                continue

            gross_profit = P - cost_yes - cost_no
            if gross_profit <= 0:
                continue

            # Fee estimation on actual executable average prices
            avg_yes = cost_yes / P
            avg_no  = cost_no  / P
            fee_yes = self._fee_rate(avg_yes) * cost_yes
            fee_no  = self._fee_rate(avg_no)  * cost_no
            net_profit  = gross_profit - fee_yes - fee_no
            net_gap_pct = net_profit / P

            if net_gap_pct < settings.SYNTH_MIN_GAP:
                continue

            opportunities.append((net_gap_pct, market, yes_tok, no_tok, avg_yes, avg_no))
            await asyncio.sleep(0.05)  # gentle rate limit between book fetches

        if not opportunities:
            logger.debug(
                f"SynthArb: {len(candidates)} candidate(s) checked — "
                f"no executable gap >= {settings.SYNTH_MIN_GAP * 100:.1f}% after fees"
            )
            return

        opportunities.sort(key=lambda x: x[0], reverse=True)
        logger.info(
            f"SynthArb: {len(opportunities)} real ask-side gap(s) found — "
            f"entering up to {min(len(opportunities), slots_available)}"
        )

        for net_gap_pct, market, yes_tok, no_tok, avg_yes, avg_no in opportunities[:slots_available]:
            cid = market.get("conditionId") or market.get("id", "")
            if cid in self._entered_ids:
                continue
            await self._enter(market, yes_tok, no_tok, avg_yes, avg_no, net_gap_pct)

    def _get_executable_cost(
        self, order_book: dict, target_shares: float
    ) -> Optional[float]:
        """
        FIX 3 + FIX 6: Walk the ask side of the order book for target_shares.
        Returns total USDC cost to fill target_shares, or None if depth is
        insufficient to fill the full amount at any price.
        Asks are sorted ascending (lowest price = best fill first).
        """
        raw_asks = order_book.get("asks", [])
        if not raw_asks:
            return None

        try:
            asks = sorted(
                [{"price": float(a["price"]), "size": float(a["size"])} for a in raw_asks],
                key=lambda x: x["price"],
            )
        except (KeyError, TypeError, ValueError):
            return None

        remaining = target_shares
        total_cost = 0.0

        for level in asks:
            fill = min(remaining, level["size"])
            total_cost += fill * level["price"]
            remaining -= fill
            if remaining <= 1e-6:
                return total_cost

        return None  # FIX 6: book exhausted before target filled

    async def _fetch_active_markets(self) -> list[dict]:
        """Paginated fetch of active Polymarket markets from Gamma API."""
        markets: list[dict] = []
        url = "https://gamma-api.polymarket.com/markets"
        try:
            async with aiohttp.ClientSession() as session:
                offset = 0
                while len(markets) < self.MAX_MARKETS_PER_SCAN:
                    async with session.get(
                        url,
                        params={
                            "active": "true",
                            "closed": "false",
                            "limit": 100,
                            "offset": offset,
                        },
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                        batch = data if isinstance(data, list) else data.get("data", [])
                        if not batch:
                            break
                        markets.extend(batch)
                        if len(batch) < 100:
                            break
                        offset += 100
        except Exception as exc:
            logger.warning(f"SynthArb: Gamma API market fetch failed: {exc}")
        return markets

    async def _batch_midpoints(self, token_ids: list[str]) -> dict[str, float]:
        """Fetch midpoints for many tokens in BATCH_SIZE chunks (pre-filter only)."""
        result: dict[str, float] = {}
        loop = asyncio.get_event_loop()

        for i in range(0, len(token_ids), self.BATCH_SIZE):
            chunk = token_ids[i:i + self.BATCH_SIZE]
            try:
                raw = await loop.run_in_executor(
                    None, self._client.get_midpoints_batch, chunk
                )
                result.update(raw)
            except Exception as exc:
                logger.debug(f"SynthArb: batch midpoints chunk {i} failed: {exc}")
            if i + self.BATCH_SIZE < len(token_ids):
                await asyncio.sleep(0.1)

        return result

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _enter(
        self,
        market: dict,
        yes_token_id: str,
        no_token_id: str,
        yes_price: float,   # ask-side average fill price
        no_price: float,    # ask-side average fill price
        net_gap_pct: float,
    ) -> None:
        cid = market.get("conditionId") or market.get("id", "")
        question = market.get("question", cid[:40])
        P = settings.SYNTH_POSITION_SIZE
        yes_cost   = P * yes_price
        no_cost    = P * no_price
        total_cost = yes_cost + no_cost

        # FIX 5: synth-specific halt check (does NOT use shared risk manager's halt)
        if self._synth_risk.is_halted():
            return

        # Still delegate position-size limits to the shared risk manager
        ok_yes, reason_yes = self._risk.approve_order("BUY", yes_cost, f"{cid}_yes", self.name)
        ok_no,  reason_no  = self._risk.approve_order("BUY", no_cost,  f"{cid}_no",  self.name)
        if not ok_yes:
            logger.debug(f"SynthArb: YES leg rejected by risk: {reason_yes}")
            return
        if not ok_no:
            logger.debug(f"SynthArb: NO leg rejected by risk: {reason_no}")
            return

        logger.info(
            f"SynthArb ENTER: {question[:55]}… "
            f"YES@{yes_price:.3f}(ask) + NO@{no_price:.3f}(ask) = {yes_price + no_price:.3f} "
            f"net_gap={net_gap_pct * 100:.2f}% after fees | "
            f"cost=${total_cost:.2f} → payout=${P:.0f}"
        )

        trade_id = f"synth_{cid[:20]}_{uuid.uuid4().hex[:8]}"
        # FIX 4: FOK orders
        yes_r, no_r = await self._execute_both_legs_fok(
            yes_token_id, no_token_id, yes_price, no_price, P, question
        )

        if yes_r and no_r:
            self._risk.record_order_placed(cid, total_cost)
            db.insert_trade(
                trade_id=trade_id,
                strategy=self.name,
                market_id=cid,
                question=question,
                side="BUY",
                size=P,
                price=yes_price + no_price,
                fill_price=0.0,
                pnl=0.0,
                status="open",
                exit_reason="",
                dry_run=settings.DRY_RUN,
                asset="SYNTH",
            )
            pos = SynthPosition(
                trade_id=trade_id,
                condition_id=cid,
                question=question,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                yes_price=yes_price,
                no_price=no_price,
                payout_target=P,
                total_cost=total_cost,
                gap_pct=net_gap_pct,
                opened_at=time.time(),
            )
            self._open[cid] = pos
            self._entered_ids.add(cid)

            logger.info(
                f"SynthArb: both FOK legs {'dry-run accepted' if settings.DRY_RUN else 'filled'} — "
                f"expected profit ${P * net_gap_pct:.2f} USDC — "
                f"{'attempting merge' if settings.SYNTH_MERGE_ENABLED else 'merge disabled, will hold to resolution'}"
            )

            # FIX 1: attempt immediate on-chain merge after short settlement delay
            if settings.SYNTH_MERGE_ENABLED:
                await asyncio.sleep(self.MERGE_WAIT_SECS)
                await self._try_merge(cid, pos)

        elif yes_r and not no_r:
            logger.warning(
                f"SynthArb: NO FOK leg did not fill for {cid[:20]} — unwinding YES leg"
            )
            await self._unwind_leg(yes_token_id, yes_price, P)
            self._entered_ids.add(cid)

        elif no_r and not yes_r:
            logger.warning(
                f"SynthArb: YES FOK leg did not fill for {cid[:20]} — unwinding NO leg"
            )
            await self._unwind_leg(no_token_id, no_price, P)
            self._entered_ids.add(cid)

        else:
            logger.info(
                f"SynthArb: both FOK legs rejected for {cid[:20]} — no position opened "
                "(market moved; gap closed before fill)"
            )
            self._entered_ids.add(cid)

    async def _execute_both_legs_fok(
        self,
        yes_token_id: str,
        no_token_id: str,
        yes_price: float,
        no_price: float,
        payout: float,
        question: str,
    ) -> tuple[Optional[dict], Optional[dict]]:
        """
        FIX 4: Submit YES and NO as a FOK (Fill or Kill) batch.
        FOK orders fill immediately at the given price or are cancelled — no
        passive resting. Returns (yes_result, no_result); None means no fill.
        """
        from py_clob_client.clob_types import OrderArgs, OrderType

        yes_price_r = round_price(yes_price)
        no_price_r  = round_price(no_price)

        if settings.DRY_RUN:
            logger.info(
                f"DRY RUN — SynthArb: FOK BUY {payout:.0f} YES@{yes_price_r:.3f} + "
                f"NO@{no_price_r:.3f} on {question[:50]}"
            )
            return (
                {"id": f"dry_yes_{int(time.time())}", "status": "dry_run"},
                {"id": f"dry_no_{int(time.time())}",  "status": "dry_run"},
            )

        loop = asyncio.get_event_loop()
        yes_args = OrderArgs(price=yes_price_r, size=payout, side="BUY", token_id=yes_token_id)
        no_args  = OrderArgs(price=no_price_r,  size=payout, side="BUY", token_id=no_token_id)
        try:
            results = await loop.run_in_executor(
                None,
                self._client.post_orders_batch,
                [(yes_args, OrderType.FOK), (no_args, OrderType.FOK)],  # FIX 4
            )
            yes_r = results[0] if results and len(results) > 0 else None
            no_r  = results[1] if results and len(results) > 1 else None

            if yes_r and yes_r.get("errorCode"):
                logger.warning(f"SynthArb: YES FOK error: {yes_r.get('errorCode')}")
                yes_r = None
            if no_r and no_r.get("errorCode"):
                logger.warning(f"SynthArb: NO FOK error: {no_r.get('errorCode')}")
                no_r = None

            return yes_r, no_r
        except Exception as exc:
            logger.error(f"SynthArb: FOK batch order failed: {exc}")
            return None, None

    async def _unwind_leg(self, token_id: str, entry_price: float, size: float) -> None:
        """Sell a filled leg at a slight discount to return to flat quickly."""
        if settings.DRY_RUN:
            logger.info(f"DRY RUN — SynthArb: would unwind leg {token_id[:20]}")
            return
        from py_clob_client.clob_types import OrderArgs, OrderType
        loop = asyncio.get_event_loop()
        try:
            sell_price = round_price(max(0.01, entry_price - 0.02))
            sell_args  = OrderArgs(price=sell_price, size=size, side="SELL", token_id=token_id)
            await loop.run_in_executor(
                None, self._client.post_order, sell_args, OrderType.GTC
            )
            logger.info(f"SynthArb: unwind order placed for {token_id[:20]} @ {sell_price:.3f}")
        except Exception as exc:
            logger.error(f"SynthArb: unwind order failed for {token_id[:20]}: {exc}")

    # ── FIX 1: On-chain merge ─────────────────────────────────────────────────

    async def _try_merge(self, cid: str, pos: SynthPosition) -> bool:
        """
        FIX 1: Call CTF.mergePositions() to convert P YES + P NO tokens back
        into P USDC on-chain immediately after fills. If successful, the position
        is closed with realized profit and capital is freed in seconds.

        Dry-run: simulates a successful merge and closes the position in the DB.
        Live: submits a real Polygon transaction. Falls back to hold-to-resolution
        if the transaction reverts (e.g., tokens not yet settled).
        """
        if settings.DRY_RUN:
            pnl = pos.payout_target * pos.gap_pct
            logger.info(
                f"DRY RUN — SynthArb MERGE simulated: {pos.question[:50]}… "
                f"PnL={pnl:+.4f} USDC — capital recycled immediately"
            )
            self._close_position(cid, pos, 1.0, pnl, "merged")
            return True

        if not settings.SYNTH_MERGE_ENABLED:
            return False

        loop = asyncio.get_event_loop()
        try:
            merged = await loop.run_in_executor(None, self._do_merge_sync, cid, pos)
        except Exception as exc:
            logger.warning(f"SynthArb: merge attempt raised for {cid[:20]}: {exc}")
            merged = False

        if merged:
            pnl = pos.payout_target * pos.gap_pct
            logger.info(
                f"SynthArb MERGED: {pos.question[:50]}… "
                f"PnL={pnl:+.4f} USDC — capital freed"
            )
            self._close_position(cid, pos, 1.0, pnl, "merged")
            return True

        pos.merge_attempts += 1
        if pos.merge_attempts >= self.MAX_MERGE_ATTEMPTS:
            logger.warning(
                f"SynthArb: merge gave up after {self.MAX_MERGE_ATTEMPTS} attempts "
                f"for {cid[:20]} — will hold to resolution"
            )
        return False

    def _do_merge_sync(self, cid: str, pos: SynthPosition) -> bool:
        """
        Synchronous web3 call to CTF.mergePositions(). Called via run_in_executor.

        Polymarket binary markets use Gnosis CTF. For a binary condition:
          partition = [1, 2]  (YES = index set {0} = 2^0 = 1; NO = index set {1} = 2^1 = 2)
          amount    = payout_target * 10^6  (USDC 6-decimal precision)

        The CTF contract is the token issuer and burns your ERC-1155 outcome
        tokens directly — no separate approval needed for mergePositions.
        """
        from web3 import Web3
        from web3.middleware import ExtraDataToPOAMiddleware

        w3 = Web3(Web3.HTTPProvider(settings.POLYGON_RPC_URL))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        if not w3.is_connected():
            logger.warning("SynthArb: cannot connect to Polygon RPC for merge")
            return False

        account = w3.eth.account.from_key(settings.POLY_PRIVATE_KEY)
        wallet  = account.address

        ctf = w3.eth.contract(
            address=Web3.to_checksum_address(settings.CTF_ADDRESS),
            abi=self._CTF_MERGE_ABI,
        )

        # condition_id as bytes32 (strip 0x prefix, left-pad to 64 hex chars)
        cid_bytes = bytes.fromhex(cid.lstrip("0x").zfill(64))

        # partition [1, 2] = YES (index set {0}) and NO (index set {1})
        partition = [1, 2]

        # amount in smallest token unit (6 decimals, same as USDC)
        amount = int(pos.payout_target * 10 ** 6)

        nonce     = w3.eth.get_transaction_count(wallet)
        gas_price = w3.eth.gas_price

        tx = ctf.functions.mergePositions(
            Web3.to_checksum_address(settings.USDC_ADDRESS),  # collateral token
            b"\x00" * 32,                                      # parentCollectionId = null (root)
            cid_bytes,                                         # conditionId
            partition,                                         # [1, 2] for binary market
            amount,                                            # shares in 6-decimal units
        ).build_transaction({
            "from":     wallet,
            "nonce":    nonce,
            "gas":      200_000,
            "gasPrice": gas_price,
            "chainId":  settings.CHAIN_ID,
        })

        signed   = account.sign_transaction(tx)
        tx_hash  = w3.eth.send_raw_transaction(signed.rawTransaction)
        receipt  = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

        if receipt.status == 1:
            logger.info(
                f"SynthArb: merge tx confirmed — "
                f"https://polygonscan.com/tx/{tx_hash.hex()}"
            )
            return True

        logger.warning(
            f"SynthArb: merge tx reverted — "
            f"https://polygonscan.com/tx/{tx_hash.hex()}"
        )
        return False

    # ── FIX 7: Resolution — Gamma API only, no price thresholds ──────────────

    async def _check_resolutions_and_retry_merges(self) -> None:
        """
        For every open position:
          1. If merge is enabled and we haven't exceeded MAX_MERGE_ATTEMPTS,
             retry the on-chain merge (tokens may have settled by now).
          2. If merge not applicable or already given up, check Gamma API for
             official market close (closed=true). Close only on that signal.

        FIX 7: The previous version closed positions whenever a token midpoint
        crossed 0.99, which is not the same as oracle resolution. That logic is
        removed entirely. The bot now only closes when Gamma API says closed=true.
        """
        if not self._open:
            return

        for cid in list(self._open.keys()):
            pos = self._open.get(cid)
            if pos is None:
                continue

            # Retry merge if we haven't succeeded and haven't hit the limit
            if (
                settings.SYNTH_MERGE_ENABLED
                and pos.merge_attempts > 0             # only retry if we've tried before
                and pos.merge_attempts < self.MAX_MERGE_ATTEMPTS
            ):
                merged = await self._try_merge(cid, pos)
                if merged:
                    continue  # position closed; removed from self._open

            # FIX 7: official close check — Gamma API only
            await self._resolve_via_gamma_official(cid, pos)

    async def _resolve_via_gamma_official(self, cid: str, pos: SynthPosition) -> None:
        """
        FIX 7: Poll Gamma API for official market close.
        Closes the position ONLY when closed=true is returned.
        Does NOT use price thresholds or midpoint heuristics.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"clob_token_ids": pos.yes_token_id},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    markets = data if isinstance(data, list) else data.get("data", [])
                    if not markets:
                        return
                    market = markets[0]

            # FIX 7: closed=true means oracle has resolved — only close on this
            if not market.get("closed", False):
                return  # still live — wait

            token_ids = extract_clob_token_ids(market)
            pnl = pos.payout_target * pos.gap_pct  # fee-adjusted net profit locked at entry

            outcome_prices_raw = market.get("outcomePrices")
            if outcome_prices_raw:
                prices = [
                    float(p) for p in (
                        json.loads(outcome_prices_raw)
                        if isinstance(outcome_prices_raw, str)
                        else outcome_prices_raw
                    )
                ]
                yes_idx = (
                    token_ids.index(pos.yes_token_id)
                    if pos.yes_token_id in token_ids
                    else None
                )
                if yes_idx is not None and yes_idx < len(prices):
                    won_side = "resolved_yes_won" if prices[yes_idx] >= 0.99 else "resolved_no_won"
                    self._close_position(cid, pos, 1.0, pnl, won_side)
                    return

            # Closed but outcome indeterminate — credit the locked gap profit
            logger.warning(
                f"SynthArb: {cid[:20]} officially closed, no outcome prices — "
                f"crediting locked gap profit {pnl:+.4f} USDC"
            )
            self._close_position(cid, pos, 1.0, pnl, "resolved")

        except Exception as exc:
            logger.debug(
                f"SynthArb: Gamma official resolve check failed for {cid[:20]}: {exc}"
            )

    def _close_position(
        self,
        cid: str,
        pos: SynthPosition,
        fill_price: float,
        pnl: float,
        reason: str,
    ) -> None:
        logger.info(
            f"SynthArb CLOSED [{reason}]: {pos.question[:50]}… "
            f"cost=${pos.total_cost:.2f} payout=${pos.payout_target:.0f} "
            f"PnL={pnl:+.4f} USDC"
        )
        db.update_trade(
            pos.trade_id,
            fill_price=fill_price,
            pnl=pnl,
            status="closed",
            exit_reason=reason,
        )
        # FIX 5: record PnL in synth-specific guard AND shared risk manager
        self._synth_risk.record_pnl(pnl)
        self._risk.record_fill(pnl)
        self._open.pop(cid, None)

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _fee_rate(mid: float) -> float:
        """Polymarket taker fee rate at a given price (quadratic model)."""
        return SynthArb.MAX_FEE_RATE * 4.0 * mid * (1.0 - mid)

    def open_positions_summary(self) -> list[dict]:
        """Called by the dashboard API to display open synth positions."""
        return [
            {
                "condition_id":   pos.condition_id,
                "question":       pos.question,
                "yes_price":      round(pos.yes_price, 4),
                "no_price":       round(pos.no_price, 4),
                "total_cost":     round(pos.total_cost, 2),
                "payout_target":  pos.payout_target,
                "expected_profit": round(pos.payout_target * pos.gap_pct, 4),
                "gap_pct":        round(pos.gap_pct * 100, 2),
                "opened_at":      pos.opened_at,
                "age_minutes":    round((time.time() - pos.opened_at) / 60, 1),
                "merge_attempts": pos.merge_attempts,
                "synth_daily_pnl": round(self._synth_risk.daily_pnl, 4),
            }
            for pos in self._open.values()
        ]
