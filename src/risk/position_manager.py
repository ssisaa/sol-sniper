"""
L5 Risk & Position Manager + Exit Loop (SOL-SNIPER v1.2)

- Tracks open positions (paper or live)
- Applies exit rules: time stop, trailing stop, take-profit ladder,
  hard stop, volume-death placeholder, creator-sell placeholder
- Circuit breaker on consecutive losses / daily drawdown
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

def _pulse_row(mint: str):
    try:
        from src.observability.dashboard import pulse_for_mint
        return pulse_for_mint(mint) or {}
    except Exception:
        return {}


def _shown_pct(pos) -> Optional[float]:
    row = _pulse_row(getattr(pos, "mint", ""))
    if row.get("percentage_sol") is not None:
        try:
            return round(float(row["percentage_sol"]), 2)
        except (TypeError, ValueError):
            pass
    return _pnl_pct(None, pos)


def _stream_saw_exit(pos) -> bool:
    """The trade socket saw this copy's caller sell the whole coin."""
    try:
        from src.observability.dashboard import _stream_if_on
        stream = _stream_if_on()
    except Exception:
        return False
    if stream is None:
        return False
    wallet = str(getattr(pos, "callout_wallet", "") or "")
    mint = str(getattr(pos, "mint", "") or "")
    after = float(getattr(pos, "entry_time", 0) or 0) - 600
    if len(wallet) >= 32:
        return stream.exited(wallet, mint, after)
    return bool(stream.exited_any(mint, after))


def _dropoff_for(pos) -> bool:
    """Sell in full now that the caller is out: Dropoff on, or Follow caller exit on and the socket saw the sale."""
    try:
        from src.config.runtime import get_config
        cfg = get_config()
    except Exception:
        return False
    if bool(getattr(cfg, "callout_dropoff", False)):
        return True
    return bool(getattr(cfg, "copy_follow_exit", True)) and _stream_saw_exit(pos)


def _profit_sell_reason(reason: str) -> bool:
    """Fast Pace, take-profit, and trail. Stops, caller exits, and the Sell button are not these."""
    r = str(reason or "")
    return (
        r == "fast_pace_sell"
        or r == "trailing_stop"
        or r == "peak_giveback"
        or r.startswith("take_profit_")
        or r.startswith("tp_touch_")
    )


def _drop_pending_tp(pos, action: str, pending_tp) -> None:
    if pending_tp is not None:
        try:
            pos.tp_ladder_hit.remove(pending_tp)
        except ValueError:
            pass
    if str(action or "").startswith("take_profit_"):
        try:
            lvl = int(str(action).split("_")[2].replace("pct", "")) / 100.0
            if lvl in pos.tp_ladder_hit:
                pos.tp_ladder_hit.remove(lvl)
        except (ValueError, IndexError):
            pass


def _shown_sol_pct(pos) -> Optional[float]:
    shown = _shown_pct(pos)
    if shown is not None and _pulse_row(getattr(pos, "mint", "")).get("percentage_sol") is not None:
        return shown
    if getattr(pos, "_book_sol_pct", None) is None:
        return None
    try:
        return round(float(pos._book_sol_pct), 2)
    except (TypeError, ValueError):
        return None


def _shown_usd(pos):
    row = _pulse_row(getattr(pos, "mint", ""))
    if row.get("unrealized_usd") is not None:
        try:
            return round(float(row["unrealized_usd"]), 6)
        except (TypeError, ValueError):
            pass
    if getattr(pos, "_book_usd", None) is None:
        return None
    try:
        return round(float(pos._book_usd), 6)
    except (TypeError, ValueError):
        return None


def _shown_pnl_sol(pos):
    row = _pulse_row(getattr(pos, "mint", ""))
    if row.get("unrealized_sol") is not None:
        try:
            return round(float(row["unrealized_sol"]), 6)
        except (TypeError, ValueError):
            pass
    if getattr(pos, "_book_pnl_sol", None) is not None:
        try:
            return round(float(pos._book_pnl_sol), 6)
        except (TypeError, ValueError):
            pass
    if getattr(pos, "since_buy_pct", None) is None:
        return None
    return round(
        float(pos.entry_sol or 0) * (float(pos.since_buy_pct) / 100.0) * float(pos.remaining_frac or 1),
        6,
    )


def _pnl_pct(manager, pos) -> Optional[float]:
    """Gain from the buy until now. Never the 5-minute candle, never a raw token price."""
    pct = getattr(pos, "since_buy_pct", None)
    if pct is None:
        return None
    try:
        return round(float(pct), 2)
    except (TypeError, ValueError):
        return None


def _pump_m5_now(mint: str):
    """Pump 5-minute percent.

    ('ok', number) when Pump sent a percent.
    ('null', None) when Pump answered and the 5m percent is null.
    ('down', None) when the request failed. That is not a null tape.
    """
    url = (
        "https://swap-api.pump.fun/v1/coins/"
        f"{mint}/market-activity?program=pump&chainId=solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
    )
    try:
        from src.data.fast_get import pump_get

        # cache_s=0 so a second null check is a real second read, on the warm socket.
        body = pump_get(url, timeout=0.9, cache_s=0)
        if not isinstance(body, dict) or not body:
            return "down", None
        if "5m" not in body:
            return "down", None
        block = body.get("5m")
        # {"5m": null} is a real null tape. A missing reply is not.
        if block is None:
            return "null", None
        if not isinstance(block, dict):
            return "down", None
        if "priceChangePercent" not in block or block.get("priceChangePercent") is None:
            return "null", None
        try:
            # 0 and negatives are numbers. They are not null.
            return "ok", float(block.get("priceChangePercent"))
        except (TypeError, ValueError):
            return "down", None
    except Exception:
        return "down", None


log = logging.getLogger("sol-sniper.risk")


def _flag_if_worthless(mint: str, raw_tokens: int) -> None:
    """Background: ask Jupiter what the whole bag sells for. Under 0.00001 SOL, save DUST so a
    restart does not restore the coin again. A computed -100% alone is never trusted."""
    import threading
    import urllib.request

    if not mint or raw_tokens <= 0:
        return

    def _run() -> None:
        try:
            url = (
                "https://lite-api.jup.ag/swap/v1/quote?inputMint=" + mint
                + "&outputMint=So11111111111111111111111111111111111111112"
                + "&amount=" + str(int(raw_tokens)) + "&slippageBps=1000"
            )
            with urllib.request.urlopen(url, timeout=8) as resp:
                body = json.loads(resp.read().decode() or "{}")
            out = int(body.get("outAmount") or 0)
            if body.get("errorCode") or out < 10_000:
                from src.storage.mint_flags import mint_flag, note_dust_source, set_mint_flag
                if mint_flag(mint) not in ("DUST", "NODUST"):
                    note_dust_source(mint, "worthless")
                    set_mint_flag(mint, "DUST")
                    log.warning("FLAG DUST mint=%s — whole bag quotes %.7f SOL. Saved.", mint[:8], out / 1e9)
            else:
                log.warning("DUST check mint=%s — bag still quotes %.6f SOL, not flagged", mint[:8], out / 1e9)
        except Exception:
            log.debug("worthless check failed %s", mint[:8], exc_info=True)

    threading.Thread(target=_run, name="dust-check", daemon=True).start()


class PositionMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


@dataclass
class OpenPosition:
    id: str
    mint: str
    mode: PositionMode
    entry_price: float          # quote in SOL per token (normalized) or SOL spent basis
    entry_sol: float            # SOL notional deployed
    tokens: float               # token amount (paper: synthetic)
    entry_time: float           # unix
    entry_slot: Optional[int] = None
    peak_price: float = 0.0
    status: str = "OPEN"        # OPEN | CLOSING | CLOSED
    tp_ladder_hit: List[float] = field(default_factory=list)
    remaining_frac: float = 1.0  # fraction of original size still held
    source: str = "unknown"
    signature: Optional[str] = None
    exit_reason: Optional[str] = None
    exit_price: Optional[float] = None
    exit_time: Optional[float] = None
    realized_pnl_sol: float = 0.0
    fees_sol: float = 0.0
    stop_price: Optional[float] = None  # absolute price SL (optional)
    stop_pct: Optional[float] = None  # e.g. -0.04 = -4% from entry
    min_hold_seconds: float = 0.0  # no discretionary SL/TP before this
    strategy: str = ""  # launch | dayscalp | ...
    symbol: str = ""
    name: str = ""
    since_buy_pct: Optional[float] = None
    peak_since_pct: Optional[float] = None
    low_since_pct: Optional[float] = None
    live_m5_pct: Optional[float] = None
    entry_m5_pct: Optional[float] = None
    entry_m1_pct: Optional[float] = None
    entry_s1_pct: Optional[float] = None
    entry_h1_pct: Optional[float] = None
    token_ath_pct: Optional[float] = None

    def mark_price(self, price: float) -> None:
        if price > self.peak_price:
            self.peak_price = price


@dataclass
class ClosedTrade:
    position_id: str
    mint: str
    mode: str
    entry_sol: float
    exit_sol: float
    pnl_sol: float
    fees_sol: float
    hold_seconds: float
    exit_reason: str
    source: str
    closed_at: str
    tokens: float = 0.0
    exit_signature: str = ""


@dataclass
class RiskConfig:
    max_position_sol: float = 0.15
    max_position_pct: float = 0.007
    total_capital_sol: float = 10.0
    max_concurrent: int = 5
    hard_stop_pct: float = -0.35
    runner_stop_pct: float = -0.12
    time_stop_seconds: float = 300.0
    time_stop_min_gain: float = 0.15
    trail_activate_pct: float = 0.80
    trail_drawdown_pct: float = 0.32
    tp_levels: tuple = (0.25,)
    tp_fractions: tuple = (0.50,)
    fallback_arm_pct: float = 0.0
    exit_style: str = "plan"
    consecutive_loss_pause: int = 4
    daily_loss_limit_pct: float = 0.10
    paper_slippage_pct: float = 0.03
    paper_fee_pct: float = 0.01


class PositionManager:
    def __init__(self, config: Optional[RiskConfig] = None):
        self.cfg = config or RiskConfig()
        self.positions: Dict[str, OpenPosition] = {}
        self.closed: List[ClosedTrade] = []
        self.consecutive_losses = 0
        self.daily_pnl_sol = 0.0
        self.paused_until: float = 0.0
        self._lock = asyncio.Lock()
        self._exit_pass = asyncio.Lock()
        self._on_close: List[Callable] = []
        self._sell_handler = None
        self._sell_cooldown: Dict[str, float] = {}  # mint -> unix ts until retry
        # Simple mid-price cache for paper marks (mint -> price in SOL terms)
        self._prices: Dict[str, float] = {}

    def add_close_callback(self, cb: Callable) -> None:
        self._on_close.append(cb)

    def set_sell_handler(self, handler) -> None:
        """async handler(pos, frac, reason) -> (ok, exit_price, signature)"""
        self._sell_handler = handler

    def is_paused(self) -> bool:
        return time.time() < self.paused_until

    def pause(self, seconds: float, reason: str = "") -> None:
        self.paused_until = time.time() + seconds
        log.warning("Circuit breaker PAUSE %.0fs (%s)", seconds, reason)

    def can_open(self) -> bool:
        if self.is_paused():
            return False
        open_count = sum(1 for p in self.positions.values() if p.status == "OPEN")
        if open_count >= self.cfg.max_concurrent:
            return False
        if self.daily_pnl_sol <= -self.cfg.total_capital_sol * self.cfg.daily_loss_limit_pct:
            self.pause(3600, "daily loss limit")
            return False
        return True

    def size_for_entry(self, size_multiplier: float = 1.0) -> float:
        base = min(
            self.cfg.max_position_sol,
            self.cfg.total_capital_sol * self.cfg.max_position_pct,
        )
        return max(0.001, base * max(0.25, min(1.25, size_multiplier)))

    def set_price(self, mint: str, price: float) -> None:
        """Paper price oracle: SOL per whole position unit (we use entry-relative)."""
        self._prices[mint] = price

    def get_price(self, pos: OpenPosition) -> float:
        # Default: random-walk style mark around entry for paper if no external feed
        return self._prices.get(pos.mint, pos.entry_price)

    async def open_position(
        self,
        mint: str,
        mode: PositionMode,
        entry_sol: float,
        entry_price: float = 1.0,
        tokens: Optional[float] = None,
        slot: Optional[int] = None,
        source: str = "unknown",
        signature: Optional[str] = None,
        fees_sol: float = 0.0,
        stop_price: Optional[float] = None,
        stop_pct: Optional[float] = None,
        min_hold_seconds: float = 0.0,
        strategy: str = "",
        force: bool = False,
    ) -> Optional[OpenPosition]:
        async with self._lock:
            for p in self.positions.values():
                if p.mint == mint and p.status == "OPEN":
                    log.info("Already open on %s", mint[:8])
                    return p
            if not force and not self.can_open():
                log.info("Cannot open position (paused or max concurrent)")
                return None

            tok = tokens if tokens is not None else (entry_sol / entry_price if entry_price else entry_sol)
            pos = OpenPosition(
                id=str(uuid.uuid4())[:12],
                mint=mint,
                mode=mode,
                entry_price=entry_price,
                entry_sol=entry_sol,
                tokens=tok,
                entry_time=time.time(),
                entry_slot=slot,
                peak_price=entry_price,
                source=source,
                signature=signature,
                fees_sol=fees_sol,
                stop_price=stop_price,
                stop_pct=stop_pct,
                min_hold_seconds=min_hold_seconds,
                strategy=strategy or source,
            )
            pos._orig_raw = float(tok or 0)
            self.positions[pos.id] = pos
            self._prices.setdefault(mint, entry_price)
            log.info(
                "OPEN %s %s sol=%.4f price=%.6f source=%s",
                mode.value,
                mint[:8],
                entry_sol,
                entry_price,
                source,
            )
            self._push_remote()
            return pos

    async def close_position(
        self,
        pos_id: str,
        reason: str,
        exit_price: Optional[float] = None,
        frac: float = 1.0,
    ) -> Optional[ClosedTrade]:
        async with self._lock:
            pos = self.positions.get(pos_id)
            if not pos or pos.status == "CLOSED":
                return None

            price = exit_price if exit_price is not None else self.get_price(pos)
            frac = max(0.0, min(1.0, frac))
            # Fee-aware PnL: mark exit value vs full cost basis (entry + fees already on pos)
            notional = pos.entry_sol * frac * pos.remaining_frac
            ret = (price / pos.entry_price - 1.0) if pos.entry_price else 0.0
            gross = ret * notional
            live = pos.mode == PositionMode.LIVE
            if live:
                exit_fee = 0.0
            else:
                exit_fee = notional * self.cfg.paper_fee_pct
                pos.fees_sol += exit_fee
            pnl = gross - exit_fee
            pos.realized_pnl_sol += pnl
            pos.remaining_frac *= (1.0 - frac)

            if pos.remaining_frac <= 0.05 or frac >= 0.99:
                pos.status = "CLOSED"
                pos.exit_reason = reason
                pos.exit_price = price
                pos.exit_time = time.time()
                hold = pos.exit_time - pos.entry_time
                sol_out = getattr(pos, "_sol_out", None)
                if sol_out is None or float(sol_out) < 0:
                    sol_out = 0.0
                else:
                    sol_out = float(sol_out)
                exit_paid = float(getattr(pos, "_exit_fee", 0) or 0)
                book = reason not in ("dust_left", "dust_unsellable", "DUST", "not_in_wallet")
                trade = ClosedTrade(
                    position_id=pos.id,
                    mint=pos.mint,
                    mode=pos.mode.value,
                    entry_sol=pos.entry_sol,
                    exit_sol=sol_out if book else 0.0,
                    pnl_sol=(sol_out - pos.entry_sol - float(pos.fees_sol or 0) - exit_paid) if book else 0.0,
                    fees_sol=float(pos.fees_sol or 0) + exit_paid,
                    hold_seconds=hold,
                    exit_reason=reason or "unknown",
                    source=pos.source,
                    closed_at=datetime.now(timezone.utc).isoformat(),
                    tokens=pos.tokens,
                    exit_signature=getattr(pos, "_last_sell_sig", "") or "",
                )
                if book:
                    self.closed.append(trade)
                    self.daily_pnl_sol += trade.pnl_sol
                    if trade.pnl_sol < 0:
                        self.consecutive_losses += 1
                        if self.consecutive_losses >= self.cfg.consecutive_loss_pause:
                            self.pause(45 * 60, f"{self.consecutive_losses} consecutive losses")
                    else:
                        self.consecutive_losses = 0
                else:
                    log.warning(
                        "DUST drop mint=%s — not booked, PnL unchanged",
                        pos.mint[:8],
                    )
                    bag = getattr(self, "_no_reopen", None)
                    if bag is None:
                        bag = set()
                        self._no_reopen = bag
                    bag.add(pos.mint)
                log.info(
                    "CLOSE %s %s pnl=%.4f SOL reason=%s hold=%.0fs",
                    pos.mode.value,
                    pos.mint[:8],
                    trade.pnl_sol,
                    reason,
                    hold,
                )
                for cb in self._on_close:
                    try:
                        res = cb(trade)
                        if asyncio.iscoroutine(res):
                            asyncio.create_task(res)
                    except Exception:
                        log.exception("on_close callback error")
                try:
                    asyncio.get_running_loop().run_in_executor(None, self._push_remote)
                except Exception:
                    self._push_remote()
                return trade
            else:
                log.info(
                    "PARTIAL CLOSE %s %s frac=%.2f pnl=%.4f remaining=%.2f",
                    pos.mint[:8],
                    reason,
                    frac,
                    pnl,
                    pos.remaining_frac,
                )
                self._push_remote()
                return None


    async def _fresh_since_pct(self, pos) -> Optional[float]:
        """% since buy from a new coin price. None means the read failed — the sell still goes."""
        try:
            base = float(getattr(pos, "_pump_entry_px", 0) or getattr(pos, "entry_price_usd", 0) or 0)
        except (TypeError, ValueError):
            base = 0.0
        if base <= 0:
            return None

        def _pull():
            from src.data.fast_get import pump_get
            return pump_get(
                f"https://frontend-api-v3.pump.fun/coins-v2/{pos.mint}",
                timeout=0.7,
                cache_s=0,
            )

        try:
            coin = await asyncio.wait_for(asyncio.to_thread(_pull), timeout=0.85)
        except Exception:
            return None
        if not isinstance(coin, dict):
            return None
        try:
            px = float(coin.get("price_usd") or 0)
        except (TypeError, ValueError):
            return None
        if px <= 0:
            return None
        return round((px / base - 1.0) * 100.0, 2)

    async def _maybe_sell(self, pos: OpenPosition, frac: float, reason: str, mark_price: float):
        """
        Execute on-chain/paper sell before closing books.
        Returns (ok, exit_price). For LIVE, ok=False keeps position open.
        """
        from src.execution.fast_lane import get_lane
        lane = get_lane()
        if lane.started and lane.loop is not None and not lane.here():
            fut = asyncio.run_coroutine_threadsafe(
                self._maybe_sell(pos, frac, reason, mark_price),
                lane.loop,
            )
            return await asyncio.wrap_future(fut)
        if not self._sell_handler:
            # LIVE must never book-close without an on-chain sell
            if pos.mode == PositionMode.LIVE:
                log.error("SELL blocked — no sell_handler wired (mint=%s)", pos.mint[:8])
                return False, mark_price
            return True, mark_price  # paper-only fallback
        if getattr(pos, "_sell_stopped", False) and not str(reason).endswith("stop") and "stop" not in str(reason):
            log.warning("ACTION SL blocked mint=%s — sell was stopped after fee burns, reason=%s", pos.mint[:8], reason)
            return False, mark_price
        if "stop" in str(reason):
            pos._sell_stopped = False
        # Backoff after failed sells (avoid Jupiter 429 spam)
        until = self._sell_cooldown.get(pos.mint, 0.0)
        if time.time() < until:
            if "stop" in str(reason) and getattr(pos, "_sl_wait_log", 0) + 30 < time.time():
                pos._sl_wait_log = time.time()
                log.warning(
                    "ACTION SL waiting mint=%s reason=%s — retry in %.0fs",
                    pos.mint[:8], reason, until - time.time(),
                )
            return False, mark_price
        if _profit_sell_reason(reason):
            # Sell lane is already running. Read the price once more before the swap.
            # A negative print aborts this profit sell. No price means the sell still goes.
            if time.time() < float(getattr(pos, "_neg_skip_until", 0) or 0):
                pos._last_sell_error = "price_negative"
                return False, mark_price
            pct = await self._fresh_since_pct(pos)
            if pct is not None and pct < 0:
                pos.since_buy_pct = pct
                prev_low = getattr(pos, "low_since_pct", None)
                try:
                    pos.low_since_pct = round(pct if prev_low is None else min(float(prev_low), pct), 2)
                except (TypeError, ValueError):
                    pos.low_since_pct = pct
                pos._neg_skip_until = time.time() + 1.5
                pos._last_sell_error = "price_negative"
                log.warning(
                    "SELL skipped mint=%s reason=%s — price went down negative (%+.2f%%). So didn't sell.",
                    pos.mint[:8], reason, pct,
                )
                try:
                    from src.observability.action_log import record_action
                    record_action(
                        "SELL",
                        level="WARNING",
                        detail=(
                            f"{getattr(pos, 'symbol', '') or pos.mint[:8]} price went down negative "
                            f"({pct:+.2f}%). So didn't sell."
                        ),
                        mint=pos.mint,
                        symbol=getattr(pos, "symbol", "") or "",
                    )
                except Exception:
                    pass
                return False, mark_price
        try:
            res = self._sell_handler(pos, frac, reason)
            if asyncio.iscoroutine(res):
                res = await res
            ok, exit_price, sig = res
            if not ok:
                err = str(getattr(pos, "_last_sell_error", "") or "")
                landed_unknown = "not_confirmed" in err or "NOT CONFIRMED" in err
                broadcast = ("broadcast" in err) and not landed_unknown
                no_route = (
                    "NO_ROUTES" in err.upper()
                    or "NO ROUTES" in err.upper()
                    or "NOT_TRADABLE" in err.upper()
                )
                # Lightning "pool not found" is not dust. Jupiter has to say no route.
                if no_route and not getattr(pos, "_nodust", False):
                    pos._dust_value = True  # type: ignore[attr-defined]
                    pos._quote_giveup = True  # type: ignore[attr-defined]
                    pos._last_sell_error = "sell_dust_quote out=0 no_route"  # type: ignore[attr-defined]
                    self._sell_cooldown[pos.mint] = time.time() + 3600.0
                    try:
                        from src.storage.mint_flags import flag_dust_if_needed
                        flag_dust_if_needed(pos.mint, "Jupiter has no route")
                    except Exception:
                        pass
                    log.warning(
                        "ACTION QUOTE_DUST mint=%s — Jupiter has no route. Left in the wallet. Not selling again.",
                        pos.mint[:8],
                    )
                elif (
                    "POOL ACCOUNT NOT FOUND" in err.upper()
                    or "BONDING CURVE" in err.upper()
                    or "TOKEN HAS MIGRATED" in err.upper()
                    or "6063" in err
                ):
                    self._sell_cooldown[pos.mint] = time.time() + 2.0
                    log.warning(
                        "SELL still open mint=%s — Lightning missed (%s). Not dust. Jupiter runs next.",
                        pos.mint[:8], err[:80],
                    )
                elif broadcast:
                    n = int(getattr(pos, "_sell_broadcast_fails", 0) or 0) + 1
                    pos._sell_broadcast_fails = n  # type: ignore[attr-defined]
                    if n >= 2:
                        pos._sell_stopped = True  # type: ignore[attr-defined]
                        self._sell_cooldown[pos.mint] = time.time() + 3600.0
                        log.error(
                            "ACTION SELL_STOP mint=%s — %d failed swaps already paid fees. No more retries.",
                            pos.mint[:8], n,
                        )
                    else:
                        self._sell_cooldown[pos.mint] = time.time() + 180.0
                        log.warning(
                            "ACTION SELL_FAIL mint=%s broadcast %d/2 — wait 180s, a retry now only burns another fee",
                            pos.mint[:8], n,
                        )
                elif "quote" in err.lower():
                    no_route = (
                        "NO_ROUTES" in err.upper()
                        or "NO ROUTES" in err.upper()
                        or "NOT_TRADABLE" in err.upper()
                    )
                    rate_limited = "429" in err
                    if no_route or (rate_limited and int(getattr(pos, "_noroute_sells", 0) or 0) >= 1):
                        n = int(getattr(pos, "_noroute_sells", 0) or 0) + (0 if rate_limited and not no_route else 1)
                        pos._noroute_sells = n  # type: ignore[attr-defined]
                        try:
                            from src.execution.tx_route import mint_venue
                            venue = mint_venue(pos.mint)
                        except Exception:
                            venue = ""
                        # Jupiter is the only market for this mint. No route means it cannot be sold.
                        # Leave the coins in the wallet, close the book as DUST, and stop the loop.
                        if venue == "jupiter" and n >= 1 and not getattr(pos, "_nodust", False):
                            pos._dust_value = True  # type: ignore[attr-defined]
                            pos._quote_giveup = True  # type: ignore[attr-defined]
                            pos._last_sell_error = "sell_dust_quote out=0 no_route"  # type: ignore[attr-defined]
                            self._sell_cooldown[pos.mint] = time.time() + 3600.0
                            log.warning(
                                "ACTION QUOTE_DUST mint=%s — Jupiter has no route. Left in the wallet. Not selling again.",
                                pos.mint[:8],
                            )
                        else:
                            wait = 30.0
                            self._sell_cooldown[pos.mint] = time.time() + wait
                            log.warning(
                                "SELL still open mint=%s — no route, retry in %.0fs",
                                pos.mint[:8], wait,
                            )
                    else:
                        n = int(getattr(pos, "_quote_fails", 0) or 0) + 1
                        pos._quote_fails = n  # type: ignore[attr-defined]
                        unsellable = "400" in err or "NOT_TRADABLE" in err.upper()
                        if str(reason) == "unpriced_5m" and n >= 2:
                            pos._dust_value = True  # type: ignore[attr-defined]
                            pos._quote_giveup = True  # type: ignore[attr-defined]
                            self._sell_cooldown[pos.mint] = time.time() + 1.0
                            log.warning(
                                "ACTION QUOTE_DUST mint=%s — quote failed %d times (%s), clearing the panel",
                                pos.mint[:8], n, err[:80],
                            )
                        else:
                            urgent = (
                                str(reason).startswith("tp_")
                                or str(reason).startswith("take_profit")
                                or str(reason) in ("caller_closed", "caller_drop_7", "callout_age", "manual_now")
                            )
                            wait = 12.0 if ("429" in err or "stop" in str(reason)) else (2.0 if urgent else 15.0)
                            self._sell_cooldown[pos.mint] = time.time() + wait
                            log.warning(
                                "ACTION QUOTE_RETRY %d/25 mint=%s err=%s — wait %.0fs, no swap was sent",
                                n, pos.mint[:8], err[:140], wait,
                            )
                            if n >= 25:
                                log.error(
                                    "ACTION QUOTE_GIVEUP mint=%s reason=quote_failed_25 last_err=%s",
                                    pos.mint[:8], err[:160],
                                )
                                pos._quote_giveup = True  # type: ignore[attr-defined]
                elif "not_landed" in err:
                    wait = 3.0 if "stop" in str(reason) else 20.0
                    self._sell_cooldown[pos.mint] = time.time() + wait
                    log.warning(
                        "ACTION SELL_NOT_LANDED mint=%s — tx never confirmed, no fee counted, retry in %.0fs",
                        pos.mint[:8], wait,
                    )
                elif landed_unknown:
                    urgent = (
                        str(reason).startswith("tp_")
                        or str(reason).startswith("take_profit")
                        or "stop" in str(reason)
                        or str(reason) in ("caller_closed", "caller_drop_7", "callout_age", "manual_now")
                    )
                    wait = 2.0 if urgent else 30.0
                    self._sell_cooldown[pos.mint] = time.time() + wait
                    log.warning(
                        "ACTION SELL_NOT_LANDED mint=%s — tx not confirmed, tokens still held, retry in %.0fs",
                        pos.mint[:8], wait,
                    )
                else:
                    urgent = (
                        str(reason).startswith("tp_")
                        or str(reason).startswith("take_profit")
                        or "stop" in str(reason)
                        or str(reason) in ("caller_closed", "caller_drop_7", "callout_age", "manual_now")
                    )
                    wait = 1.0 if urgent else 10.0
                    self._sell_cooldown[pos.mint] = time.time() + wait
                    log.warning(
                        "SELL not sent mint=%s err=%s — wait %.0fs (simulation reject spends no fee)",
                        pos.mint[:8], err[:140], wait,
                    )
                return False, mark_price
            self._sell_cooldown.pop(pos.mint, None)
            pos._last_sell_sig = sig or ""  # type: ignore[attr-defined]
            log.info(
                "SELL ok mint=%s reason=%s exit_px=%s sig=%s",
                pos.mint[:8], reason,
                f"{exit_price:.8f}" if exit_price else "n/a",
                (sig or "")[:16],
            )
            return True, (exit_price if exit_price is not None else mark_price)
        except Exception:
            log.exception("sell handler error mint=%s", pos.mint[:8])
            if pos.mode == PositionMode.LIVE:
                return False, mark_price
            return True, mark_price

    async def sell_source_now(self, source: str) -> Dict[str, Any]:
        """Sell every open trade of the Live Control source. 100%, even at a loss."""
        source = str(source or "").strip().lower()
        if source not in ("launch", "dayscalp", "predict", "callout", "opencopy"):
            return {
                "ok": False,
                "error": "Select launch, dayscalp, predict, callout, or opencopy in Live controls",
                "source": source,
                "sold": 0,
            }
        targets = []
        for pos in self.positions.values():
            if pos.status != "OPEN":
                continue
            src = str(getattr(pos, "source", "") or "").lower()
            strat = str(getattr(pos, "strategy", "") or "").lower()
            match = src == source or strat == source
            if match:
                targets.append(pos)
        if not targets:
            return {"ok": False, "error": f"No open {source} trade", "source": source, "sold": 0}
        sold = 0
        for pos in targets:
            log.warning(
                "MANUAL -1 mint=%s %s — selling this trade in full now",
                pos.mint[:8], getattr(pos, "symbol", "") or "",
            )
            if await self._fire_manual(pos):
                sold += 1
        still = [pos for pos in targets if pos.status == "OPEN"]
        if sold <= 0:
            err = str(getattr(still[0], "_last_sell_error", "") or "") if still else "sell_failed"
            return {
                "ok": False,
                "error": err or "Sell did not land",
                "source": source,
                "sold": 0,
                "open": len(still),
            }
        return {"ok": True, "source": source, "sold": sold, "open": len(still)}

    async def sell_mint_now(self, mint: str, force: bool = False) -> Dict[str, Any]:
        """Sell one open token. force=True sells that mint now, any source. Otherwise −1 waits for UserHolds OUT."""
        mint = str(mint or "").strip()
        pos = next((p for p in self.positions.values() if p.status == "OPEN" and p.mint == mint), None)
        if pos is None:
            return {"ok": False, "error": "This bot is not holding that coin", "sold": 0}
        callout = str(getattr(pos, "source", "") or "") in ("callout", "opencopy") or str(getattr(pos, "strategy", "") or "") in ("callout", "opencopy")
        if not force and not callout:
            return {"ok": False, "error": "That trade is not a Callout", "sold": 0}
        if not force and not getattr(pos, "_caller_closed", False):
            return {"ok": False, "error": "−1 is only enabled when UserHolds is OUT", "sold": 0}
        log.warning(
            "MANUAL -1 mint=%s %s — selling this trade in full now",
            pos.mint[:8], getattr(pos, "symbol", "") or "",
        )
        ok = await self._fire_manual(pos)
        if not ok:
            err = str(getattr(pos, "_last_sell_error", "") or "") or "Sell did not land"
            return {"ok": False, "error": err, "sold": 0, "mint": mint}
        try:
            from src.observability.dashboard import set_openhold_status
            set_openhold_status(mint, "")
        except Exception:
            log.debug("openhold status clear failed", exc_info=True)
        return {"ok": True, "sold": 1, "mint": mint}

    def kick_caller_closed(self, pos) -> None:
        """Sell the moment the caller is out. Does not wait for the exit scan."""
        if pos is None or not _dropoff_for(pos):
            return
        if getattr(pos, "status", "") != "OPEN":
            return
        if getattr(pos, "_sell_busy", False):
            return
        pos._caller_closed = True
        pos._sell_busy = True
        pos._sell_stopped = False
        self._sell_cooldown.pop(pos.mint, None)

        async def _urgent(p=pos) -> None:
            try:
                pid = next((i for i, item in self.positions.items() if item is p), None)
                ok, exit_px = await self._maybe_sell(p, 1.0, "caller_closed", float(p.entry_price or 0))
                if ok and pid is not None and getattr(p, "status", "") == "OPEN":
                    await self.close_position(pid, "caller_closed", exit_price=exit_px or 0.0, frac=1.0)
            finally:
                p._sell_busy = False

        from src.execution.fast_lane import fire_sell
        fire_sell(pos.mint, f"caller_closed {getattr(pos, 'symbol', '') or pos.mint[:8]}", _urgent)

    async def _fire_manual(self, pos) -> bool:
        """Sell this coin now. Does not wait for the exit scan."""
        if getattr(pos, "_sell_busy", False):
            return False
        pos._sell_busy = True
        pos._sell_stopped = False
        pos._force_sell = False
        self._sell_cooldown.pop(pos.mint, None)
        try:
            ok, exit_px = await self._maybe_sell(pos, 1.0, "manual_now", float(pos.entry_price or 0))
            if not ok:
                err = str(getattr(pos, "_last_sell_error", "") or "")
                if "wallet_token_zero" in err:
                    pid = next((i for i, item in self.positions.items() if item is pos), None)
                    if pid is not None:
                        log.warning(
                            "MANUAL mint=%s — wallet holds 0, trade cleared. PnL unchanged",
                            pos.mint[:8],
                        )
                        await self.close_position(pid, "not_in_wallet", exit_price=0.0, frac=1.0)
                return False
            pid = next((i for i, item in self.positions.items() if item is pos), None)
            if pid is not None:
                await self.close_position(pid, "manual_now", exit_price=exit_px or 0.0, frac=1.0)
            return pos.status != "OPEN"
        finally:
            pos._sell_busy = False

    async def evaluate_exits(self) -> List[ClosedTrade]:
        """One exit pass at a time so a manual sell cannot double-send."""
        lock = getattr(self, "_exit_pass", None)
        if lock is None:
            self._exit_pass = asyncio.Lock()
            lock = self._exit_pass
        async with lock:
            return await self._evaluate_exits_locked()

    async def _evaluate_exits_locked(self) -> List[ClosedTrade]:
        """Run exit rules; LIVE positions only close after successful sell."""
        closed: List[ClosedTrade] = []
        now = time.time()
        # A caller who just sold is first. Other coins' wallet reads must not hold that sell.
        items = list(self.positions.items())
        items.sort(key=lambda item: 0 if self._exit_urgent(item[1]) else 1)
        for pos_id, pos in items:
            if pos.status != "OPEN" or getattr(pos, "_sell_busy", False):
                continue
            copy_now = (
                getattr(pos, "strategy", "") in ("callout", "opencopy", "predict")
                or getattr(pos, "source", "") in ("callout", "opencopy", "predict")
            )
            dropoff_now = False
            if copy_now and getattr(pos, "_caller_closed", False):
                dropoff_now = _dropoff_for(pos)
            if getattr(pos, "_force_sell", False) or dropoff_now:
                why = "manual_now" if getattr(pos, "_force_sell", False) else "caller_closed"
                pos._force_sell = False
                self._sell_cooldown.pop(pos.mint, None)

                async def _urgent(p=pos, reason=why, pid=pos_id) -> None:
                    if reason == "manual_now":
                        await self._fire_manual(p)
                        return
                    ok, exit_px = await self._maybe_sell(p, 1.0, reason, float(p.entry_price or 0))
                    if ok:
                        await self.close_position(pid, reason, exit_price=exit_px or 0.0, frac=1.0)

                from src.execution.fast_lane import fire_sell
                fire_sell(pos.mint, f"{why} {getattr(pos, 'symbol', '') or pos.mint[:8]}", _urgent)
                continue
            if pos.mode == PositionMode.LIVE and not getattr(pos, "_nodust", False):
                dead = False
                try:
                    from src.execution.tx_route import pump_mint
                    from src.storage.mint_flags import dust_source, mint_flag
                    err = str(getattr(pos, "_last_sell_error", "") or "")
                    src = dust_source(pos.mint) if mint_flag(pos.mint) == "DUST" else ""
                    dead = src in ("manual", "jupiter", "frozen", "worthless") or (
                        not pump_mint(pos.mint)
                        and ("NO_ROUTES" in err.upper() or "NOT_TRADABLE" in err.upper() or "NO ROUTES" in err.upper())
                    )
                except Exception:
                    dead = False
                if dead or bool(getattr(pos, "_dust_value", False) and getattr(pos, "_quote_giveup", False)):
                    try:
                        from src.storage.mint_flags import flag_dust_if_needed
                        flag_dust_if_needed(pos.mint, "no Jupiter route, left in the wallet")
                    except Exception:
                        pass
                    if not getattr(pos, "_dust_closed_logged", False):
                        pos._dust_closed_logged = True
                        log.warning(
                            "HELD %s is DUST — left on the open trade until the coins actually leave the wallet.",
                            getattr(pos, "symbol", "") or pos.mint[:8],
                        )
                    self._sell_cooldown[pos.mint] = time.time() + 3600.0
                    pos._sell_stopped = True
                    continue
            if pos.mode == PositionMode.LIVE and float(getattr(pos, "tokens", 0) or 0) <= 1 and not self._exit_urgent(pos):
                held = None
                try:
                    from src.execution.signer import pubkey_from_env
                    from src.execution.wallet_tokens import get_token_holding
                    pk = pubkey_from_env()
                    if pk:
                        one = await asyncio.wait_for(
                            get_token_holding(pk, pos.mint, fast=False, confirm_empty=2, ignore_seen=True),
                            timeout=2.5,
                        )
                        if one is not None:
                            held = int(one.get("raw") or 0) > 0
                except Exception:
                    held = None
                if held is False:
                    log.warning(
                        "FAKE OPEN mint=%s — wallet holds 0, clearing the card. PnL unchanged",
                        pos.mint[:8],
                    )
                    self._release_hold_mint(pos.mint)
                    t = await self.close_position(pos_id, "not_in_wallet", exit_price=0.0, frac=1.0)
                    if t:
                        closed.append(t)
                    continue
            if pos.mode == PositionMode.LIVE and not self._exit_urgent(pos):
                sold = self._confirmed_wallet_exit(pos)
                if sold:
                    held = await self._wallet_still_has(pos.mint)
                    if held is False:
                        t = await self._book_wallet_sale(pos, sold)
                        if t:
                            closed.append(t)
                        continue
                    if held is True:
                        log.info(
                            "WALLET still holds %s — a history sell is not a close",
                            pos.mint[:8],
                        )
            copy = (
                getattr(pos, "strategy", "") in ("callout", "opencopy", "predict")
                or getattr(pos, "source", "") in ("callout", "opencopy", "predict")
            )
            if copy and getattr(pos, "_caller_closed", False):
                dropoff_now = _dropoff_for(pos)
                if dropoff_now:
                    pos._sell_stopped = False
                    self._sell_cooldown.pop(pos.mint, None)
                    if not getattr(pos, "_caller_sell_logged", False):
                        pos._caller_sell_logged = True
                        try:
                            shown = f"{float(pos.since_buy_pct):+.2f}%"
                        except (TypeError, ValueError):
                            shown = "…"
                        log.warning(
                            "CALLOUT dropoff on mint=%s %s caller sold at %s — selling 100%%",
                            pos.mint[:8], getattr(pos, "symbol", "") or "", shown,
                        )
                    ok, exit_px = await self._maybe_sell(pos, 1.0, "caller_closed", float(pos.entry_price or 0))
                    if ok:
                        t = await self.close_position(pos_id, "caller_closed", exit_price=exit_px or 0.0, frac=1.0)
                        if t:
                            closed.append(t)
                    elif getattr(pos, "_quote_giveup", False):
                        log.warning(
                            "SELL still open mint=%s — no Jupiter route, position stays until a sell lands",
                            pos.mint[:8],
                        )
                        pos._quote_giveup = False  # type: ignore[attr-defined]
                    continue
            try:
                from src.storage.mint_flags import mint_flag
                manual = mint_flag(pos.mint)
            except Exception:
                manual = ""
            if manual == "NODUST":
                pos._nodust = True  # type: ignore[attr-defined]
            elif manual == "DUST" and pos.mode == PositionMode.LIVE:
                from src.storage.mint_flags import dust_source, set_mint_flag
                src = dust_source(pos.mint)
                if src not in ("manual", "jupiter", "frozen", "worthless"):
                    set_mint_flag(pos.mint, "CLEAR")
                    pos._dust_value = False  # type: ignore[attr-defined]
                    pos._quote_giveup = False  # type: ignore[attr-defined]
                    pos._sell_stopped = False  # type: ignore[attr-defined]
                    log.warning(
                        "FLAG DUST cleared mint=%s — Lightning missed. Not dust until Jupiter says no route.",
                        pos.mint[:8],
                    )
                else:
                    # The Held Token button, or Jupiter no-route, is final.
                    log.warning(
                        "MANUAL DUST mint=%s — stored. Left in the wallet, even if it can still be traded.",
                        pos.mint[:8],
                    )
                    self._release_hold_mint(pos.mint)
                    t = await self.close_position(pos_id, "DUST", exit_price=0.0, frac=1.0)
                    if t:
                        closed.append(t)
                    continue
            price = self.get_price(pos)
            # A raw token price is not "flat". Do not pretend the trade is at entry.
            if pos.mode == PositionMode.LIVE and getattr(pos, "_mark_at", 0) + 15 < now and not self._exit_urgent(pos):
                pos._mark_at = now  # type: ignore[attr-defined]
                try:
                    jup = getattr(self, "_trader_jup", None)
                    raw = int(pos.tokens or 0)
                    if jup and raw > 0 and pos.entry_sol > 0:
                        q = await jup.quote_token_to_sol(pos.mint, raw, slippage_bps=1000)
                        if q and q.out_amount > 0:
                            sol_out = q.out_amount / 1_000_000_000
                            if pos.entry_price < 1e-6:
                                pos.entry_price = 1.0
                                if not pos.peak_price or pos.peak_price < 0.2:
                                    pos.peak_price = 1.0
                            mark = sol_out / pos.entry_sol
                            shown = getattr(pos, "since_buy_pct", None)
                            fake_gain = shown is not None and float(shown) > 50 and mark < 0.5
                            predict_here = (
                                getattr(pos, "strategy", "") == "predict"
                                or getattr(pos, "source", "") == "predict"
                                or pos.mint in (getattr(self, "_predict_mints", None) or set())
                            )
                            callout_here = (
                                getattr(pos, "strategy", "") == "callout"
                                or getattr(pos, "source", "") == "callout"
                            )
                            if predict_here or callout_here:
                                pos._quote_sol = sol_out
                                if sol_out < 0.000001:
                                    pos._dust_value = True  # type: ignore[attr-defined]
                                cost_now = float(pos.entry_sol or 0) + float(getattr(pos, "fees_sol", 0) or 0)
                                wallet_ret = (sol_out / cost_now - 1.0) if cost_now > 0 else (mark - 1)
                                log.info(
                                    "%s mark quote mint=%s value=%.6f SOL cost=%.4f ret=%+.1f%% — panel stays on the Pump price",
                                    "CALLOUT" if callout_here else "PREDICT",
                                    pos.mint[:8], sol_out, cost_now, wallet_ret * 100,
                                )
                            elif fake_gain or mark < 0.5:
                                self.set_price(pos.mint, max(mark, 0.01))
                                price = max(mark, 0.01)
                                pos.entry_price = 1.0
                                pos.since_buy_pct = round((mark - 1) * 100, 2)
                                if fake_gain:
                                    pos.peak_since_pct = 0.0
                                else:
                                    prev_peak = getattr(pos, "peak_since_pct", None)
                                    pos.peak_since_pct = round(max(0.0, float(prev_peak or 0), float(pos.since_buy_pct)), 2)
                                pos.peak_price = max(mark, 0.01)
                                pos.tp_ladder_hit = []
                                pos._dex_mark_at = 0
                                if sol_out < 0.0005:
                                    pos._dust_value = True
                                log.info(
                                    "EXIT MARK real quote mint=%s value=%.6f SOL cost=%.4f ret=%+.1f%% — panel percent cleared",
                                    pos.mint[:8], sol_out, pos.entry_sol, (mark - 1) * 100,
                                )
                            elif getattr(pos, "_dex_mark_at", 0) + 20 >= now and abs((shown or 0) - (mark - 1) * 100) < 40:
                                log.info(
                                    "EXIT MARK skip jupiter mint=%s value=%.4f mark=%.3f — panel already matches",
                                    pos.mint[:8], sol_out, mark,
                                )
                            else:
                                self.set_price(pos.mint, mark)
                                price = mark
                                pos._noroute = 0  # type: ignore[attr-defined]
                                if sol_out < 0.000001:
                                    pos._dust_value = True  # type: ignore[attr-defined]
                                log.info(
                                    "EXIT MARK %s value=%.4f SOL cost=%.4f ret=%+.1f%% hold=%.0fs",
                                    pos.mint[:8], sol_out, pos.entry_sol, (mark - 1) * 100, now - pos.entry_time,
                                )
                        else:
                            pos._noroute = int(getattr(pos, "_noroute", 0) or 0) + 1  # type: ignore[attr-defined]
                            err = str(getattr(jup, "_last_error", "") or "")
                            log.info(
                                "EXIT no route mint=%s count=%d hold=%.0fs",
                                pos.mint[:8], pos._noroute, now - pos.entry_time,
                            )
                            no_route = "NO_ROUTES" in err.upper() or "NO ROUTES" in err.upper() or "NOT_TRADABLE" in err.upper()
                            try:
                                from src.execution.tx_route import pump_mint
                                jupiter_only = not pump_mint(pos.mint)
                            except Exception:
                                jupiter_only = True
                            if no_route and jupiter_only and not getattr(pos, "_nodust", False):
                                pos._dust_value = True  # type: ignore[attr-defined]
                                pos._quote_giveup = True  # type: ignore[attr-defined]
                                pos._last_sell_error = err  # type: ignore[attr-defined]
                                try:
                                    from src.storage.mint_flags import flag_dust_if_needed
                                    flag_dust_if_needed(pos.mint, "Jupiter has no route")
                                except Exception:
                                    pass
                                log.warning(
                                    "ACTION QUOTE_DUST mint=%s — Jupiter has no route. Left in the wallet. Not selling again.",
                                    pos.mint[:8],
                                )
                                self._sell_cooldown[pos.mint] = time.time() + 3600.0
                                self._release_hold_mint(pos.mint)
                                t = await self.close_position(pos_id, "DUST", exit_price=0.0, frac=1.0)
                                if t:
                                    closed.append(t)
                                continue
                except Exception as e:
                    text = str(e)
                    if "429" not in text:
                        pos._noroute = int(getattr(pos, "_noroute", 0) or 0) + 1
                        log.info(
                            "EXIT no route mint=%s count=%d hold=%.0fs err=%s",
                            pos.mint[:8], pos._noroute, now - pos.entry_time, text[:80],
                        )
                    else:
                        log.debug("exit mark failed", exc_info=True)
            if price <= 0 and pos.entry_price > 0:
                price = pos.entry_price
            pos.mark_price(price)
            ret = (price / pos.entry_price - 1.0) if pos.entry_price else 0.0
            if price <= 0:
                ret = 0.0
            hold = now - pos.entry_time
            action = None
            pending_tp = None
            frac = 1.0
            predict_pos = (
                getattr(pos, "strategy", "") == "predict"
                or getattr(pos, "source", "") == "predict"
                or bool(getattr(pos, "quick_entry", False))
                or pos.mint in (getattr(self, "_predict_mints", None) or set())
            )
            kind = str(getattr(pos, "strategy", "") or "").strip().lower()
            src = str(getattr(pos, "source", "") or "").strip().lower()
            callout_pos = kind in ("callout", "opencopy") or src in ("callout", "opencopy")
            if not callout_pos and bool(getattr(pos, "_from_openholds", False)):
                if src not in ("launch", "dayscalp", "predict") and kind not in ("launch", "dayscalp", "predict"):
                    callout_pos = True
                    pos.strategy = src if src in ("callout", "opencopy") else "opencopy"
                    pos.source = pos.strategy
                    pos.stop_pct = None
            if callout_pos:
                pos.stop_pct = None
            held_quiet = predict_pos or callout_pos
            noroute = int(getattr(pos, "_noroute", 0) or 0)
            # A missing Jupiter route is not dust. Only a real quote under 0.000001 SOL is.
            # Callout and OpenCopy stay open until take-profit, stop, or the caller exit actually sells.
            unsellable = (
                hold >= 45
                and bool(getattr(pos, "_dust_value", False))
                and not held_quiet
                and not getattr(pos, "_nodust", False)
            )
            if pos.mode == PositionMode.LIVE and unsellable:
                log.warning(
                    "DUST unsellable mint=%s ret=%+.1f%% noroute=%d — close book, no swap, next coin can be bought",
                    pos.mint[:8], ret * 100, noroute,
                )
                try:
                    from src.storage.mint_flags import flag_dust_if_needed
                    flag_dust_if_needed(pos.mint, "unsellable, left in the wallet")
                except Exception:
                    log.debug("dust flag failed", exc_info=True)
                self._release_hold_mint(pos.mint)
                t = await self.close_position(pos_id, "DUST", exit_price=0.0, frac=1.0)
                if t:
                    closed.append(t)
                continue

            # Live: empty wallet OR dust (sell value ~0) → auto-drop and move on
            if pos.mode == PositionMode.LIVE and getattr(pos, "_dust_check_at", 0) + 20 < now and not self._exit_urgent(pos):
                pos._dust_check_at = now  # type: ignore[attr-defined]
                try:
                    from src.execution.signer import pubkey_from_env
                    from src.execution.wallet_tokens import get_token_holding
                    pk = pubkey_from_env()
                    if pk:
                        held = await get_token_holding(pk, pos.mint)
                        raw_bal = None if held is None else int(held.get("raw") or 0)
                        if raw_bal is None:
                            log.debug("EXIT balance unknown mint=%s — keep OPEN", pos.mint[:8])
                        elif raw_bal <= 0 and hold >= 45:
                            log.warning(
                                "EXIT balance read 0 mint=%s hold=%.0fs — left OPEN, wallet may still hold it",
                                pos.mint[:8], hold,
                            )
                        elif raw_bal <= 0:
                            log.info(
                                "EXIT balance 0 mint=%s hold=%.0fs — keep OPEN until the buy can land",
                                pos.mint[:8], hold,
                            )
                        else:
                            sol_val = None
                            try:
                                if self._sell_handler and getattr(self, "_trader_jup", None):
                                    jup = self._trader_jup
                                    q = await jup.quote_token_to_sol(pos.mint, int(raw_bal), slippage_bps=1000)
                                    if q:
                                        sol_val = q.out_amount / 1e9
                            except Exception:
                                sol_val = None
                            # Only real quoted dust. A failed quote is not dust.
                            # Under 0.000001 SOL. NODUST in the database keeps it.
                            if (
                                sol_val is not None
                                and sol_val < 0.000001
                                and hold >= 60
                                and not getattr(pos, "_nodust", False)
                            ):
                                log.warning(
                                    "EXIT dust_drop mint=%s sol_out≈%.6f entry=%.4f hold=%.0fs",
                                    pos.mint[:8], sol_val, pos.entry_sol, hold,
                                )
                                action, frac = "dust_drop", 1.0
                except Exception:
                    log.debug("dust check failed", exc_info=True)

            if getattr(pos, "_force_sell", False):
                pos._force_sell = False
                pos._sell_stopped = False
                self._sell_cooldown.pop(pos.mint, None)
                if action is None:
                    action, frac = "manual_now", 1.0
                    log.warning(
                        "MANUAL -1 mint=%s %s — selling this trade in full now",
                        pos.mint[:8], getattr(pos, "symbol", "") or "",
                    )
            if callout_pos and action is None and getattr(pos, "_caller_closed", False):
                try:
                    since_n = float(pos.since_buy_pct)
                except (TypeError, ValueError):
                    since_n = None
                dropoff = False
                try:
                    from src.config.runtime import get_config
                    dropoff = _dropoff_for(pos)
                    drop_at = float(getattr(get_config(), "callout_drop_pct", 7.0))
                except (TypeError, ValueError):
                    dropoff = False
                    drop_at = 7.0
                if drop_at <= 0:
                    drop_at = 7.0
                # Dropoff on: the caller leaving is the exit. Sell the whole trade now.
                if dropoff:
                    action, frac = "caller_closed", 1.0
                    pos._sell_stopped = False
                    self._sell_cooldown.pop(pos.mint, None)
                    if not getattr(pos, "_caller_sell_logged", False):
                        pos._caller_sell_logged = True
                        shown = "…" if since_n is None else f"{since_n:+.2f}%"
                        log.warning(
                            "CALLOUT dropoff on mint=%s %s caller sold at %s — selling 100%%",
                            pos.mint[:8], getattr(pos, "symbol", "") or "", shown,
                        )
                        try:
                            from src.observability.action_log import record_action
                            record_action(
                                "CALLOUT",
                                level="WARNING",
                                detail=(
                                    f"{getattr(pos, 'symbol', '') or pos.mint[:8]} caller sold. "
                                    f"Dropoff is on — selling 100% at {shown} since buy"
                                ),
                                mint=pos.mint,
                                symbol=getattr(pos, "symbol", "") or "",
                            )
                        except Exception:
                            pass
                else:
                    # Dropoff off. Under +1% the trade, the pin, and the caller stay.
                    # At +1% or more, sell now. A negative print after the caller is out
                    # arms the recovery percent. While the caller still holds, it does not sell.
                    if since_n is not None and since_n < 0:
                        pos._drop_was_neg = True
                    came_back = bool(getattr(pos, "_drop_was_neg", False))
                    if since_n is not None and came_back and since_n + 1e-9 >= drop_at:
                        action, frac = "caller_drop_7", 1.0
                        pos._sell_stopped = False
                        self._sell_cooldown.pop(pos.mint, None)
                        if not getattr(pos, "_caller_sell_logged", False):
                            pos._caller_sell_logged = True
                            log.warning(
                                "CALLOUT caller already out mint=%s %s back from negative to %+.2f%% — selling now",
                                pos.mint[:8], getattr(pos, "symbol", "") or "", since_n,
                            )
                            try:
                                from src.observability.action_log import record_action
                                record_action(
                                    "CALLOUT",
                                    level="WARNING",
                                    detail=(
                                        f"{getattr(pos, 'symbol', '') or pos.mint[:8]} caller already left. "
                                        f"Since buy came back from negative and touched {since_n:+.2f}% — selling now"
                                    ),
                                    mint=pos.mint,
                                    symbol=getattr(pos, "symbol", "") or "",
                                )
                            except Exception:
                                pass
                    elif since_n is not None and not came_back and since_n + 1e-9 >= 1.0:
                        action, frac = "caller_closed", 1.0
                        pos._sell_stopped = False
                        self._sell_cooldown.pop(pos.mint, None)
                        if not getattr(pos, "_caller_sell_logged", False):
                            pos._caller_sell_logged = True
                            log.warning(
                                "CALLOUT caller closed mint=%s %s since %+.2f%% — selling now",
                                pos.mint[:8], getattr(pos, "symbol", "") or "", since_n,
                            )
                            try:
                                from src.observability.action_log import record_action
                                record_action(
                                    "CALLOUT",
                                    level="WARNING",
                                    detail=(
                                        f"{getattr(pos, 'symbol', '') or pos.mint[:8]} left the caller wallet "
                                        f"at {since_n:+.2f}% since buy — selling now"
                                    ),
                                    mint=pos.mint,
                                    symbol=getattr(pos, "symbol", "") or "",
                                )
                            except Exception:
                                pass
            if callout_pos and action is None:
                try:
                    from src.config.runtime import callout_sell_seconds, get_config
                    age_limit = callout_sell_seconds(getattr(get_config(), "callout_sell_age", "0"))
                except Exception:
                    age_limit = 0.0
                if age_limit > 0 and hold + 1e-9 >= age_limit:
                    action, frac = "callout_age", 1.0
                    pos._sell_stopped = False
                    self._sell_cooldown.pop(pos.mint, None)
                    log.warning(
                        "CALLOUT AGE mint=%s held %.0fs — indication %s, selling this trade in full",
                        pos.mint[:8], hold, getattr(get_config(), "callout_sell_age", ""),
                    )
                    try:
                        from src.observability.action_log import record_action
                        record_action(
                            "CALLOUT",
                            level="WARNING",
                            detail=(
                                f"{getattr(pos, 'symbol', '') or pos.mint[:8]} held {hold:.0f}s "
                                f"— Callout sell indication {getattr(get_config(), 'callout_sell_age', '')}, selling 100%"
                            ),
                            mint=pos.mint,
                            symbol=getattr(pos, "symbol", "") or "",
                        )
                    except Exception:
                        pass

            if callout_pos and action is None:
                try:
                    from src.config.runtime import get_config
                    copy_stop = float(getattr(get_config(), "copy_stop_pct", -15.0) or 0)
                except (TypeError, ValueError):
                    copy_stop = 0.0
                live_since = _shown_pct(pos)
                if copy_stop < 0 and live_since is not None and float(live_since) <= copy_stop:
                    action, frac = "copy_stop", 1.0
                    pos._sell_stopped = False
                    if self._sell_cooldown.get(pos.mint, 0) <= now:
                        log.warning(
                            "COPY STOP mint=%s %s since buy %+.2f%% hit %+.0f%% — selling 100%%",
                            pos.mint[:8], getattr(pos, "symbol", "") or "", float(live_since), copy_stop,
                        )

            # No since-buy percent (the panel shows …).
            # No price and Jupiter will not quote it: that is dust. Leave it.
            # A coin that simply has not been priced yet still gets one sell try after 5 minutes.
            # The card percent is the one on screen. The exit must use that, not a flat 0%.
            shown_now = _shown_pct(pos)
            if shown_now is not None:
                pos.since_buy_pct = shown_now
                prev_peak = getattr(pos, "peak_since_pct", None)
                pos.peak_since_pct = round(max(0.0, float(prev_peak or 0), float(shown_now)), 2)
            since = getattr(pos, "since_buy_pct", None)
            if since is None and action not in ("dust_drop", "dust_unsellable", "manual_now", "callout_age", "caller_closed", "caller_drop_7", "m5_null"):
                # No live price at the time stop is dust. Do not sell it.
                limit = float(self.cfg.time_stop_seconds or 0)
                try:
                    from src.storage.mint_flags import flag_dust_if_needed, mint_flag
                    already = mint_flag(pos.mint) == "DUST"
                except Exception:
                    already = False
                    flag_dust_if_needed = None
                if not held_quiet and not getattr(pos, "_nodust", False) and (already or (limit > 0 and hold >= limit)):
                    if flag_dust_if_needed:
                        flag_dust_if_needed(pos.mint, "no live price, left in the wallet")
                    log.warning(
                        "DUST unpriced mint=%s held %.0fs — no live price, left in the wallet. Not selling.",
                        pos.mint[:8], hold,
                    )
                    self._sell_cooldown[pos.mint] = time.time() + 3600.0
                    self._release_hold_mint(pos.mint)
                    t = await self.close_position(pos_id, "DUST", exit_price=0.0, frac=1.0)
                    if t:
                        closed.append(t)
                    continue
                if not held_quiet:
                    continue
            if held_quiet and action == "unpriced_5m":
                action = None
            if since is not None:
                ret = float(since) / 100.0
                scalp = (
                getattr(pos, "strategy", "") in ("dayscalp", "launch")
                and not callout_pos
            )
                if scalp or (pos.entry_price and pos.entry_price >= 0.2):
                    pos.entry_price = 1.0 if scalp else pos.entry_price
                    price = (1.0 if scalp else pos.entry_price) * (1.0 + ret)
                if scalp and getattr(pos, "peak_since_pct", None) is not None:
                    peak_px = 1.0 + float(pos.peak_since_pct) / 100.0
                    if peak_px > float(pos.peak_price or 0):
                        pos.peak_price = peak_px
                shown = round(float(since), 1)
                if action is None and getattr(pos, "_pct_shown", None) != shown:
                    pos._pct_shown = shown
                    pos._pct_seen_at = now
                elif (
                    action is None
                    and hold >= 300
                    and now - float(getattr(pos, "_pct_seen_at", now) or now) >= 300
                ):
                    if bool(getattr(pos, "_dust_value", False)) and not getattr(pos, "_nodust", False):
                        log.warning(
                            "SINCE BUY stuck at %+.1f%% for 5m and dust — drop mint=%s, next coin",
                            shown, pos.mint[:8],
                        )
                        action, frac = "dust_drop", 1.0
                    else:
                        if not held_quiet:
                            log.warning(
                                "SINCE BUY stuck at %+.1f%% for %.0fs — selling mint=%s",
                                shown, now - float(pos._pct_seen_at), pos.mint[:8],
                            )
                            action, frac = "stale_pct_5m", 1.0

            scalp = (
                getattr(pos, "strategy", "") in ("dayscalp", "launch")
                and not callout_pos
            )
            levels = [float(x) for x in (self.cfg.tp_levels or [])]
            first_tp = levels[0] if levels else 0.30
            second_tp = levels[1] if len(levels) > 1 else None
            third_tp = levels[2] if len(levels) > 2 else None

            def _hit(level: float) -> bool:
                if level is None:
                    return False
                return any(abs(float(x) - float(level)) < 1e-4 for x in (pos.tp_ladder_hit or []))

            banked = _hit(first_tp)
            second_banked = _hit(second_tp)
            third_banked = _hit(third_tp)
            raw_runner = getattr(self.cfg, "runner_stop_pct", None)
            runner = -0.12 if raw_runner is None else float(raw_runner)
            style = getattr(self.cfg, "exit_style", "ath") or "ath"
            # ath, and any saved style with two sales: floor and trail wait for the second sale.
            # plan keeps the floor after the first sale. classic is unchanged.
            two_step = style != "classic" and style != "plan" and second_tp is not None

            if since is not None and scalp:
                if two_step and second_banked and third_tp is None:
                    pos.stop_pct = runner
                    if ret <= runner:
                        action, frac = "runner_stop", 1.0
                        if self._sell_cooldown.get(pos.mint, 0) <= now:
                            log.warning(
                                "ACTION RUNNER STOP mint=%s since %+.1f%% floor %+.0f%% — selling the rest",
                                pos.mint[:8], ret * 100, runner * 100,
                            )
                elif two_step and banked and third_tp is not None:
                    # Hard stop is only while no sale has filled. The last piece
                    # waits for +75% or reverse sale 3.
                    pos.stop_pct = None
                elif banked and not two_step:
                    pos.stop_pct = runner
                    if ret <= runner:
                        action, frac = "runner_stop", 1.0
                        if self._sell_cooldown.get(pos.mint, 0) <= now:
                            log.warning(
                                "ACTION RUNNER STOP mint=%s since %+.1f%% floor %.0f%% — selling the rest",
                                pos.mint[:8], ret * 100, runner * 100,
                            )
                else:
                    pos.stop_pct = float(self.cfg.hard_stop_pct)
                    if ret <= float(self.cfg.hard_stop_pct):
                        action, frac = "hard_stop", 1.0
                        if self._sell_cooldown.get(pos.mint, 0) <= now:
                            log.warning(
                                "ACTION SL HIT mint=%s since %+.1f%% hard stop %.0f%% — selling 100%%",
                                pos.mint[:8], ret * 100, self.cfg.hard_stop_pct * 100,
                            )
            elif since is not None and not held_quiet and ret <= self.cfg.hard_stop_pct:
                action, frac = "hard_stop", 1.0
                if self._sell_cooldown.get(pos.mint, 0) <= now:
                    log.warning(
                        "ACTION SL HIT mint=%s since %+.1f%% hard stop %.0f%% — selling 100%%",
                        pos.mint[:8], ret * 100, self.cfg.hard_stop_pct * 100,
                    )

            # Worth less than 0.000001 SOL: leave the dust in the wallet and trade the next coin.
            worth = (pos.entry_sol * (1.0 + ret)) if since is not None else None
            if worth is not None and worth < 0.000001 and hold >= 60 and not getattr(pos, "_nodust", False):
                log.warning(
                    "ACTION DUST leave mint=%s worth≈%.6f SOL since %+.1f%% — no swap, next coin",
                    pos.mint[:8], worth, ret * 100,
                )
                _flag_if_worthless(pos.mint, int(pos.tokens or 0))
                self._release_hold_mint(pos.mint)
                t = await self.close_position(pos_id, "dust_left", exit_price=0.0, frac=1.0)
                if t:
                    closed.append(t)
                continue

            # Discretionary exits only after min_hold (stops instant 1-2s SL noise)
            held_enough = hold >= float(getattr(pos, "min_hold_seconds", 0.0) or 0.0)

            if action is None and held_enough:
                # Percent SL from entry (preferred for dayscalp)
                if pos.stop_pct is not None and ret <= float(pos.stop_pct):
                    action, frac = "pct_stop", 1.0
                # Optional absolute stop price
                elif pos.stop_price and pos.entry_price and price > 0 and price <= pos.stop_price:
                    action, frac = "price_stop", 1.0

            if getattr(pos, "peak_since_pct", None) is not None:
                peak_ret = float(pos.peak_since_pct) / 100.0
            else:
                peak_ret = (pos.peak_price / pos.entry_price - 1.0) if pos.entry_price and pos.peak_price and pos.entry_price >= 0.2 else 0.0

            # Bank only at the live price. A peak that already fell back is not a fill.
            # Fractions are of the original buy. 25% of a half-sold coin is half of what is left.
            if action is None:
                checked = float(getattr(pos, "_predict_m5_at", 0) or 0)
                if now - checked >= 2:
                    pos._predict_m5_at = now
                    try:
                        status, value = await asyncio.to_thread(_pump_m5_now, pos.mint)
                    except Exception:
                        status, value = "down", None
                    if status == "ok":
                        # 0 and any negative number stay open.
                        pos._pump_m5 = float(value)
                        pos._m5_nulls = 0
                        pos._pump_m5_null = False
                        if callout_pos:
                            pos.live_m5_pct = float(value)
                    elif status == "null" and callout_pos:
                        # Sell only if a second read is also JSON null.
                        # A 0 on either read keeps the trade open.
                        try:
                            again, again_value = await asyncio.to_thread(_pump_m5_now, pos.mint)
                        except Exception:
                            again, again_value = "down", None
                        if again == "ok":
                            pos._pump_m5 = float(again_value)
                            pos.live_m5_pct = float(again_value)
                            pos._m5_nulls = 0
                            pos._pump_m5_null = False
                        elif again != "null":
                            pass
                        else:
                            # A JSON null is shown. It does not sell. 0 does not sell either.
                            pos.live_m5_pct = None
                            pos._pump_m5 = None
                            pos._pump_m5_null = True
                    elif status == "null":
                        pos._pump_m5 = None
                        pos._pump_m5_null = True
                if predict_pos and action is None:
                    m5_now = getattr(pos, "_pump_m5", None)
                    entry_m5 = getattr(pos, "entry_m5_pct", None)
                    # Locked at the open. +6.4% at the buy means the stop is -6.4%, and it never moves.
                    stop_at = None if entry_m5 is None else -abs(float(entry_m5))
                    if m5_now is not None and stop_at is not None and float(m5_now) <= stop_at:
                        action, frac = "m5_negative", 1.0
                        pos._exit_m5 = float(m5_now)
                        log.warning(
                            "PREDICT 5m %+.1f%% hit the fixed stop %+.1f%% — selling mint=%s",
                            float(m5_now), stop_at, pos.mint[:8],
                        )
                        try:
                            from src.observability.action_log import record_action
                            record_action(
                                "PREDICT",
                                level="WARNING",
                                detail=f"5m {float(m5_now):+.1f}% hit fixed stop {stop_at:+.1f}% — selling {pos.mint[:8]}",
                                mint=pos.mint,
                            )
                        except Exception:
                            pass

            pace_on = False
            pace_margin = 5.0
            try:
                from src.config.runtime import get_config
                live_cfg = get_config()
                pace_on = bool(getattr(live_cfg, "fast_pace_sell", False))
                pace_margin = float(getattr(live_cfg, "fast_pace_margin_pct", 5.0) or 0)
            except Exception:
                pace_on = False
            if (
                action is None
                and pace_on
                and pace_margin > 0
                and since is not None
                and float(since) + 1e-9 >= pace_margin
            ):
                action, frac = "fast_pace_sell", 1.0
                if self._sell_cooldown.get(pos.mint, 0) <= now:
                    log.warning(
                        "FAST PACE mint=%s since %+.2f%% — target +%.2f%% selling 100%%",
                        pos.mint[:8], float(since), pace_margin,
                    )

            if callout_pos and action is None and held_enough and since is not None:
                levels_now = [float(x) for x in (self.cfg.tp_levels or [])]
                if levels_now:
                    touch = float(levels_now[0])
                    need = touch * 100.0
                    paid = float(pos.entry_sol or 0)
                    quote = float(getattr(pos, "_quote_sol", 0) or 0)
                    now_pct = float(since)
                    # SOL back versus what was paid at the open. The live 5m tape is not this.
                    # A peak that already fell back is not a fill. Both have to be at the target now.
                    sol_pct = None
                    if paid > 0 and 0.000001 <= quote < paid * 8:
                        sol_pct = (quote / paid - 1.0) * 100.0
                        if now_pct > -30 and sol_pct < now_pct - 40:
                            sol_pct = None
                    use = now_pct
                    # Live Control take-profit. Since-buy is the wallet pnlPercentage.
                    if (
                        touch not in (pos.tp_ladder_hit or [])
                        and use + 1e-9 >= need
                    ):
                        action, frac = f"tp_touch_{int(touch * 100)}pct", 1.0
                        pos.tp_ladder_hit.append(touch)
                        pending_tp = touch
                        log.warning(
                            "CALLOUT TOUCH mint=%s from the buy %+.1f%% sol %s — target +%.0f%% selling this trade",
                            pos.mint[:8], now_pct,
                            "n/a" if sol_pct is None else f"{sol_pct:+.1f}%",
                            touch * 100.0,
                        )
                        try:
                            from src.observability.action_log import record_action
                            record_action(
                                "CALLOUT",
                                level="WARNING",
                                detail=f"From the buy {now_pct:+.1f}% — target +{touch * 100:.0f}% — selling {pos.mint[:8]}",
                                mint=pos.mint,
                                symbol=getattr(pos, "symbol", "") or "",
                            )
                        except Exception:
                            pass

            if predict_pos and action is None and held_enough:
                levels_now = [float(x) for x in (self.cfg.tp_levels or [])]
                if levels_now:
                    touch = float(levels_now[0])
                    need = touch * 100.0
                    peak = float(getattr(pos, "peak_since_pct", 0) or 0)
                    now_pct = float(getattr(pos, "since_buy_pct", 0) or 0)
                    # Once the high tags the target, sell at the current price.
                    # Still at +15% or already back under +7%: both sell. Do not wait for a dip under the target.
                    if touch not in pos.tp_ladder_hit and (peak + 1e-9 >= need or now_pct + 1e-9 >= need):
                        action, frac = f"tp_touch_{int(touch * 100)}pct", 1.0
                        pos.tp_ladder_hit.append(touch)
                        pending_tp = touch
                        log.warning(
                            "PREDICT TOUCH mint=%s high %+.1f%% now %+.1f%% — target +%.0f%% was reached, selling at the current price",
                            pos.mint[:8], peak, now_pct, touch * 100.0,
                        )
                        try:
                            from src.observability.action_log import record_action
                            record_action(
                                "PREDICT",
                                level="WARNING",
                                detail=f"Touched +{touch * 100:.0f}% (high {peak:+.1f}%, now {now_pct:+.1f}%) — selling {pos.mint[:8]}",
                                mint=pos.mint,
                                symbol=getattr(pos, "symbol", "") or "",
                            )
                        except Exception:
                            pass

            if predict_pos and action is None and held_enough:
                quote_sol = float(getattr(pos, "_quote_sol", 0) or 0)
                cost = float(pos.entry_sol or 0) + float(getattr(pos, "fees_sol", 0) or 0)
                if quote_sol > 0 and cost > 0:
                    wallet_ret = quote_sol / cost - 1.0
                    peak = float(getattr(pos, "peak_since_pct", 0) or 0)
                    rem = float(pos.remaining_frac or 1.0)
                    for lvl, f in zip(self.cfg.tp_levels, self.cfg.tp_fractions):
                        need = float(lvl) * 100.0
                        # The Pump high and the SOL quote both have to clear the target.
                        # A quote alone sold coins that had never been up, and the fill lost.
                        if wallet_ret >= float(lvl) and peak + 1e-9 >= need and lvl not in pos.tp_ladder_hit:
                            part = float(f)
                            if 0 < rem < 0.99:
                                part = min(1.0, part / rem)
                            action, frac = f"take_profit_{int(float(lvl) * 100)}pct", part
                            pos.tp_ladder_hit.append(lvl)
                            pending_tp = lvl
                            log.info(
                                "PREDICT TP mint=%s quote %.4f SOL cost %.4f is %+.1f%% — selling",
                                pos.mint[:8], quote_sol, cost, wallet_ret * 100.0,
                            )
                            break
                    if action is None and quote_sol + 1e-9 < cost:
                        log.info(
                            "PREDICT no TP mint=%s quote %.4f SOL is under cost %.4f",
                            pos.mint[:8], quote_sol, cost,
                        )

            if action is None and held_enough and ret > 0 and not predict_pos and not callout_pos:
                rem = float(pos.remaining_frac or 1.0)
                for lvl, f in zip(self.cfg.tp_levels, self.cfg.tp_fractions):
                    if ret >= lvl and lvl not in pos.tp_ladder_hit:
                        part = float(f)
                        if 0 < rem < 0.99:
                            part = min(1.0, part / rem)
                        action, frac = f"take_profit_{int(lvl * 100)}pct", part
                        pos.tp_ladder_hit.append(lvl)
                        pending_tp = lvl
                        log.info(
                            "TP HIT mint=%s sell %.0f%% of original at +%.0f%% (now %+.1f%% peak %+.1f%%)",
                            pos.mint[:8], float(f) * 100, lvl * 100, ret * 100, peak_ret * 100,
                        )
                        break

            # A saved fallback replaces the built-in reverse ladder.
            # Arm at +13% means: tagged +13%, now back under +13%, the sale has not filled.
            arm = float(getattr(self.cfg, "fallback_arm_pct", 0) or 0)
            style_now = getattr(self.cfg, "exit_style", "ath") or "ath"
            if action is None and held_enough and since is not None and scalp and arm > 0:
                target = levels[-1] if levels else None
                if target is not None and not _hit(target) and peak_ret >= arm and ret < arm:
                    action, frac = "reverse_fallback", 1.0
                    log.warning(
                        "ACTION FALLBACK mint=%s peak %+.1f%% now %+.1f%% — tagged +%.0f%%, sale not filled, selling 100%%",
                        pos.mint[:8], peak_ret * 100, ret * 100, arm * 100,
                    )
            elif action is None and held_enough and since is not None and scalp and style_now == "ath":
                if third_tp is not None and not third_banked and peak_ret >= 0.70 and ret <= 0.65:
                    action, frac = "reverse_sale_3", 1.0
                    log.warning(
                        "ACTION REVERSE3 mint=%s peak %+.1f%% now %+.1f%% — third sale not filled, selling the last piece",
                        pos.mint[:8], peak_ret * 100, ret * 100,
                    )
                elif not second_banked and peak_ret >= 0.25 and ret <= 0.22:
                    action, frac = "reverse_sale_2", 1.0
                    log.warning(
                        "ACTION REVERSE2 mint=%s peak %+.1f%% now %+.1f%% — second sale not filled, selling the rest",
                        pos.mint[:8], peak_ret * 100, ret * 100,
                    )
                elif not second_banked and peak_ret >= 0.10 and ret <= 0.07:
                    action, frac = "reverse_sale_1", 1.0
                    log.warning(
                        "ACTION REVERSE1 mint=%s peak %+.1f%% now %+.1f%% — no second sale, selling the rest",
                        pos.mint[:8], peak_ret * 100, ret * 100,
                    )

            # The last 25% is the +75% sale. Do not trail it away before that sale.
            style = getattr(self.cfg, "exit_style", "ath") or "ath"
            trail_on = float(getattr(self.cfg, "trail_activate_pct", 0) or 0) > 0 and peak_ret >= self.cfg.trail_activate_pct and pos.peak_price > 0
            if style not in ("plan", "classic") and scalp and third_tp is not None:
                trail_on = False
            elif style not in ("plan", "classic") and scalp and second_tp is not None:
                trail_on = trail_on and second_banked
            elif style == "plan" and scalp:
                trail_on = trail_on and banked
            if action is None and held_enough and trail_on and not held_quiet:
                dd = (price / pos.peak_price) - 1.0
                if dd <= -self.cfg.trail_drawdown_pct:
                    action, frac = "trailing_stop", 1.0
                    log.info(
                        "TRAIL HIT mint=%s peak=%.1f%% now=%.1f%% dd=%.1f%% (need dd<=%.1f%%)",
                        pos.mint[:8], peak_ret * 100, ret * 100, dd * 100,
                        -self.cfg.trail_drawdown_pct * 100,
                    )

            if action is None and held_enough and pos.peak_price > 0 and not scalp and not held_quiet:
                dd_from_peak = (price / pos.peak_price) - 1.0
                if peak_ret >= 0.25 and dd_from_peak <= -0.40:
                    action, frac = "peak_giveback", 1.0
                    log.info(
                        "PEAK GIVEBACK mint=%s peak=%.1f%% now=%.1f%% dd_peak=%.1f%%",
                        pos.mint[:8], peak_ret * 100, ret * 100, dd_from_peak * 100,
                    )

            style = getattr(self.cfg, "exit_style", "ath") or "ath"
            # Time stop covers every print above the hard stop. The old -3%
            # floor left a gap down to the hard stop, so -4% could sit for an hour.
            if style == "classic":
                quiet = ret < self.cfg.time_stop_min_gain
            else:
                quiet = peak_ret < float(self.cfg.time_stop_min_gain or 0) and ret > float(self.cfg.hard_stop_pct)
            if (
                action is None
                and held_enough
                and not (scalp and banked)
                and not held_quiet
                and not getattr(pos, "_dust_value", False)
                and not getattr(pos, "_quote_giveup", False)
                and float(self.cfg.time_stop_seconds or 0) > 0
                and hold >= self.cfg.time_stop_seconds
                and (quiet or getattr(pos, "since_buy_pct", None) is None)
            ):
                if getattr(pos, "_time_stop_logged", False) and self._sell_cooldown.get(pos.mint, 0) > now:
                    continue
                action, frac = "time_stop", 1.0
                if not getattr(pos, "_time_stop_logged", False):
                    pos._time_stop_logged = True  # type: ignore[attr-defined]
                    try:
                        from src.observability.action_log import record_action
                        record_action(
                            "TIME STOP",
                            level="WARNING",
                            detail=f"{getattr(pos, 'symbol', '') or pos.mint[:8]} held {hold:.0f}s since {ret * 100:+.1f}%",
                            mint=pos.mint,
                            symbol=getattr(pos, "symbol", "") or "",
                        )
                    except Exception:
                        pass
                    if getattr(pos, "since_buy_pct", None) is None:
                        log.warning(
                            "ACTION TIME STOP mint=%s held %.0fs — no live price yet, selling on the %ss time stop",
                            pos.mint[:8], hold, int(self.cfg.time_stop_seconds),
                        )
                    else:
                        log.warning(
                            "ACTION TIME STOP mint=%s held %.0fs since %+.1f%% — under the gain needed",
                            pos.mint[:8], hold, ret * 100,
                        )

            # A full coin that never tagged a sale cannot block the next buy.
            # This is a real sell, not a dust drop.
            if (
                action is None
                and scalp
                and held_enough
                and not banked
                and hold >= 20 * 60
            ):
                action, frac = "max_hold", 1.0
                log.warning(
                    "ACTION MAX HOLD mint=%s held %.0fs since %+.1f%% — not dust, selling 100%%",
                    pos.mint[:8], hold, ret * 100,
                )

            # Callout, OpenCopy, and Predict do not use the scalp stops.
            # Hard stop is the exception: 0 in Live Control means off. Any other value sells 100%.
            if callout_pos and action in (
                "time_stop", "pct_stop", "price_stop",
                "runner_stop", "trailing_stop", "peak_giveback", "max_hold",
                "stale_pct_5m", "unpriced_5m",
            ):
                action = None
                pos.stop_pct = None
            hard = float(getattr(self.cfg, "hard_stop_pct", 0) or 0)
            if hard < 0 and (callout_pos or predict_pos) and action is None and since is not None:
                since_pct = float(since)
                if since_pct <= hard * 100.0:
                    action, frac = "hard_stop", 1.0
                    if self._sell_cooldown.get(pos.mint, 0) <= now:
                        log.warning(
                            "ACTION HARD STOP mint=%s since %+.2f%% hard stop %.0f%% — selling 100%%",
                            pos.mint[:8], since_pct, hard * 100.0,
                        )
            trail_on = False
            try:
                from src.config.runtime import get_config
                trail_on = bool(getattr(get_config(), "callout_trailing", False))
            except Exception:
                trail_on = False
            arm = float(getattr(self.cfg, "trail_activate_pct", 0) or 0)
            drop = float(getattr(self.cfg, "trail_drawdown_pct", 0) or 0)
            if (
                trail_on
                and callout_pos
                and not predict_pos
                and action is None
                and since is not None
                and arm > 0
                and drop > 0
            ):
                now_pct = float(since)
                peak_pct = float(getattr(pos, "peak_since_pct", None) if getattr(pos, "peak_since_pct", None) is not None else now_pct)
                if peak_pct >= arm * 100.0 and (1.0 + peak_pct / 100.0) > 0:
                    retrace = ((1.0 + now_pct / 100.0) / (1.0 + peak_pct / 100.0)) - 1.0
                    if retrace <= -drop:
                        action, frac = "trailing_stop", 1.0
                        if self._sell_cooldown.get(pos.mint, 0) <= now:
                            log.warning(
                                "ACTION TRAIL mint=%s peak %+.2f%% now %+.2f%% retrace %.1f%% — need %.0f%% off the high, selling 100%%",
                                pos.mint[:8], peak_pct, now_pct, retrace * 100.0, -drop * 100.0,
                            )

            if not action:
                continue

            if action != "manual_now" and self._sell_cooldown.get(pos.mint, 0) > now:
                continue

            if _profit_sell_reason(action) and time.time() < float(getattr(pos, "_neg_skip_until", 0) or 0):
                _drop_pending_tp(pos, action, pending_tp)
                continue

            if action == "dust_drop":
                log.warning("ACTION DUST leave mint=%s — no swap, next coin", pos.mint[:8])
                _flag_if_worthless(pos.mint, int(pos.tokens or 0))
                self._release_hold_mint(pos.mint)
                t = await self.close_position(pos_id, "dust_left", exit_price=0.0, frac=1.0)
                if t:
                    closed.append(t)
                continue

            async def _send(pid=pos_id, p=pos, part=frac, why=action, mark=price, tp=pending_tp) -> None:
                await self._finish_exit(pid, p, part, why, mark, tp)

            from src.execution.fast_lane import fire_sell
            fire_sell(pos.mint, f"{action} {getattr(pos, 'symbol', '') or pos.mint[:8]}", _send)
            continue
        return closed

    async def _finish_exit(self, pos_id, pos, frac, action, price, pending_tp) -> None:
        """The sell and the book update. Runs on the sell lane, not inside the scan."""
        ok, exit_px = await self._maybe_sell(pos, frac, action, price)
        if not ok and pending_tp is not None:
            try:
                pos.tp_ladder_hit.remove(pending_tp)
            except ValueError:
                pass
        if not ok:
            if action.startswith("take_profit_"):
                try:
                    lvl = int(action.split("_")[2].replace("pct", "")) / 100.0
                    if lvl in pos.tp_ladder_hit:
                        pos.tp_ladder_hit.remove(lvl)
                except (ValueError, IndexError):
                    pass
            err = str(getattr(pos, "_last_sell_error", "") or "")
            if err == "price_negative":
                pos._last_sell_error = ""
                return
            if (
                ("dust" in err or "not_in_wallet" in err or "6025" in err)
                and "sell_dust_quote" not in err
                and "sell_backstop" not in err
                and "wallet_token_zero" not in err
            ):
                log.warning(
                    "ACTION leave mint=%s err=%s — coin is not in the wallet, next coin can be bought",
                    pos.mint[:8], err[:120],
                )
                self._release_hold_mint(pos.mint)
                if "6025" in err or "sell_not_in_wallet" in err:
                    try:
                        from src.execution.wallet_gate import forget_absent_mint
                        forget_absent_mint(pos.mint, err[:120])
                    except Exception:
                        log.debug("forget absent mint failed", exc_info=True)
                await self.close_position(pos_id, "not_in_wallet", exit_price=0.0, frac=1.0)
                return
            if "sell_dust_quote" in err:
                out = 0
                try:
                    out = int(err.split("out=")[-1].split()[0])
                except (ValueError, IndexError):
                    out = 0
                if out >= 1_000_000:
                    log.warning(
                        "ACTION SELL waiting mint=%s — quote %.6f SOL is not dust, stop stays armed",
                        pos.mint[:8], out / 1_000_000_000,
                    )
                    return
                log.warning(
                    "ACTION leave mint=%s err=%s — quote is already dust, not opened again",
                    pos.mint[:8], err[:120],
                )
                try:
                    from src.storage.mint_flags import flag_dust_if_needed
                    flag_dust_if_needed(pos.mint, err[:120])
                except Exception:
                    log.debug("dust flag failed", exc_info=True)
                self._release_hold_mint(pos.mint)
                await self.close_position(pos_id, "DUST", exit_price=0.0, frac=1.0)
                return
            if "wallet_token_zero" in err:
                log.warning(
                    "ACTION SELL mint=%s — wallet holds 0, clearing the trade. No swap, PnL unchanged",
                    pos.mint[:8],
                )
                try:
                    from src.execution.wallet_gate import forget_absent_mint
                    forget_absent_mint(pos.mint, "wallet balance is 0")
                except Exception:
                    log.debug("clear supabase after zero balance failed", exc_info=True)
                try:
                    from src.execution.wallet_tokens import mark_sold
                    mark_sold(pos.mint)
                except Exception:
                    pass
                self._release_hold_mint(pos.mint)
                await self.close_position(pos_id, "not_in_wallet", exit_price=0.0, frac=1.0)
                return
            if "sell_backstop" in err:
                log.warning(
                    "ACTION SELL waiting mint=%s — backstop was a real coin, stop stays armed",
                    pos.mint[:8],
                )
                return
            if getattr(pos, "_quote_giveup", False):
                log.warning(
                    "SELL still open mint=%s — no Jupiter route, position stays until a sell lands",
                    pos.mint[:8],
                )
                pos._quote_giveup = False
                return
            return
        await self.close_position(pos_id, action, exit_price=exit_px, frac=frac)

    def _exit_urgent(self, pos) -> bool:
        """True when a sell rule is already true. The scan must not quote before that send."""
        if getattr(pos, "_force_sell", False) or getattr(pos, "_caller_closed", False):
            return True
        kind = str(getattr(pos, "strategy", "") or "").strip().lower()
        src = str(getattr(pos, "source", "") or "").strip().lower()
        if kind == "predict" or src == "predict":
            return False
        try:
            since = float(pos.since_buy_pct)
        except (TypeError, ValueError):
            return False
        hard = float(getattr(self.cfg, "hard_stop_pct", 0) or 0)
        if hard < 0 and since <= hard * 100.0:
            return True
        try:
            from src.config.runtime import get_config
            live_cfg = get_config()
            if bool(getattr(live_cfg, "fast_pace_sell", False)):
                margin = float(getattr(live_cfg, "fast_pace_margin_pct", 5.0) or 0)
                if margin > 0 and since + 1e-9 >= margin:
                    return True
        except Exception:
            pass
        levels = getattr(self.cfg, "tp_levels", None) or []
        if levels and since + 1e-9 >= float(levels[0]) * 100.0:
            return True
        return False

    def _push_remote(self) -> None:
        rows = []
        for p in self.positions.values():
            if p.status != "OPEN":
                continue
            try:
                from src.execution.wallet_gate import _ABSENT, _load_absent
                _load_absent()
                if p.mint in _ABSENT:
                    continue
            except Exception:
                pass
            rows.append(self._position_row(p))
        try:
            from src.storage.supabase_store import get_store
            get_store().write_book(rows)
        except Exception:
            log.debug("supabase position write failed", exc_info=True)

    def restore_remote(self) -> int:
        snap = self._consume_restart_hold()
        if snap is not None:
            n = self._restore_row_list(snap, "RESTART HOLD")
            self._push_remote()
            log.warning("RESTART HOLD restored %d open position(s) — saved book matched to the snapshot", n)
            return n
        try:
            from src.storage.supabase_store import get_store
            book = get_store().read_book()
            rows = book.get("payload") if isinstance(book, dict) else None
            if not isinstance(rows, list):
                rows = get_store().get("open_position")
            if book.get("status") == "OPEN" and not rows and book.get("mint"):
                rows = [book]
        except Exception:
            return 0
        if not isinstance(rows, list):
            return 0
        return self._restore_row_list(rows, "SQLITE")

    def _restart_hold_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "logs" / "restart_hold.json"

    def save_restart_hold(self) -> None:
        """Write the open trade before a reload. An empty list means the book was flat."""
        rows = [self._position_row(p) for p in self.positions.values() if p.status == "OPEN"]
        path = self._restart_hold_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"ts": time.time(), "positions": rows}), encoding="utf-8")
        except Exception:
            log.exception("restart snapshot write failed")
        try:
            import threading
            threading.Thread(target=self._push_remote, name="restart-book", daemon=True).start()
        except Exception:
            log.debug("restart book push failed", exc_info=True)
        if rows:
            log.warning(
                "RESTART snapshot saved %s — it is restored after the reload",
                ",".join((r.get("symbol") or r["mint"][:8]) for r in rows),
            )
        else:
            log.warning("RESTART snapshot saved flat — a stuck saved row will not be restored")

    def _consume_restart_hold(self):
        """Fresh snapshot, or None. Empty list means this reload was flat on purpose."""
        path = self._restart_hold_path()
        if not path.is_file():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            log.warning("restart snapshot unreadable — using the saved book")
            return None
        age = time.time() - float((data or {}).get("ts") or 0)
        if age < 0 or age > 30 * 60:
            try:
                path.unlink()
            except Exception:
                pass
            return None
        try:
            path.unlink()
        except Exception:
            log.debug("restart snapshot delete failed", exc_info=True)
        rows = (data or {}).get("positions") or []
        if not isinstance(rows, list):
            rows = []
        keep = {str(r.get("mint") or "") for r in rows if isinstance(r, dict)}
        try:
            from src.storage.supabase_store import get_store
            book = get_store().read_book() or {}
            old = str(book.get("mint") or "")
            if old and old not in keep:
                from src.execution.wallet_gate import forget_absent_mint
                forget_absent_mint(old, "not in the restart snapshot")
                log.warning("RESTART dropped stuck saved hold %s", old[:8])
        except Exception:
            log.debug("restart snapshot supabase compare failed", exc_info=True)
        return rows

    def _position_row(self, p: OpenPosition) -> dict:
        return {
            "id": p.id,
            "mint": p.mint,
            "mode": p.mode.value,
            "entry_price": p.entry_price,
            "entry_sol": p.entry_sol,
            "tokens": p.tokens,
            "entry_time": p.entry_time,
            "peak_price": p.peak_price,
            "source": p.source,
            "fees_sol": p.fees_sol,
            "stop_pct": p.stop_pct,
            "min_hold_seconds": p.min_hold_seconds,
            "strategy": p.strategy,
            "remaining_frac": p.remaining_frac,
            "symbol": getattr(p, "symbol", "") or "",
            "name": getattr(p, "name", "") or "",
            "entry_m5_pct": getattr(p, "entry_m5_pct", None),
            "entry_m1_pct": getattr(p, "entry_m1_pct", None),
            "entry_s1_pct": getattr(p, "entry_s1_pct", None),
            "entry_h1_pct": getattr(p, "entry_h1_pct", None),
            "entry_price_usd": getattr(p, "entry_price_usd", None),
            "tp_hit": [float(x) for x in (p.tp_ladder_hit or [])],
            "peak_since_pct": getattr(p, "peak_since_pct", None),
            "low_since_pct": getattr(p, "low_since_pct", None),
            "since_buy_pct": getattr(p, "since_buy_pct", None),
            "quick_entry": bool(getattr(p, "quick_entry", False)),
            "callout_user": getattr(p, "callout_user", "") or "",
            "callout_wallet": getattr(p, "callout_wallet", "") or "",
            "from_openholds": bool(getattr(p, "_from_openholds", False)),
            "drop_was_neg": bool(getattr(p, "_drop_was_neg", False)),
        }

    def _restore_row_list(self, rows: list, label: str) -> int:
        n = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            mint = str(row.get("mint") or "")
            if not mint or any(p.status == "OPEN" and p.mint == mint for p in self.positions.values()):
                continue
            kind = str(row.get("source") or "").strip().lower()
            if kind in ("wallet_resume", "wallet_reconcile", "ledger_restore"):
                continue
            # A wallet-history sell is not proof the coin is gone. The balance check
            # in the exit loop closes it only when the wallet read is actually zero.
            try:
                from src.storage.mint_flags import mint_flag
                if mint_flag(mint) == "DUST":
                    log.warning("SQLITE skip restore %s — DUST flag, not an open trade", mint[:8])
                    continue
            except Exception:
                pass
            try:
                mode = PositionMode(row.get("mode") or "live")
            except ValueError:
                mode = PositionMode.LIVE
            pos = OpenPosition(
                id=str(row.get("id") or uuid.uuid4())[:12],
                mint=mint,
                mode=mode,
                entry_price=float(row.get("entry_price") or 1.0),
                entry_sol=float(row.get("entry_sol") or 0.02),
                tokens=float(row.get("tokens") or 0),
                entry_time=float(row.get("entry_time") or time.time()),
                peak_price=float(row.get("peak_price") or row.get("entry_price") or 1.0),
                source=str(row.get("source") or "saved"),
                fees_sol=float(row.get("fees_sol") or 0),
                stop_pct=row.get("stop_pct"),
                min_hold_seconds=float(row.get("min_hold_seconds") or 0),
                strategy=str(row.get("strategy") or ""),
                remaining_frac=float(row.get("remaining_frac") or 1),
            )
            pos.symbol = row.get("symbol") or ""
            pos.name = row.get("name") or ""
            if row.get("entry_m5_pct") is not None:
                pos.entry_m5_pct = float(row["entry_m5_pct"])
            if row.get("entry_m1_pct") is not None:
                pos.entry_m1_pct = float(row["entry_m1_pct"])
            if row.get("entry_s1_pct") is not None:
                pos.entry_s1_pct = float(row["entry_s1_pct"])
            if row.get("entry_h1_pct") is not None:
                pos.entry_h1_pct = float(row["entry_h1_pct"])
            if row.get("entry_price_usd"):
                pos.entry_price_usd = float(row["entry_price_usd"])
            pos.tp_ladder_hit = [float(x) for x in (row.get("tp_hit") or [])]
            if row.get("peak_since_pct") is not None:
                pos.peak_since_pct = float(row["peak_since_pct"])
            if row.get("low_since_pct") is not None:
                pos.low_since_pct = float(row["low_since_pct"])
            if row.get("since_buy_pct") is not None:
                pos.since_buy_pct = float(row["since_buy_pct"])
            pos.quick_entry = bool(row.get("quick_entry"))
            pos.callout_user = str(row.get("callout_user") or "")
            pos.callout_wallet = str(row.get("callout_wallet") or "")
            pos._from_openholds = bool(row.get("from_openholds"))
            pos._drop_was_neg = bool(row.get("drop_was_neg"))
            saved_px = float(getattr(pos, "entry_price_usd", 0) or 0)
            if saved_px > 0:
                pos._pump_entry_px = saved_px
                if pos.strategy in ("callout", "opencopy") or pos.source in ("callout", "opencopy"):
                    pos._entry_px_locked = True
                    pos._ath_from_entry = True
            if pos.strategy in ("callout", "opencopy") or pos.source in ("callout", "opencopy"):
                pos.stop_pct = None
            if pos.strategy == "predict" or pos.source == "predict" or pos.quick_entry:
                bag = getattr(self, "_predict_mints", None)
                if bag is None:
                    self._predict_mints = set()
                    bag = self._predict_mints
                bag.add(mint)
                pos.stop_pct = None
            pos._orig_raw = float(row.get("tokens") or 0)
            if pos.entry_price < 1e-6 or pos.entry_price > 100:
                pos.entry_price = 1.0
            if pos.peak_price < 0.2:
                pos.peak_price = max(pos.entry_price, 1.0)
            self.positions[pos.id] = pos
            self._prices.setdefault(mint, pos.peak_price or pos.entry_price)
            n += 1
            log.warning("%s restored open %s %s", label, pos.symbol or mint[:8], mint[:8])
        return n

    async def _wallet_still_has(self, mint: str) -> Optional[bool]:
        """True if this mint is still in the wallet. False only when a read says the balance is 0.

        None means the read failed. A failed read must not close the trade.
        """
        try:
            from src.execution.signer import pubkey_from_env
            from src.execution.wallet_tokens import get_token_holding
            pk = pubkey_from_env()
            if not pk or not mint:
                return None
            one = await asyncio.wait_for(
                get_token_holding(pk, mint, fast=True, confirm_empty=1),
                timeout=1.2,
            )
        except Exception:
            return None
        if one is None:
            return None
        return int(one.get("raw") or 0) > 0

    def _confirmed_wallet_exit(self, pos: OpenPosition) -> Optional[dict]:
        try:
            from src.observability.exec_history import get_exec_history
            return get_exec_history().confirmed_exit(
                pos.mint,
                float(pos.entry_time or 0),
                float(pos.tokens or 0),
            )
        except Exception:
            log.debug("wallet exit lookup failed", exc_info=True)
            return None

    async def _book_wallet_sale(self, pos: OpenPosition, sold: dict) -> Optional[ClosedTrade]:
        """The wallet history already shows this bag sold. Close the book and clear the saved hold."""
        got = max(0.0, float(sold.get("sol") or 0))
        pos._sol_out = got
        pos._last_sell_sig = str(sold.get("sig") or "")
        mark = getattr(pos, "since_buy_pct", None)
        peak = getattr(pos, "peak_since_pct", None)
        ath = 0.0
        if peak is not None:
            ath = max(ath, float(peak))
        if mark is not None:
            ath = max(ath, float(mark))
        live_px = float(getattr(pos, "_live_px", 0) or 0)
        entry_px = float(getattr(pos, "entry_price_usd", 0) or 0)
        px = live_px or entry_px or None
        pos._sales = [{
            "sol": got,
            "fee": 0.0,
            "frac": 1.0,
            "pct": None if mark is None else round(float(mark), 2),
            "ath": round(ath, 2),
            "sig": str(sold.get("sig") or ""),
            "reason": "wallet_already_sold",
            "px": px,
            "m5": getattr(pos, "live_m5_pct", None),
            "h1": getattr(pos, "live_h1_pct", None),
        }]
        try:
            from src.execution.wallet_gate import forget_absent_mint
            forget_absent_mint(pos.mint, "wallet history already sold")
        except Exception:
            log.debug("clear supabase after wallet sell failed", exc_info=True)
        try:
            from src.execution.wallet_tokens import mark_sold
            mark_sold(pos.mint)
        except Exception:
            pass
        self._release_hold_mint(pos.mint)
        log.warning(
            "WALLET ALREADY SOLD %s sol=%.6f — book and saved hold cleared, no second sell",
            pos.mint[:8], got,
        )
        return await self.close_position(pos.id, "wallet_already_sold", exit_price=pos.entry_price, frac=1.0)

    def _release_hold_mint(self, mint: str) -> None:
        path = Path(__file__).resolve().parents[2] / "logs" / "open_mints.json"
        kept: list = []
        try:
            data = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else []
            kept = [m for m in data if m != mint]
        except Exception:
            log.exception("could not read hold mint")
        try:
            from src.storage.supabase_store import get_store
            remote = get_store().get("open_mints")
            if isinstance(remote, list):
                kept = [m for m in remote if m != mint]
            get_store().set_open_mints(kept)
        except Exception:
            log.exception("could not release hold mint in supabase")

    def snapshot(self) -> Dict[str, Any]:
        def _caller_label(p) -> str:
            try:
                from src.observability.dashboard import live_caller_name
                source = str(getattr(p, "strategy", "") or getattr(p, "source", "") or "")
                return live_caller_name(
                    getattr(p, "mint", "") or "",
                    source,
                    getattr(p, "callout_user", "") or "",
                )
            except Exception:
                return "No caller holds"

        def _callout_hold(p) -> str:
            state = ""
            try:
                from src.observability.dashboard import hold_for_mint
                state = hold_for_mint(getattr(p, "mint", "") or "")
            except Exception:
                state = ""
            if state == "live":
                if not getattr(p, "_caller_closed", False):
                    p.user_holds = "live"
                    p._caller_closed = False
                    p._drop_was_neg = False
                else:
                    p.user_holds = "out"
                return "live" if not getattr(p, "_caller_closed", False) else "out"
            if state == "out":
                p.user_holds = "out"
                p._caller_closed = True
                return "out"
            return getattr(p, "user_holds", "") or ""

        open_pos = [p for p in self.positions.values() if p.status == "OPEN"]
        # A sold coin or a caller who has closed is not a live card. The book stays
        # until the wallet is empty, but the panels must not keep showing it.
        visible = []
        for p in open_pos:
            hold = _callout_hold(p)
            if hold == "out" or getattr(p, "_caller_closed", False):
                continue
            visible.append(p)
        unreal = 0.0
        for p in visible:
            if getattr(p, "since_buy_pct", None) is not None:
                unreal += float(p.entry_sol or 0) * (float(p.since_buy_pct) / 100.0) * float(p.remaining_frac or 1)
        closed_n = len(self.closed)
        try:
            from src.observability.ledger import get_ledger
            closed_n = int(get_ledger().session_trade_counts().get("closed_trades") or closed_n)
        except Exception:
            pass
        try:
            from src.config.runtime import get_config
            next_pct = float(getattr(get_config(), "callout_next_pct", -25.0))
        except Exception:
            next_pct = -25.0
        return {
            "open_count": len(visible),
            "closed_count": closed_n,
            "daily_pnl_sol": round(self.daily_pnl_sol + unreal, 6),
            "realized_pnl_sol": round(self.daily_pnl_sol, 6),
            "unrealized_pnl_sol": round(unreal, 6),
            "consecutive_losses": self.consecutive_losses,
            "paused": self.is_paused(),
            "paused_until": self.paused_until,
            "callout_next_pct": next_pct,
            "open": [
                {
                    "id": p.id,
                    "mint": p.mint,
                    "mode": p.mode.value,
                    "entry_sol": p.entry_sol,
                    "remaining_frac": p.remaining_frac,
                    "remaining_sol": round(float(p.entry_sol or 0) * float(p.remaining_frac or 1), 6),
                    "wallet_ui": getattr(p, "_wallet_ui", None),
                    "tokens": p.tokens,
                    "token_ui": getattr(p, "_ui_amount", None),
                    "entry_price": p.entry_price,
                    "entry_usd": (
                        None if float(getattr(p, "entry_price_usd", 0) or getattr(p, "_pump_entry_px", 0) or 0) <= 0
                        else float(getattr(p, "entry_price_usd", 0) or getattr(p, "_pump_entry_px", 0))
                    ),
                    "price_usd": None if float(getattr(p, "_live_px", 0) or 0) <= 0 else float(p._live_px),
                    "mcap_usd": None if float(getattr(p, "_live_mcap", 0) or 0) <= 0 else float(p._live_mcap),
                    "mark": self.get_price(p),
                    "pnl_pct": _shown_pct(p),
                    "pnl_usd": _shown_usd(p),
                    "sol_paid_pct": _shown_sol_pct(p),
                    "priced": getattr(p, "since_buy_pct", None) is not None,
                    "hold_s": int(time.time() - p.entry_time),
                    "entry_ts": p.entry_time,
                    "source": p.source,
                    "strategy": getattr(p, "strategy", "") or "",
                    "quick_entry": bool(getattr(p, "quick_entry", False)),
                    "symbol": getattr(p, "symbol", "") or "",
                    "name": getattr(p, "name", "") or "",
                    "entry_m5_pct": getattr(p, "entry_m5_pct", None),
                    "entry_m1_pct": getattr(p, "entry_m1_pct", None),
                    "entry_s1_pct": getattr(p, "entry_s1_pct", None),
                    "entry_h1_pct": getattr(p, "entry_h1_pct", None),
                    "live_m5_pct": getattr(p, "live_m5_pct", None),
                    "m5_null": bool(getattr(p, "_pump_m5_null", False)),
                    "live_h1_pct": getattr(p, "live_h1_pct", None),
                    "live_m1_pct": getattr(p, "live_m1_pct", None),
                    "live_s1_pct": getattr(p, "live_s1_pct", None),
                    "token_ath_pct": getattr(p, "token_ath_pct", None),
                    "fees_sol": round(float(p.fees_sol or 0), 6),
                    "stop_pct": p.stop_pct,
                    "hard_stop_pct": self.cfg.hard_stop_pct,
                    "trail_activate_pct": self.cfg.trail_activate_pct,
                    "trail_drawdown_pct": self.cfg.trail_drawdown_pct,
                    "tp_levels": [float(x) for x in self.cfg.tp_levels],
                    "tp_fractions": [float(x) for x in self.cfg.tp_fractions],
                    "tp_hit": [float(x) for x in (p.tp_ladder_hit or [])],
                    "peak_pct": round(max(0.0, float(getattr(p, "peak_since_pct", None) if getattr(p, "peak_since_pct", None) is not None else (((p.peak_price / p.entry_price - 1) * 100) if p.entry_price and p.peak_price and p.entry_price >= 0.2 else 0))), 2),
                    "low_pct": None if getattr(p, "low_since_pct", None) is None else round(float(p.low_since_pct), 2),
                    "pnl_sol": _shown_pnl_sol(p),
                    "user_holds": _callout_hold(p),
                    "caller_name": _caller_label(p),
                }
                for p in visible
            ],
            "recent_closed": [
                {
                    "mint": t.mint,
                    "pnl_sol": round(t.pnl_sol, 6),
                    "reason": t.exit_reason,
                    "hold_s": int(t.hold_seconds),
                    "mode": t.mode,
                }
                for t in self.closed[-10:]
            ],
        }


async def exit_loop(
    manager: PositionManager,
    interval_s: float = 1.0,
    stop_event: Optional[asyncio.Event] = None,
    on_tick: Optional[Callable] = None,
) -> None:
    """Sub-second-ish exit evaluation loop."""
    log.info("Exit loop started (interval=%.2fs)", interval_s)
    while True:
        if stop_event and stop_event.is_set():
            break
        try:
            closed = await manager.evaluate_exits()
            if on_tick:
                res = on_tick(manager, closed)
                if asyncio.iscoroutine(res):
                    await res
        except Exception:
            log.exception("Exit loop error")
        await asyncio.sleep(interval_s)
    log.info("Exit loop stopped")
