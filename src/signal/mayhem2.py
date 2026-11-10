"""Mayhem2: copy Mayhem_Mode's BUY / SELL trades on one young Mayhem coin at a time.

1. Discovery   advanced-indexer boards/mayhem: Mayhem coins, youngest first (age in seconds,
               ms = mayhem state). Only pump coins younger than 3 minutes are candidates.
2. Enrichment  mint-positions/<mint>: Mayhem_Mode's pnlUsd / pnlPercentage / amountHeld per coin.
3. Pin         the youngest active candidate that Mayhem_Mode is holding. One pin at a time.
               Its trades (trades/...?wallets=<Mayhem_Mode>) are read every 0.25s, and the
               on-chain trade socket (caller stream) feeds the same handler when it sees them
               first. Newest side wins: BUY -> buy Max position when flat; SELL -> sell 100%
               when holding. Signals older than the pin are never acted on.
4. Pause       coins-v2/<mint>/mayhem-state every 0.5s (and the board's ms every 0.8s).
               paused -> sell what is held, unpin, move to the next new coin. Mayhem_Mode no
               longer holding it (two reads) -> the same.

Trades only while Live Controls source is mayhem2 and Auto Trade is on. The page reads run while
the source is on or the Mayhem2 page was opened in the last 30s.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("mayhem2")

BOARD_URL = "https://advanced-indexer.pump.fun/boards/mayhem?tier=web&offset=0&limit=30"
POS_URL = "https://frontend-api-v3.pump.fun/mint-positions/{mint}?sortBy=TOP&pageSize=50"
TRADES_URL = "https://frontend-api-v3.pump.fun/trades/solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/{mint}?wallets={wallet}"
STATE_URL = "https://frontend-api-v3.pump.fun/coins-v2/{mint}/mayhem-state"

YOUNG_S = 180.0          # discovery window: coins younger than 3 minutes
TRADES_EVERY = 0.25
STATE_EVERY = 0.5
BOARD_EVERY = 0.8
POS_EVERY = 0.35         # one mint-positions read per tick, pinned coin first
STALE_SIGNAL_S = 6.0     # a trade older than this when first seen is history, not a signal

MIN_SIGNAL_SOL = 0.001   # a Mayhem_Mode trade smaller than this is noise: no signal, no price

# Exits (percent since our buy, from the live price). Sell fees are ~2%, so +3% is a real profit.
PROFIT_MIN_PCT = 3.0     # Mayhem_Mode SELL: sell now only at or above this
WAIT_PROFIT_S = 10.0     # ...otherwise wait up to this long for it, then sell anyway
TAKE_PROFIT_PCT = 20.0   # sell at once, no signal needed (these coins spike, then crash)
TRAIL_ARM_PCT = 10.0     # after the peak reached this...
TRAIL_KEEP = 0.5         # ...sell when half of the peak gain is gone
STOP_PCT = -15.0         # sell at once
MAX_HOLD_S = 25.0        # sell any trade held this long: half of Mayhem coins pause by ~34s old


def _wallet() -> str:
    from src.config.runtime import MAYHEM_WALLET
    return MAYHEM_WALLET


def _user() -> str:
    from src.config.runtime import MAYHEM_USER
    return MAYHEM_USER


def _f(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


class Mayhem2:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.board: List[Dict[str, Any]] = []
        self.board_at = 0.0
        self.board_err = ""
        self.pos: Dict[str, Dict[str, Any]] = {}       # mint -> Mayhem_Mode position on that coin
        self.state: Dict[str, Dict[str, Any]] = {}     # mint -> {state, reason, at}
        self.pin: Optional[Dict[str, Any]] = None
        self.used: Dict[str, Dict[str, Any]] = {}      # mints already traded / passed: never re-pinned
        self.seen_tx: Dict[str, float] = {}
        self.signals: deque = deque(maxlen=30)         # pinned coin's Mayhem_Mode trades, newest first
        self.notes: deque = deque(maxlen=40)
        self.view_at = 0.0
        self.want = ""                                  # newest signal side on the pin: buy / sell
        self.want_at = 0.0
        self.busy = ""                                  # "buy" while a buy is in flight
        self.last_px: Dict[str, float] = {}
        self.manager = None
        self.trader = None
        self.fill_px: Optional[Callable[[Any], float]] = None
        self.apply_since: Optional[Callable[[Any], None]] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.gone_reads: Dict[str, int] = {}
        self.kicked: Dict[str, float] = {}             # mint -> last sell start (retry at most every 2s)
        self.exiting: Dict[str, str] = {}              # mint -> why: sell decided, retried until gone
        self.wait_profit: Dict[str, float] = {}        # mint -> when Mayhem_Mode sold while we were not in profit
        self.sell_after_buy = False
        self.started = False

    # ---------- switches ----------
    def trading(self) -> bool:
        try:
            from src.config.runtime import bot_stopped, get_config, mayhem2_on
            cfg = get_config()
            return mayhem2_on() and bool(cfg.trade_enabled) and not bot_stopped()
        except Exception:
            return False

    def active(self) -> bool:
        try:
            from src.config.runtime import mayhem2_on
            if mayhem2_on():
                return True
        except Exception:
            pass
        return time.time() - self.view_at < 30

    def note(self, level: str, message: str, mint: str = "") -> None:
        now = time.time()
        with self.lock:
            if self.notes and self.notes[0]["m"] == message and now - self.notes[0]["ts"] < 20:
                self.notes[0]["n"] = int(self.notes[0].get("n") or 1) + 1
                self.notes[0]["ts"] = now
                return
            self.notes.appendleft({"ts": now, "level": level, "m": message, "mint": mint, "n": 1})
        log.info("MAYHEM2 %s", message)
        if level in ("buy", "sell", "ok", "warn"):
            try:
                from src.observability.action_log import record_action
                record_action("MAYHEM2", level="WARN" if level == "warn" else "INFO", detail=message[:220])
            except Exception:
                pass

    # ---------- reads ----------
    def _get(self, url: str, lane: str, timeout: float = 1.0) -> Any:
        from src.data.fast_get import mayhem2_get
        return mayhem2_get(url, lane=lane, timeout=timeout)

    def _open_pos(self, mint: str = ""):
        mgr = self.manager
        if mgr is None:
            return None
        for p in list(mgr.positions.values()):
            if p.status != "OPEN" or not p.mint:
                continue
            if "mayhem2" not in (str(getattr(p, "source", "") or ""), str(getattr(p, "strategy", "") or "")):
                continue
            if mint and p.mint != mint:
                continue
            return p
        return None

    def _other_open(self, mint: str):
        """An open Mayhem2 trade on another coin that can still sell (dust does not count)."""
        mgr = self.manager
        if mgr is None:
            return None
        for p in list(mgr.positions.values()):
            if p.status != "OPEN" or not p.mint or p.mint == mint:
                continue
            if "mayhem2" not in (str(getattr(p, "source", "") or ""), str(getattr(p, "strategy", "") or "")):
                continue
            if getattr(p, "_dust_value", False):
                continue
            try:
                from src.storage.mint_flags import mint_flag
                if mint_flag(p.mint) == "DUST":
                    continue
            except Exception:
                pass
            return p
        return None

    async def _board_loop(self) -> None:
        while True:
            if not self.active():
                await asyncio.sleep(0.5)
                continue
            t0 = time.time()
            try:
                body = await asyncio.to_thread(self._get, BOARD_URL, "mayhem2board", 1.5)
                rows = list((body or {}).get("entries") or []) if isinstance(body, dict) else []
                with self.lock:
                    self.board = rows
                    self.board_at = time.time()
                    self.board_err = ""
                    for r in rows:
                        mint = str(r.get("m") or "")
                        ms = str(r.get("ms") or "").lower()
                        if mint and ms:
                            prev = self.state.get(mint) or {}
                            # the coin's own mayhem-state read is newer than the board: keep it
                            if float(prev.get("at") or 0) < time.time() - 1.0 or prev.get("src") == "board":
                                self.state[mint] = {"state": ms, "reason": prev.get("reason", ""),
                                                    "at": time.time(), "src": "board"}
            except Exception as exc:
                with self.lock:
                    self.board_err = f"{type(exc).__name__}: {exc}"[:120]
            await asyncio.sleep(max(0.05, BOARD_EVERY - (time.time() - t0)))

    def candidates(self) -> List[Dict[str, Any]]:
        """Board coins younger than 3 minutes, pump only, active, newest first."""
        now = time.time()
        with self.lock:
            rows = list(self.board)
            age_shift = max(0.0, now - self.board_at) if self.board_at else 0.0
            states = dict(self.state)
        out = []
        for r in rows:
            mint = str(r.get("m") or "")
            if not mint or str(r.get("pg") or "") != "pump":
                continue
            age = float(r.get("age") or 0) + age_shift
            if age > YOUNG_S:
                continue
            st = str((states.get(mint) or {}).get("state") or r.get("ms") or "").lower()
            if st != "active":
                continue
            out.append(dict(r, _age=age))
        out.sort(key=lambda r: r["_age"])
        return out

    async def _pos_loop(self) -> None:
        turn = 0
        while True:
            if not self.active():
                await asyncio.sleep(0.5)
                continue
            t0 = time.time()
            pin = self.pin
            mint = ""
            if pin and time.time() - float((self.pos.get(pin["mint"]) or {}).get("at") or 0) >= 0.9:
                mint = pin["mint"]
            else:
                cands = [r["m"] for r in self.candidates() if r["m"] not in self.used]
                if cands:
                    cands.sort(key=lambda m: float((self.pos.get(m) or {}).get("at") or 0))
                    mint = cands[0]
                turn += 1
            if mint:
                try:
                    body = await asyncio.to_thread(self._get, POS_URL.format(mint=mint), "mayhem2pos", 1.2)
                    rows = (body or {}).get("positions") if isinstance(body, dict) else None
                    mine = None
                    for row in rows or []:
                        if str(row.get("walletAddress") or "") == _wallet():
                            mine = row
                            break
                    rec = {
                        "at": time.time(),
                        "found": mine is not None,
                        "held": float((mine or {}).get("amountHeld") or 0),
                        "pnlUsd": _f((mine or {}).get("pnlUsd")),
                        "pnlPercentage": _f((mine or {}).get("pnlPercentage")),
                        "costBasisUsd": _f((mine or {}).get("costBasisUsd")),
                        "realizedPnlUsd": _f((mine or {}).get("realizedPnlUsd")),
                    }
                    with self.lock:
                        self.pos[mint] = rec
                        if len(self.pos) > 300:
                            for old in sorted(self.pos, key=lambda k: self.pos[k]["at"])[:100]:
                                self.pos.pop(old, None)
                except Exception:
                    pass
            # Pinned: trades (4/s) and state (2/s) carry the trading, so side reads slow to stay under
            # pump's ~10 req/s. Unpinned: full speed to find the next coin.
            every = 0.6 if self.pin else POS_EVERY
            await asyncio.sleep(max(0.05, every - (time.time() - t0)))

    async def _trades_loop(self) -> None:
        while True:
            pin = self.pin
            if not pin:
                await asyncio.sleep(0.05)
                continue
            t0 = time.time()
            mint = pin["mint"]
            try:
                body = await asyncio.to_thread(
                    self._get, TRADES_URL.format(mint=mint, wallet=_wallet()), "mayhem2", 1.0,
                )
                rows = (body or {}).get("trades") if isinstance(body, dict) else None
                if rows and self.pin and self.pin["mint"] == mint:
                    fresh = []
                    for t in rows:
                        tx = str(t.get("txId") or "")
                        side = str(t.get("side") or "").lower()
                        if not tx or side not in ("buy", "sell"):
                            continue
                        if str((t.get("trader") or {}).get("address") or _wallet()) != _wallet():
                            continue
                        fresh.append({
                            "tx": tx, "side": side,
                            "ts": float(t.get("blockTimeMs") or 0) / 1000.0,
                            "usd": _f(t.get("valueUsd")), "sol": _f(t.get("valueNative")),
                            "px": _f(t.get("priceUsd")),
                        })
                    sized = [x for x in fresh if x.get("px") and (x.get("sol") or 0) >= 0.002]
                    if sized:
                        newest = max(sized, key=lambda x: x["ts"])
                        self._price(mint, float(newest["px"]))
                        self._signals(mint, fresh, "api")
            except Exception:
                pass
            await asyncio.sleep(max(0.02, TRADES_EVERY - (time.time() - t0)))

    async def _state_loop(self) -> None:
        while True:
            pin = self.pin
            if not pin:
                await asyncio.sleep(0.1)
                continue
            t0 = time.time()
            mint = pin["mint"]
            try:
                body = await asyncio.to_thread(self._get, STATE_URL.format(mint=mint), "mayhem2state", 1.0)
                if isinstance(body, dict) and body.get("state"):
                    with self.lock:
                        self.state[mint] = {
                            "state": str(body.get("state") or "").lower(),
                            "reason": str(body.get("pause_reason") or ""),
                            "at": time.time(), "src": "state",
                        }
            except Exception:
                pass
            await asyncio.sleep(max(0.05, STATE_EVERY - (time.time() - t0)))

    # ---------- signals ----------
    def on_stream(self, ev: Dict[str, Any]) -> None:
        """Caller-stream thread: an on-chain trade. Mayhem_Mode on the pinned coin only."""
        pin = self.pin
        if not pin or ev.get("mint") != pin["mint"] or ev.get("wallet") != _wallet():
            return
        sig = str(ev.get("signature") or "")
        if not sig or self.loop is None:
            return
        row = {"tx": sig, "side": str(ev.get("kind") or ""), "ts": float(ev.get("ts") or time.time()),
               "sol": _f(ev.get("sol")), "usd": None, "px": None}
        self.loop.call_soon_threadsafe(self._signals, pin["mint"], [row], "chain")

    def _signals(self, mint: str, rows: List[Dict[str, Any]], src: str) -> None:
        pin = self.pin
        if not pin or pin["mint"] != mint:
            return
        now = time.time()
        new = []
        for r in rows:
            if r["tx"] in self.seen_tx:
                continue
            self.seen_tx[r["tx"]] = now
            if r["ts"] < float(pin["ts"]) - 1.0 or now - r["ts"] > STALE_SIGNAL_S:
                continue            # before the pin, or history: shown nowhere, never traded
            r["src"] = src
            r["seen"] = now
            new.append(r)
        if len(self.seen_tx) > 5000:
            cut = now - 600
            self.seen_tx = {k: v for k, v in self.seen_tx.items() if v >= cut}
        if not new:
            return
        new.sort(key=lambda r: r["ts"])
        for r in new:
            self.signals.appendleft(r)
            if r.get("sol") is not None and float(r["sol"]) < MIN_SIGNAL_SOL:
                r["act"] = "ignored: tiny trade"
                continue
            self.want = r["side"]
            self.want_at = r["ts"]
            r["act"] = self._on_signal(r)
            log.info("MAYHEM2 SIGNAL %s %s via %s (%.2fs after the block) -> %s", r["side"].upper(),
                     pin.get("symbol") or mint[:8], src, max(0.0, now - r["ts"]), r["act"])

    def _price(self, mint: str, px: float) -> None:
        if px <= 0:
            return
        self.last_px[mint] = px
        pos = self._open_pos(mint)
        if pos is None:
            return
        pos._live_px = px
        pos._stream_px_at = time.time()
        if float(getattr(pos, "entry_price_usd", 0) or 0) <= 0:
            pos.entry_price_usd = px
            pos._pump_entry_px = px
        if self.apply_since is not None:
            try:
                self.apply_since(pos)
            except Exception:
                pass

    def _on_signal(self, r: Dict[str, Any]) -> str:
        """One signal, once. BUY buys only when flat and idle. SELL starts the exit once. A BUY that
        comes while buying, holding or selling is ignored: after the coins are sold, the next BUY buys."""
        pin = self.pin
        mint = pin["mint"]
        pos = self._open_pos(mint)
        if r["side"] == "buy":
            if self.busy == "buy":
                return "ignored: buy already sending"
            if pos is not None:
                return "ignored: selling" if (getattr(pos, "_sell_busy", False) or mint in self.exiting) else "ignored: holding"
            return self._try_buy(pin)
        if self.busy == "buy":
            self.sell_after_buy = True
            return "sell right after the buy lands"
        if pos is None:
            return "ignored: nothing held"
        if getattr(pos, "_sell_busy", False) or mint in self.exiting:
            return "ignored: already selling"
        return self._exit_on_signal(pos)

    def _try_buy(self, pin: Dict[str, Any]) -> str:
        mint = pin["mint"]
        if not self.trading():
            return "not bought: Auto Trade off / source not mayhem2"
        if pin.get("buys_off_until", 0) > time.time():
            return "not bought: last buy failed, short wait"
        try:
            paused_until = float(getattr(self.manager, "paused_until", 0) or 0)
        except Exception:
            paused_until = 0.0
        if paused_until > time.time():
            if time.time() - float(getattr(self, "_cb_note_at", 0) or 0) > 60:
                self._cb_note_at = time.time()
                left = (paused_until - time.time()) / 60.0
                self.note("warn", f"Circuit breaker on (losses in a row): no buy for {left:.0f} more min. Nothing is sent.")
            return "not bought: circuit breaker"
        if self._other_open(mint) is not None:
            return "not bought: another Mayhem2 trade is open"
        self.busy = "buy"
        self.sell_after_buy = False
        asyncio.ensure_future(self._buy(dict(pin)))
        return "BUY sent"

    # ---------- exits ----------
    def _since(self, pos) -> Optional[float]:
        v = getattr(pos, "since_buy_pct", None)
        try:
            return None if v is None else float(v)
        except (TypeError, ValueError):
            return None

    def _exit_on_signal(self, pos) -> str:
        """Mayhem_Mode sold. In profit after fees: sell now. Not yet: wait a short while for profit,
        inside the stop and the max hold (the coin can pause at any moment)."""
        since = self._since(pos)
        if since is None:
            self._sell(pos, "Mayhem_Mode SOLD (no live % yet)")
            return "SELL sent"
        if since >= PROFIT_MIN_PCT:
            self._sell(pos, f"Mayhem_Mode SOLD at {since:+.1f}% (profit)")
            return "SELL sent (profit)"
        if since <= STOP_PCT:
            self._sell(pos, f"Mayhem_Mode SOLD at {since:+.1f}% (at the stop)")
            return "SELL sent (stop)"
        self.wait_profit[pos.mint] = time.time()
        self.note("warn", f"{getattr(pos, 'symbol', '') or pos.mint[:6]} — Mayhem_Mode SOLD at {since:+.1f}%. "
                  f"Not a profit yet: holding up to {WAIT_PROFIT_S:.0f}s for {PROFIT_MIN_PCT:+.0f}% (stop {STOP_PCT:.0f}%).", pos.mint)
        return f"waiting for profit (now {since:+.1f}%)"

    def _sell(self, pos, why: str) -> None:
        self.exiting[pos.mint] = why
        self.wait_profit.pop(pos.mint, None)
        if self._may_kick(pos, 0.0) and self.manager.kick_now(pos, "mayhem2_sell"):
            self.kicked[pos.mint] = time.time()
        self.note("sell", f"{getattr(pos, 'symbol', '') or pos.mint[:6]} — {why}. Selling 100% now.", pos.mint)

    def _exit_check(self) -> None:
        """Every 0.1s on the open Mayhem2 trade: take profit, trail, stop, max hold, and the
        wait-for-profit after a SELL signal. Exits run even with Auto Trade off."""
        mgr = self.manager
        if mgr is None:
            return
        now = time.time()
        for pos in list(mgr.positions.values()):
            if pos.status != "OPEN" or not pos.mint:
                continue
            if "mayhem2" not in (str(getattr(pos, "source", "") or ""), str(getattr(pos, "strategy", "") or "")):
                continue
            mint = pos.mint
            if mint in self.exiting:
                if self._may_kick(pos, 2.0) and mgr.kick_now(pos, "mayhem2_sell"):
                    self.kicked[mint] = now      # the sell did not land: again, at most every 2s
                continue
            if getattr(pos, "_sell_busy", False):
                continue
            since = self._since(pos)
            peak = float(getattr(pos, "peak_since_pct", 0) or 0)
            held = now - float(getattr(pos, "entry_time", 0) or now)
            why = ""
            if since is not None and since >= TAKE_PROFIT_PCT:
                why = f"take profit {since:+.1f}%"
            elif since is not None and peak >= TRAIL_ARM_PCT and since <= peak * TRAIL_KEEP:
                why = f"gave back from {peak:+.1f}% to {since:+.1f}% (trail)"
            elif since is not None and since <= STOP_PCT:
                why = f"stop {since:+.1f}%"
            elif held >= MAX_HOLD_S:
                why = f"max hold {MAX_HOLD_S:.0f}s ({'n/a' if since is None else f'{since:+.1f}%'}) — pause risk"
            elif mint in self.wait_profit:
                if since is not None and since >= PROFIT_MIN_PCT:
                    why = f"profit {since:+.1f}% after Mayhem_Mode's SELL"
                elif now - self.wait_profit[mint] >= WAIT_PROFIT_S:
                    why = f"no profit {WAIT_PROFIT_S:.0f}s after Mayhem_Mode's SELL ({'n/a' if since is None else f'{since:+.1f}%'})"
            if why:
                self._sell(pos, why)
        for mint in list(self.exiting):
            if self._open_pos(mint) is None:
                self.exiting.pop(mint, None)
                self.wait_profit.pop(mint, None)

    def _may_kick(self, pos, every: float = 2.0) -> bool:
        """A sell that did not land retries, but not faster than every 2s, and never on DUST."""
        if getattr(pos, "_sell_busy", False) or getattr(pos, "_dust_value", False):
            return False
        try:
            from src.storage.mint_flags import mint_flag
            if mint_flag(pos.mint) == "DUST":
                return False
        except Exception:
            pass
        return time.time() - float(self.kicked.get(pos.mint) or 0) >= every

    async def _buy(self, pin: Dict[str, Any]) -> None:
        mint = pin["mint"]
        symbol = pin.get("symbol") or mint[:6]
        try:
            fill = await self.trader.buy(mint=mint, size_multiplier=1.0, source="mayhem2", ref_price=1.0)
        except Exception as exc:
            fill = None
            log.exception("MAYHEM2 buy crashed mint=%s", mint[:8])
            err = f"{type(exc).__name__}: {exc}"[:160]
        else:
            err = "" if (fill and fill.ok) else str(getattr(fill, "error", None) or "buy_failed")
        try:
            if not (fill and fill.ok):
                cur = self.pin
                if cur and cur["mint"] == mint:
                    cur["fails"] = int(cur.get("fails") or 0) + 1
                    cur["buys_off_until"] = time.time() + min(30.0, 3.0 * cur["fails"])
                self.note("warn", f"{symbol} — buy failed: {err}. Nothing bought.", mint)
                return
            self._tag(fill, pin)
            self.note("buy", f"{symbol} — Mayhem_Mode BOUGHT. Bought {float(fill.sol_in or 0):.4f} SOL.", mint)
        finally:
            self.busy = ""
            if self.sell_after_buy:
                self.sell_after_buy = False
                pos = self._open_pos(mint)
                if pos is not None:
                    self._exit_on_signal(pos)      # a SELL came in while buying

    def _tag(self, fill, pin: Dict[str, Any]) -> None:
        mint = pin["mint"]
        pos = None
        for p in self.manager.positions.values():
            if p.status == "OPEN" and p.mint == mint:
                pos = p
                break
        if pos is None:
            log.warning("MAYHEM2 %s — fill did not open a position", pin.get("symbol") or mint[:8])
            return
        px = 0.0
        try:
            px = float(self.fill_px(fill)) if self.fill_px else 0.0
        except Exception:
            px = 0.0
        if px <= 0:
            px = float(self.last_px.get(mint) or 0)
        pos.strategy = "mayhem2"
        pos.source = "mayhem2"
        pos.symbol = pin.get("symbol") or pos.symbol
        pos.name = pin.get("name") or getattr(pos, "name", "")
        pos.entry_price = 1.0
        pos.peak_price = 1.0
        pos.stop_pct = None
        pos.stop_price = None
        pos.min_hold_seconds = 0
        pos.entry_sol = float(fill.sol_in or pos.entry_sol or 0)
        pos.peak_since_pct = 0.0
        pos.since_buy_pct = 0.0
        pos.low_since_pct = 0.0
        pos._ath_from_entry = True
        pos.callout_user = _user()
        pos.callout_wallet = _wallet()
        pos.user_holds = "live"
        if px > 0:
            pos._pump_entry_px = px
            pos.entry_price_usd = px
            pos._entry_px_locked = True
        try:
            self.manager.set_price(mint, 1.0)
            self.manager._push_remote()
        except Exception:
            log.debug("mayhem2 entry save failed", exc_info=True)
        try:
            from src.observability.ledger import get_ledger
            get_ledger().record_open(
                id=f"m2-{mint[:8]}-{int(time.time())}",
                mint=mint,
                symbol=pos.symbol or mint[:6],
                mode=fill.mode,
                source="mayhem2",
                token_amount=fill.tokens_out,
                entry_sol=fill.sol_in,
                entry_fee_sol=fill.fees_sol,
                entry_tip_sol=0.0,
                price_sol_per_token=fill.effective_price,
                signature=fill.signature or "",
                price_usd=px or None,
                since_buy_pct=0.0,
                ath_since_pct=0.0,
                name=pos.name or "",
                callout_user=_user(),
            )
        except Exception:
            log.exception("mayhem2 ledger open failed")
        log.info("MAYHEM2 HOLDING %s sol=%.4f", pos.symbol or mint[:8], float(fill.sol_in or 0))

    # ---------- pin ----------
    def _set_pin(self, row: Dict[str, Any], why: str) -> None:
        mint = str(row.get("m") or row.get("mint") or "")
        self.pin = {
            "mint": mint,
            "symbol": str(row.get("t") or row.get("symbol") or ""),
            "name": str(row.get("n") or row.get("name") or ""),
            "image": str(row.get("i") or ""),
            "ts": time.time(),
            "age0": float(row.get("_age") or 0),
        }
        self.want = ""
        self.want_at = time.time() - 1.0
        self.signals.clear()
        self.gone_reads.pop(mint, None)
        self.note("ok", f"{self.pin['symbol'] or mint[:6]} — pinned ({why}). Following Mayhem_Mode's BUY / SELL.", mint)

    def _unpin(self, why: str, reason: str) -> None:
        pin = self.pin
        if not pin:
            return
        mint = pin["mint"]
        sym = pin.get("symbol") or mint[:6]
        self.used[mint] = {"why": why, "ts": time.time(), "symbol": sym}
        if len(self.used) > 400:
            for old in sorted(self.used, key=lambda k: self.used[k]["ts"])[:100]:
                self.used.pop(old, None)
        self.pin = None
        self.want = ""
        pos = self._open_pos(mint)
        if pos is not None:
            self.exiting[mint] = why
            self.wait_profit.pop(mint, None)
            started = self.manager.kick_now(pos, reason)
            if started:
                self.kicked[mint] = time.time()
            self.note("sell", f"{sym} — {why}. {'Selling 100% now' if started else 'Sell already going'}; unpinned.", mint)
        else:
            self.note("warn", f"{sym} — {why}. Unpinned; nothing held.", mint)

    async def _brain_loop(self) -> None:
        last_wait_note = 0.0
        while True:
            await asyncio.sleep(0.1)
            try:
                self._exit_check()          # open trades are always watched, page open or not
            except Exception:
                log.debug("mayhem2 exit check failed", exc_info=True)
            try:
                if not self.active():
                    continue
                pin = self.pin
                if pin:
                    mint = pin["mint"]
                    st = self.state.get(mint) or {}
                    state_now = str(st.get("state") or "")
                    if state_now == "paused":
                        self._unpin(f"pump paused it ({st.get('reason') or 'paused'})", "mayhem2_paused")
                        continue
                    if state_now == "completed":
                        # Mayhem run is over for this coin: sell what is held and take the next new coin.
                        self._unpin("Mayhem state is completed", "mayhem2_paused")
                        continue
                    rec = self.pos.get(mint) or {}
                    if rec.get("at") and float(rec["at"]) > float(pin["ts"]) and rec.get("held", 0) <= 0:
                        key = int(float(rec["at"]) * 10)
                        if self.gone_reads.get(mint + ":last") != key:
                            self.gone_reads[mint + ":last"] = key
                            self.gone_reads[mint] = int(self.gone_reads.get(mint) or 0) + 1
                        if self.gone_reads[mint] >= 2:
                            self._unpin("Mayhem_Mode no longer holds it", "mayhem2_paused")
                            continue
                    elif rec.get("held", 0) > 0:
                        self.gone_reads[mint] = 0
                    self._retry_unpinned()
                    continue
                # No pin. A Mayhem2 trade still open (restart, or a sell that did not land) is pinned
                # again first, so its SELL / pause is still followed.
                self._retry_unpinned()
                held = self._open_pos()
                if held is not None and held.mint not in self.used:
                    self._set_pin({"m": held.mint, "t": getattr(held, "symbol", ""), "n": getattr(held, "name", "")},
                                  "the bot holds it")
                    continue
                for row in self.candidates():
                    mint = row["m"]
                    if mint in self.used:
                        continue
                    rec = self.pos.get(mint) or {}
                    if not rec.get("at"):
                        continue          # its Mayhem_Mode read comes in a moment
                    if rec.get("held", 0) <= 0:
                        continue
                    self._set_pin(row, f"newest active coin, {row['_age']:.0f}s old, Mayhem_Mode holding")
                    break
                else:
                    if time.time() - last_wait_note > 60 and self.board_at:
                        last_wait_note = time.time()
                        self.note("info", "No active Mayhem coin under 3 minutes that Mayhem_Mode holds yet. Watching.")
            except Exception:
                log.debug("mayhem2 brain failed", exc_info=True)

    def _retry_unpinned(self) -> None:
        """A coin left (paused / closed) whose sell did not land: sell again every 3s until DUST."""
        if not self.used or self.manager is None:
            return
        for p in list(self.manager.positions.values()):
            if p.status != "OPEN" or p.mint not in self.used:
                continue
            if "mayhem2" not in (str(getattr(p, "source", "") or ""), str(getattr(p, "strategy", "") or "")):
                continue
            if self._may_kick(p, 3.0):
                why = str(self.used[p.mint].get("why") or "")
                if self.manager.kick_now(p, "mayhem2_paused" if "paus" in why or "no longer" in why else "mayhem2_sell"):
                    self.kicked[p.mint] = time.time()

    # ---------- wiring ----------
    def start(self, manager, trader, fill_px=None, apply_since=None) -> None:
        if self.started:
            return
        self.started = True
        self.manager = manager
        self.trader = trader
        self.fill_px = fill_px
        self.apply_since = apply_since
        self.loop = asyncio.get_running_loop()
        try:
            from src.data.caller_stream import get_stream
            get_stream().add_raw_listener(self.on_stream)
        except Exception:
            log.debug("mayhem2 stream hook failed", exc_info=True)
        for coro in (self._board_loop(), self._pos_loop(), self._trades_loop(), self._state_loop(), self._brain_loop()):
            asyncio.ensure_future(coro)
        log.info("MAYHEM2 engine started (board %.1fs, trades %.2fs, state %.1fs)", BOARD_EVERY, TRADES_EVERY, STATE_EVERY)

    def stream_mints(self) -> List[str]:
        pin = self.pin
        return [pin["mint"]] if pin else []

    # ---------- page ----------
    def snapshot(self) -> Dict[str, Any]:
        self.view_at = time.time()
        now = time.time()
        try:
            from src.config.runtime import get_config, mayhem2_on
            cfg = get_config()
            on = mayhem2_on()
            trade_on = bool(cfg.trade_enabled)
            max_pos = float(getattr(cfg, "max_position_sol", 0) or 0)
        except Exception:
            on, trade_on, max_pos = False, False, 0.0
        with self.lock:
            board = list(self.board)
            board_at = self.board_at
            err = self.board_err
            states = dict(self.state)
            posd = dict(self.pos)
            notes = [dict(n) for n in list(self.notes)[:30]]
            signals = [dict(s) for s in list(self.signals)[:20]]
        shift = max(0.0, now - board_at) if board_at else 0.0
        pin = dict(self.pin) if self.pin else None
        rows = []
        for r in board:
            mint = str(r.get("m") or "")
            if not mint or str(r.get("pg") or "") != "pump":
                continue
            age = float(r.get("age") or 0) + shift
            st = states.get(mint) or {}
            state = str(st.get("state") or r.get("ms") or "")
            is_pin = bool(pin and pin["mint"] == mint)
            if age > YOUNG_S and not is_pin:
                continue
            p = posd.get(mint) or {}
            rows.append({
                "mint": mint, "symbol": r.get("t") or "", "name": r.get("n") or "", "image": r.get("i") or "",
                "age": round(age, 1), "state": state, "mode": r.get("mm") or "",
                "mcUsd": _f(r.get("mc")), "tx5": r.get("tx5"), "volUsd": _f(r.get("vUsd")),
                "held": p.get("held"), "pnlUsd": p.get("pnlUsd"), "pnlPercentage": p.get("pnlPercentage"),
                "costBasisUsd": p.get("costBasisUsd"), "posAge": round(now - float(p["at"]), 1) if p.get("at") else None,
                "pinned": is_pin, "used": (self.used.get(mint) or {}).get("why", ""),
            })
        rows.sort(key=lambda r: 0 if r["pinned"] else 1)     # the pinned coin always on top
        if pin and not any(r["pinned"] for r in rows):
            p = posd.get(pin["mint"]) or {}
            st = states.get(pin["mint"]) or {}
            rows.insert(0, {"mint": pin["mint"], "symbol": pin.get("symbol"), "name": pin.get("name"),
                            "image": pin.get("image"), "age": None, "state": st.get("state") or "", "mode": "",
                            "held": p.get("held"), "pnlUsd": p.get("pnlUsd"), "pnlPercentage": p.get("pnlPercentage"),
                            "costBasisUsd": p.get("costBasisUsd"), "pinned": True, "used": ""})
        bot = None
        if pin:
            pos = self._open_pos(pin["mint"])
            if pos is not None:
                bot = {
                    "sol": float(getattr(pos, "entry_sol", 0) or 0),
                    "pct": getattr(pos, "since_buy_pct", None),
                    "entryUsd": float(getattr(pos, "entry_price_usd", 0) or 0),
                    "liveUsd": float(getattr(pos, "_live_px", 0) or 0),
                    "since": round(now - float(getattr(pos, "entry_time", 0) or now), 1),
                    "selling": bool(getattr(pos, "_sell_busy", False)),
                }
            st = states.get(pin["mint"]) or {}
            pin.update({
                "pinnedFor": round(now - float(pin["ts"]), 1),
                "state": st.get("state") or "", "stateAge": round(now - float(st["at"]), 1) if st.get("at") else None,
                "want": self.want, "busy": self.busy, "bot": bot,
                "exiting": self.exiting.get(pin["mint"], ""),
                "waitProfit": round(WAIT_PROFIT_S - (now - self.wait_profit[pin["mint"]]), 1) if pin["mint"] in self.wait_profit else None,
                "lastPx": self.last_px.get(pin["mint"]),
            })
        used = sorted(
            ({"mint": m, **v} for m, v in self.used.items()), key=lambda x: -x["ts"],
        )[:12]
        return {
            "ok": True, "on": on, "trading": on and trade_on, "tradeOn": trade_on, "maxPos": max_pos,
            "boardAge": round(now - board_at, 1) if board_at else None, "error": err,
            "pin": pin, "rows": rows, "signals": signals, "notes": notes, "used": used,
            "youngS": YOUNG_S,
            "rules": {"profitMin": PROFIT_MIN_PCT, "waitS": WAIT_PROFIT_S, "tp": TAKE_PROFIT_PCT,
                      "trailArm": TRAIL_ARM_PCT, "trailKeep": TRAIL_KEEP, "stop": STOP_PCT, "maxHoldS": MAX_HOLD_S},
        }


_engine: Optional[Mayhem2] = None


def get_engine() -> Mayhem2:
    global _engine
    if _engine is None:
        _engine = Mayhem2()
    return _engine
