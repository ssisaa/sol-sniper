"""Caller trades and coin prices the moment they print.

Transport, fastest first:
  1. PumpPortal trade socket (needs PUMPPORTAL_API_KEY funded with 0.02 SOL or more).
  2. Solana RPC logsSubscribe on each caller wallet and coin, decoding pump.fun's
     TradeEvent straight from the logs (no getTransaction round trip).

The pump.fun portfolio API shows a caller's new open seconds after the buy lands,
and shows the sale seconds after that. This socket pushes the same trade within
a few hundred milliseconds of the transaction. OpenHolds, the auto-buy, the
caller-sold exit, and the copy since-buy percent all read from here first and
fall back to the polled books when the socket has nothing.

One socket, its own thread and event loop, so a busy main loop never delays a tick.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

log = logging.getLogger("sol-sniper.caller-stream")

WS_URL = "wss://pumpportal.fun/api/data"
RPC_WSS = "wss://api.mainnet-beta.solana.com"
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
_TRADE_EVENT = bytes.fromhex("bddb7fd34ee661ee")
# A caller balance under this many tokens is a full exit (dust left by rounding).
_DUST_TOKENS = 1.0
_MAX_MINTS = 220   # PumpPortal socket (free): OpenHolds coins plus the CallOuts coins
_MAX_WALLETS = 60
_SUBS_PER_TICK = 4   # logsSubscribe requests per 0.5s on the RPC fallback
_MIN_VSOL = 30.0     # a standard pump curve never holds less virtual SOL
_MIN_OPEN_SOL = 0.01  # a caller buy smaller than this does not count as a new open
# PumpPortal is ~0.1s ahead of RPC logs but skips some trades (measured ~40%), so both run
# together: PumpPortal for speed, public RPC logs on the coins that matter to fill the gaps.
try:   # RPC logs: held + caller-open coins first, then the most active listed ones
    _RPC_MAX_MINTS = max(5, int(__import__("os").environ.get("SOL_SNIPER_STREAM_RPC_MINTS") or 40))
except ValueError:
    _RPC_MAX_MINTS = 40


def _b58(raw: bytes) -> str:
    try:
        import base58
        return base58.b58encode(raw).decode()
    except Exception:
        from solders.pubkey import Pubkey
        return str(Pubkey.from_bytes(raw))


def decode_trade_logs(logs: List[str]) -> List[Dict[str, Any]]:
    """pump.fun TradeEvent from 'Program data:' lines, shaped like a PumpPortal trade."""
    import base64
    import struct

    out: List[Dict[str, Any]] = []
    for line in logs or []:
        if not isinstance(line, str) or not line.startswith("Program data: "):
            continue
        try:
            raw = base64.b64decode(line[len("Program data: "):])
        except Exception:
            continue
        if len(raw) < 113 or raw[:8] != _TRADE_EVENT:
            continue
        sol, tok = struct.unpack_from("<QQ", raw, 40)
        v_sol, v_tok = struct.unpack_from("<QQ", raw, 97)
        out.append({
            "mint": _b58(raw[8:40]),
            "solAmount": sol / 1e9,
            "tokenAmount": tok / 1e6,
            "txType": "buy" if raw[56] else "sell",
            "traderPublicKey": _b58(raw[57:89]),
            "vSolInBondingCurve": v_sol / 1e9,
            "vTokensInBondingCurve": v_tok / 1e6,
            "newTokenBalance": None,
        })
    return out


def _rpc_http() -> str:
    import os
    for url in (os.environ.get("SOLANA_RPC_URLS") or os.environ.get("SOLANA_RPC_URL") or "").split(","):
        url = url.strip()
        if url.startswith("http"):
            return url
    return "https://api.mainnet-beta.solana.com"


def _token_balance(wallet: str, mint: str, url: str = "", timeout: float = 1.5) -> Optional[float]:
    """Caller's UI balance of this coin right now. None when the RPC did not answer."""
    import urllib.request

    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "getTokenAccountsByOwner",
        "params": [wallet, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "processed"}],
    }).encode()
    req = urllib.request.Request(url or _rpc_http(), data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode() or "{}")
    except Exception:
        return None
    value = ((data or {}).get("result") or {}).get("value")
    if not isinstance(value, list):
        return None
    total = 0.0
    for acct in value:
        try:
            total += float(acct["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"] or 0)
        except (KeyError, TypeError, ValueError):
            continue
    return total


def _f(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


class CallerStream:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._wallets: Dict[str, str] = {}          # wallet -> user name (wanted)
        self._mints: Set[str] = set()               # coin trades wanted
        self._pos_mints: Set[str] = set()           # our own open copies, always kept
        self._pos_wallets: Dict[str, str] = {}      # callers behind our open copies, always kept
        self._px: Dict[str, Tuple[float, float]] = {}   # mint -> (SOL per token, ts)
        self._scale: Dict[str, List[float]] = {}        # mint -> recent trade/curve price ratios
        # (wallet, mint) -> caller position seen on the socket
        self._opens: Dict[Tuple[str, str], Dict[str, Any]] = {}
        # (wallet, mint) -> ts of the full exit
        self._exits: Dict[Tuple[str, str], float] = {}
        self._listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._started = False
        self._connected = False
        self._last_msg = 0.0
        self._trades = 0
        self._caller_trades = 0
        self._mode = ""
        self._seen_sigs: Dict[str, float] = {}
        self._portal_refused = False
        self._portal_retry_at = 0.0
        self._mint_order: List[str] = []             # self._mints, most active first
        self._up: Dict[str, bool] = {}               # "pumpportal" / "rpc-logs" -> connected
        self._seen_trades: Dict[Tuple[str, str, str, int], Tuple[float, str, float]] = {}
        # PumpPortal's solAmount assumes a standard curve; on custom curves it is wrong (seen 5-44%).
        # Per coin: did PumpPortal's SOL match the on-chain TradeEvent? Its price is used only then.
        self._pp_trust: Dict[str, List[bool]] = {}
        self._paid_fails = 0
        self._targets: Optional[Callable[[], Tuple[Dict[str, str], Iterable[str]]]] = None

    # ---------- wiring ----------
    def set_target_provider(self, fn: Callable[[], Tuple[Dict[str, str], Iterable[str]]]) -> None:
        self._targets = fn

    def set_positions(self, mints: Iterable[str], wallets: Optional[Dict[str, str]] = None) -> None:
        """Our open copy trades. Their coins and callers stay subscribed whatever the page shows."""
        with self._lock:
            self._pos_mints = {str(m) for m in mints if m}
            self._pos_wallets = {str(w): str(u or "") for w, u in (wallets or {}).items() if w and len(str(w)) >= 32}

    def watch_mint(self, mint: str) -> None:
        if mint:
            with self._lock:
                self._mints.add(str(mint))

    def add_listener(self, fn: Callable[[Dict[str, Any]], None]) -> None:
        self._listeners.append(fn)

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        threading.Thread(target=self._run, name="caller-stream", daemon=True).start()

    # ---------- reads ----------
    def curve_scale(self, mint: str) -> Optional[float]:
        """Real trade price over curve price for this coin, from recent trades. 1.0 on a standard
        curve. None until 3 trades with amounts were seen."""
        samples = sorted(self._scale.get(str(mint or "")) or [])
        if len(samples) < 3:
            return None
        mid = samples[len(samples) // 2]
        return 1.0 if abs(mid - 1.0) <= 0.08 else mid

    def price_sol(self, mint: str, max_age: float = 5.0) -> Optional[Tuple[float, float]]:
        hit = self._px.get(str(mint or ""))
        if not hit:
            return None
        price, ts = hit
        if price <= 0 or time.time() - ts > max_age:
            return None
        return price, ts

    def _check_topup(self, key, tokens: float) -> None:
        wallet, mint = key
        try:
            from src.config.runtime import MAYHEM_WALLET
        except Exception:
            MAYHEM_WALLET = ""
        if wallet == MAYHEM_WALLET:
            # Mayhem_Mode gets ~all of a Mayhem coin's supply when it is created, so its balance is
            # always far above a buy. The coin's age decides: made in the last 5 minutes is a new
            # open, older is a top-up of a coin it already held (SOL/PEPE was 3 hours old).
            created = None
            try:
                from src.data.fast_get import pump_get
                body = pump_get(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}", timeout=1.4, cache_s=60)
                created = float((body or {}).get("created_timestamp") or 0) / 1000.0 or None
            except Exception:
                created = None
            with self._lock:
                row = self._opens.get(key)
                if row is None:
                    return
                row.pop("checking", None)
                if created is not None and time.time() - created > 300:
                    row["topup"] = True
                    log.info("CALLER STREAM %s %s — coin made %.0f min ago: a top-up, not a new open",
                             row.get("user") or wallet[:6], mint[:8], (time.time() - created) / 60)
            return
        # Paid RPC first (ordered failover; one ~1KB call per new caller buy), then a public node.
        urls: List[str] = []
        try:
            from src.execution import rpc_pool
            urls = list(rpc_pool.healthy_paid())[:2]
        except Exception:
            urls = []
        urls.append("https://solana-rpc.publicnode.com")
        left = None
        for url in urls:
            left = _token_balance(wallet, mint, url, timeout=0.5)
            if left is not None:
                break
        with self._lock:
            row = self._opens.get(key)
            if row is None:
                return
            row.pop("checking", None)
            if left is not None and left > tokens * 1.05:
                row["topup"] = True
                log.info("CALLER STREAM %s %s — top-up of a coin it already held (balance %.0f, bought %.0f): not a new open",
                         row.get("user") or wallet[:6], mint[:8], left, tokens)

    def opens_for(self, wallet: str, since: float = 0.0) -> List[Dict[str, Any]]:
        wallet = str(wallet or "")
        out: List[Dict[str, Any]] = []
        with self._lock:
            for (w, mint), row in self._opens.items():
                if w != wallet or float(row.get("first_ts") or 0) < since:
                    continue
                out.append(dict(row, mint=mint, wallet=w))
        return out

    def saw_trade(self, wallet: str, mint: str) -> bool:
        """True when the socket saw this wallet buy or fully sell this coin (its own signed trade)."""
        key = (str(wallet or ""), str(mint or ""))
        with self._lock:
            return key in self._opens or key in self._exits

    def exited(self, wallet: str, mint: str, after: float = 0.0) -> bool:
        """True when the socket saw this caller sell this coin down to nothing after `after`."""
        ts = self._exits.get((str(wallet or ""), str(mint or "")))
        return bool(ts) and ts >= after

    def exited_any(self, mint: str, after: float = 0.0) -> str:
        """The caller wallet that fully left this coin after `after`, or ''."""
        mint = str(mint or "")
        for (w, m), ts in list(self._exits.items()):
            if m == mint and ts >= after:
                return w
        return ""

    def holding(self, wallet: str, mint: str) -> bool:
        return (str(wallet or ""), str(mint or "")) in self._opens

    def stats(self) -> Dict[str, Any]:
        return {
            "connected": self._connected,
            "mode": self._mode,
            "last_msg_age": round(time.time() - self._last_msg, 2) if self._last_msg else None,
            "wallets": len(self._wallets),
            "mints": len(self._mints | self._pos_mints),
            "trades": self._trades,
            "caller_trades": self._caller_trades,
        }

    @property
    def connected(self) -> bool:
        return self._connected and time.time() - self._last_msg < 60

    # ---------- socket ----------
    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._forever())
        except Exception:
            log.exception("caller stream stopped")

    def _refresh_targets(self) -> None:
        fn = self._targets
        if fn is None:
            return
        try:
            wallets, mints = fn()
        except Exception:
            log.debug("caller stream targets failed", exc_info=True)
            return
        with self._lock:
            merged = {str(w): str(u or "") for w, u in list((wallets or {}).items())[:_MAX_WALLETS] if w}
            for w, u in self._pos_wallets.items():
                merged.setdefault(w, u)
            self._wallets = merged
            # Coins a caller just bought on the socket first, then the ranked OpenHolds coins.
            wanted = [m for (_w, m) in self._opens.keys()]
            wanted += [str(m) for m in (mints or []) if m and str(m) not in wanted]
            keep = set(wanted[:_MAX_MINTS])
            self._mints = keep
            self._mint_order = wanted[:_MAX_MINTS]

    async def _forever(self) -> None:
        await asyncio.gather(self._loop("pumpportal"), self._loop("rpc-logs"))

    def _set_up(self, name: str, on: bool) -> None:
        self._up[name] = on
        live = [k for k, v in self._up.items() if v]
        self._connected = bool(live)
        self._mode = "+".join(sorted(live, reverse=True))

    async def _loop(self, name: str) -> None:
        import os

        backoff = 1.0
        while True:
            self._refresh_targets()
            if not self._wallets and not (self._mints | self._pos_mints):
                await asyncio.sleep(1.0)
                continue
            key = (os.environ.get("PUMPPORTAL_API_KEY") or "").strip()
            if name == "pumpportal":
                if not key:
                    return
                if self._portal_refused:
                    await asyncio.sleep(max(1.0, self._portal_retry_at - time.time()))
                    self._portal_refused = False
                    continue
            try:
                if name == "pumpportal":
                    await self._run_portal(key)
                else:
                    await self._run_rpc()
                backoff = 1.0
            except Exception as exc:
                text = str(exc)
                if "429" in text or "1013" in text or "Rate limit" in text:
                    # Public node rate limit: back off hard, or it keeps refusing.
                    backoff = max(backoff, 10.0)
                log.warning("CALLER STREAM %s dropped: %s — reconnect in %.0fs", name, text[:160], backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)
            finally:
                self._set_up(name, False)

    async def _sync_loop_targets(self):
        self._refresh_targets()
        with self._lock:
            return set(self._wallets), set(self._mints) | set(self._pos_mints)

    async def _run_portal(self, key: str) -> None:
        import websockets

        async with websockets.connect(
            f"{WS_URL}?api-key={key}", open_timeout=10, ping_interval=15, ping_timeout=10, max_size=2**20,
        ) as ws:
            self._set_up("pumpportal", True)
            sub_w: Set[str] = set()
            sub_m: Set[str] = set()
            log.info("CALLER STREAM connected — PumpPortal")
            last_sync = 0.0
            while True:
                now = time.time()
                if now - last_sync >= 0.5:
                    last_sync = now
                    want_w, want_m = await self._sync_loop_targets()
                    add_w, drop_w = want_w - sub_w, sub_w - want_w
                    add_m, drop_m = want_m - sub_m, sub_m - want_m
                    if add_w:
                        await ws.send(json.dumps({"method": "subscribeAccountTrade", "keys": sorted(add_w)}))
                    if drop_w:
                        await ws.send(json.dumps({"method": "unsubscribeAccountTrade", "keys": sorted(drop_w)}))
                    if add_m:
                        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": sorted(add_m)}))
                    if drop_m:
                        await ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": sorted(drop_m)}))
                    sub_w, sub_m = want_w, want_m
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                self._last_msg = time.time()
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(msg, dict):
                    continue
                if msg.get("mint"):
                    msg["_src"] = "pp"
                    self._on_trade(msg)
                elif "only available" in str(msg.get("message") or msg.get("errors") or ""):
                    # Key missing funds. Use the RPC socket instead of sitting on a silent feed.
                    log.warning("CALLER STREAM PumpPortal refused the trade feed (%s) — RPC logs only for 10 min", msg)
                    self._portal_refused = True
                    self._portal_retry_at = time.time() + 600.0
                    return

    async def _run_rpc(self) -> None:
        """One logsSubscribe per caller wallet and per coin. TradeEvent comes in the log lines."""
        import os

        url = (os.environ.get("SOL_SNIPER_STREAM_WSS") or "").strip()
        if not url:
            # Public node by default: logsSubscribe pushes every transaction's full logs, which
            # costs gigabytes an hour on a paid plan billed by bandwidth. Paid only on opt-in.
            url = RPC_WSS
            primary = (os.environ.get("SOLANA_RPC_URL") or "").strip()
            opt_in = (os.environ.get("SOL_SNIPER_STREAM_PAID") or "").strip().lower() in ("1", "true", "yes")
            paid = primary.startswith("http") and "mainnet-beta.solana.com" not in primary
            if opt_in and paid and self._paid_fails < 3:
                url = "ws" + primary[4:]
            elif opt_in:
                self._paid_fails = 0
        try:
            await self._run_rpc_url(url)
            self._paid_fails = 0
        except Exception:
            if url != RPC_WSS:
                self._paid_fails += 1
            raise

    async def _run_rpc_url(self, url: str) -> None:
        import websockets

        async with websockets.connect(url, open_timeout=10, ping_interval=20, ping_timeout=15, max_size=2**22) as ws:
            self._set_up("rpc-logs", True)
            log.info("CALLER STREAM connected — RPC logs %s", url.split("?")[0])
            by_key: Dict[str, int] = {}        # wallet or mint -> subscription id
            pending: Dict[int, str] = {}       # request id -> key
            unsub_ids: Set[int] = set()
            refused: Dict[str, float] = {}     # key -> time it may be asked again
            next_id = 10
            last_sync = 0.0
            while True:
                now = time.time()
                if now - last_sync >= 0.5:
                    last_sync = now
                    want_w, want_m = await self._sync_loop_targets()
                    with self._lock:
                        held_m = set(self._pos_mints)
                        order = list(self._mint_order)
                    # Few subscriptions: every one streams each trade's full logs.
                    keep = list(held_m) + [m for m in order if m not in held_m]
                    want_m = set(keep[:_RPC_MAX_MINTS]) & (want_m | held_m)
                    want = want_w | want_m
                    have = set(by_key) | set(pending.values())
                    with self._lock:
                        held = set(self._pos_mints)
                    # Callers first, then coins we hold, then the rest. Few per tick so
                    # the public node does not rate-limit the socket.
                    todo = [k for k in want - have if refused.get(k, 0.0) <= now]
                    todo.sort(key=lambda k: (k not in want_w, k not in held, k))
                    for key in todo[:_SUBS_PER_TICK]:
                        next_id += 1
                        pending[next_id] = key
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0", "id": next_id, "method": "logsSubscribe",
                            "params": [{"mentions": [key]}, {"commitment": "processed"}],
                        }))
                    for key in sorted(set(by_key) - want):
                        sid = by_key.pop(key)
                        next_id += 1
                        unsub_ids.add(next_id)
                        await ws.send(json.dumps({"jsonrpc": "2.0", "id": next_id, "method": "logsUnsubscribe", "params": [sid]}))
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                self._last_msg = time.time()
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(msg, dict):
                    continue
                rid = msg.get("id")
                if rid is not None:
                    key = pending.pop(rid, None)
                    if key is not None and isinstance(msg.get("result"), int):
                        by_key[key] = msg["result"]
                    elif key is not None:
                        refused[key] = now + 30.0
                        log.debug("logsSubscribe refused for %s: %s", key[:8], msg.get("error"))
                    unsub_ids.discard(rid)
                    continue
                value = ((msg.get("params") or {}).get("result") or {}).get("value") or {}
                if value.get("err") is not None:
                    continue
                sig = str(value.get("signature") or "")
                if sig:
                    if sig in self._seen_sigs:
                        continue
                    self._seen_sigs[sig] = now
                    if len(self._seen_sigs) > 5000:
                        cut = now - 120
                        self._seen_sigs = {k: v for k, v in self._seen_sigs.items() if v >= cut}
                for trade in decode_trade_logs(value.get("logs") or []):
                    trade["signature"] = sig
                    trade["_src"] = "rl"
                    self._on_trade(trade)

    def _on_trade(self, msg: Dict[str, Any]) -> None:
        now = time.time()
        mint = str(msg.get("mint") or "")
        trader = str(msg.get("traderPublicKey") or "")
        kind = str(msg.get("txType") or "").lower()
        sol = _f(msg.get("solAmount")) or 0.0
        sig = str(msg.get("signature") or "")
        src = str(msg.get("_src") or "")
        tokens = _f(msg.get("tokenAmount")) or 0.0
        if sig:
            # Same trade from PumpPortal and from RPC logs: count it once (first one wins).
            # Keyed on tokens: both sources agree on them; PumpPortal's SOL can differ.
            tkey = (sig, mint, kind, int(round(tokens)))
            with self._lock:
                prev = self._seen_trades.get(tkey)
                if prev is None:
                    self._seen_trades[tkey] = (now, src, sol)
                    if len(self._seen_trades) > 20000:
                        cut = now - 120
                        self._seen_trades = {k: v for k, v in self._seen_trades.items() if v[0] >= cut}
            if prev is not None:
                if src == "rl" and prev[1] == "pp":
                    self._pp_checked(mint, trader, kind, prev[2], sol, tokens, now)
                return
        v_sol = _f(msg.get("vSolInBondingCurve"))
        v_tok = _f(msg.get("vTokensInBondingCurve"))
        # The real price is what this trade actually swapped at (pump and Jupiter price there).
        # Reserves give the same number only on a standard curve; custom curves differ.
        price = 0.0
        if src == "pp":
            # PumpPortal's reserves are a standard-curve estimate, not the chain: no curve price,
            # no scale samples; its trade price only once it has matched the chain for this coin.
            if sol >= 0.001 and tokens > 0 and self._pp_trusted(mint):
                price = sol / tokens
        else:
            curve = (v_sol / v_tok) if (v_sol and v_tok and v_sol > 0 and v_tok > 0) else 0.0
            if sol >= 0.001 and tokens > 0:
                price = sol / tokens
                if curve > 0:
                    samples = self._scale.setdefault(mint, [])
                    samples.append(price / curve)
                    del samples[:-7]
            elif curve > 0 and v_sol >= _MIN_VSOL and self.curve_scale(mint) in (None, 1.0):
                price = curve
        if price > 0:
            self._px[mint] = (price, now)
        self._trades += 1
        if kind not in ("buy", "sell"):
            return
        user = self._wallets.get(trader)
        if user is None:
            return
        self._caller_trades += 1
        balance = _f(msg.get("newTokenBalance"))
        key = (trader, mint)
        event: Dict[str, Any] = {
            "kind": kind, "mint": mint, "wallet": trader, "user": user,
            "sol": sol, "tokens": tokens, "price_sol": price, "ts": now,
            "signature": str(msg.get("signature") or ""),
        }
        with self._lock:
            row = self._opens.get(key)
            if kind == "buy" and row is None and sol < _MIN_OPEN_SOL:
                # 0-SOL / dust "buys" are bait; they are not a caller entry to copy.
                pass
            elif kind == "buy":
                if row is None:
                    row = {"first_ts": now, "sol": 0.0, "tokens": 0.0, "user": user}
                    self._opens[key] = row
                    if balance is not None and tokens > 0 and balance > tokens * 1.05:
                        # Balance after this buy is well above what it bought: the caller already
                        # held this coin (bought before we watched, or older than pump's newest
                        # page). A top-up, not a new open: never auto-copied as one.
                        row["topup"] = True
                    else:
                        event["new"] = True
                        if balance is None and tokens > 0:
                            # RPC-log trades carry no balance: one free public-RPC read decides it.
                            # The row waits at most 1.5s for it (checking), then counts as new.
                            row["checking"] = now + 1.5
                            threading.Thread(target=self._check_topup, args=(key, tokens),
                                             name="topup-check", daemon=True).start()
                row["sol"] = float(row["sol"]) + sol
                row["tokens"] = float(row["tokens"]) + tokens
                row["last_ts"] = now
                row["balance"] = balance if balance is not None else row["tokens"]
                self._exits.pop(key, None)
                self._mints.add(mint)
            else:
                left: Optional[float] = balance
                if left is None and row is not None:
                    left = max(0.0, float(row.get("balance") or row.get("tokens") or 0) - tokens)
                if row is not None:
                    row["balance"] = left if left is not None else row.get("balance")
                    row["last_ts"] = now
                if balance is None:
                    # RPC log: no balance in the event. One balance read decides it.
                    event["confirm"] = True
                elif left is not None and left <= _DUST_TOKENS:
                    self._opens.pop(key, None)
                    self._exits[key] = now
                    event["full_exit"] = True
            if len(self._exits) > 400:
                for old in sorted(self._exits, key=self._exits.get)[:100]:
                    self._exits.pop(old, None)
        if event.get("confirm"):
            threading.Thread(target=self._confirm_exit, args=(event,), daemon=True).start()
            return
        if kind == "buy":
            log.info(
                "CALLER STREAM %s BUY %s %.3f SOL%s",
                user or trader[:6], mint[:8], sol, " (new)" if event.get("new") else "",
            )
        elif event.get("full_exit"):
            log.warning("CALLER STREAM %s SOLD ALL %s", user or trader[:6], mint[:8])
        for fn in list(self._listeners):
            try:
                fn(event)
            except Exception:
                log.debug("caller stream listener failed", exc_info=True)

    def _pp_trusted(self, mint: str) -> bool:
        marks = self._pp_trust.get(mint) or []
        return len(marks) >= 2 and all(marks[-3:])

    def _pp_checked(self, mint: str, trader: str, kind: str, pp_sol: float, sol: float, tokens: float, now: float) -> None:
        """The on-chain copy of a trade PumpPortal sent first: grade PumpPortal, keep the true price."""
        if sol >= 0.001:
            marks = self._pp_trust.setdefault(mint, [])
            marks.append(abs(pp_sol - sol) <= 0.005 * sol)
            del marks[:-4]
            if len(self._pp_trust) > 2000:
                self._pp_trust = dict(list(self._pp_trust.items())[-1000:])
            if tokens > 0:
                hit = self._px.get(mint)
                if not hit or hit[1] <= now:
                    self._px[mint] = (sol / tokens, now)
        if kind == "buy" and abs(pp_sol - sol) > 1e-9:
            with self._lock:
                row = self._opens.get((trader, mint))
                if row is not None:
                    row["sol"] = max(0.0, float(row.get("sol") or 0) + (sol - pp_sol))

    def _confirm_exit(self, event: Dict[str, Any]) -> None:
        wallet, mint = event["wallet"], event["mint"]
        left = None
        for _ in range(3):
            left = _token_balance(wallet, mint)
            if left is not None:
                break
            time.sleep(0.15)
        if left is None:
            return
        key = (wallet, mint)
        with self._lock:
            row = self._opens.get(key)
            if row is not None:
                row["balance"] = left
            if left > _DUST_TOKENS:
                return
            self._opens.pop(key, None)
            self._exits[key] = time.time()
        event = dict(event, full_exit=True, confirm=False)
        log.warning("CALLER STREAM %s SOLD ALL %s", event.get("user") or wallet[:6], mint[:8])
        for fn in list(self._listeners):
            try:
                fn(event)
            except Exception:
                log.debug("caller stream listener failed", exc_info=True)


_stream: Optional[CallerStream] = None


def get_stream() -> CallerStream:
    global _stream
    if _stream is None:
        _stream = CallerStream()
    return _stream
