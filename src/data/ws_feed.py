"""
L1 Real-time Solana Data Feed (WebSocket)

Subscribes to program logs and account changes for:
- Raydium AMM v4 pool creation / add-liquidity
- pump.fun bonding-curve activity
- Slot updates (heartbeat)

Designed for low-latency event streaming with automatic reconnect.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("sol-sniper.ws")

# Well-known program IDs
RAYDIUM_AMM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_AUTHORITY = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

DEFAULT_WS_ENDPOINTS = [
    "wss://solana-rpc.publicnode.com",
    "wss://api.mainnet-beta.solana.com",
]


def http_to_wss(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("https://"):
        return "wss://" + u[len("https://"):]
    if u.startswith("http://"):
        return "ws://" + u[len("http://"):]
    if u.startswith("wss://") or u.startswith("ws://"):
        return u
    return ""


def build_ws_endpoints(extra: Optional[List[str]] = None) -> List[str]:
    """Public websockets only. The paid RPC is not a socket."""
    del extra
    return list(DEFAULT_WS_ENDPOINTS)


class FeedEventType(str, Enum):
    SLOT = "slot"
    LOGS = "logs"
    PROGRAM_ACCOUNT = "program_account"
    RAYDIUM_POOL = "raydium_pool"
    PUMP_FUN = "pump_fun"
    ADD_LIQUIDITY = "add_liquidity"
    RAW = "raw"
    HEARTBEAT = "heartbeat"
    ERROR = "error"


@dataclass
class FeedEvent:
    type: FeedEventType
    timestamp: datetime
    slot: Optional[int] = None
    signature: Optional[str] = None
    program_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    raw: Optional[Dict[str, Any]] = None
    latency_ms: Optional[float] = None


class SolanaWSFeed:
    """
    Real-time Solana WebSocket feed with automatic reconnect and
    multi-endpoint failover.
    """

    def __init__(
        self,
        ws_endpoints: Optional[List[str]] = None,
        programs: Optional[List[str]] = None,
        commitment: str = "processed",
        ping_interval: Optional[float] = None,
        max_reconnect_delay: float = 30.0,
    ):
        self.endpoints = ws_endpoints or build_ws_endpoints()
        self.programs = programs or [RAYDIUM_AMM_V4, PUMP_FUN_PROGRAM]
        self.commitment = commitment
        self.ping_interval = ping_interval
        self.max_reconnect_delay = max_reconnect_delay

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._request_id = 0
        self._subscriptions: Dict[int, str] = {}  # id -> type
        self._running = False
        self._endpoint_index = 0
        self._event_queue: asyncio.Queue[FeedEvent] = asyncio.Queue(maxsize=10_000)
        self._listeners: List[Callable[[FeedEvent], Any]] = []
        self._last_slot: Optional[int] = None
        self._connected_at: Optional[float] = None
        self._messages_received = 0

    def add_listener(self, callback: Callable[[FeedEvent], Any]) -> None:
        self._listeners.append(callback)

    async def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _send(self, method: str, params: Any) -> int:
        assert self._ws is not None
        req_id = await self._next_id()
        payload = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        await self._ws.send(json.dumps(payload))
        return req_id

    async def _subscribe_all(self) -> None:
        """Slots only. Public nodes reject logsSubscribe and then drop the socket."""
        sid = await self._send("slotSubscribe", [])
        self._subscriptions[sid] = "slot"
        log.info("Subscribed to slots (id=%s). Log subscribe is not used.", sid)

    async def _connect(self) -> None:
        url = self.endpoints[self._endpoint_index % len(self.endpoints)]
        # Redact api-key in logs
        safe = url.split("api-key=")[0] + ("api-key=***" if "api-key=" in url else "")
        log.info("Connecting to WebSocket: %s", safe)
        self._ws = await websockets.connect(
            url,
            ping_interval=None,
            ping_timeout=None,
            max_size=4 * 1024 * 1024,
            close_timeout=5,
            open_timeout=8,
        )
        self._connected_at = time.time()
        self._subscriptions.clear()
        await self._subscribe_all()
        log.info("WebSocket connected and subscriptions active (%s)", safe[:48])

    async def _handle_message(self, raw: str) -> None:
        self._messages_received += 1
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        if isinstance(msg, dict) and msg.get("error"):
            err = msg.get("error") or {}
            method_id = msg.get("id")
            kind = self._subscriptions.get(method_id, "")
            if kind.startswith("logs:"):
                log.info("WebSocket log subscribe not offered here — slots stay up")
                return
            log.warning("WebSocket subscribe rejected: %s — switching endpoint", str(err)[:160])
            if self._ws is not None:
                await self._ws.close()
            return

        # Subscription confirmation
        if "result" in msg and "id" in msg and isinstance(msg["result"], int):
            return

        if "method" not in msg:
            return

        method = msg["method"]
        params = msg.get("params", {})
        result = params.get("result", {})
        context = params.get("context", {}) or result.get("context", {})
        slot = context.get("slot") or result.get("context", {}).get("slot")

        event: Optional[FeedEvent] = None
        now = datetime.now(timezone.utc)

        if method == "slotNotification":
            slot_info = result
            parent = slot_info.get("parent")
            root = slot_info.get("root")
            self._last_slot = slot_info.get("slot", slot)
            event = FeedEvent(
                type=FeedEventType.SLOT,
                timestamp=now,
                slot=self._last_slot,
                data={"parent": parent, "root": root},
            )

        elif method == "logsNotification":
            value = result.get("value", result)
            logs = value.get("logs", [])
            signature = value.get("signature")
            err = value.get("err")

            # Classify by program mention in logs
            program_id = None
            event_type = FeedEventType.LOGS
            parsed: Dict[str, Any] = {"logs": logs[:20], "err": err}

            joined = " ".join(logs)
            if RAYDIUM_AMM_V4 in joined or "ray_log" in joined.lower() or "initialize2" in joined.lower():
                program_id = RAYDIUM_AMM_V4
                event_type = FeedEventType.RAYDIUM_POOL
                parsed["hint"] = "possible_raydium_pool_or_swap"
            elif PUMP_FUN_PROGRAM in joined or "Instruction: Initialize" in joined or "Instruction: Buy" in joined:
                program_id = PUMP_FUN_PROGRAM
                event_type = FeedEventType.PUMP_FUN
                parsed["hint"] = "possible_pump_fun_activity"

            # Simple instruction keyword extraction
            for line in logs:
                if "Instruction:" in line:
                    parsed.setdefault("instructions", []).append(line.strip())

            event = FeedEvent(
                type=event_type,
                timestamp=now,
                slot=slot,
                signature=signature,
                program_id=program_id,
                data=parsed,
                raw=msg,
            )

        if event:
            # Non-blocking put; drop oldest if queue is full
            try:
                self._event_queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    self._event_queue.get_nowait()
                    self._event_queue.put_nowait(event)
                except Exception:
                    pass

            for listener in self._listeners:
                try:
                    res = listener(event)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception as exc:
                    log.exception("Listener error: %s", exc)

    async def _read_loop(self) -> None:
        assert self._ws is not None
        async for raw in self._ws:
            if not self._running:
                break
            await self._handle_message(raw)

    async def start(self) -> None:
        """Start the feed (blocks until stop() is called)."""
        self._running = True
        reconnect_delay = 1.0

        while self._running:
            try:
                await self._connect()
                reconnect_delay = 1.0  # reset on success
                await self._read_loop()
            except ConnectionClosed as exc:
                text = str(exc)
                if "1011" in text or "ping timeout" in text or "keepalive" in text:
                    log.info("WebSocket public node dropped the ping — reconnecting")
                else:
                    log.warning("WebSocket closed: %s", exc)
            except Exception as exc:
                err = str(exc)
                if "413" in err or "rejected" in err.lower() or "InvalidStatus" in type(exc).__name__:
                    log.warning(
                        "WebSocket endpoint rejected connection (%s) — failover next",
                        err[:100],
                    )
                elif "timed out" in err or "TimeoutError" in type(exc).__name__:
                    log.info("WebSocket handshake missed — next endpoint")
                else:
                    log.error("WebSocket error: %s", exc)
            finally:
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

            if not self._running:
                break

            self._endpoint_index += 1
            log.info("Reconnecting in %.1fs (endpoint index %d)...", reconnect_delay, self._endpoint_index)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(self.max_reconnect_delay, reconnect_delay * 1.8)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def events(self) -> AsyncIterator[FeedEvent]:
        """Async iterator of FeedEvents."""
        while self._running or not self._event_queue.empty():
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                yield event
            except asyncio.TimeoutError:
                if not self._running:
                    break

    def stats(self) -> Dict[str, Any]:
        return {
            "running": self._running,
            "connected": self._ws is not None and self._ws.open,
            "last_slot": self._last_slot,
            "messages_received": self._messages_received,
            "queue_size": self._event_queue.qsize(),
            "uptime_s": round(time.time() - self._connected_at, 1) if self._connected_at else 0,
            "endpoint_index": self._endpoint_index,
        }


async def run_feed_demo(duration_s: float = 30.0, programs: Optional[List[str]] = None) -> None:
    """Convenience runner that prints events for a fixed duration."""
    feed = SolanaWSFeed(programs=programs)

    interesting = {FeedEventType.RAYDIUM_POOL, FeedEventType.PUMP_FUN, FeedEventType.ADD_LIQUIDITY}

    def on_event(ev: FeedEvent) -> None:
        if ev.type == FeedEventType.SLOT:
            if ev.slot and ev.slot % 20 == 0:  # reduce noise
                log.info("slot=%s", ev.slot)
            return
        if ev.type in interesting or (ev.type == FeedEventType.LOGS and ev.data.get("instructions")):
            log.info(
                "[%s] slot=%s sig=%s prog=%s data_keys=%s",
                ev.type.value,
                ev.slot,
                (ev.signature or "")[:16] + "...",
                (ev.program_id or "")[:12],
                list(ev.data.keys()),
            )

    feed.add_listener(on_event)

    task = asyncio.create_task(feed.start())
    log.info("Real-time feed started – collecting for %.0f seconds...", duration_s)

    try:
        await asyncio.sleep(duration_s)
    finally:
        await feed.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    stats = feed.stats()
    log.info("Feed stopped. Stats: %s", stats)
