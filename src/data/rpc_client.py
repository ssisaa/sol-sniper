"""
L1 Data Layer – Multi-RPC Client with failover (SOL-SNIPER v1.2)

Uses pure JSON-RPC over HTTP via httpx.
Designed for low-latency, health-aware failover across multiple endpoints.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("sol-sniper.rpc")


def _is_rate_limit(text: str) -> bool:
    t = (text or "").lower()
    return "429" in t or "too many request" in t or "rate limit" in t


@dataclass
class RPCEndpoint:
    url: str
    name: str = "rpc"
    weight: int = 1
    last_latency_ms: float = 9999.0
    consecutive_failures: int = 0
    healthy: bool = True
    last_checked: float = 0.0


@dataclass
class RPCResponse:
    result: Any
    endpoint: str
    latency_ms: float
    error: Optional[str] = None


class SolanaRPCClient:
    """
    Health-aware multi-endpoint Solana JSON-RPC client.

    Features:
    - Round-robin + latency-aware selection
    - Automatic failover on errors / high latency
    - Periodic health probes (getHealth + getSlot)
    - Concurrent fan-out support for critical calls
    """

    def __init__(
        self,
        endpoints: List[str],
        timeout_ms: int = 3000,
        max_failures_before_unhealthy: int = 3,
        health_interval_s: float = 15.0,
    ):
        if not endpoints:
            raise ValueError("At least one RPC endpoint is required")

        self.endpoints: List[RPCEndpoint] = [
            RPCEndpoint(url=url.strip(), name=f"rpc-{i}")
            for i, url in enumerate(endpoints)
        ]
        self.timeout = timeout_ms / 1000.0
        self.max_failures = max_failures_before_unhealthy
        self.health_interval = health_interval_s
        self._client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()
        self._rr_index = 0

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        # Initial health check
        await self.health_check_all()
        log.info(
            "RPC client started | endpoints=%d | healthy=%d",
            len(self.endpoints),
            sum(1 for e in self.endpoints if e.healthy),
        )

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _select_endpoint(self) -> RPCEndpoint:
        """Pick the best currently healthy endpoint (lowest recent latency)."""
        healthy = [e for e in self.endpoints if e.healthy]
        if not healthy:
            # All unhealthy – reset and try the least-bad one
            log.warning("All RPC endpoints marked unhealthy – forcing reset")
            for e in self.endpoints:
                e.healthy = True
                e.consecutive_failures = 0
            healthy = self.endpoints

        # Prefer lowest latency
        healthy.sort(key=lambda e: e.last_latency_ms)
        return healthy[0]

    async def _call(
        self,
        method: str,
        params: Optional[List[Any]] = None,
        endpoint: Optional[RPCEndpoint] = None,
    ) -> RPCResponse:
        if self._client is None:
            raise RuntimeError("RPC client not started – call await client.start()")

        ep = endpoint or self._select_endpoint()
        if endpoint is not None:
            return await self._call_one(method, params, ep)
        last = RPCResponse(result=None, endpoint="", latency_ms=0.0, error="no HTTP 200")
        for cand in self.endpoints:
            last = await self._call_one(method, params, cand)
            if last.error is None:
                return last
        return last

    async def _call_one(
        self,
        method: str,
        params: Optional[List[Any]],
        ep: RPCEndpoint,
    ) -> RPCResponse:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        start = time.perf_counter()
        try:
            resp = await self._client.post(ep.url, json=payload)
            latency = (time.perf_counter() - start) * 1000.0
            if resp.status_code != 200:
                log.debug("RPC skip HTTP %s %s", resp.status_code, ep.url.split("//")[-1].split("/")[0].split("?")[0])
                return RPCResponse(result=None, endpoint=ep.url, latency_ms=latency, error=f"http {resp.status_code}")
            data = resp.json()
            if "error" in data:
                err_msg = str(data["error"])
                if _is_rate_limit(err_msg) or _is_version_skip(err_msg):
                    log.debug("RPC skip %s %s", ep.url.split("//")[-1].split("/")[0].split("?")[0], err_msg[:80])
                    return RPCResponse(result=None, endpoint=ep.url, latency_ms=latency, error=err_msg)
                await self._record_failure(ep, err_msg)
                return RPCResponse(result=None, endpoint=ep.url, latency_ms=latency, error=err_msg)
            await self._record_success(ep, latency)
            return RPCResponse(result=data.get("result"), endpoint=ep.url, latency_ms=latency)
        except Exception as exc:
            latency = (time.perf_counter() - start) * 1000.0
            log.debug("RPC skip %s %s", ep.url.split("//")[-1].split("/")[0].split("?")[0], exc)
            return RPCResponse(result=None, endpoint=ep.url, latency_ms=latency, error=str(exc))

    async def _record_success(self, ep: RPCEndpoint, latency_ms: float) -> None:
        async with self._lock:
            ep.last_latency_ms = latency_ms
            ep.consecutive_failures = 0
            ep.healthy = True
            ep.last_checked = time.time()

    async def _record_failure(self, ep: RPCEndpoint, reason: str) -> None:
        async with self._lock:
            ep.consecutive_failures += 1
            ep.last_latency_ms = 9999.0
            if ep.consecutive_failures >= self.max_failures:
                ep.healthy = False
                log.warning("Endpoint marked unhealthy: %s (%s)", ep.url.split("//")[-1].split("/")[0].split("?")[0], reason)
            ep.last_checked = time.time()

    async def health_check_all(self) -> Dict[str, Any]:
        """Probe every endpoint with getHealth + getSlot."""
        results = {}
        for ep in self.endpoints:
            health = await self._call("getHealth", endpoint=ep)
            # getSlot with no params uses node default (usually processed)
            slot = await self._call("getSlot", [], endpoint=ep)
            results[ep.url] = {
                "healthy": health.error is None and slot.error is None,
                "health": health.result if health.error is None else health.error,
                "slot": slot.result if slot.error is None else None,
                "latency_ms": round((health.latency_ms + slot.latency_ms) / 2, 1),
            }
        return results

    # ------------------------------------------------------------------
    # Public convenience methods
    # ------------------------------------------------------------------

    async def get_slot(self, commitment: str = "processed") -> RPCResponse:
        # Modern Solana RPC accepts optional config object; empty list = node default
        if commitment and commitment != "processed":
            return await self._call("getSlot", [{"commitment": commitment}])
        return await self._call("getSlot", [])

    async def get_latest_blockhash(self, commitment: str = "processed") -> RPCResponse:
        return await self._call("getLatestBlockhash", [{"commitment": commitment}])

    async def get_health(self) -> RPCResponse:
        return await self._call("getHealth")

    async def get_version(self) -> RPCResponse:
        return await self._call("getVersion")

    async def get_balance(self, pubkey: str) -> RPCResponse:
        return await self._call("getBalance", [pubkey])

    async def get_account_info(self, pubkey: str, encoding: str = "base64") -> RPCResponse:
        return await self._call(
            "getAccountInfo",
            [pubkey, {"encoding": encoding, "commitment": "processed"}],
        )

    async def get_token_supply(self, mint: str) -> RPCResponse:
        return await self._call("getTokenSupply", [mint])

    async def get_transaction(
        self,
        signature: str,
        commitment: str = "confirmed",
        max_supported_transaction_version: int = 0,
    ) -> RPCResponse:
        """Fetch a transaction with jsonParsed encoding for instruction decoding."""
        return await self._call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": commitment,
                    "maxSupportedTransactionVersion": 1,
                },
            ],
        )

    async def simulate_transaction(
        self, serialized_tx_base64: str, sig_verify: bool = False
    ) -> RPCResponse:
        return await self._call(
            "simulateTransaction",
            [
                serialized_tx_base64,
                {
                    "encoding": "base64",
                    "sigVerify": sig_verify,
                    "commitment": "processed",
                    "replaceRecentBlockhash": True,
                },
            ],
        )

    async def send_transaction(
        self, serialized_tx_base64: str, skip_preflight: bool = True
    ) -> RPCResponse:
        return await self._call(
            "sendTransaction",
            [
                serialized_tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": skip_preflight,
                    "preflightCommitment": "processed",
                    "maxRetries": 0,
                },
            ],
        )

    def status(self) -> List[Dict[str, Any]]:
        return [
            {
                "url": e.url.split("//")[-1].split("/")[0].split("?")[0],
                "healthy": e.healthy,
                "latency_ms": round(e.last_latency_ms, 1),
                "failures": e.consecutive_failures,
            }
            for e in self.endpoints
        ]


# Default public endpoints (no API key required for basic use).
# Prefer official + well-known free endpoints. Users should add Helius / Triton / QuickNode for production.
DEFAULT_ENDPOINTS = [
    "https://solana-rpc.publicnode.com",
    "https://api.mainnet-beta.solana.com",
]


async def create_default_client(
    extra_endpoints: Optional[List[str]] = None,
) -> SolanaRPCClient:
    eps = list(DEFAULT_ENDPOINTS)
    if extra_endpoints:
        eps = extra_endpoints + eps
    client = SolanaRPCClient(endpoints=eps, timeout_ms=4000)
    await client.start()
    return client
