
"""Jupiter swap client with multi-host fallback, 429 backoff, and light rate limiting."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

log = logging.getLogger("sol-sniper.jupiter")

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000
# A mint Jupiter cannot price. Do not ask again for 30 minutes.
_NO_MARKET: Dict[str, float] = {}

QUOTE_URLS = [
    "https://lite-api.jup.ag/swap/v1/quote",
    "https://api.jup.ag/swap/v1/quote",
]
SWAP_URLS = [
    "https://lite-api.jup.ag/swap/v1/swap",
    "https://api.jup.ag/swap/v1/swap",
]


_TRADE_BUSY_UNTIL = 0.0   # monotonic: a buy/sell quote is in flight; price polls stand aside

# Background quotes (value marks, dust checks, wallet scans) share one budget across every client:
# one network quote per BG_GAP_S, keyless host only, cached BG_CACHE_S. Buys and sells (urgent) skip it.
BG_GAP_S = 6.0
BG_CACHE_S = 45.0
_BG = {"next": 0.0, "backoff": 0.0}
_METER: Dict[str, int] = {}
_METER_AT = [time.monotonic()]


def _meter(key: str) -> None:
    _METER[key] = _METER.get(key, 0) + 1
    now = time.monotonic()
    if now - _METER_AT[0] >= 60.0:
        _METER_AT[0] = now
        parts = ", ".join(f"{k} {v}" for k, v in sorted(_METER.items(), key=lambda kv: -kv[1]))
        _METER.clear()
        log.info("JUPITER 60s: %s", parts or "none")


def _caller() -> str:
    import sys
    f = sys._getframe(3)
    while f is not None and f.f_code.co_name in ("quote", "quote_sol_to_token", "quote_token_to_sol"):
        f = f.f_back
    return f.f_code.co_name if f is not None else "?"


def bg_allowed() -> bool:
    """A background Jupiter call (quote or price) may go out now."""
    now = time.monotonic()
    return not trade_busy() and now >= _BG["next"] and now >= _BG["backoff"]


def bg_quiet() -> bool:
    """Jupiter is not rate-limiting background calls and no trade needs it."""
    return not trade_busy() and time.monotonic() >= _BG["backoff"]


def bg_spend() -> None:
    _BG["next"] = time.monotonic() + BG_GAP_S


def bg_rate_limited(seconds: float = 20.0) -> None:
    _BG["backoff"] = max(_BG["backoff"], time.monotonic() + seconds)


def trade_busy() -> bool:
    """True while a buy or sell needs Jupiter. Background price polls must not spend the rate limit."""
    return time.monotonic() < _TRADE_BUSY_UNTIL


@dataclass
class JupiterQuote:
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    price_impact_pct: float
    route_plan: List[Any]
    raw: Dict[str, Any]

    @property
    def in_sol(self) -> float:
        return self.in_amount / LAMPORTS_PER_SOL if self.input_mint == SOL_MINT else 0.0

    @property
    def out_sol(self) -> float:
        return self.out_amount / LAMPORTS_PER_SOL if self.output_mint == SOL_MINT else 0.0


class JupiterClient:
    def __init__(self, timeout_s: float = 20.0, min_interval_s: float = 0.55):
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=8.0),
            limits=httpx.Limits(max_keepalive_connections=2, max_connections=4),
        )
        self._api_key = os.environ.get("JUPITER_API_KEY", "").strip()
        self._min_interval = min_interval_s
        self._lock = asyncio.Lock()
        self._last_call = 0.0
        self._backoff_until = 0.0
        self._quote_cache: Dict[str, Tuple[float, JupiterQuote]] = {}
        self._cache_ttl = 5.0
        # Circuit breaker: after 429 / failed swap storm, block new swaps
        self._circuit_until = 0.0
        self._fail_streak = 0
        self._last_error = ""

    async def close(self) -> None:
        await self._client.aclose()

    def _urls(self, kind: str) -> List[str]:
        """lite-api needs no key and answers. api.jup.ag is only used when a key is set.

        The paid host is what was raising ConnectTimeout from this machine.
        """
        urls = list(QUOTE_URLS if kind == "quote" else SWAP_URLS)
        if self._api_key:
            # Keyed host first: its rate limit belongs to buys and sells (price polls use lite-api).
            urls.sort(key=lambda u: "lite-api.jup.ag" in u)
        if not self._api_key:
            urls = [url for url in urls if "lite-api.jup.ag" in url]
        return urls or ["https://lite-api.jup.ag/swap/v1/" + ("quote" if kind == "quote" else "swap")]

    async def _http(self, method: str, url: str, *, params: dict | None = None, json_body=None, timeout: float = 3.0):
        from src.execution.tx_route import sync_https
        return await asyncio.to_thread(
            sync_https, method, url, params=params, json_body=json_body, headers=self._headers(), timeout=timeout,
        )

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json", "Content-Type": "application/json"}
        if self._api_key:
            h["x-api-key"] = self._api_key
        return h

    async def _throttle(self, urgent: bool = False) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._backoff_until:
                wait_b = self._backoff_until - now
                if urgent:
                    wait_b = min(wait_b, 0.35)
                await asyncio.sleep(wait_b)
                now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0 and not urgent:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()

    def _set_backoff(self, seconds: float) -> None:
        # Cap flood: at least 3s, up to 20s on repeated 429s
        seconds = max(3.0, min(20.0, seconds))
        self._backoff_until = max(self._backoff_until, time.monotonic() + seconds)
        log.warning("Jupiter backoff %.1fs (rate limit)", seconds)

    def _trip_circuit(self, reason: str, seconds: float = 25.0) -> None:
        self._fail_streak += 1
        # Only hard-block after 2 consecutive failures; shorter pauses
        if self._fail_streak < 2:
            self._set_backoff(max(3.0, seconds * 0.4))
            self._last_error = reason
            log.warning("Jupiter soft fail (%s) streak=%s — backoff only", reason, self._fail_streak)
            return
        pause = min(90.0, max(20.0, 20.0 * (self._fail_streak - 1)))
        self._circuit_until = max(self._circuit_until, time.monotonic() + pause)
        self._last_error = reason
        log.error(
            "Jupiter CIRCUIT OPEN %.0fs — pause buy/sell (%s) streak=%s",
            pause, reason, self._fail_streak,
        )

    def _clear_fail_streak(self) -> None:
        if self._fail_streak:
            log.info("Jupiter healthy again (streak was %s)", self._fail_streak)
        self._fail_streak = 0
        self._last_error = ""

    def trading_allowed(self) -> bool:
        """False while rate-limited / circuit open — do not buy or sell."""
        return time.monotonic() >= self._circuit_until

    def blocked_reason(self) -> str:
        if self.trading_allowed():
            return ""
        left = max(0.0, self._circuit_until - time.monotonic())
        return f"jupiter_circuit {left:.0f}s left ({self._last_error or '429/swap_fail'})"

    async def quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        slippage_bps: int = 150,
        only_direct_routes: bool = False,
        urgent: bool = False,
    ) -> Optional[JupiterQuote]:
        global _TRADE_BUSY_UNTIL
        token = output_mint if input_mint == SOL_MINT else input_mint
        if time.monotonic() < _NO_MARKET.get(token, 0):
            self._last_error = "NO_ROUTES_FOUND"
            return None
        cache_key = f"{input_mint}:{output_mint}:{amount}:{slippage_bps}"
        hit = self._quote_cache.get(cache_key)
        if urgent:
            _TRADE_BUSY_UNTIL = max(_TRADE_BUSY_UNTIL, time.monotonic() + 6.0)
            if hit and time.monotonic() - hit[0] < self._cache_ttl:
                return hit[1]
            _meter("trade")
        else:
            # A value mark or scan quote. It never competes with a trade and never uses the key.
            if hit and time.monotonic() - hit[0] < BG_CACHE_S:
                return hit[1]
            if not bg_allowed() or time.monotonic() < self._backoff_until or not self.trading_allowed():
                self._last_error = "background_skip"
                _meter("skipped")
                return None
            bg_spend()
            _meter(_caller())

        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(slippage_bps),
        }
        if only_direct_routes:
            params["onlyDirectRoutes"] = "true"

        last_err = None
        urls = self._urls("quote")
        if not urgent:
            urls = [u for u in urls if "lite-api.jup.ag" in u] or urls[:1]
        for url in urls:
            await self._throttle(urgent=urgent)
            try:
                status, text = await self._http("GET", url, params=params, timeout=30.0)
                if status != 200:
                    body = (text or "")[:160].replace("\n", " ") if isinstance(text, str) else ""
                    last_err = f"{url} HTTP {status} {body}".strip()
                    if status == 429:
                        _meter("429")
                        bg_rate_limited(20.0)
                        if not urgent:
                            # A background quote: no backoff for trades, no second host. Just stop.
                            self._last_error = last_err
                            return None
                        self._set_backoff(8.0)
                        if not getattr(self, "_rate_log_at", 0) or time.monotonic() - self._rate_log_at > 30:
                            self._rate_log_at = time.monotonic()
                            log.warning("Jupiter quote 429 on %s — trying the next host", url.split("/")[2])
                        continue   # the other host has its own limit (api.jup.ag uses the API key)
                    if "NO_ROUTES" in body.upper() or "NOT_TRADABLE" in body.upper():
                        _NO_MARKET[token] = time.monotonic() + 1800
                        self._last_error = last_err
                        log.warning(
                            "Jupiter no market mint=%s — not asking again for 30m",
                            token[:8],
                        )
                        return None
                    log.debug("Jupiter skip non-200 %s", last_err)
                    continue
                data = json.loads(text or "{}")
                if "outAmount" not in data:
                    last_err = "missing outAmount"
                    continue
                q = JupiterQuote(
                    input_mint=input_mint,
                    output_mint=output_mint,
                    in_amount=int(data.get("inAmount") or amount),
                    out_amount=int(data["outAmount"]),
                    price_impact_pct=float(data.get("priceImpactPct") or 0),
                    route_plan=data.get("routePlan") or [],
                    raw=data,
                )
                self._quote_cache[cache_key] = (time.monotonic(), q)
                self._clear_fail_streak()
                log.info("Jupiter quote OK host=%s out=%s", url.split("/")[2], data["outAmount"])
                return q
            except Exception as exc:
                last_err = str(exc) or type(exc).__name__
                if not getattr(self, "_quote_warn_at", 0) or time.monotonic() - self._quote_warn_at > 20:
                    self._quote_warn_at = time.monotonic()
                    log.warning("Jupiter quote error: %s", last_err)
                else:
                    log.debug("Jupiter quote error: %s", last_err)
        if not getattr(self, "_quote_fail_at", 0) or time.monotonic() - self._quote_fail_at > 20:
            self._quote_fail_at = time.monotonic()
            log.warning("Jupiter quote all hosts failed: %s", last_err or "no reply")
        else:
            log.debug("Jupiter quote all hosts failed: %s", last_err or "no reply")
        self._last_error = str(last_err or "")
        return None

    async def quote_sol_to_token(self, token_mint: str, sol_amount: float, slippage_bps: int = 150, urgent: bool = False):
        lamports = int(sol_amount * LAMPORTS_PER_SOL)
        if lamports <= 0:
            return None
        return await self.quote(SOL_MINT, token_mint, lamports, slippage_bps=slippage_bps, urgent=urgent)

    async def quote_token_to_sol(self, token_mint: str, token_amount_raw: int, slippage_bps: int = 200, urgent: bool = False):
        if token_amount_raw <= 0:
            return None
        return await self.quote(token_mint, SOL_MINT, int(token_amount_raw), slippage_bps=slippage_bps, urgent=urgent)

    async def swap_transaction(
        self,
        quote_raw: Dict[str, Any],
        user_public_key: str,
        wrap_and_unwrap_sol: bool = True,
        prioritization_fee_lamports: Optional[int] = None,
        dynamic_compute_unit_limit: bool = True,
        urgent: bool = False,
    ) -> Optional[str]:
        if not urgent and not self.trading_allowed():
            log.warning("Jupiter swap blocked: %s", self.blocked_reason())
            return None
        body: Dict[str, Any] = {
            "quoteResponse": quote_raw,
            "userPublicKey": user_public_key,
            "wrapAndUnwrapSol": wrap_and_unwrap_sol,
            "dynamicComputeUnitLimit": dynamic_compute_unit_limit,
        }
        if prioritization_fee_lamports is not None:
            body["prioritizationFeeLamports"] = int(prioritization_fee_lamports)

        last_err = None
        rounds = 2 if urgent else 3
        for attempt in range(1, rounds):
            for url in self._urls("swap"):
                await self._throttle(urgent=urgent)
                try:
                    status, text = await self._http("POST", url, json_body=body, timeout=30.0)
                    if status != 200:
                        last_err = f"{url} HTTP {status}"
                        log.debug("Jupiter swap skip non-200 %s", last_err)
                        continue
                    tx = (json.loads(text or "{}") or {}).get("swapTransaction")
                    if tx:
                        if attempt > 1:
                            log.info("Jupiter swap OK on retry attempt=%s", attempt)
                        self._clear_fail_streak()
                        return tx
                    last_err = "missing swapTransaction"
                except Exception as exc:
                    last_err = str(exc)
                    log.warning("Jupiter swap error: %s", exc)
            await asyncio.sleep(0.25 if urgent else 0.8 * attempt)
        log.warning("Jupiter swap all hosts failed: %s", last_err)
        if not urgent:
            self._trip_circuit(f"swap_failed:{last_err}", 60.0)
        return None
