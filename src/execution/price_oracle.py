"""
Real price marks via Jupiter quotes (and optional RPC reserve hooks).

Primary: Jupiter quote SOL→token and token→SOL for mid/impact-aware marks.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from src.execution.jupiter import JupiterClient, SOL_MINT, LAMPORTS_PER_SOL

log = logging.getLogger("sol-sniper.oracle")


@dataclass
class PriceMark:
    mint: str
    sol_per_raw_token: float
    raw_tokens_per_sol: float
    price_impact_pct: float
    source: str
    ts: float


class PriceOracle:
    def __init__(self, jupiter: Optional[JupiterClient] = None, cache_ttl_s: float = 8.0):
        self.jup = jupiter or JupiterClient()
        self._owns_jup = jupiter is None
        self.cache_ttl = cache_ttl_s
        self._cache: Dict[str, Tuple[float, PriceMark]] = {}

    async def close(self) -> None:
        if self._owns_jup:
            await self.jup.close()

    async def mark_sol_per_raw_token(self, mint: str, probe_sol: float = 0.05) -> Optional[PriceMark]:
        """
        Quote buying `probe_sol` SOL worth of `mint` → derive SOL per raw token unit.
        """
        now = time.time()
        cached = self._cache.get(mint)
        if cached and now - cached[0] < self.cache_ttl:
            return cached[1]

        q = await self.jup.quote_sol_to_token(mint, probe_sol, slippage_bps=150)
        if not q or q.out_amount <= 0:
            return None

        sol_per_raw = (q.in_amount / LAMPORTS_PER_SOL) / q.out_amount
        raw_per_sol = q.out_amount / (q.in_amount / LAMPORTS_PER_SOL) if q.in_amount else 0.0
        mark = PriceMark(
            mint=mint,
            sol_per_raw_token=sol_per_raw,
            raw_tokens_per_sol=raw_per_sol,
            price_impact_pct=q.price_impact_pct,
            source="jupiter_quote",
            ts=now,
        )
        self._cache[mint] = (now, mark)
        return mark

    async def value_position_sol(self, mint: str, raw_token_amount: int) -> Optional[float]:
        """Estimate SOL value of a token balance via sell quote."""
        if raw_token_amount <= 0:
            return 0.0
        q = await self.jup.quote_token_to_sol(mint, raw_token_amount, slippage_bps=150)
        if not q:
            return None
        return q.out_amount / LAMPORTS_PER_SOL
