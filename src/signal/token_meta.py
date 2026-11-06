
"""
Fetch token name / symbol / website for UI enrichment.

Sources (best-effort, non-blocking failures):
  1. DexScreener token API
  2. Jupiter token search (lite)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional

import httpx

log = logging.getLogger("sol-sniper.meta")


@dataclass
class TokenMetaInfo:
    mint: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    website: Optional[str] = None
    image_uri: Optional[str] = None
    source: str = "none"


_cache: Dict[str, TokenMetaInfo] = {}


async def fetch_pump_brief(mint: str, timeout: float = 3.0) -> Dict[str, Optional[str]]:
    """Name, ticker and age from Pump. A coin a few seconds old is not on DexScreener yet."""
    out: Dict[str, Optional[str]] = {"name": None, "symbol": None, "created": None, "website": None}
    if not mint:
        return out
    urls = (
        f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
        f"https://frontend-api-v3.pump.fun/coins/{mint}",
    )
    try:
        async with httpx.AsyncClient(timeout=timeout, headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Origin": "https://pump.fun",
            "Referer": f"https://pump.fun/coin/{mint}",
        }) as client:
            for url in urls:
                try:
                    r = await client.get(url)
                except Exception:
                    continue
                if r.status_code != 200:
                    continue
                data = r.json() or {}
                if not isinstance(data, dict):
                    continue
                out["name"] = data.get("name") or out["name"]
                out["symbol"] = data.get("symbol") or out["symbol"]
                created = data.get("created_timestamp") or data.get("created_at")
                if created not in (None, ""):
                    out["created"] = str(created)
                out["website"] = data.get("website") or out["website"]
                if out["symbol"] or out["name"]:
                    break
    except Exception as e:
        log.debug("pump meta failed for %s: %s", mint[:8], e)
    return out


async def fetch_token_meta(mint: str, timeout: float = 4.0) -> TokenMetaInfo:
    if not mint:
        return TokenMetaInfo(mint=mint or "")
    if mint in _cache:
        return _cache[mint]

    info = TokenMetaInfo(mint=mint)

    brief = await fetch_pump_brief(mint, timeout=min(timeout, 3.0))
    info.name = brief.get("name") or None
    info.symbol = brief.get("symbol") or None
    info.website = brief.get("website") or None
    if info.symbol or info.name:
        info.source = "pump"

    # DexScreener, only if Pump did not already name the coin.
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
            if r.status_code == 200:
                data = r.json()
                pairs = data.get("pairs") or []
                if pairs:
                    # Prefer highest liquidity pair
                    pairs = sorted(
                        pairs,
                        key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
                        reverse=True,
                    )
                    p0 = pairs[0]
                    base = p0.get("baseToken") or {}
                    info.name = base.get("name") or info.name
                    info.symbol = base.get("symbol") or info.symbol
                    info.image_uri = (p0.get("info") or {}).get("imageUrl") or info.image_uri
                    # websites / socials
                    websites = (p0.get("info") or {}).get("websites") or []
                    if websites:
                        # list of {url: ...} or strings
                        w0 = websites[0]
                        info.website = w0.get("url") if isinstance(w0, dict) else str(w0)
                    if not info.website:
                        url = p0.get("url")  # dexscreener page
                        if url:
                            info.website = url
                    # fallback link to dexscreener
                    if not info.website:
                        info.website = f"https://dexscreener.com/solana/{mint}"
                    info.source = "dexscreener"
    except Exception as e:
        log.debug("dexscreener meta failed for %s: %s", mint[:8], e)

    # Jupiter lite token API fallback for name/symbol
    if not info.symbol or not info.name:
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(f"https://tokens.jup.ag/token/{mint}")
                if r.status_code == 200:
                    data = r.json()
                    info.name = info.name or data.get("name")
                    info.symbol = info.symbol or data.get("symbol")
                    info.image_uri = info.image_uri or data.get("logoURI")
                    if info.source == "none":
                        info.source = "jupiter"
        except Exception as e:
            log.debug("jupiter meta failed for %s: %s", mint[:8], e)

    if not info.website:
        info.website = f"https://solscan.io/token/{mint}"

    if info.symbol or info.name:
        _cache[mint] = info
    return info
