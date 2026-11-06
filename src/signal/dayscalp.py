"""Day-scalp scanner.

Plain Python. The old file was 3.10 bytecode, which this bot cannot load
(bad marshal data). The rules are the same: 5m band, 1m in that band,
1s from +0.10% to +2.0%, volume floor, then one buy.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import httpx

log = logging.getLogger("sol-sniper.dayscalp")

GECKO_PUMPFUN = "https://api.geckoterminal.com/api/v2/networks/solana/dexes/pump-fun/pools"
DEX_TOKEN = "https://api.dexscreener.com/latest/dex/tokens"
DEX_SEARCH = "https://api.dexscreener.com/latest/dex/search"
DEXSCREENER_RANK_URL = (
    "https://dexscreener.com/solana/pumpfun?rankBy=priceChangeM5&order=desc&min5MVol=1000"
)
PUMP_DEX_IDS = frozenset({"pumpfun", "pump.fun", "pumpswap"})
_RANK_CACHE: List[dict] = []
_RANK_CACHE_TS = 0.0
_RANK_CURSOR = 0
_RANK_CACHE_TTL = 15.0
_UA = {"User-Agent": "sol-sniper/1.2", "Accept": "application/json"}


def _num(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class DayScalpConfig:
    scan_interval_sec: float = 3
    min_liquidity_usd: float = 0
    min_volume_m5_usd: float = 1000
    min_m5_change_pct: float = 25
    max_m5_change_pct: float = 1000
    min_m1_support_bounce_pct: float = 0.2
    max_candidates_per_scan: int = 1
    max_position_sol: float = 0.02
    hard_stop_pct: float = -0.35
    runner_stop_pct: float = -0.12
    time_stop_seconds: float = 300
    time_stop_min_gain: float = 0.15
    trail_activate_pct: float = 0.8
    trail_drawdown_pct: float = 0.32
    tp_levels: List[float] = field(default_factory=lambda: [0.3, 0.8])
    tp_fractions: List[float] = field(default_factory=lambda: [0.5, 0.5])
    require_buy_side_pressure: bool = False
    blacklist_keywords: List[str] = field(default_factory=lambda: ["scam", "test", "rug"])
    entry_polls: int = 3
    entry_poll_interval_sec: float = 1
    buy_max_attempts: int = 8
    buy_retry_sec: float = 2
    stop_pct: float = -0.35
    min_hold_seconds: float = 20
    hard_stop_pct_override: float = -0.35
    top_volume_pool: int = 50
    gecko_pages: int = 2
    rugcheck_enabled: bool = False


@dataclass
class DayScalpCandidate:
    mint: str
    symbol: str
    name: str
    price_usd: float
    liquidity_usd: float
    volume_m5_usd: float
    change_m5_pct: float
    change_h1_pct: float = 0.0
    buys_m5: int = 0
    sells_m5: int = 0
    url: str = ""
    pair_address: str = ""
    reasons: List[str] = field(default_factory=list)
    pump_m5_pct: Optional[float] = None
    quick_entry: bool = False


_RUGCHECK_ENABLED = False


def rugcheck_is_on() -> bool:
    return bool(_RUGCHECK_ENABLED)


def set_rugcheck_enabled(on=None) -> None:
    global _RUGCHECK_ENABLED
    _RUGCHECK_ENABLED = bool(on)


def _blacklisted(symbol: str, name: str, cfg: DayScalpConfig) -> bool:
    blob = f"{symbol} {name}".lower()
    return any(str(kw).lower() in blob for kw in (cfg.blacklist_keywords or []))


def _band() -> Tuple[float, float]:
    floor, ceiling = 25.0, 1000.0
    try:
        from src.config.runtime import get_config
        cfg = get_config()
        floor = float(getattr(cfg, "min_m5_pct", floor))
        ceiling = float(getattr(cfg, "max_m5_pct", ceiling))
    except Exception:
        pass
    return floor, ceiling


def _apply_live_band(cfg: DayScalpConfig) -> DayScalpConfig:
    floor, ceiling = _band()
    # Live Controls win when they were actually set. The dataclass defaults stay
    # if the runtime object has no 5m floor yet.
    try:
        from src.config.runtime import get_config
        live = get_config()
        if getattr(live, "min_m5_pct", None) is not None:
            cfg.min_m5_change_pct = floor
        if getattr(live, "max_m5_pct", None) is not None:
            cfg.max_m5_change_pct = ceiling
    except Exception:
        pass
    return cfg


def dayscalp_config_from_dict(d: Optional[dict]) -> DayScalpConfig:
    d = d or {}
    cfg = DayScalpConfig()
    for key, value in d.items():
        if hasattr(cfg, key) and value is not None:
            setattr(cfg, key, value)
    cfg.max_candidates_per_scan = 1
    if float(cfg.min_volume_m5_usd) < 1000:
        cfg.min_volume_m5_usd = 1000
    return cfg


def _row_from_gecko(pool: dict) -> Optional[dict]:
    attr = pool.get("attributes") or {}
    rel = (pool.get("relationships") or {}).get("base_token") or {}
    token_id = str((rel.get("data") or {}).get("id") or "")
    mint = token_id.split("_", 1)[-1] if token_id else ""
    if len(mint) < 32:
        return None
    pc = attr.get("price_change_percentage") or {}
    vol = attr.get("volume_usd") or {}
    name = str(attr.get("name") or "")
    symbol = name.split("/")[0].strip()[:16] or mint[:6]
    pair = str(attr.get("address") or "")
    return {
        "mint": mint,
        "pair_address": pair,
        "symbol": symbol,
        "name": name[:40],
        "price_usd": _num(attr.get("base_token_price_usd")),
        "liquidity_usd": _num(attr.get("reserve_in_usd")),
        "volume_m5_usd": _num(vol.get("m5")),
        "change_m5_pct": _num(pc.get("m5")),
        "change_h1_pct": _num(pc.get("h1")),
        "buys_m5": 0,
        "sells_m5": 0,
        "url": f"https://dexscreener.com/solana/{pair or mint}",
    }


def _row_from_dex_pair(pair: dict) -> Optional[dict]:
    if str(pair.get("chainId") or "").lower() not in ("", "solana"):
        return None
    dex = str(pair.get("dexId") or "").lower()
    base = pair.get("baseToken") or {}
    mint = str(base.get("address") or "")
    if len(mint) < 32:
        return None
    if dex not in PUMP_DEX_IDS and not mint.lower().endswith("pump"):
        return None
    ch = pair.get("priceChange") or {}
    vol = pair.get("volume") or {}
    tx = ((pair.get("txns") or {}).get("m5") or {})
    liq = pair.get("liquidity") or {}
    pa = str(pair.get("pairAddress") or "")
    return {
        "mint": mint,
        "pair_address": pa,
        "symbol": str(base.get("symbol") or mint[:6])[:16],
        "name": str(base.get("name") or "")[:40],
        "price_usd": _num(pair.get("priceUsd")),
        "liquidity_usd": _num(liq.get("usd")),
        "volume_m5_usd": _num(vol.get("m5")),
        "change_m5_pct": _num(ch.get("m5")),
        "change_h1_pct": _num(ch.get("h1")),
        "buys_m5": int(_num(tx.get("buys"))),
        "sells_m5": int(_num(tx.get("sells"))),
        "url": pair.get("url") or f"https://dexscreener.com/solana/{pa or mint}",
    }


def ranked_mints() -> List[str]:
    return [str(row.get("mint") or "") for row in (_RANK_CACHE or []) if row.get("mint")]


async def fetch_pumpfun_ranked_by_m5(client: httpx.AsyncClient, pages: int = 2) -> List[dict]:
    """DexScreener search first. Gecko pump pools only fill gaps. Highest 5m volume wins."""
    global _RANK_CACHE, _RANK_CACHE_TS, _RANK_CURSOR
    now = time.time()
    if _RANK_CACHE and now - _RANK_CACHE_TS < _RANK_CACHE_TTL:
        log.info("DAYSCALP using rank cache age=%.0fs n=%d", now - _RANK_CACHE_TS, len(_RANK_CACHE))
        return list(_RANK_CACHE)
    rows: Dict[str, dict] = {}

    def keep(row: Optional[dict]) -> None:
        if not row or len(str(row.get("mint") or "")) < 32:
            return
        prev = rows.get(row["mint"])
        if prev is None or _num(row.get("volume_m5_usd")) > _num(prev.get("volume_m5_usd")):
            rows[row["mint"]] = row

    queries = ("solana",)

    async def one_search(q: str) -> List[dict]:
        del q
        try:
            mints = []
            for url in (
                "https://api.dexscreener.com/token-boosts/latest/v1",
                "https://api.dexscreener.com/token-profiles/latest/v1",
                "https://api.dexscreener.com/token-boosts/top/v1",
            ):
                try:
                    boosts = await client.get(url, timeout=5.0)
                except Exception:
                    continue
                if boosts.status_code != 200:
                    continue
                for item in boosts.json() or []:
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("chainId") or "") != "solana":
                        continue
                    mint = str(item.get("tokenAddress") or "")
                    if mint.lower().endswith("pump") and mint not in mints:
                        mints.append(mint)
            if not mints:
                return []
            found = []
            for start_at in range(0, min(len(mints), 45), 15):
                chunk = ",".join(mints[start_at:start_at + 15])
                response = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{chunk}", timeout=5.0)
                if response.status_code != 200:
                    continue
                for pair in (response.json() or {}).get("pairs") or []:
                    row = _row_from_dex_pair(pair)
                    if row and _num(row.get("volume_m5_usd")) > 0:
                        found.append(row)
            return found
        except Exception as exc:
            log.warning("dex boosts: %s", exc)
            return []

    for batch in await asyncio.gather(*(one_search(q) for q in queries)):
        for row in batch:
            keep(row)
    dex_n = len(rows)
    fresh_n = 0
    try:
        newest = await client.get(
            "https://frontend-api-v3.pump.fun/coins",
            params={
                "offset": "0",
                "limit": "30",
                "sort": "created_timestamp",
                "order": "DESC",
                "includeNsfw": "false",
            },
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://pump.fun",
                "Referer": "https://pump.fun/",
            },
            timeout=5.0,
        )
        if newest.status_code == 200:
            for coin in newest.json() or []:
                if not isinstance(coin, dict):
                    continue
                mint = str(coin.get("mint") or "")
                if len(mint) < 32 or mint in rows:
                    continue
                rows[mint] = {
                    "mint": mint,
                    "pair_address": "",
                    "symbol": str(coin.get("symbol") or mint[:6])[:16],
                    "name": str(coin.get("name") or "")[:40],
                    "price_usd": 0.0,
                    "liquidity_usd": _num(coin.get("usd_market_cap")),
                    "volume_m5_usd": 0.0,
                    "change_m5_pct": 0.0,
                    "change_h1_pct": 0.0,
                    "buys_m5": 0,
                    "sells_m5": 0,
                    "url": f"https://pump.fun/coin/{mint}",
                    "fresh": True,
                }
                fresh_n += 1
    except Exception as exc:
        log.warning("pump newest: %s", exc)
    fresh = [row for row in rows.values() if row.get("fresh")]
    rest = sorted(
        (row for row in rows.values() if not row.get("fresh")),
        key=lambda row: _num(row.get("change_m5_pct")),
        reverse=True,
    )
    ranked = fresh + rest
    _RANK_CACHE = ranked
    _RANK_CACHE_TS = now
    global _RANK_CURSOR
    _RANK_CURSOR = 0
    hot = sum(1 for row in ranked if _num(row.get("volume_m5_usd")) >= 1000)
    log.info(
        "DAYSCALP pumpfun ranked by priceChangeM5 desc | n=%d dex=%d new=%d vol>=1000=%d | target=%s",
        len(ranked), dex_n, fresh_n, hot, DEXSCREENER_RANK_URL,
    )
    for i, row in enumerate(ranked[:8], start=1):
        log.info(
            "DAYSCALP rank#%d %s m5=%.2f%% vol_m5=$%.0f liq=$%.0f mint=%s",
            i, row.get("symbol"), _num(row.get("change_m5_pct")),
            _num(row.get("volume_m5_usd")), _num(row.get("liquidity_usd")),
            str(row.get("mint") or "")[:8],
        )
    return ranked


def _best_pair(payload: dict) -> Optional[dict]:
    best = None
    best_liq = -1.0
    for pair in payload.get("pairs") or []:
        if str(pair.get("chainId") or "").lower() not in ("", "solana"):
            continue
        liq = _num((pair.get("liquidity") or {}).get("usd"))
        if liq >= best_liq:
            best = pair
            best_liq = liq
    return best


async def fetch_m5_pct(mint: str) -> Optional[float]:
    mint = (mint or "").strip()
    if len(mint) < 32:
        return None
    try:
        async with httpx.AsyncClient(timeout=4.0, headers=_UA) as client:
            response = await client.get(DEX_TOKEN + "/" + mint)
        if response.status_code != 200:
            return None
        pair = _best_pair(response.json() or {})
        if not pair:
            return None
        change = (pair.get("priceChange") or {}).get("m5")
        return None if change is None else float(change)
    except Exception:
        return None


async def fetch_h1_pct(mint: str) -> Optional[float]:
    mint = (mint or "").strip()
    if len(mint) < 32:
        return None
    try:
        async with httpx.AsyncClient(timeout=4.0, headers=_UA) as client:
            response = await client.get(DEX_TOKEN + "/" + mint)
        if response.status_code != 200:
            return None
        pair = _best_pair(response.json() or {})
        if not pair:
            return None
        change = (pair.get("priceChange") or {}).get("h1")
        return None if change is None else float(change)
    except Exception:
        return None


async def fetch_price_usd(mint: str) -> Optional[float]:
    mint = (mint or "").strip()
    if len(mint) < 32:
        return None
    try:
        async with httpx.AsyncClient(timeout=4.0, headers=_UA) as client:
            response = await client.get(DEX_TOKEN + "/" + mint)
        if response.status_code != 200:
            return None
        pair = _best_pair(response.json() or {})
        if not pair or pair.get("priceUsd") is None:
            return None
        return float(pair["priceUsd"])
    except Exception:
        return None


async def _pair_address(client: httpx.AsyncClient, mint: str) -> str:
    try:
        response = await client.get(DEX_TOKEN + "/" + mint)
        if response.status_code != 200:
            return ""
        pair = _best_pair(response.json() or {})
        return str((pair or {}).get("pairAddress") or "")
    except Exception:
        return ""


async def fetch_m1_pct(mint: str) -> Optional[float]:
    """The minute that is printing: last gecko minute candle, close versus open."""
    mint = (mint or "").strip()
    if len(mint) < 32:
        return None
    try:
        async with httpx.AsyncClient(timeout=4.0, headers=_UA) as client:
            pair = await _pair_address(client, mint)
            if not pair:
                return None
            response = await client.get(
                f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pair}/ohlcv/minute",
                params={"aggregate": 1, "limit": 2, "currency": "usd"},
            )
        if response.status_code != 200:
            return None
        candles = ((response.json() or {}).get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
        if not candles:
            return None
        last = candles[0]
        if not isinstance(last, (list, tuple)) or len(last) < 5:
            return None
        open_px, close_px = float(last[1]), float(last[4])
        if open_px <= 0:
            return None
        return (close_px / open_px - 1.0) * 100.0
    except Exception:
        return None


def _pct_from_trades(trades: List[Tuple[float, float]]) -> Optional[float]:
    """Move across the last two seconds of trades. A single print is not a candle."""
    points = sorted((ts, px) for ts, px in trades if px > 0)
    if len(points) < 2:
        return None
    last_ts = points[-1][0]
    window = [(ts, px) for ts, px in points if 0 <= last_ts - ts <= 2.0 and px > 0]
    if len(window) < 2 or window[-1][0] - window[0][0] < 0.4:
        return None
    base = window[0][1]
    if base <= 0:
        return None
    return (window[-1][1] - base) / base * 100.0


async def fetch_1s_pct(mint: str) -> Optional[float]:
    mint = (mint or "").strip()
    if len(mint) < 32:
        return None
    try:
        async with httpx.AsyncClient(timeout=4.0, headers=_UA) as client:
            pair = await _pair_address(client, mint)
            if not pair:
                return None
            response = await client.get(
                f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pair}/trades",
                params={"trade_volume_in_usd_greater_than": 0},
            )
        if response.status_code != 200:
            return None
        points: List[Tuple[float, float]] = []
        want = mint.lower()
        for row in (response.json() or {}).get("data") or []:
            attr = row.get("attributes") or {}
            stamp = str(attr.get("block_timestamp") or "").replace("Z", "+00:00")
            try:
                from datetime import datetime
                ts = datetime.fromisoformat(stamp).timestamp()
            except Exception:
                continue
            frm = str(attr.get("from_token_address") or "").lower()
            to = str(attr.get("to_token_address") or "").lower()
            if frm == want:
                px = _num(attr.get("price_from_in_usd"))
            elif to == want:
                px = _num(attr.get("price_to_in_usd"))
            else:
                px = 0.0
            if px > 0:
                points.append((ts, px))
        return _pct_from_trades(points)
    except Exception:
        return None


def _judge_second(pct, floor=None, ceiling=None):
    if pct is None:
        return False, "1s none"
    pct = float(pct)
    if floor is not None or ceiling is not None:
        lo = 0.0 if floor is None else float(floor)
        hi = None if ceiling is None else float(ceiling)
        if pct < lo:
            return False, f"1s candle {pct:+.2f}% below {lo:+.2f}% — no buy"
        if hi is not None and pct > hi:
            return False, f"1s candle {pct:+.2f}% above {hi:+.2f}% — no buy"
        return True, f"1s up {pct:+.2f}%"
    if pct < 0:
        return False, f"1s candle {pct:+.2f}% is not up — no buy"
    if pct == 0:
        return False, "1s candle +0.00% is not up — no buy"
    return True, f"1s up {pct:+.2f}%"


_dex_hist: Dict[str, List[Tuple[float, float]]] = {}
_dex_move_cache: Dict[str, Tuple[float, Optional[float], Optional[float]]] = {}


async def _remember_dex_price(mint: str) -> Optional[float]:
    px = await fetch_price_usd(mint)
    if px and px > 0:
        now = time.time()
        hist = _dex_hist.setdefault(mint, [])
        hist.append((now, float(px)))
        _dex_hist[mint] = [(ts, price) for ts, price in hist if now - ts < 180][-40:]
    return px


def _move_over(hist: List[Tuple[float, float]], lo: float, hi: float) -> Optional[float]:
    if len(hist) < 2:
        return None
    latest_ts, latest_px = hist[-1]
    for ts, old in hist:
        age = latest_ts - ts
        if lo <= age <= hi and old > 0 and latest_px > 0:
            return (latest_px - old) / old * 100.0
    return None


async def dex_moves(mint: str):
    """1-second and 1-minute move from DexScreener's own price."""
    now = time.time()
    hit = _dex_move_cache.get(mint)
    if hit and now - hit[0] < 5:
        return hit[1], hit[2]
    await _remember_dex_price(mint)
    hist = list(_dex_hist.get(mint) or [])
    second = _move_over(hist, 0.7, 3.0)
    if second is None and hist:
        await asyncio.sleep(1.0)
        await _remember_dex_price(mint)
        hist = list(_dex_hist.get(mint) or [])
        second = _move_over(hist, 0.7, 3.0)
    minute = _move_over(hist, 45.0, 180.0)
    if minute is None:
        minute = await fetch_m1_pct(mint)
    if len(_dex_move_cache) > 400:
        _dex_move_cache.clear()
    _dex_move_cache[mint] = (time.time(), second, minute)
    return second, minute


async def _gecko_second(client, mint):
    del client
    pct = await fetch_1s_pct(mint)
    if pct is None:
        return None, ""
    return pct, ""


async def _dex_second(client, mint):
    """DexScreener price now, against the price one second earlier."""
    del client
    second, _minute = await dex_moves(mint)
    if second is None:
        return None, "1s(dex) none"
    return second, ""


async def _pump_second(client, mint):
    del client, mint
    return None, ""


async def _second_candle_up(client, mint, pair_address="", floor=None, ceiling=None):
    del pair_address
    pct, _note = await _dex_second(client, mint)
    if pct is None:
        pct, _note = await _gecko_second(client, mint)
    if pct is None:
        return False, "1s(dex) none"
    return _judge_second(pct, floor, ceiling)


async def still_rising(client, mint, min_m5=None, max_m5=None):
    """Buy only when the 1-second print is +0.10% to +2.0%. A missing print is not a buy."""
    del min_m5, max_m5
    pair = ""
    try:
        response = await client.get(DEX_TOKEN + "/" + mint, timeout=4.0)
        if response.status_code == 200:
            found = _best_pair(response.json() or {})
            pair = str((found or {}).get("pairAddress") or "")
    except Exception:
        pair = ""
    return await _second_candle_up(client, mint, pair, floor=0.1, ceiling=2.0)


def m5_skip_reason(m5, floor=None, ceiling=None):
    if floor is None or ceiling is None:
        floor, ceiling = _band()
    if m5 is None:
        return f"5m missing — must be inside {floor:+.1f}% to {ceiling:+.0f}%"
    if float(m5) < float(floor):
        return f"5m {float(m5):+.1f}% below {float(floor):+.1f}% — no buy"
    if float(m5) > float(ceiling):
        return f"5m {float(m5):+.1f}% above {float(ceiling):+.1f}% — no buy"
    return None


def m1_skip_reason(m1, floor=None, ceiling=None):
    if floor is None or ceiling is None:
        floor, ceiling = _band()
    if m1 is None:
        return f"1m missing — must be inside {floor:+.1f}% to {ceiling:+.0f}%"
    if float(m1) < float(floor):
        return f"1m {float(m1):+.1f}% below {float(floor):+.1f}% — no buy"
    if float(m1) > float(ceiling):
        return f"1m {float(m1):+.1f}% above {float(ceiling):+.1f}% — no buy"
    return None


def s1_skip_reason(s1):
    """Unknown does not skip. Outside +0.10% to +2.0% does."""
    if s1 is None:
        return None
    pct = float(s1)
    if pct < 0.1:
        return f"1s candle {pct:+.2f}% below +0.10% — no buy"
    if pct > 2.0:
        return f"1s candle {pct:+.2f}% above +2.00% — no buy"
    return None


async def read_dayscalp_entry(mint: str, min_m5: float, max_m5: float):
    m5, m1, s1, h1 = await asyncio.gather(
        fetch_m5_pct(mint), fetch_m1_pct(mint), fetch_1s_pct(mint), fetch_h1_pct(mint),
    )
    block = m5_skip_reason(m5, min_m5, max_m5)
    if block is None:
        block = m1_skip_reason(m1, min_m5, max_m5)
    if block is None:
        block = s1_skip_reason(s1)
    return m5, m1, s1, block, h1


async def read_mark_candles(mint: str):
    m5, m1, s1, h1, px = await asyncio.gather(
        fetch_m5_pct(mint), fetch_m1_pct(mint), fetch_1s_pct(mint), fetch_h1_pct(mint), fetch_price_usd(mint),
    )
    return m5, m1, s1, h1, px


async def read_entry_candles(mint: str):
    return await read_mark_candles(mint)


def schedule_m1_fill(mint: str, ts: float = 0.0):
    async def _later():
        await asyncio.sleep(2)
        pct = await fetch_m1_pct(mint)
        if pct is None:
            return
        try:
            from src.observability.ledger import get_ledger
            get_ledger().fill_m1(mint, pct, ts or time.time())
        except Exception:
            return None
    try:
        asyncio.get_running_loop().create_task(_later())
    except Exception:
        return None


def schedule_m5_fill(mint: str, ts: float = 0.0):
    return schedule_m1_fill(mint, ts)


async def rugcheck_score_block(client, mint: str):
    if not rugcheck_is_on():
        return None
    try:
        from src.config.runtime import get_config
        if getattr(get_config(), "filter_mode", "") != "dayscalp":
            return None
    except Exception:
        return None
    try:
        response = await client.get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")
        if response.status_code != 200:
            return f"rugcheck summary HTTP {response.status_code} — no buy"
        body = response.json() or {}
        score = body.get("score_normalised", body.get("scoreNormalised"))
        if score is None:
            return "rugcheck score_normalised missing — no buy"
        score = float(score)
        if score > 21:
            return f"rugcheck score_normalised {score:.0f} not in Good 0-21 — no buy"
    except Exception as exc:
        return f"rugcheck report failed — {type(exc).__name__} — no buy"
    return None


async def _board_gate(client, row: dict) -> Optional[str]:
    if not rugcheck_is_on():
        return None
    return await rugcheck_score_block(client, row.get("mint") or "")


def _soft_filter_reason(row: dict, cfg: DayScalpConfig, vol_floor: float) -> Optional[str]:
    if _blacklisted(row.get("symbol") or "", row.get("name") or "", cfg):
        return "blacklist"
    m5 = _num(row.get("change_m5_pct"))
    if m5 < float(cfg.min_m5_change_pct):
        return f"m5 {m5:.1f}% < min {cfg.min_m5_change_pct}"
    if m5 > float(cfg.max_m5_change_pct):
        return f"m5 {m5:.1f}% > max {cfg.max_m5_change_pct}"
    vol = _num(row.get("volume_m5_usd"))
    if vol < vol_floor:
        return f"vol_m5 ${vol:.0f} < ${vol_floor:.0f}"
    if len(str(row.get("mint") or "")) < 32:
        return "bad_mint"
    return None


async def _show_decoded(row: dict, verdict: str, reason: str) -> None:
    """Put the coin on Decoded Tokens so a skip is visible, not only a log line."""
    try:
        from src.observability.dashboard import publish_decoded
        await publish_decoded(
            mint=row.get("mint") or "",
            liquidity_sol=_num(row.get("liquidity_usd")) / 150.0,
            mint_renounced=True,
            freeze_renounced=True,
            verdict=verdict,
            size_multiplier=0.0,
            source="dayscalp",
            signature="",
            reasons=[reason],
            name=row.get("name") or "",
            symbol=row.get("symbol") or "",
            website=row.get("url") or "",
        )
    except Exception:
        return None


async def _drop_client(client) -> None:
    try:
        await asyncio.wait_for(client.aclose(), timeout=1)
    except Exception:
        pass


async def select_top1_dayscalp(cfg: Optional[DayScalpConfig] = None) -> Optional[DayScalpCandidate]:
    """First pump.fun coin that passes the 5m band, the volume floor, and a rising 1s candle."""
    if not isinstance(cfg, DayScalpConfig):
        cfg = DayScalpConfig()
    log.info("DAYSCALP scan pass started")
    cfg = _apply_live_band(cfg)
    try:
        from src.config.runtime import get_config
        set_rugcheck_enabled(bool(getattr(get_config(), "rugcheck_enabled", False)))
    except Exception:
        pass
    client = httpx.AsyncClient(timeout=8.0, headers=_UA)
    try:
        ranked = await fetch_pumpfun_ranked_by_m5(client, pages=1)
        if not ranked:
            log.warning("DAYSCALP no pumpfun pools returned")
            return None
        vol_floor = float(cfg.min_volume_m5_usd)
        liquid = [row for row in ranked if _num(row.get("volume_m5_usd")) >= vol_floor]
        pool = ranked
        if not pool:
            log.warning("DAYSCALP no pumpfun pools returned")
            return None
        global _RANK_CURSOR
        width = 8
        start = _RANK_CURSOR % len(pool)
        show = [pool[(start + i) % len(pool)] for i in range(min(width, len(pool)))]
        _RANK_CURSOR = (start + len(show)) % len(pool)
        log.info(
            "DAYSCALP window %d-%d of %d · next pass continues down the list",
            start + 1, start + len(show), len(pool),
        )
        if not liquid:
            log.warning("DAYSCALP no pairs with vol >= $%.0f — showing what came back", vol_floor)
        else:
            log.info(
                "DAYSCALP top %d by 5m change — band %+.0f%% to %+.0f%%, vol>=$%.0f, 1m same band, 1s +0.10%% to +2.0%%",
                len(liquid), float(cfg.min_m5_change_pct), float(cfg.max_m5_change_pct), vol_floor,
            )
        settings_floor, settings_cap = _band()
        for i, row in enumerate(show, start=1):
            m5 = _num(row.get("change_m5_pct"))
            symbol = row.get("symbol") or "?"
            log.info("DAYSCALP check #%d %s m5=%+.1f%% vol=$%.0f", i, symbol, m5, _num(row.get("volume_m5_usd")))
            if not liquid:
                await _show_decoded(row, "SKIP", f"vol ${_num(row.get('volume_m5_usd')):.0f} below ${vol_floor:.0f}")
                continue
            if not (settings_floor <= m5 <= settings_cap):
                reason = f"5m {m5:+.1f}% outside {settings_floor:+.0f}% to {settings_cap:+.0f}%"
                log.info("DAYSCALP skip #%d %s — %s", i, symbol, reason)
                await _show_decoded(row, "SKIP", reason)
                continue
            why = _soft_filter_reason(row, cfg, vol_floor)
            if why:
                log.info("DAYSCALP skip #%d %s — %s", i, symbol, why)
                await _show_decoded(row, "SKIP", why)
                continue
            try:
                up, reason = await asyncio.wait_for(still_rising(client, row.get("mint") or ""), timeout=2)
            except Exception:
                up, reason = False, "1s check timed out"
            if not up:
                log.info("DAYSCALP skip #%d %s — %s", i, symbol, reason)
                await _show_decoded(row, "SKIP", reason)
                continue
            try:
                m1 = await asyncio.wait_for(fetch_m1_pct(row.get("mint") or ""), timeout=2)
            except Exception:
                m1 = None
            m1_why = m1_skip_reason(m1, settings_floor, settings_cap)
            if m1_why:
                log.info("DAYSCALP skip #%d %s — %s", i, symbol, m1_why)
                await _show_decoded(row, "SKIP", m1_why)
                continue
            log.info("DAYSCALP pick %s m5=%+.1f%% mint=%s", symbol, m5, str(row.get("mint") or "")[:8])
            await _show_decoded(row, "BUYING", reason)
            return DayScalpCandidate(
                mint=row["mint"],
                symbol=row.get("symbol") or "",
                name=row.get("name") or "",
                price_usd=_num(row.get("price_usd")),
                liquidity_usd=_num(row.get("liquidity_usd")),
                volume_m5_usd=_num(row.get("volume_m5_usd")),
                change_m5_pct=m5,
                change_h1_pct=_num(row.get("change_h1_pct")),
                buys_m5=int(row.get("buys_m5") or 0),
                sells_m5=int(row.get("sells_m5") or 0),
                url=row.get("url") or "",
                pair_address=row.get("pair_address") or "",
                reasons=[reason, f"m5={m5:+.1f}%", f"vol_m5=${_num(row.get('volume_m5_usd')):.0f}"],
            )
        log.info("DAYSCALP scan pass finished — none passed, next pass")
        return None
    finally:
        asyncio.create_task(_drop_client(client))


def _pct_num(block):
    if not isinstance(block, dict) or block.get("priceChangePercent") is None:
        return None
    try:
        return float(block["priceChangePercent"])
    except (TypeError, ValueError):
        return None


def _shown(value, label: str) -> str:
    return f"{label} none" if value is None else f"{label} {float(value):+.2f}%"


def _pump_activity_sync(mint: str):
    url = (
        f"https://swap-api.pump.fun/v1/coins/{mint}/market-activity"
        "?program=pump&chainId=solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
    )
    try:
        from src.data.fast_get import pump_get
        import httpx

        body = pump_get(url, timeout=0.9, cache_s=1.0)
        return 200, body if isinstance(body, dict) else None, ""
    except httpx.HTTPStatusError as exc:
        return exc.response.status_code, None, f"pump HTTP {exc.response.status_code}"
    except Exception as exc:
        return 0, None, f"pump {type(exc).__name__}"


async def _pump_activity(mint: str):
    return await asyncio.to_thread(_pump_activity_sync, mint)


async def pump_activity_text(client: httpx.AsyncClient, mint: str) -> str:
    del client
    status, body, note = await _pump_activity(mint)
    if status != 200 or not isinstance(body, dict):
        return note or f"Pump activity HTTP {status}"

    def one(key: str, label: str) -> str:
        block = body.get(key) if isinstance(body, dict) else None
        if not isinstance(block, dict) or block.get("priceChangePercent") is None:
            return f"{label} none"
        try:
            return f"{label} {float(block['priceChangePercent']):+.1f}%"
        except (TypeError, ValueError):
            return f"{label} none"

    return f"{one('5m', '5m')} · {one('1h', '1h')}"


async def pump_activity_pair(client: httpx.AsyncClient, mint: str):
    del client
    status, body, note = await _pump_activity(mint)
    if status != 200 or not isinstance(body, dict):
        return None, None, note or f"Pump activity HTTP {status}"

    def num(key: str):
        block = body.get(key) if isinstance(body, dict) else None
        if not isinstance(block, dict) or block.get("priceChangePercent") is None:
            return None
        try:
            return float(block["priceChangePercent"])
        except (TypeError, ValueError):
            return None

    m5, h1 = num("5m"), num("1h")
    return m5, h1, f"{_shown(m5, '5m')} · {_shown(h1, '1h')}"


_window_cache: dict = {}


async def window_line(client: httpx.AsyncClient, mint: str, s1=None) -> str:
    del client
    now = time.time()
    hit = _window_cache.get(mint)
    if hit and now - hit[0] < 5:
        return hit[1]
    m5 = h1 = None
    pump_note = ""
    status, body, pump_note = await _pump_activity(mint)
    if status == 200 and isinstance(body, dict):
        m5, h1 = _pct_num(body.get("5m")), _pct_num(body.get("1h"))
        pump_note = ""
    dex_s1 = s1
    m1 = None
    if dex_s1 is None:
        try:
            dex_s1 = await asyncio.wait_for(fetch_1s_pct(mint), timeout=2.0)
        except Exception:
            dex_s1 = None
    try:
        m1 = await asyncio.wait_for(fetch_m1_pct(mint), timeout=1.5)
    except Exception:
        m1 = None
    parts = [_shown(dex_s1, "1s"), _shown(m1, "1m"), _shown(m5, "5m"), _shown(h1, "1h")]
    if pump_note and m5 is None and h1 is None:
        parts.append(pump_note)
    text = " · ".join(parts)
    _window_cache[mint] = (now, text)
    if len(_window_cache) > 400:
        _window_cache.clear()
    return text
