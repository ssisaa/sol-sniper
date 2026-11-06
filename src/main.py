"""
SOL-SNIPER v1.2 – Entry Point

Modes:
  demo       – Hard filter demonstration (offline)
  live-rpc   – Connect to Solana mainnet HTTP RPC and show chain status
  live-feed  – Real-time WebSocket feed + full transaction decoding
  paper      – Paper trading scaffold
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone

from src.decision.hard_filters import HardFilterStack
from src.types.models import (
    AuthorityInfo,
    HolderSnapshot,
    LiquidityInfo,
    TokenCandidate,
    TokenMetadata,
    Verdict,
)

threading.stack_size(1024 * 1024)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sol-sniper")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
from src.observability.action_log import install_action_log
install_action_log()


def demo_hard_filters() -> None:
    filters = HardFilterStack()

    clean = TokenCandidate(
        mint="CleanMint1111111111111111111111111111111",
        metadata=TokenMetadata(
            mint="CleanMint1111111111111111111111111111111",
            name="Clean Meme",
            symbol="CLEAN",
            description="AI dog political meme",
        ),
        authorities=AuthorityInfo(mint_renounced=True, freeze_renounced=True),
        liquidity=LiquidityInfo(initial_sol=15.0, locked=True, lock_duration_days=180),
        holder_snapshot=HolderSnapshot(
            top5_pct=18.0, top10_pct=28.0, largest_non_lp_pct=4.5, unique_holders=210
        ),
        source_event="pump_graduation",
    )

    rug = TokenCandidate(
        mint="RugMint111111111111111111111111111111111",
        metadata=TokenMetadata(
            mint="RugMint111111111111111111111111111111111", symbol="RUG"
        ),
        authorities=AuthorityInfo(mint_renounced=False, freeze_renounced=True),
        liquidity=LiquidityInfo(initial_sol=8.0),
        source_event="raydium_pool",
    )

    log.info("=== SOL-SNIPER v1.2 Hard Filter Demo ===")
    for name, candidate in [("CLEAN", clean), ("RUG", rug)]:
        result = filters.evaluate(candidate)
        log.info(
            "token=%s verdict=%s size_mult=%.3f latency_ms=%.3f reasons=%s",
            name,
            result.verdict.value,
            result.size_multiplier,
            result.latency_ms,
            result.reasons,
        )
    log.info("Hard Filter Stack OK")


async def live_rpc_demo(extra_endpoints: list[str] | None = None) -> None:
    from src.data.rpc_client import create_default_client

    log.info("=== SOL-SNIPER v1.2 Live Solana RPC Connection ===")
    client = await create_default_client(extra_endpoints=extra_endpoints)
    try:
        health = await client.health_check_all()
        for url, info in health.items():
            short = url[:55] + "..." if len(url) > 55 else url
            status = "OK" if info["healthy"] else "FAIL"
            log.info("[%s] %s | slot=%s | %.0fms", status, short, info.get("slot"), info.get("latency_ms", 0))

        slot_resp = await client.get_slot()
        blockhash_resp = await client.get_latest_blockhash("processed")
        if not slot_resp.error:
            log.info("Current slot: %s (%.1fms)", slot_resp.result, slot_resp.latency_ms)
        if blockhash_resp.result:
            bh = blockhash_resp.result.get("value", {})
            log.info("Latest blockhash: %s...", str(bh.get("blockhash", ""))[:24])
        log.info("Live RPC OK")
    finally:
        await client.close()


async def dex_held(manager, mint: str) -> dict:
    """Name and price for an open mint. Pump only. DexScreener was the ConnectTimeout."""
    now = time.time()
    cache = getattr(manager, "_dex_cache", None) or {}
    manager._dex_cache = cache
    hit = cache.get(mint)
    if hit and now - hit.get("ts", 0) < 4:
        return hit
    out = {"symbol": "", "name": "", "m5": None, "price_usd": None, "ts": now}
    try:
        def _coin():
            from src.data.fast_get import pump_get
            return pump_get(
                f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
                timeout=0.6,
                cache_s=0.4,
            )
        body = await asyncio.wait_for(asyncio.to_thread(_coin), timeout=0.8)
        if isinstance(body, dict):
            out["symbol"] = body.get("symbol") or ""
            out["name"] = body.get("name") or ""
            try:
                out["price_usd"] = float(body.get("price_usd") or 0) or None
            except (TypeError, ValueError):
                out["price_usd"] = None
    except Exception:
        log.debug("held coin lookup failed mint=%s", mint[:8])
        if hit:
            return hit
    cache[mint] = out
    return out


async def launch_entry(mint: str) -> tuple[bool, str, float | None]:
    """Launch only. The 1-second move must be from +0.10% to +2.0%.

    The create feed already has the price. HTTP is only the backup.
    """
    from src.signal.dayscalp import _judge_second

    pct = None
    for _ in range(8):
        pct = launch_live_move(mint)
        if pct is not None:
            break
        await asyncio.sleep(0.2)
    if pct is None:
        import httpx
        from src.signal.dayscalp import _second_candle_up
        gap = launch_live_gap(mint)
        try:
            async with httpx.AsyncClient(timeout=4.0, headers={"User-Agent": "sol-sniper/1.2"}) as client:
                ok, why = await _second_candle_up(client, mint, "", floor=0.1, ceiling=2.0)
        except Exception as exc:
            return False, f"no 1s price — backup failed ({type(exc).__name__}) · {gap}", None
        try:
            pct = float(str(why).split()[-1].replace("%", ""))
        except (TypeError, ValueError):
            pct = None
        if not ok or pct is None or pct < 0.1 or pct > 2.0:
            text = str(why)
            if "1s candle missing" in text or text.startswith("no 1s price"):
                text = f"{text} · {gap}"
            return False, text, pct
        return True, f"1s {pct:+.2f}%", pct
    ok, why = _judge_second(pct, 0.1, 2.0)
    if not ok:
        return False, why, pct
    return True, f"1s {pct:+.2f}%", pct


async def launch_either(mint: str) -> tuple[bool, str, float | None]:
    """Buy when either door is open. Both are not required.

    Door 1: the 1-second move is +0.10% to +2.0%.
    Door 2: Pump 5m is +0.10% to +2.0% and that number equals the 1h percent.
    """
    import httpx
    from src.signal.dayscalp import pump_activity_pair

    why = "1s not in +0.10% to +2.0%"
    pct = None
    try:
        up, why, pct = await asyncio.wait_for(launch_entry(mint), timeout=8)
    except Exception as exc:
        up, why, pct = False, f"1s check failed ({type(exc).__name__})", None
    if up:
        return True, f"1s band · {why}", pct
    detail = "1s none · 1m none · 5m none · 1h none"
    m5 = h1 = None
    try:
        from src.signal.dayscalp import window_line
        async with httpx.AsyncClient(timeout=4.0) as client:
            m5, h1, _old = await asyncio.wait_for(pump_activity_pair(client, mint), timeout=5)
            detail = await asyncio.wait_for(window_line(client, mint, pct), timeout=8)
    except Exception as exc:
        detail = f"1s none · 1m none · 5m none · 1h none ({type(exc).__name__})"
    same = m5 is not None and h1 is not None and abs(float(m5) - float(h1)) <= 0.01
    if same and 0.10 <= float(m5) <= 2.0:
        return True, f"5m=1h band · {detail}", pct
    return False, f"neither · {detail}", pct


async def bull_entry(mint: str, min_m5: float = 0.0, max_m5: float | None = None) -> tuple[bool, str]:
    """Re-read the deepest pool right before the buy. Flat or down is not an entry."""
    import httpx
    from src.signal.dayscalp import still_rising

    try:
        async with httpx.AsyncClient(timeout=4.0, headers={"User-Agent": "sol-sniper/1.2"}) as client:
            ok, why = await still_rising(client, mint, min_m5, max_m5)
    except Exception:
        return False, "bull check failed — no buy"
    return bool(ok), str(why)


_launch_born: dict = {}
_launch_http: list = []
_launch_sub_q: list = []


def _launch_px(msg: dict) -> float:
    try:
        px = float(msg.get("marketCapSol") or 0)
    except (TypeError, ValueError):
        px = 0.0
    if px > 0:
        return px
    try:
        sol = float(msg.get("solAmount") or 0)
        tok = float(msg.get("tokenAmount") or 0)
    except (TypeError, ValueError):
        sol = tok = 0.0
    if sol > 0 and tok > 0:
        return sol / tok
    try:
        vsol = float(msg.get("vSolInBondingCurve") or 0)
        vtok = float(msg.get("vTokensInBondingCurve") or 0)
    except (TypeError, ValueError):
        return 0.0
    if vsol > 0 and vtok > 0:
        return vsol / vtok
    return 0.0


def _note_launch_px(mint: str, px: float) -> None:
    row = _launch_born.get(mint)
    if not row or px <= 0:
        return
    pts = row.setdefault("px", [])
    pts.append((time.time(), float(px)))
    if len(pts) > 40:
        del pts[:-40]


def launch_live_move(mint: str):
    """Percent from the create price to a trade about one second later. No HTTP."""
    row = _launch_born.get(mint)
    if not row:
        return None
    pts = [(t, p) for t, p in (row.get("px") or []) if p > 0]
    if len(pts) < 2:
        return None
    last_t, last_p = pts[-1]
    base = None
    for t, p in pts:
        span = last_t - t
        if 0.35 <= span <= 2.5:
            base = p
            break
    if base is None and last_t - pts[0][0] >= 0.35:
        base = pts[0][1]
    if not base or base <= 0:
        return None
    return (last_p - base) / base * 100.0


def launch_live_gap(mint: str) -> str:
    """What the create feed does not have yet. Shown in Decoded Tokens."""
    row = _launch_born.get(mint)
    if not row:
        return "create feed never saw this mint"
    pts = [(t, p) for t, p in (row.get("px") or []) if p > 0]
    if not pts:
        return "create arrived with no price"
    if len(pts) == 1:
        return "only the create price — no second trade yet"
    span = pts[-1][0] - pts[0][0]
    if span < 0.35:
        return f"second trade came {span:.2f}s after create — need 0.35s"
    return "create prices did not make a 1s move"


def _launch_born_rows() -> list:
    """Coins seen at the moment they were created. Age is counted from that moment."""
    now = time.time()
    dead = [mint for mint, row in _launch_born.items() if now - float(row.get("created") or 0) > 15]
    for mint in dead:
        _launch_born.pop(mint, None)
    out = []
    for row in _launch_born.values():
        item = dict(row)
        item["age_sec"] = max(0.0, now - float(row.get("created") or now))
        out.append(item)
    return out


async def _launch_birth_feed() -> None:
    """New coins and their next trades, on one free socket. Only while Launch is selected."""
    import json
    import websockets

    def _launch_selected() -> bool:
        try:
            from src.config.runtime import get_config
            cfg = get_config()
        except Exception:
            return False
        return getattr(cfg, "filter_mode", "") == "launch" or bool(getattr(cfg, "launch_enabled", False))

    while True:
        if not _launch_selected():
            await asyncio.sleep(5)
            continue
        try:
            async with websockets.connect(
                "wss://pumpportal.fun/api/data",
                open_timeout=15,
                ping_interval=20,
            ) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("LAUNCH create feed connected")
                while True:
                    if not _launch_selected():
                        break
                    extra = _launch_sub_q[:20]
                    del _launch_sub_q[:len(extra)]
                    if extra:
                        try:
                            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": extra}))
                        except Exception:
                            log.debug("launch trade subscribe failed", exc_info=True)
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=0.25)
                    except asyncio.TimeoutError:
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(msg, dict):
                        continue
                    mint = str(msg.get("mint") or "")
                    if len(mint) < 32:
                        continue
                    kind = str(msg.get("txType") or "").lower()
                    px = _launch_px(msg)
                    if kind == "create":
                        try:
                            liq = float(msg.get("vSolInBondingCurve") or 0)
                        except (TypeError, ValueError):
                            liq = 0.0
                        _launch_born[mint] = {
                            "mint": mint,
                            "symbol": msg.get("symbol") or "",
                            "name": msg.get("name") or "",
                            "liq_sol": liq,
                            "vol_usd": 0.0,
                            "source": "pump_fun",
                            "website": "",
                            "created": time.time(),
                            "px": [],
                        }
                        if mint not in _launch_sub_q:
                            _launch_sub_q.append(mint)
                        log.info("LAUNCH born %s %s", msg.get("symbol") or mint[:8], mint[:8])
                    if px > 0 and mint in _launch_born:
                        _note_launch_px(mint, px)
        except Exception as exc:
            if not _launch_selected():
                await asyncio.sleep(5)
                continue
            log.warning("LAUNCH create feed: %s", exc)
            await asyncio.sleep(3)


async def _launch_http_feed() -> None:
    global _launch_http
    while True:
        try:
            from src.config.runtime import get_config
            if get_config().filter_mode != "launch":
                await asyncio.sleep(5)
                continue
            _launch_http = await _launch_sources()
        except Exception:
            log.debug("launch http list failed", exc_info=True)
        await asyncio.sleep(8)


async def _launch_sources() -> list:
    """Newest pump.fun creates and new Solana pools. Used only while filter_mode is launch."""
    import httpx

    headers = {"User-Agent": "sol-sniper/1.2", "Accept": "application/json"}

    def as_sol(value) -> float:
        try:
            v = float(value or 0)
        except (TypeError, ValueError):
            return 0.0
        if v > 1_000_000:
            return v / 1_000_000_000
        return v

    def age_of(value) -> float | None:
        if value is None or value == "":
            return None
        try:
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace(".", "", 1).isdigit()):
                ts = float(value)
                if ts > 1_000_000_000_000:
                    ts /= 1000.0
                return max(0.0, time.time() - ts)
        except (TypeError, ValueError):
            pass
        try:
            text = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, time.time() - dt.timestamp())
        except (TypeError, ValueError):
            return None

    async def pump_rows(client: "httpx.AsyncClient") -> list:
        del client
        rows_out = []
        url = "https://frontend-api-v3.pump.fun/coins?offset=0&limit=20&sort=created_timestamp&order=DESC&includeNsfw=false"

        def pull() -> list:
            from src.data.fast_get import pump_get
            body = pump_get(url, timeout=1.5, cache_s=0.5)
            return body if isinstance(body, list) else []

        try:
            rows = await asyncio.to_thread(pull)
        except Exception:
            log.debug("pump latest list failed", exc_info=True)
            return rows_out
        for c in rows[:20]:
            if not isinstance(c, dict):
                continue
            mint = c.get("mint") or c.get("address") or ""
            if not mint:
                continue
            real = as_sol(c.get("real_sol_reserves"))
            virtual = as_sol(c.get("virtual_sol_reserves"))
            vol = 0.0
            for key in ("volume_5m_usd", "volume_m5_usd"):
                try:
                    vol = max(vol, float(c.get(key) or 0))
                except (TypeError, ValueError):
                    pass
            rows_out.append({
                "mint": mint,
                "symbol": c.get("symbol") or "",
                "name": c.get("name") or "",
                "liq_sol": real if real > 0 else virtual,
                "vol_usd": vol,
                "source": "pump_fun",
                "website": c.get("website") or "",
                "age_sec": age_of(c.get("created_timestamp") or c.get("created_at")),
            })
        return rows_out

    async def pool_rows(client: "httpx.AsyncClient") -> list:
        rows_out = []
        try:
            r = await client.get("https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1")
            if r.status_code != 200:
                return rows_out
            for row in (r.json() or {}).get("data") or []:
                attr = row.get("attributes") or {}
                rel = row.get("relationships") or {}
                dex = ((rel.get("dex") or {}).get("data") or {}).get("id") or ""
                if dex not in ("pump-fun", "pumpswap", "raydium", "raydium-clmm"):
                    continue
                base = ((rel.get("base_token") or {}).get("data") or {}).get("id") or ""
                mint = base.split("_", 1)[-1] if base else ""
                if not mint or mint.startswith("So111"):
                    continue
                try:
                    usd = float(attr.get("reserve_in_usd") or 0)
                except (TypeError, ValueError):
                    usd = 0.0
                vol_usd = 0.0
                raw_vol = attr.get("volume_usd")
                if isinstance(raw_vol, dict):
                    for key in ("m5",):
                        try:
                            vol_usd = max(vol_usd, float(raw_vol.get(key) or 0))
                        except (TypeError, ValueError):
                            pass
                else:
                    try:
                        vol_usd = float(raw_vol or 0)
                    except (TypeError, ValueError):
                        vol_usd = 0.0
                name = attr.get("name") or ""
                symbol = name.split("/")[0].strip() if "/" in name else name
                rows_out.append({
                    "mint": mint,
                    "symbol": symbol,
                    "name": name,
                    "liq_sol": usd / 150.0 if usd else 0.0,
                    "vol_usd": vol_usd,
                    "source": "raydium_pool" if "raydium" in dex else "pump_fun",
                    "website": "",
                    "age_sec": age_of(attr.get("pool_created_at")),
                })
        except Exception:
            log.debug("new pool scan failed", exc_info=True)
        return rows_out

    async def dex_rows(client: "httpx.AsyncClient") -> list:
        """Newest Pump pairs on DexScreener. Used when the Pump list is late or blocked."""
        rows_out = []
        try:
            r = await client.get("https://api.dexscreener.com/latest/dex/search?q=pumpfun")
            if r.status_code != 200:
                return rows_out
            for pair in (r.json() or {}).get("pairs") or []:
                if not isinstance(pair, dict):
                    continue
                if (pair.get("chainId") or "") != "solana":
                    continue
                if (pair.get("dexId") or "").lower() not in ("pumpfun", "pumpswap"):
                    continue
                mint = ((pair.get("baseToken") or {}).get("address")) or ""
                if not mint or not str(mint).lower().endswith("pump"):
                    continue
                base = pair.get("baseToken") or {}
                created = pair.get("pairCreatedAt")
                age = None
                try:
                    ts = float(created or 0)
                    if ts > 1_000_000_000_000:
                        ts /= 1000.0
                    if ts > 0:
                        age = max(0.0, time.time() - ts)
                except (TypeError, ValueError):
                    age = None
                liq = 0.0
                try:
                    usd = float((pair.get("liquidity") or {}).get("usd") or 0)
                    liq = usd / 150.0 if usd else 0.0
                except (TypeError, ValueError):
                    liq = 0.0
                vol = 0.0
                try:
                    vol = float((pair.get("volume") or {}).get("m5") or 0)
                except (TypeError, ValueError):
                    vol = 0.0
                rows_out.append({
                    "mint": mint,
                    "symbol": base.get("symbol") or "",
                    "name": base.get("name") or "",
                    "liq_sol": liq,
                    "vol_usd": vol,
                    "source": "pump_fun",
                    "website": pair.get("url") or "",
                    "age_sec": age,
                })
                if len(rows_out) >= 20:
                    break
        except Exception:
            log.debug("dex launch scan failed", exc_info=True)
        return rows_out

    headers = {"User-Agent": "sol-sniper/1.2", "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=4.0, headers=headers) as client:
        pump, dex = await asyncio.gather(pump_rows(client), dex_rows(client))
    by_mint = {}
    for row in list(pump) + list(dex):
        mint = row.get("mint") or ""
        if mint and mint not in by_mint:
            by_mint[mint] = row
    return list(by_mint.values())


async def live_feed_demo(
    duration: float = 60.0,
    max_decodes_per_min: int = 30,
    enable_ui: bool = True,
    trade_mode: str = "paper",
    file_cfg: dict | None = None,
) -> None:
    """
    Full pipeline:
      WS logs → decode tx → hard filters → paper/live buy → exit loop → UI
    """
    import random
    from pathlib import Path
    from src.data.rpc_client import create_default_client
    from src.data.ws_feed import SolanaWSFeed, FeedEventType, build_ws_endpoints
    from src.signal.tx_decoder import fetch_and_decode, decoded_to_candidate
    from src.signal.token_meta import fetch_token_meta
    from src.risk.position_manager import PositionManager, RiskConfig, exit_loop
    from src.config import get_config, update_config, snapshot_config, append_trade_journal, apply_scalp_profile
    from src.observability.ledger import get_ledger
    from src.observability.exec_history import get_exec_history, fetch_wallet_signatures
    from src.execution.trader import Trader, ExecMode
    from src.observability.dashboard import (
        start_ws_server,
        start_http_static_server,
        publish_decoded,
        publish_stats,
        publish_heartbeat,
        publish_alert,
        publish_positions,
        publish_wallet,
        publish_ledger,
        publish_exec_history,
        publish_trade,
        publish_config,
        DEFAULT_HTTP_PORT,
        DEFAULT_WS_PORT,
    )

    log.info("=== SOL-SNIPER v1.2 FULL PIPELINE (feed + decode + trade + exits) ===")
    if os.environ.get("SOL_SNIPER_JUST_RESTARTED") == "1":
        log.warning("SNIPER RESTART completed")
        try:
            from src.observability.action_log import record_action
            record_action("RESTART", level="WARNING", detail="SNIPER RESTART completed")
        except Exception:
            pass
    log.info(
        "Duration: %.0fs | max_decodes/min: %d | UI=%s | trade_mode=%s",
        duration, max_decodes_per_min, enable_ui, trade_mode,
    )

    http_server = None
    ws_task = None
    if enable_ui:
        http_server = start_http_static_server(DEFAULT_HTTP_PORT)
        ws_task = asyncio.create_task(start_ws_server(port=DEFAULT_WS_PORT))
        log.info("Open UI → http://localhost:%d/", DEFAULT_HTTP_PORT)

    rpc = await create_default_client()
    feed = SolanaWSFeed(ws_endpoints=build_ws_endpoints())
    log.info("WS endpoints: %s", [e.split("api-key=")[0] + ("***" if "api-key=" in e else "") for e in feed.endpoints[:4]])
    filters = HardFilterStack()

    risk_cfg = RiskConfig(
        max_position_sol=0.12,
        total_capital_sol=10.0,
        max_concurrent=5,
        time_stop_seconds=60.0,  # scalping: exit if no quick move
        hard_stop_pct=-0.35,
        trail_activate_pct=0.25,
        trail_drawdown_pct=0.15,
        tp_levels=(0.30, 0.80),
        tp_fractions=(0.50, 0.30),

    )
    manager = PositionManager(risk_cfg)
    restored = manager.restore_remote()
    if restored:
        log.warning("SQLite restored %d open position(s)", restored)
    try:
        from src.storage.mint_flags import load_flags
        dust = {m for m, flag in load_flags(force=True).items() if str(flag).upper() == "DUST"}
        led = get_ledger()
        closed = {e.mint for e in led.entries if e.side == "CLOSE"}
        open_now = {p.mint for p in manager.positions.values() if p.status == "OPEN"}
        for e in list(led.entries):
            if e.side != "OPEN" or e.mint not in dust or e.mint in closed or e.mint in open_now:
                continue
            led.record_close(
                id=f"dust-{e.mint[:8]}",
                mint=e.mint,
                symbol=e.symbol or e.mint[:8],
                mode=e.mode or "live",
                source=e.source or "",
                token_amount=float(e.token_amount or 0),
                entry_sol=float(e.entry_sol or 0),
                entry_fee_sol=float(e.entry_fee_sol or 0),
                entry_tip_sol=float(e.entry_tip_sol or 0),
                exit_sol=0.0,
                exit_fee_sol=0.0,
                hold_seconds=0.0,
                reason="dust_left",
                since_buy_pct=e.since_buy_pct,
                ath_since_pct=e.ath_since_pct,
                sale_label="Full Closed",
                group_id=e.id or e.mint,
                count_as_trade=True,
                m5_pct=e.m5_pct,
                m1_pct=e.m1_pct,
                s1_pct=e.s1_pct,
                h1_pct=e.h1_pct,
                price_usd=e.price_usd,
            )
            closed.add(e.mint)
            log.warning("LEDGER dust close written for %s — open had no close", e.symbol or e.mint[:8])
    except Exception:
        log.debug("dust ledger backfill failed", exc_info=True)
    try:
        from src.observability.dashboard import reconcile_openhold_status
        reconcile_openhold_status([
            {
                "mint": p.mint,
                "source": getattr(p, "source", "") or "",
                "strategy": getattr(p, "strategy", "") or "",
                "from_openholds": bool(getattr(p, "_from_openholds", False)),
            }
            for p in manager.positions.values()
            if p.status == "OPEN"
        ])
    except Exception:
        log.debug("openhold status restore failed", exc_info=True)
    exec_mode = ExecMode.PAPER if trade_mode != "live" else ExecMode.LIVE
    trader = Trader(manager, mode=exec_mode, risk=risk_cfg)
    if enable_ui:
        from src.observability.dashboard import set_wallet_busy_check

        def _wallet_busy() -> str:
            if getattr(trader, "_inflight_mint", ""):
                return "A buy is still sending. Wait, then change the wallet."
            return ""

        set_wallet_busy_check(_wallet_busy)

    async def _sell_handler(pos, frac, reason):
        fill = await trader.sell(pos.mint, pos.tokens * pos.remaining_frac, frac=frac, reason=reason)
        if not fill.ok:
            pos._last_sell_error = str(fill.error or "")
            pos._last_sell_error_at = time.time()
            log.warning("ACTION SELL_FAIL mint=%s reason=%s err=%s", pos.mint[:8], reason, fill.error)
            return False, None, None
        # A real stop can legitimately return a small SOL amount. Do not reopen it.
        stop_exit = reason in (
            "hard_stop", "pct_stop", "time_stop", "price_stop", "trailing_stop", "peak_giveback",
            "manual_now", "callout_age", "m5_null", "caller_closed", "caller_drop_7", "copy_stop",
        )
        if (
            not stop_exit
            and frac >= 0.99
            and fill.sol_in is not None
            and fill.sol_in < max(0.00005, pos.entry_sol * 0.05)
            and float(getattr(pos, "since_buy_pct", 0) or 0) > -50
        ):
            from src.execution.wallet_tokens import get_token_holding
            pk = pubkey_from_env()
            row = await get_token_holding(pk, pos.mint) if pk else None
            if row is not None and int(row.get("raw") or 0) <= 0:
                pos._last_sell_error = "wallet_token_zero"
                log.warning(
                    "FAKE OPEN mint=%s — dust sell and token is not in the wallet, dropping it",
                    pos.mint[:8],
                )
                return False, None, None
            log.error(
                "SELL suspicious sol_out=%.6f entry=%.6f mint=%s — keep OPEN (possible fake/partial)",
                fill.sol_in, pos.entry_sol, pos.mint[:8],
            )
            return False, None, fill.signature
        log.info("ACTION SELL mint=%s reason=%s sol≈%.6f sig=%s", pos.mint[:8], reason, fill.sol_in, (fill.signature or "")[:16])
        got = max(0.0, float(fill.sol_in or 0))
        pos._sol_out = max(0.0, float(getattr(pos, "_sol_out", 0) or 0)) + got
        fee_now = max(0.0, float(fill.fees_sol or 0))
        pos._exit_fee = max(0.0, float(getattr(pos, "_exit_fee", 0) or 0)) + fee_now
        sales = getattr(pos, "_sales", None)
        if sales is None:
            pos._sales = []
            sales = pos._sales
        sold = max(0.0, min(1.0, float(frac) * float(getattr(pos, "remaining_frac", 1) or 1)))
        mark = getattr(pos, "since_buy_pct", None)
        peak = getattr(pos, "peak_since_pct", None)
        ath = max(0.0, float(peak)) if peak is not None else 0.0
        if mark is not None:
            ath = max(ath, float(mark), 0.0)
        mark_m5 = mark_m1 = mark_s1 = mark_h1 = mark_px = None
        sales.append({
            "sol": got,
            "fee": fee_now,
            "frac": sold,
            "pct": None if mark is None else round(float(mark), 2),
            "ath": round(ath, 2),
            "sig": fill.signature or "",
            "reason": reason,
            "m5": mark_m5,
            "m1": mark_m1,
            "s1": mark_s1,
            "h1": mark_h1,
            "px": mark_px,
        })
        try:
            get_exec_history().record(
                side="SELL",
                mint=pos.mint,
                symbol=pos.mint[:8],
                sol_amount=fill.sol_in,
                token_amount=pos.tokens * frac,
                signature=fill.signature or "",
                status="confirmed",
                route=fill.route or "",
                reason=reason,
                mode=fill.mode,
            )
        except Exception:
            log.exception("exec history sell record failed")
        return True, fill.effective_price, fill.signature

    manager.set_sell_handler(_sell_handler)


    # --- Wallet / live arming diagnostics (safe: no secrets logged) ---
    from src.execution.signer import live_armed, pubkey_from_env, SignerError, load_keypair_from_env

    stats = {
        "events": 0,
        "decode_attempts": 0,
        "decode_success": 0,
        "mints_found": 0,
        "buy_signals": 0,
        "downsize_signals": 0,
        "skip_signals": 0,
        "paper_fills": 0,
        "fills": 0,
        "open_positions": 0,
        "closed_trades": 0,
        "win_rate": 0.0,
        "filter_mode": "strict",
        "trade_mode": trade_mode,
        "wallet_armed": False,
        "daily_pnl_sol": 0.0,
    }
    pipeline_seen: set[str] = set()

    def _note_pipeline(mints) -> None:
        """Keep Events, Decodes, and Mints moving from the scans, not only the log socket."""
        fresh = 0
        for mint in mints or []:
            mint = str(mint or "")
            if len(mint) < 32 or mint in pipeline_seen:
                continue
            pipeline_seen.add(mint)
            fresh += 1
        if len(pipeline_seen) > 8000:
            pipeline_seen.clear()
        seen_n = len([m for m in (mints or []) if m])
        stats["events"] = int(stats.get("events") or 0) + 1
        stats["decode_success"] = int(stats.get("decode_success") or 0) + seen_n
        stats["mints_found"] = int(stats.get("mints_found") or 0) + fresh
        if enable_ui:
            try:
                asyncio.get_running_loop().create_task(publish_stats(dict(stats)))
            except Exception:
                pass

    log.info("========== LIVE ARMING STATUS ==========")
    log.info("trade_mode=%s | Trader.mode=%s", trade_mode, trader.mode.value)
    log.info("SOL_SNIPER_LIVE=%r", os.environ.get("SOL_SNIPER_LIVE"))
    log.info("SOL_SNIPER_KEYPAIR=%s", os.environ.get("SOL_SNIPER_KEYPAIR") or "(not set)")
    log.info("SOL_SNIPER_PRIVATE_KEY=%s", "set" if os.environ.get("SOL_SNIPER_PRIVATE_KEY") else "(not set)")
    try:
        import solders  # noqa: F401
        log.info("solders: installed")
    except ImportError:
        log.warning("solders: NOT installed — live signing impossible (pip install solders)")
    if live_armed():
        pk = pubkey_from_env()
        stats["wallet_armed"] = True
        log.info("WALLET ARMED | pubkey=%s... | REAL txs can be sent", (pk or "")[:12])
    else:
        stats["wallet_armed"] = False
        log.warning(
            "WALLET NOT ARMED | buys will not broadcast. Need: SOL_SNIPER_LIVE=1 + keypair + solders"
        )
    
    log.info("========================================")

    def sync_runtime_to_components():
        cfg = get_config()
        # launch: paper always; live only with SOL_SNIPER_ALLOW_LAUNCH=1
        import os
        allow_launch = (
            cfg.filter_mode == "launch"
            and (
                trade_mode == "paper"
                or os.environ.get("SOL_SNIPER_ALLOW_LAUNCH", "").strip() in ("1", "true", "TRUE", "yes")
            )
        )
        allow_curve = (trade_mode == "paper" and cfg.filter_mode == "pump_curve")
        # Live without ALLOW_LAUNCH cannot use launch/pump_curve authority exceptions
        if trade_mode == "live" and not allow_launch and cfg.filter_mode in ("launch", "pump_curve"):
            log.warning(
                "Live + filter=%s without SOL_SNIPER_ALLOW_LAUNCH=1 → forcing strict authority checks",
                cfg.filter_mode,
            )
            effective_mode = "strict"
            allow_curve = False
            allow_launch = False
        else:
            effective_mode = cfg.filter_mode
        filters.set_mode(
            effective_mode,
            allow_pump_curve=allow_curve,
            allow_launch=allow_launch,
        )
        filters.min_liquidity_sol = cfg.min_liquidity_sol
        filters.pump_curve_min_liq_sol = cfg.pump_curve_min_liq_sol
        filters.pump_curve_size_mult = cfg.pump_curve_size_mult
        filters.launch_size_mult = getattr(cfg, "launch_size_mult", 0.5)
        manager.cfg.max_position_sol = cfg.max_position_sol
        manager.cfg.total_capital_sol = cfg.total_capital_sol
        manager.cfg.max_concurrent = cfg.max_concurrent
        if cfg.filter_mode in ("dayscalp", "launch"):
            try:
                from src.config.file_config import mode_exit_profile
                ds = mode_exit_profile(cfg.filter_mode)
                # A Live Controls edit already in runtime wins over the file
                # until the next switch. Apply writes the file, then this reads it.
                manager.cfg.hard_stop_pct = float(cfg.hard_stop_pct if cfg.hard_stop_pct is not None else ds["hard_stop_pct"])
                runner = getattr(cfg, "runner_stop_pct", None)
                if runner is None:
                    runner = ds["runner_stop_pct"]
                manager.cfg.runner_stop_pct = float(runner)
                manager.cfg.exit_style = getattr(cfg, "exit_style", "plan") or "plan"
                secs = getattr(cfg, "time_stop_seconds", None)
                manager.cfg.time_stop_seconds = float(ds["time_stop_seconds"] if secs is None else secs)
                manager.cfg.time_stop_min_gain = float(cfg.time_stop_min_gain if cfg.time_stop_min_gain is not None else ds["time_stop_min_gain"])
                manager.cfg.trail_activate_pct = float(cfg.trail_activate_pct if cfg.trail_activate_pct is not None else ds["trail_activate_pct"])
                manager.cfg.trail_drawdown_pct = float(cfg.trail_drawdown_pct if cfg.trail_drawdown_pct is not None else ds["trail_drawdown_pct"])
                manager.cfg.tp_levels = tuple(cfg.tp_levels or ds["tp_levels"])
                manager.cfg.tp_fractions = tuple(cfg.tp_fractions or ds["tp_fractions"])
                manager.cfg.fallback_arm_pct = float(getattr(cfg, "fallback_arm_pct", 0) or 0)
                manager.cfg.max_concurrent = 1
                floor = float(manager.cfg.hard_stop_pct)
                for pos in manager.positions.values():
                    if pos.status != "OPEN":
                        continue
                    if (
                        getattr(pos, "source", "") in ("predict", "callout", "opencopy")
                        or getattr(pos, "strategy", "") in ("predict", "callout", "opencopy")
                        or bool(getattr(pos, "quick_entry", False))
                    ):
                        if getattr(pos, "source", "") in ("predict", "callout", "opencopy"):
                            pos.strategy = pos.source
                        pos.stop_pct = None
                        continue
                    pos.strategy = cfg.filter_mode if pos.source != "dayscalp" or cfg.filter_mode == "dayscalp" else pos.strategy
                    if cfg.filter_mode == "launch" and pos.source != "dayscalp":
                        pos.strategy = "launch"
                    if cfg.filter_mode == "dayscalp" and pos.source == "dayscalp":
                        pos.strategy = "dayscalp"
                        if not pos.tp_ladder_hit:
                            pos.stop_pct = floor
            except Exception:
                log.debug("exit profile overlay failed", exc_info=True)
        else:
            manager.cfg.hard_stop_pct = cfg.hard_stop_pct
            manager.cfg.time_stop_seconds = cfg.time_stop_seconds
            manager.cfg.time_stop_min_gain = cfg.time_stop_min_gain
            manager.cfg.trail_activate_pct = cfg.trail_activate_pct
            manager.cfg.trail_drawdown_pct = cfg.trail_drawdown_pct
            manager.cfg.tp_levels = tuple(cfg.tp_levels)
            manager.cfg.tp_fractions = tuple(cfg.tp_fractions)
            manager.cfg.fallback_arm_pct = float(getattr(cfg, "fallback_arm_pct", 0) or 0)
        trader.tip_model.max_edge_ratio = cfg.max_edge_ratio
        trader.tip_model.absolute_max_tip_sol = cfg.absolute_max_tip_sol
        trader.tip_model.base_tip_sol = cfg.base_tip_sol
        labels = []
        if getattr(cfg, "launch_enabled", False):
            labels.append("launch")
        if getattr(cfg, "dayscalp_enabled", False):
            labels.append("dayscalp")
        if getattr(cfg, "predict_enabled", False):
            labels.append("predict")
        if getattr(cfg, "callout_enabled", False):
            labels.append("callout")
        if getattr(cfg, "opencopy_enabled", False) or getattr(cfg, "filter_mode", "") == "opencopy":
            labels.append("opencopy")
        stats["filter_mode"] = " + ".join(labels) if labels else cfg.filter_mode
        stats["trade_mode"] = trade_mode
        return cfg

    def on_config_update(data):
        before = get_config().filter_mode
        data = dict(data or {})
        reset = bool(data.pop("reset_profile", False))
        load_style = bool(data.pop("load_exit_style", False))
        preset_name = str(data.pop("preset_name", "") or "").strip()
        save_preset = bool(data.pop("save_preset", False))
        mode = data.get("filter_mode") or before
        exit_keys = (
            "hard_stop_pct", "runner_stop_pct", "time_stop_seconds", "time_stop_min_gain",
            "trail_activate_pct", "trail_drawdown_pct", "tp_levels", "tp_fractions",
            "fallback_arm_pct",
        )
        if reset or load_style:
            from src.config.file_config import profile_for_ui, style_profile
            if load_style:
                style = data.get("exit_style") or "ath"
                prof = style_profile(style)
                data = {**prof, "exit_style": prof.get("exit_style") or style}
                if "filter_mode" not in prof:
                    data["filter_mode"] = mode
                log.info("Exit style %s loaded. Dayscalp scan range is unchanged.", data["exit_style"])
            else:
                data = profile_for_ui(mode)
                log.info("Load Profile — %s restored from file defaults (yaml not changed)", mode)
        else:
            switching = "filter_mode" in data and not any(k in data for k in exit_keys)
            if mode in ("launch", "dayscalp") and switching:
                from src.config.file_config import mode_exit_profile
                data = {**mode_exit_profile(mode), **data}
                log.info("Live Controls %s — loaded that mode's exit plan", mode)
            # Apply changes the running bot only. sniper.yaml stays the reset point.
        changed = update_config(data)
        cfg = sync_runtime_to_components()
        try:
            from src.signal.dayscalp import set_rugcheck_enabled
            set_rugcheck_enabled(bool(getattr(cfg, "rugcheck_enabled", False)))
        except Exception:
            pass
        if save_preset:
            from src.config.file_config import BUILTIN_STYLES, preset_slug, save_user_preset
            selected = str(getattr(cfg, "exit_style", "") or "")
            typed = preset_slug(preset_name)
            if selected.lower() not in BUILTIN_STYLES:
                if typed and typed.lower() not in BUILTIN_STYLES and typed != selected:
                    target = typed
                else:
                    target = selected
                saved_id = save_user_preset(target, snapshot_config())
                if saved_id and saved_id != selected:
                    update_config({"exit_style": saved_id})
                    cfg = sync_runtime_to_components()
                log.info("Custom style '%s' updated. Built-in defaults were not changed.", saved_id or target)
            elif typed and typed.lower() not in BUILTIN_STYLES:
                saved_id = save_user_preset(typed, snapshot_config())
                if saved_id:
                    update_config({"exit_style": saved_id})
                    cfg = sync_runtime_to_components()
                log.info("Saved new style '%s'. Built-in %s was not overwritten.", saved_id or typed, selected)
            else:
                log.info(
                    "Built-in %s was not overwritten. These numbers are saved for the next restart.",
                    selected,
                )
        if "tx_route" in changed:
            log.warning("TX ROUTE %s — next buy and sell use this, then Jupiter only if it returns no signature", cfg.tx_route)
        if "fast_pace_sell" in changed or "fast_pace_margin_pct" in changed or "fast_pace_max_sol" in changed:
            if getattr(cfg, "fast_pace_sell", False):
                log.warning(
                    "FAST PACE on — sell 100%% at +%.2f%% for every source. Max position %.4f SOL is the full-size target only.",
                    float(getattr(cfg, "fast_pace_margin_pct", 5) or 5),
                    float(getattr(cfg, "fast_pace_max_sol", 0) or cfg.max_position_sol or 0),
                )
            else:
                log.warning("FAST PACE off — existing take profit and trail are unchanged")
        if "filter_mode" in changed:
            log.warning(
                "FILTER SWITCH %s → %s (Live Controls). sniper.yaml is only the startup default.",
                before, cfg.filter_mode,
            )
        log.info(
            "UI config update %s → filter_mode=%s trade_enabled=%s",
            changed,
            cfg.filter_mode,
            cfg.trade_enabled,
        )
        if enable_ui:
            asyncio.create_task(publish_config(snapshot_config()))
            asyncio.create_task(publish_stats(stats))

    if enable_ui:
        from src.observability.dashboard import bus as dash_bus
        dash_bus.set_config_handler(on_config_update)

        async def _restart_process(reason: str) -> None:
            if getattr(_restart_process, "started", False):
                log.warning("SNIPER RESTART already in progress")
                return
            _restart_process.started = True
            import os
            import subprocess
            from pathlib import Path
            from src.config.runtime import save_runtime_state
            from src.observability.action_log import record_action
            log.warning("SNIPER RESTART started")
            try:
                record_action("RESTART", level="WARNING", detail="SNIPER RESTART started")
                save_runtime_state(snapshot_config())
                try:
                    manager.save_restart_hold()
                except Exception:
                    log.exception("RESTART snapshot failed")
                cfg_now = get_config()
                log.warning(
                    "SNIPER RESTART started — keeping filter=%s style=%s trade=%s max_pos=%.4f SOL (%s)",
                    cfg_now.filter_mode,
                    getattr(cfg_now, "exit_style", "plan"),
                    cfg_now.trade_enabled,
                    float(cfg_now.max_position_sol or 0),
                    reason,
                )
                root = Path(__file__).resolve().parents[1]
                cmd = [sys.executable, "-m", "src.main", *sys.argv[1:]]
                env = os.environ.copy()
                env["SOL_SNIPER_JUST_RESTARTED"] = "1"
                subprocess.Popen(
                    ["bash", "-c", "sleep 0.2; exec \"$@\"", "sniper-restart", *cmd],
                    cwd=str(root),
                    env=env,
                    start_new_session=True,
                    stdin=subprocess.DEVNULL,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                )
            except Exception:
                _restart_process.started = False
                log.exception("SNIPER RESTART failed")
                record_action("RESTART", level="ERROR", detail="SNIPER RESTART failed")
                return
            log.warning("SNIPER RESTART handing over — this process will exit")
            # Do not shut the server down from here. shutdown() waits, the port stays
            # taken, and the page sits on RESTARTING. The reply is already sent.
            def _leave() -> None:
                time.sleep(0.15)
                os._exit(0)
            threading.Thread(target=_leave, name="restart-exit", daemon=True).start()

        async def _auto_restart() -> None:
            try:
                await asyncio.sleep(_AUTO_RESTART_MIN * 60)
            except asyncio.CancelledError:
                return
            if _AUTO_RESTART_MIN <= 0:
                return
            if not _hold["on"] or hold_path.is_file():
                log.warning("AUTO RESTART skipped — bot is stopped")
                return
            trader._restart_hold = True
            log.warning("AUTO RESTART due — saving the open trade, then reloading. No new buy while it reloads.")
            deadline = time.time() + 45
            while time.time() < deadline:
                if not _hold["on"] or hold_path.is_file():
                    trader._restart_hold = False
                    log.warning("AUTO RESTART cancelled — bot was stopped")
                    return
                inflight = getattr(trader, "_inflight_mint", "") or ""
                if not inflight:
                    break
                log.info("AUTO RESTART waiting — buy still sending %s", inflight[:8])
                await asyncio.sleep(2)
            if not _hold["on"] or hold_path.is_file():
                trader._restart_hold = False
                log.warning("AUTO RESTART cancelled — bot was stopped")
                return
            open_now = [p for p in manager.positions.values() if p.status == "OPEN"]
            name = (getattr(open_now[0], "symbol", "") or open_now[0].mint[:8]) if open_now else "flat"
            try:
                manager.save_restart_hold()
            except Exception:
                log.exception("AUTO RESTART snapshot failed — reloading anyway")
            try:
                from src.observability.action_log import record_action
                record_action(
                    "RESTART",
                    level="WARNING",
                    detail=f"AUTO RESTART every 30 min — saved {name}, reloading even with an open trade",
                )
            except Exception:
                pass
            log.warning("AUTO RESTART — reloading now. Saved %s.", name)
            await _restart_process("30min")

        dash_bus.set_restart_handler(lambda: _restart_process("button"))
        from src.observability.dashboard import set_http_restart, set_http_clear, set_http_run, set_http_sell
        _loop = asyncio.get_running_loop()
        set_http_restart(lambda: asyncio.run_coroutine_threadsafe(_restart_process("button"), _loop))
        _AUTO_RESTART_MIN = 0  # Off. The Restart button still reloads. Set this to 30 to arm it again.
        _hold = {"task": None, "on": True}
        hold_path = Path(__file__).resolve().parents[1] / "logs" / "bot_stopped"

        def _arm_auto() -> None:
            if _AUTO_RESTART_MIN <= 0:
                return
            old = _hold.get("task")
            if old and not old.done():
                old.cancel()
            _hold["task"] = asyncio.create_task(_auto_restart())
            log.warning("AUTO RESTART armed — reload every %s min, even if a trade is open", _AUTO_RESTART_MIN)

        async def _set_running(start: bool):
            if start:
                if hold_path.is_file():
                    hold_path.unlink()
                _hold["on"] = True
                update_config({"trade_enabled": True})
                _arm_auto()
                log.warning("START — trading on, scans on. The page shows RUNNING.")
            else:
                _hold["on"] = False
                hold_path.parent.mkdir(parents=True, exist_ok=True)
                hold_path.write_text("stopped\n", encoding="utf-8")
                old = _hold.get("task")
                if old and not old.done():
                    old.cancel()
                update_config({"trade_enabled": False})
                log.warning("STOP — trading off, scans off, auto-restart off. The page shows STOPPED. Press START to run again.")
            if enable_ui:
                await publish_config(snapshot_config())
                await publish_stats(stats)
            return {"ok": True, "running": start}

        set_http_run(lambda start: asyncio.run_coroutine_threadsafe(_set_running(bool(start)), _loop))

        async def _sell_now(mint: str = "", force: bool = False):
            mint = str(mint or "").strip()
            if mint:
                result = await manager.sell_mint_now(mint, force=bool(force))
                label = f"Manual sell {mint[:8]}"
            else:
                source = str(get_config().filter_mode or "").strip().lower()
                result = await manager.sell_source_now(source)
                label = f"Manual -1 {source}"
            if enable_ui:
                try:
                    await publish_positions(manager.snapshot())
                except Exception:
                    log.debug("manual sell publish failed", exc_info=True)
            try:
                from src.observability.action_log import record_action
                record_action(
                    "SELL",
                    level="WARNING",
                    detail=(
                        f"{label} — sold {int(result.get('sold') or 0)}"
                        + (f" · {result.get('error')}" if result.get("error") else "")
                    )[:220],
                )
            except Exception:
                log.debug("manual sell log failed", exc_info=True)
            log.warning("%s — %s", label, result)
            return result

        def _sell_click(mint="", force=False):
            lane = get_lane()
            loop = lane.loop if lane.started and lane.loop is not None else _loop
            log.info("FAST LANE sell click — not waiting for the scan")
            return asyncio.run_coroutine_threadsafe(_sell_now(mint, bool(force)), loop)

        set_http_sell(_sell_click)

        async def _openhold_buy_now(order: dict) -> None:
            """A clicked Buy. Starts on the loop immediately. It does not wait for the scan."""
            from src.observability.dashboard import note_openhold_seen, set_openhold_status
            mint = str((order or {}).get("coinMint") or "")
            symbol = (order or {}).get("symbol") or (mint[:6] if mint else "Coin")
            if not mint:
                return
            if getattr(_openhold_buy_now, "busy", False):
                log.warning("OPENHOLDS %s — buy already sending", symbol)
                return
            _openhold_buy_now.busy = True
            try:
                set_openhold_status(mint, "BUYING")
                note_openhold_seen(str((order or {}).get("userName") or ""), mint)
                log.info("OPENHOLDS %s — clicked buy. Sending now.", symbol)
                copy_on = (
                    bool(getattr(get_config(), "opencopy_enabled", False))
                    or str(getattr(get_config(), "filter_mode", "") or "") == "opencopy"
                )
                buy_source = "opencopy" if copy_on else "callout"
                try:
                    fill = await trader.buy(mint=mint, size_multiplier=1.0, source=buy_source, ref_price=1.0)
                    err = "" if (fill and fill.ok) else str(getattr(fill, "error", None) or "buy_failed")
                except Exception as exc:
                    fill = None
                    err = f"{type(exc).__name__}: {exc}"[:180]
                    log.exception("OPENHOLDS buy crashed mint=%s", mint[:8])
                if not (fill and fill.ok):
                    set_openhold_status(mint, "MISSED")
                    log.warning("OPENHOLDS MISSED %s — %s", symbol, err or "buy_failed")
                    return
                tagged = False
                for pos in manager.positions.values():
                    if pos.status != "OPEN" or pos.mint != mint:
                        continue
                    pos.strategy = buy_source
                    pos.source = buy_source
                    pos.symbol = symbol
                    pos.name = (order or {}).get("name") or ""
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
                    pos._from_openholds = True
                    pos.callout_user = str((order or {}).get("userName") or "")
                    pos.callout_wallet = str((order or {}).get("wallet") or "")
                    pos.user_holds = "checking"
                    manager.set_price(mint, 1.0)
                    tagged = True
                    break
                if not tagged:
                    set_openhold_status(mint, "MISSED")
                    log.warning("OPENHOLDS MISSED %s — fill did not open a position", symbol)
                    return
                set_openhold_status(mint, "HOLDING")
                try:
                    from src.observability.ledger import get_ledger
                    get_ledger().record_open(
                        id=f"oh-{mint[:8]}-{int(time.time())}",
                        mint=mint,
                        symbol=symbol,
                        mode=fill.mode,
                        source=buy_source,
                        token_amount=fill.tokens_out,
                        entry_sol=fill.sol_in,
                        entry_fee_sol=fill.fees_sol,
                        entry_tip_sol=0.0,
                        price_sol_per_token=fill.effective_price,
                        signature=fill.signature or "",
                        since_buy_pct=0.0,
                        ath_since_pct=0.0,
                        name=(order or {}).get("name") or "",
                        callout_user=str((order or {}).get("userName") or ""),
                    )
                except Exception:
                    log.exception("openholds ledger open failed")
                log.info("OPENHOLDS HOLDING %s sol=%.4f — exit rules unchanged", symbol, fill.sol_in)
            finally:
                _openhold_buy_now.busy = False

        from src.observability.dashboard import set_http_openhold, set_http_opensell
        from src.execution.fast_lane import get_lane

        def _on_lane(coro):
            lane = get_lane()
            loop = lane.loop if lane.started and lane.loop is not None else _loop
            log.info("FAST LANE click handed off — not waiting for the scan")
            asyncio.run_coroutine_threadsafe(coro, loop)

        set_http_openhold(lambda order: _on_lane(_openhold_buy_now(order)))
        set_http_opensell(lambda order: _on_lane(_openhold_sell_now(order)))

        async def _openhold_sell_now(order: dict) -> None:
            mint = str((order or {}).get("coinMint") or "")
            symbol = (order or {}).get("symbol") or (mint[:6] if mint else "Coin")
            if not mint:
                return
            log.warning("OPENHOLDS %s — clicked sell. Sending now.", symbol)
            try:
                result = await manager.sell_mint_now(mint, force=True)
            except Exception as exc:
                log.exception("OPENHOLDS sell crashed mint=%s", mint[:8])
                result = {"ok": False, "error": type(exc).__name__}
            if result.get("ok"):
                log.warning("OPENHOLDS sold %s", symbol)
            else:
                log.warning("OPENHOLDS sell %s — %s", symbol, result.get("error") or "not sold")

        async def _clear_books(what: str):
            from src.observability.ledger import get_ledger
            led = get_ledger()
            if what == "30d":
                board = led.clear_30d()
                log.warning("CLEAR 30-day PnL set to 0 — total left as it was")
            elif what == "total":
                from src.storage.invested import clear_invested
                clear_invested()
                board = led.clear_total()
                log.warning("CLEAR total PnL set to 0 and invested capital cleared — 30-day left as it was")
            else:
                # A saved coin can sit here with no wallet. Clear drops it. It is not a sell.
                dropped = [p.mint for p in manager.positions.values() if p.status == "OPEN"]
                for pid in [p.id for p in manager.positions.values() if p.status == "OPEN"]:
                    manager.positions.pop(pid, None)
                manager._ds_held = set()
                try:
                    trader._save_holds(set())
                except Exception:
                    log.exception("clear holds failed")
                try:
                    manager._push_remote()
                except Exception:
                    log.exception("clear book push failed")
                board = led.clear_session()
                manager.closed.clear()
                manager.daily_pnl_sol = 0.0
                manager.consecutive_losses = 0
                stats["closed_trades"] = 0
                stats["win_rate"] = 0.0
                stats["daily_pnl_sol"] = 0.0
                stats["buy_signals"] = 0
                stats["fills"] = 0
                stats["skip_signals"] = 0
                stats["paper_fills"] = 0
                stats["open_positions"] = 0
                from src.observability.action_log import clear_session_logs, record_action
                from src.observability.exec_history import get_exec_history
                from src.storage.supabase_store import get_store
                clear_session_logs()
                get_exec_history().clear()
                get_store().clear_session_rows()
                if enable_ui:
                    dash_bus._actions = []
                    dash_bus._exec_history = []
                    await dash_bus.publish({"type": "actions_cleared"})
                    await publish_exec_history([])
                    await publish_positions(manager.snapshot())
                record_action("SESSION", level="WARNING", detail="Session cleared. Open coin, logs, and database wiped. Wallet keypair kept.")
                log.warning(
                    "CLEAR session — dropped %s. Logs and database wiped. Wallet keypair kept. 30-day and total kept",
                    ",".join(m[:8] for m in dropped) or "nothing",
                )
            if enable_ui:
                summary = led.summary()
                if what not in ("30d", "total"):
                    summary = {**summary, "cleared": True}
                await publish_ledger(led.recent(250), summary)
                await publish_stats({**stats, **board})
            return {"ok": True, "board": board, "what": what}

        set_http_clear(lambda what: asyncio.run_coroutine_threadsafe(_clear_books(what), _loop))
        if hold_path.is_file():
            update_config({"trade_enabled": False})
            log.warning("Bot is STOPPED — press START to run again")
        else:
            _arm_auto()
            if _AUTO_RESTART_MIN <= 0:
                log.info("AUTO RESTART off — it will not reload by itself. Use the Restart button.")

    # Respect the filter saved in Live Controls. The yaml value is only the
    # first-run default. A restart must put the same set back, not a new one.
    import os as _os
    if _os.environ.get("SOL_SNIPER_SETTINGS_RESTORED") == "1":
        log.info(
            "Restart kept the saved set — filter=%s trade=%s max_pos=%.4f callout=%s",
            get_config().filter_mode,
            get_config().trade_enabled,
            float(get_config().max_position_sol or 0),
            get_config().callout_enabled,
        )
    else:
        configured = (get_config().filter_mode or "").strip()
        if not configured:
            configured = (_os.environ.get("SOL_SNIPER_FILTER_MODE") or "").strip()
        if configured == "dayscalp":
            update_config({"filter_mode": "dayscalp", "trade_enabled": True})
            log.info("Strategy: DAYSCALP (DexScreener 5m momentum — launch sniper feed still available)")
        elif trade_mode == "paper" and configured in ("", "strict"):
            update_config({"filter_mode": "launch", "trade_enabled": True, "max_position_sol": 0.08})
            log.info("Paper AUTO-SNIPE: filter_mode=launch (buy pump creates even if mint auth SET)")
        elif trade_mode == "live" and configured in ("", "strict"):
            if _os.environ.get("SOL_SNIPER_ALLOW_LAUNCH", "").strip() in ("1", "true", "TRUE", "yes"):
                update_config({"filter_mode": "launch", "trade_enabled": True})
                log.warning("Live AUTO-SNIPE: LAUNCH enabled via SOL_SNIPER_ALLOW_LAUNCH=1 (higher rug risk)")
            else:
                update_config({"filter_mode": "strict", "trade_enabled": True})
                log.info("Live auto-trade: strict (set allow_launch in config for launch snipes)")
        else:
            update_config({"filter_mode": configured or "launch", "trade_enabled": True})
            log.info("Using configured filter_mode=%s", configured or "launch")

    if (Path(__file__).resolve().parents[1] / "logs" / "bot_stopped").is_file():
        update_config({"trade_enabled": False})
        log.warning("Bot is STOPPED — no buys until you press START")

    if get_config().filter_mode == "scalp" and _os.environ.get("SOL_SNIPER_SETTINGS_RESTORED") != "1":
        apply_scalp_profile()
    sync_runtime_to_components()
    try:
        from src.config.runtime import save_runtime_state
        save_runtime_state()
    except Exception:
        log.debug("could not store the live settings", exc_info=True)
    if enable_ui:
        await publish_config(snapshot_config())

    async def _metaplex_name(mint: str):
        """Read name and symbol from the token's on-chain metadata."""
        import base64
        import httpx
        from solders.pubkey import Pubkey
        meta_prog = Pubkey.from_string("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
        pda, _ = Pubkey.find_program_address(
            [b"metadata", bytes(meta_prog), bytes(Pubkey.from_string(mint))],
            meta_prog,
        )
        urls = []
        try:
            from src.config.file_config import load_config
            rpc = (load_config() or {}).get("rpc") or {}
            if rpc.get("primary"):
                urls.append(rpc["primary"])
            urls.extend(rpc.get("urls") or [])
        except Exception:
            pass
        urls.append("https://api.mainnet-beta.solana.com")
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
            "params": [str(pda), {"encoding": "base64"}],
        }
        raw = b""
        async with httpx.AsyncClient(timeout=8.0) as client:
            for url in urls:
                try:
                    r = await client.post(url, json=payload)
                    if r.status_code != 200:
                        continue
                    val = ((r.json() or {}).get("result") or {}).get("value") or {}
                    data = (val.get("data") or [None])[0]
                    if data:
                        raw = base64.b64decode(data)
                        break
                except Exception:
                    continue
        if len(raw) < 70:
            return "", ""

        def _read_str(buf, i):
            if i + 4 > len(buf):
                return "", i
            n = int.from_bytes(buf[i:i + 4], "little")
            i += 4
            if n > 200 or i + n > len(buf):
                return "", i
            return buf[i:i + n].decode("utf-8", "ignore").strip("\x00").strip(), i + n

        name, i = _read_str(raw, 65)
        sym, _ = _read_str(raw, i)
        return name, sym

    async def _dex_held(mint: str) -> dict:
        now = _time.time()
        cache = getattr(manager, "_dex_cache", None) or {}
        manager._dex_cache = cache
        hit = cache.get(mint)
        if hit and now - hit.get("ts", 0) < 4:
            return hit
        out = {"symbol": "", "name": "", "m5": None, "price_usd": None, "ts": now}
        try:
            import httpx
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
            if r.status_code == 200:
                best = None
                for pair in (r.json() or {}).get("pairs") or []:
                    if (pair.get("chainId") or "") not in ("", "solana"):
                        continue
                    liq = float(((pair.get("liquidity") or {}).get("usd")) or 0)
                    if best is None or liq > best[0]:
                        best = (liq, pair)
                if best:
                    pair = best[1]
                    base = pair.get("baseToken") or {}
                    quote = pair.get("quoteToken") or {}
                    side = base if base.get("address") == mint else quote if quote.get("address") == mint else base
                    out["symbol"] = side.get("symbol") or ""
                    out["name"] = side.get("name") or ""
                    m5 = (pair.get("priceChange") or {}).get("m5")
                    out["m5"] = float(m5) if m5 is not None else None
                    try:
                        out["price_usd"] = float(pair.get("priceUsd") or 0) or None
                    except (TypeError, ValueError):
                        out["price_usd"] = None
                    if side is quote and out["price_usd"] and base.get("address") == mint:
                        pass
        except Exception:
            log.debug("held token dex lookup failed", exc_info=True)
        if not out["symbol"] or out.get("m5") is None:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=8.0) as client:
                    for url in (
                        f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
                    ):
                        try:
                            r = await client.get(url)
                        except Exception:
                            continue
                        if r.status_code != 200:
                            continue
                        body = r.json() or {}
                        out["symbol"] = body.get("symbol") or out["symbol"]
                        out["name"] = body.get("name") or out["name"]
                        if out.get("m5") is None and body.get("price_change_5m") is not None:
                            try:
                                out["m5"] = float(body.get("price_change_5m"))
                            except (TypeError, ValueError):
                                pass
                        if not out.get("price_usd"):
                            try:
                                mc = float(body.get("usd_market_cap") or 0)
                                supply = float(body.get("total_supply") or 0)
                                if mc > 0 and supply > 0:
                                    out["price_usd"] = mc / supply
                            except (TypeError, ValueError):
                                pass
                        if out["symbol"]:
                            break
                    if not out["symbol"]:
                        r = await client.get(f"https://lite-api.jup.ag/tokens/v2/search?query={mint}")
                        if r.status_code == 200:
                            rows = r.json() or []
                            if isinstance(rows, dict):
                                rows = rows.get("tokens") or rows.get("data") or []
                            for row in rows:
                                if (row.get("id") or row.get("address") or "") == mint:
                                    out["symbol"] = row.get("symbol") or out["symbol"]
                                    out["name"] = row.get("name") or out["name"]
                                    break
            except Exception:
                log.debug("token name lookup failed", exc_info=True)
        if not out["symbol"]:
            try:
                name, sym = await _metaplex_name(mint)
                out["name"] = name or out["name"]
                out["symbol"] = sym or out["symbol"]
            except Exception:
                log.debug("metaplex name failed", exc_info=True)
        named = getattr(manager, "_named", None)
        if named is None:
            manager._named = set()
            named = manager._named
        if mint not in named:
            if out["symbol"]:
                log.info("TOKEN %s (%s) mint=%s m5=%s", out["symbol"], out["name"] or "", mint[:8], out.get("m5"))
            else:
                log.info("TOKEN name missing mint=%s", mint[:8])
            named.add(mint)
        cache[mint] = out
        return out

    async def _sync_held_token() -> None:
        """If the wallet holds a real token, keep it as the one open position and refresh its live %."""
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_accounts
        from src.risk.position_manager import PositionMode
        pk = pubkey_from_env()
        if not pk:
            return
        try:
            accounts = await asyncio.wait_for(get_token_accounts(pk), timeout=8.0)
        except Exception:
            return
        if not accounts:
            return
        best = None
        held_mints = set()
        for acc in accounts:
            ui = float(acc.get("ui_amount") or 0)
            raw = int(acc.get("raw") or 0)
            mint_i = acc.get("mint") or ""
            if raw > 0 and mint_i:
                held_mints.add(mint_i)
            if ui < 1.0 and raw < 1000:
                continue
            if best is None or ui > best["ui"]:
                best = {"mint": mint_i, "ui": ui, "raw": raw}
        for pos in list(manager.positions.values()):
            if getattr(pos, "status", "") != "OPEN":
                continue
            if pos.mint in held_mints:
                continue
            log.warning(
                "FAKE dropped %s — not in the wallet. Ledger open removed. PnL not touched.",
                (getattr(pos, "symbol", "") or pos.mint[:8]),
            )
            manager.positions.pop(pos.id, None)
            try:
                from src.observability.ledger import get_ledger
                get_ledger().drop_unfilled(pos.mint)
            except Exception:
                log.debug("drop unfilled ledger failed", exc_info=True)
        if not best or not best["mint"]:
            return
        mint = best["mint"]
        try:
            from src.execution.wallet_gate import _ABSENT, _load_absent
            from src.execution.wallet_tokens import sold_recently
            _load_absent()
            if mint in _ABSENT or sold_recently(mint):
                log.info("WALLET leftover %s was already sold — not opened again", mint[:8])
                return
        except Exception:
            pass
        try:
            from src.storage.mint_flags import mint_flag
            if mint_flag(mint) == "DUST":
                log.info("WALLET leftover %s is flagged DUST — not opened, PnL not touched", mint[:8])
                return
        except Exception:
            pass
        dex = await dex_held(manager, mint)
        remembered = (getattr(manager, "_held_meta", None) or {}).get(mint) or {}
        pos = next((p for p in manager.positions.values() if p.status == "OPEN" and p.mint == mint), None)
        created = False
        if pos is None:
            raw_held = int(best["raw"])
            pos = await manager.open_position(
                mint=mint,
                mode=PositionMode.LIVE,
                entry_sol=0.0,
                entry_price=1.0,
                tokens=float(raw_held),
                source="opencopy",
                fees_sol=0.0,
                strategy="opencopy",
                force=True,
            )
            created = pos is not None
        if pos is None:
            return
        if dex.get("symbol"):
            pos.symbol = dex["symbol"]
        elif remembered.get("symbol"):
            pos.symbol = remembered["symbol"]
        if dex.get("name"):
            pos.name = dex["name"]
        elif remembered.get("name"):
            pos.name = remembered["name"]
        if getattr(pos, "entry_m5_pct", None) is None and remembered.get("entry_m5") is not None:
            pos.entry_m5_pct = remembered["entry_m5"]
        if dex.get("m5") is not None:
            pos.live_m5_pct = dex["m5"]
        if created:
            try:
                from src.observability.ledger import get_ledger
                get_ledger().record_open(
                    id=f"hold-{mint[:8]}-{int(time.time())}",
                    mint=mint,
                    symbol=getattr(pos, "symbol", "") or mint[:8],
                    mode="live",
                    source="opencopy",
                    token_amount=float(best["raw"]),
                    entry_sol=0.0,
                    entry_fee_sol=0.0,
                    entry_tip_sol=0.0,
                    price_sol_per_token=0.0,
                    name=getattr(pos, "name", "") or "",
                )
            except Exception:
                log.debug("wallet restore ledger failed", exc_info=True)
            log.warning(
                "HELD RESTORED %s — it is in the wallet and was missing from the book",
                getattr(pos, "symbol", "") or mint[:8],
            )
        if enable_ui:
            snap = manager.snapshot()
            stats["open_positions"] = snap["open_count"]
            await publish_positions(snap)
            await publish_stats(dict(stats))

    async def _book_bought_still_held(accounts) -> None:
        """Open only a real buy still in the wallet. A dust flag is never cleared."""
        if not accounts:
            return
        try:
            from src.observability.exec_history import get_exec_history
            from src.observability.ledger import get_ledger
            from src.risk.position_manager import PositionMode
            from src.storage.mint_flags import mint_flag
            flagged = set()
            for acc in accounts:
                mint = str(acc.get("mint") or "")
                if not mint:
                    continue
                if acc.get("dust") or mint_flag(mint) == "DUST" or str(acc.get("state") or "").lower() == "frozen":
                    flagged.add(mint)
            dropped = False
            for pos in list(manager.positions.values()):
                if getattr(pos, "status", "") != "OPEN":
                    continue
                if pos.mint in flagged or mint_flag(pos.mint) == "DUST":
                    # Still in the wallet. Do not write a close and do not drop the open row.
                    continue
            held = {}
            still_there = set()
            for acc in accounts:
                mint = str(acc.get("mint") or "")
                raw = int(acc.get("raw") or 0)
                ui = float(acc.get("ui_amount") or acc.get("amount") or 0)
                if mint and raw > 0:
                    still_there.add(mint)
                if not mint or (raw <= 1000 and ui < 1):
                    continue
                held[mint] = (ui, raw or int(ui))
                try:
                    from src.execution.wallet_tokens import unmark_sold
                    unmark_sold(mint)
                except Exception:
                    pass
            try:
                if get_ledger().repair_book(still_there):
                    if enable_ui:
                        await publish_ledger(get_ledger().recent(250), get_ledger().summary())
            except Exception:
                log.debug("ledger repair failed", exc_info=True)
            latest_side = {}
            hist = get_exec_history()
            for entry in list(hist.wallet_entries) + list(hist.entries):
                side = (entry.side or "").upper()
                if side not in ("BUY", "SELL") or (entry.status or "") == "failed" or not entry.mint:
                    continue
                # A 0-SOL row with a handful of tokens is a bad parse, not a sale.
                if side == "SELL" and float(getattr(entry, "sol_amount", 0) or 0) <= 0 and float(getattr(entry, "token_amount", 0) or 0) < 100:
                    continue
                prev = latest_side.get(entry.mint)
                if prev is None or float(entry.ts or 0) >= float(getattr(prev, "ts", 0) or 0):
                    latest_side[entry.mint] = entry
            try:
                from src.observability.dashboard import openhold_status, set_openhold_status
            except Exception:
                openhold_status = lambda _mint: ""
                set_openhold_status = lambda _mint, _status: None
            open_mints = {p.mint for p in manager.positions.values() if getattr(p, "status", "") == "OPEN"}
            closed_dust = set(getattr(manager, "_no_reopen", None) or ())
            dropped_ghost = False
            for pos in list(manager.positions.values()):
                if getattr(pos, "status", "") != "OPEN":
                    continue
                if pos.mint in held or pos.mint in flagged:
                    continue
                age = time.time() - float(getattr(pos, "entry_time", 0) or 0)
                if age < 20:
                    continue
                log.warning(
                    "HELD %s is not in the wallet — live card removed",
                    getattr(pos, "symbol", "") or pos.mint[:8],
                )
                manager.positions.pop(pos.id, None)
                open_mints.discard(pos.mint)
                dropped_ghost = True
                try:
                    get_ledger().drop_unfilled(pos.mint)
                except Exception:
                    pass
            wanted = []
            for mint in held:
                if mint in closed_dust:
                    continue
                entry = latest_side.get(mint)
                marked = openhold_status(mint) == "HOLDING"
                side = (entry.side or "").upper() if entry else ""
                ui, _raw_held = held[mint]
                if mint not in open_mints and (marked or side in ("BUY", "SELL")):
                    wanted.append(mint)
            for mint, entry in latest_side.items():
                if openhold_status(mint) == "HOLDING" and mint not in held and mint not in open_mints:
                    set_openhold_status(mint, "")
            if not wanted:
                if (dropped or dropped_ghost) and enable_ui:
                    snap = manager.snapshot()
                    stats["open_positions"] = snap["open_count"]
                    await publish_positions(snap)
                    await publish_stats(dict(stats))
                return
            opened_any = False
            for mint in wanted[:4]:
                entry = latest_side.get(mint)
                buy = entry if entry and (entry.side or "").upper() == "BUY" else None
                if buy is None:
                    for old in reversed(list(hist.wallet_entries) + list(hist.entries)):
                        if old.mint == mint and (old.side or "").upper() == "BUY" and (old.status or "") != "failed":
                            buy = old
                            break
                ui, raw_held = held[mint]
                sol_in = float(getattr(buy, "sol_amount", 0) or 0) if buy else 0.0
                symbol = (getattr(buy, "symbol", "") if buy else "") or (getattr(entry, "symbol", "") if entry else "") or mint[:8]
                pos = await manager.open_position(
                    mint=mint,
                    mode=PositionMode.LIVE,
                    entry_sol=sol_in,
                    entry_price=(sol_in / raw_held) if sol_in and raw_held else 1.0,
                    tokens=float(raw_held),
                    source="opencopy",
                    signature=getattr(entry, "signature", "") or "" if entry else "",
                    fees_sol=0.0,
                    strategy="opencopy",
                    force=True,
                )
                if pos is None:
                    continue
                pos.symbol = symbol
                opened_any = True
                try:
                    from src.observability.dashboard import bind_trade_caller
                    bind_trade_caller(pos)
                except Exception:
                    log.debug("held buy caller bind failed", exc_info=True)
                already = any(
                    (old.get("mint") or "") == mint and (old.get("side") or "") == "OPEN"
                    for old in get_ledger().recent(250)
                )
                if not already:
                    get_ledger().record_open(
                        id=f"hold-{mint[:8]}-{int(getattr(entry, 'ts', 0) or time.time()) if entry else int(time.time())}",
                        mint=mint,
                        symbol=symbol,
                        mode="live",
                        source="opencopy",
                        token_amount=float(raw_held),
                        entry_sol=sol_in,
                        entry_fee_sol=0.0,
                        entry_tip_sol=0.0,
                        price_sol_per_token=(sol_in / raw_held) if sol_in and raw_held else 0.0,
                        signature=getattr(entry, "signature", "") or "" if entry else "",
                        name=getattr(pos, "name", "") or "",
                    )
                log.warning("HELD RESTORED %s — bought and still in the wallet", symbol)
            if not opened_any:
                return
            if enable_ui:
                snap = manager.snapshot()
                stats["open_positions"] = snap["open_count"]
                await publish_positions(snap)
                await publish_stats(dict(stats))
                await publish_ledger(get_ledger().recent(250), get_ledger().summary())
        except Exception:
            log.warning("book held buy failed", exc_info=True)

    async def _publish_wallet_and_ledger():
        now = time.time()
        if now - getattr(_publish_wallet_and_ledger, "at", 0) < 1:
            return
        _publish_wallet_and_ledger.at = now
        try:
            from src.execution.signer import pubkey_from_env
            from src.execution.trader import note_sol
            from src.execution.wallet_tokens import WSOL, quote_mints, read_wallet_now
            pk = pubkey_from_env() or ""
            sol = None
            accounts = None
            token_read = False
            if pk:
                try:
                    sol, accounts = await asyncio.wait_for(read_wallet_now(pk), timeout=0.95)
                except Exception:
                    sol, accounts = None, None
                token_read = accounts is not None
                if sol is not None:
                    note_sol(float(sol))
            if sol is None and not token_read:
                _publish_wallet_and_ledger.at = 0
                return
            if token_read:
                accounts = list(accounts)
                _publish_wallet_and_ledger.coins = accounts
            else:
                accounts = list(getattr(_publish_wallet_and_ledger, "coins", None) or [])
            mints = [a.get("mint") for a in accounts if a.get("mint")][:60]
            quotes = {}
            if sol is not None or mints:
                try:
                    quotes = await asyncio.wait_for(quote_mints([WSOL, *mints], named=True), timeout=1.5)
                except Exception:
                    quotes = {}
            sol_px = (quotes.get(WSOL) or {}).get("price_usd")
            usd = (float(sol) * float(sol_px)) if sol is not None and sol_px else None
            tokens = []
            sold = set()
            try:
                from src.observability.exec_history import get_exec_history
                latest = {}
                hist = get_exec_history()
                for entry in list(hist.wallet_entries) + list(hist.entries):
                    mint_key = entry.mint or ""
                    if not mint_key or (entry.status or "") == "failed":
                        continue
                    prev = latest.get(mint_key)
                    if prev is None or float(entry.ts or 0) >= float(prev.ts or 0):
                        latest[mint_key] = entry
                sold = {mint_key for mint_key, entry in latest.items() if (entry.side or "").upper() == "SELL" and (entry.status or "") == "confirmed"}
            except Exception:
                sold = set()
            for a in accounts:
                mint = a.get("mint") or ""
                if not mint:
                    continue
                q = quotes.get(mint) or {}
                px = q.get("price_usd")
                ui = float(a.get("ui_amount") or 0)
                if ui <= 0:
                    continue
                usd_val = (ui * float(px)) if px else None
                symbol = q.get("symbol") or ""
                name = q.get("name") or ""
                if not symbol:
                    symbol = name or (mint[:4] if mint else "—")
                tokens.append({
                    "mint": mint,
                    "symbol": symbol,
                    "name": name,
                    "amount": ui,
                    "price_usd": px,
                    "usd": usd_val,
                })
            tokens.sort(key=lambda t: (t["usd"] is None, -(t["usd"] or 0), t["symbol"]))
            token_usd = sum(t["usd"] for t in tokens if t["usd"] is not None)
            total_usd = None
            if usd is not None or token_usd:
                total_usd = float(usd or 0) + float(token_usd)
            invested = None
            try:
                from src.storage.invested import get_invested
                invested = get_invested()
            except Exception:
                invested = None
            sol_px_f = float(sol_px) if sol_px else 0.0
            coin_lines = []
            coin_sol = 0.0
            if sol_px_f > 0:
                for t in tokens:
                    if t.get("usd") is None:
                        continue
                    vsol = float(t["usd"]) / sol_px_f
                    coin_sol += vsol
                    coin_lines.append({
                        "symbol": t.get("symbol") or "—",
                        "mint": t.get("mint") or "",
                        "sol": round(vsol, 6),
                    })
            equity = None if sol is None else round(float(sol) + coin_sol, 6)
            vs_invested = None
            if equity is not None and invested is not None:
                vs_invested = round(equity - float(invested), 6)
            if sol is not None and equity is None:
                equity = round(float(sol) + coin_sol, 6)
            if equity is not None and vs_invested is None and invested is not None:
                vs_invested = round(equity - float(invested), 6)
            try:
                from src.storage.capital_pnl import note_equity
                missing_price = False
                cap = None
                if token_read and sol is not None and sol_px_f > 0:
                    cap = note_equity(equity, invested)
                if cap and enable_ui:
                    board = get_ledger().pnl_board()
                    from src.observability.dashboard import bus as dash_bus
                    dash_bus.update_stats(**board)
                    await dash_bus.publish({"type": "stats", "stats": board})
            except Exception:
                log.debug("capital pnl update failed", exc_info=True)
            trade_line = None
            try:
                opens = [p for p in manager.positions.values() if getattr(p, "status", "") == "OPEN"]
            except Exception:
                opens = []
            if opens:
                pos0 = opens[0]
                matched = next((c for c in coin_lines if c.get("mint") == pos0.mint), None)
                if matched:
                    live_sol = float(matched["sol"])
                elif getattr(pos0, "since_buy_pct", None) is not None:
                    live_sol = float(pos0.entry_sol or 0) * float(pos0.remaining_frac or 1) * (1.0 + float(pos0.since_buy_pct) / 100.0)
                else:
                    live_sol = float(pos0.entry_sol or 0) * float(pos0.remaining_frac or 1)
                sold_sol = float(getattr(pos0, "_sol_out", 0) or 0)
                cost_sol = float(pos0.entry_sol or 0) + float(getattr(pos0, "fees_sol", 0) or 0)
                cost_check = None if sol is None else round(float(sol) + cost_sol - sold_sol, 6)
                trade_line = {
                    "symbol": getattr(pos0, "symbol", "") or (pos0.mint[:4] if pos0.mint else "—"),
                    "live_sol": round(live_sol, 6),
                    "sold_sol": round(sold_sol, 6),
                    "cost_sol": round(cost_sol, 6),
                    "pnl_sol": round(sold_sol + live_sol - cost_sol, 6),
                    "cost_check_sol": cost_check,
                }
            if enable_ui:
                if not pk or sol is None:
                    from src.observability.dashboard import bus as wallet_bus
                    prev_w = getattr(wallet_bus, "_wallet", None) or {}
                    if not pk:
                        pk = str(prev_w.get("pubkey") or "")
                    if sol is None:
                        return
                await publish_wallet({
                    "pubkey": pk,
                    "sol": sol,
                    "sol_usd": sol_px,
                    "usd": usd,
                    "tokens": tokens[:40],
                    "token_count": len(tokens),
                    "token_usd": token_usd,
                    "total_usd": total_usd,
                    "armed": bool(stats.get("wallet_armed")),
                    "invested_sol": invested,
                    "coin_sol": round(coin_sol, 6),
                    "coin_lines": coin_lines[:6],
                    "equity_sol": equity,
                    "vs_invested_sol": vs_invested,
                    "trade": trade_line,
                    "tokens_known": token_read,
                })
                shown = (None if sol is None else round(float(sol), 4), len(tokens))
                if shown != getattr(_publish_wallet_and_ledger, "shown", None):
                    _publish_wallet_and_ledger.shown = shown
                    log.info("WALLET panel %s SOL · %d coin(s)", "—" if sol is None else f"{float(sol):.4f}", len(tokens))
                led = get_ledger()
                await publish_ledger(led.recent(250), led.summary())
                try:
                    from src.observability.dashboard import bind_trade_caller
                    for pos in manager.positions.values():
                        if getattr(pos, "status", "") == "OPEN":
                            bind_trade_caller(pos)
                except Exception:
                    log.debug("caller bind on wallet publish failed", exc_info=True)
                await _book_bought_still_held(accounts)
            if sol is None:
                _publish_wallet_and_ledger.at = 0
        except Exception:
            _publish_wallet_and_ledger.at = 0
            log.warning("wallet panel publish failed", exc_info=True)

    async def _arm_uploaded_wallet(pub: str) -> None:
        import os as _os
        from src.execution.signer import live_armed
        if trade_mode == "live":
            _os.environ["SOL_SNIPER_LIVE"] = "1"
        armed = live_armed()
        stats["wallet_armed"] = armed
        if armed and trade_mode == "live":
            trader.arm_live(True)
        log.info("WALLET UPLOAD %s | pubkey=%s…", "ARMED" if armed else "NOT ARMED", (pub or "")[:8])
        if enable_ui:
            await publish_stats(dict(stats))
            _publish_wallet_and_ledger.at = 0
            await _publish_wallet_and_ledger()

    if enable_ui:
        from src.observability.dashboard import set_http_wallet
        _wallet_loop = asyncio.get_running_loop()
        set_http_wallet(lambda pub: asyncio.run_coroutine_threadsafe(_arm_uploaded_wallet(pub), _wallet_loop))

    log.info(
        "AUTO-TRADE ON | mode=%s filter=%s trade_enabled=%s max_pos=%.3f SOL",
        trade_mode,
        get_config().filter_mode,
        get_config().trade_enabled,
        get_config().max_position_sol,
    )



    async def on_trade_closed(trade):
        if trade.exit_reason == "quote_failed_25":
            bag = getattr(manager, "_ds_held", None)
            if bag:
                bag.discard(trade.mint)
            giveup = getattr(manager, "_ds_giveup", None)
            if giveup is None:
                manager._ds_giveup = set()
                giveup = manager._ds_giveup
            giveup.add(trade.mint)
            log.error(
                "ACTION MOVE_ON mint=%s reason=quote_failed_25 — released hold, next scan buys new TOP1",
                trade.mint[:8],
            )
        if trade.exit_reason == "not_in_wallet":
            log.warning(
                "LEDGER skip mint=%s — not in the wallet is not a close",
                trade.mint[:8],
            )
            if enable_ui:
                await publish_positions(manager.snapshot())
            return
        append_trade_journal({
            "mint": trade.mint, "pnl_sol": trade.pnl_sol, "reason": trade.exit_reason,
            "hold_s": trade.hold_seconds, "mode": trade.mode, "entry_sol": trade.entry_sol,
            "source": trade.source, "closed_at": trade.closed_at,
            "exit_sol": trade.exit_sol, "fees_sol": trade.fees_sol,
        })
        try:
            led = get_ledger()
            if any(
                e.side == "CLOSE" and (e.id == trade.position_id or getattr(e, "group_id", "") == trade.position_id)
                for e in led.entries
            ):
                log.info("LEDGER close already stored for %s", trade.mint[:8])
            else:
                opened = [e for e in led.entries if e.side == "OPEN" and e.mint == trade.mint]
                src = opened[-1] if opened else None
                pos = next((p for p in manager.positions.values() if p.mint == trade.mint), None)
                symbol = ""
                if pos and getattr(pos, "symbol", ""):
                    symbol = pos.symbol
                elif src and src.symbol and not trade.mint.startswith(src.symbol):
                    symbol = src.symbol
                else:
                    symbol = trade.mint[:8]
                entry_sol = src.entry_sol if src else trade.entry_sol
                entry_fee = src.entry_fee_sol if src and src.entry_fee_sol else 0.0
                entry_tip = src.entry_tip_sol if src else 0.0
                if pos and not entry_fee:
                    entry_fee = float(getattr(pos, "fees_sol", 0) or 0)
                sol_out = max(0.0, float(getattr(pos, "_sol_out", 0) or 0)) if pos else max(0.0, float(trade.exit_sol or 0))
                if sol_out <= 0:
                    sol_out = max(0.0, float(trade.exit_sol or 0))
                sig = getattr(trade, "exit_signature", "") or ""
                exit_fee = float(getattr(pos, "_exit_fee", 0) or 0) if pos else 0.0
                sales = list(getattr(pos, "_sales", None) or []) if pos else []
                paid_sales = [s for s in sales if float(s.get("sol") or 0) > 0]
                if paid_sales:
                    sales = paid_sales

                closed_row = None
                try:
                    from src.execution.signer import pubkey_from_env
                    closed_body = await asyncio.to_thread(
                        _portfolio_body, pubkey_from_env() or "", True
                    )
                    closed_row = _portfolio_row(closed_body, trade.mint)
                    if closed_row is None:
                        await asyncio.sleep(0.6)
                        closed_body = await asyncio.to_thread(
                            _portfolio_body, pubkey_from_env() or "", True
                        )
                        closed_row = _portfolio_row(closed_body, trade.mint)
                except Exception:
                    log.debug("closed portfolio grab failed", exc_info=True)
                    closed_row = None
                frozen_usd = _portfolio_num(closed_row, "pnlUsd")
                frozen_pct = _portfolio_num(closed_row, "pnlPercentage")
                frozen_px = _portfolio_num(closed_row, "tokenPriceUsd")

                def _as_wallet(exit_sol, entry, fee, exit_fee, since, ath, reason):
                    cost = float(entry or 0) + float(fee or 0)
                    out = float(exit_sol or 0)
                    if out > 0 and cost > 0:
                        realized = (out - cost - float(exit_fee or 0)) / cost * 100.0
                        if since is not None and abs(float(since) - realized) > 5:
                            if ath is not None and abs(float(ath) - float(since)) < 0.4:
                                ath = max(0.0, realized)
                            since = realized
                    if reason == "m5_negative" and pos is not None and getattr(pos, "_exit_m5", None) is not None:
                        return since, ath, float(pos._exit_m5)
                    return since, ath, None

                async def _close_marks(mint: str, sale=None, source: str = "", hold_s: float = 0.0):
                    m5 = m1 = s1 = h1 = px = None
                    if isinstance(sale, dict):
                        m5, m1, s1, h1, px = (
                            sale.get("m5"), sale.get("m1"), sale.get("s1"), sale.get("h1"), sale.get("px"),
                        )
                    if any(v is None for v in (m5, m1, s1, h1)):
                        try:
                            from src.signal.dayscalp import read_mark_candles
                            fm5, fm1, fs1, fh1, fpx = await read_mark_candles(mint)
                            m5 = fm5 if m5 is None else m5
                            m1 = fm1 if m1 is None else m1
                            s1 = fs1 if s1 is None else s1
                            h1 = fh1 if h1 is None else h1
                            px = fpx if px is None else px
                        except Exception:
                            log.debug("close candle read failed", exc_info=True)
                    if str(source or "") == "callout" and any(v is None for v in (m5, m1, h1)):
                        try:
                            from src.observability.dashboard import callout_entry_marks
                            snap = await asyncio.to_thread(callout_entry_marks, mint)
                            if m5 is None:
                                m5 = snap.get("m5")
                            if m1 is None:
                                m1 = snap.get("m1")
                            if h1 is None:
                                h1 = snap.get("h1")
                        except Exception:
                            log.debug("callout close tape failed", exc_info=True)
                    del source, hold_s
                    return m5, m1, s1, h1, px

                callout_book = (
                    (getattr(pos, "strategy", "") == "callout" or getattr(pos, "source", "") == "callout")
                    if pos is not None
                    else bool(src is not None and getattr(src, "source", "") == "callout")
                )
                if pos is not None and not callout_book:
                    led.fill_open_marks(
                        trade.mint,
                        m5_pct=getattr(pos, "entry_m5_pct", None),
                        m1_pct=getattr(pos, "entry_m1_pct", None),
                        s1_pct=getattr(pos, "entry_s1_pct", None),
                        h1_pct=getattr(pos, "entry_h1_pct", None),
                        price_usd=getattr(pos, "entry_price_usd", None),
                    )

                def _kept(fresh, *saved):
                    from src.observability.ledger import _mark
                    number = _mark(fresh)
                    if number is not None:
                        return number
                    for item in saved:
                        number = _mark(item)
                        if number is not None:
                            return number
                    return None

                def _trade_source() -> str:
                    vals = []
                    if pos is not None:
                        vals.extend((getattr(pos, "source", ""), getattr(pos, "strategy", "")))
                    if src is not None:
                        vals.append(getattr(src, "source", ""))
                    vals.append(getattr(trade, "source", ""))
                    for value in vals:
                        text = str(value or "").strip().lower()
                        if text in ("opencopy", "callout", "predict"):
                            return text
                    for value in vals:
                        text = str(value or "").strip()
                        if text:
                            return text
                    return ""

                if not sales:
                    since_pct = getattr(pos, "since_buy_pct", None) if pos else None
                    peak = getattr(pos, "peak_since_pct", None) if pos else None
                    ath_pct = max(0.0, float(peak)) if peak is not None else 0.0
                    if since_pct is not None:
                        ath_pct = max(ath_pct, float(since_pct), 0.0)
                    cm5, cm1, cs1, ch1, cpx = await _close_marks(
                        trade.mint,
                        source=(getattr(pos, "strategy", "") if pos else "") or (src.source if src else "") or getattr(trade, "source", ""),
                        hold_s=trade.hold_seconds,
                    )
                    close_px = getattr(pos, "entry_price_usd", None) if pos else None
                    if close_px and since_pct is not None:
                        close_px = float(close_px) * (1.0 + float(since_pct) / 100.0)
                    cm5 = _kept(cm5, getattr(pos, "live_m5_pct", None) if pos else None)
                    cm1 = _kept(cm1, getattr(pos, "live_m1_pct", None) if pos else None)
                    cs1 = _kept(cs1, getattr(pos, "live_s1_pct", None) if pos else None)
                    ch1 = _kept(ch1, getattr(pos, "live_h1_pct", None) if pos else None)
                    cpx = _kept(
                        cpx, close_px,
                        getattr(pos, "_live_px", None) if pos else None,
                        getattr(src, "price_usd", None) if src else None,
                    )
                    since_pct, ath_pct, forced_m5 = _as_wallet(
                        sol_out, entry_sol, entry_fee, exit_fee, since_pct, ath_pct, trade.exit_reason,
                    )
                    if frozen_pct is not None:
                        since_pct = frozen_pct
                    if frozen_px is not None:
                        cpx = frozen_px
                    if forced_m5 is not None:
                        cm5 = forced_m5
                    led.record_close(
                        id=trade.position_id,
                        mint=trade.mint,
                        symbol=symbol,
                        mode=trade.mode,
                        source=_trade_source(),
                        token_amount=(src.token_amount if src else getattr(trade, "tokens", 0.0)) or 0.0,
                        entry_sol=entry_sol,
                        entry_fee_sol=entry_fee,
                        entry_tip_sol=entry_tip,
                        exit_sol=sol_out,
                        exit_fee_sol=exit_fee,
                        hold_seconds=trade.hold_seconds,
                        reason="DUST" if trade.exit_reason in ("dust_left", "dust_unsellable") else trade.exit_reason,
                        signature=sig,
                        since_buy_pct=None if since_pct is None else round(float(since_pct), 2),
                        ath_since_pct=round(ath_pct, 2),
                        sale_label="DUST" if trade.exit_reason in ("dust_left", "dust_unsellable") else "Full Closed",
                        group_id=trade.position_id,
                        count_as_trade=True,
                        m5_pct=cm5,
                        m1_pct=cm1,
                        s1_pct=cs1,
                        h1_pct=ch1,
                        price_usd=cpx,
                        name=(getattr(pos, "name", "") if pos else "") or "",
                        callout_user=(getattr(pos, "callout_user", "") if pos else "") or "",
                        low_since_pct=getattr(pos, "low_since_pct", None) if pos else None,
                        pnl_usd_locked=frozen_usd,
                    )
                else:
                    n = len(sales)
                    total_frac = sum(float(s.get("frac") or 0) for s in sales) or 1.0
                    for i, s in enumerate(sales):
                        frac = float(s.get("frac") or 0)
                        if frac <= 0:
                            frac = 1.0 / n
                        if n == 1:
                            label = "Full Closed"
                        elif i == 0:
                            label = "First sale Closed"
                        elif i == n - 1:
                            label = "Last Closed"
                        elif i == 1:
                            label = "Second sale Closed"
                        else:
                            label = f"Sale {i + 1} Closed"
                        slice_entry = entry_sol * frac
                        slice_fee = entry_fee * frac
                        slice_tip = entry_tip * frac
                        slice_out = float(s.get("sol") or 0)
                        slice_exit_fee = float(s.get("fee") or 0)
                        tokens = (src.token_amount if src else getattr(trade, "tokens", 0.0)) or 0.0
                        cm5, cm1, cs1, ch1, cpx = await _close_marks(
                            trade.mint,
                            s,
                            source=(getattr(pos, "strategy", "") if pos else "") or (src.source if src else "") or getattr(trade, "source", ""),
                            hold_s=trade.hold_seconds,
                        )
                        sale_px = s.get("px")
                        cm5 = _kept(cm5, s.get("m5"), getattr(pos, "live_m5_pct", None) if pos else None)
                        cm1 = _kept(cm1, s.get("m1"), getattr(pos, "live_m1_pct", None) if pos else None)
                        cs1 = _kept(cs1, s.get("s1"), getattr(pos, "live_s1_pct", None) if pos else None)
                        ch1 = _kept(ch1, s.get("h1"), getattr(pos, "live_h1_pct", None) if pos else None)
                        cpx = _kept(
                            cpx, sale_px,
                            getattr(pos, "_live_px", None) if pos else None,
                            getattr(pos, "entry_price_usd", None) if pos else None,
                            getattr(src, "price_usd", None) if src else None,
                        )
                        sale_since, sale_ath, forced_m5 = _as_wallet(
                            slice_out, slice_entry, slice_fee, slice_exit_fee,
                            s.get("pct"), s.get("ath"), s.get("reason") or trade.exit_reason,
                        )
                        if forced_m5 is not None:
                            cm5 = forced_m5
                        if i == n - 1:
                            if frozen_pct is not None:
                                sale_since = frozen_pct
                            if frozen_px is not None:
                                cpx = frozen_px
                        led.record_close(
                            id=f"{trade.position_id}:{i}",
                            mint=trade.mint,
                            symbol=symbol,
                            mode=trade.mode,
                            source=_trade_source(),
                            token_amount=float(tokens) * frac,
                            entry_sol=slice_entry,
                            entry_fee_sol=slice_fee,
                            entry_tip_sol=slice_tip,
                            exit_sol=slice_out,
                            exit_fee_sol=slice_exit_fee,
                            hold_seconds=trade.hold_seconds,
                            reason=s.get("reason") or trade.exit_reason,
                            signature=s.get("sig") or (sig if i == n - 1 else ""),
                            since_buy_pct=None if sale_since is None else round(float(sale_since), 2),
                            ath_since_pct=max(0.0, float(sale_ath or 0)),
                            sale_label=label,
                            group_id=trade.position_id,
                            count_as_trade=(i == n - 1),
                            m5_pct=cm5,
                            m1_pct=cm1,
                            s1_pct=cs1,
                            h1_pct=ch1,
                            price_usd=cpx,
                            name=(getattr(pos, "name", "") if pos else "") or "",
                            callout_user=(getattr(pos, "callout_user", "") if pos else "") or "",
                            low_since_pct=getattr(pos, "low_since_pct", None) if pos else None,
                            pnl_usd_locked=frozen_usd if i == n - 1 else None,
                        )
                    if abs(total_frac - 1.0) > 0.15:
                        log.info(
                            "LEDGER %s sold in %d parts, fractions add to %.2f",
                            symbol, n, total_frac,
                        )
        except Exception:
            log.exception("ledger close record failed")
        try:
            closed_pos = next((p for p in manager.positions.values() if p.mint == trade.mint), None)
            closed_src = ""
            if closed_pos is not None:
                closed_src = getattr(closed_pos, "strategy", "") or getattr(closed_pos, "source", "")
            if not closed_src:
                closed_src = getattr(trade, "source", "") or ""
            if closed_src in ("callout", "opencopy") or getattr(trade, "source", "") in ("callout", "opencopy"):
                from src.observability.dashboard import mark_callout, set_openhold_status
                mark_callout(trade.mint, "CLOSED")
                set_openhold_status(trade.mint, "")
        except Exception:
            log.debug("callout status close failed", exc_info=True)
        if enable_ui:
            try:
                led = get_ledger()
                await publish_ledger(led.recent(250), led.summary())
            except Exception:
                pass
        wins = sum(1 for t in manager.closed if t.pnl_sol > 0)
        stats["closed_trades"] = len(manager.closed)
        stats["win_rate"] = (wins / len(manager.closed)) if manager.closed else 0.0
        stats["daily_pnl_sol"] = manager.daily_pnl_sol
        stats["open_positions"] = sum(1 for p in manager.positions.values() if p.status == "OPEN")

        if enable_ui and trade.exit_reason not in ("DUST", "dust_left", "dust_unsellable", "not_in_wallet"):
            await publish_trade({
                "mint": trade.mint,
                "pnl_sol": trade.pnl_sol,
                "reason": trade.exit_reason,
                "hold_s": int(trade.hold_seconds),
                "mode": trade.mode,
                "entry_sol": trade.entry_sol,
            })
            await publish_alert(
                level="buy" if trade.pnl_sol > 0 else "skip",
                title=f"Closed {'+' if trade.pnl_sol >= 0 else ''}{trade.pnl_sol:.4f} SOL",
                message=f"{trade.mint[:8]}… · {trade.exit_reason} · hold {int(trade.hold_seconds)}s",
                mint=trade.mint,
                verdict="BUY" if trade.pnl_sol > 0 else "SKIP",
                liquidity_sol=trade.entry_sol,
                source=trade.source,
            )
            await publish_positions(manager.snapshot())

    manager.add_close_callback(on_trade_closed)
    manager._trader_jup = trader.jup  # for dust value checks in exit loop

    stop_event = asyncio.Event()

    async def on_exit_tick(mgr, closed):
        # LIVE: only real Jupiter marks (no fake PnL). PAPER: Jupiter then mild jitter.
        for p in list(mgr.positions.values()):
            if p.status != "OPEN":
                continue
            try:
                m = await trader.mark_price(p.mint)
                if m is not None:
                    continue
            except Exception:
                pass
            if p.mode.value == "live":
                # keep last mark; do not invent profitable random walks for live
                continue
            px = mgr.get_price(p)
            px *= (1.0 + random.uniform(-0.08, 0.10))
            mgr.set_price(p.mint, max(px, 1e-12))
        if enable_ui:
            await publish_positions(mgr.snapshot())
        for t in closed or []:
            if getattr(t, "exit_reason", "") in ("dust_unsellable", "dust_left"):
                bag = getattr(mgr, "_ds_held", None)
                if bag:
                    bag.discard(t.mint)
                try:
                    holds = set(trader._load_holds())
                    holds.discard(t.mint)
                    trader._save_holds(holds)
                except Exception:
                    log.debug("hold clear after dust failed", exc_info=True)
                log.warning("DUST closed %s — scan can buy the next coin", (t.mint or "")[:8])

    
    # Seed a few paper positions so exit rules are observable during short runs
    if trade_mode == "paper":
        from src.risk.position_manager import PositionMode
        for i, (m, px) in enumerate([
            ("PaperMintDemo1111111111111111111111111", 1.0),
            ("PaperMintDemo2222222222222222222222222", 1.0),
        ]):
            await manager.open_position(
                mint=m,
                mode=PositionMode.PAPER,
                entry_sol=0.08,
                entry_price=px,
                source="paper_seed",
            )
            manager.set_price(m, px * (1.05 + 0.1 * i))
        log.info("Seeded %d paper positions for exit-loop demo", 2)

    exit_task = asyncio.create_task(
        exit_loop(manager, interval_s=0.35, stop_event=stop_event, on_tick=on_exit_tick)
    )

    async def _paint_held() -> None:
        """Pump price and 5m/1h for the open coin. Does not wait on the dayscalp scanner."""
        import json as _json
        import urllib.request

        def _pull(url: str, mint: str):
            from src.data.fast_get import pump_get

            del mint
            return pump_get(url, timeout=0.8, cache_s=1.0)

        def _is_copy(pos) -> bool:
            return (
                getattr(pos, "strategy", "") in ("callout", "opencopy")
                or getattr(pos, "source", "") in ("callout", "opencopy")
            )

        _own = {"pk": None}

        def _own_wallet() -> str:
            if _own["pk"] is not None:
                return _own["pk"]
            try:
                from src.execution.signer import pubkey_from_env
                _own["pk"] = pubkey_from_env() or ""
            except Exception:
                _own["pk"] = ""
            return _own["pk"]

        def _apply_book(pos, body, sol_usd: float) -> None:
            """% Since Buy is user-positions pnlPercentage. That is what the sell uses.

            pnlUsd is the dollar result. That dollar divided by the SOL price is the
            SOL result. SOL % is that SOL result against SOL paid.
            """
            if not isinstance(body, dict):
                return
            rows = body.get("positions")
            if not isinstance(rows, list):
                return
            row = None
            for item in rows:
                if isinstance(item, dict) and str(item.get("coinMint") or "") == str(pos.mint):
                    row = item
                    break
            if row is None or row.get("pnlPercentage") is None or row.get("pnlUsd") is None:
                return
            try:
                pct = float(row["pnlPercentage"])
                usd = float(row["pnlUsd"])
            except (TypeError, ValueError):
                return
            live_pct = None
            base = float(getattr(pos, "_pump_entry_px", 0) or getattr(pos, "entry_price_usd", 0) or 0)
            px = float(getattr(pos, "_live_px", 0) or 0)
            if base > 0 and px > 0:
                live_pct = (px / base - 1.0) * 100.0
            # An empty position row comes back as -100% even while the coin price is fine.
            if live_pct is not None and pct <= -90 and live_pct > -40:
                if float(getattr(pos, "low_since_pct", 0) or 0) <= -90:
                    pos.low_since_pct = round(min(0.0, live_pct), 2)
                return
            pos.since_buy_pct = round(pct, 2)
            pos._book_usd = usd
            paid = float(getattr(pos, "entry_sol", 0) or 0)
            if sol_usd > 0:
                sol_pnl = usd / sol_usd
                pos._book_pnl_sol = sol_pnl
                if paid > 0:
                    pos._book_sol_pct = round((sol_pnl / paid) * 100.0, 2)
            prev = getattr(pos, "peak_since_pct", None)
            pos.peak_since_pct = round(max(0.0, pct if prev is None else max(float(prev), pct)), 2)
            prev_low = getattr(pos, "low_since_pct", None)
            pos.low_since_pct = round(pct if prev_low is None else min(float(prev_low), pct), 2)
            pos.peak_price = 1.0 + float(pos.peak_since_pct) / 100.0
            return (
                getattr(pos, "strategy", "") in ("callout", "opencopy")
                or getattr(pos, "source", "") in ("callout", "opencopy")
            )

        def _apply_coin(pos, coin) -> None:
            if not isinstance(coin, dict):
                return
            if coin.get("symbol"):
                pos.symbol = coin.get("symbol") or pos.symbol
            if coin.get("name"):
                pos.name = coin.get("name") or getattr(pos, "name", "")
            px = float(coin.get("price_usd") or 0)
            usd = float(coin.get("usd_market_cap") or 0)
            if px > 0:
                pos._live_px = px
            if usd > 0:
                pos._live_mcap = usd
            sol_mcap = float(coin.get("market_cap") or 0)
            ath = float(coin.get("ath_market_cap") or 0)
            if ath > 0 and usd > 0 and sol_mcap > 0 and ath < usd * 0.2:
                ath = ath * (usd / sol_mcap)
            if ath > 0 and usd > 0 and sol_mcap > 0:
                launch_usd = 27.96 * (usd / sol_mcap)
                if launch_usd > 0:
                    pos.token_ath_pct = round((ath / launch_usd - 1.0) * 100.0, 1)
            if px <= 0:
                return
            callout_held = (
                getattr(pos, "strategy", "") in ("callout", "opencopy")
                or getattr(pos, "source", "") in ("callout", "opencopy")
            )
            predict_held = (
                callout_held
                or getattr(pos, "strategy", "") == "predict"
                or getattr(pos, "source", "") == "predict"
                or bool(getattr(pos, "quick_entry", False))
            )
            if callout_held:
                base = float(getattr(pos, "_pump_entry_px", 0) or getattr(pos, "entry_price_usd", 0) or 0)
                if base > 0 and px > 0:
                    manager.set_price(pos.mint, max(px / base, 0.01))
                    _callout_price_since(pos, px)
                return
            if predict_held:
                saved = float(getattr(pos, "entry_price_usd", 0) or 0)
                if float(getattr(pos, "_pump_entry_px", 0) or 0) <= 0:
                    if saved > 0:
                        pos._pump_entry_px = saved
                    else:
                        pos._pump_entry_px = px
                        if getattr(pos, "peak_since_pct", None) is None:
                            pos.peak_since_pct = 0.0
                base = float(pos._pump_entry_px or 0)
                if base > 0:
                    since = (px / base - 1.0) * 100.0
                    pos.entry_price_usd = base
                    pos.entry_price = 1.0
                    pos.since_buy_pct = round(since, 2)
                    pos.since_buy_usd_pct = round(since, 2)
                    prev = float(getattr(pos, "peak_since_pct", 0) or 0)
                    pos.peak_since_pct = round(max(prev, since), 2)
                    pos.peak_price = 1.0 + float(pos.peak_since_pct) / 100.0
                    manager.set_price(pos.mint, max(px / base, 0.01))
                return
            entry_usd = float(getattr(pos, "entry_price_usd", 0) or 0)
            if entry_usd <= 0:
                pos.entry_price_usd = px
                entry_usd = px
            mark = px / entry_usd
            if 0 < mark < 8:
                pos.entry_price = 1.0
                pos.since_buy_pct = round((mark - 1.0) * 100.0, 2)
                prev = float(getattr(pos, "peak_since_pct", 0) or 0)
                pos.peak_since_pct = round(max(0.0, prev, float(pos.since_buy_pct)), 2)
                pos.peak_price = 1.0 + float(pos.peak_since_pct) / 100.0
                manager.set_price(pos.mint, mark)

        while not (stop_event and stop_event.is_set()):
            opened = [p for p in manager.positions.values() if p.status == "OPEN"]
            if not opened:
                await asyncio.sleep(1.0)
                continue

            async def _pair(pos):
                coin_url = f"https://frontend-api-v3.pump.fun/coins-v2/{pos.mint}"
                act_url = (
                    "https://swap-api.pump.fun/v1/coins/"
                    f"{pos.mint}/market-activity?program=pump&chainId=solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
                )

                wallet = _own_wallet()
                book_url = (
                    f"https://frontend-api-v3.pump.fun/user-positions/{wallet}"
                    f"?mints={pos.mint}&updatesLimit=0"
                    if wallet else ""
                )

                async def _one(url: str):
                    if not url:
                        return None
                    try:
                        return await asyncio.wait_for(
                            asyncio.to_thread(_pull, url, pos.mint),
                            timeout=0.9,
                        )
                    except Exception:
                        return None

                coin, activity, book = await asyncio.gather(
                    _one(coin_url), _one(act_url), _one(book_url)
                )
                return pos, coin, activity, book

            async def _sol_usd() -> float:
                try:
                    body = await asyncio.wait_for(
                        asyncio.to_thread(
                            _pull, "https://frontend-api-v3.pump.fun/sol-price", ""
                        ),
                        timeout=0.9,
                    )
                    return float((body or {}).get("solPrice") or 0)
                except Exception:
                    return 0.0

            priced, sol_px = await asyncio.gather(
                asyncio.gather(*[_pair(pos) for pos in opened]),
                _sol_usd(),
            )
            if sol_px > 0:
                _paint_held._sol = sol_px
            else:
                sol_px = float(getattr(_paint_held, "_sol", 0) or 0)
            copies = []
            for pos, coin, activity, book in priced:
                if _is_copy(pos):
                    copies.append(pos)
                try:
                    _apply_coin(pos, coin)
                except Exception:
                    log.debug("held price apply failed", exc_info=True)
                try:
                    _apply_book(pos, book, sol_px)
                except Exception:
                    log.debug("held book apply failed", exc_info=True)
                if not isinstance(activity, dict):
                    continue
                for key, attr in (("5m", "live_m5_pct"), ("1h", "live_h1_pct")):
                    if key == "5m" and "5m" in activity and activity.get("5m") is None:
                        pos.live_m5_pct = None
                        pos._pump_m5_null = True
                        continue
                    block = activity.get(key)
                    if isinstance(block, dict) and block.get("priceChangePercent") is not None:
                        setattr(pos, attr, float(block["priceChangePercent"]))
                        if key == "5m":
                            pos._pump_m5_null = False
                        if key == "5m" and (
                            getattr(pos, "strategy", "") == "predict"
                            or getattr(pos, "source", "") == "predict"
                            or bool(getattr(pos, "quick_entry", False))
                        ):
                            pos._pump_m5 = float(block["priceChangePercent"])
                            pos._predict_m5_at = time.time()
                        entry_attr = "entry_m5_pct" if key == "5m" else "entry_h1_pct"
                        if not _is_copy(pos) and getattr(pos, entry_attr, None) is None:
                            setattr(pos, entry_attr, float(block["priceChangePercent"]))
            fresh = [p for p in opened if not getattr(p, "_entry_px_locked", False)]
            if fresh:
                wallet = _own_wallet()
                try:
                    port = await asyncio.wait_for(
                        asyncio.to_thread(_portfolio_body, wallet, False),
                        timeout=1.4,
                    )
                except Exception:
                    port = None
                for pos in fresh:
                    row = _portfolio_row(port, pos.mint)
                    px = _portfolio_num(row, "tokenPriceUsd")
                    if px is None or px <= 0:
                        continue
                    pos.entry_price_usd = px
                    pos._pump_entry_px = px
                    pos._entry_px_locked = True
                    try:
                        from src.observability.ledger import get_ledger
                        get_ledger().freeze_open_price(pos.mint, px)
                    except Exception:
                        log.debug("open price freeze failed", exc_info=True)
            if copies and _since_touching(copies):
                asyncio.create_task(manager.evaluate_exits())
            if enable_ui:
                try:
                    await publish_positions(manager.snapshot())
                except Exception:
                    log.debug("held panel publish failed", exc_info=True)
            for pos in opened:
                if (
                    _is_copy(pos)
                    and getattr(pos, "entry_m5_pct", None) is None
                    and not getattr(pos, "_m5_recover_done", False)
                ):
                    saved = None
                    try:
                        from src.observability.ledger import get_ledger
                        saved = get_ledger().saved_open_mark(pos.mint, "m5_pct")
                    except Exception:
                        saved = None
                    age = time.time() - float(getattr(pos, "entry_time", 0) or 0)
                    live = getattr(pos, "live_m5_pct", None)
                    if saved is not None:
                        pos.entry_m5_pct = float(saved)
                        pos._m5_recover_done = True
                    elif live is not None and 0 <= age <= 20:
                        pos.entry_m5_pct = float(live)
                        pos._m5_recover_done = True
                    elif age > 20:
                        pos._m5_recover_done = True
            await asyncio.sleep(0.05 if copies else 0.2)

    asyncio.create_task(_paint_held())

    def _since_touching(opened) -> bool:
        try:
            from src.config.runtime import get_config
            cfg = get_config()
            levels = list(getattr(cfg, "tp_levels", None) or [0.30])
            need = float(levels[0]) * 100.0
            drop = float(getattr(cfg, "callout_drop_pct", 7.0) or 7.0)
            dropoff = bool(getattr(cfg, "callout_dropoff", False))
        except Exception:
            need, drop, dropoff = 30.0, 7.0, False
        for pos in opened:
            if pos.status != "OPEN":
                continue
            since = getattr(pos, "since_buy_pct", None)
            if since is None:
                continue
            since = float(since)
            hit = list(getattr(pos, "tp_ladder_hit", None) or [])
            if (need / 100.0) not in hit and since + 1e-9 >= need:
                return True
            if not getattr(pos, "_caller_closed", False):
                continue
            if dropoff or since + 1e-9 >= 1.0:
                return True
            if getattr(pos, "_drop_was_neg", False) and since + 1e-9 >= drop:
                return True
        return False

    asyncio.create_task(_paint_held())


    decode_timestamps: deque[float] = deque(maxlen=max_decodes_per_min)
    seen_signatures: set[str] = set()
    decode_lock = asyncio.Lock()


    async def try_decode(signature: str, event_type: str) -> None:
        now = time.time()
        while decode_timestamps and now - decode_timestamps[0] > 60:
            decode_timestamps.popleft()
        if len(decode_timestamps) >= max_decodes_per_min:
            return
        if signature in seen_signatures:
            return
        seen_signatures.add(signature)
        if len(seen_signatures) > 5000:
            seen_signatures.clear()

        async with decode_lock:
            decode_timestamps.append(time.time())
            stats["decode_attempts"] += 1
            decoded = await fetch_and_decode(rpc, signature, commitment="confirmed")

        if not decoded.success or not decoded.mints:
            if decoded.success:
                stats["decode_success"] += 1
            return

        stats["decode_success"] += 1
        stats["mints_found"] += 1
        candidate = decoded_to_candidate(decoded)
        if not candidate:
            return

        result = filters.evaluate(candidate)
        primary = decoded.mints[0]
        show_verdict = result.verdict.value
        show_reasons = list(result.reasons[:4])
        if get_config().filter_mode == "launch" and result.verdict in (Verdict.BUY, Verdict.DOWNSIZE):
            open_now = [p for p in manager.positions.values() if p.status == "OPEN"]
            if open_now:
                held = getattr(open_now[0], "symbol", "") or open_now[0].mint[:8]
                show_verdict = "BLOCKED"
                show_reasons = [f"already holding {held} — no second buy"] + show_reasons[:2]
            else:
                # A new launch is under 2 SOL. That is not a reason to block it.
                show_verdict = "SKIP"
                show_reasons = ["age check next"] + show_reasons[:1]

        attempting = (
            show_verdict in ("BUY", "DOWNSIZE")
            and get_config().trade_enabled
            and not manager.is_paused()
            and get_config().filter_mode != "dayscalp"
        )
        if attempting:
            show_verdict = "BUYING"
            show_reasons = ["BUYING — not in the wallet yet"] + show_reasons[:2]

        if show_verdict == "BUY":
            stats["buy_signals"] += 1
        elif show_verdict == "DOWNSIZE":
            stats["downsize_signals"] = stats.get("downsize_signals", 0) + 1
        elif show_verdict in ("BLOCKED", "SKIP"):
            stats["skip_signals"] += 1
        elif show_verdict != "BUYING":
            stats["skip_signals"] += 1
            log.info("ACTION SKIP mint=%s reasons=%s", primary.mint[:8], result.reasons[:2])

        log.info(
            "DECODED %s | mint=%s... | liq=%.2f | mintAuth=%s | verdict=%s mult=%.2f",
            event_type,
            primary.mint[:8],
            candidate.liquidity.initial_sol,
            "REN" if primary.mint_renounced else "SET",
            show_verdict,
            result.size_multiplier,
        )

        meta = await fetch_token_meta(primary.mint)
        try:
            candidate.metadata.name = meta.name
            candidate.metadata.symbol = meta.symbol
            candidate.metadata.website = meta.website
            candidate.metadata.image_uri = meta.image_uri
        except Exception:
            pass
        if meta.name or meta.symbol:
            log.info("META %s | name=%s symbol=%s", primary.mint[:8], meta.name, meta.symbol)

        launch_buy = False
        if (
            get_config().filter_mode == "launch"
            and show_verdict == "SKIP"
            and show_reasons
            and show_reasons[0] == "age check next"
        ):
            from src.signal.token_meta import fetch_pump_brief
            brief = await fetch_pump_brief(primary.mint)
            if not meta.symbol and brief.get("symbol"):
                meta.symbol = brief.get("symbol")
            if not meta.name and brief.get("name"):
                meta.name = brief.get("name")
            created_raw = brief.get("created")
            if not meta.symbol and not meta.name:
                again = await fetch_pump_brief(primary.mint)
                meta.symbol = again.get("symbol") or meta.symbol
                meta.name = again.get("name") or meta.name
                if not created_raw:
                    created_raw = again.get("created")
            age = None
            if created_raw not in (None, ""):
                try:
                    ts = float(created_raw)
                    if ts > 1_000_000_000_000:
                        ts /= 1000.0
                    if ts > 0:
                        age = max(0.0, time.time() - ts)
                except (TypeError, ValueError):
                    age = None
            if age is None or (age >= 1.0 and age <= 3.0):
                up, why, _pct = False, "neither door", None
                try:
                    up, why, _pct = await asyncio.wait_for(launch_either(primary.mint), timeout=14)
                except Exception as exc:
                    up, why = False, f"check failed ({type(exc).__name__})"
                ticker = meta.symbol or meta.name or primary.mint[:8]
                if up:
                    launch_buy = True
                    show_verdict = "BUYING"
                    show_reasons = [f"{ticker} {why} — sending buy"]
                else:
                    show_reasons = [f"{ticker} {why}"]
            elif age < 1.0:
                _launch_born[primary.mint] = {
                    "mint": primary.mint,
                    "symbol": meta.symbol or "",
                    "name": meta.name or "",
                    "liq_sol": float(candidate.liquidity.initial_sol or 0),
                    "vol_usd": 0.0,
                    "source": "pump_fun",
                    "website": meta.website or "",
                    "created": time.time() - age,
                }
                show_reasons = [f"{meta.symbol or 'new'} age {age:.1f}s — waiting for 1s"]
            elif age > 3.0:
                up, why, _pct = False, "neither door", None
                try:
                    up, why, _pct = await asyncio.wait_for(launch_either(primary.mint), timeout=14)
                except Exception as exc:
                    up, why = False, f"check failed ({type(exc).__name__})"
                ticker = meta.symbol or meta.name or primary.mint[:8]
                if up:
                    launch_buy = True
                    show_verdict = "BUYING"
                    show_reasons = [f"{ticker} age {age:.0f}s · {why} — sending buy"]
                else:
                    show_reasons = [f"{ticker} age {age:.0f}s · {why}"]
            else:
                up, why, launch_s1 = await asyncio.wait_for(launch_either(primary.mint), timeout=14)
                if not up:
                    show_reasons = [why]
                else:
                    launch_buy = True
                    show_verdict = "BUYING"
                    show_reasons = [f"{meta.symbol or 'new'} age {age:.0f}s · {why} — sending buy"]

        if enable_ui:
            await publish_decoded(
                mint=primary.mint,
                liquidity_sol=candidate.liquidity.initial_sol,
                mint_renounced=primary.mint_renounced,
                freeze_renounced=primary.freeze_renounced,
                verdict=show_verdict,
                size_multiplier=result.size_multiplier,
                source=candidate.source_event,
                signature=signature,
                reasons=show_reasons,
                slot=decoded.slot,
                name=meta.name,
                symbol=meta.symbol,
                website=meta.website,
            )
            await publish_stats(stats)
            level = "buy" if show_verdict == "BUY" else (
                "warn" if show_verdict == "DOWNSIZE" else "skip"
            )
            await publish_alert(
                level=level,
                title=f"New token · {show_verdict}",
                message=(
                    f"{primary.mint[:8]}… · {candidate.liquidity.initial_sol:.2f} SOL · "
                    f"{candidate.source_event}"
                ),
                mint=primary.mint,
                verdict=show_verdict,
                liquidity_sol=candidate.liquidity.initial_sol,
                source=candidate.source_event,
                signature=signature,
            )

        # Dayscalp has its own buyer. Launch buys here only when the coin is still 1–3s old.
        cfg_now = get_config()
        scanner_owns = (
            getattr(cfg_now, "dayscalp_enabled", False)
            or cfg_now.filter_mode == "dayscalp"
            or (
                (getattr(cfg_now, "launch_enabled", False) or cfg_now.filter_mode == "launch")
                and not launch_buy
            )
        )
        if scanner_owns:
            if not launch_buy:
                log.info("AUTO-TRADE skipped — source scanner owns buys (%s)", primary.mint[:8])
        elif show_verdict in ("BUY", "DOWNSIZE", "BUYING") and not manager.is_paused() and get_config().trade_enabled:
            open_now = [p for p in manager.positions.values() if p.status == "OPEN"]
            fill = None
            if get_config().filter_mode == "launch" and open_now:
                log.info(
                    "LAUNCH holding %s — no second buy until sold",
                    getattr(open_now[0], "symbol", "") or open_now[0].mint[:8],
                )
            else:
                log.info("AUTO-TRADE trigger %s %s mult=%.2f", result.verdict.value, primary.mint[:8], result.size_multiplier)
                fill = await trader.buy(
                    mint=primary.mint,
                    size_multiplier=result.size_multiplier,
                    source="launch" if get_config().filter_mode == "launch" else candidate.source_event,
                    slot=decoded.slot,
                    signature_hint=signature,
                    ref_price=1.0,
                )
                if fill is not None and not fill.ok and enable_ui:
                    await publish_decoded(
                        mint=primary.mint,
                        liquidity_sol=candidate.liquidity.initial_sol,
                        mint_renounced=primary.mint_renounced,
                        freeze_renounced=primary.freeze_renounced,
                        verdict="BLOCKED",
                        size_multiplier=0,
                        source=candidate.source_event,
                        signature=signature,
                        reasons=[str(fill.error or "buy blocked")],
                        slot=decoded.slot,
                        name=meta.name,
                        symbol=meta.symbol,
                        website=meta.website,
                        force=True,
                    )
            if fill is not None and fill.ok:
                if get_config().filter_mode == "launch":
                    for pos in manager.positions.values():
                        if pos.status == "OPEN" and pos.mint == primary.mint:
                            pos.strategy = "launch"
                            pos.stop_pct = float(manager.cfg.hard_stop_pct)
                            pos.symbol = meta.symbol or pos.symbol
                            pos.name = meta.name or getattr(pos, "name", "")
                            break
                stats["paper_fills"] += 1
                stats["fills"] = stats.get("fills", 0) + 1
                stats["buy_signals"] = stats.get("buy_signals", 0) + 1
                log.info("ACTION OPEN %s mint=%s sol=%.4f route=%s", fill.mode, primary.mint[:8], fill.sol_in, fill.route)
                if enable_ui:
                    await publish_decoded(
                        mint=primary.mint,
                        liquidity_sol=candidate.liquidity.initial_sol,
                        mint_renounced=primary.mint_renounced,
                        freeze_renounced=primary.freeze_renounced,
                        verdict="BUY",
                        size_multiplier=result.size_multiplier,
                        source=candidate.source_event,
                        signature=fill.signature or signature,
                        reasons=["buy confirmed — token in wallet"],
                        slot=decoded.slot,
                        name=meta.name,
                        symbol=meta.symbol,
                        website=meta.website,
                        force=True,
                    )
                    await publish_stats(stats)
                    await publish_positions(manager.snapshot())
                    await publish_alert(
                        level="buy",
                        title=f"Opened {fill.mode} · {fill.sol_in:.3f} SOL",
                        message=f"{primary.mint[:8]}… confirmed",
                        mint=primary.mint,
                        verdict="BUY",
                        liquidity_sol=candidate.liquidity.initial_sol,
                        source=candidate.source_event,
                        signature=fill.signature,
                    )
                try:
                    get_ledger().record_open(
                        id=f"{primary.mint[:8]}-{int(__import__('time').time())}",
                        mint=primary.mint,
                        symbol=(getattr(primary, 'metadata', None) and getattr(primary.metadata, 'symbol', None)) or primary.mint[:8],
                        mode=fill.mode,
                        source="launch" if get_config().filter_mode == "launch" else get_config().filter_mode,
                        token_amount=fill.tokens_out,
                        entry_sol=fill.sol_in,
                        entry_fee_sol=fill.fees_sol,
                        entry_tip_sol=getattr(fill, 'tip_sol', 0.0) or 0.0,
                        price_sol_per_token=fill.effective_price,
                        signature=fill.signature or "",
                    )
                    get_exec_history().record(
                        side="BUY",
                        mint=primary.mint,
                        symbol=(getattr(primary, 'metadata', None) and getattr(primary.metadata, 'symbol', None)) or primary.mint[:8],
                        sol_amount=fill.sol_in,
                        token_amount=fill.tokens_out,
                        signature=fill.signature or "",
                        status="confirmed" if fill.ok else "failed",
                        route=fill.route or "",
                        reason="entry",
                        mode=fill.mode,
                    )
                except Exception:
                    log.exception("ledger open record failed")
            elif fill is not None:
                stats["skip_signals"] = stats.get("skip_signals", 0) + 1
                log.warning("ACTION SKIP_FILL mint=%s err=%s", primary.mint[:8], fill.error)

        stats["daily_pnl_sol"] = manager.daily_pnl_sol

    def on_event(ev) -> None:
        if ev.type == FeedEventType.SLOT:
            if ev.slot and ev.slot % 40 == 0:
                snap = manager.snapshot()
                log.info(
                    "heartbeat slot=%s | events=%d decodes=%d buys=%d fills=%d open=%d pnl=%.4f",
                    ev.slot,
                    stats["events"],
                    stats["decode_success"],
                    stats["buy_signals"],
                    stats["paper_fills"],
                    snap["open_count"],
                    snap["daily_pnl_sol"],
                )
                if enable_ui:
                    stats["daily_pnl_sol"] = snap["daily_pnl_sol"]
                    asyncio.create_task(publish_heartbeat(ev.slot, {"stats": dict(stats)}))
                    asyncio.create_task(_publish_wallet_and_ledger())
                    asyncio.create_task(publish_positions(snap))
            return

        if ev.type not in (FeedEventType.RAYDIUM_POOL, FeedEventType.PUMP_FUN, FeedEventType.LOGS):
            return
        if not ev.signature:
            return

        stats["events"] += 1
        should_decode = ev.type in (FeedEventType.RAYDIUM_POOL, FeedEventType.PUMP_FUN)
        if not should_decode:
            instr = ev.data.get("instructions") or []
            joined = " ".join(instr).lower()
            if "initialize" in joined or "create" in joined:
                should_decode = True
        if should_decode:
            asyncio.create_task(try_decode(ev.signature, ev.type.value))

    feed.add_listener(on_event)
    task = asyncio.create_task(feed.start())

    async def _pipeline_pulse() -> None:
        while True:
            await asyncio.sleep(2)
            mints = []
            try:
                from src.signal.dayscalp import ranked_mints
                mints.extend(ranked_mints())
            except Exception:
                pass
            try:
                mints.extend((row.get("mint") or "") for row in list(_launch_http))
                mints.extend((row.get("mint") or "") for row in _launch_born_rows())
            except Exception:
                pass
            try:
                _note_pipeline(mints)
            except Exception:
                log.debug("pipeline count failed", exc_info=True)
            if not enable_ui:
                continue
            try:
                await publish_stats(dict(stats))
            except Exception:
                log.debug("pipeline stats publish failed", exc_info=True)

    asyncio.create_task(_pipeline_pulse())
    try:
        from src.execution.tx_route import ensure_lightning_warm
        ensure_lightning_warm()
    except Exception:
        log.debug("lightning warm not started", exc_info=True)
    # Always start dayscalp worker; it no-ops until filter_mode == dayscalp
    dayscalp_task = asyncio.create_task(
        dayscalp_loop(trader, manager, file_cfg or {}, duration, enable_ui, stats)
    )

    async def reconcile_wallet_positions():
        """Adopt only tokens with real sellable value; drop worthless dust."""
        try:
            from src.execution.signer import pubkey_from_env
            from src.execution.wallet_tokens import get_token_accounts
            from src.risk.position_manager import PositionMode
            pk = pubkey_from_env()
            if not pk or trade_mode != "live":
                return
            try:
                from src.config.runtime import get_config
                if str(getattr(get_config(), "filter_mode", "") or "") in ("callout", "opencopy", "predict"):
                    return
            except Exception:
                pass
            try:
                accounts = await asyncio.wait_for(get_token_accounts(pk), timeout=8)
            except Exception:
                accounts = None
            if accounts is None:
                log.warning("RECONCILE wallet read failed — no new hold was opened, scan continues")
                return
            in_wallet = {str(a.get("mint") or "") for a in accounts if int(a.get("raw") or 0) > 0}
            now = time.time()
            for pos in list(manager.positions.values()):
                if pos.status != "OPEN" or pos.mint in in_wallet:
                    continue
                age = now - float(getattr(pos, "entry_time", 0) or 0)
                if age < 90:
                    continue
                pos.status = "CLOSED"
                pos.exit_reason = "not_in_wallet"
                pos.remaining_frac = 0.0
                log.warning("RECONCILE dropped %s — wallet read succeeded and this coin is not in it", pos.mint[:8])
            tracked = {p.mint for p in manager.positions.values() if p.status == "OPEN"}
            adopted = 0
            skipped = 0
            for acc in accounts:
                mint = acc.get("mint") or ""
                raw = int(acc.get("raw") or 0)
                ui = float(acc.get("ui_amount") or 0)
                if not mint or raw <= 0 or mint in tracked:
                    continue
                if ui <= 0:
                    continue
                # Quote sell value — skip dust / illiquid junk that would stuck the bot
                try:
                    q = await trader.jup.quote_token_to_sol(mint, raw, slippage_bps=800)
                    sol_val = (q.out_amount / 1_000_000_000) if q else None
                except Exception:
                    sol_val = None
                if sol_val is not None and sol_val < 0.000001:
                    skipped += 1
                    log.info(
                        "RECONCILE skip mint=%s ui=%.4g sol_out=%.6f — dust, not an open trade",
                        mint[:8], ui, sol_val,
                    )
                    continue
                if sol_val is None and ui < 1.0:
                    skipped += 1
                    log.info(
                        "RECONCILE skip mint=%s ui=%.4g — no quote and too small to be the held token",
                        mint[:8], ui,
                    )
                    continue
                entry_sol = float(sol_val) if sol_val else float(get_config().max_position_sol or 0.02)
                entry_sol = min(entry_sol, float(get_config().max_position_sol or 0.02))
                entry_price = entry_sol / float(raw) if raw else 1e-18
                pos = await manager.open_position(
                    mint=mint,
                    mode=PositionMode.LIVE,
                    entry_sol=entry_sol,
                    entry_price=max(entry_price, 1e-18),
                    tokens=float(raw),
                    source="wallet_reconcile",
                    fees_sol=0.0,
                )
                if pos:
                    pos.min_hold_seconds = 15.0
                    pos.stop_pct = float(manager.cfg.hard_stop_pct)
                    pos.strategy = get_config().filter_mode if get_config().filter_mode in ("launch", "dayscalp") else "dayscalp"
                    bag = getattr(manager, "_ds_held", None)
                    if bag is None:
                        manager._ds_held = set()
                        bag = manager._ds_held
                    bag.add(mint)
                    adopted += 1
                    log.warning(
                        "RECONCILE adopted mint=%s ui=%.4g — exit loop can sell this token",
                        mint[:8], ui,
                    )
            log.info("RECONCILE done adopted=%d skipped_dust=%d", adopted, skipped)
        except Exception:
            log.exception("wallet reconcile failed")

    async def drop_empty_positions():
        """Close book positions with zero on-chain balance so dayscalp is not stuck."""
        try:
            from src.execution.signer import pubkey_from_env
            from src.execution.wallet_tokens import get_token_raw_balance
            pk = pubkey_from_env()
            if not pk:
                return
            for pos in list(manager.positions.values()):
                if pos.status != "OPEN" or pos.mode.value != "live":
                    continue
                try:
                    raw = await get_token_raw_balance(pk, pos.mint)
                except Exception:
                    continue
                if raw is None or raw > 0:
                    continue
                if __import__("time").time() - float(getattr(pos, "_restored_at", 0) or 0) < 120:
                    continue
                age = __import__("time").time() - float(getattr(pos, "entry_time", 0) or 0)
                if age < 180:
                    continue
                log.warning(
                    "DROP empty position mint=%s source=%s — confirmed 0 balance",
                    pos.mint[:8], pos.source,
                )
                pos.status = "CLOSED"
                pos.exit_reason = "empty_wallet"
                pos.exit_time = __import__("time").time()
                pos.remaining_frac = 0.0
        except Exception:
            log.debug("drop_empty_positions failed", exc_info=True)

    asyncio.create_task(reconcile_wallet_positions())
    asyncio.create_task(drop_empty_positions())

    async def _adopt_launch_hold() -> None:
        """Wallet still has a real coin but the book is empty. Put that coin back so it can be sold."""
        if any(p.status == "OPEN" for p in manager.positions.values()):
            return
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_accounts
        from src.risk.position_manager import PositionMode
        pk = pubkey_from_env()
        if not pk:
            return
        try:
            accounts = await asyncio.wait_for(get_token_accounts(pk), timeout=8.0)
        except Exception:
            accounts = None
        if accounts is None or not accounts:
            from src.execution.wallet_gate import supabase_held
            status, stored = supabase_held()
            if status == "fail":
                log.warning("LAUNCH wallet read empty — saved wallet unread, hold left as-is, no new buy")
                return
            stored = [t for t in stored if t.get("mint")]
            if not stored:
                log.info("LAUNCH wallet read empty — saved wallet has no held coin")
                return
            from src.execution.wallet_gate import _ABSENT, _load_absent
            from src.execution.wallet_tokens import sold_recently
            _load_absent()
            stored = [t for t in stored if t.get("mint") not in _ABSENT and not sold_recently(t.get("mint") or "")]
            if not stored:
                log.info("LAUNCH wallet read empty — stored coin was already cleared (6025)")
                return
            t = max(stored, key=lambda x: float(x.get("value_sol") or 0) or float(x.get("ui_amount") or 0))
            log.warning("LAUNCH keeping %s from the saved wallet — empty wallet read will not clear it", t["mint"][:8])
            sol = float(t.get("value_sol") or 0) or float(get_config().max_position_sol or 0.015)
            raw = int(t.get("raw") or 0) or 1
            acc = {"mint": t["mint"], "ui_amount": float(t.get("ui_amount") or 0)}
            dex = await dex_held(manager, acc["mint"])
            pos = await manager.open_position(
                mint=acc["mint"],
                mode=PositionMode.LIVE,
                entry_sol=min(float(sol), float(get_config().max_position_sol or 0.015)),
                entry_price=1.0,
                tokens=float(raw),
                source="launch",
                strategy="launch",
                fees_sol=0.0,
            )
            if pos:
                pos.strategy = "launch"
                pos.symbol = dex.get("symbol") or ""
                pos.name = dex.get("name") or ""
                pos._orig_raw = float(raw)
                pos._wallet_raw = raw
                pos._wallet_ui = float(acc.get("ui_amount") or 0)
                trader._save_holds({acc["mint"]})
                log.warning(
                    "LAUNCH HOLD %s restored from the saved wallet — no new buy until it is sold",
                    pos.symbol or acc["mint"][:8],
                )
                if enable_ui:
                    await publish_positions(manager.snapshot())
            return
        best = None
        for acc in accounts:
            mint = acc.get("mint") or ""
            raw = int(acc.get("raw") or 0)
            ui = float(acc.get("ui_amount") or 0)
            if not mint or raw <= 0 or ui < 1:
                continue
            try:
                from src.execution.wallet_gate import _ABSENT, _load_absent
                from src.execution.wallet_tokens import sold_recently
                _load_absent()
                if mint in _ABSENT or sold_recently(mint):
                    continue
            except Exception:
                pass
            try:
                q = await trader.jup.quote_token_to_sol(mint, raw, slippage_bps=800)
                sol = (q.out_amount / 1_000_000_000) if q else 0
            except Exception:
                sol = 0
            if sol < 0.001:
                try:
                    from src.execution.wallet_tokens import quote_mints
                    px = (await quote_mints([mint])).get(mint) or {}
                    usd = float(px.get("price_usd") or 0) * ui
                    if usd >= 0.0001:
                        sol = usd / 150.0
                except Exception:
                    pass
            try:
                from src.storage.mint_flags import mint_flag, set_mint_flag
                if mint_flag(mint) == "DUST":
                    continue
            except Exception:
                pass
            if sol < 0.001:
                continue
            if best is None or sol > best[0]:
                best = (sol, acc, raw)
        if not best:
            return
        sol, acc, raw = best
        dex = await dex_held(manager, acc["mint"])
        pos = await manager.open_position(
            mint=acc["mint"],
            mode=PositionMode.LIVE,
            entry_sol=min(float(sol), float(get_config().max_position_sol or 0.015)),
            entry_price=1.0,
            tokens=float(raw),
            source="launch",
            strategy="launch",
            fees_sol=0.0,
        )
        if not pos:
            return
        pos.strategy = "launch"
        pos.symbol = dex.get("symbol") or ""
        pos.name = dex.get("name") or ""
        pos._orig_raw = float(raw)
        pos._wallet_raw = raw
        pos._wallet_ui = float(acc.get("ui_amount") or 0)
        trader._save_holds({acc["mint"]})
        log.warning(
            "LAUNCH HOLD %s still in wallet ≈%.4f SOL — no new buy until it is sold",
            pos.symbol or acc["mint"][:8], sol,
        )
        if enable_ui:
            await publish_positions(manager.snapshot())

    async def launch_scan_loop() -> None:
        """Poll new pump.fun creates and new pools while Live Controls is Launch."""
        from src.observability.action_log import record_action
        from src.observability.dashboard import publish_decoded

        seen = set()
        last_note = 0.0
        pending_buy = {}
        min_age = 1.0
        max_age = 15.0
        min_vol = 0.0
        asyncio.create_task(_launch_birth_feed())
        asyncio.create_task(_launch_http_feed())

        async def _launch_scan_pass() -> None:
            nonlocal last_note, pending_buy
            found = []
            have = set()
            for row in _launch_born_rows() + list(_launch_http):
                mint = row.get("mint") or ""
                if not mint or mint in have:
                    continue
                have.add(mint)
                found.append(row)
            fresh = []
            for c in found:
                if c["mint"] in seen:
                    continue
                age = c.get("age_sec")
                if age is None:
                    continue
                if age > max_age:
                    continue
                if age < min_age:
                    continue
                fresh.append(c)
            fresh.sort(key=lambda c: float(c.get("age_sec") or 999))
            need_vol = [c for c in fresh if float(c.get("vol_usd") or 0) < min_vol][:8]
            if need_vol:
                import httpx
                async def _fill_vol(client, coin):
                    best = float(coin.get("vol_usd") or 0)
                    try:
                        r = await client.get(f"https://frontend-api-v3.pump.fun/coins-v2/{coin['mint']}")
                        if r.status_code == 200:
                            data = r.json()
                            if isinstance(data, dict):
                                for key in ("volume_5m_usd", "volume_m5_usd"):
                                    try:
                                        best = max(best, float(data.get(key) or 0))
                                    except (TypeError, ValueError):
                                        pass
                    except Exception:
                        pass
                    if best < min_vol:
                        try:
                            r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{coin['mint']}")
                            if r.status_code == 200:
                                for pair in (r.json() or {}).get("pairs") or []:
                                    vol = pair.get("volume") or {}
                                    for key in ("m5",):
                                        try:
                                            best = max(best, float(vol.get(key) or 0))
                                        except (TypeError, ValueError):
                                            pass
                        except Exception:
                            pass
                    coin["vol_usd"] = best
                async with httpx.AsyncClient(timeout=4.0, headers={"User-Agent": "sol-sniper/1.2"}) as client:
                    await asyncio.gather(*[_fill_vol(client, c) for c in need_vol])
            if len(seen) > 4000:
                seen.clear()
            now = time.time()
            ages = [float(c["age_sec"]) for c in found if c.get("age_sec") is not None]
            youngest = min(ages) if ages else None
            log.info(
                "LAUNCH SCAN age 1–15s listed=%d fresh=%d youngest=%s",
                len(found), len(fresh), f"{youngest:.0f}s" if youngest is not None else "—",
            )
            if fresh or now - last_note >= 1:
                young = f"{youngest:.0f}s" if youngest is not None else "none"
                record_action(
                    "LAUNCH",
                    detail=(
                        f"SCAN every 1s · buy if 1s +0.10% to +2.0% OR 5m +0.10% to +2% equals 1h · "
                        f"listed={len(found)} fresh={len(fresh)} youngest={young}"
                    ),
                )
                last_note = now
            _note_pipeline([c.get("mint") or "" for c in found])
            if enable_ui and not fresh:
                shown = sorted(found, key=lambda c: float(c.get("age_sec") or 999))[:8]
                lines = {}
                try:
                    import httpx
                    from src.signal.dayscalp import window_line
                    async with httpx.AsyncClient(timeout=3.0) as client:
                        async def _one(row):
                            s1 = launch_live_move(row["mint"])
                            lines[row["mint"]] = await window_line(client, row["mint"], s1)
                        await asyncio.wait_for(asyncio.gather(*[_one(row) for row in shown]), timeout=8)
                except Exception as exc:
                    log.warning("LAUNCH percents unavailable: %s", exc)
                for row in shown:
                    age_row = row.get("age_sec")
                    age_txt = "age unknown" if age_row is None else f"age {float(age_row):.0f}s"
                    ticker = row.get("symbol") or row["mint"][:8]
                    perc = lines.get(row["mint"]) or "1s none · 1m none · 5m none · 1h none"
                    try:
                        await publish_decoded(
                            mint=row["mint"],
                            liquidity_sol=float(row.get("liq_sol") or 0),
                            mint_renounced=False,
                            freeze_renounced=True,
                            verdict="SKIP",
                            size_multiplier=1.0,
                            source=row.get("source") or "pump_fun",
                            signature="",
                            reasons=[f"{ticker} {age_txt} · {perc}"],
                            name=row.get("name") or "",
                            symbol=row.get("symbol") or "",
                            website=row.get("website") or "",
                        )
                    except Exception:
                        log.debug("launch row publish failed", exc_info=True)
            open_now = [p for p in manager.positions.values() if p.status == "OPEN"]
            if fresh and not open_now:
                try:
                    block = await asyncio.wait_for(trader._block_second_buy(""), timeout=8)
                except Exception as exc:
                    block = f"wallet check failed ({type(exc).__name__})"
                if block:
                    log.warning("LAUNCH no buy — %s", block)
                    record_action("LAUNCH", level="WARNING", detail=f"NO BUY — {block}")
                    try:
                        await _adopt_launch_hold()
                    except Exception:
                        log.debug("launch adopt hold failed", exc_info=True)
                    open_now = [p for p in manager.positions.values() if p.status == "OPEN"]
                    if not open_now:
                        await asyncio.sleep(1)
                        return
                else:
                    cleared = await trader.release_sold_holds()
                    if cleared:
                        log.info(
                            "LAUNCH wallet empty — cleared %s, next coin age 1–15s can buy",
                            ",".join(m[:8] for m in cleared),
                        )
            for c in fresh[:1]:
                age = float(c.get("age_sec") or 0)
                candidate = TokenCandidate(
                    mint=c["mint"],
                    metadata=TokenMetadata(mint=c["mint"], symbol=c.get("symbol") or None, name=c.get("name") or None),
                    authorities=AuthorityInfo(mint_renounced=False, freeze_renounced=True),
                    liquidity=LiquidityInfo(initial_sol=float(c.get("liq_sol") or 0), dex="pump_fun" if c["source"] == "pump_fun" else "raydium"),
                    source_event=c["source"],
                    raw={"instructions": ["create"], "source": c["source"]},
                )
                result = filters.evaluate(candidate)
                show = result.verdict.value
                reasons = list(result.reasons[:3])
                vol = float(c.get("vol_usd") or 0)
                dusted = False
                try:
                    from src.storage.mint_flags import mint_flag
                    dusted = mint_flag(c["mint"]) == "DUST"
                except Exception:
                    dusted = False
                if dusted:
                    show = "BLOCKED"
                    reasons = ["flagged DUST — not bought again"]
                    seen.add(c["mint"])
                elif open_now:
                    held = getattr(open_now[0], "symbol", "") or open_now[0].mint[:8]
                    show = "BLOCKED"
                    reasons = [f"already holding {held} — no second buy"]
                    seen.add(c["mint"])
                elif result.verdict not in (Verdict.BUY, Verdict.DOWNSIZE):
                    show = result.verdict.value
                    seen.add(c["mint"])
                elif not get_config().trade_enabled:
                    show = "BLOCKED"
                    reasons = ["trade is off in Live Controls"]
                elif manager.is_paused():
                    show = "BLOCKED"
                    reasons = ["trading paused"]
                else:
                    up, why, launch_s1 = await asyncio.wait_for(launch_either(c["mint"]), timeout=14)
                    if not up:
                        show = "SKIP"
                        reasons = [why]
                    else:
                        show = "BUYING"
                        c["s1"] = launch_s1
                        reasons = [f"age {age:.0f}s · {why} — sending buy"]
                        pending_buy = {
                            "mint": c["mint"],
                            "symbol": c.get("symbol") or "",
                            "name": c.get("name") or "",
                            "website": c.get("website") or "",
                            "liq": float(c.get("liq_sol") or 0),
                            "source": c["source"],
                        }
                if show not in ("BUY", "BUYING"):
                    stats["skip_signals"] += 1
                log.info(
                    "LAUNCH %s %s liq=%.2f vol=$%.0f verdict=%s",
                    c.get("symbol") or c["mint"][:8], c["mint"][:8],
                    float(c.get("liq_sol") or 0), vol, show,
                )
                if enable_ui:
                    await publish_decoded(
                        mint=c["mint"],
                        liquidity_sol=float(c.get("liq_sol") or 0),
                        mint_renounced=False,
                        freeze_renounced=True,
                        verdict=show,
                        size_multiplier=result.size_multiplier,
                        source=c["source"],
                        signature="",
                        reasons=reasons,
                        name=c.get("name") or "",
                        symbol=c.get("symbol") or "",
                        website=c.get("website") or "",
                    )
                if show != "BUYING":
                    continue
                buy_m5 = None
                buy_m1 = None
                buy_s1 = c.get("s1")
                buy_h1 = None
                log.info(
                    "LAUNCH sending buy %s fresh=%.0fs vol=$%.0f 5m=any 1s=%s",
                    c.get("symbol") or c["mint"][:8],
                    age,
                    vol,
                    "n/a" if buy_s1 is None else f"{buy_s1:+.2f}%",
                )
                record_action(
                    "LAUNCH",
                    detail=f"BUYING {c.get('symbol') or c['mint'][:8]} age {age:.0f}s — sending buy",
                    mint=c["mint"],
                    symbol=c.get("symbol") or "",
                )
                buy_started = time.time()
                buy_exc = None
                try:
                    fill = await asyncio.wait_for(trader.buy(
                        mint=c["mint"],
                        size_multiplier=result.size_multiplier,
                        source="launch",
                        slot=0,
                        ref_price=1.0,
                    ), timeout=20)
                except Exception as exc:
                    fill = None
                    buy_exc = exc
                if fill and fill.ok:
                    seen.add(c["mint"])
                    stats["fills"] = stats.get("fills", 0) + 1
                    stats["buy_signals"] = stats.get("buy_signals", 0) + 1
                    try:
                        from src.signal.dayscalp import read_mark_candles
                        om5, om1, os1, oh1, opx = await read_mark_candles(c["mint"])
                    except Exception:
                        om5 = om1 = os1 = oh1 = opx = None
                    buy_m5 = om5
                    buy_m1 = om1
                    buy_h1 = oh1
                    if buy_s1 is None:
                        buy_s1 = os1
                    for pos in manager.positions.values():
                        if pos.status == "OPEN" and pos.mint == c["mint"]:
                            pos.strategy = "launch"
                            pos.stop_pct = float(manager.cfg.hard_stop_pct)
                            pos.symbol = c.get("symbol") or pos.symbol
                            pos.name = c.get("name") or getattr(pos, "name", "")
                            if buy_m5 is not None:
                                pos.entry_m5_pct = float(buy_m5)
                                pos.live_m5_pct = float(buy_m5)
                            if buy_m1 is not None:
                                pos.entry_m1_pct = float(buy_m1)
                            if buy_s1 is not None:
                                pos.entry_s1_pct = float(buy_s1)
                            if buy_h1 is not None:
                                pos.entry_h1_pct = float(buy_h1)
                            open_now = [pos]
                            break
                    log.info("LAUNCH FILL %s sol=%.4f sig=%s", c.get("symbol") or c["mint"][:8], fill.sol_in, (fill.signature or "")[:16])
                    record_action(
                        "LAUNCH",
                        detail=f"BUY {c.get('symbol') or c['mint'][:8]} sol={float(fill.sol_in or 0):.4f} sig={(fill.signature or '')[:16]}",
                        mint=c["mint"],
                        symbol=c.get("symbol") or "",
                    )
                    m5_since = time.time()
                    buy_px = opx
                    try:
                        from src.signal.dayscalp import fetch_price_usd
                        got_px = await fetch_price_usd(c["mint"])
                        if got_px:
                            buy_px = got_px
                    except Exception:
                        pass
                    try:
                        get_ledger().record_open(
                            id=f"ln-{c['mint'][:8]}-{int(time.time())}",
                            mint=c["mint"],
                            symbol=c.get("symbol") or c["mint"][:8],
                            mode=fill.mode,
                            source="launch",
                            token_amount=fill.tokens_out,
                            entry_sol=fill.sol_in,
                            entry_fee_sol=fill.fees_sol,
                            entry_tip_sol=getattr(fill, "tip_sol", 0.0) or 0.0,
                            price_sol_per_token=fill.effective_price,
                            signature=fill.signature or "",
                            m5_pct=buy_m5,
                            m1_pct=buy_m1,
                            s1_pct=buy_s1,
                            h1_pct=buy_h1,
                            price_usd=buy_px,
                        )
                    except Exception:
                        log.warning("launch ledger open failed", exc_info=True)
                    if buy_m1 is None:
                        def _stamp_launch_m1(m1, mint=c["mint"]):
                            for pos in manager.positions.values():
                                if pos.status == "OPEN" and pos.mint == mint and getattr(pos, "entry_m1_pct", None) is None:
                                    pos.entry_m1_pct = float(m1)

                        try:
                            from src.signal.dayscalp import schedule_m1_fill
                            schedule_m1_fill(c["mint"], m5_since - 2, _stamp_launch_m1)
                        except Exception:
                            log.debug("m1 fill schedule skipped", exc_info=True)
                    pending_buy = {}
                    if enable_ui:
                        await publish_decoded(
                            mint=c["mint"],
                            liquidity_sol=float(c.get("liq_sol") or 0),
                            mint_renounced=False,
                            freeze_renounced=True,
                            verdict="BUY",
                            size_multiplier=result.size_multiplier,
                            source=c["source"],
                            signature=fill.signature or "",
                            reasons=["buy confirmed — token in wallet"],
                            name=c.get("name") or "",
                            symbol=c.get("symbol") or "",
                            website=c.get("website") or "",
                            force=True,
                        )
                        await publish_stats(stats)
                else:
                    if buy_exc is not None:
                        err = f"buy {type(buy_exc).__name__}"
                    else:
                        err = str((fill.error if fill else "") or "buy blocked")
                    sent = err.startswith("submit_failed") or "buy_not_in_wallet" in err or "sign_failed" in err
                    age_now = age + max(0.0, time.time() - buy_started)
                    retry = buy_exc is None and not sent and (age_now + 1.0) <= max_age
                    if not retry:
                        seen.add(c["mint"])
                    record_action(
                        "LAUNCH",
                        level="WARNING",
                        detail=f"BUY FAIL {c.get('symbol') or c['mint'][:8]} age {age_now:.0f}s — {err[:120]}",
                        mint=c["mint"],
                        symbol=c.get("symbol") or "",
                        error=err[:180],
                    )
                    log.info(
                        "LAUNCH no buy %s age %.0fs — %s%s",
                        c.get("symbol") or c["mint"][:8],
                        age_now,
                        err[:80],
                        " — retry" if retry else "",
                    )
                    if enable_ui:
                        await publish_decoded(
                            mint=c["mint"],
                            liquidity_sol=float(c.get("liq_sol") or 0),
                            mint_renounced=False,
                            freeze_renounced=True,
                            verdict="MISS",
                            size_multiplier=0 if sent or not retry else result.size_multiplier,
                            source=c["source"],
                            signature="",
                            reasons=[(f"retry next pass — {err[:80]}" if retry else err[:140])],
                            name=c.get("name") or "",
                            symbol=c.get("symbol") or "",
                            website=c.get("website") or "",
                            force=True,
                        )
                        if not retry:
                            pending_buy = {}
                    if open_now:
                        break
            await asyncio.sleep(1)

        while True:
            from src.config.runtime import bot_stopped
            if bot_stopped():
                await asyncio.sleep(2)
                continue
            if not getattr(get_config(), "launch_enabled", False) and get_config().filter_mode != "launch":
                await asyncio.sleep(3)
                continue
            try:
                await _drop_positions_not_in_wallet()
            except Exception:
                log.debug("launch wallet drop failed", exc_info=True)
            try:
                await _launch_scan_pass()
            except Exception as exc:
                log.exception("LAUNCH scan pass failed — next pass in 1s")
                stuck = pending_buy
                pending_buy = {}
                try:
                    record_action(
                        "LAUNCH",
                        level="ERROR",
                        detail=f"SCAN pass failed — {type(exc).__name__}: {exc}"[:180],
                    )
                except Exception:
                    pass
                if stuck and enable_ui:
                    try:
                        await publish_decoded(
                            mint=stuck["mint"],
                            liquidity_sol=stuck.get("liq") or 0,
                            mint_renounced=False,
                            freeze_renounced=True,
                            verdict="MISS",
                            size_multiplier=0,
                            source=stuck.get("source") or "pump_fun",
                            signature="",
                            reasons=[f"buy crashed: {type(exc).__name__}: {exc}"[:140]],
                            name=stuck.get("name") or "",
                            symbol=stuck.get("symbol") or "",
                            website=stuck.get("website") or "",
                            force=True,
                        )
                    except Exception:
                        log.debug("could not clear stuck BUYING", exc_info=True)
            await asyncio.sleep(1)

    asyncio.create_task(launch_scan_loop())
    log.info("LAUNCH scanner started (on when Launch is enabled in Settings)")
    log.info("DAYSCALP worker started (on when Dayscalp is enabled in Settings)")
    asyncio.create_task(_publish_wallet_and_ledger())

    async def _wallet_panel_loop() -> None:
        """Wallet panel follows the chain every second. A failed read is not shown as the old wallet."""
        while True:
            started = time.time()
            try:
                _publish_wallet_and_ledger.at = 0
                await _publish_wallet_and_ledger()
            except Exception:
                log.debug("wallet panel loop failed", exc_info=True)
            wait = 1.0 - (time.time() - started)
            await asyncio.sleep(wait if wait > 0.05 else 0.05)

    asyncio.create_task(_wallet_panel_loop())

    async def _book_wallet_buys(rows) -> None:
        """A buy that landed in the wallet but was not booked still becomes a trade.

        Only a recent confirmed buy the wallet still holds. Old dust is left alone.
        """
        import time as _time
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_holding
        from src.observability.ledger import get_ledger
        from src.risk.position_manager import PositionMode
        now = _time.time()
        open_mints = {p.mint for p in manager.positions.values() if p.status == "OPEN" and p.mint}
        pk = pubkey_from_env() or ""
        for row in rows or []:
            if str(row.get("side") or "").upper() != "BUY":
                continue
            if str(row.get("status") or "") == "failed":
                continue
            mint = str(row.get("mint") or "")
            if not mint or mint in open_mints:
                continue
            ts = float(row.get("block_time") or row.get("ts") or 0)
            if not ts or now - ts > 6 * 3600:
                continue
            raw = 0
            if pk:
                try:
                    held = await asyncio.wait_for(
                        get_token_holding(pk, mint, fast=True, confirm_empty=1),
                        timeout=1.2,
                    )
                    raw = int((held or {}).get("raw") or 0)
                except Exception:
                    raw = 0
            if raw <= 0:
                continue
            sol = float(row.get("sol_amount") or 0)
            if sol <= 0:
                sol = 0.015
            pos = await manager.open_position(
                mint=mint,
                mode=PositionMode.LIVE,
                entry_sol=sol,
                entry_price=(sol / raw) if raw else 0.0,
                tokens=float(raw),
                source="opencopy",
                signature=str(row.get("signature") or ""),
            )
            if pos is None:
                continue
            pos.symbol = str(row.get("symbol") or "") or mint[:6]
            pos._from_openholds = True
            try:
                from src.observability.dashboard import bind_trade_caller
                bind_trade_caller(pos)
            except Exception:
                log.debug("wallet buy caller bind failed", exc_info=True)
            try:
                get_ledger().record_open(
                    id=f"wal-{mint[:8]}-{int(ts)}",
                    mint=mint,
                    symbol=pos.symbol,
                    mode="live",
                    source="opencopy",
                    token_amount=float(raw),
                    entry_sol=sol,
                    entry_fee_sol=0.0,
                    entry_tip_sol=0.0,
                    price_sol_per_token=(sol / raw) if raw else 0.0,
                    signature=str(row.get("signature") or ""),
                )
            except Exception:
                log.debug("wallet buy ledger failed", exc_info=True)
            open_mints.add(mint)
            log.warning(
                "WALLET BUY booked %s — coins are in the wallet and the book had no trade",
                mint[:8],
            )

    async def _wallet_exec_loop():
        from src.execution.signer import pubkey_from_env
        from src.observability.exec_history import build_wallet_rows
        log.info("EXEC history syncs from public RPCs every 3s — primary stays at 1 call/sec")
        while True:
            try:
                pk = pubkey_from_env() or ""
                hist = get_exec_history()
                if pk:
                    try:
                        rows, sigs = await build_wallet_rows(pk, limit=50, parse_budget=4)
                        if sigs:
                            hist.set_from_wallet(rows, sigs)
                            await _book_wallet_buys(rows)
                        elif not hist.wallet_entries:
                            log.warning("EXEC wallet sync returned no transactions")
                    except Exception:
                        log.warning("EXEC wallet sync failed", exc_info=True)
                    bag = [e.to_dict() for e in hist.wallet_entries]
                    led = get_ledger()
                    led.set_chain_rows(bag)
                    changed = led.reconcile_wallet_sells(bag)
                    if enable_ui and changed:
                        await publish_ledger(led.recent(250), led.summary())
                    if enable_ui:
                        await publish_stats(stats)
                if enable_ui:
                    await publish_exec_history(hist.wallet_recent())
            except Exception:
                log.warning("EXEC wallet sync failed", exc_info=True)
            await asyncio.sleep(3)

    if enable_ui:
        asyncio.create_task(_wallet_exec_loop())


    try:
        if duration and duration > 0:
            log.info("Running for %.0f seconds (use --duration 0 for endless)", duration)
            await asyncio.sleep(duration)
        else:
            log.info("Running ENDLESSLY — press Ctrl+C to stop, or use the Restart button")
            while True:
                await asyncio.sleep(3600)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        stop_event.set()
        await feed.stop()
        task.cancel()
        exit_task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        try:
            await exit_task
        except asyncio.CancelledError:
            pass
        if ws_task:
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass
        if http_server:
            http_server.shutdown()
        await trader.close()
        await rpc.close()

    snap = manager.snapshot()
    log.info("========== FULL PIPELINE SUMMARY ==========")
    log.info("Events=%s decodes=%s mints=%s buy_signals=%s", stats["events"], stats["decode_success"], stats["mints_found"], stats["buy_signals"])
    log.info("Fills=%s open=%s closed=%s daily_pnl=%.6f SOL | mode=%s", stats["paper_fills"], snap["open_count"], snap["closed_count"], snap["daily_pnl_sol"], trade_mode)
    for t in snap.get("recent_closed") or []:
        log.info("  closed %s pnl=%.4f reason=%s", t["mint"][:8], t["pnl_sol"], t["reason"])
    log.info("UI: http://localhost:%d/", DEFAULT_HTTP_PORT)



def _portfolio_body(wallet: str, closed: bool):
    """One portfolio read. Open locks the buy price. Closed locks the sale."""
    if not wallet:
        return None
    from src.data.fast_get import pump_get

    url = (
        f"https://frontend-api-v3.pump.fun/user-portfolio/{wallet}?filter=CLOSED&sortBy=RECENCY"
        if closed
        else f"https://frontend-api-v3.pump.fun/user-portfolio/{wallet}?filter=open"
    )
    try:
        return pump_get(url, timeout=1.2, cache_s=0)
    except Exception:
        log.debug("portfolio read failed", exc_info=True)
        return None


def _portfolio_row(body, mint: str):
    if not isinstance(body, (dict, list)) or not mint:
        return None
    hit = {"row": None}

    def walk(node) -> None:
        if hit["row"] is not None:
            return
        if isinstance(node, dict):
            if str(node.get("coinMint") or "") == mint:
                hit["row"] = node
                return
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(body)
    return hit["row"]


def _portfolio_num(row, key: str):
    if not isinstance(row, dict) or row.get(key) is None:
        return None
    try:
        return float(row.get(key))
    except (TypeError, ValueError):
        return None


def _callout_price_since(pos, px: float) -> None:
    """% Since Buy is live price against the price locked at the buy. 5m and 1h are not used."""
    import time as _time

    try:
        px = float(px or 0)
    except (TypeError, ValueError):
        return
    if px <= 0:
        return
    base = float(getattr(pos, "_pump_entry_px", 0) or 0)
    if base <= 0:
        saved = float(getattr(pos, "entry_price_usd", 0) or 0)
        if saved > 0:
            base = saved
            pos._pump_entry_px = saved
            pos._entry_px_locked = True
        else:
            age = _time.time() - float(getattr(pos, "entry_time", 0) or 0)
            if not (0 <= age <= 8):
                return
            pos._pump_entry_px = px
            pos.entry_price_usd = px
            pos._entry_px_locked = True
            pos.since_buy_pct = 0.0
            pos.since_buy_usd_pct = 0.0
            pos.peak_since_pct = 0.0
            pos.low_since_pct = 0.0
            return
    since = round((px / base - 1.0) * 100.0, 2)
    pos.entry_price_usd = base
    pos.entry_price = 1.0
    pos.since_buy_pct = since
    pos.since_buy_usd_pct = since
    prev = getattr(pos, "peak_since_pct", None)
    pos.peak_since_pct = round(max(0.0, since if prev is None else max(float(prev), since)), 2)
    prev_low = getattr(pos, "low_since_pct", None)
    pos.low_since_pct = round(since if prev_low is None else min(float(prev_low), since), 2)
    pos.peak_price = 1.0 + float(pos.peak_since_pct) / 100.0


def _write_callout_book(pos, row) -> bool:
    if not isinstance(row, dict) or row.get("percentage_sol") is None:
        return False
    try:
        since = float(row["percentage_sol"])
    except (TypeError, ValueError):
        return False
    if since == 0 and float(getattr(pos, "peak_since_pct", 0) or 0) > 0.05:
        return False
    pos.since_buy_pct = round(since, 2)
    if row.get("percentage_usd") is not None:
        try:
            pos.since_buy_usd_pct = round(float(row["percentage_usd"]), 2)
        except (TypeError, ValueError):
            pass
    if row.get("unrealized_sol") is not None:
        try:
            pos._book_pnl_sol = float(row["unrealized_sol"])
        except (TypeError, ValueError):
            pass
    if not getattr(pos, "_book_pnl", False):
        pos.peak_since_pct = round(max(0.0, since), 2)
        pos.low_since_pct = round(min(0.0, since), 2)
        pos._book_pnl = True
        log.info(
            "CALLOUT %s since buy is the wallet book: SOL %+.2f%% USD %s",
            getattr(pos, "symbol", "") or pos.mint[:8],
            since,
            "—" if getattr(pos, "since_buy_usd_pct", None) is None else f"{float(pos.since_buy_usd_pct):+.2f}%",
        )
    else:
        pos.peak_since_pct = round(max(float(getattr(pos, "peak_since_pct", 0) or 0), since, 0.0), 2)
        prev_low = getattr(pos, "low_since_pct", None)
        pos.low_since_pct = round(since if prev_low is None else min(float(prev_low), since), 2)
    pos.peak_price = 1.0 + float(pos.peak_since_pct) / 100.0
    pos.entry_price = 1.0
    pos._book_ts = __import__("time").time()
    return True


def _apply_balance_body(positions, body) -> None:
    """percentage_sol / percentage_usd from the same style of Pump read as 5m."""
    data = body.get("data") if isinstance(body, dict) else None
    tokens = data.get("tokens") if isinstance(data, dict) else None
    if not isinstance(tokens, list):
        return
    by_mint = {}
    for tok in tokens:
        if not isinstance(tok, dict):
            continue
        key = str(tok.get("token_mint") or "")
        pnl = tok.get("tokenPnL") if isinstance(tok.get("tokenPnL"), dict) else {}
        if key and pnl.get("percentage_sol") is not None:
            by_mint[key] = pnl
    for pos in positions or []:
        row = by_mint.get(str(getattr(pos, "mint", "") or ""))
        if row:
            _write_callout_book(pos, row)


def _fill_px_usd(fill) -> float:
    """What this bot actually paid per token, in USD. 0 when the fill or the SOL price is not usable."""
    try:
        raw = float(getattr(fill, "tokens_out", 0) or 0)
        sol = float(getattr(fill, "sol_in", 0) or 0)
        if raw <= 0 or sol <= 0:
            return 0.0
        from src.observability.dashboard import _oh_sol_fast, stream_px_usd
        usd = float(_oh_sol_fast() or 0)
        if usd <= 0:
            return 0.0
        px = sol / (raw / 1_000_000.0) * usd
        hit = stream_px_usd(str(getattr(fill, "mint", "") or ""), 60.0)
        # Guard against a unit mix-up: the fill must sit near the live trade price.
        if hit and not (hit[0] / 3.0 <= px <= hit[0] * 3.0):
            return 0.0
        return px
    except Exception:
        return 0.0


def apply_callout_since(pos) -> None:
    """% since buy is the live price against the entry price. ATH is the highest of those."""
    live = float(getattr(pos, "_live_px", 0) or 0)
    entry = float(getattr(pos, "entry_price_usd", 0) or getattr(pos, "_pump_entry_px", 0) or 0)
    if live <= 0 or entry <= 0:
        return
    since = round((live / entry - 1.0) * 100.0, 2)
    pos.since_buy_pct = since
    prev = getattr(pos, "peak_since_pct", None)
    peak = since if prev is None else max(float(prev), since, 0.0)
    pos.peak_since_pct = round(peak, 2)
    pos.entry_price = 1.0
    pos.peak_price = 1.0 + float(pos.peak_since_pct) / 100.0
    try:
        from src.observability.ledger import get_ledger
        get_ledger().note_open_progress(pos.mint, since, pos.peak_since_pct)
    except Exception:
        pass


def refresh_open_callout_books(positions) -> None:
    """One wallet read, then every open Callout. Does not wait on a coin price."""
    mine = [
        pos for pos in (positions or [])
        if getattr(pos, "strategy", "") == "callout" or getattr(pos, "source", "") == "callout"
    ]
    if not mine:
        return
    need = {str(getattr(pos, "mint", "") or "") for pos in mine}
    need.discard("")
    try:
        from src.execution.signer import pubkey_from_env
        from src.observability.dashboard import own_wallet_book
        book = own_wallet_book(pubkey_from_env() or "", need)
    except Exception:
        log.debug("callout book read failed", exc_info=True)
        return
    if not isinstance(book, dict):
        return
    for pos in mine:
        row = book.get(str(getattr(pos, "mint", "") or ""))
        if row is None:
            if not getattr(pos, "_book_miss_logged", False):
                pos._book_miss_logged = True
                log.warning(
                    "CALLOUT %s is not in the wallet balance book",
                    getattr(pos, "symbol", "") or pos.mint[:8],
                )
            continue
        pos._book_miss_logged = False
        _write_callout_book(pos, row)


async def run_paper_mode() -> None:
    log.info("Paper mode – RPC + WS feed + decoder + hard filters ready")
    log.info("Full loop still TODO")
    await asyncio.sleep(0.2)




async def dayscalp_loop(
    trader,
    manager,
    file_cfg: dict,
    duration: float,
    enable_ui: bool,
    stats: dict | None = None,
) -> None:
    """
    Sequential day-scalp ONLY (filter_mode == dayscalp):
      scan DexScreener → TOP1 by 5m vol/trend/% → buy (retry until fill)
      → wait until position fully sold → scan again.
    Does not change launch/strict/scalp/pump_curve behavior.
    """
    try:
        from src.signal.dayscalp import select_top1_dayscalp, dayscalp_config_from_dict
    except Exception as exc:
        log.exception("DAYSCALP cannot start")
        try:
            from src.observability.action_log import record_action
            record_action(
                "DAYSCALP",
                level="ERROR",
                detail=f"scanner did not start — {type(exc).__name__}: {exc}"[:180],
            )
        except Exception:
            pass
        return
    from src.config.runtime import get_config, update_config
    from src.observability.dashboard import publish_alert, publish_decoded, publish_positions, publish_stats
    from src.observability.ledger import get_ledger
    from src.observability.exec_history import get_exec_history, fetch_wallet_signatures
    import time as _time

    ds_cfg = dayscalp_config_from_dict((file_cfg or {}).get("dayscalp") or {})
    from src.storage.supabase_store import get_store
    # Do not change the selected mode. These exits apply only while dayscalp is on.
    if get_config().filter_mode == "dayscalp":
        cfg_now = get_config()
        manager.cfg.max_position_sol = min(float(cfg_now.max_position_sol or 0.015), 0.015)
        manager.cfg.max_concurrent = 1
        manager.cfg.exit_style = getattr(cfg_now, "exit_style", "plan") or "plan"
        manager.cfg.hard_stop_pct = float(cfg_now.hard_stop_pct)
        manager.cfg.runner_stop_pct = float(cfg_now.runner_stop_pct)
        manager.cfg.time_stop_seconds = float(cfg_now.time_stop_seconds)
        manager.cfg.time_stop_min_gain = float(cfg_now.time_stop_min_gain)
        manager.cfg.trail_activate_pct = float(cfg_now.trail_activate_pct)
        manager.cfg.trail_drawdown_pct = float(cfg_now.trail_drawdown_pct)
        manager.cfg.tp_levels = tuple(cfg_now.tp_levels)
        manager.cfg.tp_fractions = tuple(cfg_now.tp_fractions)
        log.info(
            "DAYSCALP exits style=%s tp=%s hard=%.0f%% trail@+%.0f%% giveback=%.0f%%",
            manager.cfg.exit_style,
            ",".join(f"+{int(float(x)*100)}%" for x in cfg_now.tp_levels),
            float(cfg_now.hard_stop_pct) * 100,
            float(cfg_now.trail_activate_pct) * 100,
            float(cfg_now.trail_drawdown_pct) * 100,
        )

    async def _bind_wallet_token() -> None:
        """The open row must be the coin actually in the wallet, with its name."""
        await asyncio.sleep(2)
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_accounts
        from src.risk.position_manager import PositionMode
        pk = pubkey_from_env()
        if not pk:
            return
        try:
            accounts = await asyncio.wait_for(get_token_accounts(pk), timeout=12.0)
        except Exception:
            accounts = None
        if not accounts:
            await _refresh_open_marks()
            return
        held = {a.get("mint") for a in accounts if int(a.get("raw") or 0) > 0}
        for pos in list(manager.positions.values()):
            if pos.status == "OPEN" and pos.mint not in held:
                log.warning("RESUME %s is not in this wallet read — left OPEN until a sell is confirmed", pos.mint[:8])
        await _refresh_open_marks()
        manager._push_remote()
        if enable_ui:
            await publish_positions(manager.snapshot())

    log.info(
        "DAYSCALP sequential TOP1 | vol_m5>=$%.0f m5=[%.1f%%,%.1f%%] pos=%.3f SOL "
        "buy_attempts=%d wait_for_full_exit=ON",
        ds_cfg.min_volume_m5_usd,
        ds_cfg.min_m5_change_pct,
        ds_cfg.max_m5_change_pct,
        ds_cfg.max_position_sol,
        ds_cfg.buy_max_attempts,
    )

    t_end = _time.time() + duration if duration and duration > 0 else None
    shared_top: dict = {"top": None, "ts": 0.0, "last_attempt": "", "last_attempt_ts": 0.0}

    async def _drop_positions_not_in_wallet() -> None:
        """Open means the wallet holds the token. A book row with no balance is not a trade."""
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_accounts
        pk = pubkey_from_env()
        if not pk:
            return
        try:
            accounts = await asyncio.wait_for(get_token_accounts(pk), timeout=8.0)
        except Exception:
            log.warning("ACTION wallet read failed — not changing open positions")
            return
        if accounts is None:
            log.warning("ACTION wallet read failed — not changing open positions")
            return
        by_mint = {a.get("mint"): int(a.get("raw") or 0) for a in accounts}
        changed = False
        for pos in list(manager.positions.values()):
            if pos.status != "OPEN":
                continue
            raw = int(by_mint.get(pos.mint) or 0)
            if raw <= 0:
                from src.execution.wallet_tokens import _seen_recently
                if _seen_recently(pos.mint):
                    log.info("ACTION balance not visible mint=%s — seen in the last 90s, keep OPEN", pos.mint[:8])
                    continue
            age = _time.time() - float(getattr(pos, "entry_time", 0) or 0)
            if raw <= 0 and age >= 90:
                try:
                    from src.execution.wallet_tokens import get_token_holding
                    one = await asyncio.wait_for(get_token_holding(pk, pos.mint), timeout=8)
                    if one is None:
                        log.info("ACTION balance not visible mint=%s — keep OPEN", pos.mint[:8])
                        continue
                    raw = int(one.get("raw") or 0)
                    if raw <= 0:
                        pos._absent = 3
                except Exception:
                    log.info("ACTION balance not visible mint=%s — keep OPEN", pos.mint[:8])
                    continue
            if raw > 0:
                pos._absent = 0
                continue
            pos._absent = int(getattr(pos, "_absent", 0) or 0) + 1
            if age < 90 or pos._absent < 3:
                log.info(
                    "ACTION balance not visible mint=%s age=%.0fs misses=%d — keep OPEN",
                    pos.mint[:8], age, pos._absent,
                )
                continue
            log.warning("ACTION NOT_IN_WALLET mint=%s — book cleared, not a sell", pos.mint[:8])
            pos.status = "CLOSED"
            pos.exit_reason = "not_in_wallet"
            pos.remaining_frac = 0.0
            bag = getattr(manager, "_ds_held", None)
            if bag:
                bag.discard(pos.mint)
            changed = True
        if changed:
            trader._save_holds(set())
            manager._push_remote()
            if enable_ui:
                try:
                    await publish_positions(manager.snapshot())
                except Exception:
                    pass

    async def _wallet_holds_reason() -> str:
        """Block a new buy while any non-dust token is still in the wallet."""
        from src.execution.wallet_gate import wallet_buy_block
        reason = await wallet_buy_block(trader.jup)
        # A failed RPC read is not a coin. Dust is not a coin. Only a seen, valued token blocks.
        if reason and not reason.startswith("wallet still holds") and not reason.startswith("could not price"):
            if not any(p.status == "OPEN" for p in manager.positions.values()):
                log.info("Wallet not confirmed holding — dust/RPC miss is not WATCH")
                return ""
        if not reason.startswith("wallet still holds"):
            return reason
        try:
            from src.storage.supabase_store import get_store
            from src.risk.position_manager import PositionMode
            state = get_store().get("wallet_now") or {}
            holds = [t for t in state.get("tokens") or [] if not t.get("dust") and float(t.get("value_sol") or 0) >= 0.001]
            if holds and not any(p.status == "OPEN" for p in manager.positions.values()):
                t = max(holds, key=lambda x: float(x.get("value_sol") or 0))
                mint = t.get("mint") or ""
                try:
                    from src.execution.wallet_gate import _ABSENT, _load_absent
                    from src.execution.wallet_tokens import sold_recently
                    _load_absent()
                    if mint in _ABSENT or sold_recently(mint):
                        return ""
                except Exception:
                    pass
                try:
                    from src.storage.mint_flags import mint_flag
                    if mint_flag(mint) == "DUST":
                        return ""
                except Exception:
                    pass
                mode_name = get_config().filter_mode if get_config().filter_mode in ("launch", "dayscalp") else "launch"
                pos = await manager.open_position(
                    mint=mint,
                    mode=PositionMode.LIVE,
                    entry_sol=min(float(t["value_sol"]), float(get_config().max_position_sol or 0.02)),
                    entry_price=1.0,
                    tokens=float(t.get("raw") or 0),
                    source=mode_name,
                    fees_sol=0.0,
                    strategy=mode_name,
                )
                if pos:
                    pos.strategy = mode_name
                    log.warning("WALLET HOLD adopted %s ≈%.4f SOL source=%s — no new buy until sold", mint[:8], float(t["value_sol"]), mode_name)
        except Exception:
            log.debug("adopt wallet hold failed", exc_info=True)
        return reason

    async def _reopen_unsold() -> None:
        """Wallet still holds a coin the book dropped. Put it back. A Jupiter quote is not required."""
        while True:
            try:
                await _restore_held_once()
            except Exception:
                log.debug("restore held failed", exc_info=True)
            await asyncio.sleep(12)

    async def _restore_held_once() -> None:
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_accounts, get_token_holding
        from src.risk.position_manager import PositionMode
        pk = pubkey_from_env()
        if not pk:
            return
        cfg = get_config()
        mode = str(getattr(cfg, "filter_mode", "") or "").strip().lower()
        real = {"opencopy", "callout", "predict", "launch", "dayscalp"}
        fake_close = {
            "wallet_already_sold", "not_in_wallet", "not_in_wallet_on_resume",
            "empty_wallet",
        }
        fake_source = {"wallet_resume", "wallet_reconcile", "ledger_restore", "saved", "unknown", ""}
        try:
            accounts = await asyncio.wait_for(
                get_token_accounts(pk, fresh=True),
                timeout=4.0,
            )
        except Exception:
            accounts = None
        # A failed paid read is not a wallet. Do not rebuild a card from a public RPC or from Pump.
        if accounts is None:
            return
        listed: dict = {}
        for acc in accounts:
            mint = str(acc.get("mint") or "")
            raw = int(acc.get("raw") or 0)
            if mint and raw > 0:
                listed[mint] = acc
        sig = tuple(sorted(listed))
        prev = getattr(_restore_held_once, "_sig", None)
        if prev != sig:
            _restore_held_once._sig = sig
            if sig:
                log.warning("HELD wallet coins now %s", ",".join(item[:8] for item in sig))
            elif prev not in (None, ()):
                log.info("HELD wallet has no coins — nothing to restore")
        port_rows: dict = {}
        try:
            port = await asyncio.to_thread(_portfolio_body, pk, False)
        except Exception:
            port = None
        bag = []
        if isinstance(port, dict):
            bag = port.get("positions") or []
        elif isinstance(port, list):
            bag = port
        for row in bag:
            if isinstance(row, dict) and not row.get("isExited") and row.get("coinMint"):
                port_rows[str(row["coinMint"])] = row

        async def _raw_now(mint: str):
            """This mint's balance from a direct read. None if the read failed. 0 if it is gone.

            The 90s wallet cache is dropped first so a sold coin cannot look held.
            """
            from src.execution.wallet_tokens import drop_cached_mint
            drop_cached_mint(mint)
            try:
                one = await asyncio.wait_for(
                    get_token_holding(pk, mint, confirm_empty=1, ignore_seen=True),
                    timeout=1.6,
                )
            except Exception:
                return None
            if one is None:
                return None
            return int(one.get("raw") or 0)

        gone = getattr(_restore_held_once, "_gone", None)
        if gone is None:
            gone = set()
            _restore_held_once._gone = gone
        held: dict = dict(listed)
        for mint in listed:
            gone.discard(mint)
        removed = False
        for pos in list(manager.positions.values()):
            if pos.status != "OPEN":
                continue
            if pos.mint in held:
                try:
                    from src.storage.mint_flags import dust_source, mint_flag
                    flag = mint_flag(pos.mint)
                    dusty = dust_source(pos.mint) in ("manual", "jupiter", "frozen") if flag == "DUST" else False
                except Exception:
                    dusty = bool(getattr(pos, "_dust_value", False))
                if dusty:
                    try:
                        await manager.close_position(pos.id, "dust_left", exit_price=float(pos.entry_price or 0), frac=1.0)
                    except Exception:
                        pos.status = "CLOSED"
                        pos.exit_reason = "dust_left"
                    log.warning("HELD %s is DUST — live card removed, next buy can start", getattr(pos, "symbol", "") or pos.mint[:8])
                    removed = True
                continue
            raw = await _raw_now(pos.mint)
            if raw is None:
                continue
            if raw > 0:
                gone.discard(pos.mint)
                held[pos.mint] = {"mint": pos.mint, "raw": raw, "ui_amount": 0}
                continue
            gone.add(pos.mint)
            removed = True
            log.warning(
                "HELD %s is not in the wallet — live card removed",
                getattr(pos, "symbol", "") or pos.mint[:8],
            )
            try:
                await manager.close_position(pos.id, "not_in_wallet", exit_price=float(pos.entry_price or 0), frac=1.0)
            except Exception:
                pos.status = "CLOSED"
                pos.exit_reason = "not_in_wallet"
                pos.remaining_frac = 0.0
        if removed:
            manager._push_remote()
            if enable_ui:
                try:
                    await publish_positions(manager.snapshot())
                except Exception:
                    pass
        if not held:
            return
        try:
            from src.execution.wallet_gate import note_mint_present
            for mint in held:
                note_mint_present(mint)
        except Exception:
            log.debug("clear absent flag failed", exc_info=True)
        last: dict = {}
        opened: dict = {}
        for entry in get_ledger().entries:
            if not entry.mint:
                continue
            prev = last.get(entry.mint)
            if prev is None or float(entry.ts or 0) >= float(prev.ts or 0):
                last[entry.mint] = entry
            if entry.side == "OPEN":
                prev_o = opened.get(entry.mint)
                if prev_o is None or float(entry.ts or 0) >= float(prev_o.ts or 0):
                    opened[entry.mint] = entry
        sol_px = 0.0
        try:
            from src.data.fast_get import pump_get
            body = pump_get("https://frontend-api-v3.pump.fun/sol-price", timeout=0.8, cache_s=30)
            sol_px = float((body or {}).get("solPrice") or 0)
        except Exception:
            sol_px = 0.0
        cap = max(1, int(getattr(cfg, "max_concurrent", 1) or 1))
        changed = False
        ordered = sorted(
            held.items(),
            key=lambda item: 0 if str(getattr(opened.get(item[0]), "source", "") or "").lower() in real else (1 if item[0] in port_rows else 2),
        )
        for mint, acc in ordered:
            if any(p.status == "OPEN" and p.mint == mint for p in manager.positions.values()):
                continue
            if sum(1 for p in manager.positions.values() if p.status == "OPEN") >= cap:
                break
            raw = int(acc.get("raw") or 0)
            if raw <= 0:
                continue
            entry = opened.get(mint)
            last_e = last.get(mint)
            ui = float(acc.get("ui_amount") or 0)
            try:
                from src.observability.exec_history import get_exec_history
                open_ts = float(entry.ts or 0) if entry is not None else 0.0
                sold_bag = float(entry.token_amount or 0) if entry is not None else float(raw)
                if open_ts and get_exec_history().confirmed_exit(mint, open_ts, sold_bag):
                    continue
            except Exception:
                log.debug("confirmed exit check failed", exc_info=True)
            try:
                from src.storage.mint_flags import mint_flag
                flagged = mint_flag(mint) == "DUST" or mint in set(getattr(manager, "_no_reopen", None) or ())
            except Exception:
                flagged = mint in set(getattr(manager, "_no_reopen", None) or ())
            if flagged:
                continue
            if last_e is not None and last_e.side == "CLOSE" and (last_e.reason or "") not in fake_close:
                try:
                    get_ledger().drop_latest_close(mint)
                except Exception:
                    log.debug("drop false close failed", exc_info=True)
                last_e = opened.get(mint)
            source = ""
            entry_sol = 0.0
            symbol = ""
            name = ""
            price = None
            since = None
            entry_time = None
            fees = 0.0
            write_open = entry is None
            if entry is not None and str(entry.source or "").strip().lower() not in fake_source:
                source = str(entry.source or "").strip().lower()
                entry_sol = float(entry.cost_basis_sol or entry.entry_sol or 0)
                symbol = entry.symbol or ""
                price = entry.price_usd
                entry_time = float(entry.ts or 0) or None
                fees = float(entry.entry_fee_sol or 0) + float(entry.entry_tip_sol or 0)
                write_open = False
            row = port_rows.get(mint)
            if row:
                coin = row.get("coin") if isinstance(row.get("coin"), dict) else {}
                symbol = symbol or str(coin.get("symbol") or "")
                name = str(coin.get("name") or "")
                px = _portfolio_num(row, "tokenPriceUsd")
                if price is None and px and px > 0:
                    price = px
                pct = _portfolio_num(row, "pnlPercentage")
                if pct is not None:
                    since = pct
                if entry_sol <= 0:
                    cost_usd = _portfolio_num(row, "costBasisUsd")
                    if cost_usd and sol_px > 0:
                        entry_sol = float(cost_usd) / sol_px
            if not source:
                if mode not in real or row is None:
                    continue
                source = mode
            if entry_sol <= 0:
                entry_sol = float(getattr(cfg, "max_position_sol", 0) or 0.015)
            if mint in (getattr(manager, "_ds_giveup", None) or set()):
                continue
            pos = await manager.open_position(
                mint=mint,
                mode=PositionMode.LIVE,
                entry_sol=entry_sol,
                entry_price=1.0,
                tokens=float(raw),
                source=source,
                fees_sol=fees,
                strategy=source,
                force=True,
            )
            if not pos:
                continue
            pos.symbol = symbol or pos.symbol
            pos.name = name or getattr(pos, "name", "") or ""
            pos.strategy = source
            pos.source = source
            try:
                from src.observability.dashboard import bind_trade_caller
                bind_trade_caller(pos)
            except Exception:
                log.debug("restore caller bind failed", exc_info=True)
            if source in ("callout", "opencopy", "predict"):
                pos.stop_pct = None
                pos._from_openholds = source in ("callout", "opencopy")
            if entry_time:
                pos.entry_time = entry_time
            pos._restored_at = _time.time()
            if price and float(price) > 0:
                pos.entry_price_usd = float(price)
                pos._pump_entry_px = float(price)
                pos._entry_px_locked = True
                pos._live_px = float(price)
            if since is not None:
                pos.since_buy_pct = round(float(since), 2)
                pos.peak_since_pct = round(max(0.0, float(since)), 2)
                pos.peak_price = 1.0 + max(0.0, float(since)) / 100.0
                manager.set_price(mint, max(0.01, 1.0 + float(since) / 100.0))
            else:
                pos.peak_price = 1.0
                manager.set_price(mint, 1.0)
            if write_open:
                try:
                    get_ledger().record_open(
                        id=f"hold-{mint[:8]}-{int(_time.time())}",
                        mint=mint,
                        symbol=symbol or mint[:8],
                        mode="live",
                        source=source,
                        token_amount=float(raw),
                        entry_sol=entry_sol,
                        entry_fee_sol=fees,
                        entry_tip_sol=0.0,
                        price_sol_per_token=0.0,
                        price_usd=float(price) if price else None,
                        since_buy_pct=None if since is None else round(float(since), 2),
                        name=name,
                    )
                except Exception:
                    log.exception("restore ledger open failed")
            changed = True
            log.warning(
                "HELD RESTORED %s source=%s entry=%.4f SOL — paid wallet lists it",
                symbol or mint[:8], source, entry_sol,
            )
        if changed:
            manager._push_remote()
            if enable_ui:
                try:
                    await publish_positions(manager.snapshot())
                except Exception:
                    pass

    async def _place_dayscalp_buy(top):
        """Buy TOP1 immediately. Called from the scan task so a READY scan cannot sit idle."""
        try:
            from src.storage.mint_flags import mint_flag
            if mint_flag(top.mint) == "DUST":
                log.info("DAYSCALP skip %s — flagged DUST, not bought again", top.symbol)
                return
        except Exception:
            pass
        log.info("DAYSCALP place buy %s m5=+%.1f%% trade_enabled=%s", top.symbol, top.change_m5_pct, get_config().trade_enabled)
        quick_buy = bool(getattr(top, "quick_entry", False))

        async def _say(verdict: str, text: str) -> None:
            if not enable_ui:
                return
            try:
                await publish_decoded(
                    mint=top.mint,
                    liquidity_sol=top.liquidity_usd / 150.0,
                    mint_renounced=True,
                    freeze_renounced=True,
                    verdict=verdict,
                    size_multiplier=0.0,
                    source="dayscalp",
                    signature="",
                    reasons=[text],
                    name=top.name,
                    symbol=top.symbol,
                    website=top.url,
                )
            except Exception:
                pass

        held_reason = await _wallet_holds_reason()
        if held_reason:
            log.warning("DAYSCALP no buy call — %s", held_reason)
            await _say("SKIP", held_reason)
            return
        if not get_config().trade_enabled:
            log.warning("DAYSCALP no buy — trading disabled in Live Controls")
            await _say("SKIP", "trading is off")
            return
        held_reason = await _wallet_holds_reason()
        if held_reason:
            log.warning("DAYSCALP no buy — %s", held_reason)
            await _say("SKIP", held_reason)
            return
        held = getattr(manager, "_ds_held", None)
        if held:
            from src.execution.signer import pubkey_from_env
            from src.execution.wallet_tokens import get_token_raw_balance
            pk = pubkey_from_env()
            giveup = getattr(manager, "_ds_giveup", set()) or set()
            for mint in list(held):
                if mint in giveup:
                    held.discard(mint)
                    continue
                if any(p.status == "OPEN" and p.mint == mint for p in manager.positions.values()):
                    log.info("DAYSCALP skip buy — position still open %s", mint[:8])
                    await _say("SKIP", f"already holding {mint[:8]}")
                    return
                raw = None
                if pk:
                    try:
                        raw = await asyncio.wait_for(get_token_raw_balance(pk, mint), timeout=6.0)
                    except Exception:
                        raw = None
                zeros = shared_top.setdefault("zero_reads", {})
                if raw is None or raw > 0:
                    zeros[mint] = 0
                    log.info("DAYSCALP skip buy — wallet still holds %s (raw=%s)", mint[:8], raw)
                    await _say("SKIP", f"wallet still holds {mint[:8]}")
                    return
                zeros[mint] = int(zeros.get(mint) or 0) + 1
                if zeros[mint] < 2:
                    log.info(
                        "DAYSCALP skip buy — %s balance 0 only %d/2 times, not buying another",
                        mint[:8], zeros[mint],
                    )
                    return
                held.discard(mint)
        if any(p.status == "OPEN" for p in manager.positions.values()):
            log.info("DAYSCALP skip buy — open position exists")
            await _say("SKIP", "a coin is already open")
            return
        block_reason = None
        if not trader.jupiter_ok():
            block_reason = trader.jupiter_block_reason() or "jupiter_blocked"
        else:
            try:
                bal = await asyncio.wait_for(trader.get_sol_balance(), timeout=6.0)
            except Exception:
                bal = None
            size = float(ds_cfg.max_position_sol)
            reserve = 0.004  # token-account rent plus the sell fee
            need = size + reserve
            if bal is not None and bal < need:
                block_reason = f"low SOL {bal:.4f} < {need:.4f} (buy {size:.4f} + reserve {reserve:.3f})"
        if block_reason:
            log.warning("DAYSCALP no buy %s — %s", top.symbol, block_reason)
            if enable_ui:
                await publish_decoded(
                    mint=top.mint,
                    liquidity_sol=top.liquidity_usd / 150.0,
                    mint_renounced=True,
                    freeze_renounced=True,
                    verdict="BLOCKED",
                    size_multiplier=0.0,
                    source="dayscalp",
                    signature="",
                    reasons=list(top.reasons) + [block_reason],
                    name=top.name,
                    symbol=top.symbol,
                    website=top.url,
                )
            return
        prior = ""
        if top.reasons:
            prior = str(top.reasons[0] or "")
        if quick_buy:
            up, why = True, prior or "quick entry"
        elif prior.startswith("1s "):
            up, why = True, prior
        else:
            up, why = await bull_entry(
                top.mint,
                min_m5=float(ds_cfg.min_m5_change_pct),
                max_m5=float(ds_cfg.max_m5_change_pct),
            )
        if not up:
            log.info("DAYSCALP skip %s — %s", top.symbol, why)
            if enable_ui:
                await publish_decoded(
                    mint=top.mint,
                    liquidity_sol=top.liquidity_usd / 150.0,
                    mint_renounced=True,
                    freeze_renounced=True,
                    verdict="BLOCKED",
                    size_multiplier=0.0,
                    source="dayscalp",
                    signature="",
                    reasons=list(top.reasons) + [why],
                    name=top.name,
                    symbol=top.symbol,
                    website=top.url,
                )
            return
        if getattr(get_config(), "rugcheck_enabled", False):
            try:
                import httpx
                from src.signal.dayscalp import rugcheck_score_block
                async with httpx.AsyncClient(timeout=6.0) as rc_client:
                    score_block = await rugcheck_score_block(rc_client, top.mint)
            except Exception as exc:
                score_block = f"rugcheck report failed — {type(exc).__name__}: {exc} — no buy"
                log.warning("RUGCHECK buy-check %s mint=%s — %s", type(exc).__name__, (top.mint or "")[:8], exc)
            if score_block:
                log.info("DAYSCALP skip %s — %s", top.symbol, score_block)
                if enable_ui:
                    await publish_decoded(
                        mint=top.mint,
                        liquidity_sol=top.liquidity_usd / 150.0,
                        mint_renounced=True,
                        freeze_renounced=True,
                        verdict="BLOCKED",
                        size_multiplier=0.0,
                        source="dayscalp",
                        signature="",
                        reasons=list(top.reasons) + [score_block],
                        name=top.name,
                        symbol=top.symbol,
                        website=top.url,
                    )
                return
        from src.signal.dayscalp import read_dayscalp_entry, schedule_m1_fill
        if quick_buy:
            fresh_m5 = float(getattr(top, "pump_m5_pct", None) if getattr(top, "pump_m5_pct", None) is not None else top.change_m5_pct)
            buy_m1 = buy_s1 = buy_h1 = None
            candle_block = None
        else:
            fresh_m5, buy_m1, buy_s1, candle_block, buy_h1 = await read_dayscalp_entry(
                top.mint, float(ds_cfg.min_m5_change_pct), float(ds_cfg.max_m5_change_pct),
            )
        if fresh_m5 is None:
            fresh_m5 = float(top.change_m5_pct)
        elif not quick_buy:
            top.change_m5_pct = fresh_m5
        if candle_block:
            log.info("DAYSCALP skip %s — %s", top.symbol, candle_block)
            if enable_ui:
                await publish_decoded(
                    mint=top.mint,
                    liquidity_sol=top.liquidity_usd / 150.0,
                    mint_renounced=True,
                    freeze_renounced=True,
                    verdict="BLOCKED",
                    size_multiplier=0.0,
                    source="dayscalp",
                    signature="",
                    reasons=list(top.reasons) + [candle_block],
                    name=top.name,
                    symbol=top.symbol,
                    website=top.url,
                )
            return
        if enable_ui:
            await publish_decoded(
                mint=top.mint,
                liquidity_sol=top.liquidity_usd / 150.0,
                mint_renounced=True,
                freeze_renounced=True,
                verdict="BUYING",
                size_multiplier=1.0,
                source="dayscalp",
                signature="",
                reasons=list(top.reasons) + [f"{why} — sending buy"],
                name=top.name,
                symbol=top.symbol,
                website=top.url,
            )
        fill = None
        err = "not_filled"
        try:
            log.info(
                "DAYSCALP BUY once %s m5=+%.1f%% mint=%s — no retry if the swap fails",
                top.symbol, top.change_m5_pct, top.mint[:8],
            )
            try:
                fill = await asyncio.wait_for(
                    trader.buy(
                        mint=top.mint,
                        size_multiplier=1.0,
                        source="dayscalp",
                        ref_price=max(top.price_usd, 1e-12) / 150.0,
                    ),
                    timeout=60.0,
                )
            except asyncio.TimeoutError:
                err = "buy_timeout_60s"
                log.warning("DAYSCALP buy still pending on %s — checking the signature before MISS", top.symbol)
                fill = None
            except Exception as exc:
                err = f"{type(exc).__name__}: {exc}"
                log.exception("DAYSCALP buy raised on %s", top.symbol)
                fill = None
            if not (fill and fill.ok):
                err = getattr(fill, "error", None) or err
                log.warning("DAYSCALP buy failed once err=%s — scan continues, no second swap", err)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            log.exception("DAYSCALP buy loop crashed on %s", top.symbol)
            fill = None
        if fill and fill.ok:
            bag = getattr(manager, "_ds_held", None)
            if bag is None:
                manager._ds_held = set()
                bag = manager._ds_held
            bag.add(top.mint)
            log.info(
                "DAYSCALP FILL %s sol=%.4f sig=%s — structure SL + TP/trail",
                top.symbol, fill.sol_in, (fill.signature or "")[:16],
            )
            for pos in manager.positions.values():
                if pos.status == "OPEN" and pos.mint == top.mint:
                    pos.strategy = "dayscalp"
                    pos.symbol = top.symbol
                    pos.name = top.name
                    if getattr(top, "quick_entry", False) and getattr(top, "pump_m5_pct", None) is not None:
                        pos.quick_entry = True
                        pos.entry_m5_pct = float(top.pump_m5_pct)
                        pos.live_m5_pct = float(top.pump_m5_pct)
                        pos.stop_pct = None
                        pos._pump_m5 = float(top.pump_m5_pct)
                    else:
                        pos.entry_m5_pct = float(top.change_m5_pct)
                        pos.live_m5_pct = float(top.change_m5_pct)
                        pos.stop_pct = float(get_config().hard_stop_pct)
                    if buy_m1 is not None:
                        pos.entry_m1_pct = float(buy_m1)
                    if buy_s1 is not None:
                        pos.entry_s1_pct = float(buy_s1)
                    if buy_h1 is not None:
                        pos.entry_h1_pct = float(buy_h1)
                    if float(getattr(top, "price_usd", 0) or 0) > 0:
                        pos.entry_price_usd = float(top.price_usd)
                    pos.entry_price = 1.0
                    pos.peak_price = 1.0
                    manager.set_price(pos.mint, 1.0)
                    meta = getattr(manager, "_held_meta", None) or {}
                    meta[top.mint] = {
                        "symbol": top.symbol,
                        "name": top.name,
                        "entry_m5": float(top.change_m5_pct),
                    }
                    manager._held_meta = meta
                    manager._push_remote()
                    if not getattr(pos, "quick_entry", False):
                        pos.stop_pct = float(get_config().hard_stop_pct)
                    cfg_now = get_config()
                    manager.cfg.exit_style = getattr(cfg_now, "exit_style", "plan") or "plan"
                    manager.cfg.hard_stop_pct = float(cfg_now.hard_stop_pct)
                    manager.cfg.runner_stop_pct = float(cfg_now.runner_stop_pct)
                    pos.min_hold_seconds = float(getattr(ds_cfg, "min_hold_seconds", 15.0))
                    pos.stop_price = None
                    manager.cfg.tp_levels = tuple(cfg_now.tp_levels)
                    manager.cfg.tp_fractions = tuple(cfg_now.tp_fractions)
                    manager.cfg.trail_activate_pct = float(cfg_now.trail_activate_pct)
                    manager.cfg.trail_drawdown_pct = float(cfg_now.trail_drawdown_pct)
                    manager.cfg.time_stop_seconds = float(cfg_now.time_stop_seconds)
                    manager.cfg.time_stop_min_gain = float(cfg_now.time_stop_min_gain)
                    manager.cfg.max_concurrent = 1
                    log.info(
                        "DAYSCALP exits stop=%.0f%% hard=%.0f%% tp=%s trail@+%.0f%% dd=%.0f%% time=%.0fs",
                        pos.stop_pct * 100,
                        manager.cfg.hard_stop_pct * 100,
                        ",".join(f"+{int(x*100)}%" for x in manager.cfg.tp_levels),
                        manager.cfg.trail_activate_pct * 100,
                        manager.cfg.trail_drawdown_pct * 100,
                        manager.cfg.time_stop_seconds,
                    )
                    break
            buy_px = float(getattr(top, "price_usd", 0) or 0) or None
            if not buy_px:
                try:
                    from src.signal.dayscalp import fetch_price_usd
                    buy_px = await fetch_price_usd(top.mint)
                except Exception:
                    buy_px = None
            try:
                get_ledger().record_open(
                    id=f"ds-{top.mint[:8]}-{int(_time.time())}",
                    mint=top.mint,
                    symbol=top.symbol,
                    mode=fill.mode,
                    source="dayscalp",
                    token_amount=fill.tokens_out,
                    entry_sol=fill.sol_in,
                    entry_fee_sol=fill.fees_sol,
                    entry_tip_sol=getattr(fill, "tip_sol", 0.0) or 0.0,
                    price_sol_per_token=fill.effective_price,
                    signature=fill.signature or "",
                    m5_pct=fresh_m5,
                    m1_pct=buy_m1,
                    s1_pct=buy_s1,
                    h1_pct=buy_h1,
                    price_usd=buy_px,
                )
                if buy_m1 is None:
                    schedule_m1_fill(top.mint, _time.time() - 2)
                get_exec_history().record(
                    side="BUY",
                    mint=top.mint,
                    symbol=top.symbol,
                    sol_amount=fill.sol_in,
                    token_amount=fill.tokens_out,
                    signature=fill.signature or "",
                    status="confirmed",
                    route=fill.route or "dayscalp",
                    reason="dayscalp_entry",
                    mode=fill.mode,
                )
            except Exception:
                log.exception("ledger dayscalp open failed")
            bag = getattr(manager, "_ds_held", None)
            if bag is None:
                manager._ds_held = set()
                bag = manager._ds_held
            bag.add(top.mint)
            try:
                holds = set(trader._load_holds())
                holds.add(top.mint)
                trader._save_holds(holds)
            except Exception:
                log.debug("could not remember held mint", exc_info=True)
            if stats is not None:
                stats["buy_signals"] = int(stats.get("buy_signals") or 0) + 1
                stats["fills"] = int(stats.get("fills") or 0) + 1
                stats["paper_fills"] = int(stats.get("paper_fills") or 0) + 1
                stats["open_positions"] = sum(1 for p in manager.positions.values() if p.status == "OPEN")
                if enable_ui:
                    from src.observability.dashboard import publish_stats
                    await publish_stats(dict(stats))
                    await publish_positions(manager.snapshot())
            if enable_ui:
                await publish_decoded(
                    mint=top.mint,
                    liquidity_sol=top.liquidity_usd / 150.0,
                    mint_renounced=True,
                    freeze_renounced=True,
                    verdict="BUY",
                    size_multiplier=1.0,
                    source="dayscalp",
                    signature=fill.signature or "",
                    reasons=list(top.reasons) + [f"filled {fill.sol_in:.4f} SOL"],
                    name=top.name,
                    symbol=top.symbol,
                    website=top.url,
                )
        else:
            landed = bool(
                fill
                and getattr(fill, "signature", None)
                and getattr(fill, "error", None) != "buy_not_in_wallet"
                and int(getattr(fill, "tokens_out", 0) or 0) > 0
            )
            if landed:
                log.warning(
                    "DAYSCALP swap landed for %s but book said %s — keeping it, no next buy",
                    top.symbol, err,
                )
                bag = getattr(manager, "_ds_held", None)
                if bag is None:
                    manager._ds_held = set()
                    bag = manager._ds_held
                bag.add(top.mint)
                try:
                    holds = set(trader._load_holds())
                    holds.add(top.mint)
                    trader._save_holds(holds)
                except Exception:
                    pass
            else:
                log.warning("DAYSCALP gave up on %s (%s) — no buy retry, scan continues", top.symbol, err)
            if not landed:
                bag = getattr(manager, "_ds_held", None)
                if bag:
                    bag.discard(top.mint)
            held_mints = {p.mint for p in manager.positions.values() if p.status == "OPEN"}
            try:
                held_mints |= set(trader._load_holds())
            except Exception:
                pass
            if top.mint in held_mints:
                log.info("DAYSCALP keep BUY %s — token is already held, not MISS", top.symbol)
            elif enable_ui:
                await publish_decoded(
                    mint=top.mint,
                    liquidity_sol=top.liquidity_usd / 150.0,
                    mint_renounced=True,
                    freeze_renounced=True,
                    verdict="MISS",
                    size_multiplier=0.0,
                    source="dayscalp",
                    signature="",
                    reasons=list(top.reasons) + [f"buy failed: {err}"],
                    name=top.name,
                    symbol=top.symbol,
                    website=top.url,
                )

    async def _continuous_scan() -> None:
        """Always scan on scan_interval_sec. Never blocked by an open trade or a hung HTTP call."""
        while True:
            try:
                await _one_dayscalp_pass()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("DAYSCALP scan loop crashed — starting the next pass")
                try:
                    from src.observability.action_log import record_action
                    record_action("DAYSCALP", level="ERROR", detail=f"scan loop crashed — {type(exc).__name__}: {exc}"[:180])
                except Exception:
                    pass
                await asyncio.sleep(1)

    async def _one_dayscalp_pass() -> None:
            if t_end and _time.time() >= t_end:
                return
            cfg_now = get_config()
            scanning = bool(getattr(cfg_now, "dayscalp_enabled", False) or cfg_now.filter_mode == "dayscalp")
            if not scanning:
                if not getattr(_continuous_scan, "off_logged", False):
                    _continuous_scan.off_logged = True
                    log.warning(
                        "DAYSCALP scan off — filter=%s dayscalp_enabled=%s",
                        cfg_now.filter_mode, getattr(cfg_now, "dayscalp_enabled", False),
                    )
                await asyncio.sleep(5)
                return
            _continuous_scan.off_logged = False
            from src.config.runtime import bot_stopped
            if bot_stopped():
                await asyncio.sleep(2)
                return
            log.info("DAYSCALP scan pass tick")
            try:
                from src.observability.action_log import record_action
                record_action(
                    "DAYSCALP",
                    detail=(
                        f"SCAN looking · 5m +{ds_cfg.min_m5_change_pct:.0f}% to +{ds_cfg.max_m5_change_pct:.0f}% · "
                        f"1m same band · 1s +0.10% to +2.0% · vol >= ${ds_cfg.min_volume_m5_usd:.0f}"
                    ),
                )
            except Exception:
                pass
            top = None
            pass_task = asyncio.create_task(select_top1_dayscalp(ds_cfg))
            try:
                done, _pending = await asyncio.wait({pass_task}, timeout=22)
            except Exception as exc:
                log.exception("DAYSCALP scan error — not using the old coin")
                pass_task.cancel()
                done = set()
                try:
                    from src.observability.action_log import record_action
                    record_action("DAYSCALP", level="ERROR", detail=f"scan failed — {type(exc).__name__}: {exc}"[:180])
                except Exception:
                    pass
            if pass_task not in done:
                pass_task.cancel()
                log.warning("DAYSCALP scan pass abandoned — next pass starts now")
                top = None
            else:
                try:
                    top = pass_task.result()
                except Exception as exc:
                    log.warning("DAYSCALP scan pass failed — %s — next pass starts now", exc)
                    top = None
            open_now = [p for p in manager.positions.values() if p.status == "OPEN"]
            if top and open_now:
                held = getattr(open_now[0], "symbol", "") or open_now[0].mint[:8]
                log.info("DAYSCALP WATCH %s — already holding %s, no second buy", top.symbol, held)
            elif top:
                log.info(
                    "DAYSCALP READY %s m5=%+.1f%% vol=$%.0f mint=%s",
                    top.symbol, top.change_m5_pct, top.volume_m5_usd, top.mint[:8],
                )
                try:
                    await _place_dayscalp_buy(top)
                except Exception as exc:
                    log.exception("DAYSCALP place buy crashed")
                    record_action("DAYSCALP", level="ERROR", detail=f"buy crashed — {type(exc).__name__}"[:180])
            else:
                log.info("DAYSCALP scan empty — next pass in 1s")
            await asyncio.sleep(1)

    async def _predict_follow() -> None:
        """Buy a new Predict OPEN. Does not change the Dayscalp scan."""
        seen: set[str] = set()
        stale: set[str] = set()
        primed = False
        hold_mint = ""
        capture_next = False
        while True:
            await asyncio.sleep(3)
            if t_end and _time.time() >= t_end:
                return
            cfg_now = get_config()
            if not bool(getattr(cfg_now, "predict_enabled", False)) or not cfg_now.trade_enabled:
                continue
            try:
                from src.observability.dashboard import _predict_feed
                rows, _err = await asyncio.to_thread(_predict_feed)
            except Exception:
                log.debug("predict follow read failed", exc_info=True)
                continue
            opens = [r for r in rows if r.get("call") == "OPEN" and r.get("coinMint")]
            open_mints = {r["coinMint"] for r in opens}
            try:
                from src.observability.action_log import record_action
                from src.observability.dashboard import publish_decoded
                held_name = ""
                for pos in manager.positions.values():
                    if pos.status == "OPEN":
                        held_name = getattr(pos, "symbol", "") or pos.mint[:8]
                        break
                record_action(
                    "PREDICT",
                    detail=(
                        f"SCAN {len(rows)} coins · {len(opens)} open"
                        + (f" · holding {held_name} — next buy after it closes" if held_name else " · looking for a new open")
                    )[:220],
                )
                for row in (opens or rows)[:8]:
                    mint = row.get("coinMint") or ""
                    if not mint:
                        continue
                    await publish_decoded(
                        mint=mint,
                        liquidity_sol=0.0,
                        mint_renounced=True,
                        freeze_renounced=True,
                        verdict="WATCH" if row.get("call") == "OPEN" else "SKIP",
                        size_multiplier=0.0,
                        source="predict",
                        signature="",
                        reasons=[(row.get("reason") or row.get("call") or "predict")[:180]],
                        name=row.get("coinName") or "",
                        symbol=row.get("symbol") or "",
                        website=row.get("url") or "",
                    )
            except Exception:
                log.debug("predict scan publish failed", exc_info=True)
            mine = next(
                (
                    p for p in manager.positions.values()
                    if p.status == "OPEN" and getattr(p, "strategy", "") == "predict"
                ),
                None,
            )
            any_open = any(p.status == "OPEN" for p in manager.positions.values())
            if hold_mint and mine is None:
                try:
                    from src.observability.dashboard import mark_predicted
                    mark_predicted(hold_mint, "CLOSED")
                except Exception:
                    pass
                stale = set(open_mints)
                capture_next = True
                hold_mint = ""
                seen = set(open_mints)
                log.info("PREDICT closed — %d opens already on the board are ignored", len(stale))
                continue
            if capture_next:
                stale |= open_mints
                capture_next = False
                seen = set(open_mints)
                log.info("PREDICT waiting for a new OPEN — %d still on the board", len(stale))
                continue
            stale -= stale - open_mints
            if not primed:
                seen = set(open_mints)
                primed = True
                continue
            if mine is not None:
                hold_mint = mine.mint
                seen = set(open_mints)
                continue
            fresh = [r for r in opens if r["coinMint"] not in seen and r["coinMint"] not in stale]
            seen = set(open_mints)
            if any_open or not fresh:
                continue
            fresh.sort(key=lambda r: -(r.get("confidence") or 0))
            row = fresh[0]
            mint = row["coinMint"]
            symbol = row.get("symbol") or mint[:6]
            try:
                from src.observability.dashboard import remember_predicted
                remember_predicted(row, "BUYING")
            except Exception:
                pass
            log.info(
                "PREDICT OPEN %s 5m=%s — buying now",
                symbol,
                "none" if row.get("m5") is None else f"{float(row['m5']):+.1f}%",
            )
            try:
                from src.observability.action_log import record_action
                record_action(
                    "PREDICT",
                    detail=f"OPEN {symbol} — buying now. {row.get('reason') or ''}"[:220],
                    mint=mint,
                    symbol=symbol,
                )
            except Exception:
                pass
            fill = None
            err = ""
            for attempt in range(1, 9):
                try:
                    cfg_now = get_config()
                    if getattr(cfg_now, "tp_levels", None):
                        manager.cfg.tp_levels = tuple(cfg_now.tp_levels)
                        manager.cfg.tp_fractions = tuple(cfg_now.tp_fractions)
                    fill = await asyncio.wait_for(
                        trader.buy(mint=mint, size_multiplier=1.0, source="predict", ref_price=1e-8),
                        timeout=45.0,
                    )
                    err = "" if (fill and fill.ok) else str(getattr(fill, "error", None) or "buy_failed")
                except Exception as exc:
                    fill = None
                    err = str(exc)
                if fill and fill.ok:
                    break
                retryable = err in ("buy_quote_failed", "jupiter_swap_tx_failed") or "quote" in err
                if not retryable or attempt == 8:
                    break
                log.warning(
                    "PREDICT quote retry %s %s/8 — %s, trying again in 3s",
                    symbol, attempt, err,
                )
                try:
                    from src.observability.action_log import record_action
                    record_action(
                        "PREDICT",
                        level="WARNING",
                        detail=f"QUOTE RETRY {attempt}/8 {symbol} — {err}, no swap was sent",
                        mint=mint,
                        symbol=symbol,
                    )
                except Exception:
                    pass
                await asyncio.sleep(3)
            if not (fill and fill.ok):
                log.warning("PREDICT buy not filled %s — %s", symbol, err or getattr(fill, "error", None))
                try:
                    from src.observability.dashboard import mark_predicted
                    mark_predicted(mint, "MISSED")
                except Exception:
                    pass
                continue
            hold_mint = mint
            bag = getattr(manager, "_predict_mints", None)
            if bag is None:
                manager._predict_mints = set()
                bag = manager._predict_mints
            bag.add(mint)
            try:
                from src.observability.dashboard import remember_predicted
                remember_predicted(row, "HOLDING")
            except Exception:
                pass
            bm5 = bm1 = bs1 = bh1 = bpx = None
            try:
                from src.signal.dayscalp import read_mark_candles
                bm5, bm1, bs1, bh1, bpx = await read_mark_candles(mint)
            except Exception:
                log.debug("predict entry candles failed", exc_info=True)
            if bm5 is None and row.get("m5") is not None:
                bm5 = float(row["m5"])
            if bh1 is None and row.get("h1") is not None:
                bh1 = float(row["h1"])
            pump_px = 0.0
            try:
                import json as _json
                import urllib.request
                req = urllib.request.Request(
                    f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                        "Origin": "https://pump.fun",
                        "Referer": f"https://pump.fun/coin/{mint}",
                    },
                )
                def _entry_px():
                    from src.data.fast_get import pump_get
                    return pump_get(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}", timeout=0.9, cache_s=0)
                coin = await asyncio.wait_for(asyncio.to_thread(_entry_px), timeout=1.2)
                pump_px = float((coin or {}).get("price_usd") or 0)
            except Exception:
                pump_px = 0.0
            for pos in manager.positions.values():
                if pos.status == "OPEN" and pos.mint == mint:
                    pos.strategy = "predict"
                    pos.source = "predict"
                    pos.symbol = symbol
                    pos.name = row.get("coinName") or ""
                    pos.entry_price = 1.0
                    pos.peak_price = 1.0
                    pos.stop_pct = None
                    pos.stop_price = None
                    pos.min_hold_seconds = 0
                    pos.entry_sol = float(fill.sol_in or pos.entry_sol or 0)
                    if pump_px > 0:
                        pos._pump_entry_px = pump_px
                        pos.entry_price_usd = pump_px
                        pos.peak_since_pct = 0.0
                    if bm5 is not None:
                        pos.entry_m5_pct = float(bm5)
                        pos.live_m5_pct = float(bm5)
                        pos._pump_m5 = float(bm5)
                        pos._predict_m5_at = _time.time()
                    if bm1 is not None:
                        pos.entry_m1_pct = float(bm1)
                    if bs1 is not None:
                        pos.entry_s1_pct = float(bs1)
                    if bh1 is not None:
                        pos.entry_h1_pct = float(bh1)
                        pos.live_h1_pct = float(bh1)
                    manager.set_price(mint, 1.0)
                    break
            try:
                get_ledger().record_open(
                    id=f"pr-{mint[:8]}-{int(_time.time())}",
                    mint=mint,
                    symbol=symbol,
                    mode=fill.mode,
                    source="predict",
                    token_amount=fill.tokens_out,
                    entry_sol=fill.sol_in,
                    entry_fee_sol=fill.fees_sol,
                    entry_tip_sol=0.0,
                    price_sol_per_token=fill.effective_price,
                    signature=fill.signature or "",
                    m5_pct=bm5,
                    m1_pct=bm1,
                    s1_pct=bs1,
                    h1_pct=bh1,
                    price_usd=bpx,
                )
            except Exception:
                log.exception("predict ledger open failed")
            log.info("PREDICT FILL %s sol=%.4f — hard stop is 5m turning negative, no time stop", symbol, fill.sol_in)

    async def _callout_follow() -> None:
        """Buy Callout OPEN rows. Does not change Launch, Dayscalp, or Predict.

        No open Callout trade: buy the newest OPEN once.
        Do not buy again while that trade is above the next-trade percent.
        When the newest trade that has not already spawned a follow-up first
        reaches that percent (default -25), buy one different OPEN. The coin
        that crossed stays open and is never the next buy.
        A take-profit or dust close frees one slot and may fill one new OPEN.
        """
        primed = False
        seen_ids: set[str] = set()
        id_mint: dict[str, str] = {}
        free_slots = 0
        spawned: set[str] = set()
        openhold_ids: set[str] = set()
        crossed: set[str] = set()
        cooled: dict[str, float] = {}
        last_note = 0.0
        last_pub = 0.0

        async def _openholds_manual() -> list:
            """Clicks from the OpenHolds row. Sell now. Buy is returned for the free-slot pass."""
            try:
                from src.observability.dashboard import pop_openhold_orders, set_openhold_status
                orders = pop_openhold_orders()
            except Exception:
                log.debug("openholds orders failed", exc_info=True)
                return []
            buys = []
            for order in orders:
                mint = str(order.get("coinMint") or "")
                symbol = order.get("symbol") or mint[:6]
                if order.get("action") == "sell":
                    result = await manager.sell_mint_now(mint, force=True)
                    if result.get("ok"):
                        set_openhold_status(mint, "")
                        log.warning("OPENHOLDS manual sell %s", symbol)
                    else:
                        log.warning("OPENHOLDS manual sell %s — %s", symbol, result.get("error") or "not sold")
                    continue
                buys.append(order)
            return buys

        async def _openholds_take(room: int, forced: list | None = None) -> int:
            """A click buys that coin now. An auto buy is only the coin that just popped, inside the Settings window.

            Neither one looks at 5m. Both need a free slot. No slot shows a toast.
            """
            forced = list(forced or [])
            if not forced:
                return 0
            try:
                from src.observability.dashboard import (
                    note_openhold_seen, openhold_status, set_openhold_status, publish_alert,
                )
            except Exception:
                log.debug("openholds scan failed", exc_info=True)
                return 0

            async def _no_slot(row: dict) -> None:
                mint = str(row.get("coinMint") or "")
                symbol = row.get("symbol") or "Coin"
                if mint:
                    set_openhold_status(mint, "NO SLOT")
                log.info("OPENHOLDS %s — no free slot", symbol)
                try:
                    await publish_alert(
                        level="warn",
                        title="OpenHolds",
                        message=f"{symbol} — No free slot",
                        mint=mint or None,
                        source="callout",
                    )
                except Exception:
                    log.debug("openholds no-slot toast failed", exc_info=True)

            if room <= 0:
                for row in forced:
                    await _no_slot(row)
                return 0
            held = {pos.mint for pos in manager.positions.values() if pos.status == "OPEN" and pos.mint}
            now_cool = _time.time()
            took = 0
            for row in forced:
                if took >= room:
                    await _no_slot(row)
                    continue
                mint = str(row.get("coinMint") or "")
                if not mint or mint in held or cooled.get(mint, 0) > now_cool:
                    continue
                if openhold_status(mint) in ("BUYING", "HOLDING"):
                    continue
                symbol = row.get("symbol") or mint[:6]
                name = row.get("name") or ""
                user = str(row.get("userName") or "")
                wallet = str(row.get("wallet") or "")
                set_openhold_status(mint, "BUYING")
                note_openhold_seen(user, mint)
                if row.get("auto"):
                    log.info(
                        "OPENHOLDS %s — new open, age %.0fs. Buying.",
                        symbol, float(row.get("age") or 0),
                    )
                else:
                    log.info("OPENHOLDS %s — clicked buy. Buying this coin only.", symbol)
                fill = None
                err = ""
                copy_on = (
                    bool(getattr(get_config(), "opencopy_enabled", False))
                    or str(getattr(get_config(), "filter_mode", "") or "") == "opencopy"
                )
                buy_source = "opencopy" if copy_on else "callout"
                try:
                    fill = await trader.buy(mint=mint, size_multiplier=1.0, source=buy_source, ref_price=1.0)
                    err = "" if (fill and fill.ok) else str(getattr(fill, "error", None) or "buy_failed")
                except Exception as exc:
                    fill = None
                    err = f"{type(exc).__name__}: {exc}"[:180]
                    log.exception("OPENHOLDS buy crashed mint=%s", mint[:8])
                if not (fill and fill.ok):
                    if row.get("auto") and "wallet_sol_unreadable" in (err or ""):
                        set_openhold_status(mint, "")
                        try:
                            from src.observability.dashboard import forget_openhold_seen
                            forget_openhold_seen(user, mint)
                        except Exception:
                            pass
                        log.warning("OPENHOLDS %s — wallet read missed, still watching", symbol)
                    else:
                        set_openhold_status(mint, "MISSED")
                        log.warning("OPENHOLDS MISSED %s — %s", symbol, err or "buy_failed")
                    continue
                # Tag the trade at once from the fill. Marks and the ledger follow in the background,
                # so the exits own this coin from the first tick instead of after two API reads.
                pump_px = _fill_px_usd(fill)
                bm5 = bh1 = bm1 = bs1 = None
                tagged = False
                for pos in manager.positions.values():
                    if pos.status != "OPEN" or pos.mint != mint:
                        continue
                    pos.strategy = buy_source
                    pos.source = buy_source
                    pos.symbol = symbol
                    pos.name = name
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
                    pos._from_openholds = True
                    pos.callout_user = user
                    pos.callout_wallet = wallet
                    pos.user_holds = "checking"
                    if pump_px > 0:
                        pos._pump_entry_px = pump_px
                        pos.entry_price_usd = pump_px
                        pos._entry_px_locked = True
                    if bm5 is not None:
                        pos.entry_m5_pct = float(bm5)
                        pos.live_m5_pct = float(bm5)
                    if bm1 is not None:
                        pos.entry_m1_pct = float(bm1)
                    if bs1 is not None:
                        pos.entry_s1_pct = float(bs1)
                    if bh1 is not None:
                        pos.entry_h1_pct = float(bh1)
                        pos.live_h1_pct = float(bh1)
                    manager.set_price(mint, 1.0)
                    openhold_ids.add(pos.id)
                    tagged = True
                    try:
                        manager._push_remote()
                    except Exception:
                        log.debug("openholds entry save failed", exc_info=True)
                    break
                if not tagged:
                    set_openhold_status(mint, "MISSED")
                    log.warning("OPENHOLDS MISSED %s — fill did not open a position", symbol)
                    continue
                set_openhold_status(mint, "HOLDING")

                async def _late_marks(mint=mint, symbol=symbol, name=name, user=user, fill=fill,
                                      buy_source=buy_source, pump_px=pump_px) -> None:
                    bm5 = bh1 = bm1 = bs1 = None
                    try:
                        from src.observability.dashboard import callout_entry_marks
                        snap = await asyncio.to_thread(callout_entry_marks, mint)
                        bm5, bh1, bm1 = snap.get("m5"), snap.get("h1"), snap.get("m1")
                    except Exception:
                        log.debug("openholds entry tape failed", exc_info=True)
                    try:
                        from src.signal.dayscalp import read_mark_candles
                        dm5, dm1, ds1, dh1, dpx = await read_mark_candles(mint)
                        bm5 = bm5 if bm5 is not None else dm5
                        bm1 = bm1 if bm1 is not None else dm1
                        bh1 = bh1 if bh1 is not None else dh1
                        bs1 = ds1
                        if dpx and pump_px <= 0:
                            pump_px = float(dpx)
                    except Exception:
                        log.debug("openholds entry candles failed", exc_info=True)
                    for pos in manager.positions.values():
                        if pos.status != "OPEN" or pos.mint != mint:
                            continue
                        if pump_px > 0 and float(getattr(pos, "entry_price_usd", 0) or 0) <= 0:
                            pos._pump_entry_px = pump_px
                            pos.entry_price_usd = pump_px
                            pos._entry_px_locked = True
                        if bm5 is not None:
                            pos.entry_m5_pct = float(bm5)
                            pos.live_m5_pct = float(bm5)
                        if bm1 is not None:
                            pos.entry_m1_pct = float(bm1)
                        if bs1 is not None:
                            pos.entry_s1_pct = float(bs1)
                        if bh1 is not None:
                            pos.entry_h1_pct = float(bh1)
                            pos.live_h1_pct = float(bh1)
                        break
                    try:
                        from src.observability.ledger import get_ledger
                        get_ledger().record_open(
                            id=f"oh-{mint[:8]}-{int(_time.time())}",
                            mint=mint,
                            symbol=symbol,
                            mode=fill.mode,
                            source=buy_source,
                            token_amount=fill.tokens_out,
                            entry_sol=fill.sol_in,
                            entry_fee_sol=fill.fees_sol,
                            entry_tip_sol=0.0,
                            price_sol_per_token=fill.effective_price,
                            signature=fill.signature or "",
                            m5_pct=bm5,
                            m1_pct=bm1,
                            s1_pct=bs1,
                            h1_pct=bh1,
                            price_usd=pump_px or None,
                            since_buy_pct=0.0,
                            ath_since_pct=0.0,
                            name=name,
                            callout_user=user,
                        )
                    except Exception:
                        log.exception("openholds ledger open failed")

                asyncio.create_task(_late_marks())
                held.add(mint)
                took += 1
                log.info("OPENHOLDS HOLDING %s sol=%.4f — exit rules unchanged", symbol, fill.sol_in)
            return took

        while True:
            await asyncio.sleep(0.02)
            if t_end and _time.time() >= t_end:
                return
            try:
                from src.observability.dashboard import reconcile_openhold_status
                reconcile_openhold_status([
                    {
                        "mint": p.mint,
                        "source": getattr(p, "source", "") or "",
                        "strategy": getattr(p, "strategy", "") or "",
                        "from_openholds": bool(getattr(p, "_from_openholds", False)),
                    }
                    for p in manager.positions.values()
                    if p.status == "OPEN"
                ])
            except Exception:
                log.debug("openhold status sync failed", exc_info=True)
            cfg_now = get_config()
            try:
                from src.observability.dashboard import sync_callout_pins
                sync_callout_pins(list(manager.positions.values()))
            except Exception:
                log.debug("callout pin sync failed", exc_info=True)
            if not (
                bool(getattr(cfg_now, "callout_enabled", False))
                or bool(getattr(cfg_now, "opencopy_enabled", False))
                or str(getattr(cfg_now, "filter_mode", "") or "") == "opencopy"
            ) or not cfg_now.trade_enabled:
                continue
            try:
                from src.config.runtime import bot_stopped
                if bot_stopped():
                    continue
            except Exception:
                pass

            def _is_callout(pos) -> bool:
                kind = {getattr(pos, "source", ""), getattr(pos, "strategy", "")}
                return bool(kind & {"callout", "opencopy"})

            mine = [pos for pos in manager.positions.values() if pos.status == "OPEN" and _is_callout(pos)]
            ids = {pos.id for pos in mine}
            for pos in mine:
                if pos.mint:
                    id_mint[pos.id] = pos.mint
            if not primed:
                seen_ids = set(ids)
                primed = True
            else:
                gone = seen_ids - ids
                if gone:
                    free_slots += len(gone)
                    spawned -= gone
                    for gid in gone:
                        closed_mint = id_mint.pop(gid, "")
                        if closed_mint:
                            cooled[closed_mint] = _time.time() + 180
                            try:
                                from src.observability.dashboard import mark_callout, set_openhold_status
                                set_openhold_status(closed_mint, "")
                                mark_callout(closed_mint, "CLOSED")
                            except Exception:
                                pass
                        openhold_ids.discard(gid)
                    log.info("CALLOUT slot free — %d trade(s) left the book", len(gone))
                seen_ids = set(ids)
            now_cool = _time.time()
            cooled = {mint: until for mint, until in cooled.items() if until > now_cool}
            foreign = [
                pos for pos in manager.positions.values()
                if pos.status == "OPEN" and not _is_callout(pos)
            ]
            cap = max(1, int(getattr(cfg_now, "max_concurrent", 1) or 1))
            room = cap - len(mine)
            try:
                from src.observability.dashboard import set_openhold_room
                set_openhold_room(room)
            except Exception:
                log.debug("openholds room publish failed", exc_info=True)
            try:
                threshold = float(getattr(cfg_now, "callout_next_pct", -25.0))
            except (TypeError, ValueError):
                threshold = -25.0
            for pos in mine:
                since = getattr(pos, "since_buy_pct", None)
                if since is not None and float(since) <= threshold and pos.mint:
                    crossed.add(pos.mint)

            def _next_ok() -> tuple[bool, str]:
                """A second coin waits until the newest open Callout is at or below the next-trade line."""
                if not mine:
                    return True, ""
                newest = max(mine, key=lambda pos: float(pos.entry_time or 0))
                since = getattr(newest, "since_buy_pct", None)
                sym = (getattr(newest, "symbol", "") or "open")
                if since is None or float(since) > threshold:
                    shown = "…" if since is None else f"{float(since):+.1f}%"
                    return False, f"newest {sym} is {shown}. No new coin until it is {threshold:+.0f}% or below."
                return True, ""

            allowed, blocked = _next_ok()
            if room > 0:
                forced = await _openholds_manual()
                if forced and await _openholds_take(room, forced):
                    continue
                try:
                    from src.observability.dashboard import openhold_new_coins
                    fresh = await asyncio.to_thread(openhold_new_coins)
                except Exception:
                    log.debug("openholds new-open scan failed", exc_info=True)
                    fresh = []
                if fresh and await _openholds_take(room, fresh):
                    continue
                try:
                    from src.observability.dashboard import openhold_waiting, publish_alert
                    waited = openhold_waiting()
                except Exception:
                    waited = []
                now_toast = _time.time()
                told = getattr(_callout_follow, "told", None)
                if told is None:
                    told = {}
                    _callout_follow.told = told
                for row in waited[:4]:
                    mint = str(row.get("coinMint") or "")
                    if not mint or now_toast - float(told.get(mint) or 0) < 20:
                        continue
                    told[mint] = now_toast
                    symbol = row.get("symbol") or mint[:6]
                    why = row.get("why") or "not positive yet"
                    if "above 5" in why:
                        note = " Watching until it is 5% or below."
                    elif "pump" in why:
                        note = ""
                    else:
                        note = " Watching until it is positive."
                    try:
                        await publish_alert(
                            level="warn",
                            title="OpenHolds",
                            message=f"{symbol} — {why}.{note}",
                            mint=mint,
                            source="opencopy",
                        )
                    except Exception:
                        log.debug("openholds wait toast failed", exc_info=True)
            else:
                forced = await _openholds_manual()
                if forced:
                    await _openholds_take(0, forced)
            if _time.time() - last_pub > 8:
                last_pub = _time.time()
                try:
                    from src.observability.dashboard import _callout_rows, publish_decoded
                    scan_rows, _scan_err = await asyncio.to_thread(_callout_rows)
                    opens_n = sum(1 for row in (scan_rows or []) if row.get("call") == "OPEN")
                    try:
                        from src.observability.action_log import record_action
                        record_action(
                            "CALLOUT",
                            detail=f"SCAN {len(scan_rows or [])} wallets · {opens_n} open · {len(mine)} holding"[:220],
                        )
                    except Exception:
                        pass
                    for row in (scan_rows or [])[:12]:
                        mint = row.get("coinMint") or ""
                        if not mint:
                            continue
                        call = row.get("call") or "WAIT"
                        await publish_decoded(
                            mint=mint,
                            liquidity_sol=0.0,
                            mint_renounced=True,
                            freeze_renounced=True,
                            verdict="WATCH" if call == "OPEN" else "SKIP",
                            size_multiplier=0.0,
                            source="callout",
                            signature="",
                            reasons=[(row.get("callReason") or call)[:180]],
                            name=row.get("coinName") or "",
                            symbol=row.get("symbol") or "",
                            website="",
                        )
                except Exception:
                    log.debug("callout scan publish failed", exc_info=True)
            # Opencopy buys only from OpenHolds. The Callout page buy stays off.
            if not bool(getattr(cfg_now, "callout_enabled", False)):
                continue
            if room <= 0 or foreign:
                continue
            anchor = None
            pending = [pos for pos in mine if pos.id not in spawned]
            if pending:
                anchor = max(pending, key=lambda pos: float(pos.entry_time or 0))
            trigger = ""
            if not mine:
                trigger = "slot" if free_slots > 0 else "flat"
            elif (
                anchor is not None
                and getattr(anchor, "since_buy_pct", None) is not None
                and float(anchor.since_buy_pct) <= threshold
            ):
                trigger = "next"
            elif mine and _time.time() - last_note > 20:
                last_note = _time.time()
                since = getattr(anchor, "since_buy_pct", None) if anchor is not None else None
                shown = "…" if since is None else f"{float(since):+.1f}%"
                sym = (getattr(anchor, "symbol", "") if anchor is not None else "") or "open"
                log.info(
                    "CALLOUT hold — newest %s is %s. No new coin until it is %+.0f%% or below. It stays open.",
                    sym, shown, threshold,
                )
            if not trigger:
                continue
            try:
                from src.observability.dashboard import _callout_rows
                rows, _err = await asyncio.to_thread(_callout_rows)
            except Exception:
                log.debug("callout follow read failed", exc_info=True)
                continue
            held = {pos.mint for pos in manager.positions.values() if pos.status == "OPEN" and pos.mint}
            exclude = held | crossed | set(cooled)
            opens = [
                row for row in (rows or [])
                if row.get("call") == "OPEN" and row.get("coinMint") and row["coinMint"] not in exclude
            ]
            opens.sort(key=lambda row: -float(row.get("openedAt") or 0))
            if not opens:
                if _time.time() - last_note > 20:
                    last_note = _time.time()
                    log.info(
                        "CALLOUT %s — no different OPEN to buy (%d callout open, next at %+.0f%%)",
                        trigger, len(mine), threshold,
                    )
                continue
            picked = None
            passed = []
            for candidate in opens:
                raw_m5 = candidate.get("m5")
                try:
                    m5v = None if raw_m5 is None or raw_m5 == "" else float(raw_m5)
                except (TypeError, ValueError):
                    m5v = None
                if m5v is None or m5v <= 0:
                    passed.append((candidate.get("symbol") or candidate.get("coinMint", "")[:6], m5v))
                    continue
                picked = candidate
                break
            if picked is None:
                if _time.time() - last_note > 20:
                    last_note = _time.time()
                    sample = ", ".join(
                        f"{sym} {'missing' if val is None else f'{val:+.1f}%'}"
                        for sym, val in passed[:4]
                    )
                    log.info("CALLOUT %s — OPEN but 5m is not positive (%s). Not buying.", trigger, sample or "none")
                    try:
                        from src.observability.action_log import record_action
                        record_action(
                            "CALLOUT",
                            detail=f"SKIP 5m not positive — {sample or 'no OPEN'}"[:220],
                        )
                    except Exception:
                        pass
                continue
            if passed:
                sample = ", ".join(
                    f"{sym} {'missing' if val is None else f'{val:+.1f}%'}"
                    for sym, val in passed[:3]
                )
                log.info("CALLOUT passed %s — 5m is not positive", sample)
            row = picked
            mint = row["coinMint"]
            symbol = row.get("symbol") or mint[:6]

            def _frozen_pct(value):
                try:
                    if value is None or value == "":
                        return None
                    return float(value)
                except (TypeError, ValueError):
                    return None

            # Copy now. The feed rewrites this same row while the buy is in flight.
            frozen = {
                "m5": _frozen_pct(row.get("m5")),
                "h1": _frozen_pct(row.get("h1")),
                "m1": _frozen_pct(row.get("m1")),
                "name": row.get("coinName") or "",
                "user": str(row.get("userName") or ""),
                "wallet": str(row.get("walletAddress") or ""),
            }
            log.info(
                "CALLOUT %s %s — buying a different OPEN. 5m %+.1f%%. next-trade level %+.0f%%",
                trigger, symbol, float(frozen["m5"] or 0), threshold,
            )
            try:
                from src.observability.dashboard import remember_callout
                remember_callout(row, "BUYING")
            except Exception:
                pass
            try:
                from src.observability.action_log import record_action
                why = row.get("callReason") or row.get("call") or "OPEN"
                record_action(
                    "CALLOUT",
                    detail=f"{trigger.upper()} {symbol} — buying. {why}"[:220],
                    mint=mint,
                    symbol=symbol,
                )
            except Exception:
                pass
            if getattr(cfg_now, "tp_levels", None):
                manager.cfg.tp_levels = tuple(cfg_now.tp_levels)
                manager.cfg.tp_fractions = tuple(cfg_now.tp_fractions)
            manager.cfg.max_concurrent = cap
            fill = None
            err = ""
            for attempt in range(1, 6):
                try:
                    fill = await asyncio.wait_for(
                        trader.buy(mint=mint, size_multiplier=1.0, source="callout", ref_price=1.0),
                        timeout=45.0,
                    )
                    err = "" if (fill and fill.ok) else str(getattr(fill, "error", None) or "buy_failed")
                except Exception as exc:
                    fill = None
                    err = str(exc)
                if fill and fill.ok:
                    break
                retryable = err in ("buy_quote_failed", "jupiter_swap_tx_failed") or "quote" in err
                if not retryable or attempt == 5:
                    break
                log.warning("CALLOUT quote retry %s %s/5 — %s", symbol, attempt, err)
                await asyncio.sleep(3)
            if not (fill and fill.ok):
                log.warning("CALLOUT buy not filled %s — %s", symbol, err or "buy_failed")
                try:
                    from src.observability.dashboard import mark_callout
                    mark_callout(mint, "MISSED")
                except Exception:
                    pass
                continue
            if trigger == "next" and anchor is not None:
                spawned.add(anchor.id)
                crossed.add(anchor.mint)
            if free_slots > 0 and trigger in ("slot", "next", "flat"):
                free_slots -= 1
            pump_px = 0.0
            try:
                import json as _json
                import urllib.request
                req = urllib.request.Request(
                    f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                        ),
                        "Accept": "application/json, text/plain, */*",
                        "Origin": "https://pump.fun",
                        "Referer": f"https://pump.fun/coin/{mint}",
                    },
                )

                def _entry_px():
                    from src.data.fast_get import pump_get
                    return pump_get(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}", timeout=0.9, cache_s=0)

                coin = await asyncio.wait_for(asyncio.to_thread(_entry_px), timeout=1.2)
                pump_px = float((coin or {}).get("price_usd") or 0)
            except Exception:
                pump_px = 0.0
            if pump_px <= 0:
                try:
                    pump_px = float(row.get("tokenPrice") or 0)
                except (TypeError, ValueError):
                    pump_px = 0.0
            bm5, bh1, bm1 = frozen["m5"], frozen["h1"], frozen["m1"]
            bs1 = None
            try:
                from src.observability.dashboard import callout_entry_marks
                snap = await asyncio.to_thread(callout_entry_marks, mint)
                if snap.get("m5") is not None:
                    bm5 = snap.get("m5")
                if snap.get("h1") is not None:
                    bh1 = snap.get("h1")
                if snap.get("m1") is not None:
                    bm1 = snap.get("m1")
            except Exception:
                log.debug("callout entry tape failed", exc_info=True)
            try:
                from src.signal.dayscalp import read_mark_candles
                dm5, dm1, ds1, dh1, _dpx = await read_mark_candles(mint)
                if bm5 is None:
                    bm5 = dm5
                if bm1 is None:
                    bm1 = dm1
                if bh1 is None:
                    bh1 = dh1
                bs1 = ds1
            except Exception:
                log.debug("callout entry candles failed", exc_info=True)
            for pos in manager.positions.values():
                if pos.status == "OPEN" and pos.mint == mint:
                    pos.strategy = "callout"
                    pos.source = "callout"
                    pos.symbol = symbol
                    pos.name = frozen["name"] or row.get("coinName") or ""
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
                    pos.callout_user = frozen["user"] or str(row.get("userName") or "")
                    pos.callout_wallet = frozen["wallet"] or str(row.get("walletAddress") or "")
                    pos.user_holds = "checking"
                    if pump_px > 0:
                        pos._pump_entry_px = pump_px
                        pos.entry_price_usd = pump_px
                        pos._entry_px_locked = True
                    if bm5 is not None:
                        pos.entry_m5_pct = float(bm5)
                        pos.live_m5_pct = float(bm5)
                    if bm1 is not None:
                        pos.entry_m1_pct = float(bm1)
                    if bs1 is not None:
                        pos.entry_s1_pct = float(bs1)
                    if bh1 is not None:
                        pos.entry_h1_pct = float(bh1)
                        pos.live_h1_pct = float(bh1)
                    manager.set_price(mint, 1.0)
                    try:
                        manager._push_remote()
                    except Exception:
                        log.debug("callout entry save failed", exc_info=True)
                    if pos.entry_m5_pct is None:
                        log.warning(
                            "CALLOUT %s filled with no 5m at the buy — Bought at stays blank, take-profit still uses the price from entry",
                            symbol,
                        )
                    break
            try:
                from src.observability.dashboard import remember_callout
                held_row = dict(row)
                held_row["m5"] = bm5
                held_row["h1"] = bh1
                if pump_px > 0:
                    held_row["tokenPrice"] = pump_px
                remember_callout(held_row, "HOLDING")
            except Exception:
                pass
            try:
                from src.observability.ledger import get_ledger
                get_ledger().record_open(
                    id=f"co-{mint[:8]}-{int(_time.time())}",
                    mint=mint,
                    symbol=symbol,
                    mode=fill.mode,
                    source="callout",
                    token_amount=fill.tokens_out,
                    entry_sol=fill.sol_in,
                    entry_fee_sol=fill.fees_sol,
                    entry_tip_sol=0.0,
                    price_sol_per_token=fill.effective_price,
                    signature=fill.signature or "",
                    m5_pct=bm5,
                    m1_pct=bm1,
                    s1_pct=bs1,
                    h1_pct=bh1,
                    price_usd=pump_px or None,
                    since_buy_pct=0.0,
                    ath_since_pct=0.0,
                    name=str(frozen.get("name") or ""),
                    callout_user=str(frozen.get("user") or ""),
                )
            except Exception:
                log.exception("callout ledger open failed")
            log.info(
                "CALLOUT FILL %s sol=%.4f — no time stop, no hard stop, sell at the first take-profit",
                symbol, fill.sol_in,
            )

    scan_task = asyncio.create_task(_continuous_scan())
    from src.execution.fast_lane import get_lane
    get_lane().start()
    asyncio.create_task(_predict_follow())
    asyncio.create_task(_callout_follow())

    async def _callout_holder_watch() -> None:
        """Caller wallet, several times a second. A failed read never sells and never clears LIVE or OUT."""
        from src.observability.dashboard import (
            hold_for_mint, refresh_copy_holds, wallet_for_user, wallet_holding_mint,
            caller_identity, openholds_watch,
        )

        while True:
            await asyncio.sleep(0.05)
            mine = [
                p for p in manager.positions.values()
                if p.status == "OPEN" and (
                    getattr(p, "strategy", "") in ("callout", "opencopy")
                    or getattr(p, "source", "") in ("callout", "opencopy")
                )
            ]
            try:
                from src.observability.dashboard import bind_trade_caller
                for pos in mine:
                    bind_trade_caller(pos)
            except Exception:
                log.debug("holder caller bind failed", exc_info=True)
            rows = []
            for pos in mine:
                wallet = str(getattr(pos, "callout_wallet", "") or "").strip()
                user = str(getattr(pos, "callout_user", "") or "").strip()
                if len(wallet) < 32 and user:
                    found = str(wallet_for_user(user).get("walletAddress") or "").strip()
                    if found:
                        wallet = found
                        pos.callout_wallet = found
                if len(wallet) < 32:
                    found = wallet_holding_mint(pos.mint)
                    if len(found) >= 32:
                        wallet = found
                        pos.callout_wallet = found
                if len(wallet) < 32:
                    ident = caller_identity(pos.mint)
                    found = str(ident.get("wallet") or "").strip()
                    if len(found) >= 32:
                        wallet = found
                        pos.callout_wallet = found
                        if not user:
                            pos.callout_user = str(ident.get("user") or "")
                    else:
                        for pick in openholds_watch().get("picks") or []:
                            who = wallet_for_user(str(pick.get("user") or ""))
                            found = str(who.get("walletAddress") or "").strip()
                            if len(found) < 32:
                                continue
                            rows.append({
                                "mint": pos.mint,
                                "wallet": found,
                                "name": getattr(pos, "name", "") or "",
                                "symbol": getattr(pos, "symbol", "") or "",
                            })
                if len(wallet) < 32:
                    continue
                rows.append({
                    "mint": pos.mint,
                    "wallet": wallet,
                    "name": getattr(pos, "name", "") or "",
                    "symbol": getattr(pos, "symbol", "") or "",
                })
            try:
                from src.data.caller_stream import get_stream
                from src.observability.dashboard import start_caller_stream
                start_caller_stream()
                get_stream().set_positions(
                    [p.mint for p in mine if p.mint],
                    {r["wallet"]: str(getattr(p, "callout_user", "") or "") for r in rows for p in mine if p.mint == r["mint"]},
                )
            except Exception:
                log.debug("caller stream targets failed", exc_info=True)
            # The socket marks a sale before any poll. Act on that first, then poll.
            sell_now = False
            for pos in mine:
                state = hold_for_mint(pos.mint)
                if state == "live":
                    if not getattr(pos, "_caller_closed", False):
                        pos.user_holds = "live"
                        pos._drop_was_neg = False
                    else:
                        pos.user_holds = "out"
                elif state == "out":
                    fresh = not getattr(pos, "_caller_closed", False)
                    if fresh:
                        log.warning(
                            "USERHOLDS OUT %s — caller sold. Dropoff sells this trade if it is on",
                            getattr(pos, "symbol", "") or pos.mint[:8],
                        )
                    pos.user_holds = "out"
                    pos._caller_closed = True
                    try:
                        if float(pos.since_buy_pct) < 0:
                            pos._drop_was_neg = True
                    except (TypeError, ValueError):
                        pass
                    if fresh:
                        manager.kick_caller_closed(pos)
                        sell_now = True
                elif getattr(pos, "user_holds", "") not in ("live", "out"):
                    pos.user_holds = "checking"
            if sell_now:
                asyncio.create_task(manager.evaluate_exits())
            if rows:
                try:
                    await asyncio.to_thread(refresh_copy_holds, rows)
                except Exception:
                    log.debug("copy holder read failed", exc_info=True)

    asyncio.create_task(_callout_holder_watch())
    asyncio.create_task(_reopen_unsold())
    from src.execution.wallet_gate import set_wallet_listener, sync_wallet_forever
    set_wallet_listener(_book_bought_still_held)
    asyncio.create_task(sync_wallet_forever(2.0))
    log.info("Wallet sync → SQLite every 2s")

    async def _reclaim_stuck_rent() -> None:
        await asyncio.sleep(20)
        try:
            got = await trader.reclaim_empty_rent()
            if got > 0 and enable_ui:
                from src.observability.exec_history import get_exec_history
                await publish_exec_history(get_exec_history().wallet_recent())
        except Exception:
            log.debug("stuck rent reclaim failed", exc_info=True)

    asyncio.create_task(_reclaim_stuck_rent())

    async def _hot_tx_loop() -> None:
        """Keep a signed pump sell ready for every open coin. The exit only broadcasts it."""
        from src.execution.hot_tx import warm_open_sells
        while True:
            try:
                opens = [
                    (p.mint, int(getattr(p, "tokens", 0) or 0))
                    for p in manager.positions.values()
                    if getattr(p, "status", "") == "OPEN" and int(getattr(p, "tokens", 0) or 0) > 1000
                ]
                if opens:
                    await warm_open_sells(opens)
            except Exception:
                log.debug("hot sell refresh failed", exc_info=True)
            await asyncio.sleep(0.4)

    asyncio.create_task(_hot_tx_loop())

    async def _refresh_open_marks() -> None:
        """Update Now-live and the exit price from DexScreener. Does not need a wallet read."""
        opened = [p for p in manager.positions.values() if p.status == "OPEN"]
        if not opened:
            return
        for pos in opened:
            try:
                from src.signal.dayscalp import _pump_activity
                status, body, _note = await asyncio.wait_for(_pump_activity(pos.mint), timeout=1.2)
                if status == 200 and isinstance(body, dict):
                    def _win(key):
                        block = body.get(key)
                        if not isinstance(block, dict) or block.get("priceChangePercent") is None:
                            return None
                        try:
                            return float(block["priceChangePercent"])
                        except (TypeError, ValueError):
                            return None
                    pm5, ph1 = _win("5m"), _win("1h")
                    callout_held = (
                        getattr(pos, "strategy", "") == "callout"
                        or getattr(pos, "source", "") == "callout"
                    )
                    if "5m" in body and body.get("5m") is None:
                        pos.live_m5_pct = None
                        pos._pump_m5_null = True
                    elif pm5 is not None:
                        pos._pump_m5_null = False
                        pos.live_m5_pct = pm5
                        if not callout_held and getattr(pos, "entry_m5_pct", None) is None:
                            pos.entry_m5_pct = pm5
                            try:
                                get_ledger().fill_m5(pos.mint, pm5, float(pos.entry_time or 0))
                            except Exception:
                                pass
                    if ph1 is not None:
                        pos.live_h1_pct = ph1
                        if not callout_held and getattr(pos, "entry_h1_pct", None) is None:
                            pos.entry_h1_pct = ph1
                            try:
                                get_ledger().fill_h1(pos.mint, ph1, float(pos.entry_time or 0))
                            except Exception:
                                pass
            except Exception:
                log.debug("pump live percent failed", exc_info=True)
            px = 0.0
            try:
                import json as _json
                import urllib.request
                req = urllib.request.Request(
                    f"https://frontend-api-v3.pump.fun/coins-v2/{pos.mint}",
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                        ),
                        "Accept": "application/json, text/plain, */*",
                        "Origin": "https://pump.fun",
                        "Referer": f"https://pump.fun/coin/{pos.mint}",
                    },
                )
                def _coin():
                    from src.data.fast_get import pump_get
                    return pump_get(f"https://frontend-api-v3.pump.fun/coins-v2/{pos.mint}", timeout=0.9, cache_s=1.0)
                coin = await asyncio.wait_for(asyncio.to_thread(_coin), timeout=1.2)
                if isinstance(coin, dict):
                    px = float(coin.get("price_usd") or 0)
                    if not pos.symbol and coin.get("symbol"):
                        pos.symbol = coin.get("symbol") or pos.symbol
                    if not getattr(pos, "name", "") and coin.get("name"):
                        pos.name = coin.get("name") or ""
            except Exception:
                px = 0.0
            callout_held = (
                getattr(pos, "strategy", "") in ("callout", "opencopy")
                or getattr(pos, "source", "") in ("callout", "opencopy")
            )
            if px > 0 and callout_held and float(getattr(pos, "_stream_px_at", 0) or 0) > _time.time() - 3:
                # The trade socket already set a newer price. The coins-v2 read lags it.
                px = float(getattr(pos, "_live_px", 0) or px)
            if px > 0:
                pos._live_px = px
                if float(getattr(pos, "entry_price_usd", 0) or 0) <= 0:
                    pos.entry_price_usd = px
                    pos._pump_entry_px = px
            if callout_held:
                apply_callout_since(pos)
                continue
            predict_held = getattr(pos, "strategy", "") == "predict" or getattr(pos, "source", "") == "predict"
            quiet_px = predict_held or callout_held
            dex = {}
            if not quiet_px:
                try:
                    dex = await asyncio.wait_for(dex_held(manager, pos.mint), timeout=4)
                except Exception:
                    dex = {}
            if dex.get("symbol"):
                pos.symbol = dex["symbol"]
            if dex.get("name"):
                pos.name = dex["name"]
            if not quiet_px and px <= 0:
                px = float(dex.get("price_usd") or 0)
            if not quiet_px and dex.get("m5") is not None and getattr(pos, "live_m5_pct", None) is None:
                pos.live_m5_pct = float(dex["m5"])
            try:
                raw = int(pos.tokens or 0)
                q = await asyncio.wait_for(
                    trader.jup.quote_token_to_sol(pos.mint, raw, slippage_bps=800),
                    timeout=4,
                ) if raw else None
                sol_out = (q.out_amount / 1_000_000_000) if q and q.out_amount else 0
            except Exception:
                sol_out = 0
            entry_usd = float(getattr(pos, "entry_price_usd", 0) or 0)
            if px > 0 and entry_usd <= 0 and not quiet_px:
                pos.entry_price_usd = px
                entry_usd = px
            mark = None
            quoted = sol_out > 0 and pos.entry_sol > 0
            if quiet_px:
                if quoted:
                    pos._quote_sol = sol_out
                    if callout_held and sol_out < 0.000001:
                        pos._dust_value = True
                if callout_held:
                    apply_callout_since(pos)
                mark = None
            elif quoted:
                mark = sol_out / float(pos.entry_sol)
            elif px > 0 and entry_usd > 0:
                mark = px / entry_usd
            # A panel percent with no matching quote is not a profit. +7164% on a
            # 0.000140 SOL quote must not arm take-profit.
            if quoted and mark is not None and (mark > 3.0 or mark < 0.5) and sol_out < float(pos.entry_sol) * 0.5:
                if mark > 1.5:
                    log.info(
                        "LIVE ignore fake gain mint=%s panel would be %+.0f%% quote=%.6f SOL",
                        pos.mint[:8], (mark - 1) * 100, sol_out,
                    )
                mark = sol_out / float(pos.entry_sol)
            if mark and mark > 8:
                log.info("LIVE ignore mark %+.0f%% mint=%s — quote does not pay that", (mark - 1) * 100, pos.mint[:8])
                mark = (sol_out / float(pos.entry_sol)) if quoted else None
            if mark and mark > 0:
                pos.entry_price = 1.0
                pos.since_buy_pct = round((mark - 1) * 100, 2)
                if quoted and mark < 0.5 and float(getattr(pos, "peak_since_pct", 0) or 0) > 300:
                    pos.peak_since_pct = 0.0
                    pos.peak_price = 1.0
                else:
                    prev_peak = getattr(pos, "peak_since_pct", None)
                    pos.peak_since_pct = round(max(0.0, float(prev_peak or 0), float(pos.since_buy_pct)), 2)
                    peak_px = 1.0 + float(pos.peak_since_pct) / 100.0
                    if not pos.peak_price or pos.peak_price < peak_px:
                        pos.peak_price = peak_px
                pos._unpriced_since = None
                manager.set_price(pos.mint, mark)
                pos._dex_mark_at = _time.time()
                try:
                    get_ledger().note_open_progress(pos.mint, pos.since_buy_pct, pos.peak_since_pct)
                except Exception:
                    pass
                log.info(
                    "LIVE %s since buy %+.1f%% worth %.4f SOL",
                    pos.symbol or pos.mint[:8],
                    pos.since_buy_pct,
                    sol_out,
                )
            else:
                if getattr(pos, "_unpriced_since", None) is None:
                    pos._unpriced_since = _time.time()
                if 0 < sol_out < 0.000001:
                    pos._dust_value = True
        try:
            manager._push_remote()
        except Exception:
            log.debug("could not save open trade", exc_info=True)
        if enable_ui:
            snap = manager.snapshot()
            stats["open_positions"] = snap["open_count"]
            await publish_positions(snap)
            await publish_stats(dict(stats))

    asyncio.create_task(_bind_wallet_token())

    async def _live_price_loop() -> None:
        while True:
            await asyncio.sleep(1)
            try:
                await _refresh_open_marks()
            except Exception:
                log.debug("live price refresh failed", exc_info=True)

    asyncio.create_task(_live_price_loop())

    async def _stream_price_loop() -> None:
        """Copy trades re-price on every trade the socket prints, and the exit pass runs at once on a TP or stop."""
        last_kick = 0.0
        while True:
            await asyncio.sleep(0.1)
            try:
                from src.observability.dashboard import stream_px_usd
                mine = [
                    p for p in manager.positions.values()
                    if p.status == "OPEN" and (
                        getattr(p, "strategy", "") in ("callout", "opencopy")
                        or getattr(p, "source", "") in ("callout", "opencopy")
                    )
                ]
                if not mine:
                    continue
                cfg_now = get_config()
                levels = [float(x) for x in (getattr(manager.cfg, "tp_levels", None) or [])]
                first_tp = levels[0] * 100.0 if levels else None
                try:
                    stop = float(getattr(cfg_now, "copy_stop_pct", -15.0) or 0)
                except (TypeError, ValueError):
                    stop = 0.0
                kick = False
                for pos in mine:
                    hit = stream_px_usd(pos.mint, max_age=3.0)
                    if not hit:
                        continue
                    price, at = hit
                    if at <= float(getattr(pos, "_stream_px_at", 0) or 0):
                        continue
                    pos._stream_px_at = at
                    pos._live_px = price
                    if float(getattr(pos, "entry_price_usd", 0) or 0) <= 0:
                        pos.entry_price_usd = price
                        pos._pump_entry_px = price
                    apply_callout_since(pos)
                    since = getattr(pos, "since_buy_pct", None)
                    if since is None:
                        continue
                    if first_tp is not None and float(since) >= first_tp:
                        kick = True
                    if stop < 0 and float(since) <= stop:
                        kick = True
                if kick and _time.time() - last_kick >= 0.25:
                    last_kick = _time.time()
                    asyncio.create_task(manager.evaluate_exits())
            except Exception:
                log.debug("stream price loop failed", exc_info=True)

    asyncio.create_task(_stream_price_loop())

    paused_mode = None
    while True:
        if t_end and _time.time() >= t_end:
            break
        mode = get_config().filter_mode
        if mode != "dayscalp":
            if paused_mode != mode:
                log.info("Dayscalp scanner off — Live Controls filter=%s", mode)
                try:
                    from src.observability.action_log import record_action
                    record_action(
                        mode.upper(),
                        level="INFO",
                        detail=f"Live Controls filter={mode}. Dayscalp scan is off.",
                    )
                except Exception:
                    pass
                paused_mode = mode
            await asyncio.sleep(5)
            continue
        paused_mode = None
        if not get_config().trade_enabled:
            await asyncio.sleep(3)
            continue

        # Buy first. Wallet dust checks must not block the entry.
        open_positions = [p for p in manager.positions.values() if p.status == "OPEN"]
        holding = bool(open_positions)
        top = shared_top.get("top")
        top_age = _time.time() - float(shared_top.get("ts") or 0)
        fresh = bool(top) and top_age <= max(120.0, float(ds_cfg.scan_interval_sec) * 4)

        if holding:
            pos0 = open_positions[0]
            age = _time.time() - float(getattr(pos0, "entry_time", 0) or 0)
            if age > 90:
                try:
                    from src.execution.signer import pubkey_from_env
                    from src.execution.wallet_tokens import get_token_holding
                    pk = pubkey_from_env()
                    held = await asyncio.wait_for(get_token_holding(pk, pos0.mint), timeout=8) if pk else None
                    if held is not None and int(held.get("raw") or 0) <= 0:
                        log.warning(
                            "ACTION ALREADY SOLD %s — wallet balance is 0, book cleared, next coin",
                            pos0.symbol or pos0.mint[:8],
                        )
                        manager._sell_cooldown.pop(pos0.mint, None)
                        if not float(getattr(pos0, "_sol_out", 0) or 0):
                            sold = manager._confirmed_wallet_exit(pos0)
                            got = float((sold or {}).get("sol") or 0)
                            if got > 0:
                                pos0._sol_out = got
                                pos0._last_sell_sig = str((sold or {}).get("sig") or "")
                        await manager.close_position(pos0.id, "already_sold", exit_price=float(pos0.entry_price or 1.0), frac=1.0)
                        trader._save_holds(set())
                        manager._push_remote()
                        if enable_ui:
                            await publish_positions(manager.snapshot())
                        continue
                except Exception:
                    log.debug("wallet sold-check failed", exc_info=True)
            if _time.time() - float(getattr(manager, "_held_lookup_at", 0) or 0) > 5:
                manager._held_lookup_at = _time.time()
                try:
                    dex = await dex_held(manager, pos0.mint)
                    log.info(
                        "HELD DEX mint=%s symbol=%s name=%s m5=%s px=%s",
                        pos0.mint[:8], dex.get("symbol") or "—", dex.get("name") or "—",
                        dex.get("m5"), dex.get("price_usd"),
                    )
                    if dex.get("symbol"):
                        pos0.symbol = dex["symbol"]
                    if dex.get("name"):
                        pos0.name = dex["name"]
                    if dex.get("m5") is not None and getattr(pos0, "live_m5_pct", None) is None:
                        pos0.live_m5_pct = float(dex["m5"])
                    if dex.get("token_ath_pct") is not None:
                        prev = getattr(pos0, "token_ath_pct", None)
                        pos0.token_ath_pct = float(dex["token_ath_pct"]) if prev is None else max(float(prev), float(dex["token_ath_pct"]))
                    # Wallet is the balance after a partial sale. Bought SOL stays the original.
                    try:
                        from src.execution.signer import pubkey_from_env
                        from src.execution.wallet_tokens import get_token_holding
                        pk = pubkey_from_env()
                        held = await asyncio.wait_for(get_token_holding(pk, pos0.mint), timeout=8) if pk else None
                    except Exception:
                        held = None
                    if held is not None:
                        raw_now = int(held.get("raw") or 0)
                        ui_now = float(held.get("ui_amount") or 0)
                        if raw_now > 0:
                            pos0._wallet_raw = raw_now
                            pos0._wallet_ui = ui_now
                            pos0._ui_amount = ui_now
                        orig = int(getattr(pos0, "_orig_raw", 0) or 0)
                        if orig <= 0 and int(pos0.tokens or 0) > 0:
                            pos0._orig_raw = int(pos0.tokens)
                            orig = int(pos0.tokens)
                        if orig <= 0 and raw_now > 0:
                            pos0._orig_raw = raw_now
                            pos0.tokens = float(raw_now)
                            orig = raw_now
                        if orig > 0 and raw_now > 0:
                            wallet_frac = min(1.0, raw_now / float(orig))
                            book = float(pos0.remaining_frac or 1)
                            if book > 0.98 or wallet_frac + 0.02 < book:
                                pos0.remaining_frac = wallet_frac
                    raw_left = int(getattr(pos0, "_wallet_raw", 0) or 0)
                    if raw_left <= 0:
                        raw_left = int(float(pos0.tokens or 0) * float(pos0.remaining_frac or 1))
                    sol_out = 0.0
                    if raw_left > 0:
                        try:
                            q = await asyncio.wait_for(
                                trader.jup.quote_token_to_sol(pos0.mint, raw_left, slippage_bps=800),
                                timeout=8,
                            )
                            sol_out = (q.out_amount / 1_000_000_000) if q and q.out_amount else 0.0
                        except Exception:
                            log.info("HELD quote failed mint=%s — using Dex price", pos0.mint[:8])
                            sol_out = 0.0
                    px = float(dex.get("price_usd") or 0)
                    ui = float(getattr(pos0, "_ui_amount", 0) or 0)
                    if sol_out < 0.001 and px > 0 and ui > 0:
                        sol_usd = float(getattr(manager, "_sol_usd", 0) or 0)
                        if sol_usd <= 0:
                            try:
                                import httpx
                                async with httpx.AsyncClient(timeout=6.0) as client:
                                    rr = await client.get(
                                        "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112"
                                    )
                                if rr.status_code == 200:
                                    for pair in (rr.json() or {}).get("pairs") or []:
                                        try:
                                            sol_usd = float(pair.get("priceUsd") or 0)
                                        except (TypeError, ValueError):
                                            sol_usd = 0
                                        if sol_usd > 10:
                                            break
                                if sol_usd > 10:
                                    manager._sol_usd = sol_usd
                            except Exception:
                                sol_usd = 0
                        if sol_usd > 10:
                            sol_out = (ui * px) / sol_usd
                    if sol_out >= 0.001 and pos0.entry_sol > 0:
                        basis = float(pos0.entry_sol) * float(pos0.remaining_frac or 1)
                        if basis < 0.000001:
                            basis = float(pos0.entry_sol)
                        mark = sol_out / basis
                        if mark < 0.05:
                            log.info("HELD mark %.4f ignored mint=%s — not a real price", mark, pos0.mint[:8])
                        else:
                            callout_book = (
                                getattr(pos0, "strategy", "") == "callout"
                                or getattr(pos0, "source", "") == "callout"
                            )
                            if callout_book:
                                apply_callout_since(pos0)
                            else:
                                pos0.entry_price = 1.0
                                pos0.since_buy_pct = round((mark - 1) * 100, 2)
                                prev_peak = getattr(pos0, "peak_since_pct", None)
                                pos0.peak_since_pct = round(max(0.0, float(prev_peak or 0), float(pos0.since_buy_pct)), 2)
                                if not pos0.peak_price or pos0.peak_price < 0.2 or pos0.peak_price < mark:
                                    pos0.peak_price = max(mark, 1.0 if (pos0.peak_price or 0) < 0.2 else pos0.peak_price)
                                manager.set_price(pos0.mint, mark)
                            pos0._dex_mark_at = _time.time()
                            log.info(
                                "HELD %s SINCE BUY %+.1f%% worth %.4f SOL (paid %.4f at %s) 5m=%s",
                                pos0.symbol or pos0.mint[:8],
                                pos0.since_buy_pct,
                                sol_out,
                                pos0.entry_sol,
                                _time.strftime("%H:%M:%S", _time.localtime(pos0.entry_time)),
                                f"{pos0.live_m5_pct:+.1f}%" if getattr(pos0, "live_m5_pct", None) is not None else "—",
                            )
                    if stats is not None:
                        known = len(open_positions) + len(manager.closed)
                        if int(stats.get("fills") or 0) < known:
                            stats["fills"] = known
                            stats["paper_fills"] = known
                        if int(stats.get("buy_signals") or 0) < known:
                            stats["buy_signals"] = known
                        await publish_stats(dict(stats))
                    manager._push_remote()
                except Exception as exc:
                    log.warning("HELD DEX lookup failed mint=%s — %s", pos0.mint[:8], type(exc).__name__)
            if _time.time() - float(getattr(manager, "_hold_log_at", 0) or 0) > 15:
                manager._hold_log_at = _time.time()
                since = getattr(pos0, "since_buy_pct", None)
                log.info(
                    "DAYSCALP holding %s — no new buy until sold (%s since buy)",
                    pos0.symbol or pos0.mint[:8],
                    f"{since:+.1f}%" if since is not None else "price not in yet",
                )
                try:
                    from src.observability.action_log import record_action
                    record_action(
                        "DAYSCALP",
                        level="WARNING",
                        detail=(
                            f"no new buy — still holding {pos0.symbol or pos0.mint[:8]} "
                            f"({f'{since:+.1f}%' if since is not None else 'price not in yet'})"
                        ),
                        mint=pos0.mint,
                        symbol=getattr(pos0, "symbol", "") or "",
                    )
                except Exception:
                    pass
            if enable_ui:
                await publish_positions(manager.snapshot())

        await asyncio.sleep(1)

    scan_task.cancel()
    log.info("DAYSCALP loop ended")



def main() -> None:
    from src.config.file_config import apply_config_file, push_runtime_from_config
    parser = argparse.ArgumentParser(description="SOL-SNIPER v1.2")
    parser.add_argument(
        "--mode",
        choices=["demo", "live-rpc", "live-feed", "paper", "live"],
        default="demo",
    )
    parser.add_argument("--rpc", action="append", default=[], help="Extra HTTP RPC URL")
    parser.add_argument("--duration", type=float, default=60.0, help="live-feed duration seconds; 0 = run forever until Ctrl+C")
    parser.add_argument("--config", type=str, default=None, help="path to sniper.yaml config file")
    parser.add_argument(
        "--max-decodes",
        type=int,
        default=30,
        help="Max getTransaction calls per minute (rate limit)",
    )
    parser.add_argument(
        "--no-ui",
        action="store_true",
        help="Disable the live web dashboard",
    )
    parser.add_argument(
        "--trade-mode",
        choices=["paper", "live"],
        default="paper",
        help="paper (default, simulated fills) or live (skeleton, no txs until armed)",
    )
    args = parser.parse_args()
    from src.config.runtime import get_config, update_config, load_runtime_state
    # Read the last Live Controls before yaml is applied. Applying the file
    # used to save filter_mode=launch over the Dayscalp the user had set.
    saved_runtime = load_runtime_state()
    file_cfg = apply_config_file(args.config)
    from src.execution.signer import apply_uploaded_wallet_env
    apply_uploaded_wallet_env()
    push_runtime_from_config(file_cfg)
    if get_config().filter_mode in ("launch", "dayscalp") and not saved_runtime:
        from src.config.file_config import mode_exit_profile
        style = str(((file_cfg or {}).get("trading") or {}).get("exit_style") or "ath")
        update_config({"exit_style": style})
        update_config(mode_exit_profile(get_config().filter_mode))
    if saved_runtime:
        kept_mode = str(saved_runtime.get("filter_mode") or "").strip()
        # The file written at Apply and again at Restart is the running config.
        # Do not replace it with ath, plan, or the yaml defaults.
        saved_runtime["defaults_version"] = 2
        update_config(saved_runtime)
        if kept_mode:
            update_config({"filter_mode": kept_mode})
        log.info(
            "Restored settings from last run — filter=%s style=%s trade=%s max_pos=%.4f route=%s",
            get_config().filter_mode,
            getattr(get_config(), "exit_style", "ath"),
            get_config().trade_enabled,
            float(get_config().max_position_sol or 0),
            getattr(get_config(), "tx_route", ""),
        )
        __import__("os").environ["SOL_SNIPER_SETTINGS_RESTORED"] = "1"
    else:
        from src.storage.supabase_store import get_store
        remote_settings = get_store().get("runtime_config")
        if remote_settings:
            remote_settings = dict(remote_settings)
            kept_mode = str(remote_settings.get("filter_mode") or "").strip()
            update_config(remote_settings)
            if kept_mode:
                update_config({"filter_mode": kept_mode})
            log.info(
                "Saved settings restored — filter=%s style=%s",
                get_config().filter_mode,
                get_config().exit_style,
            )
            __import__("os").environ["SOL_SNIPER_SETTINGS_RESTORED"] = "1"
        else:
            remote_settings = {}
    restored = saved_runtime if saved_runtime else (remote_settings if saved_runtime is not None else {})
    from src.config.runtime import ensure_source_switches
    ensure_source_switches(restored if isinstance(restored, dict) else {})
    # CLI / env trade mode overrides
    if not args.trade_mode or args.trade_mode == "paper":
        tm = __import__("os").environ.get("SOL_SNIPER_TRADE_MODE", "").strip()
        if tm in ("paper", "live"):
            args.trade_mode = tm
    fm = __import__("os").environ.get("SOL_SNIPER_FILTER_MODE", "").strip()
    if fm and not saved_runtime and not (get_config().filter_mode or "").strip():
        from src.config.runtime import update_config
        update_config({"filter_mode": fm})


    log.info("SOL-SNIPER starting | version=2.1.0 | mode=%s", args.mode)

    if args.mode == "demo":
        demo_hard_filters()
    elif args.mode == "live-rpc":
        asyncio.run(live_rpc_demo(extra_endpoints=args.rpc or None))
    elif args.mode == "live-feed":
        asyncio.run(
            live_feed_demo(
                duration=args.duration,
                max_decodes_per_min=args.max_decodes,
                enable_ui=not args.no_ui,
                trade_mode=args.trade_mode,
                file_cfg=file_cfg,
            )
        )
    elif args.mode == "paper":
        # Paper mode = full pipeline with simulated fills
        asyncio.run(
            live_feed_demo(
                duration=args.duration,
                max_decodes_per_min=args.max_decodes,
                enable_ui=not args.no_ui,
                trade_mode="paper",
                file_cfg=file_cfg,
            )
        )
    elif args.mode == "live":
        log.error("Full live trading disabled until paper validation. Use --mode live-feed --trade-mode paper")
        sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
