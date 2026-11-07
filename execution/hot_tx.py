"""Signed buys kept warm. A coin that is not on a Pump curve is not sent to PumpPortal."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional

log = logging.getLogger("sol-sniper.hot")

_buys: Dict[str, dict] = {}
_sells: Dict[str, dict] = {}
_off_curve: Dict[str, float] = {}
_sell_inflight: Dict[str, float] = {}
_jup_buy_builder = None
_jup_sell_builder = None


def set_jup_buy_builder(fn) -> None:
    global _jup_buy_builder
    _jup_buy_builder = fn


def set_jup_sell_builder(fn) -> None:
    global _jup_sell_builder
    _jup_sell_builder = fn


def remember_off_curve(mint: str) -> None:
    _off_curve[str(mint or "")] = time.time()


def forget_off_curve(mint: str) -> None:
    _off_curve.pop(str(mint or ""), None)


def off_curve(mint: str) -> bool:
    ts = _off_curve.get(str(mint or ""))
    return ts is not None and time.time() - ts < 120.0


def peek_sell(mint: str, raw: int) -> Optional[Dict]:
    row = _sells.get(str(mint or ""))
    if not row or int(raw or 0) <= 1000:
        return None
    if time.time() - float(row.get("ts") or 0) > 45.0:
        return None
    if int(row.get("raw") or 0) != int(raw):
        return None
    if time.time() < float(_sell_inflight.get(mint) or 0):
        return None
    return row


def hold_sell(mint: str) -> None:
    _sell_inflight[str(mint or "")] = time.time() + 8.0
    _sells.pop(str(mint or ""), None)


async def warm_sell(mint: str, raw: int) -> None:
    mint = str(mint or "")
    raw = int(raw or 0)
    if not mint or raw <= 1000:
        return
    if off_curve(mint):
        return
    if time.time() < float(_sell_inflight.get(mint) or 0):
        return
    have = _sells.get(mint)
    if have and int(have.get("raw") or 0) == raw and time.time() - float(have.get("ts") or 0) < 12.0:
        return
    try:
        from src.execution.pump_curve import pump_sell_tx
        b64, minimum, why = await pump_sell_tx(mint, raw, slippage_bps=1500, quiet=True)
    except Exception:
        log.debug("hot sell build failed", exc_info=True)
        return
    if not b64:
        if why in ("not_on_curve", "graduated", "not_sol_quote"):
            remember_off_curve(mint)
        return
    _sells[mint] = {"b64": b64, "raw": raw, "min": int(minimum or 0), "ts": time.time()}


async def warm_buy(mint: str, sol: float) -> None:
    mint = str(mint or "")
    sol = float(sol or 0)
    if not mint or sol <= 0:
        return
    have = _buys.get(mint)
    fresh = 8.0 if have and have.get("kind") == "jupiter" else 0.8
    if have and abs(float(have.get("sol") or 0) - sol) < 1e-6 and time.time() - float(have.get("ts") or 0) < fresh:
        return
    b64, minimum, why = "", 0, ""
    if not off_curve(mint):
        try:
            from src.execution.pump_curve import pump_buy_tx
            b64, minimum, why = await pump_buy_tx(mint, sol, slippage_bps=1500, quiet=True)
        except Exception:
            log.debug("hot buy build failed", exc_info=True)
            return
    else:
        why = "not_on_curve"
    if b64:
        _buys[mint] = {"b64": b64, "sol": sol, "min_tokens": int(minimum or 0), "ts": time.time(), "kind": "pump"}
        return
    if why in ("not_on_curve", "graduated", "not_sol_quote"):
        remember_off_curve(mint)
        if _jup_buy_builder is None:
            return
        try:
            built = await asyncio.wait_for(_jup_buy_builder(mint, sol), timeout=2.2)
        except Exception:
            log.debug("hot jupiter buy failed", exc_info=True)
            return
        if built and built.get("b64"):
            built["ts"] = time.time()
            built["sol"] = sol
            built["kind"] = "jupiter"
            _buys[mint] = built


async def warm_open_sells(opens) -> None:
    await asyncio.gather(*(warm_sell(mint, raw) for mint, raw in (opens or [])), return_exceptions=True)
