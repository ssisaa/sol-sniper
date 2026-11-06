"""Direct Pump.fun bonding-curve buy and sell.

Builds the current buy_exact_quote_in_v2 / sell_v2 instruction locally.
No Jupiter quote. The caller sends the signed transaction on the paid RPC.
A graduated curve, or a curve not quoted in SOL, returns a reason so the
caller can fall back to Jupiter.
"""
from __future__ import annotations

import base64
import logging
import struct
import time
from typing import Optional, Tuple

import httpx

log = logging.getLogger("sol-sniper.pump")

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
FEE_PROGRAM = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
SYSTEM = "11111111111111111111111111111111"
WSOL = "So11111111111111111111111111111111111111112"
COMPUTE = "ComputeBudget111111111111111111111111111111"
BUY_EXACT_QUOTE = bytes.fromhex("c2ab1c46684d5b2f")
SELL_V2 = bytes.fromhex("5df6823ce7e940b2")
PUBLIC_RPCS = (
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
)
_GLOBAL = {"ts": 0.0, "fee": "", "buyback": "", "fee_bps": 100}
_BLOCK = {"ts": 0.0, "hash": ""}
_CURVE: dict = {}


def _pk(addr: str):
    from solders.pubkey import Pubkey
    return Pubkey.from_string(addr)


def _pda(seeds: list, program: str):
    from solders.pubkey import Pubkey
    return Pubkey.find_program_address(seeds, _pk(program))[0]


def _ata(owner, mint, token_program):
    return _pda([bytes(owner), bytes(token_program), bytes(mint)], ATA_PROGRAM)


def _meta(pubkey, writable: bool = False, signer: bool = False):
    from solders.instruction import AccountMeta
    return AccountMeta(pubkey, signer, writable)


def _ix(program: str, data: bytes, accounts: list):
    from solders.instruction import Instruction
    return Instruction(_pk(program), data, accounts)


def _cu_limit(units: int):
    return _ix(COMPUTE, bytes([2]) + struct.pack("<I", units), [])


def _cu_price(micro_lamports: int):
    return _ix(COMPUTE, bytes([3]) + struct.pack("<Q", int(micro_lamports)), [])


def _create_ata(payer, ata, owner, mint, token_program):
    return _ix(ATA_PROGRAM, bytes([1]), [
        _meta(payer, True, True),
        _meta(ata, True, False),
        _meta(owner, False, False),
        _meta(mint, False, False),
        _meta(_pk(SYSTEM), False, False),
        _meta(token_program, False, False),
    ])


def _sync_native(ata, token_program):
    return _ix(str(token_program), bytes([17]), [_meta(ata, True, False)])


def _close_account(ata, dest, owner, token_program):
    return _ix(str(token_program), bytes([9]), [
        _meta(ata, True, False),
        _meta(dest, True, False),
        _meta(owner, True, True),
    ])


def _transfer(src, dest, lamports: int):
    return _ix(SYSTEM, struct.pack("<IQ", 2, int(lamports)), [
        _meta(src, True, True),
        _meta(dest, True, False),
    ])


async def _rpc(method: str, params: list):
    """Buy and sell curve reads. The paid primary only. A public RPC is not on this path."""
    import asyncio
    import json

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        from src.execution.wallet_tokens import primary_url
        paid = primary_url()
    except Exception:
        paid = ""
    if not paid:
        return None

    def _post(url: str):
        from src.execution.tx_route import rpc_post
        status, text = rpc_post(url, payload, timeout=0.8)
        if status != 200:
            return None
        body = json.loads(text or "{}")
        if not isinstance(body, dict) or body.get("error"):
            return None
        return body.get("result")

    try:
        return await asyncio.to_thread(_post, paid)
    except Exception:
        return None


def _raw_account(item) -> bytes:
    if not item:
        return b""
    data = item.get("data")
    if isinstance(data, list) and data:
        return base64.b64decode(data[0])
    return b""


def _u64(buf: bytes, offset: int) -> int:
    return struct.unpack_from("<Q", buf, offset)[0]


async def _fees() -> Tuple[str, str, int]:
    now = time.time()
    if _GLOBAL["fee"] and now - float(_GLOBAL["ts"]) < 60:
        return _GLOBAL["fee"], _GLOBAL["buyback"], int(_GLOBAL["fee_bps"])
    global_pda = _pda([b"global"], PUMP)
    result = await _rpc("getAccountInfo", [str(global_pda), {"encoding": "base64"}])
    raw = _raw_account((result or {}).get("value"))
    from solders.pubkey import Pubkey
    fee = str(Pubkey.from_bytes(raw[41:73])) if len(raw) >= 73 else ""
    buyback = str(Pubkey.from_bytes(raw[741:773])) if len(raw) >= 773 else ""
    protocol = _u64(raw, 105) if len(raw) >= 113 else 95
    creator = _u64(raw, 154) if len(raw) >= 162 else 5
    fee_bps = max(1, min(2000, int(protocol) + int(creator)))
    if fee and buyback:
        _GLOBAL.update(ts=now, fee=fee, buyback=buyback, fee_bps=fee_bps)
        return fee, buyback, fee_bps
    if _GLOBAL["fee"] and _GLOBAL["buyback"]:
        return _GLOBAL["fee"], _GLOBAL["buyback"], int(_GLOBAL["fee_bps"])
    return fee, buyback, fee_bps


def forget_curve(mint: str) -> None:
    _CURVE.pop(str(mint or ""), None)


async def _curve(mint: str):
    now = time.time()
    cached = _CURVE.get(mint)
    if cached:
        age = now - float(cached.get("ts") or 0)
        why = cached.get("why") or ""
        # A confirmed graduation stays off the curve. A failed read is not that.
        if why == "graduated" and age < 120:
            return cached.get("curve"), why
        if why == "not_on_curve" and cached.get("sure") and age < 20:
            return cached.get("curve"), why
        if age < 3.0 and why not in ("curve_unread",):
            return cached.get("curve"), why
    mint_pk = _pk(mint)
    curve = _pda([b"bonding-curve", bytes(mint_pk)], PUMP)
    result = await _rpc("getMultipleAccounts", [[str(curve), mint], {"encoding": "base64"}])

    def _keep(curve_row, why: str, sure: bool = False):
        _CURVE[mint] = {"ts": time.time(), "curve": curve_row, "why": why, "sure": sure}
        if len(_CURVE) > 40:
            oldest = min(_CURVE, key=lambda key: float(_CURVE[key].get("ts") or 0))
            _CURVE.pop(oldest, None)
        return curve_row, why

    if result is None:
        if cached and cached.get("curve"):
            return cached.get("curve"), ""
        return await _curve_from_site(mint)
    values = result.get("value") if isinstance(result, dict) else None
    if not isinstance(values, list) or len(values) < 2 or not values[0]:
        return _keep(None, "not_on_curve", sure=True)
    raw = _raw_account(values[0])
    if len(raw) < 115:
        return _keep(None, "curve_unreadable")
    if raw[48] != 0:
        return _keep(None, "graduated", sure=True)
    from solders.pubkey import Pubkey
    creator = Pubkey.from_bytes(raw[49:81])
    quote = Pubkey.from_bytes(raw[83:115])
    quote_s = str(quote)
    # A SOL curve stores the system address. The buy instruction still uses WSOL.
    if quote_s not in (WSOL, SYSTEM):
        return _keep(None, "not_sol_quote")
    owner = str(((values[1] or {}).get("owner")) or TOKEN)
    token_program = _pk(TOKEN_2022 if owner == TOKEN_2022 else TOKEN)
    return _keep({
        "curve": curve,
        "creator": creator,
        "quote": _pk(WSOL),
        "virtual_token": _u64(raw, 8),
        "virtual_sol": _u64(raw, 16),
        "token_program": token_program,
        "quote_program": _pk(TOKEN),
        "mint": mint_pk,
    }, "")


def _tokens_for_sol(sol_lamports: int, virtual_token: int, virtual_sol: int, fee_bps: int) -> int:
    net = sol_lamports * 10_000 // (10_000 + max(fee_bps, 0))
    if virtual_sol + net <= 0:
        return 0
    return virtual_token * net // (virtual_sol + net)


def _sol_for_tokens(tokens: int, virtual_token: int, virtual_sol: int, fee_bps: int) -> int:
    if virtual_token + tokens <= 0:
        return 0
    gross = virtual_sol * tokens // (virtual_token + tokens)
    return gross * max(0, 10_000 - fee_bps) // 10_000


async def _blockhash() -> str:
    """A processed blockhash stays valid for about a minute. Do not fetch one per buy."""
    now = time.time()
    if _BLOCK["hash"] and now - float(_BLOCK["ts"]) < 25:
        return str(_BLOCK["hash"])
    block = await _rpc("getLatestBlockhash", [{"commitment": "processed"}])
    fresh = ((block or {}).get("value") or {}).get("blockhash") or ""
    if fresh:
        _BLOCK.update(ts=now, hash=fresh)
        return fresh
    if _BLOCK["hash"] and now - float(_BLOCK["ts"]) < 55:
        return str(_BLOCK["hash"])
    return ""


async def _compile(instructions: list) -> str:
    from solders.hash import Hash
    from solders.message import MessageV0
    from solders.transaction import VersionedTransaction
    from src.execution.signer import load_keypair_from_env

    blockhash = await _blockhash()
    if not blockhash:
        raise RuntimeError("no blockhash")
    kp, _pubkey = load_keypair_from_env()
    message = MessageV0.try_compile(kp.pubkey(), instructions, [], Hash.from_string(blockhash))
    signed = VersionedTransaction(message, [kp])
    return base64.b64encode(bytes(signed)).decode("ascii")


def _quote_atas(curve, user, creator, fee, buyback, quote_program):
    quote = _pk(WSOL)
    return {
        "fee": _ata(fee, quote, quote_program),
        "buyback": _ata(buyback, quote, quote_program),
        "curve": _ata(curve, quote, quote_program),
        "user": _ata(user, quote, quote_program),
        "creator": _ata(_pda([b"creator-vault", bytes(creator)], PUMP), quote, quote_program),
        "volume": _ata(_pda([b"user_volume_accumulator", bytes(user)], PUMP), quote, quote_program),
    }


def _buy_accounts(user, curve_info, fee: str, buyback: str):
    pump = _pk(PUMP)
    quote_program = curve_info["quote_program"]
    atas = _quote_atas(curve_info["curve"], user, curve_info["creator"], _pk(fee), _pk(buyback), quote_program)
    base_user = _ata(user, curve_info["mint"], curve_info["token_program"])
    return [
        _meta(_pda([b"global"], PUMP)),
        _meta(curve_info["mint"]),
        _meta(curve_info["quote"]),
        _meta(curve_info["token_program"]),
        _meta(quote_program),
        _meta(_pk(ATA_PROGRAM)),
        _meta(_pk(fee), True),
        _meta(atas["fee"], True),
        _meta(_pk(buyback), True),
        _meta(atas["buyback"], True),
        _meta(curve_info["curve"], True),
        _meta(_ata(curve_info["curve"], curve_info["mint"], curve_info["token_program"]), True),
        _meta(atas["curve"], True),
        _meta(user, True, True),
        _meta(base_user, True),
        _meta(atas["user"], True),
        _meta(_pda([b"creator-vault", bytes(curve_info["creator"])], PUMP), True),
        _meta(atas["creator"], True),
        _meta(_pda([b"sharing-config", bytes(curve_info["mint"])], FEE_PROGRAM)),
        _meta(_pda([b"global_volume_accumulator"], PUMP)),
        _meta(_pda([b"user_volume_accumulator", bytes(user)], PUMP), True),
        _meta(atas["volume"], True),
        _meta(_pda([b"fee_config", bytes(pump)], FEE_PROGRAM)),
        _meta(_pk(FEE_PROGRAM)),
        _meta(_pk(SYSTEM)),
        _meta(_pda([b"__event_authority"], PUMP)),
        _meta(pump),
    ], base_user, atas["user"]


def _sell_accounts(user, curve_info, fee: str, buyback: str):
    pump = _pk(PUMP)
    quote_program = curve_info["quote_program"]
    atas = _quote_atas(curve_info["curve"], user, curve_info["creator"], _pk(fee), _pk(buyback), quote_program)
    base_user = _ata(user, curve_info["mint"], curve_info["token_program"])
    return [
        _meta(_pda([b"global"], PUMP)),
        _meta(curve_info["mint"]),
        _meta(curve_info["quote"]),
        _meta(curve_info["token_program"]),
        _meta(quote_program),
        _meta(_pk(ATA_PROGRAM)),
        _meta(_pk(fee), True),
        _meta(atas["fee"], True),
        _meta(_pk(buyback), True),
        _meta(atas["buyback"], True),
        _meta(curve_info["curve"], True),
        _meta(_ata(curve_info["curve"], curve_info["mint"], curve_info["token_program"]), True),
        _meta(atas["curve"], True),
        _meta(user, True, True),
        _meta(base_user, True),
        _meta(atas["user"], True),
        _meta(_pda([b"creator-vault", bytes(curve_info["creator"])], PUMP), True),
        _meta(atas["creator"], True),
        _meta(_pda([b"sharing-config", bytes(curve_info["mint"])], FEE_PROGRAM)),
        _meta(_pda([b"user_volume_accumulator", bytes(user)], PUMP), True),
        _meta(atas["volume"], True),
        _meta(_pda([b"fee_config", bytes(pump)], FEE_PROGRAM)),
        _meta(_pk(FEE_PROGRAM)),
        _meta(_pk(SYSTEM)),
        _meta(_pda([b"__event_authority"], PUMP)),
        _meta(pump),
    ], atas["user"]


async def _curve_from_site(mint: str):
    """Bonding-curve numbers from Pump's own API. No Solana RPC."""
    url = f"https://frontend-api-v3.pump.fun/coins/{mint}"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(2.0, connect=0.6)) as client:
            response = await client.get(url, headers={"Accept": "application/json", "Origin": "https://pump.fun"})
        if response.status_code != 200:
            return None, "curve_unread"
        body = response.json() or {}
    except Exception:
        return None, "curve_unread"
    if body.get("complete") is True:
        _CURVE[mint] = {"ts": time.time(), "curve": None, "why": "graduated", "sure": True}
        return None, "graduated"
    try:
        creator = str(body.get("creator") or "")
        virtual_sol = int(body.get("virtual_sol_reserves") or 0)
        virtual_token = int(body.get("virtual_token_reserves") or 0)
    except (TypeError, ValueError):
        return None, "curve_unread"
    if not creator or virtual_sol <= 0 or virtual_token <= 0:
        return None, "curve_unread"
    program = str(body.get("token_program") or TOKEN)
    if program not in (TOKEN, TOKEN_2022):
        program = TOKEN
    mint_pk = _pk(mint)
    row = {
        "curve": _pda([b"bonding-curve", bytes(mint_pk)], PUMP),
        "creator": _pk(creator),
        "quote": _pk(WSOL),
        "virtual_token": virtual_token,
        "virtual_sol": virtual_sol,
        "token_program": _pk(program),
        "quote_program": _pk(TOKEN),
        "mint": mint_pk,
    }
    _CURVE[mint] = {"ts": time.time(), "curve": row, "why": "", "sure": True}
    log.info("PUMP curve from pump.fun mint=%s — RPC did not answer", mint[:8])
    return row, ""


async def pump_portal_sell(mint: str, raw_amount: int, slippage_pct: int = 30) -> Tuple[str, str]:
    """A signed Pump sell built by pump.fun. Works when the curve RPC did not answer."""
    if raw_amount <= 0:
        return "", "zero"
    try:
        from src.execution.hot_tx import off_curve
        if off_curve(mint):
            return "", "not_on_curve"
    except Exception:
        pass
    try:
        from src.execution.signer import pubkey_from_env, sign_versioned_tx_base64
        pubkey = pubkey_from_env()
        if not pubkey:
            return "", "no_pubkey"
        body = {
            "publicKey": pubkey,
            "action": "sell",
            "mint": mint,
            "amount": raw_amount / 1_000_000,
            "denominatedInSol": "false",
            "slippage": int(slippage_pct),
            "priorityFee": 0.00015,
            "pool": "auto",
        }
        async with httpx.AsyncClient(timeout=httpx.Timeout(1.2, connect=0.4)) as client:
            response = await client.post(
                "https://pumpportal.fun/api/trade-local",
                json=body,
            )
        if response.status_code != 200 or not response.content:
            detail = ""
            try:
                detail = (response.text or "")[:120].replace("\n", " ")
            except Exception:
                detail = ""
            log.info("PUMP portal sell rejected mint=%s HTTP %s %s", mint[:8], response.status_code, detail)
            return "", f"portal_{response.status_code}"
        signed = sign_versioned_tx_base64(base64.b64encode(response.content).decode("ascii"))
        log.warning("PUMP portal sell built mint=%s raw=%s — sending this, Jupiter is not used", mint[:8], raw_amount)
        return signed, ""
    except Exception as exc:
        log.info("PUMP portal sell not built mint=%s — %s", mint[:8], type(exc).__name__)
        return "", f"portal_failed:{type(exc).__name__}"


async def pump_buy_tx(mint: str, sol_in: float, slippage_bps: int = 1500, quiet: bool = False) -> Tuple[str, int, str]:
    """Signed buy. Empty string means the caller should use Jupiter."""
    try:
        curve, why = await _curve(mint)
        if not curve:
            return "", 0, why or "not_on_curve"
        fee, buyback, fee_bps = await _fees()
        if not fee or not buyback:
            return "", 0, "fee_unread"
        from src.execution.signer import load_keypair_from_env
        kp, _ = load_keypair_from_env()
        user = kp.pubkey()
        lamports = int(sol_in * 1_000_000_000)
        expected = _tokens_for_sol(lamports, curve["virtual_token"], curve["virtual_sol"], fee_bps)
        minimum = max(1, expected * max(0, 10_000 - int(slippage_bps)) // 10_000)
        accounts, base_ata, wsol_ata = _buy_accounts(user, curve, fee, buyback)
        data = BUY_EXACT_QUOTE + struct.pack("<QQB", lamports, minimum, 0)
        micro = max(1, 20_000 * 1_000_000 // 300_000)
        instructions = [
            _cu_limit(300_000),
            _cu_price(micro),
            _create_ata(user, base_ata, user, curve["mint"], curve["token_program"]),
            _create_ata(user, wsol_ata, user, curve["quote"], curve["quote_program"]),
            _transfer(user, wsol_ata, lamports),
            _sync_native(wsol_ata, curve["quote_program"]),
            _ix(PUMP, data, accounts),
            _close_account(wsol_ata, user, user, curve["quote_program"]),
        ]
        signed = await _compile(instructions)
        if not quiet:
            log.info(
                "PUMP buy built mint=%s sol=%.4f min_tokens=%s — no Jupiter",
                mint[:8], sol_in, minimum,
            )
        return signed, minimum, ""
    except Exception as exc:
        log.warning("PUMP buy not built mint=%s — %s", mint[:8], exc)
        return "", 0, f"pump_build_failed:{type(exc).__name__}"


async def pump_sell_tx(mint: str, raw_amount: int, slippage_bps: int = 1500, quiet: bool = False) -> Tuple[str, int, str]:
    """Signed sell. Empty string means the curve is closed or not quoted in SOL."""
    try:
        if raw_amount <= 0:
            return "", 0, "zero"
        curve, why = await _curve(mint)
        if not curve:
            return "", 0, why or "not_on_curve"
        fee, buyback, fee_bps = await _fees()
        if not fee or not buyback:
            return "", 0, "fee_unread"
        from src.execution.signer import load_keypair_from_env
        kp, _ = load_keypair_from_env()
        user = kp.pubkey()
        expected = _sol_for_tokens(int(raw_amount), curve["virtual_token"], curve["virtual_sol"], fee_bps)
        minimum = max(1, expected * max(0, 10_000 - int(slippage_bps)) // 10_000)
        accounts, wsol_ata = _sell_accounts(user, curve, fee, buyback)
        data = SELL_V2 + struct.pack("<QQ", int(raw_amount), int(minimum))
        micro = max(1, 150_000 * 1_000_000 // 300_000)
        instructions = [
            _cu_limit(300_000),
            _cu_price(micro),
            _create_ata(user, wsol_ata, user, curve["quote"], curve["quote_program"]),
            _ix(PUMP, data, accounts),
            _close_account(wsol_ata, user, user, curve["quote_program"]),
        ]
        signed = await _compile(instructions)
        if not quiet:
            log.info(
                "PUMP sell built mint=%s raw=%s min_sol=%.6f — no Jupiter",
                mint[:8], raw_amount, minimum / 1_000_000_000,
            )
        return signed, minimum, ""
    except Exception as exc:
        log.warning("PUMP sell not built mint=%s — %s", mint[:8], exc)
        return "", 0, f"pump_build_failed:{type(exc).__name__}"
