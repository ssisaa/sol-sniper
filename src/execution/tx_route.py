"""Buy and sell venue from Settings. The chosen route, then Jupiter only.

local      → Jupiter
lightning  → Jupiter
pump       → Jupiter
jupiter    → Jupiter only
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, List

import httpx

log = logging.getLogger("sol-sniper.route")

ORDERS = {
    "local": ["local", "jupiter"],
    "lightning": ["lightning", "jupiter"],
    "pump": ["pump", "jupiter"],
    "jupiter": ["jupiter"],
}

_CLIENT: httpx.AsyncClient | None = None
_LIGHTNING: httpx.AsyncClient | None = None
_WARM_STARTED = False
_IN_FLIGHT = 0
_V4: dict[str, tuple[float, str]] = {}


def _portal_client() -> httpx.AsyncClient:
    """One connection kept open. Opening a new one on every click was the ConnectTimeout."""
    global _CLIENT
    if _CLIENT is None or _CLIENT.is_closed:
        _CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(1.2, connect=0.8),
            limits=httpx.Limits(max_keepalive_connections=4, max_connections=8),
        )
    return _CLIENT


def _lightning_client() -> httpx.AsyncClient:
    """One warm socket. A dead keepalive must not block the next click for seconds."""
    global _LIGHTNING
    if _LIGHTNING is None or _LIGHTNING.is_closed:
        _LIGHTNING = httpx.AsyncClient(
            timeout=_lightning_timeout(),
            limits=httpx.Limits(
                max_keepalive_connections=4,
                max_connections=8,
                keepalive_expiry=30.0,
            ),
            http2=False,
        )
    return _LIGHTNING


def _lightning_timeout() -> httpx.Timeout:
    # A fresh handshake was the multi-second sell. The socket stays open.
    return httpx.Timeout(connect=0.6, read=1.4, write=0.6, pool=0.2)


async def ipv4_host(host: str) -> str:
    """One IPv4 address. A dead IPv6 route was holding the connect open for several seconds."""
    import asyncio
    import socket
    import time
    host = (host or "").strip()
    if not host:
        return ""
    now = time.monotonic()
    hit = _V4.get(host)
    if hit and hit[1] and now - hit[0] < 300:
        return hit[1]
    loop = asyncio.get_running_loop()
    try:
        infos = await asyncio.wait_for(
            loop.getaddrinfo(host, 443, family=socket.AF_INET, type=socket.SOCK_STREAM),
            timeout=1.0,
        )
    except Exception:
        return hit[1] if hit and hit[1] else ""
    ip = ""
    for item in infos or []:
        addr = item[4][0] if len(item) > 4 and item[4] else ""
        if addr and ":" not in addr:
            ip = addr
            break
    if ip:
        _V4[host] = (now, ip)
    return ip


async def ipv4_target(url: str) -> tuple[str, dict, dict]:
    """https://host/path?query → https://ipv4/path?query with the real host on SNI."""
    import urllib.parse
    raw = str(url or "")
    parsed = urllib.parse.urlsplit(raw)
    host = parsed.hostname or ""
    if parsed.scheme != "https" or not host:
        return raw, {}, {}
    ip = await ipv4_host(host)
    if not ip:
        return raw, {}, {}
    port = f":{parsed.port}" if parsed.port else ""
    auth = ""
    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth += f":{parsed.password}"
        auth += "@"
    netloc = f"{auth}{ip}{port}"
    rebuilt = urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
    return rebuilt, {"Host": host}, {"sni_hostname": host}


def _v4_blocking(host: str) -> str:
    """IPv4 only, cached. Called from a worker thread so DNS cannot stall the sell."""
    import socket
    import time
    host = (host or "").strip()
    if not host:
        raise OSError("no host")
    now = time.monotonic()
    hit = _V4.get(host)
    if hit and hit[1] and now - hit[0] < 300:
        return hit[1]
    infos = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
    ip = ""
    for item in infos or []:
        addr = item[4][0] if len(item) > 4 and item[4] else ""
        if addr and ":" not in str(addr):
            ip = str(addr)
            break
    if not ip:
        raise OSError(f"no ipv4 for {host}")
    _V4[host] = (now, ip)
    return ip


def sync_https(method: str, url: str, *, params: dict | None = None, json_body=None, headers: dict | None = None, timeout: float = 3.0, as_bytes: bool = False) -> tuple[int, str | bytes]:
    """One HTTPS call on a worker thread. IPv4 first, then IPv6. The socket timeout is real."""
    import http.client
    import json
    import ssl
    import socket
    import urllib.parse
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    if not host:
        raise OSError("no host")
    path = parsed.path or "/"
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in (params or {}).items():
        query[str(key)] = str(value)
    if query:
        path = path + "?" + urllib.parse.urlencode(query)
    payload = None
    hdrs = {str(k): str(v) for k, v in (headers or {}).items()}
    hdrs["Host"] = host
    hdrs["Connection"] = "close"
    hdrs.setdefault("Accept", "application/json")
    if json_body is not None:
        payload = json.dumps(json_body).encode()
        hdrs["Content-Type"] = "application/json"
        hdrs["Content-Length"] = str(len(payload))
    limit = max(0.4, float(timeout or 3.0))
    # Do not touch the process-wide socket timeout. The dashboard accepts
    # connections on other threads, and a short default makes those writes time out.
    infos = socket.getaddrinfo(host, parsed.port or 443, socket.AF_INET, socket.SOCK_STREAM)
    last = OSError(f"connect {host} failed")
    sock = None
    for item in infos[:1]:
        try:
            sock = socket.create_connection(item[4], timeout=limit)
            sock.settimeout(limit)
            break
        except Exception as exc:
            last = exc
            sock = None
    if sock is None:
        raise last
    ssock = None
    try:
        ssock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        ssock.settimeout(limit)
        conn = http.client.HTTPSConnection(host, timeout=limit)
        conn.sock = ssock
        conn.request(method.upper(), path, body=payload, headers=hdrs)
        resp = conn.getresponse()
        raw = resp.read()
        if as_bytes:
            return int(resp.status), raw
        return int(resp.status), raw.decode("utf-8", "replace")
    finally:
        try:
            if ssock is not None:
                ssock.close()
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass


_RPC_GATE = threading.BoundedSemaphore(8)


def _rpc_url(url: str) -> str:
    """Flux documents https://eu.fluxrpc.com/?key=... A missing slash is a 400 from nginx."""
    import urllib.parse
    parsed = urllib.parse.urlsplit(str(url or "").strip())
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return str(url or "")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", parsed.query, ""))


def _rpc_post_one(url: str, body: dict, timeout: float = 4.0) -> tuple[int, str]:
    """JSON-RPC on a real socket. A regional Flux host that does not connect falls over to the CDN.

    The CDN is the same key and picks the closest live region. 99.9% uptime does not
    help when the path from this machine to eu.fluxrpc.com drops the handshake.
    """
    import urllib.parse
    url = _rpc_url(url)
    parsed = urllib.parse.urlsplit(url)
    host = (parsed.hostname or "").lower()
    targets = [url]
    if host.endswith(".fluxrpc.com") and host != "cdn.fluxrpc.com" and parsed.query:
        targets.append(urllib.parse.urlunsplit(("https", "cdn.fluxrpc.com", "/", parsed.query, "")))
    else:
        targets.append(url)
    last: Exception | None = None
    with _RPC_GATE:
        for index, target in enumerate(targets):
            try:
                status, text = sync_https("POST", target, json_body=body, timeout=timeout)
            except Exception as exc:
                last = exc
                continue
            if index and "cdn.fluxrpc.com" in target:
                log.info("RPC %s did not connect (%s) — used cdn.fluxrpc.com", host, type(last).__name__ if last else "fail")
            if not isinstance(text, str):
                text = text.decode("utf-8", "replace")
            return status, text
    raise last or OSError(f"rpc {host or 'unknown'} failed")


def rpc_post(url: str, body: dict, timeout: float = 4.0, failover: bool = True) -> tuple[int, str]:
    """JSON-RPC. A paid RPC that fails hands the same call to the next paid RPC, then the public nodes.

    Public URLs never fail over to a paid one: scans must not spend paid credits.
    """
    from src.execution import rpc_pool
    url = str(url or "").strip()
    paid = rpc_pool.is_paid(url)
    if not paid or not failover:
        try:
            status, text = _rpc_post_one(url, body, timeout)
        except Exception as exc:
            if paid:
                rpc_pool.mark_bad(url, type(exc).__name__)
            raise
        if paid and rpc_pool.plan_blocked(status, text):
            rpc_pool.note_no_method(url, str(body.get("method", "") if isinstance(body, dict) else ""))
        elif paid and rpc_pool.bad_status(status):
            rpc_pool.mark_bad(url, f"http {status}", rpc_pool.COOL_429_S if status == 429 else rpc_pool.COOL_S)
        return status, text
    method = str(body.get("method", "") if isinstance(body, dict) else "")
    tries = ([url] if rpc_pool.supports(url, method) else []) + rpc_pool.chain_after(url, method)
    last_exc: Exception | None = None
    last_resp: tuple[int, str] | None = None
    for i, target in enumerate(tries):
        try:
            status, text = _rpc_post_one(target, body, timeout)
        except Exception as exc:
            last_exc = exc
            if rpc_pool.is_paid(target):
                rpc_pool.mark_bad(target, type(exc).__name__)
            continue
        if rpc_pool.is_paid(target) and rpc_pool.plan_blocked(status, text):
            rpc_pool.note_no_method(target, method)
            last_resp = (status, text)
            continue
        if rpc_pool.bad_status(status):
            last_resp = (status, text)
            if rpc_pool.is_paid(target):
                rpc_pool.mark_bad(target, f"http {status}", rpc_pool.COOL_429_S if status == 429 else rpc_pool.COOL_S)
            continue
        if i and target != url:
            log.debug("RPC %s via %s (failover)", method or "?", rpc_pool.host(target))
        return status, text
    if last_resp is not None:
        return last_resp
    raise last_exc or OSError("every RPC failed")


async def lightning_warm_loop() -> None:
    """Keep one PumpPortal socket open so the next sell does not handshake."""
    import asyncio
    while True:
        try:
            await _lightning_client().get("https://pumpportal.fun", timeout=1.2)
        except Exception as exc:
            log.info("TX lightning warm — %s", type(exc).__name__)
            await _drop_lightning()
        await asyncio.sleep(15)


def ensure_lightning_warm() -> None:
    import asyncio
    global _WARM_STARTED
    if _WARM_STARTED:
        return
    try:
        asyncio.get_running_loop().create_task(lightning_warm_loop())
    except RuntimeError:
        return
    _WARM_STARTED = True


async def _drop_client() -> None:
    """A dead keepalive socket is what raises ConnectTimeout on the next click."""
    global _CLIENT
    client = _CLIENT
    _CLIENT = None
    if client is not None and not client.is_closed:
        try:
            await client.aclose()
        except Exception:
            pass


def route_order() -> List[str]:
    try:
        from src.config.runtime import get_config
        pick = str(getattr(get_config(), "tx_route", "local") or "local").strip().lower()
    except Exception:
        pick = "local"
    return list(ORDERS.get(pick, ORDERS["local"]))


def lightning_key() -> str:
    return os.environ.get("PUMPPORTAL_API_KEY", "").strip()


_LIGHTNING_POOL = None


def _lightning_executor():
    """The buy does not wait in line behind a wallet scan."""
    global _LIGHTNING_POOL
    if _LIGHTNING_POOL is None:
        import concurrent.futures
        _LIGHTNING_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="lightning")
    return _LIGHTNING_POOL


def solana_mint(mint: str) -> bool:
    """True only for a Solana address. A 0x coin is another chain and PumpPortal will not take it."""
    text = (mint or "").strip()
    if not text or text[:2].lower() == "0x" or not 32 <= len(text) <= 44:
        return False
    alphabet = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    return all(ch in alphabet for ch in text)


def pump_mint(mint: str) -> bool:
    """True when the mint starts or ends with pump."""
    text = (mint or "").strip()
    return solana_mint(text) and (text.endswith("pump") or text.startswith("pump"))


def mint_venue(mint: str) -> str:
    """Ending pump is the bonding curve. Starting pump is pump-amm. Anything else is Jupiter."""
    text = (mint or "").strip()
    if not solana_mint(text):
        return ""
    if text.endswith("pump"):
        return "pump"
    if text.startswith("pump"):
        return "pump-amm"
    return "jupiter"


async def caller_pool(mint: str) -> str:
    """Ask pump.fun which market is open, the same check the website makes before it buys."""
    import asyncio
    import json
    from src.execution.hot_tx import off_curve, remember_off_curve
    if off_curve(mint):
        return "pump-amm"

    def _ask():
        return sync_https(
            "GET",
            f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
            headers={"Accept": "application/json", "Origin": "https://pump.fun"},
            timeout=0.8,
        )

    try:
        status, text = await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(_lightning_executor(), _ask),
            timeout=1.0,
        )
    except Exception:
        return "auto"
    if status != 200:
        return "auto"
    try:
        body = json.loads(text or "{}")
    except Exception:
        return "auto"
    if body.get("complete") is True:
        remember_off_curve(mint)
        return "pump-amm"
    quote = str(body.get("quote_mint") or body.get("quoteMint") or "")
    sol_quote = "So11111111111111111111111111111111111111112"
    if quote and quote != sol_quote:
        return "not_sol"
    return "pump"


async def _lightning_once(action: str, mint: str, amount, denominated_in_sol: bool, slippage: int, priority_fee: float, pool: str, key: str) -> tuple[str, str]:
    import asyncio
    import json
    body = {
        "action": action,
        "mint": mint,
        "amount": amount,
        "denominatedInSol": "true" if denominated_in_sol else "false",
        "slippage": int(slippage),
        "priorityFee": float(priority_fee),
        "pool": pool,
        # Simulate first. A trade that would fail (coin already sold, curve finished, a quote coin the
        # pump route cannot take) is refused here with no fee, instead of landing as a failed tx.
        "skipPreflight": "false",
    }
    try:
        status, text = await asyncio.to_thread(
            sync_https,
            "POST",
            "https://pumpportal.fun/api/trade",
            params={"api-key": key},
            json_body=body,
            timeout=2.5,
        )
        status, text = int(status), text or ""
    except Exception as exc:
        return "", f"lightning_{type(exc).__name__}"
    text = (text or "").strip()
    if status != 200:
        return "", f"lightning_{status}:{text[:160]}"
    sig = ""
    try:
        data = json.loads(text)
    except Exception:
        data = None
    if isinstance(data, dict):
        err = data.get("error") or data.get("errors")
        if err:
            return "", f"lightning:{str(err)[:160]}"
        sig = str(data.get("signature") or data.get("tx") or data.get("txid") or "")
    elif isinstance(data, list):
        return "", f"lightning:{str(data)[:160]}"
    elif text and " " not in text and len(text) >= 32:
        sig = text.strip('"')
    if not sig:
        return "", f"lightning_empty:{text[:120]}"
    log.info("TX lightning %s pool=%s mint=%s sig=%s", action, pool, mint[:8], sig[:16])
    return sig, ""


async def _drop_lightning() -> None:
    """Drop a dead Lightning socket without waiting on it."""
    import asyncio
    global _LIGHTNING
    client = _LIGHTNING
    _LIGHTNING = None
    if client is None or client.is_closed:
        return
    try:
        await asyncio.wait_for(client.aclose(), timeout=0.15)
    except Exception:
        pass


def _lightning_timeout_err(err: str) -> bool:
    low = (err or "").lower()
    return "timeout" in low or "connecterror" in low


def _lightning_no_pool(err: str) -> bool:
    low = (err or "").lower()
    return (
        "pool account not found" in low
        or "no supported pools" in low
        or "not found" in low
        or err.startswith("lightning_400")
        or err.startswith("lightning_404")
    )


def _migrated(err: str) -> bool:
    """The bonding curve is gone. The coin is on pump-amm."""
    low = (err or "").lower()
    return ("migrated" in low or "does not exist" in low or "bonding curve" in low or "failed to find pump" in low
            or "bondingcurvecomplete" in low or "0x1775" in low)


def _pool_reject(err: str) -> bool:
    """The curve sell landed and the program refused it. Another pool can still sell."""
    text = err or ""
    low = text.lower()
    codes = ("6063", "6022", "6023", "6024", "6005", "6094", "6013", "6025")
    # A simulation answer carries the hex form (custom program error: 0x17af) or the name.
    hexes = tuple(hex(int(c)) for c in codes)
    names = ("unsupportedquotemint", "sellzeroamount", "bondingcurvecomplete")
    return any(c in text for c in codes) or any(h in low for h in hexes) or any(n in low for n in names)


_LN_HOLD: Dict[str, tuple] = {}   # mint -> (until_ts, why): Lightning is not used for this coin


def lightning_hold(mint: str, seconds: float, why: str) -> None:
    import time as _t
    mint = str(mint or "")
    if not mint:
        return
    until = _t.time() + float(seconds)
    prev = _LN_HOLD.get(mint)
    if prev is None or prev[0] < until:
        _LN_HOLD[mint] = (until, why)
        log.warning("TX lightning off for %s %.0fs — %s", mint[:8], seconds, why)


def _non_sol_quote(mint: str) -> str:
    """The quote coin when this pump coin's curve is not SOL-quoted (read from its curve account)."""
    try:
        from src.observability.dashboard import _SOL_QUOTES, _quote_of
    except Exception:
        return ""
    quote = _quote_of.get(str(mint or ""), "")
    return quote if quote and quote not in _SOL_QUOTES else ""


def lightning_held(mint: str) -> str:
    """Why Lightning is off for this coin right now, or empty."""
    import time as _t
    hit = _LN_HOLD.get(str(mint or ""))
    if not hit:
        return ""
    if hit[0] < _t.time():
        _LN_HOLD.pop(str(mint or ""), None)
        return ""
    return hit[1]


async def lightning_trade(action: str, mint: str, amount, denominated_in_sol: bool, slippage: int, priority_fee: float, pools: tuple = ()) -> tuple[str, str]:
    """Explicit pool only. auto is not used — PumpPortal says it adds about 100ms.

    A mint that ends in pump is sent as pool=pump. If that answer fails, one
    pump-amm try, because the coin may have migrated. A connect timeout is the
    host, so the other pool is not tried.
    """
    ensure_lightning_warm()
    key = lightning_key()
    if not key:
        return "", "no_lightning_key"
    venue = mint_venue(mint)
    if venue not in ("pump", "pump-amm"):
        return "", "not_pump"
    last = "lightning_skip"
    use = tuple(pool for pool in (pools or (venue,)) if pool and pool != "auto") or (venue,)
    tried_amm = "pump-amm" in use
    for pool in use:
        sig, err = await _lightning_once(action, mint, amount, denominated_in_sol, slippage, priority_fee, pool, key)
        if sig:
            return sig, ""
        last = err or last
        if _lightning_timeout_err(last):
            # No resend. A request that timed out here is often still sent by PumpPortal and lands
            # 10-30s later; a second request doubled those stray transactions.
            log.warning("TX lightning %s pool=%s mint=%s — %s, fresh socket, no resend", action, pool, mint[:8], last[:80])
            await _drop_lightning()
            lightning_hold(mint, 90, "timed out; that request may still land")
            log.warning("TX lightning %s host not answering mint=%s — Jupiter next", action, mint[:8])
            return "", last
        if "6063" in str(last) or "0x17af" in str(last).lower():
            lightning_hold(mint, 1800, "6063 unsupported quote mint (simulation)")
            return "", last
        if pool == "pump" and not tried_amm and "6063" not in str(last):
            tried_amm = True
            log.warning(
                "TX lightning %s pool=pump failed mint=%s — pump-amm once (%s)",
                action, mint[:8], str(last)[:80],
            )
            sig, err = await _lightning_once(action, mint, amount, denominated_in_sol, slippage, priority_fee, "pump-amm", key)
            if sig:
                return sig, ""
            last = err or last
            if _lightning_timeout_err(last):
                log.warning("TX lightning %s host not answering mint=%s — Jupiter next", action, mint[:8])
                return "", last
        return "", last
    return "", last


async def local_signed(action: str, mint: str, amount, denominated_in_sol: bool, slippage: int, priority_fee: float) -> tuple[str, str]:
    """Unsigned trade-local transaction, signed here. The caller sends it."""
    import asyncio
    import base64

    from src.execution.signer import pubkey_from_env, sign_versioned_tx_base64
    pubkey = pubkey_from_env()
    if not pubkey:
        return "", "no_pubkey"
    last = "local_skip"
    for pool in ("pump", "pump-amm"):
        body = {
            "publicKey": pubkey,
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true" if denominated_in_sol else "false",
            "slippage": int(slippage),
            "priorityFee": float(priority_fee),
            "pool": pool,
        }
        try:
            status, raw = await asyncio.to_thread(
                sync_https,
                "POST",
                "https://pumpportal.fun/api/trade-local",
                json_body=body,
                timeout=3.0,
                as_bytes=True,
            )
        except Exception as exc:
            return "", f"local_{type(exc).__name__}"
        if status == 200 and raw:
            try:
                signed = sign_versioned_tx_base64(base64.b64encode(raw).decode("ascii"))
            except Exception as exc:
                return "", f"local_sign:{type(exc).__name__}"
            return signed, ""
        text = raw.decode("utf-8", "replace")[:160] if isinstance(raw, (bytes, bytearray)) else str(raw)[:160]
        last = f"local_{status}:{text}"
        if _lightning_no_pool(last):
            log.info("TX local %s pool=%s mint=%s — no pool, next", action, pool, mint[:8])
            continue
        return "", last
    return "", last


async def _pump_amm_buy(trader, mint: str, sol: float):
    """Curve buy was refused. Try the markets pump.fun actually uses: PumpSwap, then Raydium."""
    for pool in ("pump-amm",):
        sig, err = await lightning_trade(
            "buy", mint, float(sol), True, 25, 0.0, pools=(pool,),
        )
        if not sig:
            log.warning("TX buy %s not sent mint=%s err=%s", pool, mint[:8], str(err)[:120])
            if _lightning_no_pool(str(err or "")):
                continue
            continue
        raw = 0
        try:
            from src.execution.signer import pubkey_from_env
            pubkey = pubkey_from_env() or ""
            if pubkey:
                raw = await trader._raw_from_tx(sig, pubkey, mint)
        except Exception:
            raw = 0
        if raw > 0:
            log.info("TX buy filled on %s mint=%s raw=%s sig=%s", pool, mint[:8], raw, sig[:16])
            return {"ok": True, "sig": sig, "route": "lightning", "tokens": int(raw), "sol": float(sol), "err": ""}
        log.warning(
            "TX buy %s empty mint=%s err=%s",
            pool, mint[:8], str(getattr(trader, "_last_buy_tx_err", "") or err or "")[:120],
        )
    return None


def _not_on_pump(err: str) -> bool:
    text = (err or "").lower()
    return any(
        k in text
        for k in (
            "pool account not found",
            "no supported pools",
            "supported pool",
            "not_on_curve",
            "graduated",
            "not_sol_quote",
            "bonding curve",
            "curve account",
            "bad request",
        )
    )


async def _curve_exists(mint: str):
    """Paid RPC only. False means the bonding curve account is not there."""
    from src.execution.hot_tx import off_curve, remember_off_curve
    if off_curve(mint):
        return False
    try:
        from src.execution.pump_curve import PUMP, _pda, _pk
        from src.execution.wallet_tokens import primary_url
        url = primary_url()
        if not url:
            return None
        curve = str(_pda([b"bonding-curve", bytes(_pk(mint))], PUMP))
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
            "params": [curve, {"encoding": "base64"}],
        }
        response = await _portal_client().post(url, json=payload, timeout=httpx.Timeout(0.45, connect=0.25))
        if response.status_code != 200:
            return None
        value = ((response.json() or {}).get("result") or {}).get("value")
        if value is None:
            remember_off_curve(mint)
            return False
        return True
    except Exception:
        return None


async def run_swap(trader, side: str, mint: str, sol: float = 0.0, raw: int = 0, reason: str = "", full: bool = True) -> Dict[str, Any]:
    """Ending pump uses the bonding curve. Starting pump uses pump-amm. Every other coin uses Jupiter."""
    venue = mint_venue(mint)
    if not venue:
        log.warning("TX %s skip mint=%s — not a Solana coin", side, (mint or "")[:14])
        return {"ok": False, "sig": "", "route": "", "tokens": 0, "sol": 0.0, "err": "not_solana"}
    import asyncio

    from src.execution.hot_tx import off_curve, remember_off_curve

    if venue == "jupiter":
        order = ["jupiter"]
        log.info("TX %s jupiter mint=%s reason=%s", side, mint[:8], reason or "-")
    elif _non_sol_quote(mint):
        order = ["jupiter"]
        log.info("TX %s jupiter mint=%s reason=%s — its curve trades against %s, not SOL; Lightning cannot route it",
                 side, mint[:8], reason or "-", _non_sol_quote(mint)[:8])
    elif lightning_held(mint):
        order = ["jupiter"]
        log.info("TX %s jupiter mint=%s reason=%s — Lightning off for this coin (%s)", side, mint[:8], reason or "-", lightning_held(mint))
    else:
        order = ["lightning"]
        log.info("TX %s pool=%s mint=%s reason=%s", side, venue, mint[:8], reason or "-")
        ensure_lightning_warm()
        if not lightning_key():
            log.warning("TX %s Lightning selected but pumpportal.api_key is empty in sniper.yaml", side)
    limits = {"local": 8.0, "lightning": 6.0, "pump": 3.0}
    # PumpPortal turns priorityFee into a Jito tip inside the pump transaction.
    # A pump-pool coin does not need that tip. Jupiter is not cut off.
    buy_slip, buy_fee = 15, 0.0
    sell_slip, sell_fee = 30, 0.0
    last = "no_route"

    async def _try(name: str, limit: float):
        nonlocal last
        try:
            call = _one(trader, name, side, mint, sol, raw, buy_slip, buy_fee, sell_slip, sell_fee, full)
            if name == "jupiter":
                outcome = await call
            else:
                outcome = await asyncio.wait_for(call, timeout=limit)
        except asyncio.TimeoutError:
            last = f"{name}_timeout"
            log.warning("TX %s %s cutoff %.1fs mint=%s", side, name, limit, mint[:8])
            return None
        except Exception as exc:
            last = f"{name}:{type(exc).__name__}"
            log.warning("TX %s %s error mint=%s — %s", side, name, mint[:8], last)
            return None
        if not isinstance(outcome, dict) or outcome.get("status") != "sent":
            last = str((outcome or {}).get("err") or last or f"{name}_skip") if isinstance(outcome, dict) else (last or f"{name}_empty")
            log.warning("TX %s %s skipped mint=%s — %s", side, name, mint[:8], last[:140])
            if name != "jupiter" and _not_on_pump(last):
                remember_off_curve(mint)
                log.info("TX %s mint=%s not on a Pump pool — Jupiter next", side, mint[:8])
            return None
        sig = str(outcome.get("sig") or "")
        if not sig:
            last = f"{name}_empty"
            return None
        if side == "buy":
            confirmed = await trader._confirm_signature(sig, tries=1, delay=0, paid_only=True, timeout=0.6)
            err_now = str(getattr(trader, "_last_tx_err", "") or "")
            if err_now and "6063" in err_now:
                last = err_now
                lightning_hold(mint, 1800, "6063 unsupported quote mint")
                log.warning("TX buy 6063 mint=%s — Jupiter. The SOL was not spent.", mint[:8])
                return None
            if err_now and err_now != "confirm_unreachable" and _pool_reject(err_now):
                remember_off_curve(mint)
                log.warning("TX buy wrong market mint=%s err=%s — sending the open market", mint[:8], err_now[:80])
                filled = await _pump_amm_buy(trader, mint, float(sol))
                if filled:
                    return filled
                last = "pool account not found"
                return None
            if err_now and err_now != "confirm_unreachable" and not confirmed:
                last = err_now
                log.warning(
                    "TX buy rejected mint=%s sig=%s err=%s — SOL was not spent",
                    mint[:8], sig[:16], err_now[:120],
                )
                return None
            # A signature that has not landed is not a buy. The quote token
            # count used to be written as a confirmed fill. TCC was that case.
            if not confirmed:
                held_raw = 0
                try:
                    from src.execution.signer import pubkey_from_env
                    from src.execution.wallet_tokens import get_token_holding
                    pk = pubkey_from_env()
                    if pk:
                        held = await asyncio.wait_for(
                            get_token_holding(pk, mint, fast=True, confirm_empty=1),
                            timeout=0.8,
                        )
                        held_raw = int((held or {}).get("raw") or 0)
                except Exception:
                    held_raw = 0
                if held_raw > 0:
                    log.info(
                        "TX buy %s coins are in the wallet mint=%s raw=%s sig=%s",
                        name, mint[:8], held_raw, sig[:16],
                    )
                    return {
                        "ok": True,
                        "sig": sig,
                        "route": name,
                        "tokens": held_raw,
                        "sol": float(outcome.get("sol") or sol or 0),
                        "err": "",
                    }
                last = err_now or f"{name}_not_confirmed"
                log.warning(
                    "TX buy %s not on chain mint=%s sig=%s — quote is not a fill, nothing recorded",
                    name, mint[:8], sig[:16],
                )
                return None
            log.info("TX buy landed via %s mint=%s sig=%s", name, mint[:8], sig[:16])
            return {
                "ok": True,
                "sig": sig,
                "route": name,
                "tokens": int(outcome.get("tokens") or raw or 0),
                "sol": float(outcome.get("sol") or sol or 0),
                "err": "",
            }
        urgent = reason in (
            "manual_now", "caller_closed", "caller_drop_7", "callout_age",
        ) or str(reason).startswith("tp_") or str(reason).startswith("take_profit")
        tries = 4 if (name == "lightning" or urgent) else 2
        confirmed = await trader._confirm_signature(
            sig, tries=tries, delay=0.25, paid_only=True, timeout=0.8,
        )
        chain_err = str(getattr(trader, "_last_tx_err", "") or "")
        if confirmed and not chain_err:
            log.info("TX %s landed via %s mint=%s sig=%s", side, name, mint[:8], sig[:16])
            return {
                "ok": True,
                "sig": sig,
                "route": name,
                "tokens": int(outcome.get("tokens") or raw or 0),
                "sol": float(outcome.get("sol") or sol or 0),
                "err": "",
            }
        # A signature is not a sale. 6063 failed on chain and only the tip was paid.
        last = chain_err or f"{name}_not_confirmed"
        if chain_err and chain_err != "confirm_unreachable":
            log.warning("TX %s %s failed on chain mint=%s sig=%s err=%s — next route", side, name, mint[:8], sig[:16], chain_err[:120])
            if name == "lightning" and "6063" in chain_err:
                lightning_hold(mint, 1800, "6063 unsupported quote mint")
            if name == "lightning" and side == "sell" and ("6063" in chain_err or _pool_reject(chain_err)):
                remember_off_curve(mint)
                amount = "100%" if full else max(int(raw or 0), 1) / 1_000_000
                log.warning(
                    "TX sell curve rejected mint=%s err=%s — pump-amm once, then Jupiter",
                    mint[:8], chain_err[:80],
                )
                sig2, err2 = await lightning_trade(
                    "sell", mint, amount, False, max(sell_slip, 40), sell_fee,
                    pools=("pump-amm",),
                )
                if sig2:
                    confirmed2 = await trader._confirm_signature(sig2, tries=4, delay=0.25, paid_only=True, timeout=0.8)
                    err2_chain = str(getattr(trader, "_last_tx_err", "") or "")
                    if confirmed2 and not err2_chain:
                        log.info("TX sell landed via lightning pump-amm mint=%s sig=%s", mint[:8], sig2[:16])
                        return {
                            "ok": True,
                            "sig": sig2,
                            "route": "lightning",
                            "tokens": int(outcome.get("tokens") or raw or 0),
                            "sol": float(outcome.get("sol") or sol or 0),
                            "err": "",
                        }
                    last = err2_chain or err2 or chain_err
                    log.warning("TX sell pump-amm failed mint=%s err=%s — Jupiter next", mint[:8], str(last)[:120])
                else:
                    last = err2 or chain_err
                    log.warning("TX sell pump-amm not sent mint=%s err=%s — Jupiter next", mint[:8], str(last)[:120])
        else:
            log.warning("TX %s %s not on chain mint=%s sig=%s — next route", side, name, mint[:8], sig[:16])
        return None

    names = list(order)
    for name in names:
        if name == "pump" and off_curve(mint):
            log.info("TX %s mint=%s curve already rejected — skipping pump", side, mint[:8])
            continue
        got = await _try(name, limits.get(name, 1.2))
        if isinstance(got, dict):
            got.pop("stop", None)
            return got
        if name == "lightning":
            missed = str(last)
            # A sell always gets Jupiter after Lightning. A buy gets Jupiter when the
            # curve or the pump-amm pool refused the coin. Do not stop on "pool not found".
            go_jupiter = side == "sell" or any((
                "6063" in missed,
                _lightning_timeout_err(missed),
                _not_on_pump(missed),
                _lightning_no_pool(missed),
                _pool_reject(missed),
                _migrated(missed),
            ))
            if go_jupiter and "jupiter" not in names:
                names.append("jupiter")
                log.warning(
                    "TX %s lightning missed mint=%s err=%s — Jupiter now",
                    side, mint[:8], missed[:80],
                )
                continue
            if go_jupiter:
                continue
            if "not_sol_quote" not in missed:
                log.warning("TX %s lightning stopped mint=%s — %s", side, mint[:8], missed[:80])
                break
    return {"ok": False, "sig": "", "route": order[0], "tokens": 0, "sol": 0.0, "err": str(last)[:160]}


async def _one(trader, name: str, side: str, mint: str, sol: float, raw: int, buy_slip: int, buy_fee: float, sell_slip: int, sell_fee: float, full: bool) -> Dict[str, Any]:
    if name == "lightning":
        pool = mint_venue(mint)
        if pool not in ("pump", "pump-amm"):
            return {"status": "skip", "err": "not_pump"}
        log.info("TX %s market=%s mint=%s", side, pool, mint[:8])
        if side == "buy":
            sig, err = await lightning_trade("buy", mint, float(sol), True, buy_slip, 0.0, pools=(pool,))
        else:
            amount = "100%" if full else max(raw, 1) / 1_000_000
            sig, err = await lightning_trade("sell", mint, amount, False, sell_slip, sell_fee, pools=(pool,))
        if not sig:
            return {"status": "skip", "err": err}
        return {"status": "sent", "sig": sig, "tokens": raw, "sol": sol}

    if name == "local":
        if side == "buy":
            signed, err = await local_signed("buy", mint, float(sol), True, buy_slip, buy_fee)
        else:
            amount = "100%" if full else max(raw, 1) / 1_000_000
            signed, err = await local_signed("sell", mint, amount, False, sell_slip, sell_fee)
        if not signed:
            return {"status": "skip", "err": err}
        sig, send_err = await trader._send_keep_fee(signed, fast=True)
        if not sig:
            return {"status": "skip", "err": str(send_err or err or "local_send")}
        return {"status": "sent", "sig": sig, "tokens": raw, "sol": sol}

    if name == "pump":
        import asyncio
        if side == "buy":
            from src.execution.pump_curve import pump_buy_tx
            try:
                signed, tokens, err = await asyncio.wait_for(
                    pump_buy_tx(mint, float(sol), slippage_bps=1500), timeout=0.8,
                )
            except asyncio.TimeoutError:
                return {"status": "skip", "err": "curve_unread"}
            if not signed:
                return {"status": "skip", "err": err or "pump_buy_empty"}
            sig, send_err = await trader._send_keep_fee(signed, fast=True)
            if not sig:
                return {"status": "skip", "err": str(send_err or "pump_send")}
            return {"status": "sent", "sig": sig, "tokens": int(tokens or 0), "sol": sol}
        from src.execution.pump_curve import pump_sell_tx
        sell_raw = int(raw)
        if sell_raw < 1000:
            try:
                from src.execution.signer import pubkey_from_env
                from src.execution.wallet_tokens import get_token_holding
                pk = pubkey_from_env()
                held = await get_token_holding(pk, mint, fast=True, paid_only=True) if pk else None
                live = int((held or {}).get("raw") or 0)
                if live > 0:
                    sell_raw = live
            except Exception:
                sell_raw = int(raw)
        try:
            signed, min_sol, err = await asyncio.wait_for(
                pump_sell_tx(mint, int(sell_raw), slippage_bps=3000), timeout=2.5,
            )
        except asyncio.TimeoutError:
            return {"status": "skip", "err": "curve_unread"}
        if not signed:
            return {"status": "skip", "err": err or "pump_sell_empty"}
        sig, send_err = await trader._send_keep_fee(signed, fast=True)
        if not sig:
            return {"status": "skip", "err": str(send_err or "pump_send")}
        return {"status": "sent", "sig": sig, "tokens": raw, "sol": float(min_sol or 0) / 1_000_000_000}

    if name == "jupiter":
        from src.execution.signer import pubkey_from_env, sign_versioned_tx_base64
        pubkey = pubkey_from_env()
        if not pubkey:
            return {"status": "skip", "err": "no_pubkey"}
        sell_raw = int(raw)
        if side == "sell" and sell_raw < 1000 and pubkey:
            try:
                from src.execution.wallet_tokens import get_token_holding
                held = await asyncio.wait_for(
                    get_token_holding(pubkey, mint, fast=True, paid_only=True),
                    timeout=0.6,
                )
                live = int((held or {}).get("raw") or 0)
                if live > 0:
                    sell_raw = live
            except Exception:
                sell_raw = max(1, int(sell_raw))
        # 6024 is InsufficientFunds. Retry the same full balance with a smaller fee.
        # Never sell 95%. The 5% was left sitting in the wallet.
        tries = [(float(sol), 1500, 20_000)] if side == "buy" else [
            (sell_raw, 5000, 5_000),
        ]
        last_err = "jupiter"
        for amt, slip, fee in tries:
            if side == "buy":
                quote = await trader.jup.quote_sol_to_token(mint, float(sol), slippage_bps=slip, urgent=True)
            else:
                quote = await trader.jup.quote_token_to_sol(mint, int(amt), slippage_bps=slip, urgent=True)
            out = int(getattr(quote, "out_amount", 0) or 0) if quote else 0
            if not quote or out <= 0:
                last_err = str(getattr(trader.jup, "_last_error", "") or "jupiter_quote")
                continue
            unsigned = await trader.jup.swap_transaction(
                quote.raw, user_public_key=pubkey, prioritization_fee_lamports=fee, urgent=True,
            )
            if not unsigned:
                last_err = "jupiter_swap"
                continue
            try:
                signed = sign_versioned_tx_base64(unsigned)
            except Exception as exc:
                return {"status": "skip", "err": f"jupiter_sign:{type(exc).__name__}"}
            sig, send_err = await trader._send_keep_fee(signed)
            if sig:
                sol_out = float(out) / 1_000_000_000 if side == "sell" else float(sol)
                tokens = out if side == "buy" else int(amt)
                return {"status": "sent", "sig": sig, "tokens": tokens, "sol": sol_out}
            last_err = str(send_err or "jupiter_send")
            text = last_err.lower()
            if "6024" in text or "6001" in text or "0x1788" in text:
                log.warning("TX %s jupiter 6024 mint=%s — full balance again, smaller fee, paid RPC", side, mint[:8])
                continue
            break
        return {"status": "skip", "err": last_err}

    return {"status": "skip", "err": f"unknown_{name}"}
