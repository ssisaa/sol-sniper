
"""Read SPL token balances for the bot wallet (orphan detection / sell sizing)."""
from __future__ import annotations

import json
import logging
import os
import time
import asyncio
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("sol-sniper.wallet_tokens")

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
_GOOD: Dict[str, Any] = {"ts": 0.0, "owner": "", "rows": None}
# The wallet panel reads the paid RPC every PAID_EVERY_S, or every second for a while after a trade.
PAID_EVERY_S = 5.0
_PAID = {"at": 0.0, "hot_until": 0.0}


def wallet_hot(seconds: float = 20.0) -> None:
    """A buy or sell was just sent: read the wallet from the paid RPC every second for a while."""
    _PAID["hot_until"] = max(_PAID["hot_until"], time.time() + seconds)
_SEEN: Dict[str, Dict[str, Any]] = {}
_DECIMALS: Dict[str, int] = {}   # mint -> token decimals, from any balance read


def decimals_for(mint: str) -> Optional[int]:
    """Decimals seen for this mint, or None. Pump coins are always 6."""
    mint = str(mint or "")
    if mint in _DECIMALS:
        return _DECIMALS[mint]
    if mint.endswith("pump") or mint.startswith("pump"):
        return 6
    return None
_primary_at = 0.0


def _mint_prefix(symbol: str, mint: str) -> bool:
    """True when the text is just the start of the mint, not a real ticker."""
    text = str(symbol or "").strip()
    mint = str(mint or "").strip()
    if not text or not mint:
        return not text
    if text in (mint[:4], mint[:6], mint[:8]):
        return True
    return mint.startswith(text) and len(text) <= 8


def cached_label(mint: str) -> Dict[str, str]:
    """A real ticker and name, if one has already been found. A mint prefix is not a name."""
    hit = _PX.get(mint or "") or {}
    symbol = str(hit.get("symbol") or "").strip()
    name = str(hit.get("name") or "").strip()
    if _mint_prefix(symbol, mint):
        symbol = ""
    if symbol or name:
        return {"symbol": symbol or name, "name": name}
    return {"symbol": "", "name": ""}


async def _fresh_accounts(owner: str) -> Optional[List[Dict[str, Any]]]:
    """The coins in the wallet right now. Public RPCs first. Paid RPC only if those miss."""
    public = [url for url in _rpc_urls() if url][:3]
    paid = primary_url()

    async def _read(urls: List[str]):
        if not urls:
            return None, None

        async def _one(url: str, program: str):
            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    owner,
                    {"programId": program},
                    {"encoding": "jsonParsed", "commitment": "confirmed"},
                ],
            }
            try:
                async with httpx.AsyncClient(timeout=2.4) as client:
                    response = await client.post(url, json=body)
                if response.status_code != 200:
                    return None
                data = response.json() or {}
                if data.get("error"):
                    return None
                return ((data.get("result") or {}).get("value")) or []
            except Exception:
                return None

        hits = await asyncio.gather(*[_one(url, program) for url in urls for program in (TOKEN_PROGRAM, TOKEN_2022)])
        spl_hits = hits[0::2]
        t22_hits = hits[1::2]
        spl = next((rows for rows in spl_hits if rows is not None), None)
        t22 = next((rows for rows in t22_hits if rows is not None), None)
        return spl, t22

    spl, t22 = await _read(public)
    if spl is None and t22 is None and paid:
        spl, t22 = await _read([paid])
    if spl is None and t22 is None:
        return None
    seen: set = set()
    return _parse_rows(spl or [], seen) + _parse_rows(t22 or [], seen)


async def read_wallet_now(owner: str) -> tuple[Optional[float], Optional[List[Dict[str, Any]]]]:
    """SOL and coins from one RPC. The newest slot is shown. Paid and public are read together."""
    if not owner:
        return None, None
    urls = [url for url in _rpc_urls() if url]
    paid = primary_url()
    if paid and paid in urls:
        urls = [url for url in urls if url != paid]

    def _tok(program: str) -> dict:
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [owner, {"programId": program}, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        }

    bal_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [owner, {"commitment": "confirmed"}],
    }

    async def _one_url(url: str):
        def _call(body: dict):
            from src.execution.tx_route import rpc_post
            try:
                status, text = rpc_post(url, body, 0.7)
            except Exception:
                return None
            if status != 200:
                return None
            try:
                data = json.loads(text or "{}")
            except Exception:
                return None
            if data.get("error"):
                return None
            return data.get("result")

        bal, spl, t22 = await asyncio.gather(
            asyncio.to_thread(_call, bal_body),
            asyncio.to_thread(_call, _tok(TOKEN_PROGRAM)),
            asyncio.to_thread(_call, _tok(TOKEN_2022)),
        )
        sol = None
        slot = 0
        if isinstance(bal, dict) and bal.get("value") is not None:
            sol = int(bal["value"]) / 1_000_000_000
            try:
                slot = int((bal.get("context") or {}).get("slot") or 0)
            except (TypeError, ValueError):
                slot = 0
        spl_rows = spl.get("value") if isinstance(spl, dict) else None
        t22_rows = t22.get("value") if isinstance(t22, dict) else None
        # SOL and the coin list have to come from the same RPC.
        # Mixing them painted an old SOL next to the coins Phantom shows now.
        if not isinstance(spl_rows, list) or not isinstance(t22_rows, list):
            return slot, sol, None, url
        try:
            spl_slot = int((spl.get("context") or {}).get("slot") or 0) if isinstance(spl, dict) else 0
            t22_slot = int((t22.get("context") or {}).get("slot") or 0) if isinstance(t22, dict) else 0
            slot = max(slot, spl_slot, t22_slot)
        except (TypeError, ValueError):
            pass
        seen: set = set()
        rows = _parse_rows(spl_rows, seen) + _parse_rows(t22_rows, seen)
        return slot, sol, rows, url

    answers = []

    async def _race(urls: List[str], timeout: float):
        if not urls:
            return
        tasks = [asyncio.create_task(_one_url(url)) for url in urls]
        try:
            for done in asyncio.as_completed(tasks, timeout=timeout):
                try:
                    answers.append(await done)
                except Exception:
                    continue
        except TimeoutError:
            pass
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # Paid and public together. The highest slot wins, so a slow stale node cannot stick.
    # The paid RPC costs credits: three calls per read. It joins every PAID_EVERY_S, every second
    # right after a trade, or when no public node answered.
    batch = list(urls[:3])
    now_t = time.time()
    want_paid = bool(paid) and paid not in batch and (
        now_t < _PAID["hot_until"] or now_t - _PAID["at"] >= PAID_EVERY_S
    )
    if want_paid:
        _PAID["at"] = now_t
        batch.append(paid)
    await _race(batch, 0.8)
    if paid and not want_paid and not any(row[2] is not None for row in answers):
        _PAID["at"] = time.time()
        await _race([paid], 0.8)
    complete = [row for row in answers if row[1] is not None and row[2] is not None]
    pool = complete or [row for row in answers if row[1] is not None]
    if not pool:
        return None, None

    def _rank(row):
        slot, _sol, rows, url = row
        return (int(slot or 0), 1 if url == paid else 0, 1 if rows is not None else 0)

    slot, sol, rows, _url = max(pool, key=_rank)
    if rows is not None:
        # The wallet panel reads this every second. Keep it as the short cache too, so the pre-buy
        # "already holding?" check answers at once instead of a fresh 4s RPC read per buy.
        _remember_rows(rows)
        _GOOD["ts"] = time.time()
        _GOOD["owner"] = owner
        _GOOD["rows"] = rows
    return sol, rows


def _public_rpc(url: str) -> bool:
    host = (url or "").split("//")[-1].split("/")[0].lower()
    return any(part in host for part in ("mainnet-beta.solana.com", "publicnode.com"))


def primary_url() -> str:
    """Paid RPC: the first healthy one of yaml rpc.paid (then rpc.primary). All down: a public node."""
    try:
        from src.execution.rpc_pool import active_paid
        url = active_paid()
        if url:
            return url
    except Exception:
        pass
    env = os.environ.get("SOLANA_RPC_URL", "").strip()
    if env and not _public_rpc(env):
        return env
    return ""


def _rpc_urls() -> List[str]:
    """Scan reads. The paid primary is not in this list, so a public timeout cannot sit in front of a buy."""
    urls = []
    primary = primary_url()
    try:
        from src.execution.rpc_pool import paid_urls
        paid_all = set(paid_urls())
    except Exception:
        paid_all = set()
    for u in (
        os.environ.get("HELIUS_RPC_URL", "").strip(),
        os.environ.get("SOLANA_RPC_URL", "").strip(),
    ):
        if u and u != primary and u not in paid_all and u not in urls:
            urls.append(u)
    try:
        from src.config.file_config import load_config
        rpc = (load_config() or {}).get("rpc") or {}
        for u in rpc.get("urls") or []:
            u = (u or "").strip()
            if u and u != primary and u not in paid_all and u not in urls:
                urls.append(u)
    except Exception:
        pass
    for u in (
        "https://solana-rpc.publicnode.com",
        "https://api.mainnet-beta.solana.com",
    ):
        if u not in urls:
            urls.append(u)
    return urls


def allow_primary() -> bool:
    """Scans stay on the public RPCs. Buy and sell call primary_url() themselves."""
    return False


async def _program_accounts(client: httpx.AsyncClient, owner: str, program: str, urls: Optional[List[str]] = None, timeout: float = 3.5) -> Optional[list]:
    """First HTTP 200 with no RPC error. None only if every endpoint failed."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            owner,
            {"programId": program},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    }

    async def _one(url: str):
        try:
            from src.execution.tx_route import rpc_post
            import json
            status, text = await asyncio.to_thread(rpc_post, url, payload, max(4.0, float(timeout or 4)))
            if status != 200:
                return None
            body = json.loads(text or "{}")
            if body.get("error"):
                return None
            return (body.get("result") or {}).get("value") or []
        except Exception as e:
            log.info("wallet read %s: %s", (url.split("//")[-1].split("?")[0][:40]), type(e).__name__)
            return None

    tasks = [asyncio.create_task(_one(url)) for url in (urls or _rpc_urls())]
    empty_hits = 0
    try:
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, timeout=timeout + 0.4, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break
            for task in done:
                value = task.result()
                if value is None:
                    continue
                if value:
                    return value
                empty_hits += 1
                if empty_hits >= 2:
                    return []
        # One empty reply is not proof the wallet is flat. A free RPC often returns [].
        return [] if empty_hits >= 2 else None
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()


_SOLD_UNTIL: Dict[str, float] = {}


def reset_wallet_cache() -> None:
    """A new keypair must not keep the previous wallet's coins."""
    _GOOD["ts"] = 0.0
    _GOOD["owner"] = ""
    _GOOD["rows"] = None
    _SEEN.clear()
    _SOLD_UNTIL.clear()


def drop_cached_mint(mint: str) -> None:
    """A sold or missing coin must not stay in the 90s wallet cache."""
    mint = (mint or "").strip()
    if not mint:
        return
    _SEEN.pop(mint, None)
    rows = _GOOD.get("rows")
    if isinstance(rows, list):
        _GOOD["rows"] = [r for r in rows if (r.get("mint") or "") != mint]


def mark_sold(mint: str, seconds: float = 1800.0) -> None:
    mint = (mint or "").strip()
    if not mint:
        return
    drop_cached_mint(mint)
    _SOLD_UNTIL[mint] = time.time() + seconds


def sold_recently(mint: str) -> bool:
    return time.time() < float(_SOLD_UNTIL.get(mint or "", 0) or 0)


def unmark_sold(mint: str) -> None:
    _SOLD_UNTIL.pop((mint or "").strip(), None)


def _hide_closed(rows: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
    """The chain balance wins. A sold flag must not hide a coin Phantom still holds."""
    return rows


def _remember_rows(rows: List[Dict[str, Any]]) -> None:
    now = time.time()
    for row in rows or []:
        mint = row.get("mint") or ""
        raw = int(row.get("raw") or 0)
        if mint and raw > 0:
            _SEEN[mint] = {"raw": raw, "ui": float(row.get("ui_amount") or 0), "ts": now}


def note_wallet_flat() -> None:
    """A confirmed empty token list. Drop the short memory of coins that are no longer there."""
    _SEEN.clear()
    _GOOD["rows"] = []
    _GOOD["ts"] = time.time()


def _seen_recently(mint: str, seconds: float = 90.0) -> bool:
    hit = _SEEN.get(mint or "")
    return bool(hit and time.time() - float(hit.get("ts") or 0) < seconds)


def _parse_rows(value: list, seen: set) -> List[Dict[str, Any]]:
    rows = []
    for item in value or []:
        try:
            info = (((item.get("account") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
            mint = info.get("mint") or ""
            ta = info.get("tokenAmount") or {}
            raw = int(ta.get("amount") or 0)
            if not mint or raw <= 0 or mint in seen:
                continue
            seen.add(mint)
            ui = float(ta.get("uiAmount") or 0)
            decimals = int(ta.get("decimals") or 0)
            if ta.get("decimals") is not None:
                _DECIMALS[mint] = int(ta.get("decimals") or 0)
            if ui <= 0 and decimals >= 0:
                ui = raw / float(10 ** decimals)
            rows.append({
                "mint": mint,
                "ui_amount": ui,
                "raw": raw,
                "decimals": int(ta.get("decimals") or 0),
                "pubkey": item.get("pubkey") or "",
                "state": str(info.get("state") or ""),
            })
        except Exception:
            continue
    return rows


async def get_token_accounts(owner: str, include_primary: bool = False, fresh: bool = False, paid_only: bool = False, show_all: bool = False) -> Optional[List[Dict[str, Any]]]:
    """Non-zero SPL + Token-2022 accounts. None only if no RPC returned HTTP 200.

    fresh=True does not reuse the short cache. An empty reply is empty, not the coins
    from the previous read. Restore decisions need that, or a sold coin comes back.
    """
    if not owner:
        return []
    if fresh:
        live = await _fresh_accounts(owner)
        if live is not None:
            _remember_rows(live)
            _GOOD["ts"] = time.time()
            _GOOD["owner"] = owner
            _GOOD["rows"] = live
            return live if show_all else _hide_closed(live)
    now = time.time()
    if (
        not fresh
        and _GOOD["owner"] == owner
        and _GOOD["rows"] is not None
        and now - float(_GOOD["ts"]) < PAID_EVERY_S + 3.0   # the panel's paid read refreshes it every 5s
    ):
        return _hide_closed(_GOOD["rows"])
    urls = _rpc_urls()
    if paid_only:
        paid = primary_url()
        if not paid:
            log.info("wallet read skipped — no paid RPC configured")
            return None
        urls = [paid]
    elif include_primary:
        paid = primary_url()
        if paid and allow_primary() and paid not in urls:
            urls = [paid] + urls
    seen: set = set()
    out: List[Dict[str, Any]] = []
    read_timeout = 2.5 if paid_only else 3.5
    async with httpx.AsyncClient(timeout=read_timeout) as client:
        spl, t22 = await asyncio.gather(
            _program_accounts(client, owner, TOKEN_PROGRAM, urls, timeout=read_timeout),
            _program_accounts(client, owner, TOKEN_2022, urls, timeout=read_timeout),
        )
    ok = 0
    for program, value in ((TOKEN_PROGRAM, spl), (TOKEN_2022, t22)):
        if value is None:
            log.info("token program %s had no HTTP 200", program[:8])
            continue
        ok += 1
        out.extend(_parse_rows(value, seen))
    if ok < 2:
        log.info("wallet list incomplete — one token program did not answer")
        return None
    if not out:
        _GOOD["ts"] = now
        _GOOD["owner"] = owner
        _GOOD["rows"] = []
        return []
    _remember_rows(out)
    try:
        from src.storage.mint_flags import flag_dust_if_needed
        for row in out:
            if str(row.get("state") or "").lower() == "frozen":
                flag_dust_if_needed(row.get("mint") or "", "wallet read state=frozen (0x11 AccountFrozen)")
    except Exception:
        log.debug("frozen flag failed", exc_info=True)
    _GOOD["ts"] = now
    _GOOD["owner"] = owner
    _GOOD["rows"] = out
    return _hide_closed(out)


async def token_accounts_for_mint(owner: str, mint: str, paid_first: bool = False) -> Optional[List[Dict[str, Any]]]:
    """Every token account for this mint, including a zero balance. Needed to close it and return rent."""
    if not owner or not mint:
        return []
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            owner,
            {"mint": mint},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    }
    urls: List[str] = []
    if paid_first:
        paid = primary_url()
        if paid:
            urls.append(paid)
    for url in _rpc_urls():
        if url and url not in urls:
            urls.append(url)
    async with httpx.AsyncClient(timeout=8.0) as client:
        for url in urls:
            try:
                r = await client.post(url, json=payload)
                if r.status_code != 200:
                    continue
                body = r.json() or {}
                if body.get("error"):
                    continue
                rows = []
                for item in ((body.get("result") or {}).get("value") or []):
                    info = (((item.get("account") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
                    if info.get("mint") != mint:
                        continue
                    ta = info.get("tokenAmount") or {}
                    acc = item.get("account") or {}
                    rows.append({
                        "pubkey": item.get("pubkey") or "",
                        "program": acc.get("owner") or "",
                        "raw": int(ta.get("amount") or 0),
                        "lamports": int(acc.get("lamports") or 0),
                    })
                return rows
            except Exception as e:
                log.info("token account list failed %s %s", (url or "").split("//")[-1].split("/")[0].split("?")[0], type(e).__name__)
    return None


async def empty_rent_accounts(owner: str) -> List[Dict[str, Any]]:
    """Token accounts with no coins and rent still locked. The paid RPC first."""
    if not owner:
        return []
    urls: List[str] = []
    paid = primary_url()
    if paid:
        urls.append(paid)
    for url in _rpc_urls():
        if url and url not in urls:
            urls.append(url)
    found: List[Dict[str, Any]] = []
    seen = set()
    async with httpx.AsyncClient(timeout=8.0) as client:
        for program in (TOKEN_PROGRAM, TOKEN_2022):
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    owner,
                    {"programId": program},
                    {"encoding": "jsonParsed", "commitment": "confirmed"},
                ],
            }
            for url in urls:
                try:
                    r = await client.post(url, json=payload)
                    if r.status_code != 200:
                        continue
                    body = r.json() or {}
                    if body.get("error"):
                        continue
                    for item in ((body.get("result") or {}).get("value") or []):
                        info = (((item.get("account") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
                        mint = info.get("mint") or ""
                        if not mint or mint == "So11111111111111111111111111111111111111112":
                            continue
                        ta = info.get("tokenAmount") or {}
                        raw = int(ta.get("amount") or 0)
                        acc = item.get("account") or {}
                        lamports = int(acc.get("lamports") or 0)
                        pubkey = item.get("pubkey") or ""
                        if not pubkey or pubkey in seen or lamports <= 0 or raw > 100:
                            continue
                        seen.add(pubkey)
                        found.append({
                            "pubkey": pubkey,
                            "program": acc.get("owner") or program,
                            "raw": raw,
                            "lamports": lamports,
                            "mint": mint,
                        })
                    break
                except Exception:
                    continue
    return found


async def get_token_holding(owner: str, mint: str, fast: bool = False, paid_only: bool = False, confirm_empty: int = 2, ignore_seen: bool = False) -> Optional[Dict[str, Any]]:
    """Balance of one mint. Asks the RPC for that mint only, so a big wallet cannot time the read out."""
    if not owner or not mint:
        return None
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            owner,
            {"mint": mint},
            {"encoding": "jsonParsed", "commitment": "processed" if fast else "confirmed"},
        ],
    }
    urls = [url for url in _rpc_urls() if url]
    paid = primary_url()
    if paid_only:
        if not paid:
            return None
        urls = [paid]
    elif paid and paid not in urls:
        urls.append(paid)
    timeout = 0.4 if (paid_only or fast) else 2.5
    async with httpx.AsyncClient(timeout=timeout) as client:
        empty_ok = 0
        for url in urls:
            try:
                r = await client.post(url, json=payload)
                if r.status_code != 200:
                    continue
                body = r.json() or {}
                if body.get("error"):
                    continue
                best = None
                for item in ((body.get("result") or {}).get("value") or []):
                    info = (((item.get("account") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
                    if info.get("mint") != mint:
                        continue
                    ta = info.get("tokenAmount") or {}
                    row = {
                        "raw": int(ta.get("amount") or 0),
                        "ui_amount": float(ta.get("uiAmount") or 0),
                        "decimals": int(ta.get("decimals") or 0),
                    }
                    if ta.get("decimals") is not None:
                        _DECIMALS[mint] = int(ta.get("decimals") or 0)
                    if best is None or row["raw"] > best["raw"]:
                        best = row
                if best and best["raw"] > 0:
                    _remember_rows([{"mint": mint, "raw": best["raw"], "ui_amount": best["ui_amount"]}])
                    return best
                empty_ok += 1
                need = 1 if int(confirm_empty or 2) <= 1 else 2
                if empty_ok >= need:
                    if _seen_recently(mint) and not ignore_seen:
                        log.info("token %s empty from RPC — still held a moment ago, not closing", mint[:8])
                        return None
                    return {"raw": 0, "ui_amount": 0.0, "decimals": 0}
            except Exception as e:
                log.info("token read failed %s %s", (url or "").split("//")[-1].split("/")[0].split("?")[0], type(e).__name__)
    return None


async def get_token_raw_balance(owner: str, mint: str) -> Optional[int]:
    """Raw balance, 0 if confirmed absent, None if the wallet read failed."""
    accounts = await get_token_accounts(owner)
    if accounts is None:
        return None
    for a in accounts:
        if a.get("mint") == mint:
            return int(a.get("raw") or 0)
    if _seen_recently(mint):
        return None
    return 0


WSOL = "So11111111111111111111111111111111111111112"
_PX: Dict[str, Dict[str, Any]] = {}


async def quote_mints(mints: List[str], named: bool = False) -> Dict[str, Dict[str, Any]]:
    """Pump USD price, symbol, and name. A mint prefix is not stored as a ticker."""
    now = time.time()
    need: List[str] = []
    out: Dict[str, Dict[str, Any]] = {}
    for mint in mints:
        hit = _PX.get(mint)
        age = now - float((hit or {}).get("ts") or 0)
        price_fresh = bool(hit and hit.get("price_usd") and age < 2)
        name_fresh = bool(hit and (hit.get("symbol") or hit.get("name")))
        if price_fresh and (not named or name_fresh):
            out[mint] = hit
        elif mint and mint not in need:
            need.append(mint)
    if not need:
        return out

    def _dex(mint: str) -> Dict[str, str]:
        from src.data.fast_get import pump_get
        body = pump_get(
            f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
            timeout=0.7,
            cache_s=30,
        )
        pairs = (body or {}).get("pairs") if isinstance(body, dict) else None
        for pair in pairs or []:
            base = pair.get("baseToken") or {}
            if base.get("address") == mint and (base.get("symbol") or base.get("name")):
                px = None
                try:
                    px = float(pair.get("priceUsd") or 0) or None
                except (TypeError, ValueError):
                    px = None
                return {"symbol": base.get("symbol") or "", "name": base.get("name") or "", "price_usd": px}
        if pairs:
            base = (pairs[0].get("baseToken") or {})
            px = None
            try:
                px = float((pairs[0] or {}).get("priceUsd") or 0) or None
            except (TypeError, ValueError):
                px = None
            return {"symbol": base.get("symbol") or "", "name": base.get("name") or "", "price_usd": px}
        return {"symbol": "", "name": "", "price_usd": None}

    def _one(mint: str) -> Dict[str, Any]:
        from src.data.fast_get import pump_get

        if mint == WSOL:
            body = pump_get("https://frontend-api-v3.pump.fun/sol-price", timeout=0.6, cache_s=2.0)
            try:
                px = float((body or {}).get("solPrice") or 0) or None
            except (TypeError, ValueError):
                px = None
            return {"symbol": "SOL", "name": "Solana", "price_usd": px, "ts": time.time()}
        body = pump_get(
            f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
            timeout=0.7 if named else 0.5,
            cache_s=0,
        )
        symbol = ""
        name = ""
        px = None
        if isinstance(body, dict):
            symbol = str(body.get("symbol") or "")
            name = str(body.get("name") or "")
            try:
                px = float(body.get("price_usd") or 0) or None
            except (TypeError, ValueError):
                px = None
        if named and (not symbol or not px):
            found = _dex(mint)
            symbol = symbol or found.get("symbol") or ""
            name = name or found.get("name") or ""
            if px is None:
                px = found.get("price_usd")
        if _mint_prefix(symbol, mint):
            symbol = ""
        return {"symbol": symbol, "name": name, "price_usd": px, "ts": time.time()}

    async def _lookup(mint: str) -> None:
        try:
            row = await asyncio.wait_for(asyncio.to_thread(_one, mint), timeout=1.6 if named else 0.8)
        except Exception:
            row = {"symbol": "", "name": "", "price_usd": None, "ts": now}
        kept = _PX.get(mint) or {}
        if row.get("price_usd"):
            if not row.get("symbol"):
                row["symbol"] = kept.get("symbol") or ""
            if not row.get("name"):
                row["name"] = kept.get("name") or ""
            _PX[mint] = row
            out[mint] = row
        elif kept.get("price_usd"):
            out[mint] = kept
        else:
            if row.get("symbol") or row.get("name"):
                _PX[mint] = row
            out[mint] = row

    await asyncio.gather(*(_lookup(mint) for mint in need[:12]))
    return out
