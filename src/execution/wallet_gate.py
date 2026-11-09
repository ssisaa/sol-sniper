"""One rule: do not call buy while the wallet still holds a non-dust token.

Dust (no route, or a quote under 0.001 SOL) does not block.
A failed wallet read or a failed quote that is not 'no route' does block.
The result is written to SQLite: state key wallet_now, and the wallet table.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("sol-sniper.wallet")

_on_tokens = None
_DUST_SOL = 0.001


def set_wallet_listener(fn) -> None:
    """Called with the wallet rows after each successful sync."""
    global _on_tokens
    _on_tokens = fn
_cache = {"ts": 0.0, "mint": "", "reason": "", "ignore": ""}


def _is_no_route(err: str) -> bool:
    text = (err or "").lower()
    return any(
        s in text
        for s in ("no route", "no_routes", "could_not_find", "token_not_tradable", "not tradable")
    )


_ABSENT: set = set()
_ABSENT_LOADED = False


def _load_absent() -> None:
    global _ABSENT_LOADED
    if _ABSENT_LOADED:
        return
    _ABSENT_LOADED = True
    try:
        from src.storage.supabase_store import get_store
        data = get_store().get("absent_mints") or []
        if isinstance(data, list):
            _ABSENT.update(m for m in data if isinstance(m, str) and m)
    except Exception:
        log.debug("absent mint load failed", exc_info=True)


def note_mint_present(mint: str) -> None:
    """Clear a stale absent flag. A recent sell stays sold until its timer ends."""
    mint = (mint or "").strip()
    if not mint:
        return
    _load_absent()
    if mint not in _ABSENT:
        return
    _ABSENT.discard(mint)
    try:
        from src.storage.supabase_store import get_store
        get_store().put("absent_mints", sorted(_ABSENT))
    except Exception:
        log.debug("clear absent flag failed", exc_info=True)


def forget_absent_mint(mint: str, reason: str = "") -> None:
    """Chain said this token account does not exist (6025). Remove it so restart cannot open it again."""
    mint = (mint or "").strip()
    if not mint:
        return
    _load_absent()
    _ABSENT.add(mint)
    try:
        from src.execution.wallet_tokens import mark_sold
        mark_sold(mint)
    except Exception:
        log.debug("drop sold mint from wallet cache failed", exc_info=True)
    from src.storage.supabase_store import get_store
    store = get_store()
    try:
        store.put("absent_mints", sorted(_ABSENT))
        status, rows = store.read_wallet()
        if status == "ok":
            store.replace_wallet([r for r in rows if r.get("mint") != mint])
        mints = store.get("open_mints") or []
        if isinstance(mints, list):
            store.set_open_mints([m for m in mints if m != mint])
        book = store.read_book() or {}
        if book.get("mint") == mint:
            store.write_book([])
        snap = store.get("wallet_now") or {}
        if isinstance(snap, dict):
            snap["tokens"] = [t for t in (snap.get("tokens") or []) if t.get("mint") != mint]
            snap["reason"] = ""
            snap["buy_allowed"] = True
            store.put("wallet_now", snap)
    except Exception:
        log.debug("forget absent mint failed", exc_info=True)
    log.warning("SQLITE cleared %s — token account is not in the wallet (%s)", mint[:8], (reason or "6025")[:80])


def _ledger_says_closed(mint: str) -> bool:
    try:
        from src.observability.ledger import get_ledger
        rows = [e for e in get_ledger().entries if getattr(e, "mint", "") == mint]
        if not rows:
            return False
        last = max(rows, key=lambda e: float(getattr(e, "ts", 0) or 0))
        return getattr(last, "side", "") == "CLOSE"
    except Exception:
        return False


def drop_closed_holds(stored: list) -> list:
    """A sold coin must not stay in the saved wallet and block the next buy.

    Only drops a mint the ledger already closed, or one marked sold this session.
    A live open coin is left alone.
    """
    from src.execution.wallet_tokens import sold_recently

    kept = []
    for t in stored or []:
        mint = (t or {}).get("mint") or ""
        if not mint:
            continue
        if sold_recently(mint) or _ledger_says_closed(mint):
            forget_absent_mint(mint, "sold — wallet no longer holds it")
            continue
        kept.append(t)
    return kept


def supabase_held() -> tuple:
    """('ok'|'fail'|'off', real tokens). A failed read is not an empty wallet."""
    _load_absent()
    from src.storage.supabase_store import get_store

    store = get_store()
    status, rows = store.read_wallet()
    if status == "fail":
        snap = store.get("wallet_now")
        if isinstance(snap, dict) and isinstance(snap.get("tokens"), list):
            status, rows = "ok", snap.get("tokens") or []
        else:
            return "fail", []
    real = []
    for t in rows or []:
        mint = (t or {}).get("mint") or ""
        if not mint or mint in _ABSENT:
            continue
        try:
            from src.execution.wallet_tokens import sold_recently
            if sold_recently(mint):
                continue
        except Exception:
            pass
        try:
            from src.storage.mint_flags import mint_flag
            if mint_flag(mint) == "DUST":
                continue
        except Exception:
            pass
        ui = float(t.get("ui_amount") or 0)
        raw = int(float(t.get("raw") or 0))
        val = t.get("value_sol")
        worth = val is not None and float(val) >= 0.000001
        if t.get("dust") and not worth and ui < 1 and raw < 1000:
            continue
        if worth or ui >= 1 or raw >= 1000:
            real.append({
                "mint": mint,
                "ui_amount": ui,
                "raw": raw,
                "value_sol": float(val) if worth else 0.0,
            })
    book = store.read_book() or {}
    if book.get("status") == "OPEN" and book.get("mint") and book.get("mint") not in _ABSENT:
        mint = book["mint"]
        if not any(t["mint"] == mint for t in real):
            real.append({
                "mint": mint,
                "ui_amount": 0.0,
                "raw": 0,
                "value_sol": float(book.get("entry_sol") or 0),
                "symbol": book.get("symbol") or "",
            })
    return status, real


async def wallet_buy_block(jup, buying_mint: str = "", ignore_mints=None) -> str:
    """Empty string means a buy is allowed. Anything else means do not call buy.

    ignore_mints are open Callout coins. They do not block the next Callout buy.
    Dust still does not block. Any other non-dust token still does.
    """
    ignore = {m for m in (ignore_mints or set()) if m}
    ignore_key = ",".join(sorted(ignore))
    from src.execution.signer import pubkey_from_env
    from src.execution.wallet_tokens import get_token_accounts
    now = time.time()
    if (
        now - float(_cache["ts"]) < 2
        and _cache["mint"] == (buying_mint or "")
        and _cache.get("ignore") == ignore_key
    ):
        return str(_cache["reason"] or "")
    pk = pubkey_from_env()
    if not pk:
        _save({"buy_allowed": False, "reason": "no wallet pubkey", "tokens": []})
        return "no wallet pubkey"
    status, held = supabase_held()
    if status == "fail":
        log.warning("SQLITE wallet read failed — will not treat a bad read as an empty wallet")
    try:
        accounts = await asyncio.wait_for(get_token_accounts(pk, include_primary=True), 1.0)
    except Exception:   # a slow read must not hold the buy: 1s, then the miss rule below
        accounts = None
    if not accounts:
        if status == "fail":
            log.warning("SQLITE unread — wallet read will decide")
        if held:
            held = drop_closed_holds(held)
        held = [t for t in (held or []) if t.get("mint") and t["mint"] not in ignore and t["mint"] != buying_mint]
        if held:
            t = max(held, key=lambda x: float(x.get("value_sol") or 0) or float(x.get("ui_amount") or 0))
            reason = f"saved wallet still holds {t['mint'][:8]} — no second buy"
            log.warning("NO BUY CALL — %s", reason)
            return reason
        if accounts is None:
            log.warning("wallet read missed — no coin on file, buy allowed")
            return ""
        return ""

    tokens: List[Dict[str, Any]] = []
    block = ""
    for acc in accounts:
        mint = acc.get("mint") or ""
        raw = int(acc.get("raw") or 0)
        ui = float(acc.get("ui_amount") or 0)
        if not mint or raw <= 0 or mint == buying_mint or mint in ignore:
            continue
        try:
            from src.storage.mint_flags import mint_flag
            manual = mint_flag(mint)
        except Exception:
            manual = ""
        if manual == "DUST" or str(acc.get("state") or "").lower() == "frozen" or 0 < ui < 0.01 or (ui <= 0 and raw < 1000):
            if str(acc.get("state") or "").lower() == "frozen":
                try:
                    from src.storage.mint_flags import flag_dust_if_needed
                    flag_dust_if_needed(mint, "wallet read frozen")
                except Exception:
                    pass
            tokens.append({"mint": mint, "ui_amount": ui, "raw": raw, "value_sol": 0, "dust": True})
            continue
        sol_val: Optional[float] = None
        err = ""
        try:
            q = await asyncio.wait_for(jup.quote_token_to_sol(mint, raw, slippage_bps=800), timeout=0.4)
            sol_val = (q.out_amount / 1_000_000_000) if q else None
            err = getattr(jup, "_last_error", "") or ""
        except Exception as exc:
            sol_val = None
            err = str(exc)
        dust = sol_val is None or sol_val < _DUST_SOL
        if manual == "NODUST":
            dust = False
        row = {
            "mint": mint,
            "ui_amount": ui,
            "raw": raw,
            "value_sol": sol_val,
            "dust": dust,
        }
        tokens.append(row)
        if dust:
            continue
        block = block or f"wallet still holds {mint[:8]} ui={ui:.4g} ≈{sol_val:.4f} SOL"

    reason = block
    if "wallet read failed" not in (reason or ""):
        _cache["ts"] = time.time()
        _cache["mint"] = buying_mint or ""
        _cache["reason"] = reason
        _cache["ignore"] = ignore_key
    _save({"buy_allowed": not reason, "reason": reason, "tokens": tokens, "ts": time.time()})
    if reason:
        log.warning("NO BUY CALL — %s", reason)
    return reason


def _save(state: Dict[str, Any]) -> None:
    """Write the latest wallet. An empty read never deletes a hold the file already has."""
    from src.storage.supabase_store import get_store
    store = get_store()
    if not store.enabled:
        store.put("wallet_now", state)
        if not getattr(_save, "noted", False):
            _save.noted = True
            log.info("SQLite wallet state saved")
        return
    tokens = state.get("tokens") or []
    if not tokens:
        status, rows = store.read_wallet()
        if status != "ok":
            log.info("WALLET SYNC empty and the saved wallet was unread — hold left as-is")
            return
        if rows:
            log.info("WALLET SYNC empty — saved hold kept (%d)", len(rows))
            return
    store.put("wallet_now", state)
    store.replace_wallet(tokens)


async def sync_wallet_forever(interval: float = 3.0) -> None:
    """Push the live wallet into SQLite. Back off when RPC is rate-limited so buys can get through."""
    delay = interval
    while True:
        ok = False
        try:
            ok = await sync_wallet_once()
        except Exception:
            log.debug("wallet sync failed", exc_info=True)
        delay = interval if ok else 20.0
        await asyncio.sleep(delay)


async def sync_wallet_once() -> bool:
    from src.execution.signer import pubkey_from_env
    from src.execution.wallet_tokens import get_token_accounts
    from src.storage.supabase_store import get_store

    pk = pubkey_from_env()
    if not pk:
        return
    store = get_store()
    now = time.time()
    try:
        accounts = await get_token_accounts(pk)
    except Exception:
        accounts = None
    if accounts is None or not accounts:
        store.put("wallet_sync", {"ok": False, "ts": now, "reason": "empty_or_incomplete"})
        log.info("WALLET SYNC no tokens in this read — saved hold kept")
        return False
    status, stored = supabase_held()

    known_dust = set()
    try:
        prev = store.get("wallet_now") or {}
        for t in prev.get("tokens") or []:
            if t.get("dust"):
                known_dust.add(t.get("mint"))
    except Exception:
        pass

    book = store.read_book() or {}
    open_mint = book.get("mint") if book.get("status") == "OPEN" else ""
    tokens: List[Dict[str, Any]] = []
    for acc in accounts:
        mint = acc.get("mint") or ""
        raw = int(acc.get("raw") or 0)
        ui = float(acc.get("ui_amount") or 0)
        if not mint or raw <= 0:
            continue
        dust = ui < 0.01 or mint in known_dust or str(acc.get("state") or "").lower() == "frozen"
        if str(acc.get("state") or "").lower() == "frozen":
            try:
                from src.storage.mint_flags import flag_dust_if_needed
                flag_dust_if_needed(mint, "wallet sync frozen")
            except Exception:
                pass
        if mint == open_mint and not dust:
            dust = False
        tokens.append({
            "mint": mint,
            "ui_amount": ui,
            "raw": raw,
            "value_sol": None,
            "dust": dust,
        })
    present = {t["mint"] for t in tokens}
    held_real = [t["mint"] for t in tokens if not t.get("dust")]
    if open_mint and book.get("status") == "OPEN" and open_mint not in present:
        log.info("WALLET SYNC %s not in this read — saved hold kept", open_mint[:8])
        held_real.append(open_mint)
        try:
            _status, rows = store.read_wallet()
            prev = next((r for r in (rows or []) if r.get("mint") == open_mint), None)
            if prev:
                tokens.append({
                    "mint": open_mint,
                    "ui_amount": prev.get("ui_amount"),
                    "raw": prev.get("raw"),
                    "value_sol": prev.get("value_sol"),
                    "dust": False,
                })
        except Exception:
            log.debug("keep supabase hold row failed", exc_info=True)
    try:
        store.set_open_mints(held_real)
    except Exception:
        log.debug("open_mints sync failed", exc_info=True)
    held = bool(open_mint and open_mint in held_real)
    _save({
        "buy_allowed": not held,
        "reason": f"wallet holds {open_mint[:8]}" if held else "",
        "tokens": tokens,
        "ts": now,
    })
    store.put("wallet_sync", {
        "ok": True,
        "ts": now,
        "open_mint": open_mint,
        "open_still_held": held,
        "tokens": len(tokens),
        "buy_allowed": not held,
    })
    if book.get("status") == "OPEN" and open_mint and open_mint not in present:
        log.info("WALLET SYNC %s still the open trade — book kept", open_mint[:8])
    else:
        log.info("WALLET SYNC ok tokens=%d open=%s", len(tokens), (open_mint[:8] if held else "none"))
    if _on_tokens is not None:
        try:
            await _on_tokens(tokens)
        except Exception:
            log.warning("wallet listener failed", exc_info=True)
    return True
