
"""
Live results dashboard for SOL-SNIPER.

- WebSocket broadcaster on port 8765
- Static UI on port 8080
- Bidirectional: UI can push runtime config updates
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import websockets
from websockets.server import WebSocketServerProtocol

log = logging.getLogger("sol-sniper.dashboard")

_callout_cache: Dict[str, Any] = {"ts": 0.0, "rows": [], "error": ""}
_callout_marks: Dict[str, Dict[str, Any]] = {}
_callout_mark_lock = threading.Lock()
_callout_mark_busy = False


def _callout_one_mark(mint: str) -> Dict[str, Any]:
    """Age since launch, Pump 5m and 1h, and the drop from the highest market cap."""
    import time
    import urllib.request

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://pump.fun",
        "Referer": f"https://pump.fun/coin/{mint}",
    }

    def get(url: str) -> Dict[str, Any]:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=4) as response:
            body = json.loads(response.read().decode() or "{}")
        return body if isinstance(body, dict) else {}

    age = m5 = h1 = retrace = None
    try:
        coin = get(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}")
        created = coin.get("created_timestamp")
        if created:
            born = float(created)
            if born > 1_000_000_000_000:
                born = born / 1000.0
            age = max(0.0, time.time() - born)
        ath = float(coin.get("ath_market_cap") or 0)
        now_cap = float(coin.get("usd_market_cap") or coin.get("market_cap_usd") or 0)
        if ath > 0 and now_cap > 0:
            retrace = (now_cap / ath - 1.0) * 100.0
    except Exception:
        pass
    try:
        activity = get(
            "https://swap-api.pump.fun/v1/coins/"
            f"{mint}/market-activity?program=pump&chainId=solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
        )

        def pct(key: str):
            block = activity.get(key)
            if not isinstance(block, dict) or block.get("priceChangePercent") is None:
                return None
            return float(block["priceChangePercent"])

        m5, h1 = pct("5m"), pct("1h")
    except Exception:
        pass
    return {"ts": time.time(), "age": age, "m5": m5, "h1": h1, "retrace": retrace}


def _fill_callout_marks(mints: List[str]) -> None:
    global _callout_mark_busy
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = {pool.submit(_callout_one_mark, mint): mint for mint in mints}
            for fut in as_completed(futs, timeout=8):
                mint = futs[fut]
                try:
                    mark = fut.result()
                except Exception:
                    continue
                with _callout_mark_lock:
                    _callout_marks[mint] = mark
                    if len(_callout_marks) > 400:
                        oldest = sorted(_callout_marks, key=lambda k: _callout_marks[k].get("ts") or 0)[:80]
                        for key in oldest:
                            _callout_marks.pop(key, None)
    except Exception:
        log.debug("callout marks failed", exc_info=True)
    finally:
        with _callout_mark_lock:
            _callout_mark_busy = False


def _attach_callout_marks(rows: List[Dict[str, Any]]) -> None:
    """Use cached marks now, and refresh the oldest missing ones in the background."""
    import time

    now = time.time()
    stale: List[str] = []
    with _callout_mark_lock:
        for row in rows:
            hit = _callout_marks.get(row.get("coinMint") or "")
            if not hit:
                continue
            row["ageSec"] = hit.get("age")
            row["m5"] = hit.get("m5")
            row["h1"] = hit.get("h1")
            row["retrace"] = hit.get("retrace")
        global _callout_mark_busy
        if not _callout_mark_busy:
            for row in rows:
                mint = row.get("coinMint") or ""
                hit = _callout_marks.get(mint)
                if mint and (not hit or now - float(hit.get("ts") or 0) > 20):
                    stale.append(mint)
                if len(stale) >= 12:
                    break
            if stale:
                _callout_mark_busy = True
    if stale:
        threading.Thread(target=_fill_callout_marks, args=(stale,), daemon=True).start()


def _callout_rows() -> tuple:
    """Latest open position for each home-feed wallet. Not used by Launch or Dayscalp."""
    global _callout_thread_started
    if not _callout_thread_started:
        _callout_thread_started = True
        threading.Thread(target=_callout_loop, name="callouts", daemon=True).start()
    with _callout_lock:
        return list(_callout_cache["rows"]), str(_callout_cache["error"] or "")


_callout_lock = threading.Lock()
_callout_thread_started = False
_callout_by_wallet: Dict[str, Dict[str, Any]] = {}
_callout_tapes: Dict[str, Dict[str, Any]] = {}
_PUMP_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://pump.fun",
    "Referer": "https://pump.fun/",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}


def _pump_json(url: str, timeout: float = 2.0) -> Any:
    """Pump answers this kept-open socket. A fresh urllib call is refused.

    The same URL inside one second is the cached body. A failed read keeps
    the last good body for up to three seconds.
    """
    from src.data.fast_get import pump_get

    return pump_get(url, timeout=timeout, cache_s=1.0)


def _opened_ts(value: Any) -> float:
    from datetime import datetime

    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        ts = float(value)
        return ts / 1000.0 if ts > 1_000_000_000_000 else ts
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def _token_hold(wallet: str, mint: str) -> Optional[Dict[str, Any]]:
    """The balance row whose token_mint is this coin. Pages until it is found."""
    if not wallet or not mint:
        return None
    for page in range(1, 5):
        body = _pump_json(
            "https://profile-api.pump.fun/balance/tokens/"
            f"{wallet}?page={page}&size=200&chain=all"
        )
        data = body.get("data") if isinstance(body, dict) else None
        tokens = data.get("tokens") if isinstance(data, dict) else None
        if not isinstance(tokens, list):
            return None
        for tok in tokens:
            if isinstance(tok, dict) and str(tok.get("token_mint") or "") == mint:
                return tok
        if len(tokens) < 200:
            return None
    return None


def _num(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _latest_open(user_name: str, wallet: str) -> Optional[Dict[str, Any]]:
    port = _pump_json(f"https://frontend-api-v3.pump.fun/user-portfolio/{wallet}?filter=open&sortBy=RECENCY")
    positions = port.get("positions") if isinstance(port, dict) else None
    latest = None
    latest_ts = -1.0
    for pos in positions or []:
        if not isinstance(pos, dict) or pos.get("isExited"):
            continue
        ts = _opened_ts(pos.get("openedAt"))
        if ts >= latest_ts:
            latest, latest_ts = pos, ts
    if not latest:
        return None
    total = None
    try:
        summary = _pump_json(
            "https://frontend-api-v3.pump.fun/portfolio-summary"
            f"?user={wallet}&period=1d&holdings=1"
        )
        if isinstance(summary, dict) and summary.get("totalValueUsd") is not None:
            total = float(summary["totalValueUsd"])
    except Exception:
        total = None
    coin = latest.get("coin") if isinstance(latest.get("coin"), dict) else {}
    mint = latest.get("coinMint") or ""
    price = latest.get("tokenPriceUsd")
    cost_usd = latest.get("costBasisUsd")
    unreal_usd = latest.get("pnlUsd")
    pct_usd = latest.get("pnlPercentage")
    realized_usd = latest.get("realizedPnlUsd")
    cost_sol = unreal_sol = pct_sol = realized_sol = None
    hold = None
    try:
        hold = _token_hold(wallet, mint)
    except Exception:
        hold = None
    if isinstance(hold, dict):
        if hold.get("token_price") is not None:
            price = hold.get("token_price")
        pnl = hold.get("tokenPnL") if isinstance(hold.get("tokenPnL"), dict) else {}
        if pnl.get("cost_basis_usd") is not None:
            cost_usd = pnl.get("cost_basis_usd")
        if pnl.get("unrealized_usd") is not None:
            unreal_usd = pnl.get("unrealized_usd")
        if pnl.get("percentage_usd") is not None:
            pct_usd = pnl.get("percentage_usd")
        elif pnl.get("percentage") is not None:
            pct_usd = pnl.get("percentage")
        if pnl.get("realized_pnl_usd") is not None:
            realized_usd = pnl.get("realized_pnl_usd")
        cost_sol = pnl.get("cost_basis_sol")
        unreal_sol = pnl.get("unrealized_sol")
        pct_sol = pnl.get("percentage_sol")
        realized_sol = pnl.get("realized_pnl_sol")
        if not coin.get("name"):
            coin = {
                "name": hold.get("token_name") or "",
                "symbol": hold.get("token_symbol") or coin.get("symbol"),
            }
    return {
        "userName": user_name,
        "walletAddress": wallet,
        "coinMint": mint,
        "coinName": coin.get("name") or "",
        "symbol": coin.get("symbol") or "",
        "tokenPrice": _num(price),
        "costBasisUsd": _num(cost_usd),
        "costBasisSol": _num(cost_sol),
        "unrealizedUsd": _num(unreal_usd),
        "unrealizedSol": _num(unreal_sol),
        "percentageUsd": _num(pct_usd),
        "percentageSol": _num(pct_sol),
        "percentage": _num(pct_usd),
        "realizedPnlUsd": _num(realized_usd),
        "realizedPnlSol": _num(realized_sol),
        "openedAt": latest_ts,
        "totalValueUsd": total,
    }


_own_book: Dict[str, Dict[str, Any]] = {}
_own_book_lock = threading.Lock()


def own_wallet_book(wallet: str, need: Optional[Set[str]] = None) -> Optional[Dict[str, Dict[str, Any]]]:
    """Last balance book from the same 1s refresh as Callout and Open Holds. No second request."""
    wallet = (wallet or "").strip()
    tokens = _tokens_cached(wallet)
    if not tokens:
        return None
    by_mint: Dict[str, Dict[str, Any]] = {}
    for key, tok in tokens.items():
        if not isinstance(tok, dict):
            continue
        pnl = tok.get("tokenPnL") if isinstance(tok.get("tokenPnL"), dict) else {}
        if pnl.get("percentage_sol") is None:
            continue
        if need and key not in need:
            continue
        by_mint[str(key)] = {
            "percentage_sol": pnl.get("percentage_sol"),
            "percentage_usd": pnl.get("percentage_usd"),
            "unrealized_sol": pnl.get("unrealized_sol"),
            "unrealized_usd": pnl.get("unrealized_usd"),
        }
    return by_mint or None


def refresh_own_book() -> None:
    """One new connection, then close it. A reused socket answered once and then sat."""
    import time
    from src.data.fast_get import pump_get_fresh
    from src.execution.signer import pubkey_from_env

    wallet = (pubkey_from_env() or "").strip()
    if not wallet:
        return
    try:
        body = pump_get_fresh(
            "https://profile-api.pump.fun/balance/tokens/"
            f"{wallet}?page=1&size=200&chain=all&_={int(time.time() * 1000)}",
            timeout=0.8,
        )
    except Exception:
        log.debug("own book refresh failed", exc_info=True)
        return
    data = body.get("data") if isinstance(body, dict) else None
    tokens = data.get("tokens") if isinstance(data, dict) else None
    if not isinstance(tokens, list):
        return
    found: Dict[str, Dict[str, Any]] = {}
    for tok in tokens:
        if not isinstance(tok, dict):
            continue
        key = str(tok.get("token_mint") or "")
        if key:
            found[key] = tok
    if not found:
        return
    _token_cache[wallet] = {"ts": time.time(), "tokens": found}


def apply_cached_own_book(positions) -> None:
    """Copy percentage_sol / percentage_usd onto open trades. Does not call Pump."""
    import time
    from src.execution.signer import pubkey_from_env

    wallet = (pubkey_from_env() or "").strip()
    tokens = _tokens_cached(wallet)
    if not tokens:
        return
    now = time.time()
    for pos in positions or []:
        tok = tokens.get(str(getattr(pos, "mint", "") or ""))
        if not isinstance(tok, dict):
            continue
        pnl = tok.get("tokenPnL") if isinstance(tok.get("tokenPnL"), dict) else {}
        if pnl.get("percentage_sol") is None:
            continue
        try:
            since = round(float(pnl["percentage_sol"]), 2)
        except (TypeError, ValueError):
            continue
        pos.since_buy_pct = since
        if pnl.get("percentage_usd") is not None:
            try:
                pos.since_buy_usd_pct = round(float(pnl["percentage_usd"]), 2)
            except (TypeError, ValueError):
                pass
        if pnl.get("unrealized_sol") is not None:
            try:
                pos._book_pnl_sol = float(pnl["unrealized_sol"])
            except (TypeError, ValueError):
                pass
        if not getattr(pos, "_book_pnl", False):
            pos.peak_since_pct = round(max(0.0, since), 2)
            pos.low_since_pct = round(min(0.0, since), 2)
            pos._book_pnl = True
        else:
            pos.peak_since_pct = round(max(float(getattr(pos, "peak_since_pct", 0) or 0), since, 0.0), 2)
            prev_low = getattr(pos, "low_since_pct", None)
            pos.low_since_pct = round(since if prev_low is None else min(float(prev_low), since), 2)
        pos.entry_price = 1.0
        pos.peak_price = 1.0 + float(pos.peak_since_pct) / 100.0
        pos._book_ts = now


_own_since_started = False


def start_own_since_refresh() -> None:
    """Once a second, same cadence as the Callout and Open Holds pages."""
    global _own_since_started
    if _own_since_started:
        return
    _own_since_started = True

    def _loop() -> None:
        import time
        while True:
            box: Dict[str, Any] = {}

            def work() -> None:
                try:
                    refresh_own_book()
                    box["ok"] = True
                except Exception:
                    box["ok"] = False

            job = threading.Thread(target=work, daemon=True)
            job.start()
            job.join(1.0)
            time.sleep(0.15 if box.get("ok") else 0.4)

    threading.Thread(target=_loop, name="own-since", daemon=True).start()


def own_token_pnl(wallet: str, mint: str) -> Optional[Dict[str, Any]]:
    """This wallet's exact since-buy for one mint.

    percentage_sol and percentage_usd are tokenPnL on the Pump balance book.
    A failed read keeps the last good book. It does not invent a 5m change.
    """
    book = own_wallet_book(wallet)
    if not isinstance(book, dict):
        return None
    return book.get(str(mint or ""))


def _read_tape(mint: str) -> Dict[str, Any]:
    import time

    blank = {"ts": time.time(), "m5": None, "h1": None, "m1": None, "m5_null": False, "buy": 0.0, "sell": 0.0, "buyers": 0, "sellers": 0}
    try:
        body = _pump_json(
            "https://swap-api.pump.fun/v1/coins/"
            f"{mint}/market-activity?program=pump&chainId=solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp",
            timeout=3,
        )
    except Exception:
        # A failed read is not a null tape. Keep the last good percents.
        prev = _callout_tapes.get(mint)
        if isinstance(prev, dict) and (prev.get("m5") is not None or prev.get("h1") is not None or prev.get("m5_null")):
            return prev
        return blank
    if not isinstance(body, dict) or not body:
        prev = _callout_tapes.get(mint)
        if isinstance(prev, dict) and (prev.get("m5") is not None or prev.get("h1") is not None or prev.get("m5_null")):
            return prev
        return blank

    def pct(key: str):
        if key not in body:
            return None, False
        raw = body.get(key)
        if raw is None:
            return None, key == "5m"
        if not isinstance(raw, dict):
            return None, False
        value = raw.get("priceChangePercent")
        if value is None:
            return None, key == "5m"
        try:
            return float(value), False
        except (TypeError, ValueError):
            return None, False

    m5, m5_null = pct("5m")
    h1, _h1_null = pct("1h")
    m1, _m1_null = pct("1m")
    five = body.get("5m") if isinstance(body.get("5m"), dict) else {}
    return {
        "ts": time.time(),
        "m5": m5,
        "h1": h1,
        "m1": m1,
        "m5_null": m5_null,
        "buy": _num(five.get("buyVolumeUSD")),
        "sell": _num(five.get("sellVolumeUSD")),
        "buyers": int(_num(five.get("numBuyers"))),
        "sellers": int(_num(five.get("numSellers"))),
    }


def _callout_decision(row: Dict[str, Any], fresh_wallets: int, tape: Dict[str, Any], now: float):
    age = max(0.0, now - float(row.get("openedAt") or now))
    pct = _num(row.get("percentageUsd"))
    realized = _num(row.get("realizedPnlUsd"))
    cost = _num(row.get("costBasisUsd"))
    m5 = tape.get("m5")
    h1 = tape.get("h1")
    buy = _num(tape.get("buy"))
    sell = _num(tape.get("sell"))
    buyers = int(_num(tape.get("buyers")))
    sellers = int(_num(tape.get("sellers")))
    buy_share = buy / (buy + sell) if (buy + sell) > 0 else None
    crowd = f" {fresh_wallets} wallets opened it in the last 5m." if fresh_wallets >= 2 else ""
    tape_txt = "5m tape not in yet" if m5 is None else f"5m {m5:+.1f}%"

    if pct <= -12:
        return "CLOSE", f"Caller is {pct:+.0f}%. The open failed. Close it, do not add. {tape_txt}."
    if pct >= 40:
        return "CLOSE", f"Caller is already {pct:+.0f}%. That is the exit, not a new open. {tape_txt}."
    if m5 is not None and m5 <= -4 and age >= 90:
        return "CLOSE", f"5m is {m5:+.1f}% and this open is no longer fresh. The push is over."
    if m5 is not None and h1 is not None and m5 >= 45 and (buy_share is None or buy_share < 0.5):
        return "CLOSE", f"5m is {m5:+.1f}% but buyers no longer have the dollars. Close, do not chase."
    if cost > 0 and realized >= max(20.0, cost * 0.5) and pct < 8:
        return "CLOSE", f"They already booked ${realized:.0f} and the rest is only {pct:+.0f}%. Not a fresh open."

    early_tape = m5 is None or (0 <= m5 <= 25 and (h1 is None or h1 > -15))
    buyers_ok = buy_share is None or (buy_share >= 0.55 and buyers >= sellers)
    if age < 300 and -8 <= pct <= 25 and early_tape and buyers_ok:
        return "OPEN", f"New open, caller {pct:+.0f}%. Not extended. {tape_txt}.{crowd}"
    if age < 900 and m5 is not None and 0.3 <= m5 <= 18 and -8 <= pct <= 20 and buyers_ok:
        return "OPEN", f"5m is {m5:+.1f}% and the caller is only {pct:+.0f}%. The move is still early.{crowd}"
    if age >= 300 and (m5 is None or m5 < 0.3):
        return "WAIT", f"Opened {int(age)}s ago and {tape_txt}. Stand aside until a new open prints."
    return "WAIT", f"Caller {pct:+.0f}%, {tape_txt}. No clean open and no reason to close."


def _apply_calls(rows: List[Dict[str, Any]]) -> None:
    """OPEN, CLOSE, or WAIT. A read of the callout only. It does not send an order."""
    import time

    now = time.time()
    needed: List[str] = []
    for row in rows:
        mint = row.get("coinMint") or ""
        hit = _callout_tapes.get(mint)
        if mint and (not hit or now - float(hit.get("ts") or 0) > 20) and mint not in needed:
            needed.append(mint)
    for mint in needed[:6]:
        _callout_tapes[mint] = _read_tape(mint)
    if len(_callout_tapes) > 500:
        oldest = sorted(_callout_tapes, key=lambda key: _callout_tapes[key].get("ts") or 0)[:80]
        for key in oldest:
            _callout_tapes.pop(key, None)
    fresh: Dict[str, int] = {}
    for row in rows:
        mint = row.get("coinMint") or ""
        age = now - float(row.get("openedAt") or now)
        if age < 300:
            fresh[mint] = fresh.get(mint, 0) + 1
    for row in rows:
        mint = row.get("coinMint") or ""
        tape = _callout_tapes.get(mint) or {}
        call, reason = _callout_decision(row, fresh.get(mint, 0), tape, now)
        row["call"] = call
        row["callReason"] = reason
        row["m5"] = None if tape.get("m5_null") else tape.get("m5")
        row["m5_null"] = bool(tape.get("m5_null"))
        row["h1"] = tape.get("h1")
        row["m1"] = tape.get("m1")


def _callout_loop() -> None:
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.data.fast_get import callout_get

    started_ms = int(time.time() * 1000)
    feed_url = (
        "https://frontend-api-v3.pump.fun/home-feed"
        "?pageSize=48&chain=all&platform=WEB"
        "&session_id=pump_session_mvmka4se_2l262q24126z55550h5l2n5j"
        "&device_id=device-911872e4-ada0-4f57-98d5-313c4b4ea4cc"
        f"&session_start_timestamp_ms={started_ms}"
    )
    last_warn = 0.0
    while True:
        started = time.time()
        try:
            feed = callout_get(feed_url, timeout=4.0, cache_s=2.0)
            people = []
            seen = set()
            for coin in (feed.get("coins") or [])[:150]:
                if not isinstance(coin, dict):
                    continue
                pos = coin.get("position") if isinstance(coin.get("position"), dict) else {}
                wallet = pos.get("walletAddress") or ""
                if wallet and coin.get("coinMint"):
                    _note_feed_caller(coin, pos)
                if not wallet or wallet in seen:
                    continue
                seen.add(wallet)
                people.append((pos.get("userName") or "", wallet))
            fresh: Dict[str, Dict[str, Any]] = {}
            with _callout_lock:
                kept = dict(_callout_by_wallet)
            if people:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futs = {
                        pool.submit(_latest_open, name, wallet): wallet
                        for name, wallet in people
                    }
                    for fut in as_completed(futs):
                        wallet = futs[fut]
                        try:
                            row = fut.result()
                        except Exception:
                            row = None
                        if row:
                            fresh[wallet] = row
                        merged = dict(kept)
                        merged.update(fresh)
                        shown_map = {w: merged[w] for _name, w in people if w in merged}
                        shown = sorted(shown_map.values(), key=lambda item: float(item.get("openedAt") or 0), reverse=True)
                        _apply_calls(shown)
                        with _callout_lock:
                            _callout_by_wallet.clear()
                            _callout_by_wallet.update(shown_map)
                            _callout_cache["rows"] = shown
                            _callout_cache["error"] = ""
                            _callout_cache["ts"] = time.time()
                        remember_callers(shown)
            else:
                with _callout_lock:
                    _callout_cache["error"] = "home feed empty"
                    _callout_cache["ts"] = time.time()
        except Exception as exc:
            with _callout_lock:
                _callout_cache["error"] = type(exc).__name__
                _callout_cache["ts"] = time.time()
            blocked = "403" in str(exc)
            if not blocked or time.time() - last_warn > 30:
                log.warning("callout refresh failed: %s", exc)
                last_warn = time.time()
            time.sleep(30.0 if blocked else max(0.4, 2.0 - (time.time() - started)))
            continue
        time.sleep(max(0.4, 3.0 - (time.time() - started)))


_predict_cache: Dict[str, Any] = {"ts": 0.0, "rows": [], "error": ""}


def _predict_call(m5, h1, buy_share, buyers, sellers, volume) -> Dict[str, Any]:
    """When to open a long, when to close it, and when the other way is the fade.

    Tape read only. The open is while the 5-minute is still climbing and
    buyers still have the dollars. The close is when that 5-minute is
    extended or sellers take it. The fade is the same tape, traded the other way.
    """
    if m5 is None or h1 is None or buy_share is None:
        return {
            "call": "WAIT",
            "open": "No open — the 5m tape is missing",
            "close": "No close — this page holds nothing",
            "confidence": 0,
            "reason": "Pump did not return a 5m and 1h tape, so there is no moment to call.",
        }
    buy_pct = buy_share * 100.0
    buyers = int(buyers or 0)
    sellers = int(sellers or 0)
    volume = float(volume or 0)
    gap = m5 - h1
    fade = (m5 >= 45 and buy_share < 0.50) or (m5 <= -12 and h1 >= 25) or (
        sellers > buyers and m5 <= -8 and volume >= 400
    )
    close = (m5 <= -4 and h1 >= 8) or m5 >= 55 or (buy_share <= 0.42 and h1 >= 10 and volume >= 300)
    open_long = (
        4 <= m5 <= 30
        and h1 >= -8
        and buy_share >= 0.60
        and buyers > sellers
        and volume >= 300
        and gap <= 28
    )
    if fade:
        call, confidence = "FADE", 62 + min(20, abs(m5) / 5)
        open_at, close_at = "Do not open a long", "Close now if you are long"
        reason = (
            f"Other way. 5m is {m5:+.1f}% and only {buy_pct:.0f}% of that volume is buying "
            f"({buyers} buyers / {sellers} sellers), while 1h is {h1:+.1f}%. "
            f"The push is already being sold. Selling into it is the trade, not buying it."
        )
    elif close:
        call, confidence = "CLOSE", 60 + min(18, abs(m5) / 6)
        open_at, close_at = "Do not open", "Close the long now"
        why = (
            "The 5m has already run too far to still be an entry."
            if m5 >= 55
            else "The last 5 minutes stopped agreeing with the hour, which is when these coins give the profit back."
        )
        reason = (
            f"Close, don't add. 5m is {m5:+.1f}% against a 1h of {h1:+.1f}%, "
            f"and {buy_pct:.0f}% of the last 5m dollars are buys (${volume:,.0f}). {why}"
        )
    elif open_long:
        call, confidence = "OPEN", 64 + min(16, buy_pct / 10)
        open_at = "Open the long now"
        close_at = "Close if 5m turns negative, or at about +25% from this print"
        reason = (
            f"Open. 5m is {m5:+.1f}% and 1h is {h1:+.1f}%, so the move is underway but not vertical. "
            f"{buy_pct:.0f}% of the 5m volume (${volume:,.0f}) is buying, {buyers} buyers against {sellers} sellers. "
            f"That is the window. Leave if the next 5m flips red, or hand it back near +25%."
        )
    else:
        call, confidence = "WAIT", 40
        if m5 > 30:
            open_at, close_at = "Missed — wait for a cooler 5m", "If already long, tighten it. A new open is late"
            why = "the 5m is already extended, so a new open is chasing"
        elif buy_share < 0.60:
            open_at, close_at = "Not yet — buyers do not have the tape", "No new close. Nothing here is an open"
            why = "buyers do not clearly own the last 5 minutes"
        elif volume < 300:
            open_at, close_at = "Not yet — the 5m is too thin", "No close. Thin tape is not a position"
            why = "there is not enough 5m volume to trust the print"
        else:
            open_at, close_at = "Not this print", "No close signal"
            why = "the 5m and the 1h do not agree on a moment"
        reason = (
            f"Stand aside. 5m {m5:+.1f}%, 1h {h1:+.1f}%, buy share {buy_pct:.0f}% "
            f"of ${volume:,.0f}, {buyers} buyers / {sellers} sellers. {why[0].upper() + why[1:]}."
        )
    return {
        "call": call,
        "open": open_at,
        "close": close_at,
        "confidence": int(max(0, min(90, confidence))),
        "reason": reason,
    }


def _predict_feed() -> tuple:
    """Cached Pump tape. A stale cache is refreshed off the request so the page does not wait."""
    import time

    now = time.time()
    rows = list(_predict_cache.get("rows") or [])
    err = str(_predict_cache.get("error") or "")
    if now - float(_predict_cache.get("ts") or 0) >= 8 or not rows:
        _kick_predict_feed()
    return rows, err


_predict_fill_busy = False


def _kick_predict_feed() -> None:
    global _predict_fill_busy
    if _predict_fill_busy:
        return
    _predict_fill_busy = True
    threading.Thread(target=_fill_predict_feed, name="predict-feed", daemon=True).start()


def _fill_predict_feed() -> None:
    """The old blocking feed read. Runs off the page request."""
    global _predict_fill_busy
    import time
    import urllib.request
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://pump.fun",
            "Referer": "https://pump.fun/",
        }

        def get(url: str) -> Dict[str, Any]:
            from src.data.fast_get import predict_get
            body = predict_get(url, timeout=4.0, cache_s=2.0)
            return body if isinstance(body, dict) else {}

        err = ""
        try:
            feed = get(
                "https://frontend-api-v3.pump.fun/home-feed"
                "?pageSize=24&chain=solana&platform=WEB"
                "&session_id=pump_session_mvjp2fnz_0j13193t0k4j4s2h6z474m65"
                "&device_id=device-911872e4-ada0-4f57-98d5-313c4b4ea4cc"
            )
            coins = [c for c in (feed.get("coins") or []) if isinstance(c, dict) and c.get("coinMint")][:10]
        except Exception as exc:
            _predict_cache["error"] = type(exc).__name__
            _predict_cache["ts"] = time.time()
            return

        def one(coin: Dict[str, Any]) -> Dict[str, Any]:
            mint = str(coin.get("coinMint") or "")
            m5 = h1 = buy_share = None
            buyers = sellers = 0
            volume = 0.0
            try:
                act = get(
                    "https://swap-api.pump.fun/v1/coins/"
                    f"{mint}/market-activity?program=pump&chainId=solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
                )
                block5 = act.get("5m") if isinstance(act.get("5m"), dict) else {}
                block1 = act.get("1h") if isinstance(act.get("1h"), dict) else {}
                if block5.get("priceChangePercent") is not None:
                    m5 = float(block5["priceChangePercent"])
                if block1.get("priceChangePercent") is not None:
                    h1 = float(block1["priceChangePercent"])
                volume = float(block5.get("volumeUSD") or 0)
                buy_vol = float(block5.get("buyVolumeUSD") or 0)
                buy_share = (buy_vol / volume) if volume > 0 else None
                buyers = int(block5.get("numBuyers") or 0)
                sellers = int(block5.get("numSellers") or 0)
            except Exception:
                pass
            call = _predict_call(m5, h1, buy_share, buyers, sellers, volume)
            return {
                "symbol": coin.get("symbol") or "",
                "coinName": coin.get("coinName") or "",
                "coinMint": mint,
                "marketCap": coin.get("marketCap"),
                "m5": m5,
                "h1": h1,
                "buyShare": None if buy_share is None else round(buy_share * 100.0, 1),
                "volume5m": volume,
                **call,
            }

        rows: List[Dict[str, Any]] = []
        try:
            with ThreadPoolExecutor(max_workers=5) as pool:
                futs = [pool.submit(one, coin) for coin in coins]
                for fut in as_completed(futs, timeout=8):
                    try:
                        rows.append(fut.result())
                    except Exception:
                        continue
        except Exception as exc:
            err = type(exc).__name__
        order = {"OPEN": 0, "CLOSE": 1, "FADE": 2, "WAIT": 3}
        rows.sort(key=lambda row: (order.get(row.get("call"), 9), -(row.get("confidence") or 0)))
        if rows:
            _predict_cache["ts"] = time.time()
            _predict_cache["rows"] = rows
            _predict_cache["error"] = err
        elif err:
            _predict_cache["error"] = err
            _predict_cache["ts"] = time.time()
    finally:
        _predict_fill_busy = False


_predicted: List[Dict[str, Any]] = []


def remember_predicted(row: Dict[str, Any], state: str) -> None:
    """Keep a predicted coin on the Predict page after the live feed moves on."""
    mint = str((row or {}).get("coinMint") or "")
    if not mint:
        return
    kept = {
        "symbol": row.get("symbol") or "",
        "coinName": row.get("coinName") or "",
        "coinMint": mint,
        "marketCap": row.get("marketCap"),
        "m5": row.get("m5"),
        "h1": row.get("h1"),
        "buyShare": row.get("buyShare"),
        "call": row.get("call") or "OPEN",
        "open": row.get("open") or "",
        "close": row.get("close") or "",
        "confidence": row.get("confidence"),
        "reason": row.get("reason") or "",
        "state": state,
    }
    _predicted[:] = [item for item in _predicted if item.get("coinMint") != mint]
    _predicted.insert(0, kept)
    del _predicted[40:]


def mark_predicted(mint: str, state: str) -> None:
    for item in _predicted:
        if item.get("coinMint") == mint:
            item["state"] = state
            return


_called: List[Dict[str, Any]] = []
_called_lock = threading.Lock()
_feed_callers: Dict[str, Dict[str, Any]] = {}
_pin_path = Path(__file__).resolve().parents[2] / "logs" / "callout_pins.json"
_pin_save_at = 0.0
_pin_live_started = False
_STUB_NAMES = {"", "bot", "—", "-", "none", "null"}


def _useful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and value.strip().lower() in _STUB_NAMES:
        return False
    return value != ""


def _load_called() -> None:
    if not _pin_path.is_file():
        return
    try:
        data = json.loads(_pin_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(data, list):
        return
    rows = [row for row in data if isinstance(row, dict) and row.get("coinMint")]
    with _called_lock:
        _called[:] = rows[:40]


def _save_called(force: bool = False) -> None:
    global _pin_save_at
    import time

    now = time.time()
    if not force and now - _pin_save_at < 5:
        return
    _pin_save_at = now
    with _called_lock:
        raw = json.dumps(_called)
    try:
        _pin_path.parent.mkdir(parents=True, exist_ok=True)
        _pin_path.write_text(raw, encoding="utf-8")
    except Exception:
        log.debug("callout pin save failed", exc_info=True)


def _load_called_once() -> None:
    _load_called()


_load_called_once()

_caller_path = Path(__file__).resolve().parents[2] / "logs" / "callout_users.json"
_caller_lock = threading.Lock()
_callers: Dict[str, Dict[str, str]] = {}
_caller_save_at = 0.0


def _load_callers() -> None:
    if not _caller_path.is_file():
        return
    try:
        data = json.loads(_caller_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(data, list):
        return
    with _caller_lock:
        for row in data:
            if not isinstance(row, dict):
                continue
            user = str(row.get("userName") or "").strip()
            wallet = str(row.get("walletAddress") or "").strip()
            if _useful(user) and _useful(wallet):
                _callers[user.casefold()] = {"userName": user, "walletAddress": wallet}


def _save_callers(force: bool = False) -> None:
    global _caller_save_at
    import time

    now = time.time()
    if not force and now - _caller_save_at < 5:
        return
    _caller_save_at = now
    with _caller_lock:
        raw = json.dumps(list(_callers.values()))
    try:
        _caller_path.parent.mkdir(parents=True, exist_ok=True)
        _caller_path.write_text(raw, encoding="utf-8")
    except Exception:
        log.debug("callout user save failed", exc_info=True)


def remember_callers(rows: List[Dict[str, Any]]) -> None:
    """One wallet per Callout username. The same name overwrites the wallet."""
    changed = False
    with _caller_lock:
        for row in rows:
            if not isinstance(row, dict):
                continue
            user = str(row.get("userName") or "").strip()
            wallet = str(row.get("walletAddress") or "").strip()
            if not _useful(user) or not _useful(wallet):
                continue
            key = user.casefold()
            prev = _callers.get(key) or {}
            total = row.get("totalValueUsd")
            try:
                total_n = float(total) if total not in (None, "") else None
            except (TypeError, ValueError):
                total_n = None
            same_id = prev.get("userName") == user and prev.get("walletAddress") == wallet
            same_total = total_n is None or prev.get("totalValueUsd") == total_n
            if same_id and same_total:
                continue
            stored = {"userName": user, "walletAddress": wallet}
            if total_n is not None:
                stored["totalValueUsd"] = total_n
            elif prev.get("totalValueUsd") is not None:
                stored["totalValueUsd"] = prev.get("totalValueUsd")
            _callers[key] = stored
            changed = True
    if changed:
        _save_callers()


def caller_directory() -> List[Dict[str, Any]]:
    with _caller_lock:
        rows = [dict(row) for row in _callers.values()]
    live: Dict[str, float] = {}
    with _callout_lock:
        cached = list(_callout_cache.get("rows") or [])
    for row in cached:
        user = str(row.get("userName") or "").strip().casefold()
        total = row.get("totalValueUsd")
        if not user or total in (None, ""):
            continue
        try:
            live[user] = float(total)
        except (TypeError, ValueError):
            continue
    for row in rows:
        key = str(row.get("userName") or "").casefold()
        if key in live:
            row["totalValueUsd"] = live[key]

    def worth(row: Dict[str, Any]) -> float:
        try:
            return float(row.get("totalValueUsd"))
        except (TypeError, ValueError):
            return -1.0

    rows.sort(key=lambda row: (-worth(row), str(row.get("userName") or "").casefold()))
    return rows


def wallet_for_user(name: str) -> Dict[str, str]:
    key = str(name or "").strip().casefold()
    with _caller_lock:
        hit = dict(_callers.get(key) or {})
    if not hit:
        return {"userName": str(name or "").strip(), "walletAddress": ""}
    return hit


_load_callers()

_watch_path = Path(__file__).resolve().parents[2] / "logs" / "openholds_watch.json"
_watch_lock = threading.Lock()
_openholds_watch: Dict[str, Any] = {"picks": []}


def _clean_pick(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    user = str(item.get("user") or "").strip()
    if not user:
        return None
    try:
        since = float(item.get("since") or 0)
    except (TypeError, ValueError):
        since = 0.0
    return {"user": user, "since": since, "all": bool(item.get("all"))}


def _load_openholds_watch() -> None:
    if not _watch_path.is_file():
        return
    try:
        data = json.loads(_watch_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(data, dict):
        return
    picks: List[Dict[str, Any]] = []
    raw_picks = data.get("picks")
    if isinstance(raw_picks, list):
        for item in raw_picks:
            clean = _clean_pick(item)
            if clean:
                picks.append(clean)
    else:
        user = str(data.get("user") or "").strip()
        if user:
            try:
                since = float(data.get("since") or 0)
            except (TypeError, ValueError):
                since = 0.0
            picks.append({"user": user, "since": since, "all": bool(data.get("all"))})
    with _watch_lock:
        _openholds_watch["picks"] = picks


def _save_openholds_watch() -> None:
    with _watch_lock:
        raw = json.dumps({"picks": list(_openholds_watch.get("picks") or [])})
    try:
        _watch_path.parent.mkdir(parents=True, exist_ok=True)
        _watch_path.write_text(raw, encoding="utf-8")
    except Exception:
        log.debug("openholds watch save failed", exc_info=True)


def openholds_watch() -> Dict[str, Any]:
    with _watch_lock:
        return {"picks": [dict(item) for item in (_openholds_watch.get("picks") or [])]}


def pick_openholds_user(name: str) -> Dict[str, Any]:
    """Add this dropdown caller. A caller already picked stays, with the same clock."""
    import time

    user = str(name or "").strip()
    if not user:
        return openholds_watch()
    with _watch_lock:
        picks = [dict(item) for item in (_openholds_watch.get("picks") or [])]
        if not any(str(item.get("user") or "").casefold() == user.casefold() for item in picks):
            picks.append({"user": user, "since": time.time(), "all": False})
        _openholds_watch["picks"] = picks
    _save_openholds_watch()
    return openholds_watch()


def drop_openholds_user(name: str) -> Dict[str, Any]:
    """Remove one dropdown caller. The pinned Callout holder is left alone."""
    user = str(name or "").strip().casefold()
    with _watch_lock:
        picks = [
            dict(item) for item in (_openholds_watch.get("picks") or [])
            if str(item.get("user") or "").casefold() != user
        ]
        _openholds_watch["picks"] = picks
    _save_openholds_watch()
    return openholds_watch()


def set_openholds_existing(show: bool, user: str = "") -> Dict[str, Any]:
    """Openexists is per caller. An empty name applies to every picked caller."""
    want = str(user or "").strip().casefold()
    with _watch_lock:
        picks = []
        for item in _openholds_watch.get("picks") or []:
            row = dict(item)
            if not want or str(row.get("user") or "").casefold() == want:
                row["all"] = bool(show)
            picks.append(row)
        _openholds_watch["picks"] = picks
    _save_openholds_watch()
    return openholds_watch()


def clear_openholds_user() -> Dict[str, Any]:
    with _watch_lock:
        _openholds_watch["picks"] = []
    _save_openholds_watch()
    return openholds_watch()


_load_openholds_watch()


def callout_entry_marks(mint: str) -> Dict[str, Any]:
    """Pump 5m, 1m, and 1h at this moment. Used once at a Callout open and once at its close."""
    tape = _read_tape(mint)
    return {"m5": tape.get("m5"), "m1": tape.get("m1"), "h1": tape.get("h1")}


def caller_still_holds(wallet: str, name: str, symbol: str, mint: str) -> str:
    """'live' if this coin is in the caller's open portfolio, 'out' if a good reply lacks it, else 'unknown'."""
    return caller_hold_state(wallet, name, symbol, mint)


def _book_has(positions: List[Dict[str, Any]], mint: str, name: str, symbol: str) -> bool:
    want_mint = (mint or "").strip()
    want_name = " ".join((name or "").split()).casefold()
    want_symbol = " ".join((symbol or "").split()).casefold()
    for row in positions:
        if not isinstance(row, dict) or row.get("isExited"):
            continue
        coin = row.get("coin") if isinstance(row.get("coin"), dict) else {}
        row_mint = str(row.get("coinMint") or coin.get("mint") or coin.get("coinMint") or "")
        row_name = " ".join(str(coin.get("name") or row.get("name") or "").split()).casefold()
        row_symbol = " ".join(str(coin.get("symbol") or row.get("symbol") or "").split()).casefold()
        if want_mint and row_mint == want_mint:
            return True
        if want_name and want_symbol and row_name == want_name and row_symbol == want_symbol:
            return True
    return False


_hold_cache: Dict[str, Dict[str, Any]] = {}
_hold_lock = threading.Lock()


def _wallet_tokens(wallet: str) -> Dict[str, Dict[str, Any]]:
    found: Dict[str, Dict[str, Any]] = {}
    for page in range(1, 4):
        try:
            body = _pump_json(
                "https://profile-api.pump.fun/balance/tokens/"
                f"{wallet}?page={page}&size=200&chain=all",
                timeout=3,
            )
        except Exception:
            break
        data = body.get("data") if isinstance(body, dict) else None
        tokens = data.get("tokens") if isinstance(data, dict) else None
        if not isinstance(tokens, list):
            break
        for tok in tokens:
            if not isinstance(tok, dict):
                continue
            key = str(tok.get("token_mint") or "")
            if key:
                found[key] = tok
        if len(tokens) < 200:
            break
    return found


def _caller_book(wallet: str) -> Dict[str, Any]:
    """One open-portfolio read, shared for a second. A failed read keeps the last good one."""
    import time

    wallet = (wallet or "").strip()
    now = time.time()
    with _hold_lock:
        hit = _hold_cache.get(wallet)
        if hit and now - float(hit.get("ts") or 0) < 1.0:
            return hit
    book: Dict[str, Any] = {"ts": now, "ok": False, "positions": [], "tokens": {}, "error": ""}
    if not wallet:
        book["error"] = "no wallet"
        return book
    try:
        port = _pump_json(
            # RECENCY: pump returns only 200 positions. Sorted by value (the default), a caller
            # with hundreds of coins hides a small new open; by recency it is first.
            f"https://frontend-api-v3.pump.fun/user-portfolio/{wallet}?filter=open&sortBy=RECENCY",
            timeout=0.6,
        )
    except Exception as exc:
        book["error"] = type(exc).__name__
    else:
        positions = port.get("positions") if isinstance(port, dict) else None
        if isinstance(positions, list):
            book["ok"] = True
            book["positions"] = [row for row in positions if isinstance(row, dict)]
            book["error"] = ""
            total = 0.0
            have_value = False
            for row in book["positions"]:
                if row.get("isExited"):
                    continue
                try:
                    worth = float(row.get("valueUsd") or row.get("currentValueUsd") or row.get("value") or 0)
                except (TypeError, ValueError):
                    worth = 0.0
                if worth > 0:
                    have_value = True
                    total += worth
            if have_value:
                book["totalValueUsd"] = total
        else:
            book["error"] = "bad portfolio"
    with _hold_lock:
        prev = _hold_cache.get(wallet)
        if not book["ok"] and prev and prev.get("ok"):
            return prev
        _hold_cache[wallet] = book
    return book


def caller_hold_state(wallet: str, name: str, symbol: str, mint: str) -> str:
    if not _useful(wallet):
        return "unknown"
    book = _caller_book(wallet)
    if not book.get("ok"):
        return "unknown"
    polled = "live" if _book_has(book.get("positions") or [], mint, name, symbol) else "out"
    return _stream_hold(wallet, mint, polled)


def caller_hold_error(wallet: str) -> str:
    with _hold_lock:
        hit = _hold_cache.get((wallet or "").strip()) or {}
    return str(hit.get("error") or "")


_hold_by_mint: Dict[str, str] = {}
_token_cache: Dict[str, Dict[str, Any]] = {}
_token_busy: Set[str] = set()


def hold_for_mint(mint: str) -> str:
    """Last live/out read for this pinned mint. Empty if we have not had a good reply yet."""
    return _hold_by_mint.get(str(mint or ""), "")


def _cached_tokens(wallet: str) -> Dict[str, Dict[str, Any]]:
    """Balance rows for this wallet. Cached one second so the page can refresh live."""
    import time

    wallet = (wallet or "").strip()
    now = time.time()
    hit = _token_cache.get(wallet)
    if hit and now - float(hit.get("ts") or 0) < 1.0:
        return hit.get("tokens") or {}
    try:
        found = _wallet_tokens(wallet)
    except Exception:
        found = (hit or {}).get("tokens") or {}
    _token_cache[wallet] = {"ts": now, "tokens": found}
    return found


def _pin_caller(pin: Dict[str, Any]) -> Dict[str, str]:
    mint = str(pin.get("coinMint") or "")
    ident = caller_identity(mint) if mint else {}
    wallet = str(pin.get("walletAddress") or "")
    if not _useful(wallet):
        wallet = str(ident.get("wallet") or "")
    user = str(pin.get("userName") or "")
    if not _useful(user):
        user = str(ident.get("user") or "") or "caller"
    name = str(pin.get("coinName") or "") or str(ident.get("name") or "")
    symbol = str(pin.get("symbol") or "") or str(ident.get("symbol") or "")
    return {"mint": mint, "wallet": wallet, "user": user, "name": name, "symbol": symbol}


def refresh_pin_holds() -> List[Dict[str, Any]]:
    """The same caller wallet OpenHolds uses. live if that mint is still open, out if a good reply lacks it.

    One portfolio read per wallet, in parallel. A slow wallet does not hold the others.
    """
    from concurrent.futures import ThreadPoolExecutor

    with _called_lock:
        pins = [dict(row) for row in _called if row.get("state") in ("HOLDING", "BUYING")]
    ready: List[Dict[str, Any]] = []
    planned = []
    wallets: List[str] = []
    for pin in pins:
        who = _pin_caller(pin)
        wallet = who["wallet"]
        planned.append((pin, who))
        if _useful(wallet) and wallet not in wallets:
            wallets.append(wallet)
    books: Dict[str, Dict[str, Any]] = {}
    if wallets:
        workers = min(4, len(wallets))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for wallet, book in zip(wallets, pool.map(_caller_book, wallets)):
                books[wallet] = book
    for pin, who in planned:
        mint = who["mint"]
        wallet = who["wallet"]
        if not _useful(wallet):
            if mint:
                _hold_by_mint[mint] = "unknown"
            ready.append({
                "pin": pin,
                "who": who,
                "book": {"ok": False, "error": "no wallet", "positions": []},
            })
            continue
        book = books.get(wallet) or {"ok": False, "error": "checking", "positions": []}
        ready.append({"pin": pin, "who": who, "book": book})
        if not mint:
            continue
        if not book.get("ok"):
            _hold_by_mint[mint] = "unknown"
            continue
        polled = "live" if _book_has(book.get("positions") or [], mint, who["name"], who["symbol"]) else "out"
        _hold_by_mint[mint] = _stream_hold(wallet, mint, polled)
    return ready


def _cached_tape(mint: str) -> Dict[str, Any]:
    """Last 5m/1h for this mint. A stale or missing tape is read now, with a short timeout."""
    import time

    mint = str(mint or "")
    hit = _callout_tapes.get(mint)
    if isinstance(hit, dict) and time.time() - float(hit.get("ts") or 0) < 2:
        return hit
    try:
        tape = _read_tape(mint)
    except Exception:
        tape = hit if isinstance(hit, dict) else {}
    if tape:
        _callout_tapes[mint] = tape
    return tape or {}


def _stamp_tape(row: Dict[str, Any]) -> None:
    """Fill 5m from the tape cache. Does not wait on the network."""
    mint = str(row.get("coinMint") or "")
    tape = _callout_tapes.get(mint) if mint else None
    if not isinstance(tape, dict):
        return
    if tape.get("m5_null"):
        row["m5"] = None
        row["m5_null"] = True
    elif tape.get("m5") is not None:
        row["m5"] = tape.get("m5")
        row["m5_null"] = False
    if tape.get("h1") is not None:
        row["h1"] = tape.get("h1")


def _pick(source: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None


def _pos_price(pos: Dict[str, Any], tok: Dict[str, Any]) -> Any:
    for value in (tok.get("token_price"), pos.get("tokenPriceUsd")):
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    try:
        held = float(pos.get("amountHeld") or 0)
        worth = float(pos.get("valueUsd") or 0)
    except (TypeError, ValueError):
        return None
    if held > 0 and worth > 0:
        return worth / held
    return None


def _caller_bought(wallet: str, mint: str) -> bool:
    """True when the live socket saw this caller's own buy (or sell) of this coin."""
    stream = _stream_if_on()
    if stream is None or not wallet or not mint:
        return False
    try:
        return bool(stream.saw_trade(wallet, mint))
    except Exception:
        return False


def _no_buy(pos: Dict[str, Any]) -> bool:
    """pump recorded no buy by this caller: the tokens only arrived by transfer."""
    try:
        bought = float(pos.get("amountBought") or 0)
        trades = int(pos.get("tradeCount") or 0)
    except (TypeError, ValueError):
        return False
    return bought <= 0 and trades <= 0


def _hold_rows(book: Dict[str, Any], tokens: Dict[str, Dict[str, Any]], cutoff: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for pos in book.get("positions") or []:
        if not isinstance(pos, dict) or pos.get("isExited"):
            continue
        if pos.get("potentialSpam") and not pos.get("userMarkedNonSpam"):
            continue   # pump's own spam flag (airdropped junk); pump hides these too
        opened = _opened_ts(pos.get("openedAt"))
        if cutoff and (not opened or opened + 2 < cutoff):
            continue
        coin = pos.get("coin") if isinstance(pos.get("coin"), dict) else {}
        coin_mint = str(pos.get("coinMint") or "")
        tok = tokens.get(coin_mint) or {}
        pnl = tok.get("tokenPnL") if isinstance(tok.get("tokenPnL"), dict) else {}
        rows.append({
            "coinMint": coin_mint,
            "name": coin.get("name") or tok.get("token_name") or "",
            "symbol": coin.get("symbol") or tok.get("token_symbol") or "",
            "openedAt": opened,
            "token_price": _pos_price(pos, tok),
            "amount_held": pos.get("amountHeld"),
            "amount_bought_usd": pos.get("amountBoughtUsd"),
            "cost_basis_usd": _pick(pnl, "cost_basis_usd") if _pick(pnl, "cost_basis_usd") is not None else pos.get("costBasisUsd"),
            "unrealized_usd": _pick(pnl, "unrealized_usd") if _pick(pnl, "unrealized_usd") is not None else pos.get("pnlUsd"),
            "percentage_usd": _pick(pnl, "percentage_usd") if _pick(pnl, "percentage_usd") is not None else pos.get("pnlPercentage"),
            "percentage": _pick(pnl, "percentage") if _pick(pnl, "percentage") is not None else pos.get("pnlPercentage"),
            "realized_pnl_usd": _pick(pnl, "realized_pnl_usd") if _pick(pnl, "realized_pnl_usd") is not None else pos.get("realizedPnlUsd"),
            "positionHold": "OPEN",
            "botStatus": _oh_status.get(coin_mint, ""),
        })
        if _no_buy(pos) and not _caller_bought(str(pos.get("walletAddress") or ""), coin_mint):
            # Tokens only sent to the caller, never bought (dev airdrops a big caller to fake a buy).
            # Listed so you see it; auto-copy skips it.
            rows[-1]["positionHold"] = "TRANSFER"
            rows[-1]["transfer"] = True
        _stamp_tape(rows[-1])
        _apply_live_row(rows[-1], pos)
    rows.sort(key=lambda item: float(item.get("openedAt") or 0), reverse=True)
    return rows


def picked_hold(name: str, cutoff: float = 0) -> Dict[str, Any]:
    """Open coins for one saved Callout username. cutoff hides anything opened before the pick."""
    who = wallet_for_user(name)
    user = who.get("userName") or str(name or "").strip()
    wallet = who.get("walletAddress") or ""
    if not _useful(wallet):
        return {"userName": user, "wallet": "", "ok": False, "error": "no wallet stored", "rows": []}
    book = _caller_book(wallet)
    if not book.get("ok"):
        return {"userName": user, "wallet": wallet, "ok": False, "error": book.get("error") or "checking", "rows": []}
    rows = _with_stream(wallet, _hold_rows(book, _cached_tokens(wallet), 0))
    if cutoff:
        rows = [row for row in rows if float(row.get("openedAt") or 0) >= cutoff]
    return {
        "userName": user,
        "wallet": wallet,
        "ok": True,
        "error": "",
        "rows": rows,
    }


def _anchor_row(pin: Dict[str, Any], who: Dict[str, str], state: str) -> Dict[str, Any]:
    """The pinned coin, from the Callout pin, when the portfolio row is missing."""
    pct = pin.get("percentageUsd")
    if pct in (None, ""):
        pct = pin.get("percentage")
    status = state if state in ("HOLDING", "BUYING") else "HOLDING"
    row = {
        "coinMint": who.get("mint") or pin.get("coinMint") or "",
        "name": who.get("name") or pin.get("coinName") or "",
        "symbol": who.get("symbol") or pin.get("symbol") or "",
        "openedAt": pin.get("openedAt") or 0,
        "token_price": pin.get("tokenPrice"),
        "cost_basis_usd": pin.get("costBasisUsd"),
        "unrealized_usd": pin.get("unrealizedUsd"),
        "percentage_usd": pct,
        "percentage": pct,
        "realized_pnl_usd": pin.get("realizedPnlUsd"),
        "positionHold": "OPEN",
        "botStatus": status,
        "m5": None if pin.get("m5_null") else pin.get("m5"),
    }
    _stamp_tape(row)
    return row


def _book_cached(wallet: str) -> Dict[str, Any]:
    """Last portfolio read. Never touches the network. A miss is 'checking', not a closed caller."""
    wallet = (wallet or "").strip()
    with _hold_lock:
        hit = _hold_cache.get(wallet)
    if isinstance(hit, dict) and hit.get("ok"):
        return hit
    return {"ok": False, "error": (hit or {}).get("error") or "checking", "positions": []}


def _tokens_cached(wallet: str) -> Dict[str, Dict[str, Any]]:
    """Last balance rows only. The live list must not wait on another request."""
    hit = _token_cache.get((wallet or "").strip()) or {}
    tokens = hit.get("tokens") if isinstance(hit, dict) else None
    return tokens if isinstance(tokens, dict) else {}


_tokens_hot = _tokens_cached


_oh_refresh_started = False


def _openholds_refresh_loop() -> None:
    """Warm books and 5m tapes off the page request. The page itself must not wait on Pump."""
    import time
    from concurrent.futures import ThreadPoolExecutor

    while True:
        started = time.time()
        wallets: List[str] = []
        mints: List[str] = []
        try:
            with _called_lock:
                for row in _called:
                    if row.get("state") not in ("HOLDING", "BUYING"):
                        continue
                    wallet = str(row.get("walletAddress") or "")
                    mint = str(row.get("coinMint") or "")
                    if _useful(wallet):
                        wallets.append(wallet)
                    if mint:
                        mints.append(mint)
            for item in openholds_watch().get("picks") or []:
                who = wallet_for_user(str(item.get("user") or ""))
                wallet = str(who.get("walletAddress") or "")
                if _useful(wallet):
                    wallets.append(wallet)
            seen = []
            for wallet in wallets:
                if wallet not in seen:
                    seen.append(wallet)
            if seen:
                workers = min(8, len(seen))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    list(pool.map(_caller_book, seen))
            if time.time() - float(getattr(_openholds_refresh_loop, "_tape_at", 0) or 0) >= 1.0:
                _openholds_refresh_loop._tape_at = time.time()
                for wallet in seen:
                    book = _book_cached(wallet)
                    for pos in (book.get("positions") or [])[:4]:
                        mint = str(pos.get("coinMint") or "")
                        if mint and mint not in mints:
                            mints.append(mint)
                try:
                    refresh_pin_holds()
                except Exception:
                    log.debug("openholds pin refresh failed", exc_info=True)
                fresh = [mint for mint in mints if mint][:8]
                if fresh:
                    with ThreadPoolExecutor(max_workers=min(4, len(fresh))) as pool:
                        list(pool.map(_cached_tape, fresh))
        except Exception:
            log.debug("openholds refresh failed", exc_info=True)
        time.sleep(1.0)


# ---------- real-time caller stream (PumpPortal trade socket) ----------
_stream_on = False
_stream_names: Dict[str, Dict[str, str]] = {}
_stream_name_busy: Set[str] = set()


def _stream_if_on():
    if not _stream_on:
        return None
    try:
        from src.data.caller_stream import get_stream
        return get_stream()
    except Exception:
        return None


def _stream_targets():
    """Caller wallets on OpenHolds (pins and dropdown picks) and the coins those books list."""
    wallets: Dict[str, str] = {}
    with _called_lock:
        pins = [dict(row) for row in _called if row.get("state") in ("HOLDING", "BUYING")]
    for pin in pins:
        who = _pin_caller(pin)
        if _useful(who.get("wallet")):
            wallets[who["wallet"]] = who.get("user") or ""
    for item in (openholds_watch().get("picks") or []):
        name = str(item.get("user") or "")
        who = wallet_for_user(name)
        wallet = str(who.get("walletAddress") or "")
        if _useful(wallet):
            wallets[wallet] = who.get("userName") or name
    return wallets, _oh_listed_mints()


def _stream_on_event(event: Dict[str, Any]) -> None:
    """A full caller exit marks the coin out at once. The polled book can lag by seconds."""
    mint = str(event.get("mint") or "")
    if not mint:
        return
    if event.get("full_exit"):
        _hold_by_mint[mint] = "out"
    elif event.get("kind") == "buy" and mint not in _stream_names:
        _stream_fetch_name(mint)


def _stream_fetch_name(mint: str) -> None:
    if mint in _stream_name_busy:
        return
    _stream_name_busy.add(mint)

    def _run() -> None:
        try:
            body = _pump_json(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}", timeout=1.5)
            if isinstance(body, dict):
                _stream_names[mint] = {"name": str(body.get("name") or ""), "symbol": str(body.get("symbol") or "")}
        except Exception:
            pass
        finally:
            _stream_name_busy.discard(mint)

    threading.Thread(target=_run, daemon=True).start()


_stream_tried = False


def start_caller_stream() -> None:
    global _stream_on, _stream_tried
    if _stream_on or _stream_tried:
        return
    _stream_tried = True
    try:
        from src.config.runtime import get_config
        if not bool(getattr(get_config(), "copy_stream", True)):
            log.info("CALLER STREAM off in Settings — OpenHolds uses the polled books only")
            return
        from src.data.caller_stream import get_stream
        stream = get_stream()
        stream.set_target_provider(_stream_targets)
        stream.add_listener(_stream_on_event)
        stream.start()
        _stream_on = True
    except Exception:
        log.warning("caller stream did not start", exc_info=True)


def _stream_raw_usd(mint: str, max_age: float = 5.0):
    """Curve price from the last socket trade in USD, before matching it to pump's scale."""
    stream = _stream_if_on()
    if stream is None:
        return None
    hit = stream.price_sol(mint, max_age)
    if not hit:
        return None
    sol = _oh_sol_fast()
    if sol <= 0:
        return None
    return hit[0] * sol, hit[1]


def stream_px_usd(mint: str, max_age: float = 5.0):
    """(USD price, ts) from the last trade on the socket, on pump's scale, or None."""
    return _stream_raw_usd(mint, max_age)   # last trade's actual swap rate, already pump's price


def _curve_factor(mint: str):
    """Scale for the 0.2s curve read: learned from trades on the socket, else pump's own price."""
    stream = _stream_if_on()
    if stream is not None:
        try:
            factor = stream.curve_scale(mint)
        except Exception:
            factor = None
        if factor is not None:
            return factor
    return _px_factor(mint)


# Some coins (custom curves) read off by a fixed factor from the price pump shows and
# sells at. Per coin, compare once in the background with pump's own market cap and
# scale the live price by that ratio. Standard coins land on 1.0.
_px_cal: Dict[str, tuple] = {}
_px_cal_busy: Set[str] = set()
_px_cal_seen: Dict[str, float] = {}    # last unconfirmed scale per coin
_px_cal_retry: Dict[str, float] = {}   # when an unconfirmed scale was seen
_PX_CAL_NOISE = 0.25     # within 25% is timing between the two reads, not a scale


def _raw_live_px(mint: str):
    """The 0.2s curve read, unscaled. (price, ts)."""
    with _oh_px_lock:
        price = _oh_px.get(mint)
        at = float(_oh_px_at.get(mint) or 0)
    return price, at


def _px_calibrate(mint: str) -> None:
    import time
    try:
        coin = _pump_json(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}")
        if not isinstance(coin, dict):
            return
        cap = float(coin.get("usd_market_cap") or 0)
        supply = float(coin.get("total_supply") or 0) / 1e6
        raw, _at = _raw_live_px(mint)
        if cap <= 0 or supply <= 0 or not raw or raw <= 0:
            return
        factor = (cap / supply) / raw
        # A pool read has no curve-timing noise, so a small gap there is real (newer PumpSwap
        # pools price above their raw reserves). Curve reads keep the wide noise band.
        with _oh_px_lock:
            on_pool = _oh_px_src.get(mint) == "pool"
        noise = 0.02 if on_pool else _PX_CAL_NOISE
        if abs(factor - 1.0) <= noise:
            factor = 1.0
            _px_cal_seen.pop(mint, None)
        else:
            # A moving coin can look scaled for one read. Apply a scale only when two reads agree.
            prev = _px_cal_seen.get(mint)
            _px_cal_seen[mint] = factor
            if prev is None or abs(prev - factor) / factor > 0.05:
                _px_cal_retry[mint] = time.time()
                return
            if mint not in _px_cal or abs(_px_cal[mint][0] - factor) / factor > 0.05:
                log.info("PRICE SCALE %s live x%.4f to match pump (2 reads agree)", mint[:8], factor)
        _px_cal[mint] = (factor, time.time())
    except Exception:
        log.debug("price calibrate failed %s", mint[:8], exc_info=True)
    finally:
        _px_cal_busy.discard(mint)


def _px_factor(mint: str):
    """Scale from the live curve price to pump's price. None until the first check lands."""
    import time
    now = time.time()
    hit = _px_cal.get(mint)
    every = 120.0 if hit and hit[0] == 1.0 else 30.0
    retry = _px_cal_retry.get(mint)
    due = hit is None or now - hit[1] > every
    if retry is not None:
        due = now - retry > 3.0     # second read 3s later to confirm or reject the scale
    if due and mint not in _px_cal_busy:
        _px_cal_retry.pop(mint, None)
        _px_cal_busy.add(mint)
        threading.Thread(target=_px_calibrate, args=(mint,), name="px-cal", daemon=True).start()
    if hit is None or now - hit[1] > 600:
        return None
    return hit[0]


def stream_px_sol(mint: str, max_age: float = 5.0):
    stream = _stream_if_on()
    if stream is None:
        return None
    return stream.price_sol(mint, max_age)


def _stream_hold(wallet: str, mint: str, polled: str) -> str:
    """The socket wins over the polled book: it sees the sale and the buy first."""
    stream = _stream_if_on()
    if stream is None or not wallet or not mint:
        return polled
    if stream.exited(wallet, mint):
        return "out"
    if stream.holding(wallet, mint):
        return "live"
    return polled


def _stream_rows(wallet: str, have: Set[str]) -> List[Dict[str, Any]]:
    """Caller opens the socket saw that the portfolio API has not listed yet."""
    import time

    stream = _stream_if_on()
    if stream is None or not _useful(wallet):
        return []
    sol_usd = _oh_sol_fast()
    rows: List[Dict[str, Any]] = []
    for item in stream.opens_for(wallet, since=time.time() - 3600):
        mint = str(item.get("mint") or "")
        if not mint or mint in have:
            continue
        bought = float(item.get("tokens") or 0)
        held = float(item.get("balance") or bought or 0)
        cost_sol = float(item.get("sol") or 0)
        if held <= 0 or cost_sol <= 0:
            continue
        hit = stream.price_sol(mint, 3600)
        price_sol = hit[0] if hit else (cost_sol / bought if bought else 0.0)
        cost_usd = cost_sol * sol_usd if sol_usd > 0 else None
        price_usd = price_sol * sol_usd if sol_usd > 0 else None
        pnl = pct = None
        if cost_usd and price_usd:
            pnl = held * price_usd - cost_usd
            pct = pnl / cost_usd * 100.0
        names = _stream_names.get(mint) or {}
        row = {
            "coinMint": mint,
            "name": names.get("name") or "",
            "symbol": names.get("symbol") or "",
            "openedAt": float(item.get("first_ts") or 0),
            "token_price": price_usd,
            "amount_held": held,
            "amount_bought_usd": cost_usd,
            "cost_basis_usd": cost_usd,
            "unrealized_usd": pnl,
            "percentage_usd": pct,
            "percentage": pct,
            "realized_pnl_usd": 0.0,
            "positionHold": "OPEN",
            "botStatus": _oh_status.get(mint, ""),
            "live": "stream",
        }
        _stamp_tape(row)
        _apply_live_row(row, {})
        rows.append(row)
    return rows


def _with_stream(wallet: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop coins the caller already sold on the socket, add opens the API has not caught yet."""
    stream = _stream_if_on()
    if stream is None or not _useful(wallet):
        return rows
    kept = [
        row for row in rows
        if not stream.exited(wallet, str(row.get("coinMint") or ""), float(row.get("openedAt") or 0) - 2)
    ]
    have = {str(row.get("coinMint") or "") for row in kept}
    extra = _stream_rows(wallet, have)
    if extra:
        kept = extra + kept
        kept.sort(key=lambda item: float(item.get("openedAt") or 0), reverse=True)
    return kept


def _start_openholds_refresh() -> None:
    global _oh_refresh_started
    if _oh_refresh_started:
        return
    _oh_refresh_started = True
    start_caller_stream()
    threading.Thread(target=_openholds_refresh_loop, name="openholds-refresh", daemon=True).start()
    threading.Thread(target=_oh_price_loop, name="openholds-price", daemon=True).start()
    threading.Thread(target=_oh_publish_loop, name="openholds-publish", daemon=True).start()
    threading.Thread(target=_jup_loop, name="openholds-jupiter", daemon=True).start()


def open_holds_snapshot() -> Dict[str, Any]:
    """Pinned Callout holders stay listed while the pin is up.

    The page never waits on Pump. A background loop fills the book.
    The pinned coin stays while that caller still holds it. Once the caller
    has sold it, that coin leaves and the user heading stays until the pin is gone.
    Other opens from the last hour still show. Dropdown callers are separate.
    """
    import time

    _start_openholds_refresh()
    now = time.time()
    hour = 60 * 60
    with _called_lock:
        pins = [dict(row) for row in _called if row.get("state") in ("HOLDING", "BUYING")]
    built: List[Dict[str, Any]] = []
    for pin in pins:
        who = _pin_caller(pin)
        mint = who["mint"]
        wallet = who["wallet"]
        user_name = who["user"]
        state = str(pin.get("state") or "HOLDING")
        hold = hold_for_mint(mint) if mint else ""
        book = _book_cached(wallet) if _useful(wallet) else {"ok": False, "error": "no wallet", "positions": []}
        kept: List[Dict[str, Any]] = []
        book_ok = bool(book.get("ok"))
        if book_ok:
            rows = _with_stream(wallet, _hold_rows(book, _tokens_cached(wallet), 0))
            for row in rows:
                coin_mint = str(row.get("coinMint") or "")
                is_anchor = bool(mint) and coin_mint == mint
                if is_anchor and hold == "out":
                    continue
                opened = float(row.get("openedAt") or 0)
                age = (now - opened) if opened else 0
                status = str(row.get("botStatus") or "")
                if is_anchor and status not in ("HOLDING", "BUYING"):
                    row = dict(row)
                    row["botStatus"] = state if state in ("HOLDING", "BUYING") else "HOLDING"
                    status = row["botStatus"]
                if is_anchor or status in ("HOLDING", "BUYING") or (opened and age <= hour):
                    kept.append(row)
        # No book yet is not a sale. Keep the coin the Callouts page is holding.
        if hold != "out" and mint and not any(str(row.get("coinMint") or "") == mint for row in kept):
            kept.insert(0, _anchor_row(pin, who, state))
        if hold == "out" and not kept:
            error = "Caller closed this token. This user stays until the Callout pin is gone."
        elif not kept:
            error = book.get("error") or "Caller still pinned. No other open in the last hour."
        else:
            error = ""
        built.append({
            "userName": user_name,
            "wallet": wallet,
            "anchor": mint,
            "hold": hold,
            "pinned": True,
            "ok": True,
            "error": error,
            "totalValueUsd": pin.get("totalValueUsd"),
            "rows": kept,
        })
    groups: List[Dict[str, Any]] = []
    index: Dict[str, Dict[str, Any]] = {}
    for group in built:
        key = str(group.get("userName") or group.get("wallet") or "").casefold()
        if not key:
            groups.append(group)
            continue
        dest = index.get(key)
        if dest is None:
            index[key] = group
            groups.append(group)
            continue
        have = {str(row.get("coinMint") or "") for row in (dest.get("rows") or [])}
        for row in group.get("rows") or []:
            coin_mint = str(row.get("coinMint") or "")
            if coin_mint and coin_mint in have:
                continue
            dest.setdefault("rows", []).append(row)
            if coin_mint:
                have.add(coin_mint)
        if not dest.get("anchor"):
            dest["anchor"] = group.get("anchor") or ""
        dest["pinned"] = True
        if group.get("hold") == "live":
            dest["hold"] = "live"
        if dest.get("rows"):
            dest["error"] = ""
            dest["ok"] = True
    watch = openholds_watch()
    payload: Dict[str, Any] = {
        "ok": True,
        "groups": groups,
        "callers": [],
        "watch": watch,
        "picked": [],
    }
    for item in watch.get("picks") or []:
        name = str(item.get("user") or "").strip()
        if not name:
            continue
        book = _picked_cached(name, 0)
        if item.get("all"):
            view = dict(book)
            view["rows"] = list(book.get("older") or [])
            payload["picked"].append(view)
        else:
            payload["picked"].append(book)
    return payload


def _picked_cached(name: str, cutoff: float = 0) -> Dict[str, Any]:
    """Dropdown caller from the last book read. Does not block the page."""
    import time

    who = wallet_for_user(name)
    user = who.get("userName") or str(name or "").strip()
    wallet = who.get("walletAddress") or ""
    if not _useful(wallet):
        return {"userName": user, "wallet": "", "ok": False, "error": "no wallet stored", "rows": []}
    book = _book_cached(wallet)
    if not book.get("ok"):
        return {"userName": user, "wallet": wallet, "ok": False, "error": book.get("error") or "checking", "rows": []}
    rows = _with_stream(wallet, _hold_rows(book, _tokens_cached(wallet), 0))
    now = time.time()
    hour = 60 * 60
    recent: List[Dict[str, Any]] = []
    older: List[Dict[str, Any]] = []
    live_total = 0.0
    have_live = False
    for row in rows:
        opened = float(row.get("openedAt") or 0)
        if opened > 0 and now - opened <= hour:
            recent.append(row)
        else:
            older.append(row)
        try:
            held = float(row.get("amount_held") or 0)
            px = float(row.get("token_price") or 0)
        except (TypeError, ValueError):
            held = px = 0.0
        if held > 0 and px > 0:
            live_total += held * px
            have_live = True
    # The table they see is the last hour. After that the coin is only in Hide existing.
    # recent stays on the payload so auto-trade still sees a new open while that other list is open.
    return {
        "userName": user,
        "wallet": wallet,
        "ok": True,
        "error": "",
        "totalValueUsd": live_total if have_live else book.get("totalValueUsd"),
        "rows": recent,
        "recent": recent,
        "older": older,
    }


_oh_seen: Dict[str, Set[str]] = {}
_oh_status: Dict[str, str] = {}
_oh_orders: List[Dict[str, Any]] = []
_oh_order_lock = threading.Lock()


def set_openhold_status(mint: str, status: str) -> None:
    mint = str(mint or "")
    if not mint:
        return
    if status:
        _oh_status[mint] = status
    else:
        _oh_status.pop(mint, None)


def note_openhold_seen(user: str, mint: str) -> None:
    key = str(user or "").casefold()
    mint = str(mint or "")
    if key and mint:
        _oh_seen.setdefault(key, set()).add(mint)


def openhold_status(mint: str) -> str:
    return _oh_status.get(str(mint or ""), "")


_oh_room: int | None = None


def set_openhold_room(room: int) -> None:
    global _oh_room
    _oh_room = max(0, int(room))


def openhold_room_left() -> int | None:
    """Free Callout slots after buys already waiting. None until the loop has counted once."""
    if _oh_room is None:
        return None
    with _oh_order_lock:
        waiting = sum(1 for order in _oh_orders if order.get("action") == "buy")
    return max(0, _oh_room - waiting)


def queue_openhold_order(action: str, mint: str, symbol: str = "", name: str = "", user: str = "", wallet: str = "") -> None:
    mint = str(mint or "").strip()
    action = str(action or "").strip().lower()
    if action not in ("buy", "sell") or not mint:
        return
    with _oh_order_lock:
        _oh_orders.append({
            "action": action,
            "coinMint": mint,
            "symbol": symbol or "",
            "name": name or "",
            "userName": user or "",
            "wallet": wallet or "",
        })


def pop_openhold_orders() -> List[Dict[str, Any]]:
    with _oh_order_lock:
        items = list(_oh_orders)
        _oh_orders.clear()
    return items


def openhold_new_coins() -> List[Dict[str, Any]]:
    """Coins that just appeared on a live OpenHolds user and are inside the auto-open window.

    The window comes from Settings. Default is 1 minute. A clicked Buy does not use it.
    The first look at a user only remembers what is already older than that window.
    """
    import time
    try:
        from src.config.runtime import openhold_auto_seconds
        window = openhold_auto_seconds()
    except Exception:
        window = 60.0
    snap = open_holds_snapshot()
    books: List[Dict[str, Any]] = []
    for group in snap.get("groups") or []:
        if group.get("ok"):
            books.append(group)
    picked_raw = snap.get("picked") or []
    picked_list = [picked_raw] if isinstance(picked_raw, dict) else list(picked_raw)
    for picked in picked_list:
        if not isinstance(picked, dict) or not picked.get("ok"):
            continue
        name = str(picked.get("userName") or "").casefold()
        hit = next((group for group in books if str(group.get("userName") or "").casefold() == name), None)
        if hit is None:
            books.append(picked)
        else:
            have = {row.get("coinMint") for row in (hit.get("rows") or [])}
            extra = [row for row in (picked.get("rows") or []) if row.get("coinMint") not in have]
            if extra:
                merged = dict(hit)
                merged["rows"] = list(hit.get("rows") or []) + extra
                books = [merged if group is hit else group for group in books]
    live: Set[str] = set()
    fresh: List[Dict[str, Any]] = []
    now = time.time()
    for group in snap.get("groups") or []:
        if group.get("ok"):
            continue
        key = str(group.get("userName") or group.get("wallet") or "").casefold()
        if key and key in _oh_seen:
            live.add(key)
    for group in books:
        key = str(group.get("userName") or group.get("wallet") or "").casefold()
        if not key:
            continue
        live.add(key)
        source = group.get("recent") if isinstance(group.get("recent"), list) else (group.get("rows") or [])
        current = []
        for row in source:
            mint = str(row.get("coinMint") or "")
            if mint:
                current.append((mint, row))
        if key not in _oh_seen:
            # Only coins already older than the auto window are "already there".
            # A coin still inside that window is a new entry, even on the first look.
            old = set()
            for mint, row in current:
                try:
                    opened = float(row.get("openedAt") or 0)
                except (TypeError, ValueError):
                    opened = 0
                if opened and now - opened > window:
                    old.add(mint)
            _oh_seen[key] = old
        seen = _oh_seen[key]
        for mint, row in current:
            if mint in seen:
                continue
            try:
                opened = float(row.get("openedAt") or 0)
            except (TypeError, ValueError):
                opened = 0
            if not opened or now - opened > window:
                seen.add(mint)
                continue
            item = dict(row)
            item["auto"] = True
            item["userName"] = group.get("userName") or ""
            item["wallet"] = group.get("wallet") or ""
            fresh.append(item)
    fresh.sort(key=lambda row: -float(row.get("openedAt") or 0))
    return _openhold_allowed(fresh)


def caller_identity(mint: str) -> Dict[str, str]:
    """Caller wallet, name, and symbol for this mint. The live scan is included, not only the pin."""
    found = {"wallet": "", "name": "", "symbol": "", "user": ""}
    if not mint:
        return found
    with _called_lock:
        rows = [row for row in _called if row.get("coinMint") == mint]
    sources = list(rows)
    snap = _feed_callers.get(mint) or {}
    if snap:
        sources.append(snap)
    with _callout_lock:
        cached = list(_callout_cache.get("rows") or [])
        by_wallet = list(_callout_by_wallet.values())
    for row in cached:
        if str(row.get("coinMint") or "") == mint:
            sources.append(row)
    for row in by_wallet:
        if str(row.get("coinMint") or "") == mint:
            sources.append(row)
    for row in sources:
        if not isinstance(row, dict):
            continue
        if not found["wallet"] and _useful(row.get("walletAddress")):
            found["wallet"] = str(row.get("walletAddress"))
        if not found["name"] and _useful(row.get("coinName")):
            found["name"] = str(row.get("coinName"))
        if not found["symbol"] and _useful(row.get("symbol")):
            found["symbol"] = str(row.get("symbol"))
        if not found["user"] and _useful(row.get("userName")):
            found["user"] = str(row.get("userName"))
    return found


def _note_feed_caller(coin: Dict[str, Any], pos: Dict[str, Any]) -> None:
    """The home-feed coin, not the wallet's newest open. That is who is on this mint."""
    mint = str(coin.get("coinMint") or "")
    wallet = str(pos.get("walletAddress") or "")
    if not mint or not wallet:
        return
    snap = {
        "userName": pos.get("userName") or "",
        "walletAddress": wallet,
        "coinName": coin.get("coinName") or "",
        "symbol": coin.get("symbol") or "",
        "tokenPrice": pos.get("tokenPriceUsd"),
        "costBasisUsd": pos.get("costBasisUsd"),
        "unrealizedUsd": pos.get("pnlUsd"),
        "percentageUsd": pos.get("pnlPercentage"),
        "percentage": pos.get("pnlPercentage"),
        "realizedPnlUsd": pos.get("realizedPnlUsd"),
    }
    _feed_callers[mint] = snap
    if len(_feed_callers) > 400:
        for key in list(_feed_callers)[:80]:
            _feed_callers.pop(key, None)
    _adopt_feed_caller(mint)


def _adopt_feed_caller(mint: str) -> None:
    """Replace a bot stub with the caller still on the home feed. Do not replace a real caller."""
    snap = _feed_callers.get(mint) or {}
    if not snap:
        return
    with _called_lock:
        item = next((row for row in _called if row.get("coinMint") == mint), None)
        if not item:
            return
        changed = False
        if not _useful(item.get("userName")) and _useful(snap.get("userName")):
            item["userName"] = snap["userName"]
            changed = True
        if not _useful(item.get("walletAddress")) and _useful(snap.get("walletAddress")):
            item["walletAddress"] = snap["walletAddress"]
            changed = True
        if not _useful(item.get("coinName")) and _useful(snap.get("coinName")):
            item["coinName"] = snap["coinName"]
            changed = True
        if not _useful(item.get("symbol")) and _useful(snap.get("symbol")):
            item["symbol"] = snap["symbol"]
            changed = True
        # Money stays blank on a stub. Fill it once; the live hold refresh keeps it current.
        if item.get("costBasisUsd") in (None, "") and snap.get("costBasisUsd") not in (None, ""):
            item["costBasisUsd"] = snap.get("costBasisUsd")
            item["unrealizedUsd"] = snap.get("unrealizedUsd")
            item["percentageUsd"] = snap.get("percentageUsd")
            item["percentage"] = snap.get("percentage")
            item["realizedPnlUsd"] = snap.get("realizedPnlUsd")
            if snap.get("tokenPrice") not in (None, ""):
                item["tokenPrice"] = snap.get("tokenPrice")
            changed = True
    if changed:
        _save_called(force=True)


def remember_callout(row: Dict[str, Any], state: str) -> None:
    """Keep a Callout trade at the top of the CallOuts page after the live feed moves on."""
    mint = str((row or {}).get("coinMint") or "")
    if not mint:
        return
    kept = dict(row or {})
    kept["coinMint"] = mint
    kept["state"] = state
    with _called_lock:
        prev = next((item for item in _called if item.get("coinMint") == mint), None)
        if prev and prev.get("state") == state:
            for key, value in kept.items():
                if key == "state":
                    continue
                if key in ("m5", "h1", "m1") and value not in (None, ""):
                    prev[key] = value
                    continue
                if key in ("userName", "walletAddress", "coinName", "symbol"):
                    if _useful(value) and not _useful(prev.get(key)):
                        prev[key] = value
                    continue
                if _useful(value) and not _useful(prev.get(key)):
                    prev[key] = value
            found = prev
        else:
            if prev:
                merged = dict(prev)
                for key, value in kept.items():
                    if key in ("userName", "walletAddress", "coinName", "symbol") and not _useful(value):
                        continue
                    if value not in (None, ""):
                        merged[key] = value
                merged["state"] = state
                kept = merged
            _called[:] = [item for item in _called if item.get("coinMint") != mint]
            _called.insert(0, kept)
            del _called[40:]
            found = kept
    if not _useful(found.get("userName")) or found.get("costBasisUsd") in (None, ""):
        _adopt_feed_caller(mint)
    _save_called(force=state in ("BUYING", "HOLDING", "CLOSED", "MISSED"))


def mark_callout(mint: str, state: str) -> None:
    mint = str(mint or "")
    if not mint:
        return
    with _called_lock:
        for item in _called:
            if item.get("coinMint") == mint:
                item["state"] = state
                break
        else:
            snap = _feed_callers.get(mint) or {}
            _called.insert(0, {
                "coinMint": mint,
                "state": state,
                "userName": snap.get("userName") or "",
                "walletAddress": snap.get("walletAddress") or "",
                "coinName": snap.get("coinName") or "",
                "symbol": snap.get("symbol") or "",
                "call": "CLOSE" if state == "CLOSED" else "OPEN",
                "callReason": "This Callout trade is closed." if state == "CLOSED" else "",
            })
            del _called[40:]
    _save_called(force=True)


def sync_callout_pins(positions) -> None:
    """Only a Callout buy is pinned on the Callout page.

    OpenCopy is bought from OpenHolds. That page is its own list. An OpenCopy
    hold must not show as HOLDING or stay pinned on Callout.
    """
    open_mints: Set[str] = set()
    copy_mints: Set[str] = set()
    held = []
    for pos in positions or []:
        if getattr(pos, "status", "") != "OPEN":
            continue
        mint = str(getattr(pos, "mint", "") or "")
        if not mint:
            continue
        kind = str(getattr(pos, "strategy", "") or "")
        src = str(getattr(pos, "source", "") or "")
        if kind == "opencopy" or src == "opencopy":
            copy_mints.add(mint)
            set_openhold_status(mint, "HOLDING")
            continue
        if kind != "callout" and src != "callout":
            continue
        open_mints.add(mint)
        held.append(pos)
        remember_callout({
            "coinMint": mint,
            "symbol": getattr(pos, "symbol", "") or "",
            "coinName": getattr(pos, "name", "") or "",
            "userName": getattr(pos, "callout_user", "") or "",
            "walletAddress": getattr(pos, "callout_wallet", "") or "",
            "call": "OPEN",
            "callReason": "This is the Callout trade the bot is holding.",
            "openedAt": float(getattr(pos, "entry_time", 0) or 0),
        }, "HOLDING")
        set_openhold_status(mint, "HOLDING")
    with _called_lock:
        if copy_mints:
            _called[:] = [
                item for item in _called
                if str(item.get("coinMint") or "") not in copy_mints
                or item.get("state") not in ("HOLDING", "BUYING")
            ]
        pins = {str(item.get("coinMint") or ""): item for item in _called}
        stale = [
            mint for mint, item in pins.items()
            if item.get("state") in ("HOLDING", "BUYING") and mint and mint not in open_mints and mint not in copy_mints
        ]
    for mint in stale:
        mark_callout(mint, "CLOSED")
        set_openhold_status(mint, "")
    for mint in list(_oh_status):
        if mint not in open_mints and mint not in copy_mints:
            _oh_status.pop(mint, None)
    if copy_mints:
        _save_called(force=True)
    for pos in held:
        item = pins.get(str(getattr(pos, "mint", "") or ""))
        if not item:
            continue
        if not getattr(pos, "callout_user", "") and _useful(item.get("userName")):
            pos.callout_user = str(item.get("userName") or "")
        if not getattr(pos, "callout_wallet", "") and _useful(item.get("walletAddress")):
            pos.callout_wallet = str(item.get("walletAddress") or "")


def _overlay_pin(shown: Dict[str, Any], item: Dict[str, Any]) -> None:
    mint = shown.get("coinMint") or item.get("coinMint") or ""
    snap = _feed_callers.get(mint) or {}
    if not _useful(shown.get("userName")):
        if _useful(item.get("userName")):
            shown["userName"] = item.get("userName")
        elif _useful(snap.get("userName")):
            shown["userName"] = snap.get("userName")
    if not _useful(shown.get("walletAddress")):
        if _useful(item.get("walletAddress")):
            shown["walletAddress"] = item.get("walletAddress")
        elif _useful(snap.get("walletAddress")):
            shown["walletAddress"] = snap.get("walletAddress")
    for key in ("coinName", "symbol"):
        if not _useful(shown.get(key)):
            if _useful(item.get(key)):
                shown[key] = item.get(key)
            elif _useful(snap.get(key)):
                shown[key] = snap.get(key)
    if shown.get("costBasisUsd") in (None, "") and item.get("costBasisUsd") not in (None, ""):
        for key in ("costBasisUsd", "costBasisSol", "unrealizedUsd", "unrealizedSol", "percentageUsd", "percentageSol", "percentage", "realizedPnlUsd", "realizedPnlSol", "totalValueUsd"):
            if item.get(key) not in (None, ""):
                shown[key] = item.get(key)
    elif shown.get("costBasisUsd") in (None, "") and snap.get("costBasisUsd") not in (None, ""):
        for key in ("costBasisUsd", "unrealizedUsd", "percentageUsd", "percentage", "realizedPnlUsd"):
            if snap.get(key) not in (None, ""):
                shown[key] = snap.get(key)
    tape = _callout_tapes.get(mint) or {}
    if tape.get("m5_null"):
        shown["m5"] = None
        shown["m5_null"] = True
    elif tape.get("m5") is not None:
        shown["m5"] = tape.get("m5")
        shown["m5_null"] = False
    elif item.get("m5") not in (None, ""):
        shown["m5"] = item.get("m5")
    if tape.get("h1") is not None:
        shown["h1"] = tape.get("h1")
    elif item.get("h1") not in (None, ""):
        shown["h1"] = item.get("h1")
    if tape.get("m1") is not None:
        shown["m1"] = tape.get("m1")
    if item.get("priceLive") and item.get("tokenPrice") not in (None, ""):
        shown["tokenPrice"] = item.get("tokenPrice")


def _callout_page_rows() -> tuple:
    """Live CallOuts, with coins this bot bought kept at the front."""
    _start_pin_live()
    rows, err = _callout_rows()
    by_mint: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        mint = row.get("coinMint")
        if mint and mint not in by_mint:
            by_mint[mint] = row
    with _called_lock:
        items = list(_called)
    pinned: List[Dict[str, Any]] = []
    for item in items:
        if (item.get("state") or "") not in ("HOLDING", "BUYING"):
            continue
        mint = item.get("coinMint")
        live = by_mint.get(mint)
        if live:
            shown = dict(live)
            shown["state"] = item.get("state") or ""
        else:
            shown = dict(item)
        _overlay_pin(shown, item)
        if _useful(shown.get("walletAddress")) and not _useful(item.get("walletAddress")):
            item["walletAddress"] = shown.get("walletAddress")
        if _useful(shown.get("userName")) and not _useful(item.get("userName")):
            item["userName"] = shown.get("userName")
        if _useful(shown.get("coinName")) and not _useful(item.get("coinName")):
            item["coinName"] = shown.get("coinName")
        if _useful(shown.get("symbol")) and not _useful(item.get("symbol")):
            item["symbol"] = shown.get("symbol")
        if shown.get("state") in ("HOLDING", "BUYING"):
            shown["call"] = "OPEN"
            if item.get("callReason"):
                shown["callReason"] = item.get("callReason")
        pinned.append(shown)
    seen = {item.get("coinMint") for item in pinned}
    page = pinned + [row for row in rows if row.get("coinMint") not in seen]
    remember_callers(page)
    return page, err


def _apply_hold(item: Dict[str, Any], hold: Dict[str, Any]) -> None:
    pnl = hold.get("tokenPnL") if isinstance(hold.get("tokenPnL"), dict) else {}
    if pnl.get("cost_basis_usd") is not None:
        item["costBasisUsd"] = pnl.get("cost_basis_usd")
    if pnl.get("cost_basis_sol") is not None:
        item["costBasisSol"] = pnl.get("cost_basis_sol")
    if pnl.get("unrealized_usd") is not None:
        item["unrealizedUsd"] = pnl.get("unrealized_usd")
    if pnl.get("unrealized_sol") is not None:
        item["unrealizedSol"] = pnl.get("unrealized_sol")
    if pnl.get("percentage_usd") is not None:
        item["percentageUsd"] = pnl.get("percentage_usd")
        item["percentage"] = pnl.get("percentage_usd")
    elif pnl.get("percentage") is not None:
        item["percentageUsd"] = pnl.get("percentage")
        item["percentage"] = pnl.get("percentage")
    if pnl.get("percentage_sol") is not None:
        item["percentageSol"] = pnl.get("percentage_sol")
    if pnl.get("realized_pnl_usd") is not None:
        item["realizedPnlUsd"] = pnl.get("realized_pnl_usd")
    if pnl.get("realized_pnl_sol") is not None:
        item["realizedPnlSol"] = pnl.get("realized_pnl_sol")


def _pin_live_loop() -> None:
    """5m and 1h on a pinned Callout trade. The page polls every second, so this does too."""
    import time

    hold_at: Dict[str, float] = {}
    wallet_at: Dict[str, float] = {}
    while True:
        with _called_lock:
            pins = [dict(row) for row in _called if row.get("state") in ("HOLDING", "BUYING")][:4]
        if not pins:
            time.sleep(1)
            continue
        for pin in pins:
            mint = str(pin.get("coinMint") or "")
            if not mint:
                continue
            try:
                tape = _read_tape(mint)
            except Exception:
                tape = {}
            if tape:
                _callout_tapes[mint] = tape
            price = None
            try:
                coin = _pump_json(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}")
                if isinstance(coin, dict) and coin.get("price_usd") not in (None, ""):
                    price = float(coin.get("price_usd") or 0)
            except Exception:
                price = None
            now = time.time()
            wallet = str(pin.get("walletAddress") or "")
            if not _useful(wallet):
                wallet = str((_feed_callers.get(mint) or {}).get("walletAddress") or "")
            hold = None
            total = None
            if _useful(wallet) and now - hold_at.get(mint, 0) >= 3:
                hold_at[mint] = now
                try:
                    hold = _token_hold(wallet, mint)
                except Exception:
                    hold = None
            if _useful(wallet) and now - wallet_at.get(wallet, 0) >= 8:
                wallet_at[wallet] = now
                try:
                    summary = _pump_json(
                        "https://frontend-api-v3.pump.fun/portfolio-summary"
                        f"?user={wallet}&period=1d&holdings=1"
                    )
                    if isinstance(summary, dict) and summary.get("totalValueUsd") is not None:
                        total = float(summary["totalValueUsd"])
                except Exception:
                    total = None
            with _called_lock:
                item = next((row for row in _called if row.get("coinMint") == mint), None)
                if not item:
                    continue
                if tape.get("m5_null"):
                    item["m5"] = None
                    item["m5_null"] = True
                elif tape.get("m5") is not None:
                    item["m5"] = tape.get("m5")
                    item["m5_null"] = False
                if tape.get("h1") is not None:
                    item["h1"] = tape.get("h1")
                if tape.get("m1") is not None:
                    item["m1"] = tape.get("m1")
                if price and price > 0:
                    item["tokenPrice"] = price
                    item["priceLive"] = True
                if isinstance(hold, dict):
                    _apply_hold(item, hold)
                if total is not None:
                    item["totalValueUsd"] = total
            if not _useful(pin.get("userName")) or not _useful(pin.get("walletAddress")):
                _adopt_feed_caller(mint)
        time.sleep(1)


def _start_pin_live() -> None:
    global _pin_live_started
    if _pin_live_started:
        return
    _pin_live_started = True
    threading.Thread(target=_pin_live_loop, name="callout-pin-live", daemon=True).start()


def _pump_tape(mint: str):
    """Live 5m, 1h, and buy share. Not the numbers saved when the coin was predicted."""
    url = (
        "https://swap-api.pump.fun/v1/coins/"
        f"{mint}/market-activity?program=pump&chainId=solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
    )
    try:
        act = _pump_json(url, timeout=4)
    except Exception:
        return None
    if not isinstance(act, dict):
        return None
    block5 = act.get("5m") if isinstance(act.get("5m"), dict) else {}
    block1 = act.get("1h") if isinstance(act.get("1h"), dict) else {}
    m5 = h1 = buy_share = None
    try:
        if block5.get("priceChangePercent") is not None:
            m5 = float(block5["priceChangePercent"])
        if block1.get("priceChangePercent") is not None:
            h1 = float(block1["priceChangePercent"])
        volume = float(block5.get("volumeUSD") or 0)
        buy_vol = float(block5.get("buyVolumeUSD") or 0)
        if volume > 0:
            buy_share = round(buy_vol / volume * 100.0, 1)
    except (TypeError, ValueError):
        return None
    return m5, h1, buy_share


def _predict_rows() -> tuple:
    """Live tape from the cache. The page never waits on Pump."""
    import time

    rows, err = _predict_feed()
    by_mint = {row.get("coinMint"): row for row in rows}
    tapes: Dict[str, Any] = {}
    mints = [item.get("coinMint") for item in _predicted if item.get("coinMint")][:12]
    now = time.time()
    for mint in mints:
        hit = _callout_tapes.get(mint)
        if isinstance(hit, dict):
            tapes[mint] = (hit.get("m5"), hit.get("h1"), hit.get("buyShare"))
    if mints and not getattr(_predict_rows, "_busy", False):
        _predict_rows._busy = True

        def _warm() -> None:
            try:
                for mint in mints:
                    hit = _callout_tapes.get(mint)
                    if isinstance(hit, dict) and now - float(hit.get("ts") or 0) < 2:
                        continue
                    got = _pump_tape(mint)
                    if not got:
                        continue
                    m5, h1, share = got
                    _callout_tapes[mint] = {
                        "ts": time.time(),
                        "m5": m5,
                        "h1": h1,
                        "buyShare": share,
                        "m5_null": m5 is None,
                    }
            finally:
                _predict_rows._busy = False

        threading.Thread(target=_warm, name="predict-tape", daemon=True).start()
    pinned: List[Dict[str, Any]] = []
    for item in _predicted:
        mint = item.get("coinMint")
        live = by_mint.get(mint)
        shown = dict(item)
        shown["state"] = item.get("state") or "PREDICTED"
        tape = tapes.get(mint)
        if tape:
            m5, h1, buy_share = tape
            if m5 is not None:
                shown["m5"] = m5
                item["m5"] = m5
            if h1 is not None:
                shown["h1"] = h1
                item["h1"] = h1
            if buy_share is not None:
                shown["buyShare"] = buy_share
                item["buyShare"] = buy_share
        elif live:
            shown["m5"] = live.get("m5")
            shown["h1"] = live.get("h1")
            shown["buyShare"] = live.get("buyShare")
        if live and live.get("reason"):
            shown["reason"] = live.get("reason")
            shown["call"] = live.get("call") or shown.get("call")
        pinned.append(shown)
    seen = {item.get("coinMint") for item in pinned}
    return pinned + [row for row in rows if row.get("coinMint") not in seen], err


STATIC_DIR = Path(__file__).resolve().parent / "static"
DEFAULT_WS_PORT = 8765
DEFAULT_HTTP_PORT = 8080


class EventBus:
    def __init__(self, max_history: int = 150):
        self._clients: Set[WebSocketServerProtocol] = set()
        self._lock = asyncio.Lock()
        self._history: List[Dict[str, Any]] = []
        self._max_history = max_history
        self._stats: Dict[str, Any] = {
            "events": 0,
            "decode_attempts": 0,
            "decode_success": 0,
            "mints_found": 0,
            "buy_signals": 0,
            "skip_signals": 0,
            "downsize_signals": 0,
            "fills": 0,
            "open_positions": 0,
            "closed_trades": 0,
            "win_rate": 0.0,
            "daily_pnl_sol": 0.0,
            "last_slot": None,
            "filter_mode": "strict",
            "trade_mode": "paper",
            "wallet_armed": False,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self._config: Dict[str, Any] = {}
        self._positions: Dict[str, Any] = {}
        self._exec_history: List[Dict[str, Any]] = []
        self._actions: List[Dict[str, Any]] = []
        self._on_config: Optional[Callable[[Dict[str, Any]], Any]] = None
        self._on_restart: Optional[Callable[[], Any]] = None

    def set_config_handler(self, handler: Callable[[Dict[str, Any]], Any]) -> None:
        self._on_config = handler

    def set_restart_handler(self, handler: Callable[[], Any]) -> None:
        self._on_restart = handler

    async def register(self, ws: WebSocketServerProtocol) -> None:
        async with self._lock:
            self._clients.add(ws)
        stats = dict(self._stats)
        try:
            from src.observability.ledger import get_ledger
            stats.update(get_ledger().pnl_board())
            stats.update(get_ledger().session_trade_counts())
            if not getattr(bus, "_ledger", None):
                bus._ledger = get_ledger().recent(250)
                bus._ledger_summary = get_ledger().summary()
        except Exception:
            log.debug("snapshot pnl attach failed", exc_info=True)
        config = dict(self._config)
        if not config.get("exit_style") and config.get("max_position_sol") is None:
            try:
                from src.config.runtime import snapshot_config
                config = snapshot_config()
            except Exception:
                pass
        try:
            from src.config.runtime import bot_stopped
            config["bot_stopped"] = bot_stopped()
        except Exception:
            pass
        try:
            from src.config.file_config import list_user_presets
            config["exit_presets"] = list_user_presets()
        except Exception:
            config.setdefault("exit_presets", [])
        await ws.send(
            json.dumps(
                {
                    "type": "snapshot",
                    "stats": stats,
                    "history": self._history[-80:],
                    "config": config,
                    "positions": self._positions,
                    "exec_history": self._exec_history[-100:],
                    "actions": self._actions[-80:],
                    "ledger": getattr(bus, "_ledger", []) or [],
                    "ledger_summary": getattr(bus, "_ledger_summary", {}) or {},
                    "wallet": getattr(bus, "_wallet", None),
                }
            )
        )

    async def unregister(self, ws: WebSocketServerProtocol) -> None:
        async with self._lock:
            self._clients.discard(ws)

    async def publish(self, event: Dict[str, Any]) -> None:
        event.setdefault("ts", datetime.now(timezone.utc).isoformat())
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history :]
        if "stats" in event:
            self._stats.update(event["stats"])
        if event.get("type") == "config":
            self._config = event.get("config") or self._config
        if event.get("type") == "positions":
            self._positions = event.get("snapshot") or self._positions
        if event.get("type") == "actions_cleared":
            self._actions = []
            self._history = [e for e in self._history if e.get("type") == "actions_cleared"]
        if event.get("type") == "action" and isinstance(event.get("entry"), dict):
            self._actions.append(event["entry"])
            if len(self._actions) > 200:
                self._actions = self._actions[-200:]

        payload = json.dumps(event)
        async with self._lock:
            clients = list(self._clients)
        dead = []
        for ws in clients:
            try:
                await ws.send(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.unregister(ws)

    def update_stats(self, **kwargs: Any) -> None:
        self._stats.update(kwargs)

    @property
    def client_count(self) -> int:
        return len(self._clients)


bus = EventBus()
_wallet_busy: Optional[Callable[[], str]] = None
_http_restart = None
_http_clear = None
_http_run = None
_http_wallet = None
_http_sell = None


def set_wallet_busy_check(fn: Callable[[], str]) -> None:
    global _wallet_busy
    _wallet_busy = fn


def set_http_restart(fn) -> None:
    global _http_restart
    _http_restart = fn


def set_http_clear(fn) -> None:
    global _http_clear
    _http_clear = fn


def set_http_run(fn) -> None:
    global _http_run
    _http_run = fn


def set_http_wallet(fn) -> None:
    global _http_wallet
    _http_wallet = fn


def set_http_sell(fn) -> None:
    global _http_sell
    _http_sell = fn


async def _ws_handler(websocket: WebSocketServerProtocol) -> None:
    await bus.register(websocket)
    log.info("Dashboard client connected (%d total)", bus.client_count)
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            if data.get("type") == "config_update" and isinstance(data.get("config"), dict):
                if bus._on_config:
                    try:
                        result = bus._on_config(data["config"])
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        log.exception("config update handler failed")
                await bus.publish({"type": "config", "config": bus._config or data["config"]})
            elif data.get("type") == "restart":
                if bus._on_restart:
                    try:
                        result = bus._on_restart()
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        log.exception("restart handler failed")
            elif data.get("type") == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await bus.unregister(websocket)
        log.info("Dashboard client disconnected (%d total)", bus.client_count)


async def start_ws_server(host: str = "0.0.0.0", port: int = DEFAULT_WS_PORT) -> None:
    bus._loop = asyncio.get_running_loop()
    async with websockets.serve(_ws_handler, host, port, ping_interval=20, ping_timeout=20):
        log.info("Dashboard WebSocket listening on ws://%s:%d", host, port)
        await asyncio.Future()


def publish_threadsafe(event: Dict[str, Any]) -> None:
    """Queue a dashboard event from a background thread. The paid RPC is not used."""
    loop = getattr(bus, "_loop", None)
    if loop is None or not loop.is_running():
        return
    asyncio.run_coroutine_threadsafe(bus.publish(event), loop)


def start_http_static_server(port: int = DEFAULT_HTTP_PORT) -> ThreadingHTTPServer:
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(STATIC_DIR), **kwargs)
            try:
                self.connection.settimeout(20)
            except Exception:
                pass

        def end_headers(self):
            self.send_header("Cache-Control", "no-store")
            super().end_headers()

        def log_message(self, fmt, *args):
            try:
                if len(args) >= 2 and str(args[1]).startswith("2"):
                    return
                log.debug("HTTP %s", fmt % args)
            except Exception:
                return

        def _json(self, code: int, payload: Dict[str, Any]) -> None:
            raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self._raw(code, raw)

        def _raw(self, code: int, raw: bytes) -> None:
            try:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError, OSError):
                return

        def handle(self):
            try:
                super().handle()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError, OSError):
                return

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/api/callouts":
                rows, err = _callout_page_rows()
                self._json(200, {"ok": not err, "rows": rows, "error": err, "count": len(rows)})
                return
            if path == "/api/open-holds":
                from urllib.parse import parse_qs
                query = parse_qs(self.path.split("?", 1)[1]) if "?" in self.path else {}
                if "clear" in query:
                    clear_openholds_user()
                elif query.get("drop"):
                    drop_openholds_user((query.get("drop") or [""])[0])
                elif query.get("pick"):
                    pick_openholds_user((query.get("pick") or [""])[0])
                elif "all" in query:
                    flag = (query.get("all") or ["0"])[0]
                    user = (query.get("user") or [""])[0]
                    set_openholds_existing(flag not in ("0", "false", "off"), user)
                if "dir" in query:
                    self._json(200, {"ok": True, "callers": caller_directory(), "watch": openholds_watch()})
                    return
                _start_openholds_refresh()
                raw = _oh_snap_raw
                if raw:
                    self._raw(200, raw)
                    return
                self._json(200, {"ok": True, "groups": [], "picked": [], "watch": openholds_watch(), "callers": []})
                return
            if path == "/api/predict":
                rows, err = _predict_rows()
                self._json(200, {"ok": not err, "rows": rows, "error": err, "count": len(rows)})
                return
            if path == "/api/live-prices":
                self._json(200, _live_price_rows())
                return
            if path == "/api/decoded-ath":
                from urllib.parse import parse_qs
                query = parse_qs(self.path.split("?", 1)[1]) if "?" in self.path else {}
                mints = (query.get("mints") or [""])[0].split(",")
                self._json(200, {"aths": _ath_lookup(mints)})
                return
            return super().do_GET()

        def do_POST(self):
            path = self.path.split("?", 1)[0]
            if path == "/api/restart":
                if not _http_restart:
                    self._json(503, {"ok": False, "error": "Restart is not ready yet"})
                    return
                try:
                    _http_restart()
                except Exception as e:
                    self._json(500, {"ok": False, "error": str(e)[:180]})
                    return
                self._json(200, {"ok": True})
                return
            if path in ("/api/stop", "/api/start"):
                if not _http_run:
                    self._json(503, {"ok": False, "error": "Not ready yet"})
                    return
                try:
                    fut = _http_run(path == "/api/start")
                    result = fut.result(timeout=8) if hasattr(fut, "result") else fut
                except Exception as e:
                    self._json(500, {"ok": False, "error": str(e)[:180]})
                    return
                if isinstance(result, dict) and result.get("ok") is False:
                    self._json(400, result)
                    return
                self._json(200, result if isinstance(result, dict) else {"ok": True})
                return
            if path == "/api/openhold-order":
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    n = 0
                raw = self.rfile.read(n) if n > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except Exception:
                    body = {}
                action = str((body or {}).get("action") or "").strip().lower()
                mint = str((body or {}).get("mint") or "").strip()
                if action not in ("buy", "sell") or not mint:
                    self._json(400, {"ok": False, "error": "Need buy or sell, and a coin"})
                    return
                if action == "buy":
                    left = openhold_room_left()
                    if left is not None and left <= 0:
                        self._json(200, {"ok": False, "error": "No free slot"})
                        return
                queue_openhold_order(
                    action,
                    mint,
                    str((body or {}).get("symbol") or ""),
                    str((body or {}).get("name") or ""),
                    str((body or {}).get("user") or ""),
                    str((body or {}).get("wallet") or ""),
                )
                self._json(200, {"ok": True, "queued": action})
                return
            if path == "/api/sell-now":
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    n = 0
                raw = self.rfile.read(n) if n > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except Exception:
                    body = {}
                if str((body or {}).get("cmd") or "").strip() != "-1":
                    self._json(400, {"ok": False, "error": "Type -1 to sell now"})
                    return
                if not _http_sell:
                    self._json(503, {"ok": False, "error": "Sell is not ready yet"})
                    return
                mint = str((body or {}).get("mint") or "").strip()
                try:
                    fut = _http_sell(mint)
                    result = fut.result(timeout=90) if hasattr(fut, "result") else fut
                except Exception as e:
                    self._json(500, {"ok": False, "error": str(e)[:180]})
                    return
                if not isinstance(result, dict):
                    result = {"ok": True}
                self._json(200 if result.get("ok") else 409, result)
                return
            if path == "/api/mint-flag":
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    n = 0
                raw = self.rfile.read(n) if n > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except Exception:
                    body = {}
                from src.storage.mint_flags import set_mint_flag
                ok = set_mint_flag(str(body.get("mint") or ""), str(body.get("flag") or ""))
                if not ok:
                    self._json(400, {"ok": False, "error": "Need a mint and DUST, NODUST, or CLEAR"})
                    return
                self._json(200, {"ok": True, "mint": body.get("mint"), "flag": str(body.get("flag") or "").upper()})
                return
            if path == "/api/clear":
                if not _http_clear:
                    self._json(503, {"ok": False, "error": "Clear is not ready yet"})
                    return
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    n = 0
                raw = self.rfile.read(n) if n > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except Exception:
                    body = {}
                what = str((body or {}).get("what") or "session")
                try:
                    fut = _http_clear(what)
                    result = fut.result(timeout=12) if hasattr(fut, "result") else fut
                except Exception as e:
                    self._json(500, {"ok": False, "error": str(e)[:180]})
                    return
                if not isinstance(result, dict):
                    result = {"ok": True}
                self._json(200 if result.get("ok", True) else 409, result)
                return
            if path == "/api/invested":
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    n = 0
                raw = self.rfile.read(n) if n > 0 else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                    sol = float(body.get("sol"))
                except Exception:
                    self._json(400, {"ok": False, "error": "Enter the invested amount in SOL"})
                    return
                try:
                    from src.storage.invested import apply_invested
                    saved = apply_invested(sol)
                except ValueError as e:
                    self._json(400, {"ok": False, "error": str(e)})
                    return
                except Exception as e:
                    self._json(500, {"ok": False, "error": str(e)[:160]})
                    return
                self._json(200, {"ok": True, "invested_sol": saved})
                return
            if path != "/api/wallet":
                self.send_error(404)
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = 0
            if n <= 0 or n > 20000:
                self._json(400, {"ok": False, "error": "Upload the keypair file only (bot-wallet.json or a base58 secret)"})
                return
            body = self.rfile.read(n)
            busy = ""
            if _wallet_busy:
                try:
                    busy = _wallet_busy() or ""
                except Exception:
                    busy = ""
            if busy:
                self._json(409, {"ok": False, "error": busy})
                return
            try:
                from src.execution.signer import SignerError, install_uploaded_keypair
                pub = install_uploaded_keypair(body)
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)[:180]})
                return
            if _http_wallet:
                try:
                    fut = _http_wallet(pub)
                    if hasattr(fut, "result"):
                        fut.result(timeout=8)
                except Exception:
                    log.exception("wallet arm notify failed")
            self._json(200, {"ok": True, "pubkey": pub, "armed": True})

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("Dashboard UI available at http://localhost:%d/", port)
    return server


_ath_cache: Dict[str, Dict[str, Any]] = {}
_ath_lock = threading.Lock()
_ath_busy = False


def _fetch_ath(mint: str) -> None:
    import time
    import urllib.request

    ath = None
    try:
        req = urllib.request.Request(
            f"https://frontend-api-v3.pump.fun/coins-v2/{mint}",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://pump.fun",
                "Referer": f"https://pump.fun/coin/{mint}",
            },
        )
        with urllib.request.urlopen(req, timeout=4) as response:
            body = json.loads(response.read().decode() or "{}")
        if isinstance(body, dict) and body.get("ath_market_cap") is not None:
            ath_raw = float(body.get("ath_market_cap") or 0)
            usd = float(body.get("usd_market_cap") or body.get("market_cap_usd") or 0)
            sol_mcap = float(body.get("market_cap") or 0)
            ath_usd = ath_raw
            if ath_raw > 0 and usd > 0 and sol_mcap > 0 and ath_raw < usd * 0.2:
                ath_usd = ath_raw * (usd / sol_mcap)
            # Pump starts near 27.96 SOL of market cap. Percent above that, not the dollar ATH.
            if ath_usd > 0 and usd > 0 and sol_mcap > 0:
                launch_usd = 27.96 * (usd / sol_mcap)
                if launch_usd > 0:
                    ath = (ath_usd / launch_usd - 1.0) * 100.0
    except Exception:
        ath = None
    with _ath_lock:
        _ath_cache[mint] = {"ts": time.time(), "ath": ath}
        if len(_ath_cache) > 500:
            oldest = sorted(_ath_cache, key=lambda key: _ath_cache[key].get("ts") or 0)[:100]
            for key in oldest:
                _ath_cache.pop(key, None)


def _fill_ath(mints: List[str]) -> None:
    global _ath_busy
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(_fetch_ath, mint) for mint in mints]
            for fut in as_completed(futs, timeout=8):
                try:
                    fut.result()
                except Exception:
                    pass
    except Exception:
        log.debug("ath fill failed", exc_info=True)
    finally:
        with _ath_lock:
            _ath_busy = False


def _queue_ath(mints: List[str]) -> None:
    global _ath_busy
    import time

    now = time.time()
    stale: List[str] = []
    with _ath_lock:
        if _ath_busy:
            return
        for mint in mints:
            if not mint:
                continue
            hit = _ath_cache.get(mint)
            if not hit or now - float(hit.get("ts") or 0) > 25:
                stale.append(mint)
            if len(stale) >= 8:
                break
        if not stale:
            return
        _ath_busy = True
    threading.Thread(target=_fill_ath, args=(stale,), daemon=True).start()


def _ath_lookup(mints: List[str]) -> Dict[str, Any]:
    found: Dict[str, Any] = {}
    missing: List[str] = []
    with _ath_lock:
        for mint in mints[:24]:
            mint = (mint or "").strip()
            if not mint:
                continue
            hit = _ath_cache.get(mint)
            if hit and hit.get("ath") is not None:
                found[mint] = hit["ath"]
            else:
                missing.append(mint)
    if missing:
        _queue_ath(missing)
    return found


_m5_cache: Dict[str, Any] = {}


def _fmt_pct(value: Any, places: int = 1) -> str:
    if value is None:
        return "none"
    try:
        return f"{float(value):+.{places}f}%"
    except (TypeError, ValueError):
        return "none"


async def _decoded_marks(mint: str):
    """5m from Dex and Pump, plus the 1m and 1s move from Dex."""
    import time
    now = time.time()
    hit = _m5_cache.get(mint)
    if hit and now - hit[0] < 8:
        return hit[1], hit[2], hit[3], hit[4]
    dex = pump = minute = second = None
    try:
        from src.signal.dayscalp import fetch_m5_pct, _pump_activity, _pct_num, dex_moves
        try:
            dex = await asyncio.wait_for(fetch_m5_pct(mint), timeout=4)
        except Exception:
            dex = None
        try:
            status, body, _note = await asyncio.wait_for(_pump_activity(mint), timeout=4)
            if status == 200 and isinstance(body, dict):
                pump = _pct_num(body.get("5m"))
        except Exception:
            pump = None
        try:
            second, minute = await asyncio.wait_for(dex_moves(mint), timeout=6)
        except Exception:
            second = minute = None
    except Exception:
        log.debug("decoded mark lookup failed", exc_info=True)
    if len(_m5_cache) > 400:
        _m5_cache.clear()
    _m5_cache[mint] = (now, dex, pump, minute, second)
    return dex, pump, minute, second


def _marks_lead(dex, pump, minute, second) -> str:
    return (
        f"5m(dex) {_fmt_pct(dex)} · 5m(pump) {_fmt_pct(pump)} · "
        f"1m(dex) {_fmt_pct(minute)} · 1s(dex) {_fmt_pct(second, 2)}"
    )


def _with_marks(kwargs: Dict[str, Any], dex, pump, minute, second) -> Dict[str, Any]:
    lead = _marks_lead(dex, pump, minute, second)
    noise = ("gecko 1s empty", "pump has no 1s", "dex has no 1s", "no 1s price")
    reasons = [str(x) for x in (kwargs.get("reasons") or []) if x]
    reasons = [
        x for x in reasons
        if not str(x).startswith("5m(dex)") and not any(bit in str(x) for bit in noise)
    ]
    out = dict(kwargs)
    out["reasons"] = [lead, *reasons]
    return out


async def _fill_decoded_marks(kwargs: Dict[str, Any]) -> None:
    """Fetch the percents after the row is already on screen. Never blocks the scan."""
    mint = str(kwargs.get("mint") or "")
    if not mint:
        return
    try:
        dex, pump, minute, second = await _decoded_marks(mint)
        fresh = _with_marks(kwargs, dex, pump, minute, second)
        fresh["_refresh"] = False
        await publish_decoded(**fresh)
    except Exception:
        log.debug("decoded mark refresh failed", exc_info=True)


_decoded_verdicts: Dict[str, str] = {}


def _skip_on_page() -> int:
    return sum(1 for verdict in _decoded_verdicts.values() if str(verdict).upper() == "SKIP")


async def publish_decoded(**kwargs: Any) -> None:
    import time
    refresh = kwargs.pop("_refresh", True)
    mint = str(kwargs.get("mint") or "")
    verdict = str(kwargs.get("verdict") or "")
    if mint and verdict:
        _decoded_verdicts[mint] = verdict
        if len(_decoded_verdicts) > 400:
            _decoded_verdicts.pop(next(iter(_decoded_verdicts)))
    hit = _ath_cache.get(mint) if mint else None
    if hit and hit.get("ath") is not None:
        kwargs["ath_mcap"] = hit["ath"]
    elif mint:
        _queue_ath([mint])
    if mint:
        cached = _m5_cache.get(mint)
        fresh = cached and (time.time() - float(cached[0] or 0)) < 20
        if fresh:
            kwargs = _with_marks(kwargs, cached[1], cached[2], cached[3], cached[4])
        elif refresh:
            asyncio.create_task(_fill_decoded_marks(dict(kwargs)))
    payload = {"type": "decoded", **kwargs}
    await bus.publish(payload)


async def publish_stats(stats: Dict[str, Any]) -> None:
    try:
        from src.observability.ledger import get_ledger
        led = get_ledger()
        stats = {**stats, **led.pnl_board(), **led.session_trade_counts()}
    except Exception:
        log.debug("pnl board attach failed", exc_info=True)
    stats["skip_signals"] = _skip_on_page()
    bus.update_stats(**stats)
    await bus.publish({"type": "stats", "stats": dict(bus._stats)})


async def publish_heartbeat(slot: Optional[int], extra: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {"type": "heartbeat", "slot": slot}
    if extra:
        payload.update(extra)
    stats = payload.get("stats")
    if isinstance(stats, dict):
        try:
            from src.observability.ledger import get_ledger
            led = get_ledger()
            stats = {**stats, **led.pnl_board(), **led.session_trade_counts()}
            stats["skip_signals"] = _skip_on_page()
            payload["stats"] = stats
            bus.update_stats(**stats)
        except Exception:
            log.debug("heartbeat ledger counts failed", exc_info=True)
    if slot is not None:
        bus.update_stats(last_slot=slot)
    await bus.publish(payload)


async def publish_alert(
    *,
    level: str,
    title: str,
    message: str,
    mint: Optional[str] = None,
    verdict: Optional[str] = None,
    liquidity_sol: Optional[float] = None,
    source: Optional[str] = None,
    signature: Optional[str] = None,
) -> None:
    await bus.publish(
        {
            "type": "alert",
            "level": level,
            "title": title,
            "message": message,
            "mint": mint,
            "verdict": verdict,
            "liquidity_sol": liquidity_sol,
            "source": source,
            "signature": signature,
        }
    )


async def publish_positions(snapshot: Dict[str, Any]) -> None:
    live = 0.0
    priced = False
    for p in snapshot.get("open") or []:
        if not p.get("priced") or p.get("pnl_pct") is None:
            continue
        priced = True
        paid = float(p.get("entry_sol") or 0)
        fees = float(p.get("fees_sol") or 0)
        pct = float(p.get("pnl_pct") or 0)
        live += paid * (1.0 + pct / 100.0) - (paid + fees)
    if not priced and not (snapshot.get("open") or []):
        live = 0.0
    try:
        from src.observability.ledger import get_ledger
        board = get_ledger().set_live(live if priced or not snapshot.get("open") else get_ledger()._live)
        bus.update_stats(**board)
    except Exception:
        log.debug("live pnl update failed", exc_info=True)
        board = {}
    await bus.publish({"type": "positions", "snapshot": snapshot, "pnl": board or dict(bus._stats)})


async def publish_trade(trade: Dict[str, Any]) -> None:
    await bus.publish({"type": "trade", **trade})


async def publish_config(config: Dict[str, Any]) -> None:
    payload = dict(config)
    try:
        from src.config.runtime import bot_stopped
        payload["bot_stopped"] = bot_stopped()
    except Exception:
        pass
    try:
        from src.config.file_config import list_user_presets
        payload["exit_presets"] = list_user_presets()
    except Exception:
        payload["exit_presets"] = []
    bus._config = payload
    await bus.publish({"type": "config", "config": payload})

async def publish_wallet(info: dict) -> None:
    bus._wallet = info
    await bus.publish({"type": "wallet", "wallet": info})


async def publish_ledger(entries: list, summary: dict | None = None) -> None:
    bus._ledger = list(entries or [])
    bus._ledger_summary = summary or {}
    await bus.publish({"type": "ledger", "entries": bus._ledger, "summary": bus._ledger_summary})

async def publish_exec_history(entries: list) -> None:
    bus._exec_history = list(entries or [])
    await bus.publish({"type": "exec_history", "entries": bus._exec_history})


_http_openhold = None
_http_opensell = None
_oh_px: Dict[str, float] = {}
_oh_px_at: Dict[str, float] = {}
_oh_px_src: Dict[str, str] = {}   # mint -> "curve" or "pool" (where the 0.2s read came from)
_oh_px_lock = threading.Lock()
_oh_snap_raw: bytes = b""
_oh_snap_lock = threading.Lock()
_oh_amm: Dict[str, tuple] = {}
_oh_dns: Dict[str, str] = {}
_oh_sol: Dict[str, float] = {"usd": 0.0, "ts": 0.0}
_PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
_MIN_VSOL_LAMPORTS = 30_000_000_000   # standard pump curve floor
_AMM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
_WSOL_MINT = "So11111111111111111111111111111111111111112"


def _saved_wallet() -> Dict[str, Any]:
    info = getattr(bus, "_wallet", None)
    return info if isinstance(info, dict) else {}


def set_http_openhold(fn) -> None:
    global _http_openhold
    _http_openhold = fn


def set_http_opensell(fn) -> None:
    global _http_opensell
    _http_opensell = fn


def forget_openhold_seen(user: str, mint: str) -> None:
    key = str(user or "").casefold()
    mint = str(mint or "")
    seen = _oh_seen.get(key)
    if seen is not None and mint:
        seen.discard(mint)


def bind_trade_caller(pos) -> None:
    """Fill an empty caller name and wallet from the pin. Does not replace one already set."""
    if pos is None:
        return
    mint = str(getattr(pos, "mint", "") or "")
    if not mint:
        return
    have_user = str(getattr(pos, "callout_user", "") or "").strip()
    have_wallet = str(getattr(pos, "callout_wallet", "") or "").strip()
    if have_user and len(have_wallet) >= 32:
        return
    ident = caller_identity(mint)
    if not have_user and ident.get("user"):
        pos.callout_user = ident["user"]
    if len(have_wallet) < 32 and len(str(ident.get("wallet") or "")) >= 32:
        pos.callout_wallet = ident["wallet"]


def live_caller_name(mint: str, source: str = "", preferred: str = "") -> str:
    name = str(preferred or "").strip()
    if name:
        return name
    ident = caller_identity(str(mint or ""))
    return str(ident.get("user") or "") or "No caller holds"


def openhold_waiting() -> List[Dict[str, Any]]:
    return []


def reconcile_openhold_status(rows) -> None:
    """An open OpenHolds trade stays HOLDING. Anything else is left alone."""
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        mint = str(row.get("mint") or "")
        if not mint:
            continue
        source = str(row.get("source") or row.get("strategy") or "")
        if source in ("callout", "opencopy") or row.get("from_openholds"):
            if openhold_status(mint) not in ("BUYING", "HOLDING"):
                set_openhold_status(mint, "HOLDING")


def wallet_holding_mint(mint: str) -> str:
    """A caller wallet whose last open book still contains this mint."""
    mint = str(mint or "")
    if not mint:
        return ""
    with _hold_lock:
        books = list(_hold_cache.items())
    for wallet, book in books:
        if not isinstance(book, dict) or not book.get("ok"):
            continue
        if _book_has(book.get("positions") or [], mint, "", ""):
            return str(wallet or "")
    return ""


def refresh_copy_holds(rows) -> None:
    """Mark each open copy live or out from that caller's portfolio. A failed read does not change the mark."""
    from concurrent.futures import ThreadPoolExecutor

    planned = []
    wallets: List[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        mint = str(row.get("mint") or "")
        wallet = str(row.get("wallet") or "").strip()
        if not mint or not _useful(wallet):
            continue
        planned.append(row)
        if wallet not in wallets:
            wallets.append(wallet)
    books: Dict[str, Dict[str, Any]] = {}
    if wallets:
        workers = min(4, len(wallets))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for wallet, book in zip(wallets, pool.map(_caller_book, wallets)):
                books[wallet] = book
    for row in planned:
        mint = str(row.get("mint") or "")
        wallet = str(row.get("wallet") or "")
        book = books.get(wallet) or {}
        if not book.get("ok"):
            seen = _stream_hold(wallet, mint, "")
            if seen:
                _hold_by_mint[mint] = seen
            continue
        polled = "live" if _book_has(book.get("positions") or [], mint, str(row.get("name") or ""), str(row.get("symbol") or "")) else "out"
        _hold_by_mint[mint] = _stream_hold(wallet, mint, polled)


def pulse_for_mint(mint: str) -> Dict[str, Any]:
    """Last live OpenHolds percent for this mint. Empty when the price tick has not landed."""
    mint = str(mint or "")
    price, at = _best_px(mint)
    if not price:
        return {}
    return {"token_price": price, "px_at": at}


def _best_px(mint: str):
    """The fresher of the last socket trade and the 0.2s curve read. (price USD, ts)."""
    mint = str(mint or "")
    with _oh_px_lock:
        price = _oh_px.get(mint)
        at = float(_oh_px_at.get(mint) or 0)
        src = _oh_px_src.get(mint, "")
    read = None
    if price:
        if src == "pool":
            factor = _jup_factor(mint)        # Jupiter = pump's price for graduated coins
            if factor is None:
                factor = _px_factor(mint)     # pump's own price, checked against the pool
            read = price * (factor if factor is not None else 1.0)
        else:
            factor = _curve_factor(mint)
            if factor is not None:
                read = price * factor
    hit = _stream_raw_usd(mint, max_age=120.0)
    if hit and (not read or hit[1] >= at):
        return hit[0], hit[1]
    if read:
        return read, at
    jup = _jup_px.get(mint)
    if jup:
        return jup[0], jup[1]   # no pool read yet: Jupiter's own price
    return None, 0.0    # nothing matched yet: the row keeps pump's own numbers


def _openhold_allowed(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Auto-open: mint starts or ends with pump, unrealized above 0, and percent above 0 up to 5."""
    kept: List[Dict[str, Any]] = []
    for row in rows or []:
        mint = str(row.get("coinMint") or "")
        low = mint.lower()
        if not (low.startswith("pump") or low.endswith("pump")):
            continue
        if row.get("transfer") and not _caller_bought(str(row.get("wallet") or ""), mint):
            continue   # sent to the caller, not bought by them: never auto-copied
        try:
            unreal = float(row.get("unrealized_usd"))
            pct = float(row.get("percentage_usd") if row.get("percentage_usd") is not None else row.get("percentage"))
        except (TypeError, ValueError):
            continue
        if unreal > 0 and 0 < pct <= 5:
            kept.append(row)
    return kept


def _oh_sol_fast() -> float:
    import time
    try:
        usd = float(_oh_sol.get("usd") or 0)
        ts = float(_oh_sol.get("ts") or 0)
    except (TypeError, ValueError):
        usd, ts = 0.0, 0.0
    if time.time() - ts > 30 and not _oh_sol.get("_busy"):
        _oh_sol["_busy"] = True

        def _run() -> None:
            try:
                _oh_sol_usd()
            except Exception:
                pass
            finally:
                _oh_sol["_busy"] = False

        threading.Thread(target=_run, name="oh-sol", daemon=True).start()
    return usd if usd > 10 else 0.0


def _oh_sol_usd() -> float:
    import json
    import time
    from src.execution.tx_route import sync_https
    now = time.time()
    try:
        status, text = sync_https("GET", "https://api.coinbase.com/v2/prices/SOL-USD/spot", timeout=0.8)
        if status == 200:
            body = json.loads(text or "{}")
            amt = float(((body.get("data") or {}).get("amount")) or 0)
            if amt > 10:
                _oh_sol["usd"] = amt
                _oh_sol["ts"] = now
                return amt
    except Exception:
        log.debug("sol usd failed", exc_info=True)
    try:
        return float(_oh_sol.get("usd") or 0)
    except (TypeError, ValueError):
        return 0.0


def _oh_host_ip(host: str) -> str:
    import socket
    ip = str(_oh_dns.get(host) or "")
    if ip:
        return ip
    if _oh_dns.get("wait") == host:
        return ""
    box: Dict[str, str] = {}

    def _run() -> None:
        try:
            infos = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
            box["ip"] = infos[0][4][0]
        except Exception:
            box["ip"] = ""

    _oh_dns["wait"] = host
    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(0.25)
    ip = str(box.get("ip") or "")
    if ip:
        _oh_dns[host] = ip
    if _oh_dns.get("wait") == host:
        _oh_dns.pop("wait", None)
    return ip


def _oh_rpc_post(url: str, payload: dict, timeout: float = 0.35):
    import http.client
    import json
    import socket
    import ssl
    from urllib.parse import urlsplit
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    ip = _oh_host_ip(host)
    if not host or not ip:
        raise TimeoutError(host or "rpc")
    path = parsed.path or "/"
    if parsed.query:
        path = path + "?" + parsed.query
    raw = json.dumps(payload).encode()
    sock = socket.create_connection((ip, 443), timeout=timeout)
    try:
        sock.settimeout(timeout)
        ssock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        ssock.settimeout(timeout)
        conn = http.client.HTTPSConnection(host, timeout=timeout)
        conn.sock = ssock
        conn.request(
            "POST",
            path,
            body=raw,
            headers={
                "Host": host,
                "Content-Type": "application/json",
                "Content-Length": str(len(raw)),
                "Connection": "close",
                "Accept": "application/json",
            },
        )
        resp = conn.getresponse()
        return int(resp.status), resp.read().decode("utf-8", "replace")
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _oh_accounts(keys):
    import json
    if not keys:
        return []
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [list(keys), {"encoding": "base64", "commitment": "processed"}],
    }
    import os
    import time
    now = time.time()
    # Public nodes first, but skip one for a while after it fails (they rate-limit hard).
    # Then the paid primary, capped so buys and sells keep most of its rate.
    tries = []
    for url in ("https://api.mainnet-beta.solana.com", "https://solana-rpc.publicnode.com"):
        if _oh_rpc_cool.get(url, 0.0) <= now:
            tries.append((url, 0.35, True))
    primary = (os.environ.get("SOLANA_RPC_URL") or "").strip()
    if primary.startswith("http") and "mainnet-beta.solana.com" not in primary:
        if now - _oh_rpc_cool.get("_primary_last", 0.0) >= _OH_PRIMARY_GAP:
            tries.append((primary, 0.8, False))
    for url, wait, public in tries:
        if not public:
            _oh_rpc_cool["_primary_last"] = time.time()
        got = None
        try:
            status, text = _oh_rpc_post(url, payload, wait)
            if status == 200:
                body = json.loads(text or "{}")
                if isinstance(body, dict) and not body.get("error"):
                    got = (body.get("result") or {}).get("value")
        except Exception:
            got = None
        if isinstance(got, list) and len(got) == len(keys):
            return got
        if public:
            _oh_rpc_cool[url] = time.time() + 15.0
    return None


_oh_rpc_cool: Dict[str, float] = {}
_OH_READ_MINTS = 48   # one getMultipleAccounts call: 2 accounts per coin
_OH_PRIMARY_GAP = 0.5   # at most 2 price reads a second on the paid RPC


def _oh_raw(item) -> bytes:
    import base64
    if not isinstance(item, dict):
        return b""
    data = item.get("data")
    if isinstance(data, list) and data:
        try:
            return base64.b64decode(data[0])
        except Exception:
            return b""
    return b""


def _oh_listed_mints() -> List[str]:
    """Open coins on the callers' books, most active first: coins this bot holds, then the most
    recently traded or opened. Dead coins go last so they never crowd out a moving one."""
    seen: Dict[str, str] = {}
    with _hold_lock:
        books = list(_hold_cache.values())
    for book in books:
        if not isinstance(book, dict):
            continue
        for pos in book.get("positions") or []:
            if not isinstance(pos, dict) or pos.get("isExited"):
                continue
            mint = str(pos.get("coinMint") or "")
            if not mint or len(mint) < 32 or mint.startswith("0x"):
                continue
            stamp = str(pos.get("updatedAt") or pos.get("openedAt") or "")
            if stamp > seen.get(mint, ""):
                seen[mint] = stamp
    held = {m for m, st in list(_oh_status.items()) if st in ("HOLDING", "BUYING")}
    ranked = sorted(seen, key=lambda m: seen[m], reverse=True)   # latest ISO time first
    ranked.sort(key=lambda m: m not in held)                    # stable: held coins on top
    return ranked[:150]


_oh_pool_addr: Dict[str, str] = {}      # mint -> PumpSwap pool (pump's own record)
_oh_pool_asked: Dict[str, float] = {}
_jup_px: Dict[str, tuple] = {}          # mint -> (USD price, ts) from Jupiter
_jup_fac: Dict[str, List[float]] = {}   # mint -> recent Jupiter / raw pool ratios


def _oh_pool_lookup(mint: str) -> None:
    """Newer PumpSwap pools are not at the derived address. pump's coin record names the pool."""
    import time
    now = time.time()
    if mint in _oh_pool_addr or now - _oh_pool_asked.get(mint, 0.0) < 60:
        return
    _oh_pool_asked[mint] = now

    def _run() -> None:
        try:
            coin = _pump_json(f"https://frontend-api-v3.pump.fun/coins-v2/{mint}", timeout=3.0)
            pool = str((coin or {}).get("pump_swap_pool") or "") if isinstance(coin, dict) else ""
            if len(pool) >= 32:
                _oh_pool_addr[mint] = pool
                log.info("POOL %s PumpSwap pool %s (from pump)", mint[:8], pool[:8])
        except Exception:
            log.debug("pool lookup failed %s", mint[:8], exc_info=True)

    threading.Thread(target=_run, name="oh-pool", daemon=True).start()


def _jup_prices(mints: List[str]) -> Dict[str, float]:
    import json
    import os
    from src.execution.tx_route import sync_https
    key = (os.environ.get("JUPITER_API_KEY") or "").strip()
    ids = ",".join(mints[:50])
    tries = []
    if key:
        tries.append((f"https://api.jup.ag/price/v3?ids={ids}", {"x-api-key": key}))
    tries.append((f"https://lite-api.jup.ag/price/v3?ids={ids}", {}))
    for url, headers in tries:
        try:
            status, text = sync_https("GET", url, timeout=1.5, headers=headers)
        except TypeError:
            status, text = sync_https("GET", url, timeout=1.5)
        except Exception:
            continue
        if status != 200:
            continue
        body = json.loads(text or "{}")
        out: Dict[str, float] = {}
        for mint, item in (body or {}).items():
            try:
                usd = float((item or {}).get("usdPrice") or 0)
            except (TypeError, ValueError, AttributeError):
                continue
            if usd > 0:
                out[mint] = usd
        return out
    return {}


def _jup_loop() -> None:
    """Graduated coins: Jupiter's price (what pump shows) once a second, one batched call.
    It sets the scale for the 0.5s pool read, so the row ticks fast and stays on pump's price."""
    import time
    pause = 1.0
    while True:
        time.sleep(pause)
        try:
            listed = _oh_listed_mints()
            with _oh_px_lock:
                pools = [m for m in listed if _oh_px_src.get(m) == "pool" or m in _oh_pool_addr]
            if not pools:
                pause = 1.0
                continue
            got = _jup_prices(pools[:50])
            if not got:
                pause = min(5.0, pause * 1.5)   # rate-limited: ease off
                continue
            pause = 1.0
            now = time.time()
            for mint, usd in got.items():
                _jup_px[mint] = (usd, now)
                with _oh_px_lock:
                    raw = _oh_px.get(mint)
                    at = float(_oh_px_at.get(mint) or 0)
                if raw and now - at < 3.0:
                    marks = _jup_fac.setdefault(mint, [])
                    marks.append(usd / raw)
                    del marks[:-5]
        except Exception:
            log.debug("jupiter price loop failed", exc_info=True)
            pause = 3.0


def _jup_factor(mint: str):
    """Median Jupiter / pool ratio of the last few ticks, or None."""
    import time
    marks = _jup_fac.get(mint) or []
    hit = _jup_px.get(mint)
    if not marks or not hit or time.time() - hit[1] > 15:
        return None
    ordered = sorted(marks)
    return ordered[len(ordered) // 2]


def _oh_spot_once() -> None:
    """Curve coins from the curve. Graduated coins from the pool. Never drop the live price."""
    import struct
    import time
    try:
        from solders.pubkey import Pubkey
    except Exception:
        return
    # Coins the socket priced in the last 2s already have a fresher price than any read:
    # skip them, so the RPC only fills the gaps (saves most of the paid bandwidth).
    mints = [m for m in _oh_listed_mints() if _stream_raw_usd(m, 2.0) is None][:_OH_READ_MINTS]
    sol = _oh_sol_fast()
    if not mints or sol <= 0:
        return
    program = Pubkey.from_string(_PUMP)
    amm_program = Pubkey.from_string(_AMM)
    wsol = Pubkey.from_string(_WSOL_MINT)
    keys: List[str] = []
    slots: List[tuple] = []
    for mint in mints:
        try:
            mint_pk = Pubkey.from_string(mint)
        except Exception:
            continue
        if mint not in _oh_amm:
            curve = Pubkey.find_program_address([b"bonding-curve", bytes(mint_pk)], program)[0]
            keys.append(str(curve))
            slots.append((mint, "curve"))
            pool = _oh_pool_addr.get(mint)
            if not pool:
                auth = Pubkey.find_program_address([b"pool-authority", bytes(mint_pk)], program)[0]
                pool = str(Pubkey.find_program_address(
                    [b"pool", (0).to_bytes(2, "little"), bytes(auth), bytes(mint_pk), bytes(wsol)],
                    amm_program,
                )[0])
            keys.append(str(pool))
            slots.append((mint, "pool"))
        else:
            base_ata, quote_ata = _oh_amm[mint]
            keys.append(base_ata)
            slots.append((mint, "base"))
            keys.append(quote_ata)
            slots.append((mint, "quote"))
    if not keys:
        return
    values = _oh_accounts(keys[:96])
    if not isinstance(values, list):
        return
    parts: Dict[str, Dict[str, bytes]] = {}
    for (mint, kind), item in zip(slots, values):
        parts.setdefault(mint, {})[kind] = _oh_raw(item)
    fresh: Dict[str, float] = {}
    kinds: Dict[str, str] = {}
    extra_keys: List[str] = []
    extra_slots: List[tuple] = []
    now = time.time()
    for mint in mints:
        got = parts.get(mint) or {}
        curve_raw = got.get("curve") or b""
        on_curve = False
        if len(curve_raw) >= 49 and curve_raw[48] == 0:
            virtual_token = struct.unpack_from("<Q", curve_raw, 8)[0]
            virtual_sol = struct.unpack_from("<Q", curve_raw, 16)[0]
            # A standard pump curve never has under 30 virtual SOL. Below that the coin is a
            # non-SOL / custom curve that pump prices differently, so keep pump's own price.
            if virtual_token > 0 and virtual_sol >= _MIN_VSOL_LAMPORTS:
                price_usd = ((virtual_sol / 1_000_000_000) / (virtual_token / 1_000_000)) * sol
                if price_usd > 0:
                    fresh[mint] = price_usd
                    kinds[mint] = "curve"
                    on_curve = True
        pool_raw = got.get("pool") or b""
        if len(curve_raw) >= 49 and curve_raw[48] == 1 and len(pool_raw) < 203:
            _oh_pool_lookup(mint)   # graduated, but the derived pool is empty: ask pump for it
        if len(pool_raw) >= 203:
            try:
                base_mint = str(Pubkey.from_bytes(pool_raw[43:75]))
                quote_mint = str(Pubkey.from_bytes(pool_raw[75:107]))
            except Exception:
                base_mint = quote_mint = ""
            if base_mint == mint and quote_mint == _WSOL_MINT:
                base_ata = str(Pubkey.from_bytes(pool_raw[139:171]))
                quote_ata = str(Pubkey.from_bytes(pool_raw[171:203]))
                _oh_amm[mint] = (base_ata, quote_ata)
                if not on_curve:
                    extra_keys.extend((base_ata, quote_ata))
                    extra_slots.append((mint, "base"))
                    extra_slots.append((mint, "quote"))
        if on_curve:
            continue
        base_raw = got.get("base") or b""
        quote_raw = got.get("quote") or b""
        if len(base_raw) >= 72 and len(quote_raw) >= 72:
            token_amt = struct.unpack_from("<Q", base_raw, 64)[0]
            sol_amt = struct.unpack_from("<Q", quote_raw, 64)[0]
            if token_amt > 0 and sol_amt > 0:
                price_usd = ((sol_amt / 1_000_000_000) / (token_amt / 1_000_000)) * sol
                if price_usd > 0:
                    fresh[mint] = price_usd
    if extra_keys:
        more = _oh_accounts(extra_keys[:96])
        if isinstance(more, list):
            for (mint, kind), item in zip(extra_slots, more):
                raw = _oh_raw(item)
                parts.setdefault(mint, {})[kind] = raw
            for mint, _kind in extra_slots:
                if mint in fresh:
                    continue
                got = parts.get(mint) or {}
                base_raw = got.get("base") or b""
                quote_raw = got.get("quote") or b""
                if len(base_raw) < 72 or len(quote_raw) < 72:
                    continue
                token_amt = struct.unpack_from("<Q", base_raw, 64)[0]
                sol_amt = struct.unpack_from("<Q", quote_raw, 64)[0]
                if token_amt <= 0 or sol_amt <= 0:
                    continue
                price_usd = ((sol_amt / 1_000_000_000) / (token_amt / 1_000_000)) * sol
                if price_usd > 0:
                    fresh[mint] = price_usd
    if not fresh:
        return
    with _oh_px_lock:
        for mint, price in fresh.items():
            _oh_px[mint] = price
            _oh_px_at[mint] = now
            _oh_px_src[mint] = kinds.get(mint, "pool")


def _oh_price_loop() -> None:
    import time
    while True:
        started = time.time()
        try:
            _oh_spot_once()
        except Exception:
            log.debug("openholds price failed", exc_info=True)
        time.sleep(max(0.05, 0.2 - (time.time() - started)))


def _oh_publish_loop() -> None:
    """Refresh the page body on its own. A slow price read must not stall it."""
    import time
    global _oh_snap_raw
    while True:
        started = time.time()
        try:
            raw = json.dumps(open_holds_snapshot(), separators=(",", ":")).encode("utf-8")
            with _oh_snap_lock:
                _oh_snap_raw = raw
        except Exception:
            log.debug("openholds publish failed", exc_info=True)
        time.sleep(max(0.05, 0.15 - (time.time() - started)))


def _live_price_rows() -> Dict[str, Any]:
    """Prices already on the position snapshot. No network, so the poll cannot 404 or stall."""
    snap = getattr(bus, "_positions", None) or {}
    opens = snap.get("open") if isinstance(snap, dict) else None
    rows: List[Dict[str, Any]] = []
    for pos in opens or []:
        if not isinstance(pos, dict) or not pos.get("mint"):
            continue
        rows.append({
            "mint": pos.get("mint"),
            "price": pos.get("price_usd"),
            "percentage_sol": pos.get("sol_paid_pct") if pos.get("sol_paid_pct") is not None else pos.get("pnl_pct"),
            "percentage_usd": pos.get("pnl_pct"),
            "unrealized_usd": pos.get("pnl_usd"),
            "unrealized_sol": pos.get("pnl_sol"),
            "m5": pos.get("live_m5_pct"),
            "h1": pos.get("live_h1_pct"),
        })
    return {"ok": True, "rows": rows}


def _apply_live_row(row: Dict[str, Any], pos: Dict[str, Any]) -> None:
    """Profile PnL is value minus remaining cost, plus realized. Percent is that over spent."""
    mint = str((row or {}).get("coinMint") or "")
    if not mint:
        return
    price, at = _best_px(mint)
    if not price or price <= 0:
        return
    row["token_price"] = price
    row["px_at"] = at

    def _num(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    held = _num((pos or {}).get("amountHeld"))
    if held is None or held <= 0:
        held = _num(row.get("amount_held"))
    bought = _num((pos or {}).get("amountBoughtUsd"))
    if bought is None or bought <= 0:
        bought = _num(row.get("amount_bought_usd"))
    cost = _num((pos or {}).get("costBasisUsd"))
    if cost is None or cost <= 0:
        cost = _num(row.get("cost_basis_usd"))
    realized = _num((pos or {}).get("realizedPnlUsd"))
    if realized is None:
        realized = _num(row.get("realized_pnl_usd")) or 0.0
    if held is None or held <= 0 or cost is None or cost <= 0:
        return
    pnl = held * price - cost + realized
    basis = bought if bought and bought > 0 else cost
    pct = pnl / basis * 100.0
    row["unrealized_usd"] = pnl
    row["percentage_usd"] = pct
    row["percentage"] = pct

