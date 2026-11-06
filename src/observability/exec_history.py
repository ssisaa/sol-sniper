
"""
Execution transaction history — bot fills + on-chain wallet signatures.

Persists to logs/exec_history.jsonl and is shown on the dashboard
"Execution history" panel (separate from the PnL ledger).
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger("sol-sniper.exec_history")

ROOT = Path(__file__).resolve().parents[2]
HISTORY_PATH = ROOT / "logs" / "exec_history.jsonl"
WALLET_KEEP = 50
WALLET_SHOW = 50
WALLET_HISTORY_PATH = ROOT / "logs" / "wallet_exec.jsonl"


def _ticker(mint: str, symbol: str) -> str:
    """Name or ticker. A mint prefix such as CadtjS is not a name."""
    mint = str(mint or "")
    text = str(symbol or "").strip()
    try:
        from src.execution.wallet_tokens import cached_label
        label = cached_label(mint)
        if label.get("symbol") and str(label.get("symbol")).upper() != "SPL":
            return label["symbol"]
        if label.get("name") and str(label.get("name")).upper() != "SPL":
            return label["name"]
    except Exception:
        pass
    if text and text.upper() != "SPL" and mint and text not in (mint[:4], mint[:6], mint[:8]) and not mint.startswith(text):
        return text
    try:
        from src.observability.ledger import get_ledger
        for entry in reversed(get_ledger().recent(250)):
            if (entry.get("mint") or "") == mint and entry.get("symbol"):
                found = str(entry.get("symbol"))
                if found and found not in (mint[:4], mint[:6], mint[:8]):
                    return found
    except Exception:
        pass
    return text or (mint[:4] if mint else "—")


def _is_valid_tx_sig(sig: str) -> bool:
    if not sig or not isinstance(sig, str):
        return False
    s = sig.strip()
    if s.startswith("paper") or s.startswith("jito:"):
        return False
    if not (80 <= len(s) <= 90):
        return False
    alphabet = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    return all(c in alphabet for c in s)


@dataclass
class ExecTx:
    ts: float
    side: str  # BUY | SELL | ONCHAIN
    mint: str = ""
    symbol: str = ""
    sol_amount: float = 0.0
    token_amount: float = 0.0
    signature: str = ""
    status: str = "submitted"  # submitted | confirmed | failed | onchain
    route: str = ""
    reason: str = ""
    mode: str = "live"
    solscan_tx: str = ""
    solscan_token: str = ""
    source: str = "bot"  # bot | wallet_rpc

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ExecutionHistory:
    def __init__(self, path: Optional[Path] = None, max_memory: int = 200):
        self.path = path or HISTORY_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_memory = max_memory
        self.entries: List[ExecTx] = []
        self.wallet_entries: List[ExecTx] = []
        self._load()
        self._load_wallet()

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            for line in self.path.read_text(encoding="utf-8").splitlines()[-self.max_memory :]:
                if not line.strip():
                    continue
                d = json.loads(line)
                fields = {k: d[k] for k in ExecTx.__dataclass_fields__ if k in d}
                self.entries.append(ExecTx(**fields))
        except Exception as e:
            log.warning("exec history load: %s", e)

    def _load_wallet(self) -> None:
        path = WALLET_HISTORY_PATH
        if not path.is_file():
            return
        try:
            rows = []
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                d = json.loads(line)
                fields = {k: d[k] for k in ExecTx.__dataclass_fields__ if k in d}
                rows.append(ExecTx(**fields))
            rows.sort(key=lambda e: e.ts or 0.0, reverse=True)
            self.wallet_entries = rows[:WALLET_KEEP]
        except Exception as e:
            log.warning("wallet exec load: %s", e)

    def _rewrite_wallet(self) -> None:
        WALLET_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        ordered = sorted(self.wallet_entries, key=lambda e: e.ts or 0.0)
        text = "".join(json.dumps(e.to_dict()) + "\n" for e in ordered[-WALLET_KEEP:])
        WALLET_HISTORY_PATH.write_text(text, encoding="utf-8")

    def clear(self) -> None:
        self.entries.clear()
        self.wallet_entries.clear()
        try:
            self.path.write_text("", encoding="utf-8")
        except Exception:
            pass
        try:
            WALLET_HISTORY_PATH.write_text("", encoding="utf-8")
        except Exception:
            pass

    def set_from_wallet(self, rows: List[Dict[str, Any]], keep_sigs: Optional[List[str]] = None) -> None:
        """Merge decoded wallet transactions. Unparsed ONCHAIN zeros are not shown."""
        built: List[ExecTx] = []
        seen = set()
        for r in rows or []:
            sig = (r.get("signature") or "").strip()
            if not sig or sig in seen:
                continue
            side = (r.get("side") or "").upper()
            sol_amount = float(r.get("sol_amount") or 0)
            mint = r.get("mint") or ""
            if not sig:
                continue
            seen.add(sig)
            built.append(ExecTx(
                ts=float(r.get("block_time") or r.get("ts") or time.time()),
                side=side or "ONCHAIN",
                mint=mint,
                symbol=r.get("symbol") or (mint[:8] if mint else ""),
                sol_amount=sol_amount,
                token_amount=float(r.get("token_amount") or 0),
                signature=sig,
                status=r.get("status") or "confirmed",
                route="wallet",
                reason=r.get("reason") or "wallet_tx",
                mode="live",
                solscan_tx=f"https://solscan.io/tx/{sig}" if _is_valid_tx_sig(sig) else "",
                solscan_token=f"https://solscan.io/token/{mint}" if mint else "",
                source="wallet",
            ))
        old = {e.signature: e for e in self.wallet_entries if e.signature}
        for e in built:
            prev = old.get(e.signature)
            if prev and not e.symbol and prev.symbol:
                e.symbol = prev.symbol
                e.mint = e.mint or prev.mint
                e.solscan_token = e.solscan_token or prev.solscan_token
            if prev and e.sol_amount <= 0 and prev.sol_amount > 0:
                e.sol_amount = prev.sol_amount
            if prev and e.token_amount <= 0 and prev.token_amount > 0:
                e.token_amount = prev.token_amount
            if prev and e.side in ("ONCHAIN", "", "TX") and prev.side not in ("ONCHAIN", "", "TX"):
                e.side = prev.side
        by_sig = {e.signature: e for e in built}
        for sig, prev in old.items():
            if sig in by_sig:
                continue
            if prev.side in ("", "ONCHAIN", "TX") and prev.sol_amount <= 0 and not prev.mint:
                continue
            if keep_sigs is not None and sig not in keep_sigs:
                continue
            by_sig[sig] = prev
        order = list(keep_sigs or [])
        merged: List[ExecTx] = []
        used = set()
        for sig in order:
            row = by_sig.get(sig)
            if row is None or sig in used:
                continue
            used.add(sig)
            merged.append(row)
        for sig, row in by_sig.items():
            if sig not in used:
                merged.append(row)
        merged.sort(key=lambda e: e.ts or 0.0, reverse=True)
        self.wallet_entries = merged[:WALLET_KEEP]
        self._rewrite_wallet()
        if len(self.wallet_entries) != getattr(self, "_synced_n", None):
            self._synced_n = len(self.wallet_entries)
            log.info("EXEC wallet sync %d txs", len(self.wallet_entries))

    def wallet_recent(self) -> List[Dict[str, Any]]:
        """Latest 50 wallet transactions, newest first."""
        by: Dict[str, ExecTx] = {}
        for e in list(self.wallet_entries) + list(self.entries):
            key = e.signature or ""
            if not key and not e.mint:
                continue
            key = key or f"{e.side}:{e.mint}:{int(e.ts or 0)}"
            prev = by.get(key)
            if prev is None:
                by[key] = e
                continue
            if e.mint and not prev.mint:
                if e.sol_amount <= 0:
                    e.sol_amount = prev.sol_amount
                by[key] = e
                continue
            # The bot row is max position. A larger wallet figure includes the
            # token-account deposit, which is not the buy.
            bot = e if e.source == "bot" and e.sol_amount > 0 else None
            if bot is None and prev.source == "bot" and prev.sol_amount > 0:
                bot = prev
            other = prev if bot is e else e
            if bot is not None and other.sol_amount > bot.sol_amount + 0.0002:
                if other.symbol and not bot.symbol:
                    bot.symbol = other.symbol
                if other.mint and not bot.mint:
                    bot.mint = other.mint
                if other.token_amount and not bot.token_amount:
                    bot.token_amount = other.token_amount
                by[key] = bot
                continue
            if e.sol_amount > prev.sol_amount:
                by[key] = e
        ordered = sorted(by.values(), key=lambda e: e.ts or 0.0, reverse=True)
        rows = []
        for e in ordered[:WALLET_SHOW]:
            item = e.to_dict()
            item["symbol"] = _ticker(e.mint, e.symbol)
            rows.append(item)
        return rows

    def _fills(self) -> List[ExecTx]:
        """Bot fills plus wallet parses. One row per signature.

        The panel merges both. A close that only looked at the wallet parse
        missed a bot sell the wallet RPC had not decoded yet.
        """
        by: Dict[str, ExecTx] = {}
        order: List[str] = []
        for entry in list(self.entries) + list(self.wallet_entries):
            if (entry.status or "") == "failed":
                continue
            sig = (entry.signature or "").strip()
            key = sig or f"{entry.side}:{entry.mint}:{int(float(entry.ts or 0))}:{entry.sol_amount}"
            prev = by.get(key)
            if prev is None:
                by[key] = entry
                order.append(key)
                continue
            prev_ok = bool(prev.mint) and float(prev.sol_amount or 0) > 0
            new_ok = bool(entry.mint) and float(entry.sol_amount or 0) > 0
            if new_ok and (not prev_ok or (entry.source == "bot" and prev.source != "bot")):
                by[key] = entry
            elif not prev.mint and entry.mint:
                by[key] = entry
        return [by[key] for key in order]

    def confirmed_exit(self, mint: str, since_ts: float, held_tokens: float = 0.0) -> Optional[Dict[str, Any]]:
        """A wallet SELL of this mint after the buy. None if history does not show the bag is gone.

        The chain clock can be a little earlier than the book time, so 20s of skew is allowed.
        A partial sale is not a full exit. A sell from before this buy is not this trade.
        """
        mint = (mint or "").strip()
        if not mint or since_ts < 1_000_000_000:
            return None
        sells = []
        buys = []
        for e in self._fills():
            if (e.mint or "") != mint or (e.status or "") == "failed":
                continue
            if float(e.ts or 0) < since_ts - 20:
                continue
            side = (e.side or "").upper()
            if side == "SELL":
                sells.append(e)
            elif side == "BUY":
                buys.append(e)
        if not sells:
            return None
        if buys:
            buy_ts = max(float(b.ts or 0) for b in buys)
            after = [s for s in sells if float(s.ts or 0) + 15 >= buy_ts]
            if after:
                sells = after
            else:
                return None
        sold_tokens = sum(float(s.token_amount or 0) for s in sells)
        sold_sol = sum(float(s.sol_amount or 0) for s in sells)
        buy_tokens = sum(float(b.token_amount or 0) for b in buys)
        covered = _same_bag(sold_tokens, buy_tokens) or _same_bag(sold_tokens, held_tokens)
        if not covered or sold_sol <= 0:
            return None
        last = max(sells, key=lambda s: float(s.ts or 0))
        return {
            "sol": sold_sol,
            "tokens": sold_tokens,
            "sig": last.signature or "",
            "ts": float(last.ts or 0),
        }

    def _append(self, e: ExecTx) -> None:
        self.entries.append(e)
        if len(self.entries) > self.max_memory:
            self.entries = self.entries[-self.max_memory :]
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(e.to_dict()) + "\n")

    def record(
        self,
        *,
        side: str,
        mint: str = "",
        symbol: str = "",
        sol_amount: float = 0.0,
        token_amount: float = 0.0,
        signature: str = "",
        status: str = "submitted",
        route: str = "",
        reason: str = "",
        mode: str = "live",
        source: str = "bot",
        ts: Optional[float] = None,
    ) -> ExecTx:
        sig = (signature or "").strip()
        if not symbol or symbol in (mint[:4], mint[:6], mint[:8], "SPL", "spl"):
            try:
                from src.execution.wallet_tokens import cached_label
                found = cached_label(mint).get("symbol") or ""
                if found and found not in ("SPL", "spl"):
                    symbol = found
            except Exception:
                pass
        if not symbol or symbol in (mint[:4], mint[:6], mint[:8], "SPL", "spl"):
            try:
                from src.observability.ledger import get_ledger
                for old in reversed(get_ledger().entries):
                    if old.mint == mint and old.symbol and old.symbol not in (mint[:4], mint[:6], mint[:8], "SPL"):
                        symbol = old.symbol
                        break
            except Exception:
                pass
        solscan = f"https://solscan.io/tx/{sig}" if _is_valid_tx_sig(sig) else ""
        e = ExecTx(
            ts=float(ts) if ts else time.time(),
            side=side.upper(),
            mint=mint,
            symbol=symbol or (mint[:8] if mint else ""),
            sol_amount=sol_amount,
            token_amount=token_amount,
            signature=sig,
            status=status,
            route=route,
            reason=reason,
            mode=mode,
            solscan_tx=solscan,
            solscan_token=f"https://solscan.io/token/{mint}" if mint else "",
            source=source,
        )
        self._append(e)
        log.info(
            "EXEC %s %s sol=%.6f status=%s sig=%s",
            e.side,
            e.symbol or e.mint[:8],
            e.sol_amount,
            e.status,
            (e.signature or "")[:16],
        )
        return e

    def add_rent(self, mint: str, sol: float) -> None:
        """Add returned token-account rent onto the latest sell of this mint. Once."""
        mint = (mint or "").strip()
        sol = float(sol or 0)
        if not mint or sol <= 0:
            return
        hit = None
        for e in sorted(self.entries, key=lambda row: row.ts or 0.0, reverse=True):
            if (e.mint or "") == mint and (e.side or "").upper() == "SELL" and "rent_back" not in (e.reason or ""):
                hit = e
                break
        if hit is None:
            return
        hit.sol_amount = float(hit.sol_amount or 0) + sol
        hit.reason = ((hit.reason or "sell") + " rent_back").strip()
        try:
            ordered = sorted(self.entries, key=lambda row: row.ts or 0.0)
            self.path.write_text(
                "".join(json.dumps(row.to_dict()) + "\n" for row in ordered[-self.max_memory :]),
                encoding="utf-8",
            )
        except Exception:
            log.debug("rent history rewrite failed", exc_info=True)

    def recent(self, n: int = 10) -> List[Dict[str, Any]]:
        ordered = sorted(self.entries, key=lambda e: e.ts or 0.0)
        return [e.to_dict() for e in ordered[-n:]]

    def merge_onchain(self, rows: List[Dict[str, Any]]) -> None:
        """Merge wallet transactions. Refresh a row when a later parse has the symbol and amounts."""
        by_sig = {e.signature: e for e in self.entries if e.signature}
        for r in rows:
            sig = (r.get("signature") or "").strip()
            if not sig:
                continue
            existing = by_sig.get(sig)
            if existing:
                symbol = r.get("symbol") or ""
                sol_amount = float(r.get("sol_amount") or 0)
                token_amount = float(r.get("token_amount") or 0)
                richer = bool(symbol) and not existing.symbol
                richer = richer or (sol_amount > 0 and existing.sol_amount <= 0)
                richer = richer or (token_amount > 0 and existing.token_amount <= 0)
                if not richer:
                    continue
                if symbol:
                    existing.symbol = symbol
                if r.get("mint"):
                    existing.mint = r["mint"]
                    existing.solscan_token = f"https://solscan.io/token/{r['mint']}"
                if sol_amount > 0:
                    existing.sol_amount = sol_amount
                if token_amount > 0:
                    existing.token_amount = token_amount
                if r.get("side") and r["side"] != "ONCHAIN":
                    existing.side = r["side"]
                if r.get("status"):
                    existing.status = r["status"]
                continue
            self.record(
                side=r.get("side") or "ONCHAIN",
                mint=r.get("mint") or "",
                symbol=r.get("symbol") or "",
                sol_amount=float(r.get("sol_amount") or 0),
                token_amount=float(r.get("token_amount") or 0),
                signature=sig,
                status=r.get("status") or "onchain",
                route="wallet",
                reason=r.get("reason") or "wallet_history",
                mode="live",
                source="wallet",
                ts=r.get("block_time") or None,
            )


_hist: Optional[ExecutionHistory] = None


def get_exec_history() -> ExecutionHistory:
    global _hist
    if _hist is None:
        _hist = ExecutionHistory()
    return _hist


def _same_bag(sold: float, reference: float) -> bool:
    """True when two token amounts are the same bag, including a raw-vs-ui scale."""
    if sold <= 0 or reference <= 0:
        return False
    ratio = sold / reference
    if 0.8 <= ratio <= 1.2:
        return True
    for scale in (1e3, 1e6, 1e9):
        if 0.8 <= ratio * scale <= 1.2 or 0.8 <= ratio / scale <= 1.2:
            return True
    return False


_parsed_cache: Dict[str, Dict[str, Any]] = {}
_symbol_cache: Dict[str, str] = {}
_WSOL = "So11111111111111111111111111111111111111112"


def _walk_mints(obj: Any):
    if isinstance(obj, dict):
        mint = obj.get("mint")
        if isinstance(mint, str) and 32 <= len(mint) <= 44 and mint != _WSOL:
            yield mint
        for v in obj.values():
            yield from _walk_mints(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_mints(v)


async def _symbol_for(client, mint: str) -> str:
    if not mint:
        return ""
    if mint in _symbol_cache:
        return _symbol_cache[mint]
    symbol = mint[:6]
    try:
        r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=3.0)
        if r.status_code == 200:
            for pair in (r.json() or {}).get("pairs") or []:
                base = pair.get("baseToken") or {}
                if base.get("address") == mint and base.get("symbol"):
                    symbol = base["symbol"]
                    break
            else:
                pairs = (r.json() or {}).get("pairs") or []
                if pairs:
                    symbol = ((pairs[0].get("baseToken") or {}).get("symbol")) or symbol
    except Exception:
        pass
    _symbol_cache[mint] = symbol
    return symbol


async def _parse_wallet_tx(client, url: str, sig: str, pubkey: str) -> Dict[str, Any]:
    cached = _parsed_cache.get(sig)
    if cached and cached.get("side") in ("BUY", "SELL", "FEE"):
        return cached
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [sig, {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed",
        }],
    }
    try:
        r = await client.post(url, json=payload)
        res = (r.json() or {}).get("result") if r.status_code == 200 else None
    except Exception:
        return {}
    if not res:
        return {}
    meta = res.get("meta") or {}
    msg = ((res.get("transaction") or {}).get("message") or {})
    pubs = []
    for k in msg.get("accountKeys") or []:
        pubs.append(k if isinstance(k, str) else (k.get("pubkey") or ""))
    sol_delta = 0.0
    user_i = -1
    if pubkey in pubs:
        user_i = pubs.index(pubkey)
        pre_b = meta.get("preBalances") or []
        post_b = meta.get("postBalances") or []
        if user_i < len(pre_b) and user_i < len(post_b):
            sol_delta = (int(post_b[user_i]) - int(pre_b[user_i])) / 1_000_000_000
    else:
        pre_b = meta.get("preBalances") or []
        post_b = meta.get("postBalances") or []
    fee_sol = int(meta.get("fee") or 0) / 1_000_000_000
    if meta.get("err") and fee_sol > 0 and abs(sol_delta) < 1e-9:
        sol_delta = -fee_sol
    # A new token account locks about 0.00204 SOL. That is a deposit, not the buy.
    # It comes back when the coin is sold and the account closes. The trade is
    # max position plus the network fee only.
    locked = 0.0
    for i, pre in enumerate(pre_b):
        if i == user_i or i >= len(post_b):
            continue
        pre_v = int(pre or 0)
        post_v = int(post_b[i] or 0)
        if pre_v == 0 and post_v > 0:
            locked += post_v / 1_000_000_000
        elif pre_v > 0 and post_v == 0:
            locked += pre_v / 1_000_000_000
    trade_sol = abs(sol_delta) - locked
    if trade_sol < 0.00002:
        trade_sol = 0.0

    def token_bals(key: str) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for b in meta.get(key) or []:
            if (b.get("owner") or "") != pubkey:
                continue
            mint = b.get("mint") or ""
            if not mint or mint == _WSOL:
                continue
            ui = ((b.get("uiTokenAmount") or {}).get("uiAmount")) or 0
            out[mint] = out.get(mint, 0.0) + float(ui or 0)
        return out

    pre_t = token_bals("preTokenBalances")
    post_t = token_bals("postTokenBalances")
    best_mint = ""
    best_delta = 0.0
    for mint in set(pre_t) | set(post_t):
        delta = post_t.get(mint, 0.0) - pre_t.get(mint, 0.0)
        if abs(delta) > abs(best_delta):
            best_mint, best_delta = mint, delta
    if not best_mint:
        for mint in _walk_mints(msg.get("instructions") or []):
            best_mint = mint
            break
    if best_delta > 0:
        side = "BUY"
    elif best_delta < 0:
        side = "SELL"
    elif sol_delta < -0.00001:
        side = "BUY" if best_mint else "FEE"
    elif sol_delta > 0.00001:
        side = "SELL" if best_mint else "FEE"
    else:
        side = "ONCHAIN"
    symbol = await _symbol_for(client, best_mint) if best_mint else ""
    parsed = {
        "signature": sig,
        "side": side,
        "status": "failed" if meta.get("err") else "confirmed",
        "mint": best_mint,
        "symbol": symbol,
        "sol_amount": trade_sol,
        "token_amount": abs(best_delta),
        "block_time": res.get("blockTime"),
        "reason": "wallet_swap" if side in ("BUY", "SELL") else "wallet_tx",
    }
    if best_mint or parsed["sol_amount"] > 0 or parsed["side"] in ("BUY", "SELL", "FEE"):
        _parsed_cache[sig] = parsed
    elif parsed["status"] == "failed":
        _parsed_cache[sig] = parsed
    return parsed


async def _rpc_endpoints() -> List[str]:
    """Public RPCs for wallet history. The free primary is not in this list."""
    from src.execution.wallet_tokens import _rpc_urls
    return _rpc_urls()


async def list_wallet_signatures(pubkey: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Newest wallet signatures. Public RPCs first. Paid only if those return nothing."""
    import asyncio
    from urllib.parse import urlsplit

    if not pubkey:
        return []
    limit = max(1, min(int(limit or 50), WALLET_KEEP))
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [pubkey, {"limit": limit, "commitment": "confirmed"}],
    }
    from src.execution.wallet_tokens import primary_url
    public = [url for url in await _rpc_endpoints() if url][:3]
    paid = primary_url()

    def _host(url: str) -> str:
        return (urlsplit(url).hostname or "rpc")[:40]

    async def ask(url: str):
        host = _host(url)
        try:
            from src.execution.tx_route import rpc_post
            status, text = await asyncio.to_thread(rpc_post, url, payload, 4.0)
        except Exception as e:
            log.info("EXEC wallet list failed %s: %s", host, type(e).__name__)
            return None
        if status != 200:
            log.info("EXEC wallet list HTTP %s %s", status, host)
            return None
        try:
            body = json.loads(text or "{}")
        except Exception:
            log.info("EXEC wallet list bad json %s", host)
            return None
        if body.get("error"):
            log.info("EXEC wallet list %s: %s", host, body.get("error"))
            return None
        value = body.get("result") or []
        if isinstance(value, list) and value:
            return value
        return None

    async def first(urls: List[str]):
        if not urls:
            return None
        tasks = [asyncio.create_task(ask(url)) for url in urls]
        try:
            for task in asyncio.as_completed(tasks):
                found = await task
                if found:
                    for other in tasks:
                        if not other.done():
                            other.cancel()
                    return found
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
        return None

    if not public and not paid:
        log.warning("EXEC wallet list has no RPC")
        return []
    found = await first(public)
    if not found and paid and paid not in public:
        found = await first([paid])
    if not found:
        log.info("EXEC wallet list empty from every RPC")
        return []
    return found[:limit]


async def build_wallet_rows(pubkey: str, limit: int = 50, parse_budget: int = 12):
    """Latest wallet signatures, shown immediately. A few are decoded each pass."""
    import asyncio
    import httpx

    items = await list_wallet_signatures(pubkey, limit)
    if not items:
        return [], None
    sigs = [item.get("signature") or "" for item in items if item.get("signature")][:WALLET_KEEP]
    meta = {item.get("signature") or "": item for item in items}

    def stub(item: Dict[str, Any]) -> Dict[str, Any]:
        sig = item.get("signature") or ""
        return {
            "signature": sig,
            "side": "TX",
            "status": "failed" if item.get("err") else "confirmed",
            "mint": "",
            "symbol": "",
            "sol_amount": 0.0,
            "token_amount": 0.0,
            "block_time": item.get("blockTime") or 0,
            "reason": "wallet_tx",
        }

    rows: Dict[str, Dict[str, Any]] = {}
    pending: List[str] = []
    for sig in sigs:
        cached = _parsed_cache.get(sig)
        if cached and cached.get("side") in ("BUY", "SELL", "FEE"):
            rows[sig] = cached
            continue
        failed = bool((meta.get(sig) or {}).get("err"))
        if not failed:
            rows[sig] = stub(meta.get(sig) or {"signature": sig})
        if len(pending) < parse_budget and (failed or sig not in rows or rows[sig].get("symbol") in ("", None)):
            pending.append(sig)
    if pending:
        from src.execution.wallet_tokens import primary_url
        endpoints: List[str] = []
        paid = primary_url()
        if paid:
            endpoints.append(paid)
        for url in await _rpc_endpoints():
            if url and url not in endpoints:
                endpoints.append(url)
            if len(endpoints) >= 2:
                break
        if not endpoints:
            endpoints = ["https://api.mainnet-beta.solana.com"]

        async def parse_one(sig: str) -> Dict[str, Any]:
            item = meta.get(sig) or {}
            async with httpx.AsyncClient(timeout=2.5) as client:
                for url in endpoints:
                    parsed = await _parse_wallet_tx(client, url, sig, pubkey)
                    if parsed and parsed.get("status") == "failed" and not parsed.get("mint"):
                        return {}
                    if parsed and parsed.get("side") not in ("", "ONCHAIN", "TX"):
                        parsed["block_time"] = parsed.get("block_time") or item.get("blockTime")
                        return parsed
                    if parsed and parsed.get("status") == "failed":
                        if parsed.get("mint") or float(parsed.get("sol_amount") or 0) > 0:
                            parsed["block_time"] = parsed.get("block_time") or item.get("blockTime")
                            return parsed
                        return {}
            return {}

        parsed = await asyncio.gather(*(parse_one(sig) for sig in pending), return_exceptions=True)
        for row in parsed:
            if isinstance(row, dict) and row.get("signature") and row.get("side") not in ("", "ONCHAIN"):
                rows[row["signature"]] = row
    ordered = [rows[sig] for sig in sigs if sig in rows]
    return ordered, sigs


async def fetch_wallet_signatures(pubkey: str, limit: int = 50) -> List[Dict[str, Any]]:
    rows, _sigs = await build_wallet_rows(pubkey, limit=limit, parse_budget=12)
    return rows
