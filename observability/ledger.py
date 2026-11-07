
"""
Trade ledger — fee-aware cost basis and PnL.

PnL (SOL) = exit_sol_received - entry_sol_spent - entry_fees - exit_fees - tips
Only positive when proceeds exceed full cost (including gas/tips).
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger("sol-sniper.ledger")

ROOT = Path(__file__).resolve().parents[2]
LEDGER_PATH = ROOT / "logs" / "ledger.jsonl"
SOL_USD_FALLBACK = 150.0  # display only when no live price

def is_valid_solana_tx_sig(sig: str) -> bool:
    """Solana tx signatures are base58, typically 87-88 chars. Reject Jito bundle ids / paper."""
    if not sig or not isinstance(sig, str):
        return False
    s = sig.strip()
    if s.startswith("paper") or s.startswith("jito:"):
        return False
    # bundle ids sometimes look similar; require base58 alphabet + length
    if not (80 <= len(s) <= 90):
        return False
    alphabet = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    if any(c not in alphabet for c in s):
        return False
    return True


def solscan_tx_url(sig: str) -> str:
    if is_valid_solana_tx_sig(sig):
        return f"https://solscan.io/tx/{sig.strip()}"
    return ""


@dataclass
class LedgerEntry:
    id: str
    ts: float
    side: str  # OPEN | CLOSE
    mint: str
    symbol: str = ""
    mode: str = "paper"
    # OPEN fields
    token_amount: float = 0.0
    entry_sol: float = 0.0          # SOL spent on swap notional
    entry_fee_sol: float = 0.0      # estimated/priority/network + paper fee
    entry_tip_sol: float = 0.0
    price_sol_per_token: float = 0.0
    # CLOSE fields
    exit_sol: float = 0.0           # SOL received from sell
    exit_fee_sol: float = 0.0
    pnl_sol: float = 0.0            # net after all costs
    pnl_usd: float = 0.0
    cost_basis_sol: float = 0.0     # entry_sol + entry_fee + tip
    hold_seconds: float = 0.0
    reason: str = ""
    source: str = ""
    since_buy_pct: Optional[float] = None
    ath_since_pct: Optional[float] = None
    sale_label: str = ""
    group_id: str = ""
    signature: str = ""
    solscan_tx: str = ""
    solscan_token: str = ""
    sol_usd: float = SOL_USD_FALLBACK
    m5_pct: Optional[float] = None
    m1_pct: Optional[float] = None
    s1_pct: Optional[float] = None
    h1_pct: Optional[float] = None
    price_usd: Optional[float] = None
    book_locked: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _mark(value: Any) -> Optional[float]:
    """A real percent or price. A missing read stays missing. A label is not a number."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        text = value.strip().rstrip("%")
        if not text or text.lower() in {"m5", "m1", "s1", "h1", "price_usd", "none", "nan"}:
            return None
        value = text
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


KEEP = 250
SGT = 8 * 3600


def _sgt_day(ts: float) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(float(ts) + SGT))


def _source_name(source: str) -> str:
    s = (source or "").strip().lower()
    if s in ("launch", "dayscalp"):
        return s
    if "launch" in s:
        return "launch"
    if "dayscalp" in s or s in ("day_scalp", "day-scalp"):
        return "dayscalp"
    return s


class TradeLedger:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or LEDGER_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.entries: List[LedgerEntry] = []
        self._total = 0.0
        self._days: Dict[str, float] = {}
        self._seen: List[str] = []
        self._live = 0.0
        self._live_saved = 0.0
        self._live_saved_at = 0.0
        self._wins = 0
        self._losses = 0
        self._base_30 = 0.0
        self._base_total = 0.0
        self._epoch = 0.0
        self._cut_30 = 0.0
        self._cut_total = 0.0
        self._kept_30 = 0.0
        self._kept_total = 0.0
        self._closed_n = 0
        self._wr_ready = False
        self._chain_rows: List[Dict[str, Any]] = []
        self._open_dirty: set = set()
        self._open_note_at: Dict[str, float] = {}
        self._load()
        self._load_remote()
        self._load_book()
        self._drop_before_epoch()
        self._replay_closes()
        saved = self._saved_close_count()
        if saved > self._closed_n:
            self._closed_n = saved
            self._save_book(force=True)
        self.apply_known_fees()

    def _entry_from(self, d: Dict[str, Any]) -> LedgerEntry:
        if isinstance(d.get("payload"), dict):
            merged = dict(d["payload"])
            merged.update({k: v for k, v in d.items() if k != "payload" and v is not None})
            d = merged
        return LedgerEntry(**{k: d[k] for k in LedgerEntry.__dataclass_fields__ if k in d})

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                d = json.loads(line)
                self.entries.append(self._entry_from(d))
        except Exception as e:
            log.warning("ledger load: %s", e)
        self._trim_local()

    def _load_remote(self) -> None:
        try:
            from src.storage.supabase_store import get_store
            rows = get_store().fetch_ledger(100)
            try:
                get_store()._trim_ledger(100)
            except Exception:
                log.debug("ledger trim skipped", exc_info=True)
        except Exception:
            log.debug("ledger remote load skipped", exc_info=True)
            return
        if rows is None:
            return
        if rows:
            self.entries = [self._entry_from(r) for r in rows]
            self.entries.sort(key=lambda e: float(e.ts or 0))
            self._rewrite()
            log.info("LEDGER loaded %d rows from SQLite", len(self.entries))

    def _rewrite(self) -> None:
        try:
            text = "".join(json.dumps(e.to_dict()) + "\n" for e in self.entries[-KEEP:])
            self.path.write_text(text, encoding="utf-8")
        except Exception:
            log.debug("ledger rewrite failed", exc_info=True)

    def _trim_local(self) -> None:
        if len(self.entries) <= KEEP:
            return
        self.entries = self.entries[-KEEP:]
        self._rewrite()

    def _book_path(self) -> Path:
        return self.path.parent / "pnl_board.json"

    def _read_local_book(self) -> Dict[str, Any]:
        path = self._book_path()
        if not path.is_file():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            log.debug("local pnl board read failed", exc_info=True)
            return {}

    def _load_book(self) -> None:
        local = self._read_local_book()
        remote: Dict[str, Any] = {}
        try:
            from src.storage.supabase_store import get_store
            got = get_store().get("pnl_board")
            if isinstance(got, dict):
                remote = got
        except Exception:
            log.debug("pnl board read skipped", exc_info=True)
        if remote and local:
            book = remote if float(remote.get("updated_at") or 0) >= float(local.get("updated_at") or 0) else local
        else:
            book = remote or local
        if not book:
            return
        try:
            self._total = float(book.get("total_pnl_sol") or 0)
        except (TypeError, ValueError):
            self._total = 0.0
        days = book.get("days") or {}
        if isinstance(days, dict):
            clean: Dict[str, float] = {}
            for k, v in days.items():
                try:
                    clean[str(k)] = float(v)
                except (TypeError, ValueError):
                    continue
            self._days = clean
        seen = book.get("seen") or []
        if isinstance(seen, list):
            self._seen = [str(x) for x in seen][-400:]
        try:
            self._live = float(book.get("live_pnl_sol") or 0)
            self._live_saved = self._live
        except (TypeError, ValueError):
            self._live = 0.0
        try:
            self._wins = int(book.get("wins") or 0)
            self._losses = int(book.get("losses") or 0)
        except (TypeError, ValueError):
            self._wins = 0
            self._losses = 0
        try:
            self._base_30 = float(book.get("baseline_30d") or 0)
            self._base_total = float(book.get("baseline_total") or 0)
            self._epoch = float(book.get("session_epoch") or 0)
            self._cut_30 = float(book.get("cut_30") or 0)
            self._cut_total = float(book.get("cut_total") or 0)
            self._kept_30 = float(book.get("kept_30") or 0)
            self._kept_total = float(book.get("kept_total") or 0)
            self._closed_n = int(book.get("closed_n") or 0)
        except (TypeError, ValueError):
            self._base_30 = 0.0
            self._base_total = 0.0
            self._epoch = 0.0
        self._adopt_old_clears(book)
        self._wr_ready = "wins" in book
        board = self.pnl_board()
        log.info(
            "PNL board restored daily=%+.4f 30d=%+.4f total=%+.4f live=%+.4f wins=%d losses=%d",
            board["pnl_daily_sol"], board["pnl_30d_sol"], board["pnl_total_sol"], board["pnl_live_sol"],
            board["win_wins"], board["win_losses"],
        )

    def _adopt_old_clears(self, book: Dict[str, Any]) -> None:
        """A clear saved the old way (a baseline) must still read as zero after restart."""
        now = time.time()
        start = _sgt_day(now - 29 * 86400)
        rolling = sum(float(v) for k, v in self._days.items() if str(k) >= start)
        stamped = float(book.get("updated_at") or now)
        if not self._cut_30 and abs(self._base_30) > 1e-9 and abs(rolling + self._base_30) < 1e-4:
            self._cut_30 = stamped
            self._kept_30 = 0.0
            log.warning("PNL 30-day clear kept from the saved baseline")
        if not self._cut_total and abs(self._base_total) > 1e-9 and abs(self._total + self._base_total) < 1e-4:
            self._cut_total = stamped
            self._kept_total = 0.0
            log.warning("PNL total clear kept from the saved baseline")

    def _drop_before_epoch(self) -> None:
        """A cleared session must stay cleared. Older rows cannot come back on restart."""
        epoch = float(self._epoch or 0)
        if epoch <= 0:
            return
        kept = [e for e in self.entries if float(e.ts or 0) >= epoch]
        if len(kept) != len(self.entries):
            log.warning("LEDGER dropped %d rows from before the session clear", len(self.entries) - len(kept))
            self.entries = kept
            self._rewrite()

    def _replay_closes(self) -> None:
        added = False
        for e in self.entries:
            if e.side == "CLOSE" and e.id and e.id not in self._seen:
                self._apply_close(e, save=False)
                added = True
        if added:
            self._save_book(force=True)
        if not self._wr_ready:
            wins = losses = 0
            for e in self.entries:
                if e.side != "CLOSE":
                    continue
                if float(e.pnl_sol or 0) > 0:
                    wins += 1
                else:
                    losses += 1
            self._wins = wins
            self._losses = losses
            self._wr_ready = True
            self._save_book(force=True)

    def _apply_close(self, e: LedgerEntry, save: bool = True) -> None:
        if e.side != "CLOSE" or not e.id or e.id in self._seen:
            return
        self._seen.append(e.id)
        self._seen = self._seen[-400:]
        self._total += float(e.pnl_sol or 0)
        day = _sgt_day(e.ts or time.time())
        self._days[day] = float(self._days.get(day, 0) or 0) + float(e.pnl_sol or 0)
        cutoff = _sgt_day(time.time() - 40 * 86400)
        self._days = {k: v for k, v in self._days.items() if k >= cutoff}
        if float(e.pnl_sol or 0) > 0:
            self._wins += 1
        else:
            self._losses += 1
        if save:
            self._save_book(force=True)

    def _save_book(self, force: bool = False) -> None:
        board = self.pnl_board()
        payload = {
            "total_pnl_sol": self._total,
            "live_pnl_sol": self._live,
            "wins": self._wins,
            "losses": self._losses,
            "days": self._days,
            "seen": self._seen[-400:],
            "baseline_30d": self._base_30,
            "baseline_total": self._base_total,
            "session_epoch": self._epoch,
            "cut_30": self._cut_30,
            "cut_total": self._cut_total,
            "kept_30": self._kept_30,
            "kept_total": self._kept_total,
            "closed_n": self._closed_n,
            "board": board,
            "updated_at": time.time(),
        }
        try:
            self._book_path().write_text(json.dumps(payload), encoding="utf-8")
        except Exception:
            log.debug("local pnl board write failed", exc_info=True)
        try:
            from src.storage.supabase_store import get_store
            ok = get_store().put("pnl_board", payload)
            if ok:
                log.info(
                    "PNL daily=%+.4f live=%+.4f 30d=%+.4f total=%+.4f",
                    board["pnl_daily_sol"], board["pnl_live_sol"],
                    board["pnl_30d_sol"], board["pnl_total_sol"],
                )
            elif force:
                log.warning("PNL write missed — local ledger still has the close")
        except Exception:
            log.warning("PNL write failed", exc_info=True)

    def set_live(self, sol: float) -> Dict[str, Any]:
        sol = float(sol or 0)
        now = time.time()
        moved = abs(sol - self._live_saved) >= 0.00005 or (now - self._live_saved_at) >= 3
        self._live = sol
        if moved:
            self._live_saved = sol
            self._live_saved_at = now
            self._save_book()
        return self.pnl_board()

    def clear_session(self) -> Dict[str, Any]:
        """Wipe the trade list, daily, live, and win rate. Keep 30-day and total."""
        shown = self.pnl_board()
        self._kept_30 = 0.0 if self._cut_30 else float(shown["pnl_30d_sol"])
        self._kept_total = 0.0 if self._cut_total else float(shown["pnl_total_sol"])
        self._base_30 = 0.0
        self._base_total = 0.0
        self.entries = []
        self._rewrite()
        self._days = {}
        self._total = 0.0
        self._live = 0.0
        self._live_saved = 0.0
        self._wins = 0
        self._losses = 0
        self._seen = []
        self._closed_n = 0
        self._epoch = time.time()
        self._wr_ready = True
        try:
            from src.storage.supabase_store import get_store
            get_store().delete_ledger()
        except Exception:
            log.warning("ledger remote clear failed", exc_info=True)
        self._save_book(force=True)
        try:
            from src.storage.capital_pnl import pin_session_cards, restart_daily
            restart_daily()
            pin_session_cards(self._kept_30, self._kept_total)
        except Exception:
            log.debug("daily restart failed", exc_info=True)
        log.warning("LEDGER session cleared — daily restarted, 30d and total kept")
        return self.pnl_board()

    def clear_30d(self) -> Dict[str, Any]:
        """Zero only the 30-day number. Older closes never count toward it again."""
        self._cut_30 = time.time()
        self._kept_30 = 0.0
        self._base_30 = 0.0
        self._save_book(force=True)
        try:
            from src.storage.capital_pnl import mark_30d_cleared, pin_cut
            mark_30d_cleared()
            pin_cut("30d")
        except Exception:
            log.debug("30d capital mark failed", exc_info=True)
        log.warning("PNL 30-day cleared")
        return self.pnl_board()

    def clear_total(self) -> Dict[str, Any]:
        """Zero only the all-time number. Older closes never count toward it again."""
        self._cut_total = time.time()
        self._kept_total = 0.0
        self._base_total = 0.0
        self._save_book(force=True)
        try:
            from src.storage.capital_pnl import pin_cut
            pin_cut("total")
        except Exception:
            log.debug("total capital pin failed", exc_info=True)
        log.warning("PNL total cleared")
        return self.pnl_board()

    def set_chain_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Latest wallet transactions. These are the SOL amounts on Solscan."""
        self._chain_rows = list(rows or [])

    def _chain_cash(self) -> List[Dict[str, Any]]:
        """Each sell, priced from the wallet balance change, not the planned size.

        Cost is the SOL that left the wallet on the buy. Proceeds are the SOL
        that came back on the sell. A separate fee transaction is subtracted once.
        """
        rows = getattr(self, "_chain_rows", None) or []
        buys: Dict[str, List[Dict[str, float]]] = {}
        sells: List[Dict[str, Any]] = []
        fees: List[Dict[str, float]] = []
        seen = set()
        for r in rows:
            sig = str(r.get("signature") or "")
            if sig and sig in seen:
                continue
            if sig:
                seen.add(sig)
            if str(r.get("status") or "") == "failed":
                continue
            side = str(r.get("side") or "").upper()
            try:
                sol = float(r.get("sol_amount") or 0)
                tokens = float(r.get("token_amount") or 0)
                ts = float(r.get("block_time") or r.get("ts") or 0)
            except (TypeError, ValueError):
                continue
            mint = str(r.get("mint") or "")
            if side == "FEE" and sol > 0 and ts > 0:
                fees.append({"ts": ts, "sol": sol, "used": 0.0})
            elif side == "BUY" and mint and ts > 0 and sol > 0:
                buys.setdefault(mint, []).append({"ts": ts, "sol": sol, "tokens": max(0.0, tokens)})
            elif side == "SELL" and mint and ts > 0 and (sol > 0 or tokens > 0):
                sells.append({
                    "mint": mint,
                    "ts": ts,
                    "sol": sol,
                    "tokens": max(0.0, tokens),
                    "symbol": r.get("symbol") or "",
                })
        for mint in buys:
            buys[mint].sort(key=lambda b: b["ts"])
        sells.sort(key=lambda s: s["ts"])
        out: List[Dict[str, Any]] = []
        for sell in sells:
            lots = [b for b in buys.get(sell["mint"], []) if b["ts"] <= sell["ts"] + 30 and b["sol"] > 0.0000001]
            need = sell["tokens"]
            cost = 0.0
            if need > 0 and lots:
                for lot in lots:
                    if need <= 0.0000001:
                        break
                    lot_tokens = lot["tokens"] if lot["tokens"] > 0 else need
                    take = min(lot_tokens, need)
                    frac = (take / lot_tokens) if lot_tokens > 0 else 1.0
                    spend = lot["sol"] * frac
                    cost += spend
                    lot["sol"] = max(0.0, lot["sol"] - spend)
                    if lot["tokens"] > 0:
                        lot["tokens"] = max(0.0, lot["tokens"] - take)
                    need -= take
            elif lots and sell["sol"] > 0 and sell["tokens"] <= 0:
                cost = lots[-1]["sol"]
                lots[-1]["sol"] = 0.0
            elif not lots:
                opened = [
                    e for e in self.entries
                    if e.side == "OPEN" and e.mint == sell["mint"] and float(e.ts or 0) <= sell["ts"] + 30
                ]
                if opened and sell["tokens"] > 0 and float(opened[-1].token_amount or 0) > 0:
                    whole = float(opened[-1].entry_sol or 0) + float(opened[-1].entry_fee_sol or 0)
                    frac = min(1.0, sell["tokens"] / float(opened[-1].token_amount))
                    cost = whole * frac
                elif opened:
                    cost = float(opened[-1].entry_sol or 0) + float(opened[-1].entry_fee_sol or 0)
            fee = 0.0
            for item in fees:
                if item["used"] >= item["sol"]:
                    continue
                if abs(item["ts"] - sell["ts"]) <= 8 or any(abs(item["ts"] - b["ts"]) <= 8 for b in lots):
                    fee += item["sol"] - item["used"]
                    item["used"] = item["sol"]
            pnl = sell["sol"] - cost - fee
            out.append({
                "mint": sell["mint"],
                "symbol": sell["symbol"],
                "ts": sell["ts"],
                "pnl": pnl,
                "proceeds": sell["sol"],
                "cost": cost,
            })
        return out

    def _close_sol(self, e: LedgerEntry) -> float:
        """SOL after fees, the same figure the ledger row paints green or red."""
        exit_sol = float(e.exit_sol or 0)
        if exit_sol > 0:
            cost = float(e.entry_sol or 0) + float(e.entry_fee_sol or 0) + float(e.entry_tip_sol or 0)
            return exit_sol - cost - float(e.exit_fee_sol or 0)
        return float(e.pnl_sol or 0)

    def _win_loss(self, epoch: float) -> tuple:
        """One closed trade. A win is green SOL after fees, or a green % since buy."""
        groups: Dict[str, List[LedgerEntry]] = {}
        for e in self.entries:
            if e.side != "CLOSE" or float(e.ts or 0) < epoch:
                continue
            key = getattr(e, "group_id", "") or e.id
            groups.setdefault(key, []).append(e)
        wins = 0
        for rows in groups.values():
            sol = sum(self._close_sol(row) for row in rows)
            since = None
            for row in rows:
                if row.since_buy_pct is None:
                    continue
                try:
                    since = float(row.since_buy_pct)
                except (TypeError, ValueError):
                    continue
            usd = 0.0
            locked = False
            for row in rows:
                if getattr(row, "book_locked", False) and row.pnl_usd is not None:
                    locked = True
                    usd += float(row.pnl_usd or 0)
            if sol > 1e-9 or (since is not None and since > 0) or (locked and usd > 0):
                wins += 1
        losses = len(groups) - wins
        return wins, losses

    def pnl_board(self) -> Dict[str, Any]:
        now = time.time()
        today = _sgt_day(now)
        start = _sgt_day(now - 29 * 86400)
        epoch = float(self._epoch or 0)
        cut30 = float(self._cut_30 or 0)
        cut_total = float(self._cut_total or 0)
        daily = d30 = total = 0.0
        grouped: Dict[str, float] = {}
        chain = self._chain_cash()
        chain_mints = {c["mint"] for c in chain}
        for c in chain:
            ts = float(c["ts"] or 0)
            pnl = float(c["pnl"] or 0)
            day = _sgt_day(ts or now)
            if ts >= epoch and day == today:
                daily += pnl
            if ts >= cut30 and day >= start:
                d30 += pnl
            if ts >= cut_total:
                total += pnl
            if ts >= epoch:
                grouped[c["mint"] + ":" + str(int(ts))] = pnl
        for e in self.entries:
            if e.side != "CLOSE" or (e.mint and e.mint in chain_mints):
                continue
            ts = float(e.ts or 0)
            pnl = float(e.pnl_sol or 0)
            day = _sgt_day(ts or now)
            if ts >= epoch and day == today:
                daily += pnl
            if ts >= cut30 and day >= start:
                d30 += pnl
            if ts >= cut_total:
                total += pnl
            if ts >= epoch:
                grouped[getattr(e, "group_id", "") or e.id] = grouped.get(getattr(e, "group_id", "") or e.id, 0.0) + pnl
        if not cut30:
            d30 += float(self._kept_30 or 0)
        if not cut_total:
            total += float(self._kept_total or 0)
        wins, losses = self._win_loss(epoch)
        self._wins = wins
        self._losses = losses
        closed = wins + losses
        rate = (wins / closed) if closed else 0.0
        daily_out, d30_out, total_out = daily, d30, total
        try:
            from src.storage.capital_pnl import current_capital_pnl
            cap = current_capital_pnl()
        except Exception:
            cap = None
        if cap:
            daily_out = float(cap["pnl_daily_sol"])
            d30_out = float(cap["pnl_30d_sol"])
            total_out = float(cap["pnl_total_sol"])
        return {
            "pnl_daily_sol": round(daily_out, 6),
            "pnl_live_sol": round(float(self._live or 0), 6),
            "pnl_30d_sol": round(d30_out, 6),
            "pnl_total_sol": round(total_out, 6),
            "win_rate": round(rate, 4),
            "win_wins": wins,
            "win_losses": losses,
        }

    def session_trade_counts(self) -> Dict[str, Any]:
        """Buys, fills, and closes from the saved ledger. A restart does not zero them."""
        epoch = float(self._epoch or 0)
        opens = []
        closes = []
        for e in self.entries:
            if float(e.ts or 0) < epoch:
                continue
            if e.side == "OPEN":
                opens.append(e)
            elif e.side == "CLOSE":
                closes.append(e)
        nets: Dict[str, List[LedgerEntry]] = {}
        for c in closes:
            key = getattr(c, "group_id", "") or c.id
            nets.setdefault(key, []).append(c)
        covered = len({c.mint for c in closes if any(o.mint == c.mint and float(o.ts or 0) <= float(c.ts or 0) + 5 for o in opens)})
        buys = len(opens) + max(0, len(nets) - covered)
        wins, losses = self._win_loss(epoch)
        closed = wins + losses
        return {
            "buy_signals": buys,
            "fills": buys,
            "paper_fills": buys,
            "closed_trades": closed,
            "win_wins": wins,
            "win_losses": losses,
            "win_rate": (wins / closed) if closed else 0.0,
        }

    def _saved_close_count(self) -> int:
        """Closes that are already on disk, so a restart does not show 0."""
        epoch = float(self._epoch or 0)
        n = 0
        groups = set()
        for e in self.entries:
            if e.side == "CLOSE" and float(e.ts or 0) >= epoch:
                groups.add(getattr(e, "group_id", "") or e.id)
        n = len(groups)
        try:
            from datetime import datetime
            from src.config.runtime import TRADE_JOURNAL
            if TRADE_JOURNAL.is_file():
                journal = 0
                for line in TRADE_JOURNAL.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    raw = row.get("closed_at")
                    ts = 0.0
                    if isinstance(raw, (int, float)):
                        ts = float(raw)
                    elif isinstance(raw, str) and raw:
                        ts = datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
                    if ts >= epoch:
                        journal += 1
                n = max(n, journal)
        except Exception:
            pass
        return n

    def _row(self, e: LedgerEntry) -> Dict[str, Any]:
        return {
            "id": e.id,
            "ts": e.ts,
            "side": e.side,
            "mint": e.mint,
            "symbol": e.symbol,
            "source": e.source,
            "mode": e.mode,
            "token_amount": e.token_amount,
            "entry_sol": e.entry_sol,
            "entry_fee_sol": e.entry_fee_sol,
            "entry_tip_sol": e.entry_tip_sol,
            "exit_sol": e.exit_sol,
            "exit_fee_sol": e.exit_fee_sol,
            "pnl_sol": e.pnl_sol,
            "cost_basis_sol": e.cost_basis_sol,
            "hold_seconds": e.hold_seconds,
            "reason": e.reason,
            "since_buy_pct": e.since_buy_pct,
            "ath_since_pct": None if e.ath_since_pct is None else max(0.0, float(e.ath_since_pct)),
            "signature": e.signature,
            "payload": e.to_dict(),
        }

    def _push_remote(self, e: LedgerEntry) -> None:
        try:
            from src.storage.supabase_store import get_store
            ok = get_store().upsert_ledger(self._row(e))
            if ok:
                log.info("LEDGER %s %s source=%s", e.side, e.symbol, e.source or "-")
        except Exception:
            log.warning("LEDGER push failed", exc_info=True)

    def _append(self, e: LedgerEntry) -> None:
        self.entries.append(e)
        if len(self.entries) > KEEP:
            self.entries = self.entries[-KEEP:]
            self._rewrite()
        else:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(e.to_dict()) + "\n")
        self._push_remote(e)

    def drop_unfilled(self, mint: str) -> int:
        """Remove an OPEN that never put coins in the wallet. No CLOSE row is written."""
        kept = []
        dropped = 0
        for e in self.entries:
            if e.side == "OPEN" and e.mint == mint:
                dropped += 1
                continue
            kept.append(e)
        if not dropped:
            return 0
        self.entries = kept
        self._rewrite()
        log.warning("LEDGER drop unfilled OPEN mint=%s rows=%s — coins were never in the wallet", mint[:8], dropped)
        return dropped

    def fill_m5(self, mint: str, m5_pct: float, since_ts: float = 0.0) -> int:
        """Write the 5m reading onto this buy's rows that were still blank."""
        changed = []
        value = round(float(m5_pct), 2)
        for e in self.entries:
            if e.side != "OPEN" or e.mint != mint or e.m5_pct is not None:
                continue
            if float(e.ts or 0) + 1 < float(since_ts or 0):
                continue
            e.m5_pct = value
            changed.append(e)
        if not changed:
            return 0
        self._rewrite()
        for e in changed:
            self._push_remote(e)
        return len(changed)

    def fill_h1(self, mint: str, h1_pct: float, since_ts: float = 0.0) -> int:
        """Write the 1h reading onto this buy's rows that were still blank."""
        changed = []
        value = round(float(h1_pct), 2)
        for e in self.entries:
            if e.side != "OPEN" or e.mint != mint or e.h1_pct is not None:
                continue
            if float(e.ts or 0) + 1 < float(since_ts or 0):
                continue
            e.h1_pct = value
            changed.append(e)
        if not changed:
            return 0
        self._rewrite()
        for e in changed:
            self._push_remote(e)
        return len(changed)

    def fill_m1(self, mint: str, m1_pct: float, since_ts: float = 0.0) -> int:
        """Write the 1-minute candle onto this buy's rows that were still blank."""
        changed = []
        value = round(float(m1_pct), 2)
        for e in self.entries:
            if e.side != "OPEN" or e.mint != mint or e.m1_pct is not None:
                continue
            if float(e.ts or 0) + 1 < float(since_ts or 0):
                continue
            e.m1_pct = value
            changed.append(e)
        if not changed:
            return 0
        self._rewrite()
        for e in changed:
            self._push_remote(e)
        return len(changed)

    def fill_open_marks(
        self,
        mint: str,
        m5_pct: Any = None,
        m1_pct: Any = None,
        s1_pct: Any = None,
        h1_pct: Any = None,
        price_usd: Any = None,
    ) -> bool:
        """Keep the buy's numbers on the open row. Fills only what is still blank."""
        opens = [e for e in self.entries if e.side == "OPEN" and e.mint == mint]
        if not opens:
            return False
        e = opens[-1]
        changed = False
        for attr, raw in (
            ("m5_pct", m5_pct),
            ("m1_pct", m1_pct),
            ("s1_pct", s1_pct),
            ("h1_pct", h1_pct),
            ("price_usd", price_usd),
        ):
            current = getattr(e, attr)
            if current is not None and not (attr == "price_usd" and float(current or 0) <= 0):
                continue
            value = _mark(raw)
            if value is None or (attr == "price_usd" and value <= 0):
                continue
            setattr(e, attr, float(f"{value:.12g}") if attr == "price_usd" else round(value, 2))
            changed = True
        if not changed:
            return False
        self._rewrite()
        self._push_remote(e)
        return True

    def freeze_open_price(self, mint: str, price: float) -> bool:
        """One grab of the buy price. A locked row is never written again."""
        opens = [e for e in self.entries if e.side == "OPEN" and e.mint == mint]
        if not opens:
            return False
        e = opens[-1]
        if getattr(e, "book_locked", False):
            return False
        e.price_usd = round(float(price), 12)
        e.book_locked = True
        self._rewrite()
        self._push_remote(e)
        return True

    def saved_open_mark(self, mint: str, attr: str) -> Optional[float]:
        """The percent stored on this mint's latest OPEN row. Blank stays blank."""
        opens = [e for e in self.entries if e.side == "OPEN" and e.mint == mint]
        if not opens:
            return None
        return _mark(getattr(opens[-1], attr, None))

    def drop_latest_close(self, mint: str) -> bool:
        """The wallet still holds this coin, so the newest close was not a real sale."""
        closes = [e for e in self.entries if e.side == "CLOSE" and e.mint == mint]
        if not closes:
            return False
        last = closes[-1]
        self.entries = [e for e in self.entries if e is not last]
        self._rewrite()
        log.warning("LEDGER removed false close %s reason=%s — coins are still in the wallet", (mint or "")[:8], last.reason or "")
        return True

    def repair_book(self, held_mints) -> int:
        """One open per coin still held. One open and one real close once it is gone.

        A DUST row while the coins are still in the wallet is not a sale.
        """
        held = {str(m) for m in (held_mints or set()) if m}
        fake = {"DUST", "dust_left", "dust_unsellable", "not_in_wallet", "empty_wallet"}
        by: Dict[str, List[LedgerEntry]] = {}
        loose: List[LedgerEntry] = []
        for entry in self.entries:
            if entry.mint:
                by.setdefault(entry.mint, []).append(entry)
            else:
                loose.append(entry)
        kept: List[LedgerEntry] = list(loose)
        dropped = 0
        for mint, rows in by.items():
            rows.sort(key=lambda item: float(item.ts or 0))
            opens = [item for item in rows if item.side == "OPEN"]
            if mint in held:
                keep = next((item for item in opens if float(item.entry_sol or 0) > 0), None)
                if keep is None and opens:
                    keep = opens[-1]
                # A confirmed wallet sell is a real close. A late token read still
                # listing the mint must not delete it and leave only the OPEN.
                real_close = None
                try:
                    from src.observability.exec_history import get_exec_history
                    open_ts = float(keep.ts or 0) if keep else 0.0
                    tokens = float(keep.token_amount or 0) if keep else 0.0
                    sold = get_exec_history().confirmed_exit(mint, open_ts, tokens) if open_ts else None
                except Exception:
                    sold = None
                if sold:
                    real = [
                        item for item in rows
                        if item.side == "CLOSE"
                        and (item.reason or "") not in fake
                        and float(item.exit_sol or 0) > 0
                    ]
                    if real:
                        real_close = real[-1]
                for item in rows:
                    if item is keep or item is real_close:
                        kept.append(item)
                    else:
                        dropped += 1
                continue
            real = [
                item for item in rows
                if item.side == "CLOSE"
                and (item.reason or "") not in fake
                and float(item.exit_sol or 0) > 0
            ]
            if real:
                close = real[-1]
                keep = None
                for item in opens:
                    if float(item.entry_sol or 0) > 0 and float(item.ts or 0) <= float(close.ts or 0):
                        keep = item
                if keep is None and opens:
                    keep = opens[0]
                for item in rows:
                    if item is keep or item is close:
                        kept.append(item)
                    else:
                        dropped += 1
                continue
            keep = opens[-1] if opens else None
            for item in rows:
                if item is keep:
                    kept.append(item)
                else:
                    dropped += 1
        if not dropped:
            return 0
        self.entries = sorted(kept, key=lambda item: float(item.ts or 0))
        self._rewrite()
        log.warning("LEDGER repaired — removed %d fake rows", dropped)
        return dropped

    def note_open_progress(
        self,
        mint: str,
        since_buy_pct: Optional[float],
        ath_since_pct: Optional[float],
    ) -> None:
        """Keep the open row's % since buy and ATH current while that trade is still open."""
        opens = [e for e in self.entries if e.side == "OPEN" and e.mint == mint]
        if not opens:
            return
        e = opens[-1]
        since = None if since_buy_pct is None else round(float(since_buy_pct), 2)
        ath = None if ath_since_pct is None else round(max(0.0, float(ath_since_pct)), 2)
        if e.since_buy_pct != since or e.ath_since_pct != ath:
            e.since_buy_pct = since
            e.ath_since_pct = ath
            self._open_dirty.add(mint)
        if mint not in self._open_dirty:
            return
        now = time.time()
        if now - self._open_note_at.get(mint, 0.0) < 2.0:
            return
        self._open_note_at[mint] = now
        self._open_dirty.discard(mint)
        self._rewrite()

    def record_open(
        self,
        *,
        id: str,
        mint: str,
        symbol: str,
        mode: str,
        token_amount: float,
        entry_sol: float,
        entry_fee_sol: float,
        entry_tip_sol: float,
        price_sol_per_token: float,
        signature: str = "",
        sol_usd: float = SOL_USD_FALLBACK,
        source: str = "",
        m5_pct: Optional[float] = None,
        m1_pct: Optional[float] = None,
        s1_pct: Optional[float] = None,
        h1_pct: Optional[float] = None,
        price_usd: Optional[float] = None,
        since_buy_pct: Optional[float] = None,
        ath_since_pct: Optional[float] = None,
        name: str = "",
        callout_user: str = "",
    ) -> LedgerEntry:
        cost = entry_sol + entry_fee_sol + entry_tip_sol
        sig = signature or ""
        real_close = [
            old for old in self.entries
            if old.side == "CLOSE" and old.mint == mint
            and (old.reason or "") not in ("DUST", "dust_left", "dust_unsellable", "not_in_wallet", "empty_wallet")
            and float(old.exit_sol or 0) > 0
        ]
        opens = [old for old in self.entries if old.side == "OPEN" and old.mint == mint]
        if opens and (not real_close or float(opens[-1].ts or 0) > float(real_close[-1].ts or 0)):
            log.info("LEDGER skip duplicate open %s — this buy is already open", symbol or mint[:8])
            return opens[-1]
        e = LedgerEntry(
            id=id,
            ts=time.time(),
            side="OPEN",
            mint=mint,
            symbol=symbol or mint[:8],
            mode=mode,
            source=_source_name(source),
            token_amount=token_amount,
            entry_sol=entry_sol,
            entry_fee_sol=entry_fee_sol,
            entry_tip_sol=entry_tip_sol,
            price_sol_per_token=price_sol_per_token,
            cost_basis_sol=cost,
            signature=sig,
            solscan_tx=solscan_tx_url(sig),
            solscan_token=f"https://solscan.io/token/{mint}",
            sol_usd=sol_usd,
            m5_pct=_mark(m5_pct),
            m1_pct=_mark(m1_pct),
            s1_pct=_mark(s1_pct),
            h1_pct=_mark(h1_pct),
            price_usd=_mark(price_usd),
            since_buy_pct=None if since_buy_pct is None else round(float(since_buy_pct), 2),
            ath_since_pct=None if ath_since_pct is None else round(max(0.0, float(ath_since_pct)), 2),
        )
        self._append(e)
        log.info(
            "LEDGER OPEN %s tokens=%.4g sol=%.6f fees=%.6f tip=%.6f cost=%.6f (~$%.2f)",
            e.symbol, token_amount, entry_sol, entry_fee_sol, entry_tip_sol, cost, cost * sol_usd,
        )
        try:
            from src.observability.email_alert import notify_open
            payload = e.to_dict()
            if name:
                payload["name"] = name
            if callout_user:
                payload["callout_user"] = callout_user
            notify_open(payload)
        except Exception:
            log.warning("open email failed", exc_info=True)
        return e

    def record_close(
        self,
        *,
        id: str,
        mint: str,
        symbol: str,
        mode: str,
        token_amount: float,
        entry_sol: float,
        entry_fee_sol: float,
        entry_tip_sol: float,
        exit_sol: float,
        exit_fee_sol: float,
        hold_seconds: float,
        reason: str,
        signature: str = "",
        sol_usd: float = SOL_USD_FALLBACK,
        since_buy_pct: Optional[float] = None,
        ath_since_pct: Optional[float] = None,
        source: str = "",
        sale_label: str = "",
        group_id: str = "",
        count_as_trade: bool = True,
        m5_pct: Optional[float] = None,
        m1_pct: Optional[float] = None,
        s1_pct: Optional[float] = None,
        h1_pct: Optional[float] = None,
        price_usd: Optional[float] = None,
        name: str = "",
        callout_user: str = "",
        low_since_pct: Optional[float] = None,
        pnl_usd_locked: Optional[float] = None,
        ts: Optional[float] = None,
        notify: bool = True,
    ) -> Optional[LedgerEntry]:
        if (reason or "") in ("empty_wallet", "not_in_wallet", "DUST", "dust_left", "dust_unsellable"):
            # Expected: dust / not-in-wallet is not a sale, so it is never booked. Not a fault.
            log.info(
                "LEDGER no sale booked mint=%s reason=%s — left in the wallet, PnL unchanged",
                (mint or "")[:8], reason,
            )
            return None
        if float(exit_sol or 0) <= 0:
            log.error(
                "LEDGER refuse close mint=%s reason=%s — no SOL came back, so it is not a sale",
                (mint or "")[:8], reason,
            )
            return None
        cost = entry_sol + entry_fee_sol + entry_tip_sol
        pnl = exit_sol - cost - exit_fee_sol
        if str(reason or "").startswith("take_profit") and pnl < 0:
            reason = "sold_under_cost"
        now = time.time()
        sig = signature or ""
        if sig:
            for old in self.entries:
                if old.side == "CLOSE" and (old.signature or "") == sig and float(old.exit_sol or 0) > 0:
                    log.info("LEDGER skip duplicate close %s %s", symbol or mint[:8], reason)
                    return old
        for old in self.entries:
            if old.side != "CLOSE" or old.mint != mint or (old.reason or "") != (reason or ""):
                continue
            if abs(float(old.token_amount or 0) - float(token_amount or 0)) > 1:
                continue
            if now - float(old.ts or 0) < 180:
                log.info("LEDGER skip duplicate close %s %s", symbol or mint[:8], reason)
                return old
        when = float(ts) if ts and float(ts) > 1_000_000_000 else now
        if not source:
            opened = [x for x in self.entries if x.side == "OPEN" and x.mint == mint and x.source]
            source = opened[-1].source if opened else ""
        e = LedgerEntry(
            id=id,
            ts=when,
            side="CLOSE",
            mint=mint,
            symbol=symbol or mint[:8],
            mode=mode,
            source=_source_name(source),
            token_amount=token_amount,
            entry_sol=entry_sol,
            entry_fee_sol=entry_fee_sol,
            entry_tip_sol=entry_tip_sol,
            exit_sol=exit_sol,
            exit_fee_sol=exit_fee_sol,
            cost_basis_sol=cost,
            pnl_sol=pnl,
            pnl_usd=pnl * sol_usd,
            hold_seconds=hold_seconds,
            reason=reason,
            since_buy_pct=None if since_buy_pct is None else round(float(since_buy_pct), 2),
            ath_since_pct=round(max(0.0, float(ath_since_pct or 0)), 2),
            sale_label=sale_label or "",
            group_id=group_id or id,
            signature=sig,
            solscan_tx=f"https://solscan.io/tx/{sig}" if sig and not str(sig).startswith("paper") else "",
            solscan_token=f"https://solscan.io/token/{mint}",
            sol_usd=sol_usd,
            m5_pct=_mark(m5_pct),
            m1_pct=_mark(m1_pct),
            s1_pct=_mark(s1_pct),
            h1_pct=_mark(h1_pct),
            price_usd=None if _mark(price_usd) is None else round(float(_mark(price_usd)), 12),
            book_locked=pnl_usd_locked is not None,
        )
        if pnl_usd_locked is not None:
            e.pnl_usd = round(float(pnl_usd_locked), 6)
            e.book_locked = True
        self._append(e)
        if count_as_trade:
            self._closed_n = max(int(self._closed_n or 0) + 1, self._saved_close_count())
        self._apply_close(e, save=True)
        log.info(
            "LEDGER CLOSE %s pnl=%.6f SOL (~$%.2f) exit=%.6f cost=%.6f reason=%s",
            e.symbol, pnl, pnl * sol_usd, exit_sol, cost, reason,
        )
        try:
            if notify:
                from src.observability.email_alert import notify_close
                payload = e.to_dict()
                if name:
                    payload["name"] = name
                if callout_user:
                    payload["callout_user"] = callout_user
                if low_since_pct is not None:
                    payload["low_since_pct"] = low_since_pct
                notify_close(payload)
        except Exception:
            log.warning("close email failed", exc_info=True)
        return e

    def apply_known_fees(self) -> int:
        """Launch and Dayscalp have no tip. The fee is the priority fee, not the wallet spill.

        Launch buy 0.00005, Dayscalp buy 0.00002, sell 0.00001.
        """
        buy_fee = {"launch": 0.00005, "dayscalp": 0.00002, "predict": 0.00002, "callout": 0.00002}
        sell_fee = 0.00001
        segments: List[Dict[str, Any]] = []
        by_mint: Dict[str, List[LedgerEntry]] = {}
        for e in self.entries:
            if e.mint:
                by_mint.setdefault(e.mint, []).append(e)
        for items in by_mint.values():
            items.sort(key=lambda e: float(e.ts or 0))
            cur: Optional[Dict[str, Any]] = None
            for e in items:
                if e.side == "OPEN":
                    if cur:
                        segments.append(cur)
                    cur = {"open": e, "closes": []}
                elif e.side == "CLOSE":
                    if cur is None:
                        cur = {"open": None, "closes": [e]}
                    else:
                        cur["closes"].append(e)
            if cur:
                segments.append(cur)
        fixed = 0
        touched: List[LedgerEntry] = []
        for seg in segments:
            op: Optional[LedgerEntry] = seg["open"]
            closes: List[LedgerEntry] = seg["closes"]
            source = str((op.source if op else closes[0].source) or "").lower()
            fee = buy_fee.get(source)
            if fee is None:
                continue
            if op is not None:
                cost = round(float(op.entry_sol or 0) + fee, 6)
                if (
                    abs(float(op.entry_fee_sol or 0) - fee) > 1e-9
                    or abs(float(op.entry_tip_sol or 0)) > 1e-9
                    or abs(float(op.cost_basis_sol or 0) - cost) > 1e-9
                ):
                    op.entry_fee_sol = fee
                    op.entry_tip_sol = 0.0
                    op.cost_basis_sol = cost
                    fixed += 1
                    touched.append(op)
            base = float(op.entry_sol if op else 0) or sum(float(c.entry_sol or 0) for c in closes) or 0.0
            for close in closes:
                share = (float(close.entry_sol or 0) / base) if base > 0 else (1.0 / max(1, len(closes)))
                share = min(1.0, max(0.0, share))
                part = round(fee * share, 8)
                entry = float(close.entry_sol or 0)
                cost = round(entry + part, 6)
                pnl = round(float(close.exit_sol or 0) - cost - sell_fee, 6)
                if (
                    abs(float(close.entry_fee_sol or 0) - part) > 1e-9
                    or abs(float(close.entry_tip_sol or 0)) > 1e-9
                    or abs(float(close.exit_fee_sol or 0) - sell_fee) > 1e-9
                    or abs(float(close.cost_basis_sol or 0) - cost) > 1e-9
                    or abs(float(close.pnl_sol or 0) - pnl) > 1e-9
                ):
                    close.entry_fee_sol = part
                    close.entry_tip_sol = 0.0
                    close.exit_fee_sol = sell_fee
                    close.cost_basis_sol = cost
                    close.pnl_sol = pnl
                    if not getattr(close, "book_locked", False):
                        close.pnl_usd = round(pnl * float(close.sol_usd or SOL_USD_FALLBACK), 4)
                    fixed += 1
                    touched.append(close)
        if not fixed:
            return 0
        self._rewrite()
        for row in touched:
            self._push_remote(row)
        self._replay_closes()
        self._save_book(force=True)
        log.warning("LEDGER fees set to the priority fee on %d rows — no tip, no wallet spill", fixed)
        return fixed

    def reconcile_wallet_sells(self, rows: List[Dict[str, Any]]) -> int:
        """Match each wallet buy and each wallet sell to one ledger row.

        A coin sold in two transactions used to write the sum onto every close.
        Both rows then showed the full profit, and the total was counted twice.
        """
        buys: List[Dict[str, Any]] = []
        sells: List[Dict[str, Any]] = []
        fees: List[Dict[str, Any]] = []
        seen = set()
        for r in rows or []:
            sig = str(r.get("signature") or "")
            if sig and sig in seen:
                continue
            if sig:
                seen.add(sig)
            if str(r.get("status") or "") == "failed":
                continue
            side = str(r.get("side") or "").upper()
            try:
                sol = float(r.get("sol_amount") or 0)
                tokens = float(r.get("token_amount") or 0)
                ts = float(r.get("block_time") or r.get("ts") or 0)
            except (TypeError, ValueError):
                continue
            mint = str(r.get("mint") or "")
            if side == "BUY" and mint and ts > 0 and (sol > 0 or tokens > 0):
                buys.append({"sig": sig, "mint": mint, "ts": ts, "sol": sol, "tokens": tokens})
            elif side == "SELL" and mint and ts > 0 and (sol > 0 or tokens > 0):
                sells.append({"sig": sig, "mint": mint, "ts": ts, "sol": sol, "tokens": tokens})
            elif side == "FEE" and ts > 0 and sol > 0:
                fees.append({"sig": sig, "ts": ts, "sol": sol})
        if not buys and not sells:
            return 0

        segments: List[Dict[str, Any]] = []
        by_mint: Dict[str, List[LedgerEntry]] = {}
        for e in self.entries:
            if e.mint:
                by_mint.setdefault(e.mint, []).append(e)
        for mint, items in by_mint.items():
            items.sort(key=lambda e: float(e.ts or 0))
            cur: Optional[Dict[str, Any]] = None
            for e in items:
                if e.side == "OPEN":
                    if cur:
                        segments.append(cur)
                    cur = {"open": e, "closes": []}
                elif e.side == "CLOSE":
                    if cur is None:
                        cur = {"open": None, "closes": [e]}
                    else:
                        cur["closes"].append(e)
            if cur:
                segments.append(cur)

        used = set()
        fixed = 0
        for seg in segments:
            op = seg["open"]
            closes: List[LedgerEntry] = seg["closes"]
            if not closes and op is None:
                continue
            mint = (op.mint if op else closes[0].mint)
            start = float(op.ts if op else closes[0].ts) - 180
            # A buy after the sell is the next trade. Do not add it to this cost.
            if closes:
                buy_end = float(closes[0].ts) + 3
            else:
                buy_end = float(op.ts) + 90
            end = float(closes[-1].ts if closes else op.ts) + 90
            seg_buys = [b for b in buys if b["mint"] == mint and start <= b["ts"] <= buy_end and b["sig"] not in used]
            seg_sells = [s for s in sells if s["mint"] == mint and start <= s["ts"] <= end + 120 and s["sig"] not in used]
            if not seg_buys and not seg_sells:
                continue
            for leg in seg_buys + seg_sells:
                if leg["sig"]:
                    used.add(leg["sig"])
            legs = seg_buys + seg_sells
            seg_fees = []
            for fee in fees:
                if fee["sig"] in used:
                    continue
                if any(abs(fee["ts"] - leg["ts"]) <= 8 for leg in legs):
                    seg_fees.append(fee)
                    if fee["sig"]:
                        used.add(fee["sig"])
            buy_sol = sum(b["sol"] for b in seg_buys)
            buy_tokens = sum(b["tokens"] for b in seg_buys)
            fee_sol = sum(f["sol"] for f in seg_fees)
            source = str((op.source if op else closes[0].source) or "").lower()
            known = {"launch": 0.00005, "dayscalp": 0.00002, "predict": 0.00002, "callout": 0.00002}.get(source)
            # The wallet labels the SOL above the 0.015 buy as a 0.0015 fee.
            # Launch and Dayscalp do not. The fee is the priority fee only.
            if known is None:
                try:
                    from src.config.runtime import get_config
                    cap = float(get_config().max_position_sol or 0)
                except Exception:
                    cap = 0.0
                if cap > 0 and buy_sol > cap + 0.00005 and (buy_sol - cap) <= 0.006:
                    spill = round(buy_sol - cap, 6)
                    buy_sol = round(cap, 6)
                    if fee_sol <= 0.00005:
                        fee_sol = spill
            else:
                try:
                    from src.config.runtime import get_config
                    cap = float(get_config().max_position_sol or 0)
                except Exception:
                    cap = 0.0
                if cap > 0 and buy_sol > cap + 0.00005 and (buy_sol - cap) <= 0.006:
                    buy_sol = round(cap, 6)
                fee_sol = known
                if op is not None:
                    op.entry_tip_sol = 0.0
            if op is not None and buy_sol > 0:
                changed = False
                if abs(float(op.entry_sol or 0) - buy_sol) > 0.00005:
                    op.entry_sol = round(buy_sol, 6)
                    changed = True
                if buy_tokens > 0 and abs(float(op.token_amount or 0) - buy_tokens) > 0.01:
                    op.token_amount = buy_tokens
                    changed = True
                if abs(float(op.entry_fee_sol or 0) - fee_sol) > 1e-8 or (known is not None and abs(float(op.entry_tip_sol or 0)) > 1e-9):
                    op.entry_fee_sol = round(fee_sol, 8)
                    if known is not None:
                        op.entry_tip_sol = 0.0
                    changed = True
                op.cost_basis_sol = round(float(op.entry_sol or 0) + float(op.entry_fee_sol or 0) + float(op.entry_tip_sol or 0), 6)
                buy_sig = next((b["sig"] for b in seg_buys if b["sig"]), "")
                if buy_sig and op.signature != buy_sig:
                    op.signature = buy_sig
                    op.solscan_tx = solscan_tx_url(buy_sig)
                    changed = True
                if changed:
                    fixed += 1
                    self._push_remote(op)
            if not closes or not seg_sells:
                continue
            seg_sells.sort(key=lambda s: s["ts"])
            if len(closes) == 1:
                matched = [seg_sells]
            else:
                matched = []
                pool = list(seg_sells)
                for i, close in enumerate(closes):
                    hit = None
                    if close.signature:
                        for s in pool:
                            if s["sig"] and s["sig"] == close.signature:
                                hit = s
                                break
                    if hit is None and pool:
                        hit = pool[0]
                    if hit is not None:
                        pool.remove(hit)
                        matched.append([hit])
                    else:
                        matched.append([])
                if pool and matched:
                    matched[-1].extend(pool)
            weights = []
            for group in matched:
                if not group:
                    weights.append(0.0)
                else:
                    weights.append(sum(s["tokens"] for s in group) or sum(s["sol"] for s in group) or 1.0)
            weight_sum = sum(weights) or float(len(closes) or 1)
            cost_sol = buy_sol if buy_sol > 0 else float(op.entry_sol if op else closes[0].entry_sol or 0)
            for close, group, weight in zip(closes, matched, weights):
                if not group:
                    continue
                share = weight / weight_sum
                exit_sol = sum(s["sol"] for s in group)
                tokens = sum(s["tokens"] for s in group)
                entry = cost_sol * share
                fee = fee_sol * share
                exit_fee = 0.00001 if known is not None else float(close.exit_fee_sol or 0)
                pnl = exit_sol - entry - fee - exit_fee
                sig = next((s["sig"] for s in reversed(group) if s["sig"]), close.signature)
                changed = False
                if abs(float(close.exit_sol or 0) - exit_sol) > 0.00005:
                    close.exit_sol = round(exit_sol, 6)
                    changed = True
                if abs(float(close.entry_sol or 0) - entry) > 0.00005:
                    close.entry_sol = round(entry, 6)
                    changed = True
                if abs(float(close.entry_fee_sol or 0) - fee) > 0.0000005:
                    close.entry_fee_sol = round(fee, 8)
                    changed = True
                if known is not None and (
                    abs(float(close.exit_fee_sol or 0) - exit_fee) > 1e-9
                    or abs(float(close.entry_tip_sol or 0)) > 1e-9
                ):
                    close.exit_fee_sol = exit_fee
                    close.entry_tip_sol = 0.0
                    changed = True
                if tokens > 0 and abs(float(close.token_amount or 0) - tokens) > 0.01:
                    close.token_amount = tokens
                    changed = True
                close.cost_basis_sol = round(float(close.entry_sol or 0) + float(close.entry_fee_sol or 0) + float(close.entry_tip_sol or 0), 6)
                new_pnl = round(pnl, 6)
                if abs(float(close.pnl_sol or 0) - new_pnl) > 0.00005:
                    close.pnl_sol = new_pnl
                    if not getattr(close, "book_locked", False):
                        close.pnl_usd = round(new_pnl * float(close.sol_usd or SOL_USD_FALLBACK), 4)
                    changed = True
                if sig and close.signature != sig:
                    close.signature = sig
                    close.solscan_tx = solscan_tx_url(sig)
                    changed = True
                if changed:
                    fixed += 1
                    self._push_remote(close)
                    log.warning(
                        "LEDGER %s %s exit=%.4f cost=%.4f pnl=%+.4f — one wallet sell, not the sum",
                        close.symbol, close.sale_label or close.reason, close.exit_sol, close.cost_basis_sol, close.pnl_sol,
                    )
        fixed += self._book_missing_closes(sells, used)
        if not fixed:
            return 0
        self._rewrite()
        self._save_book(force=True)
        return fixed

    def _book_missing_closes(self, sells: List[Dict[str, Any]], used: set) -> int:
        """A confirmed wallet sell with no CLOSE row is still a close.

        The live card used to be removed before the book ran, so Execution
        History showed the sell and the ledger stayed OPEN.
        """
        wrote = 0
        seen = {str(sell.get("sig") or "") for sell in sells if sell.get("sig")}
        try:
            from src.observability.exec_history import get_exec_history
            for fill in get_exec_history()._fills():
                if (fill.side or "").upper() != "SELL":
                    continue
                sig = str(fill.signature or "")
                if not sig or sig in seen:
                    continue
                if not fill.mint or float(fill.sol_amount or 0) <= 0 or float(fill.ts or 0) <= 0:
                    continue
                sells.append({
                    "sig": sig,
                    "mint": str(fill.mint),
                    "ts": float(fill.ts or 0),
                    "sol": float(fill.sol_amount or 0),
                    "tokens": float(fill.token_amount or 0),
                })
                if sig:
                    seen.add(sig)
        except Exception:
            log.debug("bot sell backfill failed", exc_info=True)
        for entry in list(self.entries):
            if entry.side != "OPEN" or not entry.mint:
                continue
            opened_at = float(entry.ts or 0)
            if any(
                row.side == "CLOSE"
                and row.mint == entry.mint
                and float(row.exit_sol or 0) > 0
                and float(row.ts or 0) >= opened_at - 5
                for row in self.entries
            ):
                continue
            nxt = min(
                (
                    float(row.ts or 0)
                    for row in self.entries
                    if row.side == "OPEN" and row.mint == entry.mint and float(row.ts or 0) > opened_at + 1
                ),
                default=10**12,
            )
            bag = [
                sell for sell in sells
                if sell["mint"] == entry.mint
                and opened_at - 30 <= float(sell["ts"]) < nxt
                and float(sell["sol"]) > 0
                and sell["sig"] not in used
            ]
            if not bag:
                continue
            sold_sol = sum(float(sell["sol"]) for sell in bag)
            sold_tokens = sum(float(sell["tokens"]) for sell in bag)
            bought = float(entry.token_amount or 0)
            if bought > 100 and sold_tokens > 0:
                from src.observability.exec_history import _same_bag
                if not _same_bag(sold_tokens, bought) and sold_tokens < bought * 0.5:
                    continue
            if sold_sol <= 0:
                continue
            last = max(bag, key=lambda sell: float(sell["ts"]))
            for sell in bag:
                if sell["sig"]:
                    used.add(sell["sig"])
            self.record_close(
                id=f"wal-{entry.mint[:8]}-{int(last['ts'])}",
                mint=entry.mint,
                symbol=entry.symbol or entry.mint[:8],
                mode=entry.mode or "live",
                token_amount=sold_tokens or bought,
                entry_sol=float(entry.entry_sol or 0),
                entry_fee_sol=float(entry.entry_fee_sol or 0),
                entry_tip_sol=float(entry.entry_tip_sol or 0),
                exit_sol=sold_sol,
                exit_fee_sol=0.00001,
                hold_seconds=max(0.0, float(last["ts"]) - opened_at),
                reason="wallet_already_sold",
                signature=str(last.get("sig") or ""),
                source=entry.source or "",
                since_buy_pct=entry.since_buy_pct,
                ath_since_pct=entry.ath_since_pct,
                m5_pct=entry.m5_pct,
                m1_pct=entry.m1_pct,
                s1_pct=entry.s1_pct,
                h1_pct=entry.h1_pct,
                price_usd=entry.price_usd,
                ts=float(last["ts"]),
                notify=False,
            )
            wrote += 1
            log.warning(
                "LEDGER CLOSE %s filled from the wallet sell — the book had only the open",
                entry.symbol or entry.mint[:8],
            )
        return wrote

    def recent(self, n: int = 250) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self.entries[-n:]]

    def summary(self) -> Dict[str, Any]:
        closes = [e for e in self.entries if e.side == "CLOSE"]
        opens = [e for e in self.entries if e.side == "OPEN"]
        net = sum(e.pnl_sol for e in closes)
        wins = sum(1 for e in closes if e.pnl_sol > 0)
        return {
            "opens": len(opens),
            "closes": len(closes),
            "net_pnl_sol": net,
            "wins": wins,
            "win_rate": (wins / len(closes)) if closes else 0.0,
        }


_ledger: Optional[TradeLedger] = None


def get_ledger() -> TradeLedger:
    global _ledger
    if _ledger is None:
        _ledger = TradeLedger()
    return _ledger
