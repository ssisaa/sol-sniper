"""Local SQLite store. Same calls the bot used for Supabase.

File: sqlitebase/sniper.db
Never stores a keypair or seed phrase.
The ledger keeps the newest 100 rows.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("sol-sniper.sqlite")

_lock = threading.Lock()
_store: Optional["SupabaseStore"] = None
_ROOT = Path(__file__).resolve().parents[2]
_DB = _ROOT / "sqlitebase" / "sniper.db"
_BOOK = _ROOT / "logs" / "sniper_book.json"
_STATE = _ROOT / "logs" / "sniper_state.json"
_LEDGER_KEEP = 100


class SupabaseStore:
    """Name kept so existing imports stay. The data is SQLite, not Supabase."""

    def __init__(self) -> None:
        self.url = str(_DB)
        self.key = "sqlite"
        self._op = threading.Lock()
        _DB.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(_DB), check_same_thread=False, timeout=15)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=MEMORY").fetchone()
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute("PRAGMA temp_store=MEMORY")
        self._init()
        self._seed()

    def _ensure(self) -> None:
        """Recreate tables if a rollback or a bad file dropped them."""
        try:
            row = self._db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='state'"
            ).fetchone()
        except sqlite3.OperationalError:
            row = None
        if row is None:
            log.warning("SQLite schema missing — creating tables")
            self._init()

    def _init(self) -> None:
        schema = """
            CREATE TABLE IF NOT EXISTS state (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ledger (
              id TEXT PRIMARY KEY,
              ts REAL NOT NULL,
              row TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS wallet (
              mint TEXT PRIMARY KEY,
              ui_amount REAL,
              raw INTEGER,
              value_sol REAL,
              dust INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS position (
              id TEXT PRIMARY KEY,
              row TEXT NOT NULL
            );
        """
        try:
            self._db.executescript(schema)
            self._db.commit()
        except sqlite3.DatabaseError:
            log.warning("SQLite file unreadable — starting a new database")
            try:
                self._db.close()
            except Exception:
                pass
            try:
                _DB.unlink()
            except Exception:
                pass
            self._db = sqlite3.connect(str(_DB), check_same_thread=False, timeout=15)
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA journal_mode=OFF").fetchone()
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.executescript(schema)
            self._db.commit()
        names = {
            row[0]
            for row in self._db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "state" not in names or "position" not in names or "wallet" not in names:
            self._db.execute("PRAGMA journal_mode=OFF").fetchone()
            self._db.executescript(schema)
            self._db.commit()

    def _heal(self, exc: Exception) -> bool:
        msg = str(exc)
        if "no such table" not in msg and "disk I/O" not in msg and "malformed" not in msg:
            return False
        log.warning("SQLite healing — %s", msg.splitlines()[0][:180])
        try:
            self._db.rollback()
        except Exception:
            pass
        try:
            self._db.execute("PRAGMA journal_mode=OFF").fetchone()
        except Exception:
            pass
        self._init()
        return True

    @property
    def enabled(self) -> bool:
        return True

    def _seed(self) -> None:
        """First run copies the files already on disk so invested and settings are not lost."""
        try:
            n = self._db.execute("SELECT COUNT(*) FROM state").fetchone()[0]
        except Exception:
            return
        if n:
            self._seed_ledger()
            return
        blob = {}
        try:
            raw = json.loads(_STATE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                blob.update(raw)
        except Exception:
            pass
        for path, key in (
            (_ROOT / "logs" / "invested.json", "invested_sol"),
            (_ROOT / "logs" / "mint_flags.json", "mint_flags"),
            (_ROOT / "logs" / "open_mints.json", "open_mints"),
        ):
            if key in blob:
                continue
            try:
                blob[key] = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass
        for key, value in blob.items():
            self.put(key, value)
        try:
            book = json.loads(_BOOK.read_text(encoding="utf-8"))
            if isinstance(book, dict) and book.get("id"):
                self.upsert_position(book)
        except Exception:
            pass
        self._seed_ledger()

    def _seed_ledger(self) -> None:
        try:
            n = self._db.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        except Exception:
            return
        if n:
            return
        path = _ROOT / "logs" / "ledger.jsonl"
        if not path.is_file():
            return
        rows = []
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    rows.append(json.loads(line))
        except Exception:
            return
        for item in rows[-_LEDGER_KEEP:]:
            if isinstance(item, dict) and item.get("id"):
                self.upsert_ledger(item)

    def get(self, key: str) -> Optional[Any]:
        with self._op:
            row = None
            for attempt in (1, 2):
                self._ensure()
                try:
                    row = self._db.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
                    break
                except Exception as exc:
                    if attempt == 1 and self._heal(exc):
                        continue
                    log.warning("SQLite read %s failed", key, exc_info=True)
                    return None
        if row is None:
            return None
        try:
            return json.loads(row["value"])
        except Exception:
            return None

    def put(self, key: str, value: Any) -> bool:
        raw = json.dumps(value)
        with self._op:
            for attempt in (1, 2):
                self._ensure()
                try:
                    self._db.execute(
                        "INSERT INTO state (key, value) VALUES (?, ?) "
                        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        (key, raw),
                    )
                    self._db.commit()
                    return True
                except Exception as exc:
                    if attempt == 1 and self._heal(exc):
                        continue
                    log.warning("SQLite write %s failed", key, exc_info=True)
                    return False
        return False

    def delete(self, key: str) -> bool:
        with self._op:
            self._ensure()
            try:
                self._db.execute("DELETE FROM state WHERE key = ?", (key,))
                self._db.commit()
                return True
            except Exception:
                log.warning("SQLite delete %s failed", key, exc_info=True)
                return False

    def set_open_mints(self, mints: list) -> bool:
        payload = sorted({m for m in (mints or []) if isinstance(m, str) and m})
        if payload == getattr(self, "_last_open_mints", None):
            return True
        path = _ROOT / "logs" / "open_mints.json"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload), encoding="utf-8")
        except Exception:
            log.warning("open_mints.json write failed", exc_info=True)
        ok = self.put("open_mints", payload)
        if ok:
            self._last_open_mints = payload
            log.info("open_mints now %s", payload or "[]")
        else:
            log.warning("open_mints NOT updated — still may list a sold coin")
        return ok

    def read_wallet(self) -> tuple:
        """('ok', rows) or ('fail', []). A failure is not an empty wallet."""
        with self._op:
            rows = []
            for attempt in (1, 2):
                self._ensure()
                try:
                    rows = self._db.execute(
                        "SELECT mint, ui_amount, raw, value_sol, dust FROM wallet"
                    ).fetchall()
                    break
                except Exception as exc:
                    if attempt == 1 and self._heal(exc):
                        continue
                    log.warning("SQLite wallet read failed — not treating the wallet as empty", exc_info=True)
                    return "fail", []
        out = []
        for row in rows:
            out.append({
                "mint": row["mint"],
                "ui_amount": row["ui_amount"],
                "raw": row["raw"],
                "value_sol": row["value_sol"],
                "dust": bool(row["dust"]),
            })
        return "ok", out

    def replace_wallet(self, tokens: list) -> None:
        rows = [
            (
                t.get("mint"),
                t.get("ui_amount"),
                t.get("raw"),
                t.get("value_sol"),
                1 if t.get("dust") else 0,
            )
            for t in (tokens or [])
            if t.get("mint")
        ]
        with self._op:
            self._ensure()
            try:
                self._db.execute("DELETE FROM wallet")
                if rows:
                    self._db.executemany(
                        "INSERT INTO wallet (mint, ui_amount, raw, value_sol, dust) VALUES (?, ?, ?, ?, ?)",
                        rows,
                    )
                self._db.commit()
            except Exception:
                log.debug("sqlite wallet replace failed", exc_info=True)

    def upsert_position(self, row: dict) -> None:
        if not row.get("id"):
            return
        self._write_local(row)
        raw = json.dumps(row)
        with self._op:
            self._ensure()
            try:
                self._db.execute(
                    "INSERT INTO position (id, row) VALUES (?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET row = excluded.row",
                    (str(row.get("id")), raw),
                )
                self._db.commit()
            except Exception as exc:
                if self._heal(exc):
                    try:
                        self._db.execute(
                            "INSERT INTO position (id, row) VALUES (?, ?) "
                            "ON CONFLICT(id) DO UPDATE SET row = excluded.row",
                            (str(row.get("id")), raw),
                        )
                        self._db.commit()
                        return
                    except Exception:
                        log.warning("SQLite position write failed — local book kept, trading continues", exc_info=True)
                        return
                log.warning("SQLite position write failed — local book kept, trading continues", exc_info=True)

    def upsert_ledger(self, row: dict) -> bool:
        """Insert or update one row. The table keeps only the newest 100."""
        if not row.get("id"):
            return False
        body = dict(row)
        body.pop("payload", None)
        try:
            ts = float(body.get("ts") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        raw = json.dumps(body)
        with self._op:
            self._ensure()
            try:
                self._db.execute(
                    "INSERT INTO ledger (id, ts, row) VALUES (?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET ts = excluded.ts, row = excluded.row",
                    (str(body["id"]), ts, raw),
                )
                self._db.execute(
                    "DELETE FROM ledger WHERE id NOT IN ("
                    "SELECT id FROM ledger ORDER BY ts DESC LIMIT ?)",
                    (_LEDGER_KEEP,),
                )
                self._db.commit()
                return True
            except Exception:
                self._db.rollback()
                self._ensure()
                log.warning("SQLite ledger write failed", exc_info=True)
                return False

    def _trim_ledger(self, keep: int = _LEDGER_KEEP) -> None:
        with self._op:
            self._ensure()
            self._db.execute(
                "DELETE FROM ledger WHERE id NOT IN ("
                "SELECT id FROM ledger ORDER BY ts DESC LIMIT ?)",
                (keep,),
            )
            self._db.commit()

    def fetch_ledger(self, limit: int = _LEDGER_KEEP) -> Optional[list]:
        cap = min(int(limit or _LEDGER_KEEP), _LEDGER_KEEP)
        with self._op:
            self._ensure()
            try:
                rows = self._db.execute(
                    "SELECT row FROM ledger ORDER BY ts DESC LIMIT ?",
                    (cap,),
                ).fetchall()
            except Exception:
                log.warning("SQLite ledger read failed", exc_info=True)
                return None
        out = []
        for row in rows:
            try:
                item = json.loads(row["row"])
            except Exception:
                continue
            if isinstance(item, dict):
                out.append(item)
        return out

    def delete_ledger(self) -> bool:
        with self._op:
            self._ensure()
            try:
                self._db.execute("DELETE FROM ledger")
                self._db.commit()
                return True
            except Exception:
                log.warning("SQLite ledger clear failed", exc_info=True)
                return False

    def clear_session_rows(self) -> None:
        """Drop the session rows. Invested capital, flags, settings, and the keypair stay."""
        self.delete_ledger()
        self.set_open_mints([])
        flat = {
            "id": "current",
            "mint": "",
            "symbol": "",
            "status": "FLAT",
            "entry_sol": None,
            "buy_allowed": True,
            "block_reason": "",
            "payload": [],
        }
        self.upsert_position(flat)
        self.put("open_position", [])
        self.put("wallet_now", {"buy_allowed": True, "reason": "", "tokens": [], "ts": time.time()})
        self.replace_wallet([])
        for key in ("wallet_now", "open_position", "open_mints"):
            self.delete(key)
        log.info("SQLite session cleared — ledger, position, wallet cache. Bot still live. 30-day and total kept.")

    def read_book(self) -> dict:
        with self._op:
            row = None
            for attempt in (1, 2):
                self._ensure()
                try:
                    row = self._db.execute("SELECT row FROM position WHERE id = ?", ("current",)).fetchone()
                    break
                except Exception as exc:
                    if attempt == 1 and self._heal(exc):
                        continue
                    log.warning("SQLite book read failed — using the local book", exc_info=True)
                    row = None
                    break
        if row is not None:
            try:
                data = json.loads(row["row"])
            except Exception:
                data = None
            if isinstance(data, dict) and data.get("status"):
                self._write_local(data)
                return data
        return self._read_local()

    def write_book(self, rows: list) -> None:
        """Replace the current trade. Empty list means the wallet is flat."""
        if rows:
            first = rows[0]
            row = {
                "id": "current",
                "mint": first.get("mint") or "",
                "symbol": first.get("symbol") or "",
                "status": "OPEN",
                "entry_sol": first.get("entry_sol"),
                "buy_allowed": False,
                "block_reason": "open trade",
                "payload": rows,
            }
        else:
            row = {
                "id": "current",
                "mint": "",
                "symbol": "",
                "status": "FLAT",
                "entry_sol": None,
                "buy_allowed": True,
                "block_reason": "",
                "payload": [],
            }
        self.upsert_position(row)
        self.put("open_position", rows)
        self.set_open_mints([r.get("mint") for r in rows if r.get("mint")])

    def _read_local(self) -> dict:
        try:
            if _BOOK.is_file():
                data = json.loads(_BOOK.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
        except Exception:
            log.debug("local book read failed", exc_info=True)
        return {}

    def _write_local(self, row: dict) -> None:
        try:
            _BOOK.parent.mkdir(parents=True, exist_ok=True)
            _BOOK.write_text(json.dumps(row), encoding="utf-8")
        except Exception:
            log.debug("local book write failed", exc_info=True)


def get_store() -> SupabaseStore:
    global _store
    with _lock:
        if _store is None:
            _store = SupabaseStore()
            log.info("SQLite ready %s", _store.url)
        return _store
