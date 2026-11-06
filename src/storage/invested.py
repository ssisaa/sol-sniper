"""The SOL you say you put in. You type it. A deposit is not profit until you raise this number.

Saved in SQLite and in logs/invested.json.
Clear Session does not delete it. Clear on total PnL does.
A positive save replaces the number. A negative save subtracts from it.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger("sol-sniper.invested")

_PATH = Path(__file__).resolve().parents[2] / "logs" / "invested.json"
_cache = {"ts": 0.0, "sol": None}


def _read_local() -> Optional[float]:
    try:
        raw = json.loads(_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(raw, dict):
        raw = raw.get("sol")
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if val < 0:
        return None
    return val


def _write_local(val: Optional[float]) -> None:
    _PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"sol": val})
    tmp = _PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(_PATH)


def get_invested(force: bool = False) -> Optional[float]:
    now = time.time()
    if not force and _cache["ts"] and now - float(_cache["ts"]) < 2:
        return _cache["sol"]
    # The file is what Save wrote. SQLite is only a copy, and an old copy
    # was putting the previous amount back after a refresh.
    local = _read_local()
    remote = None
    try:
        from src.storage.supabase_store import get_store
        got = get_store().get("invested_sol")
        if isinstance(got, dict):
            got = got.get("sol")
        if got is not None:
            remote = float(got)
            if remote < 0:
                remote = None
    except (TypeError, ValueError):
        remote = None
    except Exception:
        remote = None
    val = local if local is not None else remote
    if local is not None and remote != local:
        try:
            from src.storage.supabase_store import get_store
            get_store().put("invested_sol", {"sol": local})
        except Exception:
            pass
    _cache["ts"] = now
    _cache["sol"] = val
    return val


def set_invested(sol: float) -> float:
    val = round(float(sol), 6)
    if val < 0 or val > 100000:
        raise ValueError("Invested SOL must be between 0 and 100000")
    _write_local(val)
    try:
        from src.storage.supabase_store import get_store
        ok = get_store().put("invested_sol", {"sol": val})
        if not ok:
            log.warning("Invested %.4f SOL is in the file. SQLite did not take the copy.", val)
    except Exception:
        log.warning("Invested %.4f SOL is in the file. SQLite copy failed.", val)
    _cache["ts"] = time.time()
    _cache["sol"] = val
    return val


def apply_invested(sol: float) -> float:
    """Positive replaces the stored amount. Negative subtracts from it. Never goes below 0."""
    raw = round(float(sol), 6)
    if raw > 100000:
        raise ValueError("Invested SOL must be between 0 and 100000")
    if raw < 0:
        current = get_invested(force=True)
        base = float(current) if current is not None else 0.0
        return set_invested(max(0.0, base + raw))
    return set_invested(raw)


def clear_invested() -> None:
    """Forget the amount. Used by the total PnL clear button, not Clear Session."""
    try:
        _PATH.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass
    try:
        from src.storage.supabase_store import get_store
        store = get_store()
        store.delete("invested_sol")
        store.put("invested_sol", {"sol": None})
    except Exception:
        pass
    _cache["ts"] = time.time()
    _cache["sol"] = None
    try:
        from src.storage.capital_pnl import reset_capital_marks
        reset_capital_marks()
    except Exception:
        pass
