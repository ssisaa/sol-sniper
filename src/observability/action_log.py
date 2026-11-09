"""
Action log — every trade decision and error in one place.

File: logs/actions.jsonl
Live: dashboard "Action log" panel.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
ACTION_PATH = ROOT / "logs" / "actions.jsonl"

_entries: List[Dict[str, Any]] = []
_MAX = 300
_emitting = False


def recent(n: int = 80) -> List[Dict[str, Any]]:
    return list(_entries[-n:])


def clear_actions() -> None:
    """Empty the action log in memory and on disk."""
    _entries.clear()
    try:
        ACTION_PATH.parent.mkdir(parents=True, exist_ok=True)
        ACTION_PATH.write_text("", encoding="utf-8")
    except Exception:
        pass


_KEEP_LOGS = {
    "active-wallet.json",
    "wallet_active.path",
    "runtime_state.json",
    "exit_presets.json",
    "invested.json",
    "equity_pnl.json",
    # Settings, not session history: Clear session must not drop the OpenHolds callers you picked
    # or the caller name/wallet directory (Trade History CallUser links use it).
    "openholds_watch.json",
    "callout_users.json",
}


def clear_session_logs() -> None:
    """Delete session logs. The imported keypair files are not touched."""
    folder = ACTION_PATH.parent
    if not folder.is_dir():
        return
    for path in folder.iterdir():
        if not path.is_file() or path.name in _KEEP_LOGS:
            continue
        try:
            path.unlink()
        except Exception:
            pass
    clear_actions()


# Actions named after a trade source. The Action column shows the source picked in Live Controls
# instead, so an OpenCopy run reads OPENCOPY, not the CALLOUT scan loop it shares.
_SOURCE_TAGS = {"CALLOUT", "CALLOUTS", "OPENHOLDS", "OPENHOLD", "OPENCOPY", "MAYHEM", "DAYSCALP", "LAUNCH", "PREDICT"}


def _source_label(action: str) -> str:
    if str(action or "").upper() not in _SOURCE_TAGS:
        return action
    try:
        from src.config.runtime import get_config
        mode = str(get_config().filter_mode or "").strip()
    except Exception:
        return action
    return mode.upper()[:40] if mode else action


def record_action(
    action: str,
    *,
    level: str = "INFO",
    mint: str = "",
    symbol: str = "",
    detail: str = "",
    error: str = "",
    **extra: Any,
) -> Dict[str, Any]:
    """Write one structured action. Safe to call from anywhere."""
    action = _source_label(action)
    entry = {
        "ts": time.time(),
        "level": (level or "INFO").upper(),
        "action": action,
        "mint": mint or "",
        "symbol": symbol or "",
        "detail": detail or "",
        "error": error or "",
    }
    if extra:
        entry["extra"] = {k: extra[k] for k in list(extra)[:12]}
    _store(entry)
    return entry


def _store(entry: Dict[str, Any]) -> None:
    global _emitting
    _entries.append(entry)
    if len(_entries) > _MAX:
        del _entries[:-_MAX]
    try:
        ACTION_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ACTION_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception:
        pass
    if _emitting:
        return
    _emitting = True
    try:
        _push(entry)
    finally:
        _emitting = False


def _push(entry: Dict[str, Any]) -> None:
    try:
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            return
        loop.call_soon_threadsafe(_schedule_publish, entry)
    except Exception:
        pass


def _schedule_publish(entry: Dict[str, Any]) -> None:
    try:
        asyncio.get_running_loop().create_task(_publish(entry))
    except Exception:
        pass


async def _publish(entry: Dict[str, Any]) -> None:
    try:
        from src.observability.dashboard import bus
        await bus.publish({"type": "action", "entry": entry})
    except Exception:
        pass


class _ActionHandler(logging.Handler):
    """Copy sol-sniper warnings, errors, and trade actions into the action log."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if not record.name.startswith("sol-sniper"):
                return
            msg = record.getMessage()
            keys = (
                "BUY", "FILL", "MISS", "skip", "no buy", "place buy", "gave up",
                "READY", "WATCH", "holding", "empty", "BLOCKED", "crashed", "timeout",
                "confirmed", "check #", "scan pass",
            )
            keep = record.levelno >= logging.WARNING or msg.startswith(
                ("ACTION ", "BUY ", "SELL ", "EXEC ")
            ) or (msg.startswith("DAYSCALP ") and any(k in msg for k in keys))
            if not keep:
                return
            err = ""
            if record.exc_info and record.exc_info[1] is not None:
                err = "".join(traceback.format_exception(*record.exc_info))[-800:]
            action = msg.split(" ", 1)[0][:40]
            record_action(
                action,
                level=record.levelname,
                detail=msg[:500],
                error=err,
                logger=record.name,
            )
        except Exception:
            pass


def install_action_log() -> None:
    parent = logging.getLogger("sol-sniper")
    if any(isinstance(h, _ActionHandler) for h in parent.handlers):
        return
    handler = _ActionHandler(level=logging.INFO)
    parent.addHandler(handler)
    parent.setLevel(logging.INFO)
    record_action("LOG_START", detail="action log ready → logs/actions.jsonl")
