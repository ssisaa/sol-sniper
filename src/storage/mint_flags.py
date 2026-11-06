"""Manual DUST / NODUST flags. The $0.0001 rule is unchanged.

Stored in SQLite and in logs/mint_flags.json.
Value is {"<mint>": "DUST" | "NODUST"}.
DUST: leave the coin, do not swap, buy the next one.
NODUST: do not auto-drop this mint, even if the quote is under 0.000001 SOL.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

_PATH = Path(__file__).resolve().parents[2] / "logs" / "mint_flags.json"
_SOURCE = Path(__file__).resolve().parents[2] / "logs" / "dust_source.json"
_cache = {"ts": 0.0, "flags": {}}


def load_flags(force: bool = False) -> Dict[str, str]:
    now = time.time()
    if not force and now - float(_cache["ts"]) < 3:
        return dict(_cache["flags"])
    flags: Dict[str, str] = {}
    try:
        raw = json.loads(_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            flags.update(raw)
    except Exception:
        pass
    try:
        from src.storage.supabase_store import get_store
        remote = get_store().get("mint_flags") or {}
        if isinstance(remote, dict):
            flags.update(remote)
    except Exception:
        pass
    clean = {
        str(k): str(v).upper()
        for k, v in flags.items()
        if str(v).upper() in ("DUST", "NODUST") and str(k)
    }
    _cache["ts"] = now
    _cache["flags"] = clean
    return dict(clean)


def mint_flag(mint: str) -> str:
    if not mint:
        return ""
    return load_flags().get(mint, "")


def unsellable_text(text: str) -> bool:
    """True for a frozen token. 0x11 is token error 17, AccountFrozen. The $0.0001 rule is not used here."""
    import re
    raw = text or ""
    low = raw.lower()
    if "accountfrozen" in low or "account is frozen" in low:
        return True
    if re.search(r"(?<![0-9a-f])0x11(?![0-9a-f])", low):
        return True
    if re.search(r"custom['\"]?\s*[:=]\s*17\b", low):
        return True
    return False


def dust_source(mint: str) -> str:
    """manual = the Held Token button. jupiter = Jupiter said no route. Anything else is not DUST."""
    mint = (mint or "").strip()
    if not mint:
        return ""
    try:
        raw = json.loads(_SOURCE.read_text(encoding="utf-8"))
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        return ""
    return str(raw.get(mint) or "")


def note_dust_source(mint: str, source: str) -> None:
    mint = (mint or "").strip()
    source = (source or "").strip().lower()
    if not mint:
        return
    try:
        raw = json.loads(_SOURCE.read_text(encoding="utf-8"))
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        raw = {}
    if source in ("", "clear"):
        raw.pop(mint, None)
    else:
        raw[mint] = source
    try:
        _SOURCE.parent.mkdir(parents=True, exist_ok=True)
        _SOURCE.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    except Exception:
        pass


def flag_dust_if_needed(mint: str, reason: str = "") -> bool:
    """Write DUST once. A manual NODUST flag is left alone."""
    mint = (mint or "").strip()
    if not mint:
        return False
    current = mint_flag(mint)
    if current == "NODUST":
        return False
    if current == "DUST":
        return True
    text = (reason or "").lower()
    # A Lightning pool miss is not a reason. Only Jupiter "no route" may auto-flag.
    if "no_routes" in text or "no routes" in text or "no route" in text or "not_tradable" in text or "not tradable" in text:
        note_dust_source(mint, "jupiter")
    elif "frozen" in text or "accountfrozen" in text:
        note_dust_source(mint, "frozen")
    else:
        return False
    import logging
    logging.getLogger("sol-sniper.mint_flags").warning(
        "FLAG DUST mint=%s — %s. Saved. Next buy can start.",
        mint[:8],
        (reason or "unsellable")[:140],
    )
    return set_mint_flag(mint, "DUST")


def set_mint_flag(mint: str, flag: str) -> bool:
    mint = (mint or "").strip()
    flag = (flag or "").strip().upper()
    if not mint:
        return False
    flags = load_flags(force=True)
    if flag in ("", "CLEAR"):
        flags.pop(mint, None)
        note_dust_source(mint, "clear")
    elif flag in ("DUST", "NODUST"):
        flags[mint] = flag
    else:
        return False
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        _PATH.write_text(json.dumps(flags, indent=2), encoding="utf-8")
    except Exception:
        return False
    try:
        from src.storage.supabase_store import get_store
        get_store().put("mint_flags", flags)
    except Exception:
        pass
    _cache["ts"] = time.time()
    _cache["flags"] = flags
    return True
