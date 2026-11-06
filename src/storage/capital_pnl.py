"""Daily, 30-day, and total PnL from the wallet, not from the closed-trade book.

Total = equity − the invested amount you typed.
Daily = that result minus the result at the start of today (SGT).
30-day = the same result, unless you pressed clear on the 30-day card.
A deposit is not profit: raising invested moves total with it.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

_PATH = Path(__file__).resolve().parents[2] / "logs" / "equity_pnl.json"
_cache: Dict[str, Any] = {"pnl": None}


def _sgt_day(ts: Optional[float] = None) -> str:
    ts = time.time() if ts is None else float(ts)
    return time.strftime("%Y-%m-%d", time.gmtime(ts + 8 * 3600))


def _load() -> Dict[str, Any]:
    try:
        raw = json.loads(_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _save(data: Dict[str, Any]) -> None:
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        _PATH.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass


def current_capital_pnl() -> Optional[Dict[str, float]]:
    pnl = _cache.get("pnl")
    if isinstance(pnl, dict) and pnl.get("pnl_total_sol") is not None and not _load().get("pinned"):
        return dict(pnl)
    data = _load()
    if data.get("total") is None and data.get("equity") is None:
        if data.get("pinned"):
            return {
                "pnl_daily_sol": 0.0,
                "pnl_30d_sol": round(float(data.get("pinned_30") or 0), 6),
                "pnl_total_sol": round(float(data.get("pinned_total") or 0), 6),
            }
        return None
    total = float(data.get("total") if data.get("total") is not None else (float(data.get("equity") or 0) - float(data.get("invested") or 0)))
    daily = round(total - float(data.get("day_base_total") or 0), 6)
    if data.get("cut30_total") is not None:
        d30 = round(total - float(data["cut30_total"]), 6)
    else:
        d30 = total
    pnl = {
        "pnl_daily_sol": daily,
        "pnl_30d_sol": round(d30, 6),
        "pnl_total_sol": round(total, 6),
    }
    _cache["pnl"] = pnl
    return dict(pnl)


def reset_capital_marks() -> None:
    """Total clear. The three cards stay at zero until a new invested amount is saved."""
    try:
        _PATH.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass
    _cache["pnl"] = {
        "pnl_daily_sol": 0.0,
        "pnl_30d_sol": 0.0,
        "pnl_total_sol": 0.0,
    }


def restart_daily() -> None:
    """Clear Session. Daily goes to zero from this moment and does not keep the old trades."""
    data = _load()
    if data.get("total") is not None:
        data["day_base_total"] = data.get("total")
        data["day"] = _sgt_day()
        data.pop("daily_restart_pending", None)
    else:
        data["daily_restart_pending"] = True
        data["day"] = _sgt_day()
    _save(data)
    pnl = _cache.get("pnl")
    if isinstance(pnl, dict):
        pnl = dict(pnl)
        pnl["pnl_daily_sol"] = 0.0
        _cache["pnl"] = pnl


def pin_session_cards(d30: float, total: float) -> None:
    """Clear Session keeps the 30-day and total numbers already on screen.

    Daily is zero. The next real equity change moves 30-day and total from
    these numbers. It does not replace them with a fresh equity minus invested.
    """
    data = _load()
    data["pinned"] = True
    data["pinned_30"] = round(float(d30 or 0), 6)
    data["pinned_total"] = round(float(total or 0), 6)
    data.pop("pin_equity", None)
    data.pop("pin_invested", None)
    data["day"] = _sgt_day()
    data.pop("daily_restart_pending", None)
    _save(data)
    _cache["pnl"] = {
        "pnl_daily_sol": 0.0,
        "pnl_30d_sol": round(float(d30 or 0), 6),
        "pnl_total_sol": round(float(total or 0), 6),
    }


def pin_cut(which: str) -> None:
    """A 30-day or total clear, when the cards are pinned to the session."""
    data = _load()
    if not data.get("pinned"):
        return
    if which == "30d":
        data["pinned_30"] = 0.0
    elif which == "total":
        data["pinned_total"] = 0.0
    data.pop("pin_equity", None)
    data.pop("pin_invested", None)
    _save(data)
    pnl = dict(_cache.get("pnl") or {})
    pnl["pnl_30d_sol"] = float(data.get("pinned_30") or 0)
    pnl["pnl_total_sol"] = float(data.get("pinned_total") or 0)
    _cache["pnl"] = pnl


def mark_30d_cleared() -> None:
    """The 30-day card goes back to zero. Total is not touched."""
    data = _load()
    if data.get("total") is not None:
        data["cut30_total"] = data.get("total")
        data.pop("cut30_pending", None)
    else:
        data["cut30_pending"] = True
    _save(data)
    pnl = _cache.get("pnl")
    if isinstance(pnl, dict):
        pnl = dict(pnl)
        pnl["pnl_30d_sol"] = 0.0
        _cache["pnl"] = pnl


def note_equity(equity: Optional[float], invested: Optional[float]) -> Optional[Dict[str, float]]:
    """Remember the latest wallet equity and return the three card numbers."""
    if invested is None or equity is None:
        return current_capital_pnl()
    total = round(float(equity) - float(invested), 6)
    today = _sgt_day()
    data = _load()
    if data.get("daily_restart_pending"):
        data["day_base_total"] = total
        data["day"] = today
        data.pop("daily_restart_pending", None)
    elif data.get("day") != today:
        if data.get("day") and data.get("total") is not None:
            data["day_base_total"] = data.get("total")
        else:
            data["day_base_total"] = 0.0
        data["day"] = today
    if data.get("day_base_total") is None:
        data["day_base_total"] = 0.0
    if data.get("cut30_pending"):
        data["cut30_total"] = total
        data.pop("cut30_pending", None)
    data["total"] = total
    data["equity"] = round(float(equity), 6)
    data["invested"] = round(float(invested), 6)
    data.pop("pinned", None)
    _save(data)
    daily = round(total - float(data.get("day_base_total") or 0), 6)
    if data.get("cut30_total") is not None:
        d30 = round(total - float(data["cut30_total"]), 6)
    else:
        d30 = total
    pnl = {
        "pnl_daily_sol": daily,
        "pnl_30d_sol": d30,
        "pnl_total_sol": total,
    }
    _cache["pnl"] = pnl
    return pnl
