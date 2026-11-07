"""
Runtime configuration for SOL-SNIPER — mutable while the process runs.
UI can push updates over WebSocket; pipeline reads this singleton.
"""

from __future__ import annotations

import json
import re
import threading
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List


_AGE_RE = re.compile(
    r"^(\d+(?:\.\d+)?)(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$",
    re.I,
)


def callout_sell_seconds(text: str) -> float:
    """Age text to seconds. 0, empty, or junk means off. 1h is 3600."""
    raw = re.sub(r"\s+", "", str(text or "").strip().lower())
    if raw in ("", "0", "0.0", "off", "none"):
        return 0.0
    match = _AGE_RE.match(raw)
    if not match:
        return 0.0
    try:
        amount = float(match.group(1))
    except (TypeError, ValueError):
        return 0.0
    if amount <= 0:
        return 0.0
    unit = match.group(2).lower()
    if unit.startswith("s"):
        mult = 1.0
    elif unit.startswith("m"):
        mult = 60.0
    elif unit.startswith("h"):
        mult = 3600.0
    else:
        mult = 86400.0
    return amount * mult


def normalize_openhold_auto_age(value) -> str | None:
    """Window for OpenHolds auto-open only. 2 or 2m both mean two minutes. 0 is not saved."""
    text = str(value if value is not None else "").strip()
    compact = re.sub(r"\s+", "", text.lower())
    if re.fullmatch(r"\d+(?:\.\d+)?", compact):
        compact = compact + "m"
    if callout_sell_seconds(compact) <= 0:
        return None
    return compact


def openhold_auto_seconds(text: str | None = None) -> float:
    """Seconds an auto-open may be old. Missing or junk stays at 60."""
    if text is None:
        text = getattr(get_config(), "openhold_auto_age", "1m")
    secs = callout_sell_seconds(str(text or ""))
    return secs if secs > 0 else 60.0


def normalize_callout_sell_age(value) -> str | None:
    """Store '0' for off. None means the text is not an age and must not be saved."""
    text = str(value if value is not None else "").strip()
    if text == "":
        return "0"
    compact = re.sub(r"\s+", "", text.lower())
    if compact in ("0", "0.0", "0s", "0m", "0h", "0d", "off", "none"):
        return "0"
    if callout_sell_seconds(compact) <= 0:
        return None
    return compact


@dataclass
class RuntimeConfig:
    # Filter profile: strict | scalp | pump_curve
    # pump_curve only affects paper path (enforced in main)
    filter_mode: str = "strict"
    # ath = +7% half, +25% quarter, reverse if that quarter does not fill, 35% trail, -6% stop.
    exit_style: str = "ath"
    defaults_version: int = 2

    # Risk / scalp profile
    max_position_sol: float = 0.10
    total_capital_sol: float = 10.0
    max_concurrent: int = 4
    min_liquidity_sol: float = 5.0
    hard_stop_pct: float = -0.35
    runner_stop_pct: float = -0.12
    time_stop_seconds: float = 300.0
    time_stop_min_gain: float = 0.15
    trail_activate_pct: float = 0.80
    trail_drawdown_pct: float = 0.32
    tp_levels: List[float] = field(default_factory=lambda: [0.30, 0.80])
    tp_fractions: List[float] = field(default_factory=lambda: [0.50, 0.50])
    # 0 = off. A named style can arm here: tagged this percent, back under it, sell 100%.
    fallback_arm_pct: float = 0.0

    # Tips
    max_edge_ratio: float = 0.35
    absolute_max_tip_sol: float = 0.015
    base_tip_sol: float = 0.001

    # Pipeline
    max_decodes_per_min: int = 25
    trade_enabled: bool = True  # master switch from UI
    paper_only_force: bool = True  # UI can request paper-only

    # pump_curve extras (paper only)
    pump_curve_min_liq_sol: float = 3.0
    pump_curve_size_mult: float = 0.40
    launch_size_mult: float = 0.5

    # Settings page. Each source runs only when its switch is on.
    # min_m5_pct skips a buy below this. max_m5_pct skips a buy already above it.
    launch_enabled: bool = True
    dayscalp_enabled: bool = False
    predict_enabled: bool = False
    callout_enabled: bool = False
    # OpenHolds only. The Callout page scan still runs so the dropdown has callers.
    opencopy_enabled: bool = False
    # Percent, not a fraction. -25 means a Callout trade at -25% may open the next one.
    callout_next_pct: float = -25.0
    # After the caller has sold and Since Buy has been negative, sell at this percent. Default +7.
    callout_drop_pct: float = 7.0
    # On: caller sold means sell this Callout in full now. Off: under +1% the trade and the pin stay.
    callout_dropoff: bool = False
    # On: Callout and OpenCopy use Live Control trail start and drop-from-high. Off: they do not.
    callout_trailing: bool = False
    # "0" is off. "1h" sells a Callout trade in full once it has been open that long.
    callout_sell_age: str = "0"
    # OpenHolds auto-buy only. A coin older than this is not picked up. A click has no age limit.
    openhold_auto_age: str = "1m"
    # Real-time caller trades from the PumpPortal socket. Off: the polled pump.fun books only.
    copy_stream: bool = True
    # On: the socket seeing the caller sell everything sells this copy in full at once,
    # whatever Dropoff says. That is the exit that keeps a copy from holding the caller's bag.
    copy_follow_exit: bool = True
    # Percent. A Callout/OpenCopy trade at or under this since buy sells in full. 0 is off.
    copy_stop_pct: float = -15.0
    quick_entry: bool = False
    min_m5_pct: float = -6.0
    max_m5_pct: float = 200.0
    rugcheck_enabled: bool = False
    # Buy and sell venue. local | lightning | pump | jupiter. The fallback order follows this choice.
    tx_route: str = "local"
    # Off: the existing take-profit and trail stay. On: sell 100% at the margin, all sources.
    fast_pace_sell: bool = False
    # 0 means use Live Controls max position. This does not change the buy size.
    fast_pace_max_sol: float = 0.0
    # Percent. 5 means sell 100% when Since Buy reaches +5% of what was bought.
    fast_pace_margin_pct: float = 5.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def update_from(self, data: Dict[str, Any]) -> List[str]:
        """Apply partial update; returns list of changed keys."""
        changed: List[str] = []
        for k, v in data.items():
            if not hasattr(self, k):
                continue
            cur = getattr(self, k)
            if isinstance(cur, float):
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    continue
            elif isinstance(cur, int) and not isinstance(cur, bool):
                try:
                    v = int(v)
                except (TypeError, ValueError):
                    continue
            elif isinstance(cur, bool):
                v = bool(v) if not isinstance(v, str) else v.lower() in ("1", "true", "yes", "on")
            elif isinstance(cur, list) and isinstance(v, list):
                try:
                    v = [float(x) for x in v]
                except (TypeError, ValueError):
                    continue
            elif isinstance(cur, str):
                if k == "callout_sell_age":
                    norm = normalize_callout_sell_age(v)
                    if norm is None:
                        continue
                    v = norm
                elif k == "openhold_auto_age":
                    norm = normalize_openhold_auto_age(v)
                    if norm is None:
                        continue
                    v = norm
                elif k == "tx_route":
                    v = str(v or "").strip().lower()
                    if v not in ("local", "lightning", "pump", "jupiter"):
                        continue
                else:
                    v = str(v or "").strip() or cur
            if getattr(self, k) != v:
                setattr(self, k, v)
                changed.append(k)
        return changed


_lock = threading.Lock()
_cfg = RuntimeConfig()


def get_config() -> RuntimeConfig:
    return _cfg


def update_config(data: Dict[str, Any], persist: bool = True) -> List[str]:
    with _lock:
        changed = _cfg.update_from(data)
    if changed and persist:
        try:
            from src.storage.supabase_store import get_store
            get_store().put("runtime_config", snapshot_config())
        except Exception:
            pass
        save_runtime_state()
    return changed


def snapshot_config() -> Dict[str, Any]:
    with _lock:
        return deepcopy(_cfg.to_dict())


def runtime_state_path() -> Path:
    return Path(__file__).resolve().parents[2] / "logs" / "runtime_state.json"


def save_runtime_state(data: Dict[str, Any] | None = None) -> None:
    """Local copy of the live controls. A restart reads this before anything else."""
    payload = data if data is not None else snapshot_config()
    path = runtime_state_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        import logging
        logging.getLogger("sol-sniper.runtime").exception("could not save live settings")


def bot_stopped() -> bool:
    """True after STOP. Scans and new buys stay off until START."""
    return (Path(__file__).resolve().parents[2] / "logs" / "bot_stopped").is_file()


def load_runtime_state() -> Dict[str, Any]:
    path = runtime_state_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def ensure_source_switches(saved: Dict[str, Any] | None = None) -> None:
    """First run after this setting exists follows the mode already selected.

    Launch on and Dayscalp off when the bot was in launch. The other way
    around when it was in dayscalp. Later edits on the Settings page win.
    """
    saved = saved or {}
    cfg = get_config()
    patch: Dict[str, Any] = {}
    if "launch_enabled" not in saved:
        patch["launch_enabled"] = cfg.filter_mode == "launch"
    if "dayscalp_enabled" not in saved:
        patch["dayscalp_enabled"] = cfg.filter_mode == "dayscalp"
    if "predict_enabled" not in saved:
        patch["predict_enabled"] = False
    if "callout_enabled" not in saved:
        patch["callout_enabled"] = False
    if "opencopy_enabled" not in saved:
        patch["opencopy_enabled"] = cfg.filter_mode == "opencopy"
    if "callout_next_pct" not in saved:
        patch["callout_next_pct"] = -25.0
    if "callout_drop_pct" not in saved:
        patch["callout_drop_pct"] = 7.0
    if "callout_dropoff" not in saved:
        patch["callout_dropoff"] = False
    if "callout_trailing" not in saved:
        patch["callout_trailing"] = False
    if "callout_sell_age" not in saved:
        patch["callout_sell_age"] = "0"
    if "openhold_auto_age" not in saved:
        patch["openhold_auto_age"] = "1m"
    if "copy_stream" not in saved:
        patch["copy_stream"] = True
    if "copy_follow_exit" not in saved:
        patch["copy_follow_exit"] = True
    if "copy_stop_pct" not in saved:
        patch["copy_stop_pct"] = -15.0
    if "quick_entry" not in saved:
        patch["quick_entry"] = False
    if "min_m5_pct" not in saved:
        patch["min_m5_pct"] = -6.0
    if "max_m5_pct" not in saved:
        patch["max_m5_pct"] = 200.0
    if "rugcheck_enabled" not in saved:
        patch["rugcheck_enabled"] = False
    if patch:
        update_config(patch)


def apply_scalp_profile() -> None:
    update_config(
        {
            "filter_mode": "scalp",
            "max_position_sol": 0.08,
            "max_concurrent": 3,
            "min_liquidity_sol": 4.0,
            "hard_stop_pct": -0.28,
            "time_stop_seconds": 40.0,
            "time_stop_min_gain": 0.10,
            "trail_activate_pct": 0.18,
            "trail_drawdown_pct": 0.12,
            "tp_levels": [0.22, 0.55],
            "tp_fractions": [0.50, 0.30],
            "absolute_max_tip_sol": 0.012,
        }
    )


LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
TRADE_JOURNAL = LOG_DIR / "trades.jsonl"


def append_trade_journal(record: Dict[str, Any]) -> None:
    try:
        with TRADE_JOURNAL.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
    except Exception:
        pass
