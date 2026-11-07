"""
Load SOL-SNIPER settings from config/sniper.yaml (or --config path).

Does not override already-set environment variables (env wins).
Safe: never logs private keys.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger("sol-sniper.config")

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATHS = [
    ROOT / "config" / "sniper.yaml",
    ROOT / "sniper.yaml",
    Path.cwd() / "config" / "sniper.yaml",
    Path.cwd() / "sniper.yaml",
]


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError:
        # Minimal fallback: no PyYAML — try JSON
        import json
        if path.suffix == ".json":
            return json.loads(path.read_text(encoding="utf-8"))
        log.warning("PyYAML not installed; pip install pyyaml  — config file ignored")
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def resolve_keypair_path(raw: str) -> str:
    """
    The folder above sol-sniper can change (/mnt/f, /home, D:).
    The file inside sol-sniper does not. "/sol-sniper/wallet.json" and
    "/mnt/f/sol-sniper/wallet.json" both mean <this project>/wallet.json.
    """
    text = (raw or "").strip().strip('"').strip("'")
    if not text:
        return ""
    norm = text.replace("\\", "/")
    low = norm.lower()
    rel = ""
    mark = "/sol-sniper/"
    at = low.rfind(mark)
    if at >= 0:
        rel = norm[at + len(mark):]
    elif low.startswith("sol-sniper/"):
        rel = norm[len("sol-sniper/"):]
    elif not Path(text).is_absolute():
        rel = norm
    if rel:
        return str((ROOT / rel).resolve())
    p = Path(text)
    if p.is_file():
        return str(p.resolve())
    return str((ROOT / p.name).resolve())


def _set_env_if_empty(key: str, value: Any) -> None:
    if value is None or value == "":
        return
    if os.environ.get(key, "").strip():
        return  # env already set — do not override
    if isinstance(value, bool):
        os.environ[key] = "1" if value else "0"
    else:
        os.environ[key] = str(value)


def apply_config_file(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load YAML config and map into env + return full dict for runtime use.
    """
    cfg_path: Optional[Path] = None
    if path:
        cfg_path = Path(path).expanduser().resolve()
    else:
        for p in DEFAULT_CONFIG_PATHS:
            if p.is_file():
                cfg_path = p
                break

    if not cfg_path or not cfg_path.is_file():
        log.info("No config file found (optional). Using env / defaults.")
        return {}

    data = _load_yaml(cfg_path)
    log.info("Loaded config: %s", cfg_path)

    wallet = data.get("wallet") or {}
    _set_env_if_empty("SOL_SNIPER_LIVE", wallet.get("live"))
    kp = wallet.get("keypair_path")
    if kp:
        os.environ["SOL_SNIPER_KEYPAIR"] = resolve_keypair_path(str(kp))
    if wallet.get("private_key"):
        _set_env_if_empty("SOL_SNIPER_PRIVATE_KEY", wallet.get("private_key"))
    _set_env_if_empty("SOL_SNIPER_ALLOW_LAUNCH", wallet.get("allow_launch"))

    rpc = data.get("rpc") or {}
    if rpc.get("primary"):
        os.environ["SOLANA_RPC_URL"] = str(rpc.get("primary")).strip()
        host = str(rpc.get("primary")).split("//")[-1].split("/")[0].split("?")[0]
        log.info("BUY/SELL RPC is the paid primary %s — public RPCs are for scans only", host)
    urls = rpc.get("urls") or []
    if urls and not os.environ.get("SOLANA_RPC_URL"):
        _set_env_if_empty("SOLANA_RPC_URL", urls[0])
    if urls:
        _set_env_if_empty("SOLANA_RPC_URLS", ",".join(str(u) for u in urls))

    jup = data.get("jupiter") or {}
    _set_env_if_empty("JUPITER_API_KEY", jup.get("api_key"))

    portal = data.get("pumpportal") or {}
    _set_env_if_empty("PUMPPORTAL_API_KEY", portal.get("api_key"))

    jito = data.get("jito") or {}
    if "enabled" in jito:
        _set_env_if_empty("SOL_SNIPER_JITO", jito.get("enabled"))

    email = data.get("email") or {}
    _set_env_if_empty("RESEND_API_KEY", email.get("resend_api_key") or email.get("RESEND_API_KEY"))
    _set_env_if_empty("ALERT_FROM_EMAIL", email.get("from_email") or email.get("ALERT_FROM_EMAIL"))
    _set_env_if_empty("ALERT_TO_EMAIL", email.get("to_email") or email.get("ALERT_TO_EMAIL"))

    trading = data.get("trading") or {}
    if trading.get("mode"):
        _set_env_if_empty("SOL_SNIPER_TRADE_MODE", trading.get("mode"))
    if trading.get("filter_mode"):
        _set_env_if_empty("SOL_SNIPER_FILTER_MODE", trading.get("filter_mode"))

    return data


def push_runtime_from_config(data: Dict[str, Any]) -> None:
    """Apply trading/risk/dayscalp sections into RuntimeConfig."""
    if not data:
        return
    from src.config.runtime import update_config

    trading = data.get("trading") or {}
    risk = data.get("risk") or {}
    patch: Dict[str, Any] = {}
    for k in (
        "filter_mode",
        "max_position_sol",
        "max_concurrent",
        "min_liquidity_sol",
        "trade_enabled",
    ):
        if k in trading and trading[k] is not None:
            patch[k] = trading[k]
    portal = data.get("pumpportal") or {}
    if portal.get("route"):
        patch["tx_route"] = str(portal.get("route")).strip().lower()
    for k in (
        "hard_stop_pct",
        "time_stop_seconds",
        "time_stop_min_gain",
        "trail_activate_pct",
        "trail_drawdown_pct",
        "tp_levels",
        "tp_fractions",
        "absolute_max_tip_sol",
    ):
        if k in risk and risk[k] is not None:
            patch[k] = risk[k]
    if patch:
        update_config(patch, persist=False)
        log.info("Runtime config updated from file: %s", list(patch.keys()))


EXIT_KEYS = (
    "hard_stop_pct",
    "runner_stop_pct",
    "time_stop_seconds",
    "time_stop_min_gain",
    "trail_activate_pct",
    "trail_drawdown_pct",
    "tp_levels",
    "tp_fractions",
    "fallback_arm_pct",
    "stop_pct",
)

DEFAULT_EXIT = {
    "hard_stop_pct": -0.35,
    "runner_stop_pct": -0.12,
    "time_stop_seconds": 300,
    "time_stop_min_gain": 0.15,
    "trail_activate_pct": 0.80,
    "trail_drawdown_pct": 0.32,
    "tp_levels": [0.30, 0.80],
    "tp_fractions": [0.50, 0.50],
    "fallback_arm_pct": 0.0,
    "stop_pct": -0.35,
}

PLAN_EXIT = {
    "hard_stop_pct": -0.12,
    "runner_stop_pct": 0.0,
    "time_stop_seconds": 60,
    "time_stop_min_gain": 0.10,
    "trail_activate_pct": 0.25,
    "trail_drawdown_pct": 0.30,
    "tp_levels": [0.25],
    "tp_fractions": [0.50],
    "fallback_arm_pct": 0.0,
    "stop_pct": -0.12,
}

ATH_EXIT = {
    "hard_stop_pct": -0.06,
    "runner_stop_pct": 0.05,
    "time_stop_seconds": 45,
    "time_stop_min_gain": 0.03,
    "trail_activate_pct": 0.22,
    "trail_drawdown_pct": 0.35,
    "tp_levels": [0.07, 0.25, 0.75],
    "tp_fractions": [0.50, 0.25, 0.25],
    "fallback_arm_pct": 0.0,
    "stop_pct": -0.06,
}

CLASSIC_EXIT = dict(DEFAULT_EXIT)
BUILTIN_STYLES = ("ath", "plan", "classic")
_BUILTIN_BASE = {"ath": ATH_EXIT, "plan": PLAN_EXIT, "classic": CLASSIC_EXIT}


def config_path() -> Optional[Path]:
    for p in DEFAULT_CONFIG_PATHS:
        if p.is_file():
            return p
    return None


def load_config() -> Dict[str, Any]:
    path = config_path()
    if not path:
        return {}
    return _load_yaml(path)


def style_profile(style: str) -> Dict[str, Any]:
    """ath is the default. plan and classic are the old built-ins. A saved name loads that preset."""
    key = (style or "ath").strip() or "ath"
    custom = _read_presets()
    if key in custom and key.lower() not in BUILTIN_STYLES:
        src = custom[key]
        out = dict(ATH_EXIT)
        for field in EXIT_KEYS:
            if src.get(field) is not None:
                out[field] = src[field]
        for field in ("max_position_sol", "min_liquidity_sol", "absolute_max_tip_sol", "max_concurrent", "filter_mode"):
            if src.get(field) is not None:
                out[field] = src[field]
        out["exit_style"] = key
        return out
    name = key if key in BUILTIN_STYLES else "ath"
    data = load_config()
    styles = data.get("exit_styles") if isinstance(data.get("exit_styles"), dict) else {}
    src = styles.get(name) if isinstance(styles.get(name), dict) else {}
    out = dict(_BUILTIN_BASE[name])
    for field in EXIT_KEYS:
        if src.get(field) is not None:
            out[field] = src[field]
    out["exit_style"] = name
    return out


def presets_path() -> Path:
    return Path(__file__).resolve().parents[2] / "logs" / "exit_presets.json"


def _read_presets() -> Dict[str, Any]:
    path = presets_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def list_user_presets() -> list:
    """Named styles the user saved. Built-ins are not in this list."""
    out = []
    for key, row in _read_presets().items():
        if str(key).lower() in BUILTIN_STYLES:
            continue
        name = str((row or {}).get("name") or key)
        out.append({"id": key, "name": name})
    return out


def preset_slug(name: str) -> str:
    raw = " ".join(str(name or "").strip().split())
    raw = "".join(ch for ch in raw if ch.isalnum() or ch in " -_")
    return raw[:32].strip()


def save_user_preset(name: str, values: Dict[str, Any]) -> str:
    """Save a named style. Refuses ath, plan, and classic. Returns the id, or '' if refused."""
    slug = preset_slug(name)
    if not slug or slug.lower() in BUILTIN_STYLES:
        log.warning("Preset '%s' is empty or a built-in default — not saved", name)
        return ""
    row: Dict[str, Any] = {"name": slug}
    for field in EXIT_KEYS:
        if values.get(field) is not None:
            row[field] = values[field]
    for field in ("max_position_sol", "min_liquidity_sol", "absolute_max_tip_sol", "max_concurrent", "filter_mode"):
        if values.get(field) is not None:
            row[field] = values[field]
    data = _read_presets()
    data[slug] = row
    path = presets_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    log.info("Saved exit preset '%s' (built-in defaults were not changed)", slug)
    return slug


def mode_exit_profile(mode: str) -> Dict[str, Any]:
    """Exit numbers for launch or dayscalp. The selected style wins over the mode block."""
    if mode in ("launch", "dayscalp"):
        from src.config.runtime import get_config
        style = getattr(get_config(), "exit_style", None) or "ath"
        return style_profile(style)
    data = load_config()
    src = data.get(mode) if isinstance(data.get(mode), dict) else {}
    out = dict(DEFAULT_EXIT)
    for key in EXIT_KEYS:
        if isinstance(src, dict) and src.get(key) is not None:
            out[key] = src[key]
    return out


def profile_for_ui(mode: str) -> Dict[str, Any]:
    """File defaults for the selected filter. Apply never writes these."""
    data = load_config()
    trading = data.get("trading") if isinstance(data.get("trading"), dict) else {}
    risk = data.get("risk") if isinstance(data.get("risk"), dict) else {}
    base = {
        "filter_mode": mode,
        "max_position_sol": trading.get("max_position_sol", 0.015),
        "max_concurrent": trading.get("max_concurrent", 3),
        "min_liquidity_sol": trading.get("min_liquidity_sol", 2.0),
        "absolute_max_tip_sol": risk.get("absolute_max_tip_sol", 0.015),
        "trade_enabled": True,
    }
    if mode == "scalp":
        return {
            "filter_mode": "scalp",
            "max_position_sol": 0.08,
            "max_concurrent": 3,
            "min_liquidity_sol": 4.0,
            "hard_stop_pct": -0.28,
            "runner_stop_pct": -0.12,
            "time_stop_seconds": 40.0,
            "time_stop_min_gain": 0.10,
            "trail_activate_pct": 0.18,
            "trail_drawdown_pct": 0.12,
            "tp_levels": [0.22, 0.55],
            "tp_fractions": [0.50, 0.30],
            "absolute_max_tip_sol": 0.012,
            "trade_enabled": True,
        }
    if mode in ("launch", "dayscalp"):
        out = {**base, **style_profile("ath"), "filter_mode": mode, "exit_style": "ath"}
        block = data.get(mode) if isinstance(data.get(mode), dict) else {}
        if block.get("max_position_sol") is not None:
            out["max_position_sol"] = block["max_position_sol"]
        return out
    out = {**base}
    for key in EXIT_KEYS:
        if risk.get(key) is not None:
            out[key] = risk[key]
    out.setdefault("runner_stop_pct", DEFAULT_EXIT["runner_stop_pct"])
    return out


def save_mode_exits(mode: str, values: Dict[str, Any]) -> bool:
    """Write exit numbers into that mode's yaml section only. Other sections stay."""
    if mode not in ("launch", "dayscalp"):
        return False
    path = config_path()
    if not path:
        return False
    lines = path.read_text(encoding="utf-8").splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith(f"{mode}:"):
            start = i
            break
    if start is None:
        lines.append("")
        lines.append(f"{mode}:")
        start = len(lines) - 1
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if lines[i] and not lines[i].startswith(" ") and not lines[i].startswith("#"):
            end = i
            break
    def fmt(key, val):
        if key in ("tp_levels", "tp_fractions"):
            nums = ", ".join(f"{float(x):g}" for x in val)
            return f"  {key}: [{nums}]"
        if key == "time_stop_seconds":
            return f"  {key}: {int(float(val))}"
        return f"  {key}: {float(val):g}"
    for key in EXIT_KEYS:
        if key not in values or values[key] is None:
            continue
        rendered = fmt(key, values[key])
        replaced = False
        for i in range(start + 1, end):
            stripped = lines[i].lstrip()
            if stripped.startswith(f"{key}:"):
                lines[i] = rendered
                replaced = True
                break
        if not replaced:
            lines.insert(end, rendered)
            end += 1
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("Saved %s exits to %s", mode, path.name)
    return True
