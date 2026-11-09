"""Open and close emails through Resend. A missing key never blocks a trade."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional

log = logging.getLogger("sol-sniper.email")

_warned = False
_tasks: set = set()


def _cfg() -> Dict[str, str]:
    return {
        "key": (os.environ.get("RESEND_API_KEY") or "").strip(),
        "from": (os.environ.get("ALERT_FROM_EMAIL") or "").strip(),
        "to": (os.environ.get("ALERT_TO_EMAIL") or "").strip(),
    }


def _mode_label(source: str) -> str:
    s = (source or "").strip().lower()
    if "mayhem" in s:
        return "Mayhem"
    if "opencopy" in s:
        return "OpenCopy"
    if "callout" in s:
        return "Callout"
    if "day" in s:
        return "DayScalp"
    if "launch" in s:
        return "Launch"
    return "Launch" if s != "dayscalp" else "DayScalp"


def _opencopy_user(details: Dict[str, Any]) -> str:
    """Subject extra for an OpenCopy trade: User [opencopy-<caller>]."""
    source = str(details.get("source") or "").strip().lower()
    if "mayhem" in source:
        return " User [mayhem-Mayhem_Mode]"
    if "opencopy" not in source:
        return ""
    caller = str(details.get("callout_user") or "").strip() or "caller"
    return f" User [opencopy-{caller}]"


def _pct(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return "n/a"


def _plain_reason(reason: str) -> str:
    raw = str(reason or "").strip()
    if raw == "m5_null":
        return "5m percent was null"
    if raw == "mayhem_paused":
        return "pump paused this Mayhem coin"
    if raw == "caller_closed":
        return "Caller left and since-buy was at least +1%"
    if raw == "caller_drop_7":
        return "Caller already left. Since-buy came back from negative and touched the recovery percent"
    if raw == "manual_now":
        return "Sold with the -1 button"
    if raw == "callout_age":
        return "Age limit reached"
    if raw.startswith("take_profit"):
        return "Take profit"
    if raw in ("dust_drop", "dust_unsellable", "dust_left"):
        return "Too small to keep"
    return raw or "-"


def _sgt(ts: Optional[float] = None) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S SGT", time.gmtime(float(ts or time.time()) + 8 * 3600))


def _links(mint: str, signature: str) -> str:
    lines = [
        f"Mint: {mint}",
        f"Solscan token: https://solscan.io/token/{mint}",
        f"DexScreener: https://dexscreener.com/solana/{mint}",
    ]
    if mint.endswith("pump"):
        lines.append(f"Pump.fun: https://pump.fun/coin/{mint}")
    sig = (signature or "").strip()
    if sig and not sig.startswith("paper"):
        lines.append(f"Tx: https://solscan.io/tx/{sig}")
    else:
        lines.append("Tx: not confirmed")
    return "\n".join(lines)


def _money(sol: float, usd: float) -> str:
    return f"{sol:.6f} SOL (~${sol * usd:.2f})"


def _whole_tokens(amount: float, decimals: Optional[int] = None) -> float:
    """Human token count. The book stores raw units (pump.fun is 6 decimals)."""
    n = abs(float(amount or 0))
    if n <= 0:
        return 0.0
    d = decimals
    if d is None or int(d) <= 0:
        # A real bag is thousands of tokens. 1e8+ is the raw integer, not the count.
        d = 6 if n >= 1_000_000 else 0
    return n / float(10 ** int(d))


def _fmt_tokens(amount: float, decimals: Optional[int] = None) -> str:
    ui = _whole_tokens(amount, decimals)
    if ui <= 0:
        return "0"
    if ui >= 100:
        return f"{ui:,.2f}"
    if ui >= 1:
        return f"{ui:,.4f}".rstrip("0").rstrip(".")
    text = f"{ui:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def _fmt_price(sol_per_token: float, usd: float) -> str:
    """Fixed decimals. Never scientific notation."""
    px = float(sol_per_token or 0)
    if px <= 0:
        return "n/a"
    sol_txt = f"{px:.12f}".rstrip("0").rstrip(".")
    usd_px = px * float(usd or 0)
    usd_txt = f"{usd_px:.10f}".rstrip("0").rstrip(".")
    if not sol_txt:
        sol_txt = "0"
    if not usd_txt:
        usd_txt = "0"
    return f"{sol_txt} SOL per token (~${usd_txt})"


def _decimals_of(details: Dict[str, Any]) -> Optional[int]:
    raw = details.get("token_decimals")
    try:
        d = int(raw or 0)
    except (TypeError, ValueError):
        d = 0
    return d if d > 0 else None


async def _post(subject: str, body: str) -> None:
    global _warned
    cfg = _cfg()
    if not cfg["key"] or not cfg["from"] or not cfg["to"]:
        if not _warned:
            _warned = True
            log.info(
                "EMAIL off — set RESEND_API_KEY, ALERT_FROM_EMAIL, and ALERT_TO_EMAIL in sniper.yaml"
            )
        return
    sender = cfg["from"]
    if "<" not in sender:
        sender = f"SOL Sniper <{sender}>"
    payload = {
        "from": sender,
        "to": [cfg["to"]],
        "subject": subject,
        "text": body,
    }
    try:
        import httpx
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {cfg['key']}", "Content-Type": "application/json"},
                json=payload,
            )
        if r.status_code >= 300:
            log.warning("EMAIL failed HTTP %s %s", r.status_code, (r.text or "")[:180])
            return
        log.info("EMAIL sent %s", subject)
    except Exception:
        log.warning("EMAIL send failed", exc_info=True)


def _schedule(subject: str, body: str) -> None:
    """Queue the send. Never wait on Resend, and keep the task alive until it finishes."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        log.warning("EMAIL not sent — no event loop (%s)", subject)
        return
    task = loop.create_task(_post(subject, body))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    log.info("EMAIL queued %s", subject)


def _safe_open(details: Dict[str, Any]) -> tuple:
    source = str(details.get("source") or "")
    label = _mode_label(source)
    symbol = str(details.get("symbol") or "")[:16] or str(details.get("mint") or "")[:8]
    name = str(details.get("name") or "")
    caller = str(details.get("callout_user") or "")
    usd = float(details.get("sol_usd") or 150.0)
    entry = float(details.get("entry_sol") or 0)
    fee = float(details.get("entry_fee_sol") or 0)
    tip = float(details.get("entry_tip_sol") or 0)
    dec = _decimals_of(details)
    tokens_ui = _whole_tokens(float(details.get("token_amount") or 0), dec)
    px = (entry / tokens_ui) if tokens_ui > 0 and entry > 0 else 0.0
    subject = f"[{label}]: Order placed — {symbol} opened{_opencopy_user(details)}"
    lines = [
        f"{label} order opened",
        f"Time: {_sgt(details.get('ts'))}",
        f"Token: {symbol}" + (f" ({name})" if name else ""),
        f"Mode: {details.get('mode') or 'live'}",
        f"Source: {source or label}",
        "",
        f"SOL in: {_money(entry, usd)}",
        f"Network + gas: {_money(fee, usd)}",
        f"Tip: {_money(tip, usd)}",
        f"Cost: {_money(entry + fee + tip, usd)}",
        f"Tokens: {_fmt_tokens(float(details.get('token_amount') or 0), dec)}",
        f"Price: {_fmt_price(px, usd)}",
        f"5m at buy: {_pct(details.get('m5_pct'))}",
        f"1h at buy: {_pct(details.get('h1_pct'))}",
    ]
    if label == "Callout":
        lines.extend([
            f"Caller: {caller or 'n/a'}",
            "Entry: Callout buy",
        ])
    elif label == "OpenCopy":
        lines.extend([
            f"Caller: {caller or 'n/a'}",
            "Entry: OpenCopy buy",
        ])
    lines.extend(["", _links(str(details.get("mint") or ""), str(details.get("signature") or ""))])
    return subject, "\n".join(lines)


def _safe_close(details: Dict[str, Any]) -> tuple:
    source = str(details.get("source") or "")
    label = _mode_label(source)
    symbol = str(details.get("symbol") or "")[:16] or str(details.get("mint") or "")[:8]
    name = str(details.get("name") or "")
    caller = str(details.get("callout_user") or "")
    usd = float(details.get("sol_usd") or 150.0)
    pnl = float(details.get("pnl_sol") or 0)
    since = details.get("since_buy_pct")
    ath = details.get("ath_since_pct")
    hold = float(details.get("hold_seconds") or 0)
    sale = str(details.get("sale_label") or "Closed")
    dec = _decimals_of(details)
    raw_tokens = float(details.get("token_amount") or 0)
    tokens_ui = _whole_tokens(raw_tokens, dec)
    entry_sol = float(details.get("entry_sol") or 0)
    exit_sol = float(details.get("exit_sol") or 0)
    entry_px = (entry_sol / tokens_ui) if tokens_ui > 0 and entry_sol > 0 else 0.0
    exit_px = (exit_sol / tokens_ui) if tokens_ui > 0 and exit_sol > 0 else 0.0
    subject = f"[{label}]: Order closed — {symbol} {sale} {pnl:+.4f} SOL{_opencopy_user(details)}"
    since_txt = "n/a" if since is None else f"{float(since):+.2f}%"
    ath_txt = "n/a" if ath is None else f"{max(0.0, float(ath)):+.2f}%"
    lines = [
        f"{label} order closed",
        f"Time: {_sgt(details.get('ts'))}",
        f"Token: {symbol}" + (f" ({name})" if name else ""),
        f"Close: {sale}",
        f"Reason: {_plain_reason(str(details.get('reason') or ''))}",
        f"Mode: {details.get('mode') or 'live'}",
        f"Source: {source or label}",
    ]
    if label == "Callout":
        lines.append(f"Caller: {caller or 'n/a'}")
        lines.append("Exit: Callout sell")
    elif label == "OpenCopy":
        lines.append(f"Caller: {caller or 'n/a'}")
        lines.append("Exit: OpenCopy sell")
    lines.extend([
        "",
        f"SOL in: {_money(entry_sol, usd)}",
        f"Buy fees: {_money(float(details.get('entry_fee_sol') or 0), usd)}",
        f"Buy tip: {_money(float(details.get('entry_tip_sol') or 0), usd)}",
        f"SOL out: {_money(exit_sol, usd)}",
        f"Sell fees: {_money(float(details.get('exit_fee_sol') or 0), usd)}",
        f"Net PnL: {_money(pnl, usd)}",
        "",
        f"Since buy: {since_txt}",
        f"ATH since buy: {ath_txt}",
        f"Low since buy: {_pct(details.get('low_since_pct'))}",
        f"5m at exit: {_pct(details.get('m5_pct'))}",
        f"1h at exit: {_pct(details.get('h1_pct'))}",
        f"Held: {int(hold)}s",
        f"Tokens sold: {_fmt_tokens(raw_tokens, dec)}",
        f"Entry price: {_fmt_price(entry_px, usd)}",
        f"Exit price: {_fmt_price(exit_px, usd)}",
        "",
        _links(str(details.get("mint") or ""), str(details.get("signature") or "")),
    ])
    return subject, "\n".join(lines)


def notify_open(details: Dict[str, Any]) -> None:
    """Fire after a buy is recorded. Does not wait for Resend, and never blocks the trade."""
    try:
        subject, body = _safe_open(details or {})
    except Exception:
        log.warning("EMAIL open body failed — sending a short note", exc_info=True)
        symbol = str((details or {}).get("symbol") or "")[:16]
        source = str((details or {}).get("source") or "")
        label = _mode_label(source)
        subject = f"[{label}]: Order placed — {symbol or 'token'} opened{_opencopy_user(details or {})}"
        body = f"Order opened\nToken: {symbol}\nMint: {(details or {}).get('mint') or ''}"
    _schedule(subject, body)


def notify_close(details: Dict[str, Any]) -> None:
    """Fire after a sell is recorded. A dust leftover that was not booked does not call this."""
    try:
        subject, body = _safe_close(details or {})
    except Exception:
        log.warning("EMAIL close body failed — sending a short note", exc_info=True)
        symbol = str((details or {}).get("symbol") or "")[:16]
        source = str((details or {}).get("source") or "")
        label = _mode_label(source)
        subject = f"[{label}]: Order closed — {symbol or 'token'}{_opencopy_user(details or {})}"
        body = f"Order closed\nToken: {symbol}\nMint: {(details or {}).get('mint') or ''}"
    _schedule(subject, body)
