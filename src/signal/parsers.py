"""
L2 Signal parsers – turn raw FeedEvents into TokenCandidate-like signals.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.data.ws_feed import FeedEvent, FeedEventType
from src.types.models import (
    AuthorityInfo,
    LiquidityInfo,
    TokenCandidate,
    TokenMetadata,
)

log = logging.getLogger("sol-sniper.signal")

# Very lightweight heuristics from log lines
INIT_RE = re.compile(r"Instruction:\s*(Initialize2|Initialize|CreatePool|Create)", re.I)
BUY_RE = re.compile(r"Instruction:\s*(Buy|SwapBaseIn|Swap)", re.I)
ADD_LIQ_RE = re.compile(r"Instruction:\s*(AddLiquidity|Deposit)", re.I)


def parse_logs_to_candidate(event: FeedEvent) -> Optional[TokenCandidate]:
    """
    Best-effort conversion of a logsNotification into a TokenCandidate.

    Real production parsing requires full transaction fetch + instruction decoding.
    This version extracts high-signal hints so the rest of the pipeline can be tested.
    """
    if event.type not in (
        FeedEventType.RAYDIUM_POOL,
        FeedEventType.PUMP_FUN,
        FeedEventType.LOGS,
        FeedEventType.ADD_LIQUIDITY,
    ):
        return None

    logs: List[str] = event.data.get("logs") or []
    instructions = event.data.get("instructions") or []
    joined = " ".join(logs + instructions)

    source = "unknown"
    if event.type == FeedEventType.RAYDIUM_POOL or "ray" in joined.lower():
        source = "raydium_pool"
    elif event.type == FeedEventType.PUMP_FUN or "pump" in joined.lower():
        source = "pump_fun"

    # Only emit candidates for init / create style events for now
    is_init = bool(INIT_RE.search(joined))
    is_add_liq = bool(ADD_LIQ_RE.search(joined))
    if not (is_init or is_add_liq):
        return None

    # Placeholder mint – in production we decode the account keys / inner instructions
    mint_hint = event.signature or f"unknown-{event.slot}"

    candidate = TokenCandidate(
        mint=mint_hint[:44],
        metadata=TokenMetadata(
            mint=mint_hint[:44],
            symbol="DETECT",
            name=f"Detected {source}",
        ),
        authorities=AuthorityInfo(
            mint_renounced=False,  # unknown until full tx decode
            freeze_renounced=False,
        ),
        liquidity=LiquidityInfo(
            initial_sol=0.0,  # unknown until full decode
            dex="raydium" if "raydium" in source else "pump_fun",
        ),
        detected_at=event.timestamp or datetime.now(timezone.utc),
        slot=event.slot,
        signature=event.signature,
        source_event=source,
        raw={
            "logs_preview": logs[:8],
            "instructions": instructions,
            "feed_type": event.type.value,
            "hint": event.data.get("hint"),
        },
    )
    return candidate


def summarize_event(event: FeedEvent) -> str:
    if event.type == FeedEventType.SLOT:
        return f"slot={event.slot}"
    sig = (event.signature or "")[:18]
    return (
        f"{event.type.value} slot={event.slot} sig={sig} "
        f"prog={(event.program_id or '')[:10]} "
        f"instr={len(event.data.get('instructions') or [])}"
    )
