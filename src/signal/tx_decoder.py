"""
Full transaction decoder for SOL-SNIPER.

Takes a transaction signature, fetches the parsed transaction via RPC,
and extracts:
  - mint address
  - mint / freeze authority (renounced or not)
  - initial SOL liquidity
  - creator / fee-payer
  - DEX source (raydium / pump_fun)
  - decimals, supply hints, etc.

Uses jsonParsed encoding so we can read spl-token InitializeMint and
system transfers without a full binary decoder.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.data.rpc_client import SolanaRPCClient
from src.types.models import (
    AuthorityInfo,
    LiquidityInfo,
    TokenCandidate,
    TokenMetadata,
)

log = logging.getLogger("sol-sniper.decoder")

# Program IDs
SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
RAYDIUM_AMM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
ASSOCIATED_TOKEN = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

LAMPORTS_PER_SOL = 1_000_000_000


@dataclass
class DecodedMint:
    mint: str
    decimals: int = 6
    mint_authority: Optional[str] = None
    freeze_authority: Optional[str] = None
    mint_renounced: bool = False
    freeze_renounced: bool = False
    supply: Optional[int] = None


@dataclass
class DecodedTx:
    signature: str
    slot: Optional[int] = None
    block_time: Optional[int] = None
    fee_payer: Optional[str] = None
    success: bool = True
    mints: List[DecodedMint] = field(default_factory=list)
    sol_transferred: float = 0.0          # total SOL moved via System Program
    sol_to_program: float = 0.0           # SOL that landed in Raydium/pump accounts (approx liquidity)
    source: str = "unknown"               # raydium_pool | pump_fun | unknown
    instructions_summary: List[str] = field(default_factory=list)
    raw_meta: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


def _flatten_instructions(tx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return outer + inner instructions as a flat list of parsed dicts."""
    message = tx.get("transaction", {}).get("message", {})
    outer = message.get("instructions") or []
    meta = tx.get("meta") or {}
    inner_groups = meta.get("innerInstructions") or []

    result = list(outer)
    for group in inner_groups:
        for ix in group.get("instructions") or []:
            result.append(ix)
    return result


def _parse_token_initialize(ix: Dict[str, Any]) -> Optional[DecodedMint]:
    """Parse spl-token InitializeMint / InitializeMint2 (jsonParsed)."""
    parsed = ix.get("parsed")
    if not parsed or not isinstance(parsed, dict):
        return None
    if parsed.get("type") not in ("initializeMint", "initializeMint2"):
        return None

    info = parsed.get("info") or {}
    mint = info.get("mint")
    if not mint:
        return None

    mint_auth = info.get("mintAuthority")
    freeze_auth = info.get("freezeAuthority")
    decimals = int(info.get("decimals", 6))

    return DecodedMint(
        mint=mint,
        decimals=decimals,
        mint_authority=mint_auth,
        freeze_authority=freeze_auth,
        mint_renounced=mint_auth is None or mint_auth == "",
        freeze_renounced=freeze_auth is None or freeze_auth == "",
    )


def _sol_from_system_transfer(ix: Dict[str, Any]) -> Tuple[float, Optional[str], Optional[str]]:
    """Return (sol_amount, source, destination) for System Program transfer."""
    parsed = ix.get("parsed")
    if not parsed or not isinstance(parsed, dict):
        return 0.0, None, None
    if parsed.get("type") != "transfer":
        return 0.0, None, None
    info = parsed.get("info") or {}
    lamports = int(info.get("lamports") or 0)
    return lamports / LAMPORTS_PER_SOL, info.get("source"), info.get("destination")


def decode_transaction_json(tx_json: Dict[str, Any], signature: str) -> DecodedTx:
    """Pure function: parsed getTransaction result → DecodedTx."""
    if not tx_json:
        return DecodedTx(signature=signature, success=False, error="null transaction")

    meta = tx_json.get("meta") or {}
    err = meta.get("err")
    decoded = DecodedTx(
        signature=signature,
        slot=tx_json.get("slot"),
        block_time=tx_json.get("blockTime"),
        success=err is None,
        error=str(err) if err else None,
        raw_meta={"fee": meta.get("fee"), "err": err},
    )

    message = tx_json.get("transaction", {}).get("message", {})
    account_keys = message.get("accountKeys") or []
    if account_keys:
        first = account_keys[0]
        decoded.fee_payer = first.get("pubkey") if isinstance(first, dict) else first

    instructions = _flatten_instructions(tx_json)
    program_ids_seen = set()

    for ix in instructions:
        program_id = ix.get("programId") or ix.get("program")
        if isinstance(program_id, dict):
            program_id = program_id.get("pubkey") or program_id.get("id")
        if program_id:
            program_ids_seen.add(program_id)

        # Token InitializeMint
        if program_id in (TOKEN_PROGRAM, TOKEN_2022, "spl-token", "spl-token-2022"):
            mint_info = _parse_token_initialize(ix)
            if mint_info:
                decoded.mints.append(mint_info)
                decoded.instructions_summary.append(
                    f"InitializeMint {mint_info.mint[:8]}... dec={mint_info.decimals} "
                    f"mintAuth={'None' if mint_info.mint_renounced else 'set'} "
                    f"freezeAuth={'None' if mint_info.freeze_renounced else 'set'}"
                )

        # System transfers (liquidity approximation)
        if program_id in (SYSTEM_PROGRAM, "system"):
            sol, src, dst = _sol_from_system_transfer(ix)
            if sol > 0:
                decoded.sol_transferred += sol
                # Heuristic: larger transfers toward program-owned accounts ≈ liquidity
                if sol >= 0.1:
                    decoded.sol_to_program += sol
                    decoded.instructions_summary.append(f"SOL transfer {sol:.3f}")

        # Program presence
        if program_id == RAYDIUM_AMM_V4:
            decoded.source = "raydium_pool"
        elif program_id == PUMP_FUN:
            decoded.source = "pump_fun"

        # jsonParsed type hints
        parsed = ix.get("parsed")
        if isinstance(parsed, dict):
            t = parsed.get("type")
            if t:
                decoded.instructions_summary.append(str(t))

    # Prefer the first newly initialized mint
    return decoded


async def fetch_and_decode(
    rpc: SolanaRPCClient,
    signature: str,
    commitment: str = "confirmed",
) -> DecodedTx:
    """Fetch transaction by signature and decode it."""
    resp = await rpc.get_transaction(signature, commitment=commitment)
    if resp.error:
        return DecodedTx(signature=signature, success=False, error=resp.error)
    if not resp.result:
        return DecodedTx(signature=signature, success=False, error="transaction not found / not yet available")
    return decode_transaction_json(resp.result, signature)


def decoded_to_candidate(decoded: DecodedTx) -> Optional[TokenCandidate]:
    """Convert a successfully decoded tx into a TokenCandidate for the decision engine."""
    if not decoded.success or not decoded.mints:
        return None

    primary = decoded.mints[0]
    liq_sol = max(decoded.sol_to_program, decoded.sol_transferred * 0.5)  # heuristic

    return TokenCandidate(
        mint=primary.mint,
        metadata=TokenMetadata(
            mint=primary.mint,
            symbol=None,
            name=None,
            creator=decoded.fee_payer,
        ),
        authorities=AuthorityInfo(
            mint_authority=primary.mint_authority,
            freeze_authority=primary.freeze_authority,
            mint_renounced=primary.mint_renounced,
            freeze_renounced=primary.freeze_renounced,
        ),
        liquidity=LiquidityInfo(
            initial_sol=round(liq_sol, 4),
            dex=decoded.source if decoded.source != "unknown" else "unknown",
        ),
        detected_at=datetime.now(timezone.utc),
        slot=decoded.slot,
        signature=decoded.signature,
        source_event=decoded.source,
        raw={
            "fee_payer": decoded.fee_payer,
            "sol_transferred": decoded.sol_transferred,
            "sol_to_program": decoded.sol_to_program,
            "instructions": decoded.instructions_summary[:12],
            "mints_found": len(decoded.mints),
            "decimals": primary.decimals,
        },
    )
