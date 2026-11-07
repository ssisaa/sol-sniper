"""
Shared domain models for SOL-SNIPER v1.2
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class Verdict(str, Enum):
    BUY = "BUY"
    SKIP = "SKIP"
    DOWNSIZE = "DOWNSIZE"


class CompetitionLevel(str, Enum):
    LOW = "LOW"
    MED = "MED"
    HIGH = "HIGH"
    EXTREME = "EXTREME"


class RegimeSession(str, Enum):
    US_EU_OVERLAP = "US_EU_OVERLAP"
    ASIA_ONLY = "ASIA_ONLY"
    WEEKEND = "WEEKEND"
    OTHER = "OTHER"


class KnowledgeUnit(BaseModel):
    id: str
    source: str = "solana-snipe-research-2026"
    topic: str = "SolanaNative"
    conditions: List[str] = Field(default_factory=list)
    content: str
    priority: int = Field(ge=1, le=5)
    tags: List[str] = Field(default_factory=list)
    version: int = 1
    added_at: Optional[datetime] = None
    last_verified_at: Optional[datetime] = None
    attribution_pnl: float = 0.0
    attribution_trades: int = 0


class TokenMetadata(BaseModel):
    mint: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    description: Optional[str] = None
    image_uri: Optional[str] = None
    creator: Optional[str] = None
    twitter: Optional[str] = None
    telegram: Optional[str] = None
    website: Optional[str] = None


class LiquidityInfo(BaseModel):
    initial_sol: float
    pool_address: Optional[str] = None
    dex: str = "raydium"  # raydium | pump_swap | pump_fun
    locked: bool = False
    lock_duration_days: Optional[int] = None


class AuthorityInfo(BaseModel):
    mint_authority: Optional[str] = None
    freeze_authority: Optional[str] = None
    mint_renounced: bool = False
    freeze_renounced: bool = False
    has_transfer_hooks: bool = False
    has_transfer_tax: bool = False


class HolderSnapshot(BaseModel):
    top5_pct: float = 0.0
    top10_pct: float = 0.0
    largest_non_lp_pct: float = 0.0
    unique_holders: int = 0
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TokenCandidate(BaseModel):
    """Normalized event that reaches the Decision Engine after L1/L2 pre-filter."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    mint: str
    metadata: TokenMetadata
    authorities: AuthorityInfo
    liquidity: LiquidityInfo
    holder_snapshot: Optional[HolderSnapshot] = None
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    slot: Optional[int] = None
    signature: Optional[str] = None
    source_event: str = "unknown"  # pump_graduation | raydium_pool | add_liquidity
    raw: Dict[str, Any] = Field(default_factory=dict)


class HardFilterResult(BaseModel):
    verdict: Verdict
    size_multiplier: float = 1.0
    reasons: List[str] = Field(default_factory=list)
    kb_refs: List[str] = Field(default_factory=list)
    latency_ms: float = 0.0
    filter_tier_failed: Optional[str] = None  # T1 | T2 | None


class RAGScore(BaseModel):
    size_multiplier: float = 1.0
    ranking_score: float = 0.0
    retrieved_units: List[str] = Field(default_factory=list)
    reasoning: Optional[str] = None
    latency_ms: float = 0.0


class Decision(BaseModel):
    candidate_id: str
    mint: str
    hard: HardFilterResult
    rag: Optional[RAGScore] = None
    final_size_sol: float = 0.0
    final_verdict: Verdict
    competition: CompetitionLevel = CompetitionLevel.MED
    regime: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class PositionSide(str, Enum):
    LONG = "LONG"


class PositionStatus(str, Enum):
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class Position(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    mint: str
    entry_price: float
    entry_sol: float
    entry_slot: Optional[int] = None
    entry_time: datetime = Field(default_factory=datetime.utcnow)
    size_tokens: float = 0.0
    status: PositionStatus = PositionStatus.OPEN
    peak_price: float = 0.0
    trailing_stop_price: Optional[float] = None
    take_profit_levels_hit: List[float] = Field(default_factory=list)
    exit_reason: Optional[str] = None
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    realized_pnl_sol: float = 0.0
    fees_tips_sol: float = 0.0
    kb_units_used: List[str] = Field(default_factory=list)
    decision_id: Optional[str] = None


class TradeAttribution(BaseModel):
    position_id: str
    mint: str
    net_pnl_sol: float
    fees_tips_sol: float
    kb_units: List[str]
    hard_filter_refs: List[str]
    competition: CompetitionLevel
    regime: Dict[str, Any]
    hold_seconds: float
    closed_at: datetime = Field(default_factory=datetime.utcnow)


class RegimeVector(BaseModel):
    session: RegimeSession = RegimeSession.OTHER
    sol_volatility: str = "medium"  # low | medium | high
    congestion: str = "low"         # low | medium | high
    trench_activity: str = "medium"
    competition: CompetitionLevel = CompetitionLevel.MED
    timestamp: datetime = Field(default_factory=datetime.utcnow)
