"""Unit tests for Hard Deterministic Filter Stack (F-06)."""

import pytest
from datetime import datetime

from src.decision.hard_filters import HardFilterStack
from src.types.models import (
    AuthorityInfo,
    HolderSnapshot,
    LiquidityInfo,
    TokenCandidate,
    TokenMetadata,
    Verdict,
)


def make_candidate(
    mint_renounced: bool = True,
    freeze_renounced: bool = True,
    has_hooks: bool = False,
    has_tax: bool = False,
    liquidity_sol: float = 12.0,
    top5: float = 20.0,
    top10: float = 30.0,
    largest: float = 5.0,
    sell_sim_failed: bool = False,
    blacklisted: bool = False,
    locked: bool = False,
    lock_days: int = 0,
) -> TokenCandidate:
    return TokenCandidate(
        mint="So11111111111111111111111111111111111111112",
        metadata=TokenMetadata(mint="So11111111111111111111111111111111111111112", symbol="TEST"),
        authorities=AuthorityInfo(
            mint_renounced=mint_renounced,
            freeze_renounced=freeze_renounced,
            has_transfer_hooks=has_hooks,
            has_transfer_tax=has_tax,
        ),
        liquidity=LiquidityInfo(
            initial_sol=liquidity_sol,
            locked=locked,
            lock_duration_days=lock_days if locked else None,
        ),
        holder_snapshot=HolderSnapshot(
            top5_pct=top5,
            top10_pct=top10,
            largest_non_lp_pct=largest,
            unique_holders=120,
        ),
        raw={
            "sell_simulation_failed": sell_sim_failed,
            "deployer_blacklisted": blacklisted,
        },
    )


@pytest.fixture
def filters() -> HardFilterStack:
    return HardFilterStack()


def test_clean_token_passes(filters: HardFilterStack):
    c = make_candidate()
    result = filters.evaluate(c)
    assert result.verdict == Verdict.BUY
    assert result.size_multiplier >= 0.9
    assert result.latency_ms < 5.0  # should be extremely fast
    assert "All hard and soft filters passed" in result.reasons or len(result.reasons) > 0


def test_mint_authority_rejects(filters: HardFilterStack):
    c = make_candidate(mint_renounced=False)
    result = filters.evaluate(c)
    assert result.verdict == Verdict.SKIP
    assert result.filter_tier_failed == "T1"
    assert any("mint_authority" in r for r in result.reasons)


def test_freeze_authority_rejects(filters: HardFilterStack):
    c = make_candidate(freeze_renounced=False)
    result = filters.evaluate(c)
    assert result.verdict == Verdict.SKIP
    assert result.filter_tier_failed == "T1"


def test_transfer_hooks_reject(filters: HardFilterStack):
    c = make_candidate(has_hooks=True)
    result = filters.evaluate(c)
    assert result.verdict == Verdict.SKIP
    assert "sol-snipe-013" in result.kb_refs


def test_low_liquidity_downsizes(filters: HardFilterStack):
    c = make_candidate(liquidity_sol=3.0)
    result = filters.evaluate(c)
    assert result.verdict in (Verdict.DOWNSIZE, Verdict.SKIP)
    assert result.size_multiplier < 1.0


def test_high_concentration_downsizes(filters: HardFilterStack):
    c = make_candidate(top5=55.0)
    result = filters.evaluate(c)
    assert result.verdict == Verdict.DOWNSIZE
    assert result.size_multiplier <= 0.6


def test_sell_simulation_fail_rejects(filters: HardFilterStack):
    c = make_candidate(sell_sim_failed=True)
    result = filters.evaluate(c)
    assert result.verdict == Verdict.SKIP
    assert "sol-snipe-064" in result.kb_refs


def test_liquidity_lock_boosts(filters: HardFilterStack):
    c = make_candidate(locked=True, lock_days=90)
    result = filters.evaluate(c)
    assert result.verdict == Verdict.BUY
    assert result.size_multiplier > 1.0
