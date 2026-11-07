"""
F-11 Edge-Aware Tip Model (SOL-SNIPER v1.2)

Never pay more than max_edge_ratio (default 35%) of expected edge.
Dynamically scales with competition level and recent tip market.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from src.types.models import CompetitionLevel

log = logging.getLogger("sol-sniper.tip")


@dataclass
class TipDecision:
    tip_lamports: int
    tip_sol: float
    reason: str
    expected_edge_sol: float
    max_allowed_sol: float
    competition: CompetitionLevel


class EdgeAwareTipModel:
    """
    Calculates the maximum tip the bot is willing to pay for a given setup.

    Formula:
        max_tip = expected_edge_sol * max_edge_ratio

    Then clamped by absolute ceilings and adjusted by competition.
    """

    def __init__(
        self,
        max_edge_ratio: float = 0.35,
        base_tip_sol: float = 0.001,
        absolute_max_tip_sol: float = 0.02,
        min_tip_sol: float = 0.0001,
    ):
        self.max_edge_ratio = max_edge_ratio
        self.base_tip_sol = base_tip_sol
        self.absolute_max_tip_sol = absolute_max_tip_sol
        self.min_tip_sol = min_tip_sol

    def compute(
        self,
        expected_edge_sol: float,
        competition: CompetitionLevel = CompetitionLevel.MED,
        recent_p75_tip_sol: Optional[float] = None,
        liquidity_quality: float = 1.0,  # 0.0–1.5 multiplier from hard filters
    ) -> TipDecision:
        """
        Returns a TipDecision. If expected edge is too small, tip will be near zero
        (effectively skipping the opportunity if tip cannot be competitive).
        """
        if expected_edge_sol <= 0:
            return TipDecision(
                tip_lamports=0,
                tip_sol=0.0,
                reason="non-positive expected edge → no tip",
                expected_edge_sol=expected_edge_sol,
                max_allowed_sol=0.0,
                competition=competition,
            )

        max_by_edge = expected_edge_sol * self.max_edge_ratio

        # Competition multiplier
        comp_mult = {
            CompetitionLevel.LOW: 0.6,
            CompetitionLevel.MED: 1.0,
            CompetitionLevel.HIGH: 1.4,
            CompetitionLevel.EXTREME: 1.8,
        }.get(competition, 1.0)

        # Start from base, scale by competition and liquidity quality
        desired = self.base_tip_sol * comp_mult * max(0.5, min(1.5, liquidity_quality))

        # Respect recent market if provided
        if recent_p75_tip_sol is not None and recent_p75_tip_sol > 0:
            desired = max(desired, recent_p75_tip_sol * 0.9)

        # Hard cap by edge
        tip_sol = min(desired, max_by_edge, self.absolute_max_tip_sol)
        tip_sol = max(self.min_tip_sol, tip_sol) if tip_sol > 0 else 0.0

        # If the edge-capped tip is far below market, better to skip
        if (
            recent_p75_tip_sol
            and tip_sol < recent_p75_tip_sol * 0.5
            and competition in (CompetitionLevel.HIGH, CompetitionLevel.EXTREME)
        ):
            return TipDecision(
                tip_lamports=0,
                tip_sol=0.0,
                reason=(
                    f"edge-capped tip {tip_sol:.5f} SOL too low vs market "
                    f"p75 {recent_p75_tip_sol:.5f} under {competition.value} competition"
                ),
                expected_edge_sol=expected_edge_sol,
                max_allowed_sol=max_by_edge,
                competition=competition,
            )

        tip_lamports = int(tip_sol * 1_000_000_000)

        reason = (
            f"edge={expected_edge_sol:.4f} SOL × {self.max_edge_ratio:.0%} = "
            f"{max_by_edge:.5f} max | competition={competition.value} → tip={tip_sol:.5f} SOL"
        )

        return TipDecision(
            tip_lamports=tip_lamports,
            tip_sol=round(tip_sol, 9),
            reason=reason,
            expected_edge_sol=expected_edge_sol,
            max_allowed_sol=round(max_by_edge, 9),
            competition=competition,
        )


# Default instance
default_tip_model = EdgeAwareTipModel()
