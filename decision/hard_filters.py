
"""
F-06 Hard Deterministic Filter Stack (SOL-SNIPER)

Modes:
  strict  — mint+freeze renounced (safest; misses pump launches)
  scalp   — same T1 as strict; lower liq bar
  pump_curve — paper-oriented: mint SET ok if freeze REN + min liq
  launch  — true launch snipe: pump create / early curve entry
            mint authority MAY be set (normal at pump create)
            does not require decoded SOL liquidity (often 0 on create tx)
"""

from __future__ import annotations

import time
from typing import List, Optional

from src.types.models import HardFilterResult, TokenCandidate, Verdict

MIN_LIQUIDITY_SOL = 5.0
MAX_TOP5_HOLDER_PCT = 45.0
MAX_TOP10_HOLDER_PCT = 40.0
MAX_SINGLE_NON_LP_PCT = 12.0


class HardFilterStack:
    def __init__(
        self,
        min_liquidity_sol: float = MIN_LIQUIDITY_SOL,
        max_top5_pct: float = MAX_TOP5_HOLDER_PCT,
        max_top10_pct: float = MAX_TOP10_HOLDER_PCT,
        max_single_non_lp_pct: float = MAX_SINGLE_NON_LP_PCT,
        max_slippage_pct: float = 18.0,
        filter_mode: str = "strict",
        pump_curve_min_liq_sol: float = 3.0,
        pump_curve_size_mult: float = 0.40,
        allow_pump_curve: bool = False,
        allow_launch: bool = False,
        launch_size_mult: float = 0.5,
    ):
        self.min_liquidity_sol = min_liquidity_sol
        self.max_top5_pct = max_top5_pct
        self.max_top10_pct = max_top10_pct
        self.max_single_non_lp_pct = max_single_non_lp_pct
        self.max_slippage_pct = max_slippage_pct
        self.filter_mode = filter_mode
        self.pump_curve_min_liq_sol = pump_curve_min_liq_sol
        self.pump_curve_size_mult = pump_curve_size_mult
        self.allow_pump_curve = allow_pump_curve
        self.allow_launch = allow_launch
        self.launch_size_mult = launch_size_mult

    def set_mode(
        self,
        mode: str,
        allow_pump_curve: bool = False,
        allow_launch: bool = False,
    ) -> None:
        self.filter_mode = mode
        self.allow_pump_curve = allow_pump_curve
        self.allow_launch = allow_launch

    def evaluate(self, candidate: TokenCandidate) -> HardFilterResult:
        start = time.perf_counter()
        reasons: List[str] = []
        kb_refs: List[str] = []
        size_multiplier = 1.0
        verdict = Verdict.BUY
        failed_tier: Optional[str] = None

        auth = candidate.authorities
        liq = candidate.liquidity
        holders = candidate.holder_snapshot
        source = (candidate.source_event or candidate.raw.get("source") or "").lower()
        is_pump = "pump" in source or liq.dex in ("pump_fun", "pump_swap")
        instr = " ".join(str(x) for x in (candidate.raw.get("instructions") or [])).lower()
        is_create = any(k in instr for k in ("initializemint", "initialize mint", "create", "mintto"))

        mint_ok = bool(auth.mint_renounced)
        freeze_ok = bool(auth.freeze_renounced)

        # ----- LAUNCH mode: real pump create sniping -----
        # At create, mint auth is almost always SET and decoded liq is often 0.
        launch_exception = False
        if (
            self.allow_launch
            and self.filter_mode == "launch"
            and (is_pump or is_create)
        ):
            launch_exception = True
            reasons.append(
                f"LAUNCH: pump/create snipe allowed (mintAuth={'REN' if mint_ok else 'SET'}, "
                f"liq={liq.initial_sol:.2f})"
            )
            kb_refs.append("sol-snipe-launch")
            if not mint_ok:
                reasons.append("LAUNCH-risk: mint authority still SET (normal at create)")
            if not freeze_ok:
                reasons.append("LAUNCH-risk: freeze authority still SET")
            # Still block obvious honeypot flags
            if auth.has_transfer_hooks or auth.has_transfer_tax:
                reasons.append("T1: transfer hooks/tax — launch blocked")
                return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")
            if candidate.raw.get("sell_simulation_failed") is True:
                reasons.append("T1: honeypot simulation failed")
                return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")
            if candidate.raw.get("deployer_blacklisted") is True:
                reasons.append("T1: deployer blacklisted")
                return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")
            verdict = Verdict.BUY
            return self._result(verdict, 1.0, reasons, kb_refs, start, None)

        # ----- pump_curve exception (paper) -----
        pump_curve_exception = False
        if (
            not mint_ok
            and self.allow_pump_curve
            and self.filter_mode == "pump_curve"
            and is_pump
            and freeze_ok
            and liq.initial_sol >= self.pump_curve_min_liq_sol
        ):
            pump_curve_exception = True
            reasons.append(
                f"T1-soft: pump_curve (mint SET, liq={liq.initial_sol:.2f}>={self.pump_curve_min_liq_sol})"
            )
            size_multiplier *= self.pump_curve_size_mult
            verdict = Verdict.DOWNSIZE

        if not mint_ok and not pump_curve_exception:
            reasons.append("T1: mint_authority still present")
            kb_refs.extend(["sol-snipe-012", "sol-snipe-056"])
            return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")

        if not freeze_ok:
            reasons.append("T1: freeze_authority still present")
            return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")

        if auth.has_transfer_hooks:
            reasons.append("T1: transfer hooks detected")
            return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")
        if auth.has_transfer_tax:
            reasons.append("T1: transfer tax detected")
            return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")
        if candidate.raw.get("sell_simulation_failed") is True:
            reasons.append("T1: honeypot simulation failed")
            return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")
        if candidate.raw.get("deployer_blacklisted") is True:
            reasons.append("T1: deployer blacklisted")
            return self._result(Verdict.SKIP, 0.0, reasons, kb_refs, start, "T1")

        # T2 liquidity (not applied in launch — already returned)
        min_liq = self.min_liquidity_sol
        if self.filter_mode == "scalp":
            min_liq = min(min_liq, 4.0)
        if liq.initial_sol < min_liq:
            reasons.append(f"T2: liquidity {liq.initial_sol:.2f} < {min_liq}")
            size_multiplier *= 0.4
            verdict = Verdict.DOWNSIZE

        if holders:
            if holders.top5_pct > self.max_top5_pct:
                size_multiplier *= 0.5
                verdict = Verdict.DOWNSIZE
                reasons.append("T2: top5 concentration")
            if holders.largest_non_lp_pct > self.max_single_non_lp_pct:
                size_multiplier *= 0.6
                verdict = Verdict.DOWNSIZE

        if liq.initial_sol >= 20:
            size_multiplier = min(1.25, size_multiplier * 1.1)
            reasons.append("T3: strong liquidity")
        if pump_curve_exception:
            size_multiplier = min(size_multiplier, self.pump_curve_size_mult)

        size_multiplier = max(0.15, min(1.25, size_multiplier))
        if not reasons:
            reasons.append("OK: passed hard filters")
        return self._result(verdict, size_multiplier, reasons, kb_refs, start, failed_tier)

    def _result(self, verdict, size_multiplier, reasons, kb_refs, start, failed_tier):
        return HardFilterResult(
            verdict=verdict,
            size_multiplier=size_multiplier,
            reasons=reasons,
            kb_refs=kb_refs,
            latency_ms=(time.perf_counter() - start) * 1000,
            filter_tier_failed=failed_tier,
        )
