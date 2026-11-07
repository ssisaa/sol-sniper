
"""
L4 Execution — paper fills + live Jupiter swap + optional Jito bundle.

Live path requires:
  export SOL_SNIPER_LIVE=1
  export SOL_SNIPER_KEYPAIR=/path/to/id.json   # or SOL_SNIPER_PRIVATE_KEY
  pip install solders base58

Tip policy: EdgeAwareTipModel drives prioritization_fee_lamports and skip-when-uneconomic.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from src.execution.jupiter import JupiterClient
from src.execution.jito import JitoClient
from src.execution.price_oracle import PriceOracle
from src.execution.signer import SignerError, live_armed, pubkey_from_env, sign_versioned_tx_base64
from src.execution.tip_model import EdgeAwareTipModel, TipDecision, default_tip_model
from src.risk.position_manager import PositionManager, PositionMode, RiskConfig
from src.types.models import CompetitionLevel

log = logging.getLogger("sol-sniper.execution")


class ExecMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


@dataclass
class FillResult:
    ok: bool
    mode: str
    mint: str
    sol_in: float
    tokens_out: float
    effective_price: float
    fees_sol: float
    signature: Optional[str] = None
    error: Optional[str] = None
    latency_ms: float = 0.0
    route: Optional[str] = None
    tip_sol: float = 0.0
    tip_reason: Optional[str] = None


class Trader:
    def __init__(
        self,
        manager: PositionManager,
        mode: ExecMode = ExecMode.PAPER,
        risk: Optional[RiskConfig] = None,
        use_jito: bool = True,
        slippage_bps: int = 100,
        tip_model: Optional[EdgeAwareTipModel] = None,
    ):
        self.manager = manager
        self.mode = mode
        self.risk = risk or manager.cfg
        self.use_jito = use_jito
        self.slippage_bps = slippage_bps
        self.tip_model = tip_model or default_tip_model
        self.jup = JupiterClient()
        self.jito = JitoClient()
        self.oracle = PriceOracle(self.jup)
        # Optional external signals (updated by competition estimator later)
        self.recent_p75_tip_sol: Optional[float] = None
        self.default_competition = CompetitionLevel.MED
        self._buy_lock = asyncio.Lock()
        self._holds_path = __import__("pathlib").Path(__file__).resolve().parents[2] / "logs" / "open_mints.json"

    def _load_holds(self) -> set:
        try:
            from src.storage.supabase_store import get_store
            remote = get_store().get("open_mints")
            if isinstance(remote, list):
                return {m for m in remote if isinstance(m, str) and m}
        except Exception:
            log.debug("supabase holds read failed", exc_info=True)
        try:
            data = __import__("json").loads(self._holds_path.read_text(encoding="utf-8"))
            return {m for m in (data or []) if isinstance(m, str) and m}
        except Exception:
            return set()

    def _save_holds(self, mints: set) -> None:
        try:
            from src.storage.supabase_store import get_store
            get_store().set_open_mints(sorted(m for m in mints if m))
        except Exception:
            log.exception("could not save open_mints")

    async def _block_second_buy(self, buying_mint: str, ignore_mints=None) -> str:
        """Refuse a live buy while any non-dust token is still in the wallet or hold file.

        ignore_mints: open Callout coins. They do not block another Callout buy.
        Dust still does not block. A non-callout hold still does.
        """
        ignore = set(ignore_mints or ())
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_accounts

        holds = self._load_holds()
        for pos in self.manager.positions.values():
            if pos.status == "OPEN" and pos.mint and pos.mint != buying_mint and pos.mint not in ignore:
                return f"open position {pos.mint[:8]} — no second buy"
        from src.execution.wallet_gate import supabase_held
        status, stored = supabase_held()
        if status == "fail":
            log.warning("SQLITE wallet read failed — open_mints will not be cleared")
        pk = pubkey_from_env()
        accounts = None
        if pk:
            try:
                accounts = await get_token_accounts(pk, include_primary=True)
            except Exception:
                accounts = None
        if not accounts:
            if status == "fail":
                blocking = [m for m in holds if m and m not in ignore and m != buying_mint]
                if blocking:
                    mint = blocking[0]
                    return f"saved open list {mint[:8]} — read failed, no buy"
            stored_real = [t for t in stored if t.get("mint") and t["mint"] != buying_mint and t["mint"] not in ignore]
            if isinstance(accounts, list):
                from src.execution.wallet_gate import drop_closed_holds
                stored_real = drop_closed_holds(stored_real)
            if stored_real:
                t = max(stored_real, key=lambda x: float(x.get("value_sol") or 0) or float(x.get("ui_amount") or 0))
                holds.add(t["mint"])
                self._save_holds(holds)
                return f"saved wallet still holds {t['mint'][:8]} ui={float(t.get('ui_amount') or 0):.4g} — no second buy"
            if accounts is None:
                log.warning("wallet read missed — no coin on file, buy allowed")
                return ""
        dust_mints = set()
        try:
            from src.storage.supabase_store import get_store
            snap = get_store().get("wallet_now") or {}
            dust_mints = {t.get("mint") for t in (snap.get("tokens") or []) if t.get("dust")}
        except Exception:
            dust_mints = set()
        for acc in accounts:
            mint = acc.get("mint") or ""
            raw = int(acc.get("raw") or 0)
            ui = float(acc.get("ui_amount") or 0)
            if not mint or raw <= 0 or mint == buying_mint or mint in dust_mints or mint in ignore:
                continue
            try:
                from src.execution.wallet_gate import _ABSENT, _load_absent
                from src.execution.wallet_tokens import sold_recently
                _load_absent()
                if mint in _ABSENT or sold_recently(mint):
                    continue
            except Exception:
                pass
            dust = (ui > 0 and ui < 0.01) or (ui <= 0 and raw < 1000)
            if dust:
                continue
            # A large balance with no SOL value is dust (Ego). Do not block on it.
            continue_hold = True
            try:
                q = await self.jup.quote_token_to_sol(mint, raw, slippage_bps=800)
                sol_val = (q.out_amount / 1_000_000_000) if q else None
            except Exception:
                sol_val = None
            if sol_val is None or sol_val < 0.001:
                try:
                    from src.execution.wallet_tokens import quote_mints
                    px = (await quote_mints([mint])).get(mint) or {}
                    usd = float(px.get("price_usd") or 0) * ui
                    if usd >= 0.0001:
                        holds.add(mint)
                        self._save_holds(holds)
                        return f"already holding {mint[:8]} ≈${usd:.4f} — no second buy"
                except Exception:
                    log.debug("dex hold check failed", exc_info=True)
                continue
            holds.add(mint)
            self._save_holds(holds)
            return f"already holding {mint[:8]} ui={ui:.4g} raw={raw} — no second buy"
        # Wallet read succeeded and nothing in it is worth keeping.
        # A sold coin left in the hold file (NODUS) must not block the next launch.
        stale = [m for m in holds if m and m != buying_mint and m not in ignore]
        self._cleared_holds = []
        if stale:
            for m in stale:
                holds.discard(m)
            self._save_holds(holds)
            self._cleared_holds = stale
            log.info(
                "HOLD FILE cleared %s — wallet does not hold them, buy allowed",
                ",".join(m[:8] for m in stale),
            )
        return ""

    async def release_sold_holds(self) -> list:
        """Clear sold coins from the hold file. Empty list means nothing was blocking, or a real coin is still held."""
        self._cleared_holds = []
        reason = await self._block_second_buy("")
        if reason:
            log.info("LAUNCH hold stays — %s", reason)
            return []
        return list(getattr(self, "_cleared_holds", []) or [])

    async def close(self) -> None:
        await self.jup.close()
        await self.jito.close()

    def jupiter_ok(self) -> bool:
        return bool(self.jup.trading_allowed())

    def jupiter_block_reason(self) -> str:
        return self.jup.blocked_reason() or ""

    async def can_afford_buy(self, sol_in: float) -> tuple:
        """Return (ok, bal, need, spendable). Keep a small fee reserve, rest can trade."""
        bal = await self.get_sol_balance()
        reserve = 0.00153  # 0.0015 rent + 0.00003 buy fee. Nothing else is held back.
        need = float(sol_in) + reserve
        if bal is None:
            return False, None, need, None
        spendable = max(0.0, bal - reserve)
        return bal >= need, bal, need, spendable

    async def _round_trip_ok(self, mint: str, sol_in: float, source: str) -> tuple:
        """Buy only if a sell quote comes back.

        plan: selling the buy size returns at least 92%.
        ath and saved styles: selling twice the size returns at least 85% of that larger size.
        classic: no quote gate.
        """
        if source not in ("dayscalp", "launch"):
            return True, sol_in
        try:
            from src.config.runtime import get_config
            style = getattr(get_config(), "exit_style", "ath") or "ath"
        except Exception:
            style = "ath"
        if style == "classic":
            return True, sol_in
        now = time.time()
        skip = getattr(self, "_rt_skip", None)
        if skip is None:
            self._rt_skip = {}
            skip = self._rt_skip
        until = float(skip.get(mint, 0) or 0)
        if until > now:
            return False, 0.0
        two_x = style != "plan"
        size = float(sol_in) * (2.0 if two_x else 1.0)
        need_ratio = 0.85 if two_x else 0.92
        q = await self.jup.quote_sol_to_token(mint, size, slippage_bps=200)
        if not q or q.out_amount <= 0:
            skip[mint] = now + 20
            return False, 0.0
        impact = abs(float(q.price_impact_pct or 0))
        if impact > 1:
            impact /= 100.0
        if impact > 0.12:
            skip[mint] = now + 20
            log.warning(
                "BUY skip %s — price impact %.1f%% on the buy quote. That fill is already the loss. No swap sent.",
                mint[:8], impact * 100,
            )
            return False, 0.0
        back = await self.jup.quote_token_to_sol(mint, int(q.out_amount), slippage_bps=200)
        sol_back = (back.out_amount / 1_000_000_000) if back and back.out_amount else 0.0
        need = size * need_ratio
        if sol_back < need:
            skip[mint] = now + 20
            log.warning(
                "BUY skip %s — sell quote %.4f SOL on %.4f SOL size, need %.4f (%.0f%%). No swap sent.",
                mint[:8], sol_back, size, need, need_ratio * 100,
            )
            return False, sol_back
        return True, sol_back

    def compute_tip(
        self,
        sol_in: float,
        size_multiplier: float = 1.0,
        competition: Optional[CompetitionLevel] = None,
        liquidity_quality: float = 1.0,
        expected_move: float = 0.25,
    ) -> TipDecision:
        """
        Estimate edge then ask EdgeAwareTipModel for max tip.

        expected_edge ≈ sol_in * expected_move * size_multiplier_factor
        (conservative heuristic until attribution provides real stats)
        """
        comp = competition or self.default_competition
        # Crude edge prior: fraction of notional you hope to net before tips
        expected_edge = max(0.0, sol_in * expected_move * max(0.3, min(1.2, size_multiplier)))
        decision = self.tip_model.compute(
            expected_edge_sol=expected_edge,
            competition=comp,
            recent_p75_tip_sol=self.recent_p75_tip_sol,
            liquidity_quality=liquidity_quality,
        )
        log.info(
            "Tip decision: tip=%.6f SOL (max_allowed=%.6f) edge=%.6f comp=%s | %s",
            decision.tip_sol,
            decision.max_allowed_sol,
            decision.expected_edge_sol,
            decision.competition.value,
            decision.reason,
        )
        return decision

    def _tip_for_buy(self, source: str, sol_in: float, size_multiplier: float, competition, liquidity_quality: float) -> TipDecision:
        """Dayscalp and Launch pay a tiny priority fee, never a Jito tip."""
        if source in ("dayscalp", "launch", "predict", "callout", "opencopy"):
            cap = 0.00002 if source in ("dayscalp", "predict", "callout", "opencopy") else 0.00005
            log.info("BUY no tip | %s priority cap %.5f SOL", source, cap)
            return TipDecision(
                tip_lamports=0,
                tip_sol=0.0,
                reason=f"no tip — {source} priority fee only",
                expected_edge_sol=0.0,
                max_allowed_sol=cap,
                competition=competition or self.default_competition,
            )
        return self.compute_tip(
            sol_in=sol_in,
            size_multiplier=size_multiplier,
            competition=competition,
            liquidity_quality=liquidity_quality,
        )

    @staticmethod
    def _winner_exit(reason: str) -> bool:
        """Take-profit only. A trail or stop still sells when the quote is under cost."""
        text = reason or ""
        return text.startswith("take_profit") or text.startswith("tp_touch")

    async def buy(
        self,
        mint: str,
        size_multiplier: float = 1.0,
        source: str = "unknown",
        slot: Optional[int] = None,
        signature_hint: Optional[str] = None,
        ref_price: float = 1.0,
        competition: Optional[CompetitionLevel] = None,
        liquidity_quality: float = 1.0,
    ) -> FillResult:
        from src.execution.tx_route import solana_mint
        if not solana_mint(mint):
            log.warning("BUY skip — not a Solana coin mint=%s. No transaction was sent.", (mint or "")[:14])
            return FillResult(
                ok=False, mode=self.mode.value, mint=mint or "", sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="not_solana",
                latency_ms=0.0,
            )
        from src.execution.fast_lane import get_lane
        lane = get_lane()
        if lane.started and not lane.on_buy():
            return await lane.run_buy(
                mint,
                f"{source} {(mint or '')[:8]}",
                lambda: self.buy(
                    mint, size_multiplier, source, slot, signature_hint,
                    ref_price, competition, liquidity_quality,
                ),
            )
        t0 = time.perf_counter()
        if getattr(self, "_restart_hold", False):
            return FillResult(
                ok=False, mode=self.mode.value, mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="restart_waiting_flat",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        try:
            from src.config.runtime import get_config
            mode_now = str(getattr(get_config(), "filter_mode", "") or "")
        except Exception:
            mode_now = ""
        if source == "launch" and mode_now != "launch":
            log.warning("BUY blocked — Launch does not buy while the filter is %s", mode_now or "unset")
            return FillResult(
                ok=False, mode=self.mode.value, mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=f"launch_off_filter_{mode_now or 'unset'}",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        if source == "dayscalp" and mode_now != "dayscalp":
            log.warning("BUY blocked — Dayscalp does not buy while the filter is %s", mode_now or "unset")
            return FillResult(
                ok=False, mode=self.mode.value, mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=f"dayscalp_off_filter_{mode_now or 'unset'}",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        sol_in = self.manager.size_for_entry(size_multiplier)

        # Single live balance pass: auto-downsize; only hard-block if truly broke
        if self.mode == ExecMode.LIVE:
            async with self._buy_lock:
                inflight = getattr(self, "_inflight_mint", "") or ""
                if inflight and inflight != mint:
                    reason = f"buy in flight {inflight[:8]} — no second buy"
                    log.warning("ACTION BUY_BLOCKED %s", reason)
                    return FillResult(
                        ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                        effective_price=0.0, fees_sol=0.0, error=reason,
                        latency_ms=(time.perf_counter() - t0) * 1000,
                    )
                opens = [p for p in self.manager.positions.values() if p.status == "OPEN" and p.mint != mint]
                callout_ignore = set()
                if source in ("callout", "opencopy"):
                    from src.config.runtime import get_config
                    cap = max(1, int(getattr(get_config(), "max_concurrent", 1) or 1))
                    callout_open = [
                        p for p in self.manager.positions.values()
                        if p.status == "OPEN" and (
                            getattr(p, "source", "") in ("callout", "opencopy")
                            or getattr(p, "strategy", "") in ("callout", "opencopy")
                        )
                    ]
                    if any(p.status == "OPEN" and p.mint == mint for p in self.manager.positions.values()):
                        reason = f"already holding {mint[:8]} — no second buy"
                        log.warning("ACTION BUY_BLOCKED %s", reason)
                        return FillResult(
                            ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                            effective_price=0.0, fees_sol=0.0, error=reason,
                            latency_ms=(time.perf_counter() - t0) * 1000,
                        )
                    other = [p for p in opens if p not in callout_open]
                    if len(callout_open) >= cap:
                        reason = f"callout max concurrent {cap} — no new buy"
                        log.warning("ACTION BUY_BLOCKED %s", reason)
                        return FillResult(
                            ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                            effective_price=0.0, fees_sol=0.0, error=reason,
                            latency_ms=(time.perf_counter() - t0) * 1000,
                        )
                    callout_ignore = {p.mint for p in callout_open if p.mint}
                    opens = []
                if opens:
                    held = getattr(opens[0], "symbol", "") or opens[0].mint[:8]
                    reason = f"already holding {held} — no second buy"
                    log.warning("ACTION BUY_BLOCKED %s", reason)
                    return FillResult(
                        ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                        effective_price=0.0, fees_sol=0.0, error=reason,
                        latency_ms=(time.perf_counter() - t0) * 1000,
                    )
                from src.storage.supabase_store import get_store
                book = {}
                if source not in ("opencopy", "callout"):
                    book = get_store().read_book()
                held = str((book or {}).get("mint") or "")
                book_open = (book or {}).get("status") == "OPEN" and bool(held)
                memory_open = any(p.status == "OPEN" and p.mint == held for p in self.manager.positions.values())
                book_is_callout = source in ("callout", "opencopy") and held in callout_ignore
                if source != "opencopy" and book_open and memory_open and not book_is_callout:
                    reason = f"saved open trade {held[:8]} — no second buy"
                    log.warning("ACTION BUY_BLOCKED %s", reason)
                    return FillResult(
                        ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                        effective_price=0.0, fees_sol=0.0, error=reason,
                        latency_ms=(time.perf_counter() - t0) * 1000,
                    )
                from src.execution.wallet_gate import wallet_buy_block
                # OpenCopy uses the free slot. A leftover coin in the wallet is not a reason to skip.
                if source != "opencopy":
                    blocked = await wallet_buy_block(self.jup, buying_mint=mint, ignore_mints=callout_ignore)
                    if blocked:
                        log.warning("ACTION BUY_BLOCKED %s", blocked)
                        return FillResult(
                            ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                            effective_price=0.0, fees_sol=0.0, error=blocked,
                            latency_ms=(time.perf_counter() - t0) * 1000,
                        )
                    second = await self._block_second_buy(mint, ignore_mints=callout_ignore)
                    if second:
                        log.warning("ACTION BUY_BLOCKED %s", second)
                        return FillResult(
                            ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                            effective_price=0.0, fees_sol=0.0, error=second,
                            latency_ms=(time.perf_counter() - t0) * 1000,
                        )
                self._inflight_mint = mint
            bal, _raw, wallet_err = await self.check_wallet("BUY")
            if wallet_err.startswith("wallet_sol_too_low"):
                log.warning("BUY blocked — %s mint=%s", wallet_err, mint[:8])
                self._inflight_mint = ""
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error=wallet_err,
                    latency_ms=(time.perf_counter() - t0) * 1000,
                )
            if bal is None:
                log.warning("BUY blocked — wallet SOL unreadable mint=%s", mint[:8])
                self._inflight_mint = ""
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="wallet_sol_unreadable",
                    latency_ms=(time.perf_counter() - t0) * 1000,
                )
            else:
                # 0.00204 token-account rent, 0.0009 wallet rent floor, 0.00003 tx fee.
                # Pump also takes ~1% on top of sol_in, so leave 2% headroom.
                reserve = 0.00297
                spendable = max(0.0, (bal - reserve) / 1.02)
                if spendable < 0.001:
                    spendable = 0.0
                if spendable <= 0:
                    log.warning(
                        "BUY skip — SOL %.4f cannot cover token-account rent, fees and the buy",
                        bal,
                    )
                    self._inflight_mint = ""
                    return FillResult(
                        ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                        effective_price=0.0, fees_sol=0.0,
                        error=f"insufficient_balance bal={bal:.4f}",
                        latency_ms=(time.perf_counter() - t0) * 1000,
                    )
                if sol_in > spendable:
                    log.info("BUY downsize %.4f → %.4f SOL (bal=%.4f reserve=%.5f)", sol_in, spendable, bal, reserve)
                    sol_in = spendable

        tip = self._tip_for_buy(source, sol_in, size_multiplier, competition, liquidity_quality)
        if source not in ("dayscalp", "launch", "callout", "opencopy") and tip.tip_sol > sol_in * 0.15:
            log.info("BUY tip capped %.6f → %.6f", tip.tip_sol, sol_in * 0.15)

        if source not in ("dayscalp", "launch", "callout", "opencopy") and tip.tip_lamports == 0 and (competition or self.default_competition) in (
            CompetitionLevel.HIGH,
            CompetitionLevel.EXTREME,
        ):
            if getattr(self, "_inflight_mint", "") == mint:
                self._inflight_mint = ""
            return FillResult(
                ok=False, mode=self.mode.value, mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=f"tip_uneconomic: {tip.reason}",
                latency_ms=(time.perf_counter() - t0) * 1000,
                tip_sol=0.0, tip_reason=tip.reason,
            )

        if source not in ("opencopy", "callout") and not self.jupiter_ok():
            reason = self.jupiter_block_reason()
            log.warning("BUY wait — %s mint=%s", reason, mint[:8])
            if getattr(self, "_inflight_mint", "") == mint:
                self._inflight_mint = ""
            return FillResult(
                ok=False, mode=self.mode.value, mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=reason or "jupiter_circuit",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )

        if self.mode == ExecMode.LIVE:
            try:
                return await self._buy_live(mint, sol_in, source, slot, tip, t0)
            finally:
                if getattr(self, "_inflight_mint", "") == mint:
                    self._inflight_mint = ""

        return await self._buy_paper(
            mint, sol_in, source, slot, signature_hint, ref_price, tip, t0
        )

    async def check_wallet(self, action: str, mint: str = "") -> tuple[Optional[float], Optional[int], str]:
        """Read SOL and, for a sell, the token balance. Empty error means the action may proceed."""
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_holding

        sol = await self.get_sol_balance(fast=action == "BUY")
        if sol is None and action == "BUY":
            sol = seen_sol(1800.0)
        if sol is None and action == "BUY":
            try:
                from src.observability.dashboard import _saved_wallet
                saved = _saved_wallet().get("sol")
                if saved is not None:
                    sol = float(saved)
                    note_sol(sol)
            except Exception:
                sol = None
        raw: Optional[int] = None
        if mint and action == "SELL":
            pk = pubkey_from_env()
            if pk:
                try:
                    held = await get_token_holding(pk, mint)
                    if held is not None:
                        raw = int(held.get("raw") or 0)
                except Exception:
                    raw = None
        err = ""
        if sol is None and action != "BUY":
            err = "wallet_sol_unreadable"
        elif action == "BUY" and sol is not None and sol < 0.00153:
            err = f"wallet_sol_too_low bal={sol:.4f}"
        elif action == "SELL":
            if raw is None:
                err = "wallet_token_unreadable"
            elif raw <= 0:
                err = "wallet_token_zero"
            elif sol < 0.003:
                err = f"wallet_sol_too_low_for_fees bal={sol:.4f}"
        log.info(
            "ACTION WALLET %s sol=%s token_raw=%s mint=%s result=%s",
            action,
            f"{sol:.4f}" if sol is not None else "unknown",
            raw if raw is not None else "n/a" if action != "SELL" else "unknown",
            (mint or "-")[:8],
            err or "ok",
        )
        return sol, raw, err

    async def get_sol_balance(self, fast: bool = False, fresh: bool = False) -> Optional[float]:
        """Paid RPC, then one public RPC. fresh=True is the wallet panel and does not reuse the short cache."""
        from src.execution.signer import pubkey_from_env
        pk = pubkey_from_env()
        if not pk:
            return None
        now = time.time()
        cached = getattr(self, "_sol_cache", None)
        if not fresh and cached and now - float(cached[0]) < (12.0 if fast else 2.0):
            note_sol(float(cached[1]))
            return float(cached[1])
        paid = self._paid_rpc()
        if fresh and paid:
            import json
            from src.execution.tx_route import rpc_post
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getBalance",
                "params": [pk, {"commitment": "confirmed"}],
            }
            try:
                status, text = await asyncio.to_thread(rpc_post, paid, payload, 2.0)
                lamports = ((json.loads(text or "{}").get("result") or {}).get("value"))
                if status == 200 and lamports is not None:
                    sol = lamports / 1_000_000_000
                    self._sol_cache = (now, sol)
                    note_sol(sol)
                    return sol
            except Exception as e:
                log.info("get_sol_balance paid missed: %s", type(e).__name__)
        if fast and paid:
            import json
            from src.execution.tx_route import rpc_post
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getBalance",
                "params": [pk, {"commitment": "processed"}],
            }
            try:
                status, text = await asyncio.to_thread(rpc_post, paid, payload, 0.35)
                lamports = (((json.loads(text or "{}").get("result") or {}).get("value")))
                if status == 200 and lamports is not None:
                    sol = lamports / 1_000_000_000
                    self._sol_cache = (now, sol)
                    note_sol(sol)
                    return sol
            except Exception as e:
                log.info("get_sol_balance paid missed: %s", type(e).__name__)
            if cached:
                note_sol(float(cached[1]))
                return float(cached[1])
            return seen_sol(60.0)
        urls = [paid] if paid else []
        for url in self._public_endpoints()[:3]:
            if url and url not in urls:
                urls.append(url)
        if not urls:
            if fresh:
                return None
            seen = seen_sol(1800.0)
            return seen if seen is not None else (float(cached[1]) if cached else None)
        import asyncio
        import httpx
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getBalance",
            "params": [pk, {"commitment": "processed"}],
        }

        async def _one(client, url: str):
            try:
                r = await client.post(url, json=payload)
                if r.status_code != 200:
                    return None
                lamports = ((r.json() or {}).get("result") or {}).get("value")
                if lamports is None:
                    return None
                return lamports / 1_000_000_000
            except Exception as e:
                log.info("get_sol_balance missed %s: %s", url.split("//")[-1][:24], type(e).__name__)
                return None

        try:
            async with httpx.AsyncClient(timeout=0.8 if fresh else (0.5 if fast else 0.8)) as client:
                tasks = [asyncio.create_task(_one(client, url)) for url in urls]
                found = []
                try:
                    for done in asyncio.as_completed(tasks, timeout=0.8 if fresh else 0.5):
                        val = await done
                        if val is not None:
                            found.append(val)
                            break
                except TimeoutError:
                    pass
                for task in tasks:
                    task.cancel()
        except Exception as e:
            log.info("get_sol_balance missed: %s", type(e).__name__)
            found = []
        if found:
            # A slow RPC keeps the old balance. Phantom shows the newest one.
            sol = max(found)
            self._sol_cache = (now, sol)
            note_sol(sol)
            return sol
        if fresh:
            return None
        if cached and now - float(cached[0]) < 1800:
            note_sol(float(cached[1]))
            return float(cached[1])
        return seen_sol(1800.0)

    def _paid_rpc(self) -> str:
        """The paid endpoint. Buy and sell sends only."""
        try:
            from src.execution.wallet_tokens import primary_url
            return primary_url()
        except Exception:
            return ""

    def _public_endpoints(self) -> list:
        paid = self._paid_rpc()
        urls = []
        for u in self._rpc_endpoints():
            if u and u != paid and u not in urls:
                urls.append(u)
        return urls or ["https://api.mainnet-beta.solana.com"]

    def _rpc_endpoints(self) -> list:
        import os
        urls = []
        env = os.environ.get("SOLANA_RPC_URL", "").strip()
        paid = ""
        try:
            from src.execution.wallet_tokens import primary_url
            paid = primary_url()
        except Exception:
            paid = ""
        if env and env != paid and "fluxrpc.com" not in env.lower():
            urls.append(env)
        # Public list only. The paid primary, including a Flux CDN, is never added here.
        for u in (
            os.environ.get("HELIUS_RPC_URL", "").strip(),
            "https://api.mainnet-beta.solana.com",
            "https://solana-rpc.publicnode.com",
        ):
            if u and u != paid and "fluxrpc.com" not in u.lower() and u not in urls:
                urls.append(u)
        try:
            from src.config.file_config import load_config
            cfg = load_config() or {}
            rpc = cfg.get("rpc") or {}
            for u in rpc.get("urls") or []:
                u = (u or "").strip()
                if u and u != paid and "fluxrpc.com" not in u.lower() and u not in urls:
                    urls.append(u)
        except Exception:
            pass
        return urls or ["https://api.mainnet-beta.solana.com"]


    async def _confirm_signature(self, sig: str, tries: int = 40, delay: float = 1.0, paid_only: bool = False, timeout: float = 4.0) -> bool:
        """True when any RPC reports processed/confirmed/finalized without err."""
        import asyncio
        import httpx

        if not sig:
            return False
        self._last_tx_err = ""
        if str(sig).startswith("paper"):
            return True
        if str(sig).startswith("jito:"):
            log.warning("jito bundle id cannot be confirmed as tx sig=%s", str(sig)[:24])
            return False

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [[sig], {"searchTransactionHistory": True}],
        }
        tx_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [sig, {"commitment": "confirmed", "maxSupportedTransactionVersion": 0}],
        }
        endpoints = []
        paid = self._paid_rpc()
        if paid:
            endpoints.append(paid)
        if not paid_only:
            for url in self._public_endpoints():
                if url not in endpoints:
                    endpoints.append(url)
        if not endpoints:
            self._last_tx_err = "confirm_unreachable"
            return False

        def _landed(value) -> Optional[bool]:
            if not value:
                return None
            if value.get("err") is not None:
                self._last_tx_err = str(value.get("err"))
                log.warning("tx failed on-chain sig=%s err=%s", str(sig)[:16], value.get("err"))
                return False
            st = value.get("confirmationStatus")
            if st in ("processed", "confirmed", "finalized") or value.get("meta") is not None:
                log.info("tx confirmed (%s) sig=%s", st or "confirmed", str(sig)[:16])
                return True
            return None

        import json

        def _post(url: str):
            from src.execution.tx_route import rpc_post
            try:
                status, text = rpc_post(url, payload, timeout=max(0.4, float(timeout or 4.0)))
            except Exception as exc:
                return None, type(exc).__name__
            if status != 200:
                return None, f"http_{status}"
            try:
                data = json.loads(text or "{}")
            except Exception:
                return None, "bad_json"
            if isinstance(data, dict) and data.get("error"):
                return None, "rpc_error"
            value = (((data.get("result") or {}).get("value") or [None])[0]) if isinstance(data, dict) else None
            return value, ""

        saw_rpc = False
        dead = set()
        for i in range(max(1, tries)):
            asked = False
            for url in endpoints:
                if url in dead:
                    continue
                asked = True
                value, err = await asyncio.to_thread(_post, url)
                if err:
                    dead.add(url)
                    log.debug("confirm via %s: %s", url.split("//")[-1][:40], err)
                    continue
                saw_rpc = True
                landed = _landed(value)
                if landed is not None:
                    return landed
            if not asked or not saw_rpc:
                break
            if i + 1 < tries and delay:
                await asyncio.sleep(delay)
        if not saw_rpc:
            self._last_tx_err = "confirm_unreachable"
        log.warning("tx not confirmed in time sig=%s tried=%s — not landed, safe to retry", str(sig)[:16], tries)
        return False

    async def prepare_sell(self, mint: str, raw: int) -> None:
        """Build the take-profit swap before the target is hit, so the send is only a signature."""
        if raw <= 1000:
            return
        if getattr(self, "_preparing_sell", "") == mint:
            return
        self._preparing_sell = mint
        try:
            from src.execution.signer import pubkey_from_env
            pubkey = pubkey_from_env()
            if not pubkey:
                return
            quote = await self.jup.quote_token_to_sol(mint, int(raw), slippage_bps=1000, urgent=True)
            if not quote:
                return
            swap_b64 = await self.jup.swap_transaction(
                quote.raw,
                user_public_key=pubkey,
                prioritization_fee_lamports=150_000,
                urgent=True,
            )
            if not swap_b64:
                return
            ready = getattr(self, "_ready_sell", None)
            if ready is None:
                self._ready_sell = {}
                ready = self._ready_sell
            ready[mint] = {
                "ts": time.time(),
                "raw": int(raw),
                "b64": swap_b64,
                "out": int(quote.out_amount or 0),
            }
            log.info(
                "SELL armed mint=%s out≈%.6f SOL — ready for the take-profit",
                mint[:8], int(quote.out_amount or 0) / 1_000_000_000,
            )
        except Exception:
            log.debug("prepare sell failed", exc_info=True)
        finally:
            if getattr(self, "_preparing_sell", "") == mint:
                self._preparing_sell = ""

    async def _routed_sell(self, mint: str, raw_amt: int, reason: str, t0: float, full: bool = True) -> FillResult:
        from src.execution.signer import pubkey_from_env
        from src.execution.tx_route import run_swap
        from src.execution.wallet_tokens import get_token_holding
        pk = pubkey_from_env()
        urgent = reason in (
            "manual_now", "caller_closed", "caller_drop_7", "callout_age",
        ) or str(reason).startswith("tp_") or str(reason).startswith("take_profit")
        held = None
        if pk and not (urgent and int(raw_amt) > 1000):
            try:
                held = await asyncio.wait_for(
                    get_token_holding(pk, mint, fast=True, confirm_empty=1),
                    timeout=0.6,
                )
            except Exception:
                held = None
        if held is not None and int(held.get("raw") or 0) <= 0:
            log.warning(
                "SELL blocked — wallet holds 0 mint=%s reason=%s — no swap sent",
                mint[:8], reason,
            )
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="wallet_token_zero",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        if held is not None and int(held.get("raw") or 0) > 0:
            raw_amt = int(held["raw"]) if full else min(int(raw_amt), int(held["raw"]))
        routed = await run_swap(self, "sell", mint, raw=int(raw_amt), reason=reason, full=full)
        if not routed.get("ok"):
            err = str(routed.get("err") or "sell_failed")
            log.warning("SELL routes exhausted mint=%s reason=%s err=%s", mint[:8], reason, err[:160])
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=err[:160],
                latency_ms=(time.perf_counter() - t0) * 1000,
                signature=str(routed.get("sig") or "") or None,
                route=str(routed.get("route") or ""),
            )
        try:
            held = await asyncio.wait_for(
                get_token_holding(pk, mint, fast=True, confirm_empty=1),
                timeout=0.8,
            ) if pk else None
        except Exception:
            held = None
        # The chain is the wallet. A failed Lightning tx still holds the coins.
        if held is not None and int(held.get("raw") or 0) > max(1, int(raw_amt) // 2):
            log.warning(
                "SELL not closed mint=%s — wallet still holds raw=%s. Trade stays on the panel",
                mint[:8], held.get("raw"),
            )
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="still_in_wallet",
                latency_ms=(time.perf_counter() - t0) * 1000,
                signature=str(routed.get("sig") or ""),
                route=str(routed.get("route") or ""),
            )
        booked = float(routed.get("sol") or 0)
        if booked <= 0:
            for p in self.manager.positions.values():
                if p.mint == mint and p.status == "OPEN":
                    cost = float(p.entry_sol or 0)
                    since = getattr(p, "since_buy_pct", None)
                    if since is not None and cost > 0:
                        booked = max(0.000001, cost * (1.0 + float(since) / 100.0))
                    elif cost > 0:
                        booked = cost
                    break
        log.info(
            "SELL live OK mint=%s sol_out=%.6f raw=%s sig=%s reason=%s route=%s",
            mint[:8], booked, raw_amt, str(routed.get("sig") or "")[:16], reason, routed.get("route"),
        )
        if full and pk:
            try:
                rent_sol = await self._return_token_rent(pk, mint)
            except Exception:
                log.debug("rent return failed", exc_info=True)
                rent_sol = 0.0
            if rent_sol > 0:
                booked += rent_sol
                log.info("SELL rent back mint=%s +%.6f SOL — now %.6f", mint[:8], rent_sol, booked)
        try:
            from src.execution.wallet_tokens import mark_sold
            mark_sold(mint)
            old = seen_sol(1800.0)
            if old is not None and booked > 0:
                note_sol(old + booked)
                self._sol_cache = (time.time(), old + booked)
        except Exception:
            log.debug("sell balance bump failed", exc_info=True)
        return FillResult(
            ok=True, mode="live", mint=mint, sol_in=booked, tokens_out=float(raw_amt),
            effective_price=0.0, fees_sol=0.00015, signature=str(routed.get("sig") or ""),
            latency_ms=(time.perf_counter() - t0) * 1000, route=str(routed.get("route") or ""),
        )

    async def sell(
        self,
        mint: str,
        token_amount_raw: float,
        frac: float = 1.0,
        reason: str = "exit",
    ) -> FillResult:
        """
        Sell tokens → SOL.
        Paper: simulated fill at current mark.
        Live: Jupiter token→SOL swap, sign, submit.
        """
        t0 = time.perf_counter()
        frac = max(0.0, min(1.0, frac))
        from src.execution.tx_route import solana_mint
        if not solana_mint(mint):
            log.warning("SELL skip — not a Solana coin mint=%s. No transaction was sent.", (mint or "")[:14])
            return FillResult(
                ok=False, mode=self.mode.value, mint=mint or "", sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="not_solana",
                latency_ms=0.0,
            )

        if self.mode == ExecMode.PAPER:
            raw_amt = int(max(0, token_amount_raw * frac))
            if raw_amt <= 0:
                return FillResult(
                    ok=False, mode=self.mode.value, mint=mint, sol_in=0.0, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="zero_token_amount",
                    latency_ms=(time.perf_counter() - t0) * 1000,
                )
            mark = self.manager._prices.get(mint, 0.0)
            # tokens stored as raw-ish float; value ≈ tokens * mark when mark is sol/raw
            sol_out = float(raw_amt) * mark if mark > 0 else 0.0
            # mild slippage
            sol_out *= (1.0 - self.risk.paper_slippage_pct)
            log.info("SELL paper %s raw=%s sol_out≈%.6f reason=%s", mint[:8], raw_amt, sol_out, reason)
            return FillResult(
                ok=True, mode="paper", mint=mint, sol_in=sol_out, tokens_out=float(raw_amt),
                effective_price=mark, fees_sol=sol_out * self.risk.paper_fee_pct,
                signature=f"paper-sell-{mint[:6]}", latency_ms=(time.perf_counter() - t0) * 1000,
                route="paper_sell",
            )

        # LIVE sell
        if not live_armed():
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="live_not_armed",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        pubkey = pubkey_from_env()
        if not pubkey:
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="no_pubkey",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        raw_amt = int(max(0, float(token_amount_raw) * (1.0 if frac >= 0.99 else frac)))
        if raw_amt <= 0:
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="zero_token_amount",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        return await self._routed_sell(mint, raw_amt, reason, t0, full=frac >= 0.99)

        # The route above is the live sell. The older path stays in the file and is not used.

        # A full sell uses the signature already built in the background.
        if frac >= 0.99:
            saved = 0
            for p in self.manager.positions.values():
                if p.mint == mint and p.status == "OPEN":
                    saved = int(getattr(p, "tokens", 0) or 0)
                    break
            try:
                from src.execution.hot_tx import hold_sell, peek_sell
                hot = peek_sell(mint, saved)
            except Exception:
                hot = None
            if hot:
                sent, rpc_err = await self._send_keep_fee(hot["b64"])
                if sent:
                    confirmed = await self._confirm_signature(str(sent), tries=4, delay=0.25)
                    if not confirmed:
                        log.warning("HOT SELL not confirmed mint=%s — trade stays open", mint[:8])
                        return FillResult(
                            ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                            effective_price=0.0, fees_sol=0.0, error="sell_not_confirmed",
                            latency_ms=(time.perf_counter() - t0) * 1000, route="hot_sell",
                            signature=str(sent),
                        )
                    hold_sell(mint)
                    booked = int(hot.get("min") or 0) / 1_000_000_000
                    if booked <= 0:
                        try:
                            from src.execution.wallet_tokens import get_token_holding
                            pk = pubkey_from_env()
                            held = await get_token_holding(pk, mint, fast=True) if pk else None
                        except Exception:
                            held = None
                        if held is not None and int(held.get("raw") or 0) > max(1, int(saved * 0.5)):
                            log.warning("HOT SELL confirmed but wallet still holds mint=%s — trade stays open", mint[:8])
                            return FillResult(
                                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                                effective_price=0.0, fees_sol=0.0, error="sell_still_in_wallet",
                                latency_ms=(time.perf_counter() - t0) * 1000, route="hot_sell",
                                signature=str(sent),
                            )
                    log.info(
                        "HOT SELL confirmed mint=%s raw=%s min_sol=%.6f — paid RPC",
                        mint[:8], saved, booked,
                    )
                    latency = (time.perf_counter() - t0) * 1000
                    return FillResult(
                        ok=True, mode="live", mint=mint,
                        sol_in=booked,
                        tokens_out=float(saved), effective_price=0.0, fees_sol=0.00015,
                        signature=str(sent), latency_ms=latency, route="hot_sell",
                    )
                log.info("HOT SELL send missed mint=%s err=%s — building a fresh pump sell", mint[:8], str(rpc_err or "")[:80])
            else:
                log.info("HOT SELL not ready mint=%s — building a fresh pump sell, Jupiter stays last", mint[:8])

        # On-chain balance is source of truth. A take-profit or a −1 already
        # knows the token amount from the buy, so it does not wait on a wallet read.
        fast = (
            reason.startswith("tp_touch")
            or reason.startswith("take_profit")
            or reason in ("manual_now", "caller_closed", "caller_drop_7", "callout_age")
        )
        if fast:
            saved = 0
            for p in self.manager.positions.values():
                if p.mint == mint and p.status == "OPEN":
                    saved = int(getattr(p, "tokens", 0) or 0)
                    break
            ready = None
            # Take-profit uses the Pump curve. A Jupiter quote armed earlier is not sent.
            if saved > 1000 and ready and time.time() - float(ready.get("ts") or 0) < 12 and int(ready.get("raw") or 0) == saved:
                log.info(
                    "SELL armed send mint=%s raw=%s — quote already built, paid RPC now",
                    mint[:8], saved,
                )
                try:
                    signed_b64 = sign_versioned_tx_base64(ready["b64"])
                except SignerError as e:
                    log.warning("SELL armed sign failed mint=%s err=%s — building a fresh one", mint[:8], e)
                    signed_b64 = ""
                if signed_b64:
                    sent, rpc_err = await self._send_keep_fee(signed_b64)
                    if sent:
                        log.info(
                            "SELL submitted once slip=armed raw=%s out≈%.6f SOL fee=0.00015",
                            saved, int(ready.get("out") or 0) / 1_000_000_000,
                        )
                        confirmed = await self._confirm_signature(str(sent), tries=6, delay=0.25)
                        latency = (time.perf_counter() - t0) * 1000
                        if confirmed:
                            return FillResult(
                                ok=True, mode="live", mint=mint, sol_in=int(ready.get("out") or 0) / 1_000_000_000,
                                tokens_out=float(saved), effective_price=0.0, fees_sol=0.00015,
                                signature=str(sent), latency_ms=latency, route="rpc_sell",
                            )
                        log.warning("SELL armed not confirmed yet mint=%s sig=%s — fresh sell", mint[:8], str(sent)[:16])
            if saved > 1000:
                log.info("SELL fast mint=%s — skip wallet read, %s tokens from the buy", mint[:8], saved)
                onchain_raw = saved
                wallet_err = ""
            else:
                _sol, onchain_raw, wallet_err = await self.check_wallet("SELL", mint)
        else:
            _sol, onchain_raw, wallet_err = await self.check_wallet("SELL", mint)
        if not fast and wallet_err == "wallet_token_unreadable":
            stored_raw = 0
            try:
                from src.execution.wallet_gate import supabase_held
                _status, stored = supabase_held()
                for t in stored:
                    if t.get("mint") == mint:
                        stored_raw = int(t.get("raw") or 0)
                        if stored_raw <= 1000 and float(t.get("ui_amount") or 0) >= 1:
                            stored_raw = int(float(t["ui_amount"]) * 1_000_000)
                        break
            except Exception:
                stored_raw = 0
            gone = False
            try:
                from src.execution.wallet_tokens import _seen_recently, get_token_accounts
                rows = await get_token_accounts(pubkey) if pubkey else None
                # An empty list does not delete the Supabase hold. None is a failed read.
                if rows == [] and stored_raw > 0:
                    gone = False
                else:
                    listed = None if not rows else any((r.get("mint") or "") == mint for r in rows)
                    gone = listed is False and stored_raw <= 0 and not _seen_recently(mint)
            except Exception:
                gone = False
            if gone:
                log.warning(
                    "SELL mint=%s is not in the wallet — not selling the amount saved at the buy",
                    mint[:8],
                )
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="wallet_token_zero",
                    latency_ms=(time.perf_counter() - t0) * 1000,
                )
            saved = stored_raw
            for p in self.manager.positions.values():
                if p.mint == mint and p.status == "OPEN":
                    held_raw = int(getattr(p, "tokens", 0) or 0)
                    if held_raw > saved:
                        saved = held_raw
                    break
            if saved > 1000:
                log.warning(
                    "SELL balance read failed — using the %s tokens already stored mint=%s",
                    saved, mint[:8],
                )
                onchain_raw = saved
                wallet_err = ""
        if wallet_err:
            log.warning("SELL blocked — %s mint=%s reason=%s", wallet_err, mint[:8], reason)
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=wallet_err,
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        token_amount_raw = float(onchain_raw or 0)

        # Full exit sells the entire balance. A 99% leave was stranding tokens in the wallet.
        size_frac = 1.0 if frac >= 0.99 else frac
        raw_amt = int(max(0, token_amount_raw * size_frac))
        if raw_amt <= 0:
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="zero_token_amount",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        from src.execution.pump_curve import pump_portal_sell, pump_sell_tx
        from src.execution.hot_tx import off_curve, remember_off_curve
        urgent_sell = reason in (
            "manual_now", "caller_closed", "caller_drop_7", "callout_age",
        ) or str(reason).startswith("tp_") or str(reason).startswith("take_profit")
        pump_b64, pump_min_sol, pump_why = "", 0, ""

        async def _portal():
            return await pump_portal_sell(mint, raw_amt)

        async def _curve():
            if off_curve(mint):
                return "", 0, "not_on_curve"
            return await pump_sell_tx(mint, raw_amt, slippage_bps=3000 if urgent_sell else 1500)

        if urgent_sell:
            log.warning("SELL portal first mint=%s reason=%s — Pump builds the sell, no Jupiter", mint[:8], reason)
            portal_task = asyncio.create_task(_portal())
            curve_task = asyncio.create_task(_curve())
            done, pending = await asyncio.wait(
                {portal_task, curve_task}, timeout=1.2, return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                try:
                    result = task.result()
                except Exception:
                    continue
                if task is portal_task and result and result[0]:
                    pump_b64, pump_why = result[0], "portal"
                    break
                if task is curve_task and result and result[0]:
                    pump_b64, pump_min_sol, pump_why = result
                    break
            if not pump_b64 and pending:
                more, pending = await asyncio.wait(pending, timeout=0.6)
                for task in more:
                    try:
                        result = task.result()
                    except Exception:
                        continue
                    if task is portal_task and result and result[0]:
                        pump_b64, pump_why = result[0], "portal"
                        break
                    if task is curve_task and result and result[0]:
                        pump_b64, pump_min_sol, pump_why = result
                        break
            for task in pending:
                task.cancel()
            if pump_why in ("not_on_curve", "graduated", "not_sol_quote"):
                remember_off_curve(mint)
            if not pump_b64:
                log.warning("SELL portal and curve missed mint=%s — Jupiter next", mint[:8])
        else:
            if off_curve(mint):
                pump_why = "not_on_curve"
            else:
                pump_b64, pump_min_sol, pump_why = await _curve()
            if not pump_b64:
                portal_b64, portal_why = await _portal()
                if portal_b64:
                    pump_b64, pump_why = portal_b64, "portal"
                else:
                    log.warning("SELL pump portal skipped mint=%s why=%s", mint[:8], portal_why or "empty")
        if pump_b64:
            floor_sol = pump_min_sol / 1_000_000_000
            cost = 0.0
            since_now = None
            for p in self.manager.positions.values():
                if p.mint == mint and p.status == "OPEN":
                    cost = float(p.entry_sol or 0)
                    since_now = getattr(p, "since_buy_pct", None)
                    break
            take_profit = reason.startswith("tp_touch") or reason.startswith("take_profit")
            # A portal sell already has its own slippage. There is no local minimum to compare.
            if pump_why != "portal" and take_profit and cost > 0 and floor_sol + 1e-12 < cost:
                log.warning(
                    "SELL pump minimum %.4f SOL is under the %.4f SOL paid — holding. Take-profit is from the buy, not the 5m tape.",
                    floor_sol, cost,
                )
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="tp_pool_below_cost",
                    latency_ms=(time.perf_counter() - t0) * 1000, route="pump_sell",
                )
            sent, rpc_err = await self._send_keep_fee(pump_b64)
            if sent:
                confirmed = await self._confirm_signature(str(sent), tries=4, delay=0.25)
                if not confirmed:
                    log.warning(
                        "SELL not confirmed mint=%s sig=%s — trade stays open, no close booked",
                        mint[:8], str(sent)[:16],
                    )
                    return FillResult(
                        ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                        effective_price=0.0, fees_sol=0.0, error="sell_not_confirmed",
                        latency_ms=(time.perf_counter() - t0) * 1000, route="pump_sell",
                        signature=str(sent),
                    )
                try:
                    from src.execution.wallet_tokens import get_token_holding
                    pk = pubkey_from_env()
                    held = await get_token_holding(pk, mint, fast=True) if pk else None
                except Exception:
                    held = None
                if held is not None and int(held.get("raw") or 0) > max(1, int(raw_amt * 0.5)):
                    log.warning(
                        "SELL tx confirmed but wallet still holds mint=%s raw=%s — trade stays open",
                        mint[:8], held.get("raw"),
                    )
                    return FillResult(
                        ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                        effective_price=0.0, fees_sol=0.0, error="sell_still_in_wallet",
                        latency_ms=(time.perf_counter() - t0) * 1000, route="pump_sell",
                        signature=str(sent),
                    )
                booked = pump_min_sol / 1_000_000_000
                if booked <= 0:
                    if since_now is not None and cost > 0:
                        booked = max(0.000001, cost * (1.0 + float(since_now) / 100.0))
                    elif cost > 0:
                        booked = cost
                log.info(
                    "SELL %s mint=%s raw=%s min_sol=%.6f — paid RPC, confirmed, no Jupiter",
                    "pump portal" if pump_why == "portal" else "pump curve",
                    mint[:8], raw_amt, booked,
                )
                latency = (time.perf_counter() - t0) * 1000
                return FillResult(
                    ok=True, mode="live", mint=mint,
                    sol_in=booked,
                    tokens_out=float(raw_amt), effective_price=0.0, fees_sol=0.00015,
                    signature=str(sent), latency_ms=latency, route="pump_sell",
                )
            log.warning("SELL pump send failed mint=%s err=%s — Jupiter next", mint[:8], str(rpc_err or "")[:160])
        else:
            log.info("SELL pump skipped mint=%s why=%s — Jupiter", mint[:8], pump_why or "empty")

        # A −1, a caller exit, and a take-profit must not use the tight Launch ladder.
        # One Jupiter 500 used to abort the whole sell.
        urgent_out = reason in (
            "manual_now", "callout_age", "caller_closed", "caller_drop_7",
        ) or str(reason).startswith("tp_") or str(reason).startswith("take_profit")
        open_pos = next((p for p in self.manager.positions.values() if p.mint == mint and p.status == "OPEN"), None)
        strategy = getattr(open_pos, "strategy", "") if open_pos else ""
        cheap = strategy in ("dayscalp", "launch") and not urgent_out
        ath_like = False
        if cheap:
            stop_now = reason in (
                "hard_stop", "pct_stop", "time_stop", "runner_stop", "price_stop",
                "trailing_stop", "unpriced_5m", "stale_pct_5m",
            )
            try:
                from src.config.runtime import get_config
                style = getattr(get_config(), "exit_style", "ath") or "ath"
            except Exception:
                style = "ath"
            plan = style == "plan"
            ath_like = style not in ("plan", "classic")
            # A stop must leave. 10% then 20% then 35%, in the same attempt.
            # The old 4% first try is what sat there while the coin went from -40% to -84%.
            if "stop" in reason or reason in ("manual_now", "callout_age"):
                slip_steps = [1000, 2000, 3500]
            elif ath_like and reason in ("hard_stop", "pct_stop"):
                slip_steps = [400, 1000]
            elif ath_like and reason == "time_stop":
                slip_steps = [1500]
            elif plan and stop_now:
                slip_steps = [1500, 1500]
            else:
                slip_steps = [500, 1000, 1500]
        else:
            slip_steps = [3000, 5000, 7000] if reason in (
                "manual_now", "callout_age", "caller_closed", "caller_drop_7",
            ) or str(reason).startswith("tp_") or str(reason).startswith("take_profit") else [2000, 4000, 6000]
        sell_prio = 10_000 if cheap else 150_000
        attempt_raw = raw_amt
        q = None
        sig = None
        route = "rpc_sell"
        last_err = "sell_submit_failed"
        sell_fee = 0.00001 if cheap else 0.000155
        confirmed = False

        def _below_cost(quote) -> bool:
            if not cheap or not self._winner_exit(reason) or open_pos is None or quote is None:
                return False
            stored = float(getattr(open_pos, "fees_sol", 0) or 0)
            if stored > 0.0002:
                stored = 0.00002 if strategy == "dayscalp" else 0.00005
            part = 1.0 if frac >= 0.99 else frac
            need = float(open_pos.entry_sol or 0) * part + stored * part + sell_fee
            back = quote.out_amount / 1_000_000_000
            if back <= need:
                # The screen can already say +22% while this quote is a little under the
                # slice cost. Waiting for a perfect quote is how that print becomes a dump.
                # Send it when the quote is still a real amount. Only hold a broken quote.
                floor = max(0.0015, need * 0.55)
                if back >= floor:
                    log.info(
                        "SELL take-profit anyway mint=%s quote=%.6f need=%.6f — sending, a wait lets it dump",
                        mint[:8], back, need,
                    )
                    return False
                log.info(
                    "SELL hold mint=%s reason=%s quote=%.6f need=%.6f — winner not sold, fee would eat it",
                    mint[:8], reason, back, need,
                )
                return True
            return False

        def _past_backstop(quote) -> bool:
            """Refuse only a dust-sized quote. A real hard-stop price, even -30%, must be sent."""
            if quote is None:
                return False
            back = quote.out_amount / 1_000_000_000
            if back >= 0.000001:
                return False
            log.info(
                "SELL backstop mint=%s reason=%s quote=%.8f — under 0.000001 SOL, not sent",
                mint[:8], reason, back,
            )
            return True

        async def _send_sell(quote, slip_bps: int, force_broadcast: bool = False):
            if not quote:
                return None, "sell_quote_failed"
            if quote.out_amount < 1_000:
                return None, f"sell_dust_quote out={quote.out_amount}"
            if _past_backstop(quote):
                return None, "sell_backstop"
            if _below_cost(quote):
                return None, "sell_below_cost"
            swap_b64 = await self.jup.swap_transaction(
                quote.raw,
                user_public_key=pubkey,
                prioritization_fee_lamports=sell_prio,
                urgent=True,
            )
            if not swap_b64:
                return None, "sell_swap_tx_failed"
            try:
                signed_b64 = sign_versioned_tx_base64(swap_b64)
            except SignerError as e:
                return None, f"sell_sign_failed: {e}"
            sent, rpc_err = await self._rpc_send_with_err(
                signed_b64,
                skip_preflight=bool(fast or force_broadcast),
                paid=True,
                paid_only=True,
            )
            if sent:
                log.info(
                    "SELL submitted once slip=%sbps raw=%s out≈%.6f SOL fee=%.5f — paid RPC",
                    slip_bps, attempt_raw, quote.out_amount / 1_000_000_000, sell_fee,
                )
                return sent, ""
            log.warning(
                "SELL not broadcast mint=%s err=%s — simulation rejected, no fee",
                mint[:8], str(rpc_err or "")[:160],
            )
            full = str(rpc_err or "")
            try:
                from src.storage.mint_flags import flag_dust_if_needed, unsellable_text
                if unsellable_text(full):
                    flag_dust_if_needed(mint, full)
                    return None, f"sell_dust_frozen:{full[:80]}"
            except Exception:
                pass
            # 6025 / 0x1789 = InvalidTokenAccount. The account Jupiter tried to sell does not exist.
            if "6025" in full or "0x1789" in full.lower() or "InvalidTokenAccount" in full:
                return None, "sell_not_in_wallet:6025"
            mark = "6001" if ("6001" in full or "0x1771" in full.lower()) else "sim"
            if "6024" in full or "0x1788" in full.lower() or "Insufficient" in full:
                mark = "6024"
            return None, f"sell_sim_rejected:{mark}:{full[:70]}"

        if not self.jupiter_ok():
            log.warning("SELL Jupiter down mint=%s — pump retry only, no quote", mint[:8])
        elif cheap:
            # 0x1788 means the route cannot pull this many tokens. Sell a little less and try again.
            urgent = "stop" in reason
            confirmed = False
            slipped = False
            for cut in (1.0, 0.99, 0.95, 0.90):
                attempt_raw = max(1, int(raw_amt * cut))
                if cut < 1.0:
                    log.warning(
                        "SELL retry smaller mint=%s raw=%s (%.0f%% of balance) — last route had insufficient tokens",
                        mint[:8], attempt_raw, cut * 100,
                    )
                for i, bps in enumerate(slip_steps):
                    q = await self.jup.quote_token_to_sol(mint, attempt_raw, slippage_bps=bps, urgent=urgent)
                    if not q:
                        await asyncio.sleep(0.4 if urgent else 2.0)
                        q = await self.jup.quote_token_to_sol(mint, attempt_raw, slippage_bps=bps, urgent=urgent)
                    if not q:
                        last_err = "sell_quote_failed"
                        log.warning("SELL quote failed mint=%s slip=%sbps reason=%s", mint[:8], bps, reason)
                        break
                    if i:
                        log.info("SELL retry at %.0f%% mint=%s reason=%s — fresh quote after slippage reject", bps / 100.0, mint[:8], reason)
                    else:
                        log.info("SELL try %.0f%% mint=%s raw=%s reason=%s", bps / 100.0, mint[:8], attempt_raw, reason)
                    sig, last_err = await _send_sell(q, bps)
                    if not sig:
                        if any(bit in str(last_err) for bit in ("6001", "swap_tx", "quote_failed", "429", "500", "sell_backstop")):
                            continue
                        break
                    raw_amt = attempt_raw
                    confirmed = await self._confirm_signature(str(sig), tries=6, delay=0.5)
                    chain_err = str(getattr(self, "_last_tx_err", "") or "")
                    slipped = "6001" in chain_err or "0x1771" in chain_err.lower()
                    if confirmed:
                        break
                    if slipped and i + 1 < len(slip_steps):
                        log.warning(
                            "SELL chain slippage mint=%s slip=%sbps — next wider step now, no 20s wait",
                            mint[:8], bps,
                        )
                        sig = None
                        last_err = "sell_sim_rejected:6001"
                        continue
                    break
                if confirmed or (sig and not slipped):
                    break
                if "6024" not in str(last_err):
                    break
        else:
            for i, bps in enumerate(slip_steps):
                q = await self.jup.quote_token_to_sol(mint, attempt_raw, slippage_bps=bps, urgent=True)
                if not q:
                    last_err = str(getattr(self.jup, "_last_error", "") or "sell_quote_failed")
                    from src.execution.hot_tx import forget_off_curve
                    from src.execution.pump_curve import forget_curve
                    forget_off_curve(mint)
                    forget_curve(mint)
                    again_b64, again_min, again_why = await pump_sell_tx(mint, attempt_raw, slippage_bps=3000)
                    if again_b64:
                        sent, rpc_err = await self._send_keep_fee(again_b64)
                        if sent:
                            sig = sent
                            route = "pump_sell"
                            pump_min_sol = again_min
                            log.info(
                                "SELL curve after Jupiter refused mint=%s min_sol=%.6f — paid RPC",
                                mint[:8], again_min / 1_000_000_000,
                            )
                            break
                    log.warning(
                        "SELL quote failed mint=%s slip=%sbps reason=%s curve=%s — paid RPC, next width",
                        mint[:8], bps, reason, again_why or "unread",
                    )
                    if "NOT_TRADABLE" in last_err.upper() or "not tradable" in last_err.lower():
                        log.warning("SELL Jupiter will not trade mint=%s — Pump portal was the sell", mint[:8])
                        break
                    continue
                log.info("SELL try %.0f%% mint=%s raw=%s reason=%s — paid RPC", bps / 100.0, mint[:8], attempt_raw, reason)
                sig, last_err = await _send_sell(q, bps, force_broadcast=(i == len(slip_steps) - 1))
                if sig:
                    break
                if any(bit in str(last_err) for bit in ("6001", "UNKNOWN", "-32001", "sim", "swap_tx", "quote_failed", "429", "500")):
                    continue
                break

        if not sig:
            for bps in (3000, 5000):
                again_b64, again_min, again_why = await pump_sell_tx(mint, attempt_raw, slippage_bps=bps)
                if not again_b64:
                    log.warning("SELL pump retry skipped mint=%s why=%s", mint[:8], again_why or "empty")
                    if again_why == "graduated":
                        break
                    continue
                sent, rpc_err = await self._send_keep_fee(again_b64)
                if sent:
                    sig = sent
                    route = "pump_sell"
                    pump_min_sol = again_min
                    log.info(
                        "SELL pump retry mint=%s slip=%sbps min_sol=%.6f — paid RPC, Jupiter was down",
                        mint[:8], bps, again_min / 1_000_000_000,
                    )
                    break
                last_err = f"pump_send_failed:{rpc_err}"
                log.warning("SELL pump retry failed mint=%s err=%s", mint[:8], str(rpc_err or "")[:120])

        latency = (time.perf_counter() - t0) * 1000
        if not sig:
            log.error("SELL submit failed mint=%s reason=%s err=%s", mint[:8], reason, str(last_err)[:120])
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=f"sell_submit_failed:{str(last_err)[:80]}",
                latency_ms=latency,
            )

        # Require on-chain confirmation — never book a fake sell.
        # A stop that already confirmed above must not wait another 20s.
        if not (cheap and confirmed):
            confirm_tries = 6 if fast else (8 if "stop" in reason else 20)
            confirm_delay = 0.25 if fast else (0.5 if "stop" in reason else 1.0)
            confirmed = await self._confirm_signature(str(sig), tries=confirm_tries, delay=confirm_delay)
        if not confirmed:
            still = None
            try:
                from src.execution.wallet_tokens import get_token_raw_balance
                pk = pubkey_from_env()
                if pk:
                    still = await get_token_raw_balance(pk, mint)
            except Exception:
                still = None
            sold = int(raw_amt)
            before = int(token_amount_raw or 0)
            if still is not None and before > 0 and still <= before - int(sold * 0.6):
                log.info(
                    "SELL confirmed by balance drop mint=%s %s -> %s sig=%s",
                    mint[:8], before, still, str(sig)[:16],
                )
                confirmed = True
            elif still is not None and still >= int(before * 0.9):
                log.error(
                    "SELL NOT LANDED mint=%s sig=%s reason=%s — balance unchanged, safe to retry",
                    mint[:8], str(sig)[:16], reason,
                )
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="sell_not_landed",
                    latency_ms=latency, signature=str(sig), route=route,
                )
        if not confirmed:
            log.error(
                "SELL NOT CONFIRMED mint=%s sig=%s reason=%s — position stays OPEN",
                mint[:8], str(sig)[:16], reason,
            )
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=0.0, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="sell_not_confirmed",
                latency_ms=latency, signature=str(sig), route=route,
            )

        if q is not None:
            sol_out = float(q.out_sol or 0)
        else:
            sol_out = float(pump_min_sol or 0) / 1_000_000_000
        # Price in SOL per raw token unit (matches entry_price convention)
        eff_price = (sol_out / raw_amt) if raw_amt else 0.0
        log.info(
            "SELL live OK mint=%s sol_out=%.6f raw=%s px=%.12e sig=%s reason=%s route=%s fee=%.5f",
            mint[:8], sol_out, raw_amt, eff_price, str(sig)[:16], reason, route, sell_fee,
        )
        if frac >= 0.99:
            await self._return_token_rent(pubkey, mint)
        holds = self._load_holds()
        if frac >= 0.99:
            holds.discard(mint)
            self._save_holds(holds)
            try:
                from src.execution.wallet_tokens import mark_sold
                mark_sold(mint)
            except Exception:
                log.debug("mark sold failed", exc_info=True)
        elif mint in holds:
            still = None
            try:
                from src.execution.wallet_tokens import get_token_raw_balance
                pk = pubkey_from_env()
                if pk:
                    still = await get_token_raw_balance(pk, mint)
            except Exception:
                still = None
            if still == 0:
                holds.discard(mint)
                self._save_holds(holds)
            else:
                log.warning(
                    "SELL landed but %s still in hold file (raw=%s) — no new buy until the wallet is clear",
                    mint[:8], still,
                )
        return FillResult(
            ok=True, mode="live", mint=mint, sol_in=sol_out, tokens_out=float(raw_amt),
            effective_price=eff_price, fees_sol=sell_fee, signature=str(sig),
            latency_ms=latency, route=route,
        )

    async def reclaim_empty_rent(self) -> float:
        """Close leftover empty token accounts. A sold coin keeps its rent until this runs."""
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import empty_rent_accounts

        owner = pubkey_from_env()
        if not owner:
            return 0.0
        try:
            rows = await empty_rent_accounts(owner)
        except Exception:
            log.debug("empty rent list failed", exc_info=True)
            return 0.0
        if not rows:
            return 0.0
        by_mint: dict = {}
        for row in rows:
            by_mint.setdefault(row.get("mint") or "", []).append(row)
        total = 0.0
        for mint, group in by_mint.items():
            if not mint:
                continue
            total += await self._close_rent_rows(owner, mint, group)
        if total > 0:
            log.info("RENT sweep returned %.6f SOL from empty token accounts", total)
        return total

    async def _return_token_rent(self, owner: str, mint: str) -> float:
        """Close an empty token account so the buy's rent comes back. Dust leftover is burned first."""
        from src.execution.wallet_tokens import token_accounts_for_mint

        rows = None
        for attempt in range(4):
            try:
                rows = await token_accounts_for_mint(owner, mint, paid_first=True)
            except Exception:
                rows = None
            if rows is None:
                await asyncio.sleep(0.4)
                continue
            if any(int(r.get("raw") or 0) > 100 for r in rows):
                await asyncio.sleep(0.4)
                continue
            break
        if rows is None:
            log.info("RENT skip mint=%s — wallet read failed, not sending a close", mint[:8])
            return 0.0
        pending = [
            r for r in rows
            if r.get("pubkey") and int(r.get("lamports") or 0) > 0 and int(r.get("raw") or 0) <= 100
        ]
        if not pending:
            held = sum(int(r.get("raw") or 0) for r in rows)
            log.info(
                "RENT skip mint=%s — %s",
                mint[:8],
                "token account still holds tokens" if held else "already closed",
            )
            return 0.0
        return await self._close_rent_rows(owner, mint, pending)

    async def _close_rent_rows(self, owner: str, mint: str, pending: list) -> float:
        import base64

        from src.execution.signer import load_keypair_from_env
        try:
            from solders.hash import Hash
            from solders.instruction import AccountMeta, Instruction
            from solders.message import Message
            from solders.pubkey import Pubkey
            from solders.transaction import Transaction
        except Exception:
            log.warning("RENT skip mint=%s — solders missing", mint[:8])
            return 0.0
        kp, _pk = load_keypair_from_env()
        blockhash = await self._latest_blockhash()
        if not blockhash:
            log.warning("RENT skip mint=%s — no blockhash", mint[:8])
            return 0.0
        owner_key = Pubkey.from_string(owner)
        returned = 0.0
        for row in pending:
            program = row.get("program") or ""
            account = row["pubkey"]
            raw_left = int(row.get("raw") or 0)
            lamports = int(row.get("lamports") or 0)
            if program not in (
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            ):
                log.info("RENT skip account %s — not a token program", account[:8])
                continue
            try:
                ixs = []
                if raw_left > 0:
                    ixs.append(Instruction(
                        Pubkey.from_string(program),
                        bytes([8]) + raw_left.to_bytes(8, "little"),
                        [
                            AccountMeta(Pubkey.from_string(account), is_signer=False, is_writable=True),
                            AccountMeta(Pubkey.from_string(mint), is_signer=False, is_writable=True),
                            AccountMeta(owner_key, is_signer=True, is_writable=False),
                        ],
                    ))
                ixs.append(Instruction(
                    Pubkey.from_string(program),
                    bytes([9]),
                    [
                        AccountMeta(Pubkey.from_string(account), is_signer=False, is_writable=True),
                        AccountMeta(owner_key, is_signer=False, is_writable=True),
                        AccountMeta(kp.pubkey(), is_signer=True, is_writable=False),
                    ],
                ))
                recent = Hash.from_string(blockhash)
                msg = Message.new_with_blockhash(ixs, kp.pubkey(), recent)
                tx = Transaction.new_unsigned(msg)
                tx.sign([kp], recent)
                signed_b64 = base64.b64encode(bytes(tx)).decode("ascii")
            except Exception as e:
                log.warning("RENT build failed mint=%s %s", mint[:8], e)
                continue
            sig, err = await self._rpc_send_with_err(signed_b64, skip_preflight=False, paid=True, paid_only=True)
            if not sig:
                log.info(
                    "RENT not sent mint=%s — simulation rejected, no fee (%s)",
                    mint[:8], str(err or "")[:120],
                )
                continue
            got = lamports / 1_000_000_000
            returned += got
            log.info("RENT returned mint=%s ≈%.6f SOL sig=%s", mint[:8], got, str(sig)[:16])
            try:
                from src.observability.action_log import record_action
                record_action(
                    "RENT",
                    detail=f"RENT returned {got:.6f} SOL mint={mint[:8]}",
                    mint=mint,
                )
            except Exception:
                pass
            try:
                from src.observability.exec_history import get_exec_history
                get_exec_history().add_rent(mint, got)
            except Exception:
                log.debug("rent history update failed", exc_info=True)
        return returned

    async def _latest_blockhash(self) -> Optional[str]:
        import httpx
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash",
            "params": [{"commitment": "confirmed"}],
        }
        urls = []
        paid = self._paid_rpc()
        if paid:
            urls.append(paid)
        for url in self._public_endpoints():
            if url and url not in urls:
                urls.append(url)
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                for url in urls:
                    try:
                        r = await client.post(url, json=payload)
                        if r.status_code != 200:
                            continue
                        value = ((r.json() or {}).get("result") or {}).get("value") or {}
                        bh = value.get("blockhash")
                        if bh:
                            return bh
                    except Exception:
                        continue
        except Exception:
            return None
        return None

    async def mark_price(self, mint: str) -> Optional[float]:
        pos = next(
            (p for p in self.manager.positions.values() if p.mint == mint and p.status == "OPEN"),
            None,
        )
        # Dayscalp stores entry as 1.0 = what we paid. A raw SOL-per-token
        # price is ~1e-10 and was painting a fake -100% and firing stops.
        if pos is not None and pos.entry_price >= 0.2:
            cur = self.manager.get_price(pos)
            if cur >= 0.05:
                return cur
            return None
        mark = await self.oracle.mark_sol_per_raw_token(mint)
        if not mark:
            return None
        self.manager.set_price(mint, mark.sol_per_raw_token)
        return mark.sol_per_raw_token

    async def _buy_paper(
        self,
        mint: str,
        sol_in: float,
        source: str,
        slot: Optional[int],
        signature_hint: Optional[str],
        ref_price: float,
        tip: TipDecision,
        t0: float,
    ) -> FillResult:
        q = await self.jup.quote_sol_to_token(mint, sol_in, slippage_bps=self.slippage_bps)
        if q and q.out_amount > 0:
            effective_price = (q.in_amount / 1_000_000_000) / q.out_amount
            tokens = float(q.out_amount)
            route = f"jupiter_paper_impact={q.price_impact_pct:.3f}"
            fee = sol_in * self.risk.paper_fee_pct
        else:
            slip = self.risk.paper_slippage_pct * (0.8 + 0.4 * random.random())
            fee = sol_in * self.risk.paper_fee_pct
            effective_price = ref_price * (1.0 + slip)
            tokens = (sol_in - fee) / effective_price if effective_price else 0.0
            route = "synthetic_paper"

        # Paper accounts for tip as cost drag on fees
        fee += tip.tip_sol

        pos = await self.manager.open_position(
            mint=mint,
            mode=PositionMode.PAPER,
            entry_sol=sol_in,
            entry_price=effective_price,
            tokens=tokens,
            slot=slot,
            source=source,
            signature=signature_hint,
            fees_sol=fee,
        )
        latency = (time.perf_counter() - t0) * 1000
        if not pos:
            return FillResult(
                ok=False, mode="paper", mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=effective_price, fees_sol=fee, error="position_not_opened",
                latency_ms=latency, route=route, tip_sol=tip.tip_sol, tip_reason=tip.reason,
            )
        self.manager.set_price(mint, effective_price)
        return FillResult(
            ok=True, mode="paper", mint=mint, sol_in=sol_in, tokens_out=tokens,
            effective_price=effective_price, fees_sol=fee,
            signature=f"paper-{pos.id}", latency_ms=latency, route=route,
            tip_sol=tip.tip_sol, tip_reason=tip.reason,
        )

    async def _raw_from_tx(self, sig: str, pubkey: str, mint: str) -> int:
        """Tokens this signature put in the wallet. 0 if the transaction failed or cannot be read."""
        import json
        from src.execution.tx_route import rpc_post
        from src.execution.wallet_tokens import primary_url
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [sig, {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "processed",
            }],
        }
        urls: list = []
        paid = primary_url()
        if paid:
            urls.append(paid)
        for url in ("https://solana-rpc.publicnode.com", "https://api.mainnet-beta.solana.com"):
            if url not in urls:
                urls.append(url)
        if not urls:
            return 0

        async def _one(url: str) -> int:
            try:
                status, text = await asyncio.to_thread(rpc_post, url, payload, 0.8)
            except Exception:
                return 0
            if status != 200:
                return 0
            try:
                res = (json.loads(text or "{}").get("result") or None)
            except Exception:
                return 0
            if not res:
                return 0
            meta = res.get("meta") or {}
            if meta.get("err"):
                self._last_buy_tx_err = str(meta.get("err"))
                log.warning(
                    "BUY tx failed mint=%s sig=%s err=%s",
                    mint[:8], str(sig)[:16], self._last_buy_tx_err[:80],
                )
                return -1
            best = 0
            for bal in meta.get("postTokenBalances") or []:
                if (bal.get("owner") or "") != pubkey or (bal.get("mint") or "") != mint:
                    continue
                amt = ((bal.get("uiTokenAmount") or {}).get("amount")) or "0"
                try:
                    best = max(best, int(amt))
                except (TypeError, ValueError):
                    continue
            if best > 0:
                log.info("BUY tx shows the coins mint=%s raw=%s sig=%s", mint[:8], best, str(sig)[:16])
            return best

        tasks = [asyncio.create_task(_one(url)) for url in urls]
        try:
            for done in asyncio.as_completed(tasks, timeout=2.2):
                got = await done
                if got:
                    for task in tasks:
                        task.cancel()
                    return got
        except TimeoutError:
            pass
        for task in tasks:
            task.cancel()
        return 0

    async def _buy_live(
        self,
        mint: str,
        sol_in: float,
        source: str,
        slot: Optional[int],
        tip: TipDecision,
        t0: float,
    ) -> FillResult:
        from src.execution.signer import live_armed, pubkey_from_env, sign_versioned_tx_base64, SignerError

        if not live_armed():
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="live_not_armed",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        pubkey = pubkey_from_env()
        if not pubkey:
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="no_pubkey",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        if source in ("callout", "opencopy"):
            from src.config.runtime import get_config
            cap = max(1, int(getattr(get_config(), "max_concurrent", 1) or 1))
            live_open = [p for p in self.manager.positions.values() if p.status == "OPEN"]
            foreign = [
                p for p in live_open
                if getattr(p, "source", "") not in ("callout", "opencopy") and getattr(p, "strategy", "") not in ("callout", "opencopy")
            ]
            same = [p for p in live_open if p.mint == mint]
            callouts = [p for p in live_open if p not in foreign]
            blocked = bool(same or len(callouts) >= cap)
        else:
            blocked = any(p.status == "OPEN" for p in self.manager.positions.values())
        if blocked:
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="already_holding",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )
        if source in ("callout", "opencopy"):
            if self.manager.is_paused():
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="position_not_opened",
                    latency_ms=(time.perf_counter() - t0) * 1000,
                )
        elif not self.manager.can_open():
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="position_not_opened",
                latency_ms=(time.perf_counter() - t0) * 1000,
            )

        # The balance was just read in buy(). Do not wait on it again.
        bal = getattr(self, "_sol_cache", None)
        bal = float(bal[1]) if bal and time.time() - float(bal[0]) < 2 else None
        need = sol_in + 0.00153
        if bal is not None and bal < need:
            adj = bal - 0.00153
            if adj <= 0:
                log.error("BUY blocked bal=%.4f need=%.4f mint=%s", bal, need, mint[:8])
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="insufficient_sol_for_buy",
                    latency_ms=(time.perf_counter() - t0) * 1000,
                    tip_sol=tip.tip_sol, tip_reason=tip.reason,
                )
            log.info("BUY final downsize %.4f → %.4f (bal=%.4f)", sol_in, adj, bal)
            sol_in = adj

        prio_lamports = int(tip.tip_lamports) if tip.tip_lamports > 0 else 50_000
        prio_lamports = max(50_000, min(prio_lamports, int(self.tip_model.absolute_max_tip_sol * 1_000_000_000)))
        slip = max(self.slippage_bps, 300)
        fee_sol = 0.0
        if source in ("dayscalp", "predict", "callout", "opencopy"):
            prio_lamports = 20_000
            slip = 200
            fee_sol = 0.00002
        elif source == "launch":
            prio_lamports = 50_000
            slip = 200
            fee_sol = 0.00005

        attempt_sol = sol_in
        from src.execution.tx_route import run_swap
        routed = await run_swap(self, "buy", mint, sol=float(attempt_sol), reason=source) or {}
        if not routed.get("ok"):
            err = str(routed.get("err") or "buy_failed")
            err_l = err.lower()
            # Custom 1 is a program reject. It is not "wallet is short", so a smaller buy is the same failure.
            low = (
                "insufficient" in err_l
                and "token" not in err_l
                and "6063" not in err_l
                and "custom" not in err_l
            )
            if low and attempt_sol > 0.008:
                self._sol_cache = None
                fresh = await self.get_sol_balance(fast=True)
                smaller = round(attempt_sol * 0.6, 4)
                if fresh and fresh - 0.00153 < smaller:
                    smaller = round(max(0.0, fresh - 0.00153), 4)
                if smaller > 0.001 and smaller < attempt_sol - 0.0001:
                    log.warning(
                        "BUY retry %.4f → %.4f mint=%s — paid RPC said not enough SOL",
                        attempt_sol, smaller, mint[:8],
                    )
                    attempt_sol = smaller
                    routed = await run_swap(self, "buy", mint, sol=float(attempt_sol), reason=source) or {}
                    err = str(routed.get("err") or "buy_failed")
            if not routed.get("ok"):
                log.warning("BUY routes exhausted mint=%s err=%s", mint[:8], err[:160])
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=attempt_sol, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error=err[:160],
                    latency_ms=(time.perf_counter() - t0) * 1000,
                    signature=str(routed.get("sig") or "") or None,
                    route=str(routed.get("route") or ""),
                    tip_sol=tip.tip_sol, tip_reason=tip.reason,
                )
        sig = str(routed.get("sig") or "")
        route = str(routed.get("route") or "route")
        last_err = ""
        pump_b64 = ""
        pump_why = ""
        hot_buy = None
        if not sig and hot_buy and hot_buy.get("kind") == "jupiter":
            sig, rpc_err = await self._send_keep_fee(hot_buy["b64"])
            if sig:
                route = "hot_buy"
                sol_in = attempt_sol
                quote_raw = int(hot_buy.get("min_tokens") or 0)
                log.info("HOT BUY sent mint=%s sol=%.4f — Jupiter already signed, paid RPC", mint[:8], attempt_sol)
            else:
                last_err = str(rpc_err or "hot_buy_send_failed")
                log.info("HOT BUY send missed mint=%s err=%s — building a fresh one", mint[:8], last_err[:80])
                pump_why = "not_on_curve"
        elif not sig and hot_buy:
            pump_b64 = hot_buy["b64"]
            _pump_tokens = int(hot_buy.get("min_tokens") or 0)
            log.info("HOT BUY ready mint=%s sol=%.4f — already signed", mint[:8], attempt_sol)
        elif not sig:
            from src.execution.pump_curve import pump_buy_tx
            from src.execution.hot_tx import off_curve, remember_off_curve
            if off_curve(mint):
                pump_why = "not_on_curve"
            else:
                pump_b64, _pump_tokens, pump_why = await pump_buy_tx(mint, attempt_sol, slippage_bps=max(slip, 1500))
            if pump_why in ("not_on_curve", "graduated", "not_sol_quote"):
                remember_off_curve(mint)
        if not sig and pump_b64:
            sig, rpc_err = await self._send_keep_fee(pump_b64)
            if sig:
                route = "pump"
                sol_in = attempt_sol
                log.info("BUY pump curve mint=%s sol=%.4f — paid RPC, no Jupiter", mint[:8], attempt_sol)
            else:
                last_err = str(rpc_err or "pump_send_failed")
                log.warning("BUY pump send failed mint=%s err=%s — Jupiter next", mint[:8], last_err[:160])
                sig = None
        elif not sig and pump_why:
            log.info("BUY pump skipped mint=%s why=%s — Jupiter", mint[:8], pump_why or "empty")
        if not sig:
            if source in ("opencopy", "callout"):
                ok_rt, rt_back = True, 0.0
            else:
                ok_rt, rt_back = await self._round_trip_ok(mint, attempt_sol, source)
            if not ok_rt:
                self._inflight_mint = ""
                return FillResult(
                    ok=False, mode="live", mint=mint, sol_in=attempt_sol, tokens_out=0.0,
                    effective_price=0.0, fees_sol=0.0, error="round_trip_blocked",
                    latency_ms=(time.perf_counter() - t0) * 1000,
                )
            q = await self.jup.quote_sol_to_token(mint, attempt_sol, slippage_bps=max(slip, 3000), urgent=True)
            if not q or int(getattr(q, "out_amount", 0) or 0) <= 0:
                last_err = "buy_quote_failed"
                log.warning("BUY quote fail mint=%s sol=%.4f — no swap sent", mint[:8], attempt_sol)
            else:
                swap_b64 = await self.jup.swap_transaction(
                    q.raw,
                    user_public_key=pubkey,
                    prioritization_fee_lamports=prio_lamports,
                    urgent=True,
                )
                if not swap_b64:
                    last_err = "jupiter_swap_tx_failed"
                else:
                    try:
                        signed_b64 = sign_versioned_tx_base64(swap_b64)
                    except SignerError as e:
                        return FillResult(
                            ok=False, mode="live", mint=mint, sol_in=attempt_sol, tokens_out=0.0,
                            effective_price=0.0, fees_sol=0.0, error=f"sign_failed: {e}",
                            latency_ms=(time.perf_counter() - t0) * 1000,
                            tip_sol=tip.tip_sol, tip_reason=tip.reason,
                        )
                    sig, rpc_err = await self._send_keep_fee(signed_b64)
                    if sig:
                        sol_in = attempt_sol
                        quote_raw = int(q.out_amount or 0)
                        log.info("BUY submitted once slip=%s sol=%.4f out_tokens=%s", slip, attempt_sol, q.out_amount)
                    else:
                        last_err = str(rpc_err or "submit_failed")
                        log.warning("BUY swap not sent mint=%s err=%s — no retry", mint[:8], last_err[:160])

        latency = (time.perf_counter() - t0) * 1000
        if not sig:
            # A missing quote never left the wallet. Do not call it a submitted swap.
            if last_err in ("buy_quote_failed", "jupiter_swap_tx_failed"):
                err = last_err
            else:
                err = f"submit_failed:{str(last_err)[:80]}"
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error=err,
                latency_ms=latency, route=route,
                tip_sol=tip.tip_sol, tip_reason=tip.reason,
            )

        confirmed = True if sig else False
        raw = 0
        if sig and pubkey:
            self._last_buy_tx_err = ""
            # The quote is not a balance. TCC was recorded confirmed from the
            # quote after the signature had not landed and the wallet held 0.
            raw = await self._raw_from_tx(sig, pubkey, mint)
            if raw < 0:
                err = str(getattr(self, "_last_buy_tx_err", "") or "")
                curve_reject = any(code in err for code in ("6063", "6005", "6022", "6023", "6024"))
                if curve_reject:
                    from src.execution.hot_tx import remember_off_curve
                    from src.execution.tx_route import lightning_trade
                    remember_off_curve(mint)
                    log.warning(
                        "BUY curve rejected mint=%s err=%s — buying on pump-amm. The SOL was not spent.",
                        mint[:8], err[:80],
                    )
                    sig2, err2 = await lightning_trade(
                        "buy", mint, float(attempt_sol), True, max(slip, 200), 0.0,
                        pools=("pump-amm", "auto"),
                    )
                    if sig2:
                        sig = sig2
                        route = "lightning"
                        self._last_buy_tx_err = ""
                        raw = await self._raw_from_tx(sig2, pubkey, mint)
                    else:
                        raw = -1
                        err = err2 or err
                        self._last_buy_tx_err = err
                if raw < 0:
                    return FillResult(
                        ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                        effective_price=0.0, fees_sol=0.0,
                        error=str(getattr(self, "_last_buy_tx_err", "") or "buy_tx_failed")[:160],
                        latency_ms=(time.perf_counter() - t0) * 1000, route=route, signature=str(sig),
                        tip_sol=tip.tip_sol, tip_reason=tip.reason,
                    )
        if raw <= 0 and sig and pubkey:
            from src.execution.wallet_tokens import get_token_holding
            try:
                held = await asyncio.wait_for(
                    get_token_holding(pubkey, mint, fast=True, confirm_empty=1),
                    timeout=1.5,
                )
            except Exception:
                held = None
            if held is not None and int(held.get("raw") or 0) > 0:
                raw = int(held["raw"])
        if raw <= 0:
            log.warning(
                "BUY NOT IN WALLET mint=%s sig=%s — coins are not in the wallet, trade not opened",
                mint[:8], str(sig)[:16],
            )
            return FillResult(
                ok=False, mode="live", mint=mint, sol_in=sol_in, tokens_out=0.0,
                effective_price=0.0, fees_sol=0.0, error="buy_not_in_wallet",
                latency_ms=(time.perf_counter() - t0) * 1000, route=route, signature=str(sig),
                tip_sol=tip.tip_sol, tip_reason=tip.reason,
            )
        tokens = float(raw)
        effective_price = (sol_in / tokens) if tokens else 0.0
        cached = getattr(self, "_sol_cache", None)
        if cached:
            self._sol_cache = (time.time(), max(0.0, float(cached[1]) - float(attempt_sol)))
        if source in ("dayscalp", "launch"):
            fee_est = fee_sol
            booked_tip = 0.0
        else:
            fee_est = sol_in * 0.002 + tip.tip_sol
            booked_tip = tip.tip_sol

        pos = await self.manager.open_position(
            mint=mint, mode=PositionMode.LIVE, entry_sol=sol_in,
            entry_price=effective_price, tokens=tokens, slot=slot,
            source=source, signature=str(sig), fees_sol=fee_est,
        )
        if pos is None:
            pos = await self.manager.open_position(
                mint=mint, mode=PositionMode.LIVE, entry_sol=sol_in,
                entry_price=effective_price, tokens=tokens, slot=slot,
                source=source, signature=str(sig), fees_sol=fee_est,
                force=True,
            )
        self.manager.set_price(mint, effective_price)
        holds = self._load_holds()
        holds.add(mint)
        self._save_holds(holds)
        log.info(
            "BUY live OK mint=%s sol=%.4f tokens=%.4g sig=%s source=%s",
            mint[:8], sol_in, tokens, str(sig)[:16], source,
        )
        try:
            from src.observability.exec_history import get_exec_history
            get_exec_history().record(
                side="BUY",
                mint=mint,
                sol_amount=float(sol_in or 0),
                token_amount=float(tokens or 0),
                signature=str(sig or ""),
                status="confirmed",
                route=route or "",
                reason=source or "buy",
                mode="live",
                source="bot",
            )
        except Exception:
            log.debug("exec history buy record failed", exc_info=True)
        try:
            from src.execution.hot_tx import warm_sell
            asyncio.create_task(warm_sell(mint, int(tokens)))
        except Exception:
            pass
        return FillResult(
            ok=pos is not None, mode="live", mint=mint, sol_in=sol_in,
            tokens_out=tokens, effective_price=effective_price, fees_sol=fee_est,
            signature=str(sig), latency_ms=latency, route=route,
            error=None if pos else "position_not_opened",
            tip_sol=booked_tip, tip_reason=tip.reason,
        )

    async def _send_keep_fee(self, signed_b64: str, fast: bool = False):
        """Simulate first. A rejected simulation is not broadcast, so it spends no fee.

        fast=True is the Local PumpPortal buy and sell. The simulation wait is
        capped at a quarter second, then the same tx is broadcast.
        """
        sent, err = await self._rpc_send_with_err(
            signed_b64, skip_preflight=False, paid=True, paid_only=True, timeout=3.0 if fast else 4.0,
        )
        if sent:
            return sent, None
        text = str(err or "").lower()
        if any(bit in text for bit in ("simulation", "instruction", "custom", "-32002", "0x")):
            log.warning("SEND not broadcast — simulation rejected, no fee (%s)", str(err or "")[:140])
            return None, err
        return await self._rpc_send_with_err(
            signed_b64, skip_preflight=True, paid=True, paid_only=True, timeout=3.0 if fast else 4.0,
        )

    async def _rpc_send_with_err(self, signed_b64: str, skip_preflight: bool = True, paid: bool = False, paid_only: bool = False, timeout: float = 4.0):
        """Send a signed tx. Returns (signature, error). error is None on success.

        paid=True is the buy and the sell. One call to the paid RPC. A sell
        does not fall through to the free RPC. If that paid simulator only
        answers UNKNOWN, the same transaction is broadcast on the paid RPC.
        """
        import json
        public = self._public_endpoints()
        paid_url = self._paid_rpc() if paid else ""

        def _payload(skip: bool):
            return {
                "jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
                "params": [signed_b64, {
                    "encoding": "base64",
                    "skipPreflight": bool(skip),
                    "maxRetries": 0,
                    "preflightCommitment": "processed",
                }],
            }

        payload = _payload(skip_preflight)
        last_err = "no_rpc"

        async def _post(url: str, body):
            nonlocal last_err
            try:
                from src.execution.tx_route import rpc_post
                status, text = await asyncio.to_thread(rpc_post, url, body, timeout)
                if status != 200:
                    last_err = f"http_{status}:{(text or '')[:120]}"
                    log.warning("RPC sendTransaction HTTP %s %s", status, url.split("//")[-1][:40])
                    return None
                data = json.loads(text or "{}")
                if isinstance(data, dict) and data.get("result"):
                    return data["result"]
                last_err = (data.get("error") if isinstance(data, dict) else None) or f"http_{status}"
                log.warning("RPC sendTransaction error %s: %s", url.split("//")[-1][:40], str(last_err)[:160])
            except Exception as e:
                last_err = str(e) or type(e).__name__
                log.warning("RPC sendTransaction exception %s: %s", url.split("//")[-1][:40], last_err[:160])
            return None

        if paid_url:
            sig = await _post(paid_url, payload)
            if not sig and paid_only:
                text = str(last_err).lower()
                sim = any(bit in text for bit in ("simulation", "instruction", "custom", "-32002"))
                if not sim:
                    sig = await _post(paid_url, payload)
            if sig:
                log.info("sendTransaction paid RPC once sig=%s", str(sig)[:12])
                return sig, None
            if paid_only:
                log.warning("sendTransaction paid RPC failed — not using a free RPC")
                return None, last_err
            log.warning("sendTransaction paid RPC failed once — trying the next RPCs")
            for url in public[:3]:
                sig = await _post(url, payload)
                if sig:
                    log.info("sendTransaction free RPC sig=%s", str(sig)[:12])
                    return sig, None
            return None, last_err
        for url in public:
            sig = await _post(url, payload)
            if sig:
                log.info("sendTransaction free RPC sig=%s", str(sig)[:12])
                return sig, None
        return None, last_err

    async def _rpc_send(self, signed_b64: str, skip_preflight: bool = True) -> Optional[str]:
        sig, _err = await self._rpc_send_with_err(signed_b64, skip_preflight=skip_preflight)
        return sig

    def arm_live(self, enabled: bool = False) -> None:
        if enabled:
            if not live_armed():
                log.error("Cannot arm live: SOL_SNIPER_LIVE=1 + keypair + solders required")
                self.mode = ExecMode.PAPER
                return
            log.warning("LIVE MODE ARMED — real transactions will be sent")
            self.mode = ExecMode.LIVE
        else:
            self.mode = ExecMode.PAPER


_SOL_SEEN = {"ts": 0.0, "sol": None}


def seen_sol(max_age: float = 180.0):
    sol = _SOL_SEEN.get("sol")
    if sol is None:
        return None
    if time.time() - float(_SOL_SEEN.get("ts") or 0) > max_age:
        return None
    return float(sol)


def note_sol(sol: float) -> None:
    _SOL_SEEN["ts"] = time.time()
    _SOL_SEEN["sol"] = float(sol)


def quick_buy_block() -> str:
    """Same-second answer for a clicked Buy. Does not wait on the trading loop."""
    sol = seen_sol(180.0)
    if sol is None:
        sol = _read_sol_now(0.6)
    if sol is None:
        return "wallet_sol_unreadable"
    if sol < 0.00153:
        return f"wallet_sol_too_low bal={sol:.4f}"
    return ""


def _read_sol_now(timeout: float) -> Optional[float]:
    """The click balance. Paid primary only. Public RPCs are not part of a buy."""
    from src.execution.signer import pubkey_from_env
    from src.execution.wallet_tokens import primary_url
    pk = pubkey_from_env()
    paid = primary_url()
    if not pk or not paid:
        return seen_sol(1800.0)
    try:
        from src.execution.tx_route import rpc_post
        import json
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [pk],
        }
        status, text = rpc_post(paid, payload, min(float(timeout or 0.4), 0.6))
        if status == 200:
            lamports = ((json.loads(text or "{}").get("result") or {}).get("value"))
            if lamports is not None:
                sol = int(lamports) / 1_000_000_000
                note_sol(sol)
                return sol
    except Exception:
        pass
    return seen_sol(1800.0)
