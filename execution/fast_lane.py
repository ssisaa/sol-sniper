"""Buy lane and sell lane, each on its own thread.

The scan loop can be busy. A click does not wait for it.
A buy never waits for a sell. A sell never waits for a buy.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Callable, Optional

log = logging.getLogger("sol-sniper.lane")

_lane: Optional["FastLane"] = None


def get_lane() -> "FastLane":
    global _lane
    if _lane is None:
        _lane = FastLane()
    return _lane


class FastLane:
    def __init__(self) -> None:
        self.started = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread_id = 0
        self.buy_q: Optional[asyncio.Queue] = None
        self.sell_q: Optional[asyncio.Queue] = None
        self.buys: set = set()
        self.sells: set = set()
        self._buy_depth = 0
        self._sell_depth = 0
        self._one_buy: Optional[asyncio.Lock] = None
        self._ready = threading.Event()

    def start(self) -> None:
        if self.started:
            return

        def _run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.loop = loop
            self._thread_id = threading.get_ident()

            async def _boot() -> None:
                self.buy_q = asyncio.Queue()
                self.sell_q = asyncio.Queue()
                self._one_buy = asyncio.Lock()
                loop.create_task(self._sell_loop())
                self.started = True
                self._ready.set()

            loop.create_task(_boot())
            loop.run_forever()

        threading.Thread(target=_run, name="fast-lane", daemon=True).start()
        self._ready.wait(2.0)
        log.warning("FAST LANE on — buy and sell run on their own thread, not behind the scan")

    def here(self) -> bool:
        return threading.get_ident() == self._thread_id and self._thread_id != 0

    def on_buy(self) -> bool:
        return self._buy_depth > 0

    def on_sell(self) -> bool:
        return self._sell_depth > 0

    def submit(self, coro) -> None:
        """From the click thread. The main loop is not involved."""
        loop = self.loop
        if loop is None:
            return
        asyncio.run_coroutine_threadsafe(coro, loop)

    async def run_buy(self, mint: str, label: str, factory: Callable[[], Any]):
        if not self.started or self.loop is None:
            return await factory()
        if self.here():
            return await self._guarded_buy(mint, label, factory)
        fut = asyncio.run_coroutine_threadsafe(self._guarded_buy(mint, label, factory), self.loop)
        return await asyncio.wrap_future(fut)

    async def _guarded_buy(self, mint: str, label: str, factory: Callable[[], Any]):
        if self.on_buy():
            return await factory()
        if mint in self.buys:
            log.info("BUY LANE %s — this mint is already sending", label)
            return None
        lock = self._one_buy
        if lock is None:
            return await factory()
        async with lock:
            if mint in self.buys:
                return None
            self.buys.add(mint)
            self._buy_depth += 1
            started = time.perf_counter()
            log.info("BUY LANE %s — sending now", label)
            try:
                return await factory()
            finally:
                self._buy_depth -= 1
                self.buys.discard(mint)
                log.info("BUY LANE done %s in %.2fs", label, time.perf_counter() - started)

    def fire_sell(self, mint: str, label: str, factory: Callable[[], Any]) -> bool:
        if not mint:
            return False
        if self.loop is None:
            try:
                asyncio.get_running_loop().create_task(factory())
            except RuntimeError:
                log.warning("SELL LANE %s — no loop", label)
            return True
        if self.here():
            if mint in self.sells:
                log.info("SELL LANE %s — already sending", label)
                return False
            self.sells.add(mint)
            log.info("SELL LANE %s — sending now", label)
            self.sell_q.put_nowait((mint, label, factory))
            return True
        asyncio.run_coroutine_threadsafe(self._enqueue_sell(mint, label, factory), self.loop)
        return True

    async def _enqueue_sell(self, mint: str, label: str, factory: Callable[[], Any]) -> None:
        if mint in self.sells:
            log.info("SELL LANE %s — already sending", label)
            return
        self.sells.add(mint)
        log.info("SELL LANE %s — sending now", label)
        await self.sell_q.put((mint, label, factory))

    async def _sell_loop(self) -> None:
        while True:
            mint, label, factory = await self.sell_q.get()
            self._sell_depth += 1
            started = time.perf_counter()
            try:
                await factory()
            except Exception:
                log.exception("SELL LANE %s crashed", label)
            finally:
                self._sell_depth -= 1
                self.sells.discard(mint)
                log.info("SELL LANE done %s in %.2fs", label, time.perf_counter() - started)
                self.sell_q.task_done()


def fire_sell(mint: str, label: str, factory: Callable[[], Any]) -> bool:
    return get_lane().fire_sell(mint, label, factory)
