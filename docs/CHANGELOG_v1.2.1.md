# SOL-SNIPER v1.2.1 — Bottleneck improvements

## P0
- **Filter modes**: `strict` | `scalp` | `pump_curve`
  - `strict` / `scalp`: mint+freeze must be renounced (live-safe)
  - `pump_curve`: paper-only exception for high-liq pump.fun when freeze REN but mint still SET → DOWNSIZE (0.35x)
- **Runtime config** (`src/config/runtime.py`) mutable from UI over WebSocket
- **Trade journal**: `logs/trades.jsonl`
- **Metrics**: fills, open, closed, win_rate, daily_pnl in stats/UI

## P1
- **Scalp profile** button in UI (short time stop, tighter trail/TP)
- Tip model already drives prioritization fees; uneconomic skips retained

## P2 UI — Command Center
- Live controls: filter mode, size, liq, stops, trail, tip cap, concurrent, trading enable
- Performance panel (PnL, win rate, fills)
- Decoded table: ticker, name, auth, why (reason), links
- Open positions + closed trades panels
- Apply settings without restart

## How to run 24h paper
```bash
python -m src.main --mode live-feed --trade-mode paper --duration 0
# UI http://localhost:8080/
# Select filter mode pump_curve in UI for more paper candidates (still not live-safe)
```

## Why BUY is still rare on strict
pump.fun creates usually keep mint authority SET until later lifecycle. High SOL liquidity alone never clears T1.
