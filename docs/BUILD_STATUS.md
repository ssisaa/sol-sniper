# SOL-SNIPER v1.2 — Build Status

**Last updated:** 2026-10-12 21:20 +08

## Live execution stack

| Component | File | Status |
|-----------|------|--------|
| Jupiter quote + swap tx | `src/execution/jupiter.py` | ✅ |
| Jito sendBundle | `src/execution/jito.py` | ✅ |
| Keypair load + sign | `src/execution/signer.py` | ✅ (needs `solders`) |
| Price oracle (Jupiter) | `src/execution/price_oracle.py` | ✅ |
| Trader paper/live | `src/execution/trader.py` | ✅ |
| Exit loop + risk | `src/risk/position_manager.py` | ✅ |

## Arming live trading (real SOL)

```bash
pip install solders base58
export SOL_SNIPER_LIVE=1
export SOL_SNIPER_KEYPAIR=/path/to/id.json   # Solana CLI JSON array
# or: export SOL_SNIPER_PRIVATE_KEY=<base58 secret>

PYTHONPATH=. python -m src.main --mode live-feed --trade-mode live --duration 120
```

Without `SOL_SNIPER_LIVE=1` + keypair, live buys return `live_not_armed` and no tx is sent.

## Paper (default)

```bash
PYTHONPATH=. python -m src.main --mode paper --duration 180 --max-decodes 25
```

Paper fills use **Jupiter quotes when reachable**, else synthetic slippage.
Exit marks use Jupiter for real mints; seeded demo mints use synthetic walk.
