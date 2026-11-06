# SOL-SNIPER v2.1

Stable v2.1. Callout and OpenHolds are in. State is SQLite at sqlitebase/sniper.db.

Autonomous Solana DEX sniping bot optimized for 15-second to 5-minute memecoin timeframes (pump.fun → Raydium / pump.swap).

**Status:** Foundation + Live RPC + Hard Filters + Tip Model (v1.2)

**Live mainnet connection verified** (slot + blockhash + failover working).

## Architecture Overview

```
L6  Observability · Kill Switch · Attribution Loop · Competition Estimator
L5  Risk & Portfolio Manager (sizing, circuit breakers, sub-second exit loop)
L4  Execution (Jito bundles, edge-aware tips, multi-RPC fan-out, wallet pool, simulation)
L3  Decision Engine
      Stage A: Hard Deterministic Filters (<18ms, non-overridable)
      Stage B: Regime-aware RAG + Local LLM (async, sizing/ranking only)
L2  Signal Layer (pre-filtered pump.fun / Raydium / AddLiquidity events)
L1  Data Layer (Yellowstone gRPC primary + multi-RPC failover)
```

**Critical Invariant:** Hard filters alone decide BUY / SKIP / DOWNSIZE. LLM/RAG never overrides a hard reject.

## Knowledge Base

- 100 high-quality units (`kb/knowledge_units.json`)
- Categories: Entry, Rug/Honeypot, Exit, Execution, Sizing/Risk, Regime, Metadata NLP, Post-Trade Learning
- Loaded into Qdrant with metadata filtering
- Attribution + weekly pruning/promotion (F-21 + F-25)

## Quick Start (Development)

```bash
# 1. Install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Copy and edit config
cp config/config.example.yaml config/config.yaml

# 3. Start infrastructure (Qdrant + Redis)
docker compose up -d

# 4. Ingest knowledge base
python scripts/ingest_kb.py

# 5. Run in paper mode
python -m src.main --mode paper
```

## Project Structure

```
sol-sniper/
├── config/                 # Runtime configuration
├── kb/                     # 100 knowledge units (JSON)
├── src/
│   ├── data/               # L1 – gRPC / RPC listeners
│   ├── signal/             # L2 – event parsers + in-stream pre-filter
│   ├── decision/           # L3 – Hard filters + RAG engine
│   ├── execution/          # L4 – Jito, tips, wallets, simulation
│   ├── risk/               # L5 – Sizing, exits, circuit breakers
│   ├── observability/      # L6 – Logging, attribution, alerts, kill switch
│   ├── types/              # Shared Pydantic / dataclass models
│   └── utils/              # Helpers
├── scripts/                # Ingestion, backtest, maintenance
├── tests/
└── docs/
```

## Development Phases (Current Focus)

1. **Foundation (Current)**  
   - Project skeleton + config  
   - Full 100-unit KB  
   - Hard Deterministic Filter Stack (F-06) – fully implemented  
   - Types & interfaces for all layers  
   - Paper-trading harness  

2. **Core Hot Path**  
   - In-stream pre-filter  
   - Signal parsers (pump.fun graduation, Raydium pool)  
   - Edge-aware tip model  
   - Multi-wallet manager  

3. **RAG + Regime**  
   - Qdrant integration  
   - Regime vector  
   - Local LLM (vLLM) adapter  

4. **Execution & Risk**  
   - Jito bundle construction  
   - Sub-second exit loop (separate process)  
   - Circuit breakers  

5. **Observability & Hardening**  
   - Attribution loop  
   - Competition estimator  
   - Kill switch + alerts  

## Safety & Risk Notes

- Designed for **micro-capital** only.
- Hard filters are the primary defense against rugs/honeypots.
- Edge-aware tip model prevents fee farming.
- Paper mode is the default until explicitly enabled for live.
- Never run with more capital than you can afford to lose completely.

## License & Disclaimer

This is experimental software for educational and research purposes.  
Trading cryptocurrencies involves substantial risk of loss.  
Use at your own risk. The authors assume no liability.
