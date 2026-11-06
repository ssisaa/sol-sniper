# Knowledge Base (F-08)

This directory holds the 100 knowledge units for SOL-SNIPER v1.2.

## Expected File

- `knowledge_units.json` — array of 100 objects following the exact schema:

```json
{
  "id": "sol-snipe-001",
  "source": "solana-snipe-research-2026",
  "topic": "SolanaNative",
  "conditions": ["HighVolatility", "VolumeExpansion"],
  "content": "...",
  "priority": 5,
  "tags": ["sniping", "solana", "..."]
}
```

## Current Status

The full 100 units (001–100) were generated in the design phase:

- 001–072 : Original seed (Entry, Rug, Exit, Execution, Sizing, Regime)
- 073–082 : C7 Metadata NLP
- 083–087 : C8 Post-Trade Learning
- 088–094 : C1 Entry expansion
- 095–100 : C3 Exit expansion

Place the complete JSON array into `knowledge_units.json` and run:

```bash
python scripts/ingest_kb.py
```

The ingestion script will validate, embed, and upsert into Qdrant.
