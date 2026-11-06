#!/usr/bin/env python3
"""
Ingest the 100 knowledge units into Qdrant (F-08).

Usage:
    python scripts/ingest_kb.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Ensure project root is on path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.types.models import KnowledgeUnit


def load_units(path: Path) -> list[KnowledgeUnit]:
    if not path.exists():
        print(f"[WARN] KB file not found at {path}")
        print("       Create kb/knowledge_units.json with the 100 units first.")
        return []
    raw = json.loads(path.read_text())
    units = [KnowledgeUnit(**u) for u in raw]
    print(f"Loaded {len(units)} knowledge units")
    return units


def main() -> None:
    kb_path = ROOT / "kb" / "knowledge_units.json"
    units = load_units(kb_path)

    if not units:
        print("No units to ingest. Exiting.")
        return

    # Placeholder: real implementation will embed + upsert to Qdrant
    print("Ingestion scaffold ready.")
    print(f"Would upsert {len(units)} units into collection 'sol_sniper_kb'")
    print("Next: add embedding + qdrant-client upsert logic.")


if __name__ == "__main__":
    main()
