"""
Jito block-engine bundle submission (SOL-SNIPER v1.2).

Sends base64-encoded signed transactions as a bundle.
Tip is typically an additional SOL transfer ix inside the tx or a separate tip tx.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("sol-sniper.jito")

# Public mainnet block engines (may rate-limit; prefer regional endpoint in prod)
DEFAULT_JITO_URLS = [
    "https://mainnet.block-engine.jito.wtf",
    "https://amsterdam.mainnet.block-engine.jito.wtf",
    "https://frankfurt.mainnet.block-engine.jito.wtf",
    "https://ny.mainnet.block-engine.jito.wtf",
]


@dataclass
class BundleResult:
    ok: bool
    bundle_id: Optional[str] = None
    endpoint: Optional[str] = None
    error: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


class JitoClient:
    def __init__(self, endpoints: Optional[List[str]] = None, timeout_s: float = 10.0):
        self.endpoints = endpoints or DEFAULT_JITO_URLS
        self._client = httpx.AsyncClient(timeout=timeout_s)
        self._idx = 0

    async def close(self) -> None:
        await self._client.aclose()

    def _next_url(self) -> str:
        url = self.endpoints[self._idx % len(self.endpoints)]
        self._idx += 1
        return url.rstrip("/")

    async def send_bundle(self, signed_txs_base64: List[str]) -> BundleResult:
        """
        sendBundle JSON-RPC: params = [ [base64Tx, ...], encoding ]
        """
        if not signed_txs_base64:
            return BundleResult(ok=False, error="empty bundle")

        payload = {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": "sendBundle",
            "params": [signed_txs_base64],
        }
        last_err = None
        for _ in range(len(self.endpoints)):
            base = self._next_url()
            url = f"{base}/api/v1/bundles"
            try:
                r = await self._client.post(url, json=payload)
                data = r.json()
                if r.status_code == 200 and "result" in data:
                    return BundleResult(
                        ok=True,
                        bundle_id=str(data["result"]),
                        endpoint=base,
                        raw=data,
                    )
                last_err = str(data.get("error") or data)[:300]
                log.warning("Jito sendBundle failed on %s: %s", base, last_err)
            except Exception as exc:
                last_err = str(exc)
                log.warning("Jito sendBundle error on %s: %s", base, exc)
        return BundleResult(ok=False, error=last_err or "all endpoints failed")

    async def get_bundle_statuses(self, bundle_ids: List[str]) -> Optional[Dict[str, Any]]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBundleStatuses",
            "params": [bundle_ids],
        }
        base = self._next_url()
        url = f"{base}/api/v1/bundles"
        try:
            r = await self._client.post(url, json=payload)
            return r.json()
        except Exception as e:
            log.warning("getBundleStatuses error: %s", e)
            return None
