"""Finnhub market news aggregator (GET /api/v1/news).

Free plan: personal use only, 60 calls/minute, no redistribution of the data. The API key is
sent in the X-Finnhub-Token header, never in the URL, so it can't end up in logs.

Each article object is stored as received. JSON arrays can't be split into per-item byte
ranges the way XML can, so payload_sha256 hashes a canonical serialization of the item
(sorted keys, compact separators): identical content always hashes the same.
"""

import hashlib
import json
from collections.abc import Mapping
from typing import Any

import httpx

from mnp.collectors.base import Collector, CollectorError, RawPayload, raise_for_status
from mnp.config import SourceConfig


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class FinnhubCollector(Collector):
    def __init__(self, source: SourceConfig, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(source, client)
        self.api_key = api_key
        self.category = source.options.get("category", "general")

    async def fetch(self, checkpoint: Mapping[str, Any]) -> tuple[list[RawPayload], dict[str, Any]]:
        min_id = int(checkpoint.get("min_id") or 0)
        params: dict[str, Any] = {"category": self.category}
        if min_id:
            params["minId"] = min_id  # only articles newer than this id
        response = await self.client.get(
            str(self.source.url),
            params=params,
            headers={"X-Finnhub-Token": self.api_key, "Accept": "application/json"},
        )
        raise_for_status(response)
        try:
            items = response.json()
        except ValueError as exc:
            raise CollectorError(f"invalid JSON from Finnhub: {exc}") from exc
        if not isinstance(items, list):
            raise CollectorError(f"unexpected Finnhub response: {str(items)[:200]}")

        payloads = []
        max_id = min_id
        for item in items:
            if not isinstance(item, dict):
                continue
            item_id = item.get("id")
            if isinstance(item_id, int):
                if item_id <= min_id:
                    continue
                max_id = max(max_id, item_id)
            payloads.append(
                RawPayload(
                    payload={"format": "finnhub_news", "category": self.category, "item": item},
                    payload_sha256=hashlib.sha256(canonical_json(item)).hexdigest(),
                    external_id=None if item_id is None else str(item_id),
                    url=item.get("url") or None,
                )
            )
        return payloads, ({"min_id": max_id} if max_id else {})
