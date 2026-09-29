"""Alternative.me Crypto Fear & Greed Index (GET https://api.alternative.me/fng/).

A market-wide crypto sentiment score from 0 (extreme fear) to 100 (extreme greed), one value a
day at 00:00 UTC, with history back to February 2018. Free and keyless. Terms: the source must be
credited next to any display of the data (see mnp.sentiment.ATTRIBUTIONS); commercial use is
allowed on that condition.

Each daily point is one raw item, keyed by its timestamp. `time_until_update` is dropped before
storing: it changes on every request, so keeping it would store the same value again each poll.
"""

import hashlib
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from mnp.collectors.base import Collector, CollectorError, RawPayload, raise_for_status
from mnp.collectors.finnhub import canonical_json

FORMAT = "fear_greed"
INDEX = "crypto_fear_greed"
POINT_FIELDS = ("value", "value_classification", "timestamp")
DAY = 86400


class FearGreedCollector(Collector):
    async def fetch(self, checkpoint: Mapping[str, Any]) -> tuple[list[RawPayload], dict[str, Any]]:
        last = int(checkpoint.get("last_timestamp") or 0)
        # First run: the full history. After that, the days since the last point, plus a
        # margin so a late or revised value for the latest day is picked up.
        now = datetime.now(UTC).timestamp()
        limit = 0 if not last else max(2, math.ceil((now - last) / DAY) + 2)
        payloads = [p for p in await self._get(limit) if int(p.external_id or 0) >= last]
        newest = max((int(p.external_id or 0) for p in payloads), default=last)
        return payloads, ({"last_timestamp": newest} if newest else {})

    async def fetch_history(self, since: datetime) -> list[RawPayload]:
        return [p for p in await self._get(0) if p.published_at and p.published_at >= since]

    async def _get(self, limit: int) -> list[RawPayload]:
        response = await self.client.get(
            str(self.source.url),
            params={"limit": limit, "format": "json"},
            headers={"Accept": "application/json"},
        )
        raise_for_status(response)
        try:
            body = response.json()
        except ValueError as exc:
            raise CollectorError(f"invalid JSON from Fear & Greed API: {exc}") from exc
        if not isinstance(body, dict) or not isinstance(body.get("data"), list):
            raise CollectorError(f"unexpected Fear & Greed response: {str(body)[:200]}")
        if error := (body.get("metadata") or {}).get("error"):
            raise CollectorError(f"Fear & Greed API error: {str(error)[:200]}")

        payloads = []
        for item in body["data"]:
            if not isinstance(item, dict):
                continue
            point = {k: item[k] for k in POINT_FIELDS if k in item}
            try:
                ts = int(point["timestamp"])
                float(point["value"])
            except (KeyError, TypeError, ValueError):
                continue
            payloads.append(
                RawPayload(
                    payload={"format": FORMAT, "index": INDEX, "point": point},
                    payload_sha256=hashlib.sha256(canonical_json(point)).hexdigest(),
                    external_id=str(ts),
                    published_at=datetime.fromtimestamp(ts, UTC),
                )
            )
        return payloads
