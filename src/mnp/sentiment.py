"""External sentiment series: raw payloads -> sentiment_readings, and the queries over them.

Sentiment sources are collected like news (raw_items, checkpoints, health) and go through the
normalize job, which hands their payloads here instead of building articles.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.collectors import fear_greed
from mnp.jobs import PermanentJobError
from mnp.models import Asset, RawItem, SentimentReading, Source


@dataclass(frozen=True, slots=True)
class Metric:
    name: str
    scale: str  # how to read `value`
    # Shown next to every display of the data, as the provider's terms require.
    attribution: str
    attribution_url: str


METRICS: dict[str, Metric] = {
    fear_greed.INDEX: Metric(
        name="Crypto Fear & Greed Index",
        scale="0 (extreme fear) to 100 (extreme greed), daily at 00:00 UTC",
        attribution="Crypto Fear & Greed Index by Alternative.me",
        attribution_url="https://alternative.me/crypto/fear-and-greed-index/",
    ),
}

SENTIMENT_FORMATS = frozenset({fear_greed.FORMAT})
# Source kinds that produce sentiment readings rather than articles.
SENTIMENT_KINDS = frozenset({"fear_greed"})


@dataclass(frozen=True, slots=True)
class Reading:
    metric: str
    asset: str | None  # symbol; None = market-wide
    observed_at: datetime
    value: float
    label: str | None
    source: str
    received_at: datetime


def _parse(payload: dict[str, Any]) -> dict[str, Any]:
    """The reading's columns from a raw payload. Raises PermanentJobError if malformed."""
    match payload.get("format"):
        case fear_greed.FORMAT:
            point = payload.get("point") or {}
            try:
                return {
                    "metric": payload.get("index") or fear_greed.INDEX,
                    "asset_id": None,
                    "observed_at": datetime.fromtimestamp(int(point["timestamp"]), UTC),
                    "value": float(point["value"]),
                    "label": point.get("value_classification") or None,
                }
            except (KeyError, TypeError, ValueError) as exc:
                raise PermanentJobError(f"malformed Fear & Greed point: {point!r}") from exc
        case fmt:
            raise PermanentJobError(f"not a sentiment payload format: {fmt!r}")


async def store_sentiment_reading(conn: AsyncConnection, raw_item_id: int) -> bool:
    """Store the reading in a sentiment raw item. False if the item isn't sentiment data.

    Idempotent. A later raw item for the same point (a revised value) replaces the reading.
    """
    raw = (
        await conn.execute(
            select(RawItem.payload, RawItem.source_id, RawItem.fetched_at).where(
                RawItem.id == raw_item_id
            )
        )
    ).one_or_none()
    if raw is None or raw.payload.get("format") not in SENTIMENT_FORMATS:
        return False
    values = {
        **_parse(raw.payload),
        "source_id": raw.source_id,
        "raw_item_id": raw_item_id,
        "received_at": raw.fetched_at,
    }
    stmt = insert(SentimentReading).values(**values)
    await conn.execute(
        stmt.on_conflict_do_update(
            constraint="uq_sentiment_readings_source_id_metric_asset_id_observed_at",
            set_={k: stmt.excluded[k] for k in ("value", "label", "raw_item_id", "received_at")},
            where=SentimentReading.raw_item_id < stmt.excluded.raw_item_id,
        )
    )
    return True


async def get_readings(
    conn: AsyncConnection,
    *,
    metric: str | None = None,
    asset: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100,
) -> list[Reading]:
    """Newest first. `asset` is a symbol, or "market" for market-wide series only."""
    q = (
        select(
            SentimentReading.metric,
            Asset.symbol,
            SentimentReading.observed_at,
            SentimentReading.value,
            SentimentReading.label,
            Source.name,
            SentimentReading.received_at,
        )
        .join(Source, Source.id == SentimentReading.source_id)
        .outerjoin(Asset, Asset.id == SentimentReading.asset_id)
        .order_by(SentimentReading.observed_at.desc(), SentimentReading.id.desc())
        .limit(limit)
    )
    if metric:
        q = q.where(SentimentReading.metric == metric)
    if asset == "market":
        q = q.where(SentimentReading.asset_id.is_(None))
    elif asset:
        q = q.where(Asset.symbol == asset.upper())
    if since:
        q = q.where(SentimentReading.observed_at >= since)
    if until:
        q = q.where(SentimentReading.observed_at < until)
    return [Reading(*row) for row in (await conn.execute(q)).all()]
