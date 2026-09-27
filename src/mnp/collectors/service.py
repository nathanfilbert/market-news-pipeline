"""Run collectors against the database: store payloads, checkpoints, failures, backoff."""

import asyncio
import logging
import random
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from mnp.collectors.base import Collector, CollectorUnavailable
from mnp.collectors.finnhub import FinnhubCollector
from mnp.collectors.rss import RssCollector
from mnp.config import Settings, SourceConfig
from mnp.jobs import NORMALIZE, enqueue
from mnp.models import RawItem, Source, SourceState

log = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 3600.0
MAX_ERROR_LENGTH = 2000


@dataclass(frozen=True, slots=True)
class CollectResult:
    source: str
    received: int = 0
    inserted: int = 0
    consecutive_failures: int = 0
    error: str | None = None
    retry_after: float | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def make_collector(
    source: SourceConfig, client: httpx.AsyncClient, settings: Settings
) -> Collector:
    """The collector for a source. Raises CollectorUnavailable if it can't run here."""
    match source.kind:
        case "rss":
            return RssCollector(source, client)
        case "finnhub":
            if settings.finnhub_api_key is None:
                raise CollectorUnavailable("FINNHUB_API_KEY not set")
            return FinnhubCollector(source, client, settings.finnhub_api_key.get_secret_value())
    raise CollectorUnavailable(f"no collector for kind {source.kind!r}")


async def sync_sources(engine: AsyncEngine, sources: Iterable[SourceConfig]) -> dict[str, int]:
    """Upsert sources from config, disable ones no longer configured. Returns name -> id."""
    sources = list(sources)
    ids: dict[str, int] = {}
    async with engine.begin() as conn:
        for src in sources:
            values = {
                "kind": src.kind,
                "url": str(src.url),
                "category": src.category,
                "reputation": src.reputation,
                "enabled": src.enabled,
                "poll_seconds": src.poll_seconds,
                "language": src.language,
            }
            stmt = insert(Source).values(name=src.name, **values)
            stmt = stmt.on_conflict_do_update(index_elements=[Source.name], set_=values)
            ids[src.name] = (await conn.execute(stmt.returning(Source.id))).scalar_one()
        await conn.execute(
            update(Source).where(Source.name.not_in(list(ids) or [""])).values(enabled=False)
        )
        if ids:
            await conn.execute(
                insert(SourceState)
                .values([{"source_id": source_id} for source_id in ids.values()])
                .on_conflict_do_nothing()
            )
    return ids


async def collect_once(engine: AsyncEngine, source_id: int, collector: Collector) -> CollectResult:
    """Fetch one source and store new payloads. Never raises (except on cancellation)."""
    name = collector.source.name
    try:
        async with engine.connect() as conn:
            checkpoint = (
                await conn.execute(
                    select(SourceState.checkpoint).where(SourceState.source_id == source_id)
                )
            ).scalar_one_or_none() or {}

        payloads, new_checkpoint = await collector.fetch(checkpoint)
        fetched_at = datetime.now(UTC)

        # Payloads, their normalize jobs and the checkpoint commit together, so a crash can't
        # skip items.
        async with engine.begin() as conn:
            inserted = 0
            if payloads:
                stmt = (
                    insert(RawItem)
                    .values(
                        [
                            {
                                "source_id": source_id,
                                "fetched_at": fetched_at,
                                "external_id": p.external_id,
                                "url": p.url,
                                "payload": p.payload,
                                "payload_sha256": p.payload_sha256,
                            }
                            for p in payloads
                        ]
                    )
                    .on_conflict_do_nothing(index_elements=["source_id", "payload_sha256"])
                    .returning(RawItem.id)
                )
                new_ids = (await conn.execute(stmt)).scalars().all()
                inserted = len(new_ids)
                await enqueue(conn, NORMALIZE, ((str(i), {"raw_item_id": i}) for i in new_ids))
            await conn.execute(
                update(SourceState)
                .where(SourceState.source_id == source_id)
                .values(
                    checkpoint=new_checkpoint,
                    last_success_at=fetched_at,
                    consecutive_failures=0,
                )
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
        log.warning("collect failed for %s: %s", name, error, exc_info=True)
        failures = await _record_failure(engine, source_id, error)
        return CollectResult(
            source=name,
            consecutive_failures=failures,
            error=error,
            retry_after=getattr(exc, "retry_after", None),
        )

    log.info("collected %s: %d received, %d new", name, len(payloads), inserted)
    return CollectResult(source=name, received=len(payloads), inserted=inserted)


async def _record_failure(engine: AsyncEngine, source_id: int, error: str) -> int:
    try:
        async with engine.begin() as conn:
            return (
                await conn.execute(
                    update(SourceState)
                    .where(SourceState.source_id == source_id)
                    .values(
                        last_error_at=func.now(),
                        last_error=error,
                        consecutive_failures=SourceState.consecutive_failures + 1,
                    )
                    .returning(SourceState.consecutive_failures)
                )
            ).scalar_one()
    except Exception:
        log.exception("could not record failure for source %s", source_id)
        return 1


def next_delay(
    poll_seconds: float,
    consecutive_failures: int,
    retry_after: float | None = None,
    rng: random.Random | None = None,
) -> float:
    """Seconds until the next poll: jittered interval, exponential backoff on failure."""
    rng = rng or random.Random()
    if consecutive_failures <= 0:
        delay = poll_seconds * rng.uniform(0.9, 1.1)
    else:
        cap = max(MAX_BACKOFF_SECONDS, poll_seconds)
        delay = min(cap, poll_seconds * 2 ** min(consecutive_failures, 20)) * rng.uniform(0.5, 1.0)
    return max(delay, retry_after or 0.0)


async def run_source_loop(
    engine: AsyncEngine, source_id: int, collector: Collector, stop: asyncio.Event
) -> None:
    """Poll one source until `stop` is set. Failures are recorded and backed off, never fatal."""
    while not stop.is_set():
        result = await collect_once(engine, source_id, collector)
        delay = next_delay(
            collector.source.poll_seconds, result.consecutive_failures, result.retry_after
        )
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)
