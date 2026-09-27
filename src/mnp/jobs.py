"""Postgres job queue between pipeline stages.

A job is claimed with SELECT … FOR UPDATE SKIP LOCKED and its handler runs inside the same
transaction, so the handler's writes and the job's completion commit together: a crash leaves
the job pending, never half-done. Failures roll back to a savepoint, then the attempt is
recorded and the job is retried with backoff until it succeeds or runs out of attempts.
"""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from mnp.models import Job

log = logging.getLogger(__name__)

NORMALIZE = "normalize"

MAX_ATTEMPTS = 8
MAX_ERROR_LENGTH = 2000

Handler = Callable[[AsyncConnection, dict[str, Any]], Awaitable[Any]]


class PermanentJobError(Exception):
    """Retrying can't help (e.g. unparseable input): fail the job immediately."""


@dataclass(slots=True)
class JobStats:
    done: int = 0
    retrying: int = 0
    failed: int = 0

    @property
    def processed(self) -> int:
        return self.done + self.retrying + self.failed


async def enqueue(
    conn: AsyncConnection, kind: str, jobs: Iterable[tuple[str, dict[str, Any]]]
) -> int:
    """Queue (dedupe_key, payload) jobs; ones already queued under that key are skipped."""
    values = [{"kind": kind, "dedupe_key": key, "payload": payload} for key, payload in jobs]
    if not values:
        return 0
    result = await conn.execute(
        insert(Job).values(values).on_conflict_do_nothing().returning(Job.id)
    )
    return len(result.all())


def retry_delay(attempts: int, rng: random.Random | None = None) -> float:
    """Seconds before retrying after `attempts` failed attempts: 30s doubling, capped at 1h."""
    rng = rng or random.Random()
    return min(3600.0, 30.0 * 2 ** max(0, attempts - 1)) * rng.uniform(0.5, 1.0)


async def run_pending(
    engine: AsyncEngine,
    kind: str,
    handler: Handler,
    *,
    limit: int | None = None,
    max_attempts: int = MAX_ATTEMPTS,
) -> JobStats:
    """Process due jobs of `kind` one at a time until none are left (or `limit` is reached)."""
    stats = JobStats()
    while limit is None or stats.processed < limit:
        async with engine.begin() as conn:
            job = (
                await conn.execute(
                    select(Job.id, Job.payload, Job.attempts)
                    .where(Job.kind == kind, Job.status == "pending", Job.run_after <= func.now())
                    .order_by(Job.run_after, Job.id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            ).first()
            if job is None:
                break
            attempts = job.attempts + 1
            try:
                async with conn.begin_nested():
                    await handler(conn, job.payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
                if isinstance(exc, PermanentJobError) or attempts >= max_attempts:
                    values = {"status": "failed", "finished_at": func.now()}
                    stats.failed += 1
                    log.error("%s job %d failed permanently: %s", kind, job.id, error)
                else:
                    delay = timedelta(seconds=retry_delay(attempts))
                    values = {"run_after": func.now() + delay}
                    stats.retrying += 1
                    log.warning("%s job %d failed (attempt %d): %s", kind, job.id, attempts, error)
                values |= {"attempts": attempts, "locked_at": func.now(), "last_error": error}
            else:
                values = {
                    "status": "done",
                    "attempts": attempts,
                    "locked_at": func.now(),
                    "finished_at": func.now(),
                }
                stats.done += 1
            await conn.execute(update(Job).where(Job.id == job.id).values(**values))
    return stats
