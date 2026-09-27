import asyncio
import random

import pytest
from sqlalchemy import func, insert, select

from mnp.jobs import PermanentJobError, enqueue, retry_delay, run_pending
from mnp.models import Job, Source

pytestmark = pytest.mark.db


async def queue(engine, keys, kind="test"):
    async with engine.begin() as conn:
        return await enqueue(conn, kind, ((k, {"key": k}) for k in keys))


async def job(engine, key, kind="test") -> Job:
    async with engine.connect() as conn:
        return (
            await conn.execute(select(Job).where(Job.kind == kind, Job.dedupe_key == key))
        ).one()


async def test_enqueue_dedupes_by_kind_and_key(engine):
    assert await queue(engine, ["a", "b", "a"]) == 2
    assert await queue(engine, ["a", "c"]) == 1
    assert await queue(engine, ["a"], kind="other") == 1


async def test_jobs_run_once_and_are_marked_done(engine):
    await queue(engine, ["a", "b"])
    seen = []

    async def handler(conn, payload):
        seen.append(payload["key"])

    stats = await run_pending(engine, "test", handler)
    assert (stats.done, stats.retrying, stats.failed) == (2, 0, 0)
    assert seen == ["a", "b"]
    assert (await job(engine, "a")).status == "done"
    assert (await run_pending(engine, "test", handler)).processed == 0


async def test_failure_rolls_back_handler_writes_and_schedules_retry(engine):
    await queue(engine, ["a"])

    async def handler(conn, payload):
        await conn.execute(
            insert(Source).values(
                name="ghost", kind="rss", url="u", category="c", reputation=0, poll_seconds=1
            )
        )
        raise RuntimeError("upstream down")

    stats = await run_pending(engine, "test", handler)
    assert stats.retrying == 1
    j = await job(engine, "a")
    assert (j.status, j.attempts) == ("pending", 1)
    assert "RuntimeError: upstream down" in j.last_error
    async with engine.connect() as conn:
        assert (await conn.execute(select(func.count()).select_from(Source))).scalar() == 0
        # scheduled in the future, so it isn't retried immediately
        assert (await conn.execute(select(Job.run_after > func.now()))).scalar()
    assert (await run_pending(engine, "test", handler)).processed == 0


async def test_permanent_error_and_attempt_limit_fail_the_job(engine):
    await queue(engine, ["perm", "flaky"])

    async def handler(conn, payload):
        if payload["key"] == "perm":
            raise PermanentJobError("bad input")
        raise RuntimeError("still down")

    stats = await run_pending(engine, "test", handler, max_attempts=1)
    assert stats.failed == 2
    perm = await job(engine, "perm")
    assert (perm.status, perm.attempts) == ("failed", 1)
    assert perm.finished_at is not None


async def test_concurrent_workers_never_share_a_job(engine):
    await queue(engine, [str(i) for i in range(20)])
    seen = []

    async def handler(conn, payload):
        seen.append(payload["key"])
        await asyncio.sleep(0.01)

    results = await asyncio.gather(*(run_pending(engine, "test", handler) for _ in range(4)))
    assert sum(r.done for r in results) == 20
    assert sorted(seen) == sorted(str(i) for i in range(20))


def test_retry_delay_grows_and_caps():
    rng = random.Random(0)
    assert 15 <= retry_delay(1, rng) <= 30
    assert 60 <= retry_delay(3, rng) <= 120
    assert 1800 <= retry_delay(30, rng) <= 3600
