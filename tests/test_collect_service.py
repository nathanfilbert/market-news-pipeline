import asyncio
import random

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from mnp.collectors import service
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, next_delay, run_source_loop, sync_sources
from mnp.models import RawItem, Source, SourceState
from tests.conftest import fixture_bytes, source

pytestmark = pytest.mark.db


async def count_raw(engine, source_id=None) -> int:
    stmt = select(func.count()).select_from(RawItem)
    if source_id is not None:
        stmt = stmt.where(RawItem.source_id == source_id)
    async with engine.connect() as conn:
        return (await conn.execute(stmt)).scalar_one()


async def state(engine, source_id) -> SourceState:
    async with engine.connect() as conn:
        return (
            await conn.execute(select(SourceState).where(SourceState.source_id == source_id))
        ).one()


async def test_sync_sources_upserts_and_disables_removed(engine):
    ids = await sync_sources(engine, [source("a"), source("b")])
    assert set(ids) == {"a", "b"}
    ids2 = await sync_sources(engine, [source("a", poll_seconds=300)])
    assert ids2["a"] == ids["a"]
    async with engine.connect() as conn:
        rows = dict((await conn.execute(select(Source.name, Source.enabled))).all())
        poll = (await conn.execute(select(Source.poll_seconds).where(Source.name == "a"))).scalar()
    assert rows == {"a": True, "b": False}
    assert poll == 300


async def test_collecting_twice_inserts_no_duplicates(engine, server, client):
    ids = await sync_sources(engine, [source("a")])
    server.serve("a", fixture="rss/wordpress.xml")
    collector = RssCollector(source("a"), client)

    first = await collect_once(engine, ids["a"], collector)
    second = await collect_once(engine, ids["a"], collector)

    assert (first.received, first.inserted) == (3, 3)
    assert (second.received, second.inserted) == (3, 0)
    assert await count_raw(engine) == 3


async def test_edited_item_is_stored_as_new_raw_item(engine, server, client):
    ids = await sync_sources(engine, [source("a")])
    collector = RssCollector(source("a"), client)
    server.serve("a", fixture="rss/wordpress.xml")
    await collect_once(engine, ids["a"], collector)
    server.serve("a", fixture="rss/wordpress_edited.xml")
    result = await collect_once(engine, ids["a"], collector)

    assert result.inserted == 1
    async with engine.connect() as conn:
        ext_ids = (await conn.execute(select(RawItem.external_id).order_by(RawItem.id))).scalars()
        assert list(ext_ids)[-1] == "https://news.example.com/?p=1002"


async def test_same_item_twice_in_one_feed_is_stored_once(engine, server, client):
    ids = await sync_sources(engine, [source("a")])
    data = fixture_bytes("rss/xml_base.xml")
    item = data[data.index(b"<item>") : data.index(b"</item>") + len(b"</item>")]
    server.responses["a.example.com"] = httpx.Response(200, content=data.replace(item, item * 2))

    result = await collect_once(engine, ids["a"], RssCollector(source("a"), client))
    assert (result.received, result.inserted) == (2, 1)


async def test_checkpoint_is_persisted_and_sent(engine, server, client):
    ids = await sync_sources(engine, [source("a")])
    collector = RssCollector(source("a"), client)
    server.serve("a", fixture="rss/wordpress.xml", ETag='"v1"')
    await collect_once(engine, ids["a"], collector)
    assert (await state(engine, ids["a"])).checkpoint == {"etag": '"v1"'}

    server.serve("a", status=304)
    result = await collect_once(engine, ids["a"], collector)
    assert result.ok and result.received == 0
    assert server.requests[-1].headers["if-none-match"] == '"v1"'
    assert (await state(engine, ids["a"])).checkpoint == {"etag": '"v1"'}


async def test_failed_source_does_not_stop_others(engine, server, client):
    ids = await sync_sources(engine, [source("good"), source("bad")])
    server.serve("good", fixture="rss/wordpress.xml")
    server.serve("bad", status=500)

    results = await asyncio.gather(
        collect_once(engine, ids["good"], RssCollector(source("good"), client)),
        collect_once(engine, ids["bad"], RssCollector(source("bad"), client)),
    )

    good, bad = results
    assert good.ok and good.inserted == 3
    assert not bad.ok and "HTTP 500" in bad.error
    assert await count_raw(engine, ids["good"]) == 3
    bad_state = await state(engine, ids["bad"])
    assert bad_state.consecutive_failures == 1
    assert "HTTP 500" in bad_state.last_error
    assert bad_state.last_success_at is None


async def test_failures_accumulate_then_reset_keeping_checkpoint(engine, server, client):
    ids = await sync_sources(engine, [source("a")])
    collector = RssCollector(source("a"), client)
    server.serve("a", fixture="rss/wordpress.xml", ETag='"v1"')
    await collect_once(engine, ids["a"], collector)

    server.serve("a", fixture="rss/not_a_feed.html")
    await collect_once(engine, ids["a"], collector)
    server.serve("a", status=429, **{"Retry-After": "300"})
    result = await collect_once(engine, ids["a"], collector)
    assert result.consecutive_failures == 2
    assert result.retry_after == 300
    failing = await state(engine, ids["a"])
    assert failing.checkpoint == {"etag": '"v1"'}  # unchanged by failures

    server.serve("a", status=304)
    assert (await collect_once(engine, ids["a"], collector)).ok
    recovered = await state(engine, ids["a"])
    assert recovered.consecutive_failures == 0
    assert recovered.last_error is not None  # history kept for /health


async def test_raw_items_are_append_only(engine, server, client):
    ids = await sync_sources(engine, [source("a")])
    server.serve("a", fixture="rss/wordpress.xml")
    await collect_once(engine, ids["a"], RssCollector(source("a"), client))

    for sql in ("UPDATE raw_items SET url = 'x'", "DELETE FROM raw_items"):
        with pytest.raises(DBAPIError, match="append-only"):
            async with engine.begin() as conn:
                await conn.execute(text(sql))
    assert await count_raw(engine) == 3


async def test_loop_survives_failures_until_stopped(engine, server, client, monkeypatch):
    ids = await sync_sources(engine, [source("a")])
    server.serve("a", status=503)
    stop = asyncio.Event()
    delays = []

    def fake_delay(poll_seconds, failures, retry_after=None, rng=None):
        delays.append(failures)
        if len(delays) == 3:
            stop.set()
        return 0

    monkeypatch.setattr(service, "next_delay", fake_delay)
    await asyncio.wait_for(
        run_source_loop(engine, ids["a"], RssCollector(source("a"), client), stop), timeout=5
    )
    assert delays == [1, 2, 3]


def test_next_delay_backoff():
    rng = random.Random(0)
    assert 54 <= next_delay(60, 0, rng=rng) <= 66
    assert 60 <= next_delay(60, 1, rng=rng) <= 120
    assert 240 <= next_delay(60, 3, rng=rng) <= 480
    assert 1800 <= next_delay(60, 50, rng=rng) <= 3600
    assert next_delay(60, 0, retry_after=900, rng=rng) == 900
    # Very slow sources are never polled more often than configured, even when backing off.
    assert next_delay(7200, 5, rng=rng) >= 3600
