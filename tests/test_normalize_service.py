from datetime import timedelta

import pytest
from sqlalchemy import func, select, text, update

from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.jobs import NORMALIZE, run_pending
from mnp.models import Article, ArticleVersion, Cluster, Job, RawItem
from mnp.normalize.service import handle_normalize_job, normalize_raw_item
from tests.conftest import fixture_bytes, source

pytestmark = pytest.mark.db


@pytest.fixture
async def pipeline(engine, server, client):
    """collect(name) fetches a source through the mock server; normalize() drains jobs."""
    ids: dict[str, int] = {}

    async def collect(name, fixture=None, content=None):
        if name not in ids:
            ids.update(await sync_sources(engine, [source(n) for n in {*ids, name}]))
        if fixture or content:
            server.serve(name, fixture=fixture, content=content or b"")
        return await collect_once(engine, ids[name], RssCollector(source(name), client))

    async def normalize():
        return await run_pending(engine, NORMALIZE, handle_normalize_job)

    return collect, normalize


async def counts(engine) -> tuple[int, int, int]:
    async with engine.connect() as conn:
        return tuple(
            [
                (await conn.execute(select(func.count()).select_from(t))).scalar()
                for t in (Article, ArticleVersion, Cluster)
            ]
        )


async def versions_of(engine, url_part):
    """Version rows (all article_versions columns plus the raw item's fetched_at)."""
    async with engine.connect() as conn:
        return (
            await conn.execute(
                select(ArticleVersion, RawItem.fetched_at)
                .join(Article, Article.id == ArticleVersion.article_id)
                .join(RawItem, RawItem.id == ArticleVersion.raw_item_id)
                .where(Article.canonical_url.contains(url_part))
                .order_by(ArticleVersion.version_no)
            )
        ).all()


async def cluster_of(engine, url_part) -> int:
    async with engine.connect() as conn:
        return (
            await conn.execute(
                select(Article.cluster_id).where(Article.canonical_url.contains(url_part))
            )
        ).scalar_one()


async def test_collect_enqueues_and_normalize_builds_articles(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    stats = await normalize()

    assert stats.done == 3
    assert await counts(engine) == (3, 3, 3)
    [v] = await versions_of(engine, "example-exchange-lists-token")
    assert v.version_no == 1
    assert v.headline.startswith("Example Exchange lists")
    assert v.summary.endswith("spot pairs.")
    assert v.language == "en"
    # the three timestamps stay separate
    assert v.received_at == v.fetched_at
    assert v.published_at != v.received_at


async def test_rerunning_normalize_is_a_no_op(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    await normalize()
    before = await counts(engine)

    # Queue is drained, and forcing every raw item through again writes nothing.
    assert (await normalize()).processed == 0
    async with engine.begin() as conn:
        raw_ids = (await conn.execute(select(RawItem.id))).scalars().all()
        outcomes = [await normalize_raw_item(conn, i) for i in raw_ids]
    assert all(o.version_no is None and not o.new_article for o in outcomes)
    assert await counts(engine) == before


async def test_edited_headline_creates_version_2(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    await normalize()
    await collect("a", "rss/wordpress_edited.xml")
    await normalize()

    v1, v2 = await versions_of(engine, "protocol-x-exploit")
    assert (v1.version_no, v2.version_no) == (1, 2)
    assert v1.headline == "Protocol X patches bug after $12M exploit"
    assert v2.headline == "Protocol X patches bug after $14M exploit, team says"
    assert v1.article_id == v2.article_id
    assert v2.received_at == v2.fetched_at > v1.received_at
    assert await counts(engine) == (3, 4, 3)  # no new article or cluster for an edit


async def test_markup_and_whitespace_changes_do_not_create_a_version(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    await normalize()
    noisy = fixture_bytes("rss/wordpress.xml").replace(
        b"Protocol X patches bug after $12M exploit",
        b"Protocol  X <b>patches</b> bug after $12M\n exploit",
    )
    result = await collect("a", content=noisy)
    await normalize()

    assert result.inserted == 1  # different raw bytes are still stored...
    assert len(await versions_of(engine, "protocol-x-exploit")) == 1  # ...but no new version


async def test_same_story_from_two_sources_lands_in_one_cluster(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    await collect("b", "rss/second_source.xml")
    await normalize()

    token_a = await cluster_of(engine, "news.example.com/markets/2026/09/27/example-exchange")
    token_b = await cluster_of(engine, "second.example.org/2026/09/27/example-exchange")
    minutes = await cluster_of(engine, "second.example.org/2026/09/27/minutes")
    assert token_a == token_b
    assert minutes != token_a
    assert await counts(engine) == (5, 5, 4)


async def test_same_source_template_headlines_stay_separate(engine, pipeline):
    collect, normalize = pipeline
    await collect("bank", "rss/template_headlines.xml")
    await normalize()
    assert await cluster_of(engine, "orders-alpha") != await cluster_of(engine, "orders-beta")


async def test_stories_outside_the_window_are_not_clustered(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    await normalize()
    async with engine.begin() as conn:
        await conn.execute(
            update(Article).values(first_seen_at=Article.first_seen_at - timedelta(days=3))
        )
    await collect("b", "rss/second_source.xml")
    await normalize()

    assert await cluster_of(engine, "news.example.com/markets") != await cluster_of(
        engine, "second.example.org/2026/09/27/example-exchange"
    )


async def test_tracking_params_resolve_to_the_same_article(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    tracked = fixture_bytes("rss/wordpress.xml").replace(
        b"example-exchange-lists-token</link>",
        b"example-exchange-lists-token?utm_source=rss&amp;utm_medium=feed</link>",
    )
    await collect("b", content=tracked)
    await normalize()

    assert await counts(engine) == (3, 3, 3)
    async with engine.connect() as conn:
        url = (await conn.execute(select(Article.canonical_url).where(Article.id == 1))).scalar()
    assert "utm_" not in url


async def test_unparseable_item_fails_permanently_without_blocking_others(engine, pipeline):
    collect, normalize = pipeline
    await collect("a", "rss/wordpress.xml")
    async with engine.begin() as conn:
        # Raw payloads are immutable, so point one job at a raw item that doesn't exist.
        await conn.execute(
            text("UPDATE jobs SET payload = '{\"raw_item_id\": 999}' WHERE dedupe_key = '2'")
        )
    stats = await normalize()

    assert (stats.done, stats.failed) == (2, 1)
    async with engine.connect() as conn:
        failed = (await conn.execute(select(Job).where(Job.status == "failed"))).one()
    assert "raw item 999 not found" in failed.last_error
