"""M3 acceptance: aggregator items overlapping RSS dedupe to the same article or cluster."""

import pytest
from sqlalchemy import func, select

from mnp.collectors.finnhub import FinnhubCollector
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.jobs import NORMALIZE, run_pending
from mnp.models import Article, ArticleVersion, Cluster
from mnp.normalize.service import handle_normalize_job
from tests.conftest import source

pytestmark = pytest.mark.db

RSS = source("rss")
FH = source("fh", kind="finnhub", options={"category": "crypto"})


@pytest.fixture
async def collected(engine, server, client):
    ids = await sync_sources(engine, [RSS, FH])
    server.serve("rss", fixture="rss/wordpress.xml")
    server.serve("fh", fixture="finnhub/news_crypto.json")
    await collect_once(engine, ids["rss"], RssCollector(RSS, client))
    await collect_once(engine, ids["fh"], FinnhubCollector(FH, client, api_key="k"))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    return ids


async def article(engine, url_part):
    async with engine.connect() as conn:
        return (
            await conn.execute(select(Article).where(Article.canonical_url.contains(url_part)))
        ).one()


async def version_count(engine, article_id) -> int:
    async with engine.connect() as conn:
        return (
            await conn.execute(select(func.count()).where(ArticleVersion.article_id == article_id))
        ).scalar()


async def test_same_url_and_content_is_one_article_one_version(engine, collected):
    # Finnhub links the RSS article with a tracking param; same normalized content.
    a = await article(engine, "protocol-x-exploit")
    assert "utm_" not in a.canonical_url
    assert await version_count(engine, a.id) == 1


async def test_same_url_with_different_content_adds_a_version(engine, collected):
    # Same URL, but Finnhub has no body where RSS had content:encoded: plan §5 makes this a
    # new version of the same article (the version's raw_item records which source it was).
    a = await article(engine, "example-exchange-lists-token")
    assert await version_count(engine, a.id) == 2


async def test_same_story_at_a_different_url_joins_the_rss_cluster(engine, collected):
    rss = await article(engine, "news.example.com/markets/2026/09/27/example-exchange")
    wire = await article(engine, "wire.example.net/token-perps")
    assert rss.id != wire.id
    assert rss.cluster_id == wire.cluster_id


async def test_unrelated_story_gets_its_own_cluster(engine, collected):
    stable = await article(engine, "stablecoin-supply-record")
    async with engine.connect() as conn:
        members = (
            await conn.execute(select(func.count()).where(Article.cluster_id == stable.cluster_id))
        ).scalar()
        # 3 RSS + 2 new Finnhub URLs; the other 2 Finnhub items reused RSS articles
        totals = (
            await conn.execute(
                select(
                    select(func.count()).select_from(Article).scalar_subquery(),
                    select(func.count()).select_from(Cluster).scalar_subquery(),
                )
            )
        ).one()
    assert members == 1
    assert tuple(totals) == (5, 4)
