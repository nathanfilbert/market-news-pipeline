"""Backfill: items published long before we first see them are old news."""

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest
from alembic import command
from sqlalchemy import func, select, update

from mnp.classify.assets import load_assets, sync_assets
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.config import PROJECT_ROOT, get_settings
from mnp.jobs import NORMALIZE, run_pending
from mnp.models import Article, Job
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from mnp.outputs.queries import ArticleFilter, search_articles
from tests.conftest import alembic_config, source

pytestmark = pytest.mark.db


def feed(*items: tuple[str, str, timedelta]) -> bytes:
    """An RSS feed of (slug, headline, age) items, dated relative to now."""
    now = datetime.now(UTC)
    body = "".join(
        f"<item><title>{title}</title><link>https://{slug}.example.com/a</link>"
        f"<guid>{slug}</guid><pubDate>{format_datetime(now - age)}</pubDate></item>"
        for slug, title, age in items
    )
    return f'<rss version="2.0"><channel><title>t</title>{body}</channel></rss>'.encode()


@pytest.fixture
def backfill_days(monkeypatch):
    monkeypatch.setenv("BACKFILL_AFTER_DAYS", "7")
    get_settings.cache_clear()


@pytest.fixture
async def stored(engine, server, client, backfill_days):
    await sync_assets(engine, load_assets(PROJECT_ROOT / "config" / "assets.yaml"))
    ids = await sync_sources(engine, [source("a"), source("b")])
    server.serve(
        "a",
        content=feed(
            ("old", "Exchange Z hacked for $50 million in hot wallet breach", timedelta(days=30)),
            ("fresh", "Fed holds rates steady as inflation cools", timedelta(hours=2)),
        ),
    )
    # Source b reports a *new* hack with a near-identical headline to the old one.
    server.serve(
        "b",
        content=feed(
            ("new", "Exchange Z hacked for $50 million in hot wallet breach", timedelta(hours=1))
        ),
    )
    for name in ("a", "b"):
        await collect_once(engine, ids[name], RssCollector(source(name), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)


async def article(engine, slug) -> Article:
    async with engine.connect() as conn:
        return (
            await conn.execute(select(Article).where(Article.canonical_url.contains(f"//{slug}.")))
        ).one()


async def test_old_items_are_flagged_and_not_classified(engine, stored):
    old, fresh = await article(engine, "old"), await article(engine, "fresh")
    assert old.is_backfill and not fresh.is_backfill
    async with engine.connect() as conn:
        classify_jobs = (
            await conn.execute(select(func.count()).where(Job.kind == "classify"))
        ).scalar()
    assert classify_jobs == 2  # fresh + new, not old


async def test_backfill_never_joins_or_attracts_current_clusters(engine, stored):
    old, new = await article(engine, "old"), await article(engine, "new")
    assert old.cluster_id != new.cluster_id


async def test_backfill_is_hidden_unless_requested(engine, stored):
    async with engine.connect() as conn:
        default = await search_articles(conn, ArticleFilter())
        everything = await search_articles(conn, ArticleFilter(include_backfill=True))
    assert {r.canonical_url for r in default} == {
        "https://fresh.example.com/a",
        "https://new.example.com/a",
    }
    assert len(everything) == 3
    assert [r.is_backfill for r in everything if "old" in r.canonical_url] == [True]


async def test_api_include_backfill_and_detail(engine, stored):
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as api:
        assert (await api.get("/articles")).json()["count"] == 2
        body = (await api.get("/articles", params={"include_backfill": "true"})).json()
        assert body["count"] == 3
        old = next(a for a in body["articles"] if a["is_backfill"])
        detail = (await api.get(f"/articles/{old['id']}")).json()
        assert detail["is_backfill"] is True
        cluster = (await api.get(detail["cluster_url"])).json()
        assert [a["id"] for a in cluster["articles"]] == [old["id"]]


async def test_items_without_a_date_are_not_backfill(engine, server, client, backfill_days):
    ids = await sync_sources(engine, [source("a")])
    server.serve(
        "a",
        content=b'<rss version="2.0"><channel><title>t</title><item><title>Undated</title>'
        b"<link>https://undated.example.com/a</link><guid>u</guid></item></channel></rss>",
    )
    await collect_once(engine, ids["a"], RssCollector(source("a"), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    assert not (await article(engine, "undated")).is_backfill


async def test_migration_flags_existing_old_articles(
    engine, database_url, server, client, monkeypatch
):
    # Store an old item with the rule effectively off, as before this migration existed.
    monkeypatch.setenv("BACKFILL_AFTER_DAYS", "100000")
    get_settings.cache_clear()
    ids = await sync_sources(engine, [source("a")])
    server.serve(
        "a",
        content=feed(
            ("old", "Old story", timedelta(days=30)), ("fresh", "New story", timedelta(hours=1))
        ),
    )
    await collect_once(engine, ids["a"], RssCollector(source("a"), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    async with engine.begin() as conn:
        await conn.execute(update(Article).values(is_backfill=False))

    cfg = alembic_config(database_url)
    command.downgrade(cfg, "0004")
    command.upgrade(cfg, "head")

    assert (await article(engine, "old")).is_backfill
    assert not (await article(engine, "fresh")).is_backfill
