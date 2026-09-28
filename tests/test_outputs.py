"""M5: the shared queries, the read-only API and `mnp news`."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import text, update

from mnp.classify.assets import load_assets, sync_assets
from mnp.classify.fake import FakeClassifier
from mnp.classify.service import ClassifyHandler
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.config import PROJECT_ROOT
from mnp.jobs import CLASSIFY, NORMALIZE, run_pending
from mnp.models import Article, SourceState
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from mnp.outputs.queries import ArticleFilter, parse_time, search_articles, source_status
from tests.conftest import source

NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("90m", NOW - timedelta(minutes=90)),
        ("1h", NOW - timedelta(hours=1)),
        ("2d", NOW - timedelta(days=2)),
        ("1w", NOW - timedelta(weeks=1)),
        ("2026-09-26", datetime(2026, 9, 26, tzinfo=UTC)),
        ("2026-09-26T08:30:00+02:00", datetime(2026, 9, 26, 6, 30, tzinfo=UTC)),
    ],
)
def test_parse_time(value, expected):
    assert parse_time(value, now=NOW) == expected


def test_parse_time_rejects_garbage():
    with pytest.raises(ValueError, match="invalid time"):
        parse_time("yesterday-ish", now=NOW)


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"enabled": False, "last_success_at": None}, "disabled"),
        ({"last_success_at": None}, "never_run"),
        ({"last_success_at": NOW - timedelta(minutes=2)}, "ok"),
        ({"last_success_at": NOW - timedelta(minutes=2), "consecutive_failures": 2}, "failing"),
        # 60s poll: stale after max(5 polls, 10 min) = 10 min
        ({"last_success_at": NOW - timedelta(minutes=11)}, "stale"),
        ({"last_success_at": NOW - timedelta(minutes=9)}, "ok"),
        # 300s poll: stale after 25 min
        ({"last_success_at": NOW - timedelta(minutes=20), "poll_seconds": 300}, "ok"),
    ],
)
def test_source_status(kwargs, expected):
    args = {"enabled": True, "poll_seconds": 60, "consecutive_failures": 0, "now": NOW}
    assert source_status(**(args | kwargs)) == expected


@pytest.fixture
async def data(engine, server, client):
    """Two sources collected, normalized and classified (FakeClassifier named 'jev')."""
    await sync_assets(engine, load_assets(PROJECT_ROOT / "config" / "assets.yaml"))
    ids = await sync_sources(engine, [source("a"), source("b"), source("idle")])
    server.serve(
        "a",
        content=(PROJECT_ROOT / "tests/fixtures/rss/wordpress.xml")
        .read_bytes()
        .replace(b"Protocol X patches", b"Bitcoin lender Protocol X patches"),
    )
    server.serve("b", fixture="rss/second_source.xml")
    for name in ("a", "b"):
        await collect_once(engine, ids[name], RssCollector(source(name), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    fake = FakeClassifier()
    fake.name = "jev"
    await run_pending(engine, CLASSIFY, ClassifyHandler(fake, PROJECT_ROOT / "config"))
    return ids


async def search(engine, **kwargs):
    async with engine.connect() as conn:
        return await search_articles(conn, ArticleFilter(**kwargs))


@pytest.mark.db
async def test_search_filters(engine, data):
    everything = await search(engine)
    assert len(everything) == 5
    assert all(r.classification for r in everything)

    [hack] = await search(engine, event_types=["hack_exploit"])
    assert hack.headline.startswith("Bitcoin lender Protocol X")
    assert [a.symbol for a in hack.assets] == ["BTC"]

    assert [r.id for r in await search(engine, assets=["btc"])] == [hack.id]
    assert await search(engine, assets=["BTC"], min_asset_relevance=0.99) == []
    assert {r.classification["event_type"] for r in await search(engine, min_impact=0.6)} == {
        "hack_exploit",
        "exchange_listing",
        "monetary_policy",
    }
    assert {r.source for r in await search(engine, sources=["b"])} == {"b"}
    assert len(await search(engine, domain="crypto")) == 3
    assert len(await search(engine, min_relevance=0.5)) == 4
    assert len(await search(engine, limit=2)) == 2
    assert await search(engine, since=datetime.now(UTC) + timedelta(minutes=1)) == []


@pytest.mark.db
async def test_articles_without_classification_are_listed_but_not_filtered(engine, data):
    assert len(await search(engine, question_set="v9.9")) == 5
    assert all(r.classification is None for r in await search(engine, question_set="v9.9"))
    assert await search(engine, question_set="v9.9", min_impact=0) == []


@pytest.mark.db
async def test_search_shows_cluster_size(engine, data):
    [listing] = await search(engine, event_types=["exchange_listing"], sources=["a"])
    assert listing.cluster_size == 2  # same story from source b


@pytest.fixture
async def api(engine, data):
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.db
async def test_api_articles_filtered(api):
    r = await api.get("/articles", params={"event_type": "hack_exploit", "asset": "BTC"})
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    [a] = body["articles"]
    assert a["classification"]["event_type"] == "hack_exploit"
    assert a["classification"]["question_set_version"] == "v1.1"
    assert a["assets"][0]["symbol"] == "BTC"

    both = [("event_type", "hack_exploit"), ("event_type", "opinion_analysis")]
    assert (await api.get("/articles", params=both)).json()["count"] == 2
    assert (await api.get("/articles", params={"min_impact": 0.99, "since": "1h"})).json()[
        "count"
    ] == 2


@pytest.mark.db
@pytest.mark.parametrize(
    "params", [{"since": "whenever"}, {"min_impact": 2}, {"limit": 0}, {"limit": 1000}]
)
async def test_api_rejects_bad_params(api, params):
    assert (await api.get("/articles", params=params)).status_code == 422


@pytest.mark.db
async def test_api_article_detail_cluster_and_raw(api, engine):
    listing = (await api.get("/articles", params={"event_type": "exchange_listing"})).json()
    article_id = listing["articles"][0]["id"]

    detail = (await api.get(f"/articles/{article_id}")).json()
    [version] = detail["versions"]
    [classification] = version["classifications"]
    assert classification["results"]["response"]["answers"]["event_type"]["choice"]
    assert version["raw_url"] == f"/raw/{version['raw_item_id']}"

    raw = (await api.get(version["raw_url"])).json()
    assert raw["payload"]["format"] == "xml_item"
    assert raw["payload_sha256"] == raw["payload_sha256"].lower()

    cluster = (await api.get(detail["cluster_url"])).json()
    assert {a["source"] for a in cluster["articles"]} == {"a", "b"}

    for path in ("/articles/999999", "/clusters/999999", "/raw/999999"):
        assert (await api.get(path)).status_code == 404


@pytest.mark.db
async def test_api_is_read_only(api, engine):
    # Every request runs in a READ ONLY transaction.
    app_engine = api._transport.app.state.engine
    async with app_engine.connect() as conn, conn.begin():
        await conn.execute(text("SET TRANSACTION READ ONLY"))
        with pytest.raises(Exception, match="read-only transaction"):
            await conn.execute(update(Article).values(cluster_id=None))


@pytest.mark.db
async def test_health_shows_stale_and_failing_sources(api, engine, data):
    async with engine.begin() as conn:
        await conn.execute(
            update(SourceState)
            .where(SourceState.source_id == data["b"])
            .values(last_success_at=datetime.now(UTC) - timedelta(hours=2))
        )
        await conn.execute(
            update(SourceState)
            .where(SourceState.source_id == data["a"])
            .values(consecutive_failures=3, last_error="CollectorError: HTTP 503")
        )
    body = (await api.get("/health")).json()
    statuses = {s["name"]: s["status"] for s in body["sources"]}
    assert statuses == {"a": "failing", "b": "stale", "idle": "never_run"}
    assert body["status"] == "degraded"
    a = next(s for s in body["sources"] if s["name"] == "a")
    assert a["last_error"] == "CollectorError: HTTP 503"
    assert a["last_new_item_at"] is not None
    assert body["jobs"]["classify"] == {
        "pending": 0,
        "failed": 0,
        "oldest_pending_age_seconds": None,
    }


@pytest.mark.db
async def test_health_ok_when_all_sources_fresh(api, engine, data):
    async with engine.begin() as conn:
        await conn.execute(
            update(SourceState).values(last_success_at=datetime.now(UTC), consecutive_failures=0)
        )
    assert (await api.get("/health")).json()["status"] == "ok"
