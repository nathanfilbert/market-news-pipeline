"""v1.3: the trading feed (revisions log, snapshots, /v1/feed)."""

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from mnp.classify.assets import load_assets, sync_assets
from mnp.classify.fake import FakeClassifier
from mnp.classify.service import ClassifyHandler
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.config import PROJECT_ROOT, get_settings
from mnp.feed.build import handle_feed_job, rebuild_all, update_event
from mnp.jobs import CLASSIFY, FEED, NORMALIZE, run_pending
from mnp.models import Article, FeedRevision
from mnp.normalize.recluster import recluster
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from tests.conftest import source

pytestmark = pytest.mark.db


class Pipeline:
    def __init__(self, engine, server, client):
        self.engine, self.server, self.client = engine, server, client
        self.ids: dict[str, int] = {}

    async def collect(self, name: str, fixture: str) -> None:
        if name not in self.ids:
            self.ids.update(await sync_sources(self.engine, [source(n) for n in {*self.ids, name}]))
        self.server.serve(name, fixture=fixture)
        await collect_once(self.engine, self.ids[name], RssCollector(source(name), self.client))
        await run_pending(self.engine, NORMALIZE, handle_normalize_job)

    async def classify(self) -> None:
        handler = ClassifyHandler(FakeClassifier(), PROJECT_ROOT / "config")
        await run_pending(self.engine, CLASSIFY, handler)

    async def feed(self) -> int:
        return (await run_pending(self.engine, FEED, handle_feed_job)).done

    async def revisions(self) -> list:
        async with self.engine.connect() as conn:
            return (await conn.execute(select(FeedRevision).order_by(FeedRevision.id))).all()

    async def count(self) -> int:
        async with self.engine.connect() as conn:
            return (await conn.execute(select(func.count()).select_from(FeedRevision))).scalar()


@pytest.fixture
async def pipeline(engine, server, client):
    await sync_assets(engine, load_assets(PROJECT_ROOT / "config" / "assets.yaml"))
    return Pipeline(engine, server, client)


@pytest.fixture
async def api(engine):
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def by_headline(revisions, part: str) -> list:
    return [r for r in revisions if part in (r.payload.get("headline") or "")]


async def test_new_articles_become_events(pipeline):
    await pipeline.collect("a", "rss/wordpress.xml")
    assert await pipeline.feed() == 3
    revs = await pipeline.revisions()
    assert len(revs) == 3
    assert {(r.revision, r.status) for r in revs} == {(1, "active")}
    [exploit] = by_headline(revs, "Protocol X")
    p = exploit.payload
    assert p["schema_version"] == "1"
    assert p["event_id"] == exploit.event_id
    assert p["classification"] is None and p["classified_article_count"] == 0
    assert p["sources"] == ["a"] and p["article_count"] == 1
    assert p["first_received_at"].endswith("Z")


async def test_classification_join_and_edit_each_add_a_revision(pipeline):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.feed()
    await pipeline.classify()
    assert await pipeline.feed() == 3
    [classified] = [r for r in by_headline(await pipeline.revisions(), "Protocol X")][-1:]
    c = classified.payload["classification"]
    assert classified.revision == 2
    assert c["event_type"] == "hack_exploit" and c["question_set"] == "v1.1"
    assert 0 <= c["impact"] <= 1 and -1 <= c["sentiment"] <= 1

    # The same story from a second outlet joins the existing event.
    await pipeline.collect("b", "rss/second_source.xml")
    await pipeline.feed()
    token = by_headline(await pipeline.revisions(), "TOKEN")
    latest = token[-1]
    assert latest.payload["article_count"] == 2
    assert latest.payload["sources"] == ["a", "b"]
    assert {r.event_id for r in token} == {latest.event_id}

    # An edited headline is a change to the event, too.
    await pipeline.collect("a", "rss/wordpress_edited.xml")
    await pipeline.feed()
    exploit = by_headline(await pipeline.revisions(), "Protocol X")
    assert exploit[-1].payload["headline"].startswith("Protocol X patches bug after $14M")
    assert [r.revision for r in exploit] == list(range(1, len(exploit) + 1))


async def test_unchanged_events_get_no_new_revision(pipeline):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.feed()
    before = await pipeline.count()
    async with pipeline.engine.begin() as conn:
        for r in await pipeline.revisions():
            assert await update_event(conn, r.event_id) is None
    assert await rebuild_all(pipeline.engine) == 3
    await pipeline.feed()
    assert await pipeline.count() == before


async def test_recluster_retracts_replaced_events(pipeline):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.collect("b", "rss/second_source.xml")
    await pipeline.feed()
    old_events = {r.event_id for r in await pipeline.revisions()}

    await recluster(pipeline.engine, get_settings())
    await pipeline.feed()
    revs = await pipeline.revisions()
    async with pipeline.engine.connect() as conn:
        current = set((await conn.execute(select(Article.cluster_id))).scalars())

    latest = {r.event_id: r for r in revs}  # ordered by id: last one wins
    for event_id in old_events - current:
        r = latest[event_id]
        assert r.status == "retracted"
        assert r.payload["superseded_by"] and set(r.payload["superseded_by"]) <= current
    assert all(latest[e].status == "active" for e in current)


async def test_revisions_are_append_only(pipeline):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.feed()
    for statement in (
        "UPDATE feed_revisions SET status = 'retracted'",
        "DELETE FROM feed_revisions",
    ):
        with pytest.raises(DBAPIError, match="append-only"):
            async with pipeline.engine.begin() as conn:
                await conn.execute(text(statement))


async def test_cursor_pages_cover_every_revision_once(pipeline, api):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.collect("b", "rss/second_source.xml")
    await pipeline.feed()
    await pipeline.classify()
    await pipeline.feed()
    everything = [str(r.id) for r in await pipeline.revisions()]

    seen, cursor = [], "0"
    while True:
        page = (await api.get("/v1/feed/events", params={"after": cursor, "limit": 2})).json()
        seen += [r["cursor"] for r in page["revisions"]]
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    assert seen == everything

    # Polling again at the end returns nothing and keeps the cursor.
    page = (await api.get("/v1/feed/events", params={"after": cursor})).json()
    assert page == {"revisions": [], "next_cursor": cursor, "has_more": False}

    # New changes show up after the saved cursor.
    await pipeline.collect("a", "rss/wordpress_edited.xml")
    await pipeline.feed()
    page = (await api.get("/v1/feed/events", params={"after": cursor})).json()
    assert [r["revision"] for r in page["revisions"]] == [3]
    assert page["revisions"][0]["headline"].startswith("Protocol X patches bug after $14M")


async def test_revision_shape(pipeline, api):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.feed()
    await pipeline.classify()
    await pipeline.feed()
    r = (await api.get("/v1/feed/events", params={"limit": 1000})).json()["revisions"][-1]
    assert r["status"] == "active" and r["schema_version"] == "1"
    assert set(r["classification"]) >= {"event_type", "impact", "sentiment", "urgency"}
    assert r["latency"]["received_to_classified"] >= 0
    assert r["latency"]["received_to_available"] >= 0
    assert r["articles"][0]["classified"] is True
    assert r["superseded_by"] == []


async def test_snapshot_as_of_never_shows_later_information(pipeline, api):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.feed()
    first = by_headline(await pipeline.revisions(), "Protocol X")[0]
    await pipeline.classify()
    await pipeline.feed()

    params = {"as_of": first.available_at.isoformat(), "since": "30d"}
    then = (await api.get("/v1/feed/snapshot", params=params)).json()
    [event] = [e for e in then["events"] if e["event_id"] == first.event_id]
    assert event["revision"] == 1 and event["classification"] is None
    assert all(e["available_at"] <= then["as_of"] for e in then["events"])

    now = (await api.get("/v1/feed/snapshot", params={"since": "30d"})).json()
    [event] = [e for e in now["events"] if e["event_id"] == first.event_id]
    assert event["classification"]["event_type"] == "hack_exploit"
    assert now["count"] == 3

    # Same for a single event.
    old = await api.get(f"/v1/feed/events/{first.event_id}", params={"as_of": params["as_of"]})
    assert old.json()["revision"] == 1
    history = (await api.get(f"/v1/feed/events/{first.event_id}/revisions")).json()
    assert [h["revision"] for h in history] == [1, 2]


async def test_snapshot_filters(pipeline, api):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.feed()
    await pipeline.classify()
    await pipeline.feed()

    async def headlines(**params) -> list[str]:
        body = (await api.get("/v1/feed/snapshot", params={"since": "30d", **params})).json()
        return sorted(e["headline"] for e in body["events"])

    [exploit] = await headlines(event_type="hack_exploit")
    assert exploit.startswith("Protocol X")
    assert await headlines(min_impact=0.0) == await headlines()
    assert all("Protocol X" in h for h in await headlines(min_impact=0.9))
    assert await headlines(asset="NOPE") == []
    # An event only matches an asset Jev confirmed for it.
    snapshot = (await api.get("/v1/feed/snapshot", params={"since": "30d"})).json()
    for e in snapshot["events"]:
        for a in e["assets"]:
            assert e["headline"] in await headlines(asset=a["symbol"].lower())


async def test_retracted_events_leave_the_snapshot(pipeline, api):
    await pipeline.collect("a", "rss/wordpress.xml")
    await pipeline.collect("b", "rss/second_source.xml")
    await pipeline.feed()
    await recluster(pipeline.engine, get_settings())
    await pipeline.feed()
    snapshot = (await api.get("/v1/feed/snapshot", params={"since": "30d"})).json()
    assert {e["status"] for e in snapshot["events"]} == {"active"}
    async with pipeline.engine.connect() as conn:
        current = set((await conn.execute(select(Article.cluster_id))).scalars())
    assert {e["event_id"] for e in snapshot["events"]} == current


@pytest.mark.parametrize(
    ("path", "params", "status"),
    [
        ("/v1/feed/events", {"after": "abc"}, 422),
        ("/v1/feed/events", {"limit": 0}, 422),
        ("/v1/feed/events", {"limit": 1001}, 422),
        ("/v1/feed/snapshot", {"as_of": "whenever"}, 422),
        ("/v1/feed/snapshot", {"min_impact": 2}, 422),
        ("/v1/feed/events/999", {}, 404),
        ("/v1/feed/events/999/revisions", {}, 404),
    ],
)
async def test_bad_requests(api, path, params, status):
    assert (await api.get(path, params=params)).status_code == status


async def test_openapi_documents_the_feed(api):
    spec = (await api.get("/openapi.json")).json()
    assert {"/v1/feed/events", "/v1/feed/snapshot"} <= set(spec["paths"])
    assert "EventRevision" in spec["components"]["schemas"]
