import asyncio
import json
from datetime import UTC, datetime
from itertools import pairwise

import httpx
import pytest
from sqlalchemy import select

from mnp.collectors import gdelt
from mnp.collectors.base import CollectorError, CollectorUnavailable, make_http_client
from mnp.collectors.gdelt import GdeltCollector, RequestPacer
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, make_collector, run_source_loop, sync_sources
from mnp.config import Settings
from mnp.feed.build import handle_feed_job
from mnp.jobs import FEED, NORMALIZE, run_pending
from mnp.models import Article, FeedRevision
from mnp.normalize.item import parse_payload
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from tests.conftest import fixture_bytes, source

NOW = datetime(2026, 9, 27, 10, 30, tzinfo=UTC)
GD = source(
    "gd",
    kind="gdelt",
    url="https://api.gdelt.example.com/api/v2/doc/doc",
    options={"query": "theme:SANCTIONS sourcelang:english"},
)


class FakeClock:
    """Monotonic clock whose sleeps advance time instantly, recording each wait."""

    def __init__(self) -> None:
        self.t = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


def pacer(clock: FakeClock, interval: float = 10.0) -> RequestPacer:
    return RequestPacer(interval, clock=clock, sleep=clock.sleep)


def collector(handler, *, src=GD, clock=None, now=NOW) -> GdeltCollector:
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(handler))
    return GdeltCollector(src, client, pacer=pacer(clock or FakeClock()), now=lambda: now)


def fixture_articles() -> list[dict]:
    return json.loads(fixture_bytes("gdelt/artlist.json"))["articles"]


def articles_response(articles) -> httpx.Response:
    return httpx.Response(200, json={"articles": articles})


def article(i: int, seendate: str) -> dict:
    return {"url": f"https://n.example.com/{i}", "title": f"Story {i}", "seendate": seendate}


async def test_first_fetch_looks_back_and_sets_checkpoint():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, content=fixture_bytes("gdelt/artlist.json"))

    payloads, checkpoint = await collector(handler).fetch({})

    [request] = seen
    params = request.url.params
    assert params["query"] == "theme:SANCTIONS sourcelang:english"
    assert params["mode"] == "ArtList"
    assert params["format"] == "json"
    assert params["sort"] == "DateAsc"
    assert params["maxrecords"] == "250"
    assert params["startdatetime"] == "20260927093000"  # default lookback: 60 minutes
    assert params["enddatetime"] == "20260927103000"
    assert checkpoint == {"seen_through": "20260927T101500Z"}
    assert [p.url for p in payloads] == [a["url"] for a in fixture_articles()]
    first = payloads[0]
    assert first.external_id == first.url
    assert first.published_at == datetime(2026, 9, 27, 9, 30, tzinfo=UTC)
    assert first.payload["format"] == "gdelt_doc"
    assert first.payload["query"] == "theme:SANCTIONS sourcelang:english"
    assert first.payload["item"]["domain"] == "world.example.net"


async def test_checkpoint_rereads_overlap_and_never_moves_back():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={})  # GDELT's "no matches"

    payloads, checkpoint = await collector(handler).fetch({"seen_through": "20260927T101500Z"})

    assert seen[0].url.params["startdatetime"] == "20260927094500"  # 30 minutes of overlap
    assert payloads == []
    assert checkpoint == {"seen_through": "20260927T101500Z"}


async def test_full_pages_are_followed_oldest_first():
    starts = []
    pages = [
        [article(i, f"20260927T09{30 + i // 10:02d}00Z") for i in range(250)],  # 09:30 .. 09:54
        [article(1000, "20260927T100000Z")],
    ]

    def handler(request):
        starts.append(request.url.params["startdatetime"])
        return articles_response(pages[len(starts) - 1])

    payloads, checkpoint = await collector(handler).fetch({})

    assert starts == ["20260927093000", "20260927095400"]
    assert len(payloads) == 251
    assert checkpoint == {"seen_through": "20260927T100000Z"}


async def test_page_limit_resumes_from_where_it_stopped():
    starts = []
    src = source("gd", kind="gdelt", options={"query": "q", "max_pages": 2})

    def handler(request):
        starts.append(request.url.params["startdatetime"])
        minute = 30 + len(starts) * 10
        return articles_response([article(i, f"20260927T09{minute:02d}00Z") for i in range(250)])

    _, checkpoint = await collector(handler, src=src).fetch({})

    assert len(starts) == 2
    assert starts == ["20260927093000", "20260927094000"]
    assert checkpoint == {"seen_through": "20260927T095000Z"}


async def test_full_page_within_one_second_stops():
    calls = []

    def handler(request):
        calls.append(request)
        return articles_response([article(i, "20260927T093000Z") for i in range(250)])

    payloads, _ = await collector(handler).fetch({})
    assert len(calls) == 1
    assert len(payloads) == 250


async def test_fetch_history_pages_from_since():
    seen = []

    def handler(request):
        seen.append(request)
        return articles_response(fixture_articles())

    since = datetime(2026, 9, 20, tzinfo=UTC)
    payloads = await collector(handler).fetch_history(since)

    assert seen[0].url.params["startdatetime"] == "20260920000000"
    assert len(payloads) == 3


async def test_items_without_url_are_skipped_and_hash_ignores_key_order():
    item = fixture_articles()[0]
    reordered = dict(reversed(list(item.items())))
    [a], _ = await collector(lambda r: articles_response([item, {"title": "no url"}])).fetch({})
    [b], _ = await collector(lambda r: articles_response([reordered])).fetch({})
    assert a.payload_sha256 == b.payload_sha256


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (httpx.Response(500, text="oops"), "HTTP 500"),
        (httpx.Response(200, text="Your search contained a keyword that is too short"), "GDELT"),
        (httpx.Response(200, json=[1, 2]), "unexpected GDELT response"),
        (httpx.Response(200, json={"articles": "x"}), "unexpected GDELT articles"),
    ],
)
async def test_errors(response, message):
    with pytest.raises(CollectorError, match=message):
        await collector(lambda r: response).fetch({})


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(429, text="Too Many Requests"),
        httpx.Response(
            200,
            text="Please limit requests to one every 5 seconds or contact us for larger queries.",
        ),
    ],
)
async def test_throttling_backs_off_every_gdelt_source(response):
    clock = FakeClock()
    shared = pacer(clock)
    client = make_http_client(
        Settings(_env_file=None), transport=httpx.MockTransport(lambda r: response)
    )
    a = GdeltCollector(GD, client, pacer=shared, now=lambda: NOW)
    with pytest.raises(CollectorError) as exc_info:
        await a.fetch({})
    assert exc_info.value.retry_after == gdelt.THROTTLED_PAUSE_SECONDS

    # Another GDELT source's next request waits out the pause.
    await shared.wait()
    assert clock.sleeps == [gdelt.THROTTLED_PAUSE_SECONDS]


async def test_pacer_spaces_requests_across_sources():
    clock = FakeClock()
    shared = pacer(clock, interval=10.0)
    sent = []

    def handler(request):
        sent.append(clock.t)
        return httpx.Response(200, json={})

    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(handler))
    collectors = [
        GdeltCollector(source(name, kind="gdelt", options={"query": name}), client, pacer=shared)
        for name in ("a", "b", "c")
    ]
    for c in collectors:
        await c.fetch({})
    await collectors[0].fetch({})

    assert sent == [1000.0, 1010.0, 1020.0, 1030.0]


def test_default_pacer_is_within_gdelt_limit():
    assert gdelt.PACER.interval >= 5


async def test_make_collector_and_query_required():
    async with httpx.AsyncClient() as client:
        settings = Settings(_env_file=None)
        assert isinstance(make_collector(GD, client, settings), GdeltCollector)
        with pytest.raises(CollectorUnavailable, match=r"options\.query"):
            make_collector(source("gd", kind="gdelt"), client, settings)


def test_parse_gdelt_payload():
    english, spanish, _ = fixture_articles()
    p = parse_payload({"format": "gdelt_doc", "query": "q", "item": english})
    assert p.headline == "EU adds tankers to sanctions list over oil price cap evasion"
    assert p.url == "https://world.example.net/2026/09/27/sanctions-shipping"
    assert p.summary is None and p.body is None
    assert p.language == "en"
    assert p.published_at == datetime(2026, 9, 27, 9, 30, tzinfo=UTC)
    assert parse_payload({"format": "gdelt_doc", "item": spanish}).language == "es"
    other = parse_payload({"format": "gdelt_doc", "item": {"language": "Tagalog"}})
    assert other.language == "Tagalog"
    assert other.published_at is None


async def collect_rss_and_gdelt(engine, server, client) -> None:
    """Source b (RSS) and a GDELT query both report the central bank minutes."""
    gd = source(
        "gdelt",
        kind="gdelt",
        url="https://gdelt.example.com/api/v2/doc/doc",
        options={"query": "q"},
    )
    ids = await sync_sources(engine, [source("b"), gd])
    server.serve("b", fixture="rss/second_source.xml")
    server.serve("gdelt", fixture="gdelt/artlist.json")
    await collect_once(engine, ids["b"], RssCollector(source("b"), client))
    result = await collect_once(
        engine, ids["gdelt"], GdeltCollector(gd, client, pacer=pacer(FakeClock()), now=lambda: NOW)
    )
    assert result.ok and result.inserted == 3
    await run_pending(engine, NORMALIZE, handle_normalize_job)


@pytest.mark.db
async def test_gdelt_article_joins_existing_coverage(engine, server, client):
    """A GDELT report of an event other sources already cover lands in the same cluster."""
    await collect_rss_and_gdelt(engine, server, client)

    async with engine.connect() as conn:
        rows = dict((await conn.execute(select(Article.canonical_url, Article.cluster_id))).all())
    minutes = [c for url, c in rows.items() if "minutes" in url]
    assert len(minutes) == 2 and minutes[0] == minutes[1]
    assert len(set(rows.values())) == len(rows) - 1  # nothing else merged


GDELT_CITATION = {"text": "The GDELT Project", "url": "https://www.gdeltproject.org/"}


@pytest.mark.db
async def test_gdelt_articles_carry_attribution(engine, server, client):
    await collect_rss_and_gdelt(engine, server, client)
    await run_pending(engine, FEED, handle_feed_job)
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as api:
        articles = (await api.get("/articles", params={"since": "30d"})).json()["articles"]
        by_source = {}
        for a in articles:
            by_source.setdefault(a["source"], []).append(a)
        assert all(a["attribution"] == GDELT_CITATION for a in by_source["gdelt"])
        assert all(a["attribution"] is None for a in by_source["b"])

        gdelt_id = by_source["gdelt"][0]["id"]
        detail = (await api.get(f"/articles/{gdelt_id}")).json()
        [version] = detail["versions"]
        assert version["attribution"] == GDELT_CITATION
        raw = (await api.get(version["raw_url"])).json()
        assert raw["attribution"] == GDELT_CITATION
        page = (await api.get(f"/ui/articles/{gdelt_id}")).text
        assert 'href="https://www.gdeltproject.org/"' in page

        events = (await api.get("/v1/feed/snapshot", params={"since": "30d"})).json()["events"]
    minutes = next(e for e in events if len(e["articles"]) == 2)
    assert minutes["attributions"] == [GDELT_CITATION]
    cited = {a["source"]: a["attribution"] for a in minutes["articles"]}
    assert cited == {"b": None, "gdelt": GDELT_CITATION}
    rss_only = next(e for e in events if e["sources"] == ["b"])
    assert rss_only["attributions"] == []

    # Events without a citation store no attribution keys, so their revisions are unchanged.
    async with engine.connect() as conn:
        payloads = (await conn.execute(select(FeedRevision.payload))).scalars().all()
    plain = [p for p in payloads if p["sources"] == ["b"]]
    assert plain and all("attributions" not in p for p in plain)
    assert all("attribution" not in a for p in plain for a in p["articles"])


@pytest.mark.db
async def test_continuous_running_stays_within_rate_limit(engine, monkeypatch):
    """Four GDELT sources polling back to back never send two requests within 5 seconds."""
    clock = FakeClock()
    shared = pacer(clock, interval=gdelt.MIN_INTERVAL_SECONDS)
    sent = []

    def handler(request):
        sent.append(clock.t)
        return httpx.Response(200, json={})

    configs = [
        source(f"g{i}", kind="gdelt", poll_seconds=1, options={"query": f"q{i}"}) for i in range(4)
    ]
    ids = await sync_sources(engine, configs)
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(handler))
    stop = asyncio.Event()

    async def stop_after_requests():
        while len(sent) < 12:
            await asyncio.sleep(0.01)
        stop.set()

    monkeypatch.setattr("mnp.collectors.service.next_delay", lambda *a, **k: 0.0)
    await asyncio.wait_for(
        asyncio.gather(
            stop_after_requests(),
            *(
                run_source_loop(engine, ids[c.name], GdeltCollector(c, client, pacer=shared), stop)
                for c in configs
            ),
        ),
        timeout=30,
    )
    gaps = [b - a for a, b in pairwise(sent)]
    assert gaps and min(gaps) >= 5
