"""`mnp catch-up`: history fetches, paging, adopting in-window backfill, coverage."""

import shutil
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from mnp import catchup, cli
from mnp.catchup import adopt_history, catch_up, coverage
from mnp.collectors.base import make_http_client
from mnp.collectors.rss import RssCollector, parse_feed_date, split_feed
from mnp.collectors.service import collect_history, collect_once, sync_sources
from mnp.config import PROJECT_ROOT, Settings, get_settings
from mnp.jobs import NORMALIZE, run_pending
from mnp.models import Article, Classification, Job, SourceState
from mnp.normalize.service import handle_normalize_job
from tests.conftest import fixture_bytes, jev_response, source

NOW = datetime.now(UTC)


def feed(*items: tuple[str, timedelta]) -> bytes:
    """RSS with (slug, age) items dated relative to now."""
    body = "".join(
        f"<item><title>Story {slug}</title><link>https://news.example.com/{slug}</link>"
        f"<guid>{slug}</guid><pubDate>{format_datetime(NOW - age)}</pubDate></item>"
        for slug, age in items
    )
    return f'<rss version="2.0"><channel><title>t</title>{body}</channel></rss>'.encode()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Sun, 27 Sep 2026 12:00:00 +0000", datetime(2026, 9, 27, 12, tzinfo=UTC)),
        ("Fri, 25 Sep 2026 14:05:00 -0400", datetime(2026, 9, 25, 18, 5, tzinfo=UTC)),
        ("2026-09-27T12:00:00Z", datetime(2026, 9, 27, 12, tzinfo=UTC)),
        ("2026-09-11T08:30:00-04:00", datetime(2026, 9, 11, 12, 30, tzinfo=UTC)),
        ("not a date", None),
        (None, None),
    ],
)
def test_parse_feed_date(value, expected):
    assert parse_feed_date(value) == expected


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        ("rss/wordpress.xml", datetime(2026, 9, 27, 12, tzinfo=UTC)),
        ("rss/atom.xml", datetime(2026, 9, 27, 12, tzinfo=UTC)),
        ("rss/rdf.xml", datetime(2026, 9, 11, 12, 30, tzinfo=UTC)),
        ("rss/template_headlines.xml", None),
    ],
)
def test_split_feed_records_publish_dates(fixture, expected):
    assert split_feed(fixture_bytes(fixture))[0].published == expected


def paged_collector(pages: dict[int, bytes], **options) -> tuple[RssCollector, list]:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = int(request.url.params.get("paged", 1))
        return httpx.Response(200, content=pages.get(page, pages[max(pages)]))

    cfg = source("a", options={"page_param": "paged", **options})
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(handler))
    collector = RssCollector(cfg, client)
    collector.page_delay = 0
    return collector, requests


async def test_paging_stops_once_past_the_window():
    pages = {
        1: feed(("a1", timedelta(hours=1)), ("a2", timedelta(days=2))),
        2: feed(("b1", timedelta(days=3)), ("b2", timedelta(days=6))),
        3: feed(("c1", timedelta(days=8)), ("c2", timedelta(days=12))),
        4: feed(("d1", timedelta(days=15))),
    }
    collector, requests = paged_collector(pages)
    payloads = await collector.fetch_history(NOW - timedelta(days=7))
    assert [p.external_id for p in payloads] == ["a1", "a2", "b1", "b2", "c1", "c2"]
    assert len(requests) == 3  # page 3 reached past the window, page 4 never fetched


async def test_paging_stops_when_the_site_ignores_the_parameter():
    collector, requests = paged_collector({1: feed(("a1", timedelta(hours=1)))})
    payloads = await collector.fetch_history(NOW - timedelta(days=30))
    assert [p.external_id for p in payloads] == ["a1"]
    assert len(requests) == 2  # page 2 repeated page 1: stop


async def test_paging_respects_max_pages():
    pages = {i: feed((f"p{i}", timedelta(hours=i))) for i in range(1, 50)}
    collector, requests = paged_collector(pages, max_pages="3")
    assert len(await collector.fetch_history(NOW - timedelta(days=365))) == 3
    assert len(requests) == 3


@pytest.mark.db
async def test_history_ignores_and_keeps_the_checkpoint(engine):
    def conditional(request: httpx.Request) -> httpx.Response:
        """304 to a conditional request for the current version, the feed otherwise."""
        if request.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304)
        return httpx.Response(
            200, headers={"ETag": '"v1"'}, content=fixture_bytes("rss/wordpress.xml")
        )

    ids = await sync_sources(engine, [source("a")])
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(conditional))
    collector = RssCollector(source("a"), client)
    assert (await collect_once(engine, ids["a"], collector)).inserted == 3  # stores ETag "v1"
    assert (await collect_once(engine, ids["a"], collector)).received == 0  # 304 Not Modified

    result = await collect_history(engine, ids["a"], collector, NOW - timedelta(days=7))
    assert (result.received, result.inserted) == (3, 0)  # full fetch; already stored
    async with engine.connect() as conn:
        checkpoint = (await conn.execute(select(SourceState.checkpoint))).scalar()
    assert checkpoint == {"etag": '"v1"'}


@pytest.fixture
def backfill_days(monkeypatch):
    monkeypatch.setenv("BACKFILL_AFTER_DAYS", "7")
    get_settings.cache_clear()


@pytest.mark.db
async def test_adopt_history_unflags_only_inside_the_window(engine, server, client, backfill_days):
    ids = await sync_sources(engine, [source("a")])
    server.serve(
        "a",
        content=feed(
            ("fresh", timedelta(hours=1)),
            ("ten", timedelta(days=10)),
            ("forty", timedelta(days=40)),
        ),
    )
    await collect_once(engine, ids["a"], RssCollector(source("a"), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)

    assert await adopt_history(engine, NOW - timedelta(days=30), "v1.1") == 1
    async with engine.connect() as conn:
        flags = dict((await conn.execute(select(Article.canonical_url, Article.is_backfill))).all())
        classify_keys = list(
            (await conn.execute(select(Job.dedupe_key).where(Job.kind == "classify"))).scalars()
        )
    assert flags == {
        "https://news.example.com/fresh": False,
        "https://news.example.com/ten": False,  # adopted
        "https://news.example.com/forty": True,  # older than the window
    }
    assert len(classify_keys) == 2  # fresh (at normalize) + ten (adopted)
    assert await adopt_history(engine, NOW - timedelta(days=30), "v1.1") == 0  # idempotent


@pytest.mark.db
async def test_coverage_reports_empty_days(engine, server, client):
    ids = await sync_sources(engine, [source("a")])
    server.serve("a", content=feed(("today", timedelta(minutes=5)), ("two", timedelta(days=2))))
    await collect_once(engine, ids["a"], RssCollector(source("a"), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    [cov] = await coverage(engine, NOW - timedelta(days=3), ["a"], now=NOW)
    assert cov.articles == 2
    assert cov.empty_days == [(NOW - timedelta(days=d)).date() for d in (3, 1)]


@pytest.fixture
def catchup_env(tmp_path, monkeypatch, database_url, backfill_days):
    config = tmp_path / "config"
    shutil.copytree(PROJECT_ROOT / "config" / "questions", config / "questions")
    shutil.copy(PROJECT_ROOT / "config" / "assets.yaml", config / "assets.yaml")
    (config / "sources.yaml").write_text(
        """
- {name: paged, kind: rss, url: "https://paged.example.com/rss", category: crypto,
   reputation: 0.9, options: {page_param: paged}}
- {name: flat, kind: rss, url: "https://flat.example.com/rss", category: official, reputation: 1}
- {name: fh, kind: finnhub, url: "https://fh.example.com/news", category: markets, reputation: 0.5}
"""
    )
    for key, value in {
        "CONFIG_DIR": str(config),
        "DATABASE_URL": database_url,
        "JEV_API_KEY": "sk-test",
        "FINNHUB_API_KEY": "",
    }.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    monkeypatch.setattr(RssCollector, "page_delay", 0)

    pages = {
        1: feed(("p1", timedelta(hours=2))),
        2: feed(("p2", timedelta(days=9))),  # paging reaches back past the 7-day backfill rule
        3: feed(("p3", timedelta(days=40))),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.typesafe.ai":
            return jev_response(request)
        if request.url.host == "paged.example.com":
            return httpx.Response(200, content=pages[int(request.url.params.get("paged", 1))])
        return httpx.Response(200, content=feed(("f1", timedelta(days=20))))

    real = make_http_client
    monkeypatch.setattr(
        catchup, "make_http_client", lambda s: real(s, transport=httpx.MockTransport(handler))
    )
    yield
    get_settings.cache_clear()


@pytest.mark.db
async def test_catch_up_end_to_end(catchup_env, engine):
    report = await catch_up(get_settings(), engine, since=NOW - timedelta(days=30))

    assert {r.source: (r.received, r.inserted) for r in report.fetched} == {
        "paged": (3, 3),
        "flat": (1, 1),
    }
    assert report.skipped == {"fh": "FINNHUB_API_KEY not set"}
    assert report.normalize.done == 4
    assert report.adopted == 2  # p2 (9 days) and f1 (20 days); p3 (40 days) stays old news
    assert report.classify.done == 3  # p1 + the two adopted
    async with engine.connect() as conn:
        assert (await conn.execute(select(func.count()).select_from(Classification))).scalar() == 3
    cov = {c.source: c for c in report.coverage}
    assert cov["paged"].articles == 2 and cov["flat"].articles == 1


@pytest.mark.db
def test_catch_up_cli(catchup_env, sync_engine):
    runner = CliRunner()
    result = runner.invoke(cli.app, ["catch-up", "--since", "30d"])
    assert result.exit_code == 0, result.output
    out = result.stdout
    assert "paged                 3 received,    3 new, ok" in out
    assert "fh                 skipped (FINNHUB_API_KEY not set)" in out
    assert "history adopted (old-news flag cleared inside the window): 2" in out
    assert "classify: 3 done, 0 retrying, 0 failed" in out
    assert "coverage (31 days, by publish date):" in out

    again = runner.invoke(cli.app, ["catch-up", "--since", "30d", "--no-classify"])
    assert "paged                 3 received,    0 new, ok" in again.stdout  # nothing new
    assert "classify: skipped (--no-classify)" in again.stdout
    assert runner.invoke(cli.app, ["catch-up", "-s", "nope"]).exit_code == 2
