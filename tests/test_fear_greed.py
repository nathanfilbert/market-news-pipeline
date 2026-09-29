"""Crypto Fear & Greed: collector, sentiment readings, API and dashboard."""

from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import func, select

from mnp.collectors.base import CollectorError, make_http_client
from mnp.collectors.fear_greed import FearGreedCollector
from mnp.collectors.service import collect_once, make_collector, sync_sources
from mnp.config import Settings
from mnp.jobs import NORMALIZE, run_pending
from mnp.models import Article, SentimentReading
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from tests.conftest import source

FG = source("fng", kind="fear_greed", url="https://api.alternative.me/fng/", category="sentiment")
DAY = 86400
T0 = 1790380800  # 2026-09-26 00:00 UTC


def body(*points, error=None):
    data = [
        {"value": str(v), "value_classification": label, "timestamp": str(ts)}
        for ts, v, label in points
    ]
    if data:
        data[0]["time_until_update"] = "63328"  # changes every request
    return {"name": "Fear and Greed Index", "data": data, "metadata": {"error": error}}


HISTORY = body((T0 + 2 * DAY, 74, "Greed"), (T0 + DAY, 70, "Greed"), (T0, 44, "Fear"))


class Api:
    """Mock API: serves `self.body`, records requests."""

    def __init__(self, response=HISTORY) -> None:
        self.body = response
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, json=self.body)


def collector(api) -> FearGreedCollector:
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(api))
    return FearGreedCollector(FG, client)


async def test_first_fetch_loads_full_history():
    api = Api()
    payloads, checkpoint = await collector(api).fetch({})

    assert api.requests[0].url.params["limit"] == "0"
    assert checkpoint == {"last_timestamp": T0 + 2 * DAY}
    assert [p.external_id for p in payloads] == [str(T0 + 2 * DAY), str(T0 + DAY), str(T0)]
    assert payloads[0].payload == {
        "format": "fear_greed",
        "index": "crypto_fear_greed",
        "point": {"value": "74", "value_classification": "Greed", "timestamp": str(T0 + 2 * DAY)},
    }
    assert payloads[2].published_at == datetime(2026, 9, 26, tzinfo=UTC)


async def test_hash_ignores_time_until_update():
    [a, *_], _ = await collector(Api()).fetch({})
    changed = body((T0 + 2 * DAY, 74, "Greed"))
    changed["data"][0]["time_until_update"] = "100"
    [b], _ = await collector(Api(changed)).fetch({})
    assert a.payload_sha256 == b.payload_sha256


async def test_later_fetch_asks_for_recent_days_only_and_keeps_latest_day():
    api = Api()
    payloads, checkpoint = await collector(api).fetch({"last_timestamp": T0 + DAY})

    limit = int(api.requests[0].url.params["limit"])
    assert limit >= 2
    # The day already seen stays in (a revised value is stored; an unchanged one dedupes).
    assert [p.external_id for p in payloads] == [str(T0 + 2 * DAY), str(T0 + DAY)]
    assert checkpoint == {"last_timestamp": T0 + 2 * DAY}


async def test_malformed_points_are_skipped():
    response = body((T0, 50, "Neutral"))
    response["data"] += [{"value": "x", "timestamp": "1"}, {"value": "5"}, "junk"]
    payloads, _ = await collector(Api(response)).fetch({})
    assert [p.external_id for p in payloads] == [str(T0)]


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (httpx.Response(500), "HTTP 500"),
        (httpx.Response(200, content=b"<html>"), "invalid JSON"),
        (httpx.Response(200, json={"data": None}), "unexpected Fear & Greed"),
        (httpx.Response(200, json=body(error="Limit exceeded")), "Limit exceeded"),
    ],
)
async def test_errors(response, message):
    with pytest.raises(CollectorError, match=message):
        await collector(lambda r: response).fetch({})


async def test_history_since():
    payloads = await collector(Api()).fetch_history(datetime(2026, 9, 27, tzinfo=UTC))
    assert [p.external_id for p in payloads] == [str(T0 + 2 * DAY), str(T0 + DAY)]


async def test_make_collector_needs_no_key():
    async with httpx.AsyncClient() as client:
        assert isinstance(make_collector(FG, client, Settings(_env_file=None)), FearGreedCollector)


# --- stored as sentiment readings -----------------------------------------------------------


@pytest.fixture
async def collected(engine):
    """Collects through a mock API and normalizes; returns (poll, api)."""
    [source_id] = (await sync_sources(engine, [FG])).values()
    api = Api()
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(api))

    async def poll():
        result = await collect_once(engine, source_id, FearGreedCollector(FG, client))
        await run_pending(engine, NORMALIZE, handle_normalize_job)
        return result

    yield poll, api
    await client.aclose()


async def readings(engine):
    async with engine.connect() as conn:
        return (
            await conn.execute(
                select(
                    SentimentReading.metric,
                    SentimentReading.asset_id,
                    SentimentReading.observed_at,
                    SentimentReading.value,
                    SentimentReading.label,
                ).order_by(SentimentReading.observed_at)
            )
        ).all()


@pytest.mark.db
async def test_points_become_readings_not_articles(engine, collected):
    poll, _ = collected
    result = await poll()
    assert (result.received, result.inserted) == (3, 3)

    rows = await readings(engine)
    assert [(r.metric, r.asset_id, r.value, r.label) for r in rows] == [
        ("crypto_fear_greed", None, 44.0, "Fear"),
        ("crypto_fear_greed", None, 70.0, "Greed"),
        ("crypto_fear_greed", None, 74.0, "Greed"),
    ]
    assert rows[0].observed_at == datetime(2026, 9, 26, tzinfo=UTC)
    async with engine.connect() as conn:
        assert (await conn.execute(select(func.count()).select_from(Article))).scalar() == 0


@pytest.mark.db
async def test_repeat_poll_is_idempotent_and_revision_replaces(engine, collected):
    poll, api = collected
    await poll()
    assert (await poll()).inserted == 0  # nothing new: no duplicates
    assert len(await readings(engine)) == 3

    api.body = body((T0 + 2 * DAY, 76, "Extreme Greed"))  # the latest day revised
    assert (await poll()).inserted == 1
    rows = await readings(engine)
    assert len(rows) == 3
    assert (rows[-1].value, rows[-1].label) == (76.0, "Extreme Greed")


@pytest.mark.db
async def test_api_and_dashboard_show_readings_with_attribution(engine, collected):
    poll, _ = collected
    await poll()
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/sentiment", params={"limit": 1})
        assert r.status_code == 200
        data = r.json()
        assert data["count"] == 1
        [latest] = data["readings"]
        assert latest["metric"] == "crypto_fear_greed"
        assert latest["asset"] is None
        assert (latest["value"], latest["label"]) == (74.0, "Greed")
        assert data["metrics"]["crypto_fear_greed"]["attribution_url"].startswith(
            "https://alternative.me/"
        )

        r = await c.get("/sentiment", params={"asset": "BTC"})
        assert r.json()["count"] == 0  # no per-coin series yet
        r = await c.get("/sentiment", params={"since": "2026-09-27T00:00:00Z"})
        assert r.json()["count"] == 2
        assert (await c.get("/sentiment", params={"since": "nope"})).status_code == 422

        overview = (await c.get("/ui")).text
        assert "Crypto Fear &amp; Greed Index" in overview
        assert "Alternative.me" in overview

        page = (await c.get("/ui/sentiment", params={"days": 36500})).text
        assert "Latest: <strong>74</strong> (Greed)" in page
        assert 'href="https://alternative.me/crypto/fear-and-greed-index/"' in page


@pytest.mark.db
async def test_dashboard_sentiment_page_empty(engine):
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/ui/sentiment")
    assert r.status_code == 200
    assert "No sentiment readings yet" in r.text
