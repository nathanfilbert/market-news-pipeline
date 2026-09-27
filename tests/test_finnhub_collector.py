import json

import httpx
import pytest

from mnp.collectors.base import CollectorError, CollectorUnavailable, make_http_client
from mnp.collectors.finnhub import FinnhubCollector
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import make_collector
from mnp.config import Settings
from mnp.normalize.item import parse_payload
from tests.conftest import fixture_bytes, source

FH = source("fh", kind="finnhub", options={"category": "crypto"})


def collector(handler) -> FinnhubCollector:
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(handler))
    return FinnhubCollector(FH, client, api_key="secret-key")


def fixture_response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, content=fixture_bytes("finnhub/news_crypto.json"))


async def test_fetch_sends_key_in_header_and_returns_items():
    seen = []

    def handler(request):
        seen.append(request)
        return fixture_response(request)

    payloads, checkpoint = await collector(handler).fetch({})

    [request] = seen
    assert request.headers["x-finnhub-token"] == "secret-key"
    assert "secret-key" not in str(request.url)
    assert request.url.params["category"] == "crypto"
    assert "minId" not in request.url.params
    assert checkpoint == {"min_id": 7004}
    assert [p.external_id for p in payloads] == ["7004", "7003", "7002", "7001"]
    assert payloads[0].url == "https://wire.example.net/stablecoin-supply-record"
    assert payloads[0].payload["format"] == "finnhub_news"
    assert payloads[0].payload["item"]["source"] == "Wire Example"


async def test_checkpoint_min_id_is_sent_and_older_items_ignored():
    seen = []

    def handler(request):
        seen.append(request)
        return fixture_response(request)

    payloads, checkpoint = await collector(handler).fetch({"min_id": 7002})

    assert seen[0].url.params["minId"] == "7002"
    assert [p.external_id for p in payloads] == ["7004", "7003"]
    assert checkpoint == {"min_id": 7004}


async def test_no_new_items_keeps_checkpoint():
    payloads, checkpoint = await collector(lambda r: httpx.Response(200, json=[])).fetch(
        {"min_id": 7004}
    )
    assert payloads == []
    assert checkpoint == {"min_id": 7004}


async def test_hash_ignores_key_order():
    item = json.loads(fixture_bytes("finnhub/news_crypto.json"))[0]
    reordered = dict(reversed(list(item.items())))
    [a], _ = await collector(lambda r: httpx.Response(200, json=[item])).fetch({})
    [b], _ = await collector(lambda r: httpx.Response(200, json=[reordered])).fetch({})
    assert a.payload_sha256 == b.payload_sha256


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (httpx.Response(401, json={"error": "Invalid API key"}), "HTTP 401"),
        (httpx.Response(200, json={"error": "Please use an API key."}), "unexpected Finnhub"),
        (httpx.Response(200, content=b"<html>oops</html>"), "invalid JSON"),
    ],
)
async def test_errors(response, message):
    with pytest.raises(CollectorError, match=message):
        await collector(lambda r: response).fetch({})


async def test_rate_limit_carries_retry_after():
    response = httpx.Response(429, headers={"Retry-After": "30"})
    with pytest.raises(CollectorError) as exc_info:
        await collector(lambda r: response).fetch({})
    assert exc_info.value.retry_after == 30


async def test_make_collector_requires_api_key():
    async with httpx.AsyncClient() as client:
        with pytest.raises(CollectorUnavailable, match="FINNHUB_API_KEY"):
            make_collector(FH, client, Settings(_env_file=None))
        with_key = Settings(_env_file=None, finnhub_api_key="k")
        assert isinstance(make_collector(FH, client, with_key), FinnhubCollector)
        assert isinstance(make_collector(source("r"), client, with_key), RssCollector)


def test_parse_finnhub_payload():
    item = json.loads(fixture_bytes("finnhub/news_crypto.json"))[2]
    p = parse_payload({"format": "finnhub_news", "category": "crypto", "item": item})
    assert p.headline == "Example Exchange lists ‘TOKEN’ perpetuals — trading opens today"  # noqa: RUF001
    assert p.summary == "Example Exchange said it will list TOKEN perpetual futures & spot pairs."
    assert p.url == "https://news.example.com/markets/2026/09/27/example-exchange-lists-token"
    assert p.body is None
    assert p.published_at.isoformat() == "2026-09-27T12:00:00+00:00"


def test_parse_finnhub_payload_without_timestamp():
    p = parse_payload({"format": "finnhub_news", "item": {"headline": "x", "datetime": 0}})
    assert p.published_at is None


def test_parse_google_news_style_item_drops_suffix_and_echo_summary():
    item = {
        "headline": "Fed holds rates steady, signals patience - Reuters",
        "source": "Reuters",
        "summary": "Fed holds rates steady, signals patience" + chr(0xA0) * 2 + "Reuters",
        "url": "https://news.google.com/rss/articles/CBMiabc",
        "datetime": 1790517600,
    }
    p = parse_payload({"format": "finnhub_news", "item": item})
    assert p.headline == "Fed holds rates steady, signals patience"
    assert p.summary is None


def test_parse_keeps_real_summary_and_unrelated_dashes():
    item = {
        "headline": "Stocks - what to watch this week",
        "source": "CNBC",
        "summary": "Both sides of the Fed's mandate are in focus.",
    }
    p = parse_payload({"format": "finnhub_news", "item": item})
    assert p.headline == "Stocks - what to watch this week"
    assert p.summary == "Both sides of the Fed's mandate are in focus."
