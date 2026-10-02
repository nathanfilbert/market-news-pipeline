from datetime import UTC, datetime

import httpx
import pytest

from mnp.collectors.base import CollectorError, make_http_client, parse_retry_after
from mnp.collectors.rss import RssCollector
from mnp.config import Settings, SourceConfig
from tests.conftest import fixture_bytes

URL = "https://news.example.com/rss.xml"
SOURCE = SourceConfig(name="example", kind="rss", url=URL, category="crypto", reputation=0.5)


def collector(handler) -> RssCollector:
    settings = Settings(_env_file=None, contact_email="ops@example.com")
    client = make_http_client(settings, transport=httpx.MockTransport(handler))
    return RssCollector(SOURCE, client)


async def test_fetch_returns_payloads_and_checkpoint():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["headers"] = request.headers
        return httpx.Response(
            200,
            content=fixture_bytes("rss/wordpress.xml"),
            headers={"ETag": '"v1"', "Last-Modified": "Sun, 27 Sep 2026 12:00:00 GMT"},
        )

    payloads, checkpoint = await collector(handler).fetch({})

    assert "market-news-pipeline/" in seen["headers"]["user-agent"]
    assert "ops@example.com" in seen["headers"]["user-agent"]
    assert "if-none-match" not in seen["headers"]
    assert checkpoint == {"etag": '"v1"', "last_modified": "Sun, 27 Sep 2026 12:00:00 GMT"}
    assert len(payloads) == 3
    first = payloads[0]
    assert first.external_id == "https://news.example.com/?p=1001"
    assert first.payload["format"] == "xml_item"
    assert first.payload["feed_url"] == URL
    assert first.payload["http"]["etag"] == '"v1"'
    assert len(first.payload_sha256) == 64


async def test_conditional_get_and_304():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["headers"] = request.headers
        return httpx.Response(304)

    checkpoint = {"etag": '"v1"', "last_modified": "Sun, 27 Sep 2026 12:00:00 GMT"}
    payloads, new_checkpoint = await collector(handler).fetch(checkpoint)

    assert seen["headers"]["if-none-match"] == '"v1"'
    assert seen["headers"]["if-modified-since"] == "Sun, 27 Sep 2026 12:00:00 GMT"
    assert payloads == []
    assert new_checkpoint == checkpoint


async def test_rate_limit_carries_retry_after():
    def handler(request):
        return httpx.Response(429, headers={"Retry-After": "120"})

    with pytest.raises(CollectorError, match="HTTP 429") as exc_info:
        await collector(handler).fetch({})
    assert exc_info.value.retry_after == 120


async def test_server_error_raises():
    with pytest.raises(CollectorError, match="HTTP 500") as exc_info:
        await collector(lambda request: httpx.Response(500)).fetch({})
    assert exc_info.value.retry_after is None


def test_parse_retry_after():
    now = datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC)
    assert parse_retry_after("30") == 30
    assert parse_retry_after("Sun, 27 Sep 2026 12:01:30 GMT", now=now) == 90
    assert parse_retry_after("Sun, 27 Sep 2026 11:00:00 GMT", now=now) == 0
    assert parse_retry_after("soon") is None
    assert parse_retry_after(None) is None


@pytest.mark.parametrize(("skip", "expected"), [(True, ["flash-1", "flash-3"]), (False, None)])
async def test_skip_cjk_headlines(skip, expected):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=fixture_bytes("rss/mixed_language.xml"))

    source = SOURCE.model_copy(update={"options": {"skip_cjk_headlines": True}} if skip else {})
    client = make_http_client(Settings(_env_file=None), transport=httpx.MockTransport(handler))
    payloads, _ = await RssCollector(source, client).fetch({})
    ids = [p.external_id for p in payloads]
    assert ids == (expected or ["flash-1", "flash-2", "flash-3"])
