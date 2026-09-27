import json
from datetime import UTC, datetime

import httpx
import pytest

from mnp.classify.base import ArticleState, ClassifierError, ClassifierUnavailable
from mnp.classify.jev import JevClassifier
from tests.conftest import jev_response

STATE = ArticleState(
    headline="Exchange X hacked for $40M",
    summary="Attackers drained hot wallets.",
    body=None,
    source_name="coindesk",
    source_category="crypto",
    published_at=datetime(2026, 9, 27, 12, 0, tzinfo=UTC),
)
QUESTIONS = {
    "is_market_relevant": {"type": "noul", "instructions": "Could this move prices?"},
    "domain": {
        "type": "choice",
        "instructions": "Which market?",
        "criteria": {"crypto": None, "other": None},
    },
    "impact": {"type": "score", "instructions": "How big?", "criteria": ["None", "Small", "Large"]},
}


def jev(handler) -> JevClassifier:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return JevClassifier(client, "sk-test", model="jev-latest")


async def test_request_follows_documented_shape():
    seen = []

    def handler(request):
        seen.append(request)
        return jev_response(request)

    result = await jev(handler).classify(STATE, QUESTIONS)

    [request] = seen
    assert str(request.url) == "https://api.typesafe.ai/v1/systemone"
    assert request.headers["authorization"] == "Bearer sk-test"
    body = json.loads(request.content)
    assert body["model"] == "jev-latest"
    assert body["questions"] == QUESTIONS
    assert body["state"] == {
        "headline": "Exchange X hacked for $40M",
        "summary": "Attackers drained hot wallets.",
        "body": None,
        "source": {"name": "coindesk", "category": "crypto"},
        "published_at": "2026-09-27T12:00:00+00:00",
    }
    assert result.model_version == "jev-1.13.0"  # the versioned id, not the alias
    assert result.answers["is_market_relevant"] == {"type": "noul", "noul": 0.8}
    assert result.answers["domain"]["choice"] == "crypto"
    assert result.raw["usage"]["input_tokens"] == 900
    assert result.latency_ms >= 0


def test_long_bodies_are_truncated():
    state = ArticleState("h", None, "x" * 50_000, "s", "c", None)
    assert len(state.to_json()["body"]) == 20_000


@pytest.mark.parametrize(
    ("status", "headers", "retry_after"),
    [
        (429, {"retry-after-ms": "1500"}, 1.5),
        (429, {"retry-after": "20"}, 20.0),
        (529, {}, None),
        (500, {}, None),
        (503, {}, None),
        (401, {}, None),
    ],
)
async def test_retryable_statuses(status, headers, retry_after):
    response = httpx.Response(status, headers=headers, json={"detail": "nope"})
    with pytest.raises(ClassifierUnavailable, match=f"HTTP {status}") as exc_info:
        await jev(lambda r: response).classify(STATE, QUESTIONS)
    assert exc_info.value.retry_after == retry_after


async def test_auth_error_mentions_key():
    with pytest.raises(ClassifierUnavailable, match="JEV_API_KEY"):
        await jev(lambda r: httpx.Response(401)).classify(STATE, QUESTIONS)


async def test_timeouts_and_connection_errors_are_retryable():
    def handler(request):
        raise httpx.ConnectTimeout("timed out", request=request)

    with pytest.raises(ClassifierUnavailable, match="ConnectTimeout"):
        await jev(handler).classify(STATE, QUESTIONS)


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (httpx.Response(422, json={"detail": "criteria required"}), "HTTP 422"),
        (httpx.Response(200, content=b"not json"), "invalid JSON"),
        (httpx.Response(200, json={"model": "jev-1.13.0"}), "unexpected Jev response"),
    ],
)
async def test_non_retryable_errors(response, message):
    with pytest.raises(ClassifierError, match=message):
        await jev(lambda r: response).classify(STATE, QUESTIONS)
