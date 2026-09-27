"""Jev (TypeSafe AI) classifier: POST https://api.typesafe.ai/v1/systemone.

Request/response shapes follow https://docs.typesafe.ai/api (read 2026-09-27, jev-1.13):
one call evaluates one `state` against a map of typed questions and returns one answer per
question id, plus the versioned `model` that answered.

Retrying is left to the job queue: outages, overload (429/529/5xx), timeouts and auth errors
raise ClassifierUnavailable, so jobs wait and retry with backoff and nothing is lost; a 422
(request failed validation) is a bug on our side and counts toward the job's attempts.
"""

import time
from collections.abc import Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from mnp.classify.base import (
    ArticleState,
    ClassificationResult,
    ClassifierError,
    ClassifierUnavailable,
    Question,
)

DEFAULT_URL = "https://api.typesafe.ai/v1/systemone"
RETRYABLE_STATUSES = {401, 403, 408, 429, 500, 502, 503, 504, 529}


def _retry_after(response: httpx.Response) -> float | None:
    if ms := response.headers.get("retry-after-ms"):
        try:
            return float(ms) / 1000
        except ValueError:
            pass
    value = (response.headers.get("retry-after") or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


class JevClassifier:
    name = "jev"

    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        *,
        model: str = "jev-latest",
        url: str = DEFAULT_URL,
        timeout: float = 30.0,
    ) -> None:
        self.client = client
        self.api_key = api_key
        self.model = model
        self.url = url
        self.timeout = timeout

    async def classify(
        self, state: ArticleState, questions: Mapping[str, Question]
    ) -> ClassificationResult:
        body = {"state": state.to_json(), "model": self.model, "questions": dict(questions)}
        started = time.monotonic()
        try:
            response = await self.client.post(
                self.url,
                json=body,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout,
            )
        except httpx.TransportError as exc:  # connection errors and timeouts
            raise ClassifierUnavailable(f"Jev request failed: {type(exc).__name__}: {exc}") from exc
        latency_ms = round((time.monotonic() - started) * 1000)

        if response.status_code in RETRYABLE_STATUSES:
            hint = " (check JEV_API_KEY)" if response.status_code in (401, 403) else ""
            raise ClassifierUnavailable(
                f"Jev HTTP {response.status_code}{hint}: {response.text[:300]}",
                retry_after=_retry_after(response),
            )
        if not response.is_success:
            raise ClassifierError(f"Jev HTTP {response.status_code}: {response.text[:1000]}")
        try:
            data = response.json()
        except ValueError as exc:
            raise ClassifierError(f"Jev returned invalid JSON: {exc}") from exc
        if not isinstance(data, dict) or not isinstance(data.get("answers"), dict):
            raise ClassifierError(f"unexpected Jev response: {str(data)[:500]}")
        return ClassificationResult(
            model_version=str(data.get("model") or self.model),
            answers=data["answers"],
            raw=data,
            latency_ms=latency_ms,
        )
