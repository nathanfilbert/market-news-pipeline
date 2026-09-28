"""Collector contract and shared HTTP helpers."""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from mnp import __version__
from mnp.config import Settings, SourceConfig

PROJECT_URL = "https://github.com/nathanfilbert/market-news-pipeline"


@dataclass(frozen=True, slots=True)
class RawPayload:
    """One item exactly as received, plus identifiers lifted from it for lookup."""

    payload: dict[str, Any]
    payload_sha256: str
    external_id: str | None = None
    url: str | None = None
    published_at: datetime | None = None  # if the source gives one; used to stop paging


class CollectorError(Exception):
    """A fetch failed. `retry_after` (seconds) is set when the server asked us to wait."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class CollectorUnavailable(Exception):
    """A source can't run in this environment (e.g. its API key isn't set)."""


class Collector(ABC):
    def __init__(self, source: SourceConfig, client: httpx.AsyncClient) -> None:
        self.source = source
        self.client = client

    @abstractmethod
    async def fetch(self, checkpoint: Mapping[str, Any]) -> tuple[list[RawPayload], dict[str, Any]]:
        """Return new payloads and the checkpoint to persist once they are stored."""

    async def fetch_history(self, since: datetime) -> list[RawPayload]:
        """Everything the source currently offers, back to `since` where it can page.

        Ignores the checkpoint (no conditional GET, no cursor), so nothing is skipped as
        "not modified". The default is one full fetch; collectors that can page override it.
        """
        payloads, _ = await self.fetch({})
        return payloads


def user_agent(settings: Settings) -> str:
    contact = f"; {settings.contact_email}" if settings.contact_email else ""
    return f"market-news-pipeline/{__version__} (+{PROJECT_URL}{contact})"


def make_http_client(settings: Settings, **kwargs: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": user_agent(settings)},
        timeout=httpx.Timeout(20.0, connect=10.0),
        follow_redirects=True,
        **kwargs,
    )


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Parse a Retry-After header (delta-seconds or HTTP-date) into seconds."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - (now or datetime.now(UTC))).total_seconds())


def raise_for_status(response: httpx.Response) -> None:
    """Raise CollectorError for non-2xx responses, carrying Retry-After for 429/503."""
    if response.is_success:
        return
    retry_after = None
    if response.status_code in (429, 503):
        retry_after = parse_retry_after(response.headers.get("retry-after"))
    raise CollectorError(
        f"HTTP {response.status_code} from {response.url}", retry_after=retry_after
    )
