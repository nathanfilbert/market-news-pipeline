"""GDELT DOC 2.0 article search (GET /api/v2/doc/doc?mode=ArtList&format=json).

Free, no key. GDELT allows one request per 5 seconds per client and throttled bursty clients
during testing (2026-09-27), so every GDELT source in the process shares one pacer that spaces
requests further apart than that, and a throttled response pushes all of them back.

Each source is one query (`options.query`, GDELT query syntax such as `theme:SANCTIONS`).
Results are read oldest first from a time window starting at the checkpoint, so a busy window
that needs more than `max_pages` pages carries on where it stopped at the next poll instead of
losing the middle. Articles carry URL, title, domain, language and the time GDELT first saw
them, but no summary. Each article object is stored as received.
"""

import asyncio
import hashlib
import logging
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from mnp.collectors.base import (
    Collector,
    CollectorError,
    CollectorUnavailable,
    RawPayload,
    raise_for_status,
)
from mnp.collectors.finnhub import canonical_json
from mnp.config import SourceConfig

log = logging.getLogger(__name__)

MIN_INTERVAL_SECONDS = 10.0  # GDELT's limit is one request per 5 s; stay well clear of it
# All GDELT sources wait this long after being throttled. Live testing (2026-09-28) saw 429s
# persist through 150 s of silence, so the pause is long.
THROTTLED_PAUSE_SECONDS = 300.0
MAX_RECORDS = 250  # the API's maximum per request
# GDELT often takes 20 s or more to answer (measured 2026-09-28), beyond the shared 20 s timeout.
REQUEST_TIMEOUT = httpx.Timeout(60.0, connect=15.0)
SEENDATE_FORMAT = "%Y%m%dT%H%M%SZ"
QUERY_DATE_FORMAT = "%Y%m%d%H%M%S"


class RequestPacer:
    """Spaces requests at least `interval` seconds apart across every caller in the process."""

    def __init__(
        self,
        interval: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.interval = interval
        self.clock = clock
        self.sleep = sleep
        self._next_at = 0.0
        self._lock: asyncio.Lock | None = None
        self._lock_loop: asyncio.AbstractEventLoop | None = None

    def _get_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._lock is None or self._lock_loop is not loop:
            self._lock, self._lock_loop = asyncio.Lock(), loop
        return self._lock

    async def wait(self) -> None:
        """Return when the next request may be sent."""
        async with self._get_lock():
            delay = self._next_at - self.clock()
            if delay > 0:
                await self.sleep(delay)
            self._next_at = self.clock() + self.interval

    def pause(self, seconds: float) -> None:
        """Hold every caller back for at least `seconds` from now."""
        self._next_at = max(self._next_at, self.clock() + seconds)


PACER = RequestPacer(MIN_INTERVAL_SECONDS)


def parse_seendate(value: Any) -> datetime | None:
    """GDELT's `seendate` ("20260927T120000Z") as UTC; None if missing or malformed."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, SEENDATE_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def _is_throttle_message(text: str) -> bool:
    return "limit requests" in text.casefold()


class GdeltCollector(Collector):
    def __init__(
        self,
        source: SourceConfig,
        client: httpx.AsyncClient,
        *,
        pacer: RequestPacer = PACER,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        super().__init__(source, client)
        query = source.options.get("query")
        if not isinstance(query, str) or not query.strip():
            raise CollectorUnavailable("options.query not set")
        self.query = query.strip()
        self.max_pages = int(source.options.get("max_pages", 4))
        # First poll (no checkpoint) looks this far back. GDELT's newest articles lag 30-45
        # minutes, so 60 minutes would only cover about half an hour.
        self.lookback = timedelta(minutes=float(source.options.get("lookback_minutes", 120)))
        # GDELT indexes in 15-minute batches, so an article can appear after later ones were
        # already returned: each poll re-reads this much before the checkpoint.
        self.overlap = timedelta(minutes=float(source.options.get("overlap_minutes", 30)))
        self.pacer = pacer
        self.now = now

    async def fetch(self, checkpoint: Mapping[str, Any]) -> tuple[list[RawPayload], dict[str, Any]]:
        now = self.now()
        seen_through = parse_seendate(checkpoint.get("seen_through"))
        start = seen_through - self.overlap if seen_through else now - self.lookback
        payloads, latest = await self._fetch_window(start, now, self.max_pages)
        latest = max(filter(None, (latest, seen_through)), default=None)
        return payloads, ({"seen_through": latest.strftime(SEENDATE_FORMAT)} if latest else {})

    async def fetch_history(self, since: datetime) -> list[RawPayload]:
        """Everything matching the query since `since` (GDELT searches the last 3 months)."""
        pages = int(self.source.options.get("history_max_pages", 40))
        payloads, _ = await self._fetch_window(since, self.now(), pages)
        return payloads

    async def _fetch_window(
        self, start: datetime, end: datetime, max_pages: int
    ) -> tuple[list[RawPayload], datetime | None]:
        """Page through [start, end] oldest first. Returns payloads and the latest seendate."""
        payloads: list[RawPayload] = []
        latest: datetime | None = None
        for _ in range(max_pages):
            articles = await self._request(start, end)
            page_latest: datetime | None = None
            for article in articles:
                if not isinstance(article, dict) or not article.get("url"):
                    continue
                seen = parse_seendate(article.get("seendate"))
                if seen and (page_latest is None or seen > page_latest):
                    page_latest = seen
                payloads.append(
                    RawPayload(
                        payload={"format": "gdelt_doc", "query": self.query, "item": article},
                        payload_sha256=hashlib.sha256(canonical_json(article)).hexdigest(),
                        external_id=article["url"],
                        url=article["url"],
                        published_at=seen,
                    )
                )
            if page_latest:
                latest = max(latest or page_latest, page_latest)
            if len(articles) < MAX_RECORDS:
                break
            # A full page: continue from its newest article (inclusive, since seendate has
            # one-second resolution; repeats are dropped by payload hash when stored). If the
            # whole page shares one second there is no way forward.
            if page_latest is None or page_latest <= start:
                log.warning(
                    "GDELT query for %s returned %d articles within one second; some skipped",
                    self.source.name,
                    len(articles),
                    extra={"source": self.source.name},
                )
                break
            start = page_latest
        else:
            log.warning(
                "GDELT query for %s still had results after %d pages; continuing next poll",
                self.source.name,
                max_pages,
                extra={"source": self.source.name},
            )
        return payloads, latest

    async def _request(self, start: datetime, end: datetime) -> list[Any]:
        await self.pacer.wait()
        response = await self.client.get(
            str(self.source.url),
            params={
                "query": self.query,
                "mode": "ArtList",
                "format": "json",
                "sort": "DateAsc",
                "maxrecords": MAX_RECORDS,
                "startdatetime": start.strftime(QUERY_DATE_FORMAT),
                "enddatetime": end.strftime(QUERY_DATE_FORMAT),
            },
            headers={"Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        try:
            body = response.json() if response.is_success else None
        except ValueError:
            body = None
        # Throttling ("Please limit requests to one every 5 seconds") and query errors ("Your
        # search contained a keyword that is too short") can come back as 200 with plain text.
        if response.status_code == 429 or (body is None and _is_throttle_message(response.text)):
            self.pacer.pause(THROTTLED_PAUSE_SECONDS)
            raise CollectorError(
                f"GDELT throttled the request: {response.text[:200]!r}",
                retry_after=THROTTLED_PAUSE_SECONDS,
            )
        raise_for_status(response)
        if body is None:
            if "too short or too long" in response.text:
                raise CollectorError(
                    "GDELT rejected the query as too long: keep to ~3 OR'd themes "
                    f"({len(self.query)} characters)"
                )
            raise CollectorError(f"GDELT error: {response.text[:200]!r}")
        if not isinstance(body, dict):
            raise CollectorError(f"unexpected GDELT response: {str(body)[:200]}")
        articles = body.get("articles", [])  # no matches: an empty object
        if not isinstance(articles, list):
            raise CollectorError(f"unexpected GDELT articles: {str(articles)[:200]}")
        return articles
