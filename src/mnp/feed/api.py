"""Trading feed API v1: /v1/feed. Schema and semantics: docs/feed-v1.md.

Read-only; every request runs in a READ ONLY transaction (shared with the main API).
"""

from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import Float, and_, cast, select
from sqlalchemy.dialects.postgresql import JSONB, distinct_on

from mnp.models import FeedRevision
from mnp.outputs.api import Conn
from mnp.outputs.queries import parse_time

MAX_LIMIT = 1000

router = APIRouter(prefix="/v1/feed", tags=["feed v1"])


class Classification(BaseModel):
    question_set: str = Field(description="Question set the scores come from, e.g. v1.1.")
    event_type: str = Field(description="Most likely event type (reputation-weighted).")
    event_type_prob: float = Field(ge=0, le=1)
    domain: str = Field(description="crypto, equities, macro or other.")
    market_relevance: float | None = Field(None, ge=0, le=1)
    new_information: float | None = Field(None, ge=0, le=1)
    promotional: float | None = Field(None, ge=0, le=1)
    sentiment: float | None = Field(None, ge=-1, le=1, description="-1 bearish … +1 bullish")
    impact: float | None = Field(None, ge=0, le=1)
    urgency: float | None = Field(None, ge=0, le=1)


class EventAsset(BaseModel):
    symbol: str
    name: str
    kind: str = Field(description="crypto, equity, index or macro")
    relevance: float = Field(ge=0, le=1, description="Strongest confirmation across articles.")
    article_count: int = Field(description="Articles confirming the asset (relevance >= 0.5).")


class Attribution(BaseModel):
    text: str
    url: str


class EventArticle(BaseModel):
    article_id: int
    source: str
    url: str
    headline: str
    published_at: datetime | None = Field(description="As stated by the publisher.")
    received_at: datetime = Field(description="When the pipeline first fetched it.")
    classified: bool
    attribution: Attribution | None = Field(
        None, description="Citation this article's source requires (e.g. GDELT)."
    )


class Latency(BaseModel):
    published_to_received: float | None = Field(description="Seconds, first article.")
    received_to_classified: float | None = Field(description="Seconds, first classification.")
    received_to_available: float = Field(description="Seconds until this revision was published.")


class EventRevision(BaseModel):
    cursor: str = Field(description="Opaque position in the feed; pass as `after`.")
    event_id: int = Field(description="Stable id of the real-world event (story).")
    revision: int = Field(description="1, 2, … for each change to the event.")
    status: Literal["active", "retracted"]
    available_at: datetime = Field(description="When this revision entered the feed.")
    schema_version: str
    # Active revisions:
    headline: str | None = None
    summary: str | None = None
    url: str | None = None
    first_published_at: datetime | None = None
    first_received_at: datetime | None = None
    first_classified_at: datetime | None = None
    sources: list[str] = []
    article_count: int = 0
    classified_article_count: int = 0
    classification: Classification | None = None
    assets: list[EventAsset] = []
    articles: list[EventArticle] = []
    latency: Latency | None = None
    attributions: list[Attribution] = Field(
        [],
        description="Citations required by the event's sources; show them wherever the event is "
        "used or redistributed.",
    )
    # Retracted revisions:
    superseded_by: list[int] = Field(
        [], description="Events that now hold this event's articles (after a re-cluster)."
    )


class EventPage(BaseModel):
    revisions: list[EventRevision]
    next_cursor: str = Field(description="Pass as `after` to continue; unchanged if empty.")
    has_more: bool


class Snapshot(BaseModel):
    as_of: datetime
    count: int
    events: list[EventRevision]


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    return round((end - start).total_seconds(), 3) if start and end else None


def to_revision(row: Any) -> EventRevision:
    p = dict(row.payload)
    extra: dict[str, Any] = {}
    if row.status == "active":
        received = datetime.fromisoformat(p["first_received_at"])
        published = p.get("first_published_at")
        classified = p.get("first_classified_at")
        extra["latency"] = Latency(
            published_to_received=_seconds(
                datetime.fromisoformat(published) if published else None, received
            ),
            received_to_classified=_seconds(
                received, datetime.fromisoformat(classified) if classified else None
            ),
            received_to_available=_seconds(received, row.available_at),
        )
    p.pop("status", None)
    p.pop("event_id", None)
    return EventRevision(
        cursor=str(row.id),
        event_id=row.event_id,
        revision=row.revision,
        status=row.status,
        available_at=row.available_at,
        **p,
        **extra,
    )


def _cursor(value: str) -> int:
    if not value.isdigit():
        raise HTTPException(422, "after: not a cursor from this feed")
    return int(value)


def _time(value: str | None, name: str) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_time(value)
    except ValueError as exc:
        raise HTTPException(422, f"{name}: {exc}") from exc


@router.get("/events", response_model=EventPage)
async def feed_events(
    conn: Conn,
    after: Annotated[str, Query(description="Cursor from a previous page; 0 = start.")] = "0",
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 100,
) -> EventPage:
    """Every revision after `after`, oldest first: new events and changes to existing ones.

    Poll with the returned `next_cursor`. Nothing is skipped or repeated, however irregular
    the polling.
    """
    start = _cursor(after)
    rows = (
        await conn.execute(
            select(FeedRevision)
            .where(FeedRevision.id > start)
            .order_by(FeedRevision.id)
            .limit(limit + 1)
        )
    ).all()
    page = rows[:limit]
    return EventPage(
        revisions=[to_revision(r) for r in page],
        next_cursor=str(page[-1].id) if page else str(start),
        has_more=len(rows) > limit,
    )


@router.get("/snapshot", response_model=Snapshot)
async def feed_snapshot(
    conn: Conn,
    as_of: Annotated[
        str | None, Query(description="Point in time (ISO datetime or e.g. 2h); default now.")
    ] = None,
    since: Annotated[
        str | None, Query(description="Only events first received at/after this.")
    ] = "24h",
    event_type: str | None = None,
    asset: Annotated[str | None, Query(description="Symbol, e.g. BTC.")] = None,
    min_impact: Annotated[float | None, Query(ge=0, le=1)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 200,
) -> Snapshot:
    """Every active event as the feed showed it at `as_of`: its latest revision up to then.

    Nothing the pipeline learned after `as_of` is visible, so this is safe for backtests.
    """
    at = _time(as_of, "as_of") or datetime.now(UTC)
    start = _time(since, "since")
    latest = (
        select(FeedRevision)
        .where(FeedRevision.available_at <= at)
        .ext(distinct_on(FeedRevision.event_id))
        .order_by(FeedRevision.event_id, FeedRevision.id.desc())
        .subquery("latest")
    )
    p = latest.c.payload
    conditions = [latest.c.status == "active"]
    if start:
        conditions.append(p["first_received_at"].astext >= start.astimezone(UTC).isoformat())
    if event_type:
        conditions.append(p["classification"]["event_type"].astext == event_type)
    if min_impact is not None:
        conditions.append(cast(p["classification"]["impact"].astext, Float) >= min_impact)
    if asset:
        conditions.append(p["assets"].contains(cast([{"symbol": asset.upper()}], JSONB)))
    rows = (
        await conn.execute(
            select(latest)
            .where(and_(*conditions))
            .order_by(p["first_received_at"].astext.desc(), latest.c.event_id.desc())
            .limit(limit)
        )
    ).all()
    return Snapshot(as_of=at, count=len(rows), events=[to_revision(r) for r in rows])


@router.get("/events/{event_id}", response_model=EventRevision)
async def feed_event(
    conn: Conn,
    event_id: int,
    as_of: Annotated[str | None, Query(description="Default now.")] = None,
) -> EventRevision:
    """One event's latest revision (as of `as_of`)."""
    at = _time(as_of, "as_of") or datetime.now(UTC)
    row = (
        await conn.execute(
            select(FeedRevision)
            .where(FeedRevision.event_id == event_id, FeedRevision.available_at <= at)
            .order_by(FeedRevision.id.desc())
            .limit(1)
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(404, "event not in the feed (as of that time)")
    return to_revision(row)


@router.get("/events/{event_id}/revisions", response_model=list[EventRevision])
async def feed_event_revisions(conn: Conn, event_id: int) -> list[EventRevision]:
    """Every revision of one event, oldest first."""
    rows = (
        await conn.execute(
            select(FeedRevision).where(FeedRevision.event_id == event_id).order_by(FeedRevision.id)
        )
    ).all()
    if not rows:
        raise HTTPException(404, "event not in the feed")
    return [to_revision(r) for r in rows]
