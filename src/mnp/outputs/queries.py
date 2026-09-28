"""Read-only queries shared by the CLI and the API.

An article is shown as its latest version, with that version's classification from one
classifier and question set (default: Jev and the active QUESTION_SET). Articles whose latest
version isn't classified yet still appear, unless a classification filter is used. Time filters
and ordering use `articles.first_seen_at`, when the story first reached us. Backfill articles
(old news we only just saw) are hidden unless `include_backfill` is set.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.config import get_settings
from mnp.models import (
    Article,
    ArticleAsset,
    ArticleVersion,
    Asset,
    Classification,
    Cluster,
    Job,
    RawItem,
    Source,
    SourceState,
)

DEFAULT_MIN_ASSET_RELEVANCE = 0.5
MAX_LIMIT = 500
# A source is stale when it hasn't fetched successfully for this many poll intervals
# (and at least STALE_MIN_SECONDS, so fast pollers aren't flagged by one slow cycle).
STALE_POLL_INTERVALS = 5
STALE_MIN_SECONDS = 600

_DURATION = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def parse_time(value: str, now: datetime | None = None) -> datetime:
    """'90m', '1h', '2d', '1w' (ago), or an ISO date/datetime (UTC if no offset given)."""
    now = now or datetime.now(UTC)
    if m := _DURATION.match(value):
        return now - timedelta(**{_UNITS[m.group(2)]: int(m.group(1))})
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        raise ValueError(f"invalid time {value!r}: use e.g. 1h, 2d or 2026-09-27T12:00") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass(frozen=True)
class ArticleFilter:
    since: datetime | None = None
    until: datetime | None = None
    assets: Sequence[str] = ()  # symbols; an article matches if it is about any of them
    event_types: Sequence[str] = ()
    domain: str | None = None
    sources: Sequence[str] = ()
    min_impact: float | None = None
    min_relevance: float | None = None  # is_market_relevant_prob
    min_asset_relevance: float = DEFAULT_MIN_ASSET_RELEVANCE
    include_backfill: bool = False
    classifier: str = "jev"
    question_set: str = field(default_factory=lambda: get_settings().question_set)
    limit: int = 50
    offset: int = 0

    @property
    def needs_classification(self) -> bool:
        return bool(
            self.assets
            or self.event_types
            or self.domain
            or self.min_impact is not None
            or self.min_relevance is not None
        )


@dataclass
class AssetTag:
    symbol: str
    relevance_prob: float
    via: str


@dataclass
class ArticleRow:
    id: int
    canonical_url: str
    first_seen_at: datetime
    cluster_id: int | None
    cluster_size: int
    is_backfill: bool
    source: str
    version_id: int
    version_no: int
    headline: str
    summary: str | None
    published_at: datetime | None
    received_at: datetime
    classification: dict[str, Any] | None
    assets: list[AssetTag] = field(default_factory=list)


CLASSIFICATION_FIELDS = (
    "id",
    "event_type",
    "event_type_prob",
    "domain",
    "is_market_relevant_prob",
    "is_new_information_prob",
    "is_promotional_prob",
    "sentiment",
    "impact",
    "urgency",
    "classifier",
    "model_version",
    "question_set_version",
    "classified_at",
)


def _latest_versions():
    return (
        select(ArticleVersion)
        .ext(distinct_on(ArticleVersion.article_id))
        .order_by(ArticleVersion.article_id, ArticleVersion.version_no.desc())
        .subquery("latest")
    )


async def _asset_tags(
    conn: AsyncConnection, classification_ids: Sequence[int]
) -> dict[int, list[AssetTag]]:
    if not classification_ids:
        return {}
    rows = await conn.execute(
        select(
            ArticleAsset.classification_id,
            Asset.symbol,
            ArticleAsset.relevance_prob,
            ArticleAsset.candidate_via,
        )
        .join(Asset, Asset.id == ArticleAsset.asset_id)
        .where(ArticleAsset.classification_id.in_(classification_ids))
        .order_by(ArticleAsset.relevance_prob.desc())
    )
    tags: dict[int, list[AssetTag]] = {}
    for r in rows:
        tags.setdefault(r.classification_id, []).append(
            AssetTag(r.symbol, r.relevance_prob, r.candidate_via)
        )
    return tags


async def _cluster_sizes(conn: AsyncConnection, cluster_ids: Sequence[int]) -> dict[int, int]:
    if not cluster_ids:
        return {}
    rows = await conn.execute(
        select(Article.cluster_id, func.count())
        .where(Article.cluster_id.in_(cluster_ids))
        .group_by(Article.cluster_id)
    )
    return dict(rows.all())


async def search_articles(
    conn: AsyncConnection, f: ArticleFilter, *, article_ids: Sequence[int] | None = None
) -> list[ArticleRow]:
    latest = _latest_versions()
    c = Classification.__table__.alias("c")
    stmt = (
        select(
            Article.id,
            Article.canonical_url,
            Article.first_seen_at,
            Article.cluster_id,
            Article.is_backfill,
            Source.name.label("source"),
            latest.c.id.label("version_id"),
            latest.c.version_no,
            latest.c.headline,
            latest.c.summary,
            latest.c.published_at,
            latest.c.received_at,
            *(c.c[name].label(f"c_{name}") for name in CLASSIFICATION_FIELDS),
        )
        .join(latest, latest.c.article_id == Article.id)
        .join(RawItem, RawItem.id == latest.c.raw_item_id)
        .join(Source, Source.id == RawItem.source_id)
        .join(
            c,
            and_(
                c.c.article_version_id == latest.c.id,
                c.c.classifier == f.classifier,
                c.c.question_set_version == f.question_set,
            ),
            isouter=not f.needs_classification,
        )
        .order_by(Article.first_seen_at.desc(), Article.id.desc())
        .limit(min(max(f.limit, 1), MAX_LIMIT))
        .offset(max(f.offset, 0))
    )
    if article_ids is not None:
        stmt = stmt.where(Article.id.in_(article_ids))
    if not f.include_backfill:
        stmt = stmt.where(Article.is_backfill.is_(False))
    if f.since:
        stmt = stmt.where(Article.first_seen_at >= f.since)
    if f.until:
        stmt = stmt.where(Article.first_seen_at < f.until)
    if f.sources:
        stmt = stmt.where(Source.name.in_(f.sources))
    if f.event_types:
        stmt = stmt.where(c.c.event_type.in_(f.event_types))
    if f.domain:
        stmt = stmt.where(c.c.domain == f.domain)
    if f.min_impact is not None:
        stmt = stmt.where(c.c.impact >= f.min_impact)
    if f.min_relevance is not None:
        stmt = stmt.where(c.c.is_market_relevant_prob >= f.min_relevance)
    if f.assets:
        stmt = stmt.where(
            select(ArticleAsset.asset_id)
            .join(Asset, Asset.id == ArticleAsset.asset_id)
            .where(
                ArticleAsset.classification_id == c.c.id,
                Asset.symbol.in_([s.upper() for s in f.assets]),
                ArticleAsset.relevance_prob >= f.min_asset_relevance,
            )
            .exists()
        )

    rows = (await conn.execute(stmt)).all()
    tags = await _asset_tags(conn, [r.c_id for r in rows if r.c_id is not None])
    sizes = await _cluster_sizes(conn, {r.cluster_id for r in rows if r.cluster_id})
    return [
        ArticleRow(
            id=r.id,
            canonical_url=r.canonical_url,
            first_seen_at=r.first_seen_at,
            cluster_id=r.cluster_id,
            cluster_size=sizes.get(r.cluster_id, 1),
            is_backfill=r.is_backfill,
            source=r.source,
            version_id=r.version_id,
            version_no=r.version_no,
            headline=r.headline,
            summary=r.summary,
            published_at=r.published_at,
            received_at=r.received_at,
            classification=(
                {name: getattr(r, f"c_{name}") for name in CLASSIFICATION_FIELDS}
                if r.c_id is not None
                else None
            ),
            assets=tags.get(r.c_id, []) if r.c_id is not None else [],
        )
        for r in rows
    ]


async def get_article(conn: AsyncConnection, article_id: int) -> dict[str, Any] | None:
    """An article with every version, each with all its classifications and asset tags."""
    article = (await conn.execute(select(Article).where(Article.id == article_id))).one_or_none()
    if article is None:
        return None
    versions = (
        await conn.execute(
            select(ArticleVersion, Source.name.label("source"))
            .join(RawItem, RawItem.id == ArticleVersion.raw_item_id)
            .join(Source, Source.id == RawItem.source_id)
            .where(ArticleVersion.article_id == article_id)
            .order_by(ArticleVersion.version_no)
        )
    ).all()
    classifications = (
        await conn.execute(
            select(Classification)
            .where(Classification.article_version_id.in_([v.id for v in versions]))
            .order_by(Classification.id)
        )
    ).all()
    tags = await _asset_tags(conn, [c.id for c in classifications])
    by_version: dict[int, list[dict[str, Any]]] = {}
    for cl in classifications:
        by_version.setdefault(cl.article_version_id, []).append(
            {
                **{name: getattr(cl, name) for name in CLASSIFICATION_FIELDS},
                "latency_ms": cl.latency_ms,
                "content_hash": cl.content_hash,
                "results": cl.results,
                "assets": tags.get(cl.id, []),
            }
        )
    return {
        "id": article.id,
        "canonical_url": article.canonical_url,
        "first_seen_at": article.first_seen_at,
        "cluster_id": article.cluster_id,
        "is_backfill": article.is_backfill,
        "versions": [
            {
                "id": v.id,
                "version_no": v.version_no,
                "source": v.source,
                "headline": v.headline,
                "summary": v.summary,
                "body": v.body,
                "author": v.author,
                "language": v.language,
                "published_at": v.published_at,
                "received_at": v.received_at,
                "content_hash": v.content_hash,
                "raw_item_id": v.raw_item_id,
                "classifications": by_version.get(v.id, []),
            }
            for v in versions
        ],
    }


async def get_raw_item(conn: AsyncConnection, raw_item_id: int) -> dict[str, Any] | None:
    row = (
        await conn.execute(
            select(RawItem, Source.name.label("source"))
            .join(Source, Source.id == RawItem.source_id)
            .where(RawItem.id == raw_item_id)
        )
    ).one_or_none()
    if row is None:
        return None
    return {
        "id": row.id,
        "source": row.source,
        "fetched_at": row.fetched_at,
        "external_id": row.external_id,
        "url": row.url,
        "payload_sha256": row.payload_sha256,
        "payload": row.payload,
    }


async def get_cluster(
    conn: AsyncConnection, cluster_id: int, f: ArticleFilter
) -> dict[str, Any] | None:
    cluster = (await conn.execute(select(Cluster).where(Cluster.id == cluster_id))).one_or_none()
    if cluster is None:
        return None
    ids = (await conn.execute(select(Article.id).where(Article.cluster_id == cluster_id))).scalars()
    return {
        "id": cluster.id,
        "first_seen_at": cluster.first_seen_at,
        "representative_article_id": cluster.representative_article_id,
        "articles": await search_articles(conn, f, article_ids=list(ids)),
    }


def source_status(
    *,
    enabled: bool,
    poll_seconds: int,
    last_success_at: datetime | None,
    consecutive_failures: int,
    now: datetime,
) -> str:
    """disabled | never_run | stale | failing | ok."""
    if not enabled:
        return "disabled"
    if last_success_at is None:
        return "never_run"
    stale_after = max(poll_seconds * STALE_POLL_INTERVALS, STALE_MIN_SECONDS)
    if (now - last_success_at).total_seconds() > stale_after:
        return "stale"
    if consecutive_failures:
        return "failing"
    return "ok"


async def health(conn: AsyncConnection, now: datetime | None = None) -> dict[str, Any]:
    now = now or (await conn.execute(select(func.now()))).scalar_one()
    rows = (
        await conn.execute(
            select(
                Source.name,
                Source.kind,
                Source.enabled,
                Source.poll_seconds,
                SourceState.last_success_at,
                SourceState.last_error_at,
                SourceState.last_error,
                SourceState.consecutive_failures,
                select(func.max(RawItem.fetched_at))
                .where(RawItem.source_id == Source.id)
                .scalar_subquery()
                .label("last_new_item_at"),
            )
            .outerjoin(SourceState, SourceState.source_id == Source.id)
            .order_by(Source.name)
        )
    ).all()
    sources = []
    for r in rows:
        status = source_status(
            enabled=r.enabled,
            poll_seconds=r.poll_seconds,
            last_success_at=r.last_success_at,
            consecutive_failures=r.consecutive_failures or 0,
            now=now,
        )
        sources.append(
            {
                "name": r.name,
                "kind": r.kind,
                "status": status,
                "poll_seconds": r.poll_seconds,
                "last_success_at": r.last_success_at,
                "last_new_item_at": r.last_new_item_at,
                "last_error_at": r.last_error_at,
                "last_error": r.last_error,
                "consecutive_failures": r.consecutive_failures or 0,
            }
        )

    job_rows = (
        await conn.execute(
            select(
                Job.kind,
                func.count().filter(Job.status == "pending").label("pending"),
                func.count().filter(Job.status == "failed").label("failed"),
                func.min(Job.created_at).filter(Job.status == "pending").label("oldest"),
            ).group_by(Job.kind)
        )
    ).all()
    jobs = {
        j.kind: {
            "pending": j.pending,
            "failed": j.failed,
            "oldest_pending_age_seconds": (
                round((now - j.oldest).total_seconds()) if j.oldest else None
            ),
        }
        for j in job_rows
    }
    degraded = any(s["status"] in ("stale", "failing", "never_run") for s in sources)
    return {
        "status": "degraded" if degraded else "ok",
        "checked_at": now,
        "sources": sources,
        "jobs": jobs,
    }
