"""Build event snapshots and append revisions to the feed.

An event is a cluster of articles (the same story from any number of outlets). Whenever
something about it changes, a feed job recomputes its snapshot and, if the content differs
from the last revision, appends a new one. Jobs are queued in the same transaction as the
change (cluster assignment, new article version, classification), so no change is missed.
"""

import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, insert, select, text
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from mnp.config import Settings, get_settings
from mnp.jobs import FEED, enqueue
from mnp.models import (
    Article,
    ArticleAsset,
    ArticleVersion,
    Asset,
    Classification,
    Cluster,
    FeedRevision,
    RawItem,
    Source,
)

SCHEMA_VERSION = "1"
ASSET_MIN_RELEVANCE = 0.5  # an asset is "confirmed" for an article at this relevance
MIN_WEIGHT = 0.1  # sources with reputation 0 still count a little
# One writer at a time: revisions commit in id (cursor) order. Arbitrary constant.
_FEED_LOCK_KEY = 7_214_002
SCORES = {
    "market_relevance": "is_market_relevant_prob",
    "new_information": "is_new_information_prob",
    "promotional": "is_promotional_prob",
    "sentiment": "sentiment",
    "impact": "impact",
    "urgency": "urgency",
}


async def enqueue_feed(conn: AsyncConnection, cluster_id: int | None, reason: str) -> None:
    """Queue a feed update for an event; `reason` must be unique per change."""
    if cluster_id is not None:
        await enqueue(conn, FEED, [(reason, {"cluster_id": cluster_id})])


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def _r(value: float | None) -> float | None:
    return None if value is None else round(float(value), 4)


def _weighted_mean(pairs: list[tuple[float | None, float]]) -> float | None:
    pairs = [(v, w) for v, w in pairs if v is not None]
    total = sum(w for _, w in pairs)
    return sum(v * w for v, w in pairs) / total if total else None


def _mixture(distributions: list[tuple[dict[str, float], float]]) -> tuple[str, float] | None:
    """Reputation-weighted average of probability distributions; returns (argmax, prob)."""
    total = sum(w for _, w in distributions)
    if not total:
        return None
    mixed: dict[str, float] = defaultdict(float)
    for probs, w in distributions:
        for option, p in probs.items():
            mixed[option] += p * w / total
    best = max(mixed, key=lambda k: mixed[k])
    return best, mixed[best]


async def event_snapshot(
    conn: AsyncConnection, cluster_id: int, settings: Settings
) -> dict[str, Any] | None:
    """The event's current state, or None if it has no visible (non-backfill) articles."""
    latest = (
        select(ArticleVersion)
        .ext(distinct_on(ArticleVersion.article_id))
        .order_by(ArticleVersion.article_id, ArticleVersion.version_no.desc())
        .subquery("latest")
    )
    members = (
        await conn.execute(
            select(
                Article.id,
                Article.canonical_url,
                Article.first_seen_at,
                latest.c.id.label("version_id"),
                latest.c.headline,
                latest.c.summary,
                latest.c.published_at,
                Source.name.label("source"),
                Source.reputation,
            )
            .join(latest, latest.c.article_id == Article.id)
            .join(RawItem, RawItem.id == latest.c.raw_item_id)
            .join(Source, Source.id == RawItem.source_id)
            .where(Article.cluster_id == cluster_id, Article.is_backfill.is_(False))
            .order_by(Article.first_seen_at, Article.id)
        )
    ).all()
    if not members:
        return None

    version_ids = [m.version_id for m in members]
    classifications = {
        c.article_version_id: c
        for c in (
            await conn.execute(
                select(Classification)
                .where(
                    Classification.article_version_id.in_(version_ids),
                    Classification.question_set_version == settings.question_set,
                )
                .order_by(Classification.id)  # the newest per version wins
            )
        ).all()
    }
    weight = {m.version_id: max(m.reputation, MIN_WEIGHT) for m in members}
    classified = [(c, weight[v]) for v, c in classifications.items()]

    classification = None
    if classified:

        def distribution(c, question: str) -> dict[str, float]:
            answer = c.results.get("response", {}).get("answers", {}).get(question, {})
            return answer.get("probabilities") or {getattr(c, question): 1.0}

        event_type = _mixture([(distribution(c, "event_type"), w) for c, w in classified])
        domain = _mixture([(distribution(c, "domain"), w) for c, w in classified])
        classification = {
            "question_set": settings.question_set,
            "event_type": event_type[0],
            "event_type_prob": _r(event_type[1]),
            "domain": domain[0],
            **{
                name: _r(_weighted_mean([(getattr(c, column), w) for c, w in classified]))
                for name, column in SCORES.items()
            },
        }

    asset_rows = (
        await conn.execute(
            select(
                Asset.symbol,
                Asset.name,
                Asset.kind,
                func.max(ArticleAsset.relevance_prob).label("relevance"),
                func.count(func.distinct(ArticleAsset.article_version_id)).label("articles"),
            )
            .join(Asset, Asset.id == ArticleAsset.asset_id)
            .join(Classification, Classification.id == ArticleAsset.classification_id)
            .where(
                Classification.id.in_([c.id for c, _ in classified] or [-1]),
                ArticleAsset.relevance_prob >= ASSET_MIN_RELEVANCE,
            )
            .group_by(Asset.symbol, Asset.name, Asset.kind)
            .order_by(func.max(ArticleAsset.relevance_prob).desc(), Asset.symbol)
        )
    ).all()

    representative_id = (
        await conn.execute(
            select(Cluster.representative_article_id).where(Cluster.id == cluster_id)
        )
    ).scalar_one_or_none()
    rep = next((m for m in members if m.id == representative_id), members[0])
    published = [m.published_at for m in members if m.published_at]
    classified_at = [c.classified_at for c, _ in classified]
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": cluster_id,
        "status": "active",
        "headline": rep.headline,
        "summary": rep.summary,
        "url": rep.canonical_url,
        "first_published_at": _iso(min(published)) if published else None,
        "first_received_at": _iso(min(m.first_seen_at for m in members)),
        "first_classified_at": _iso(min(classified_at)) if classified_at else None,
        "sources": sorted({m.source for m in members}),
        "article_count": len(members),
        "classified_article_count": len(classified),
        "classification": classification,
        "assets": [
            {
                "symbol": a.symbol,
                "name": a.name,
                "kind": a.kind,
                "relevance": _r(a.relevance),
                "article_count": a.articles,
            }
            for a in asset_rows
        ],
        "articles": [
            {
                "article_id": m.id,
                "source": m.source,
                "url": m.canonical_url,
                "headline": m.headline,
                "published_at": _iso(m.published_at),
                "received_at": _iso(m.first_seen_at),
                "classified": m.version_id in classifications,
            }
            for m in members
        ],
    }


def content_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


async def update_event(
    conn: AsyncConnection, cluster_id: int, settings: Settings | None = None
) -> FeedRevision | None:
    """Append a revision if the event changed. Returns the new row's values, or None."""
    settings = settings or get_settings()
    await conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _FEED_LOCK_KEY})
    last = (
        await conn.execute(
            select(FeedRevision)
            .where(FeedRevision.event_id == cluster_id)
            .order_by(FeedRevision.revision.desc())
            .limit(1)
        )
    ).one_or_none()

    payload = await event_snapshot(conn, cluster_id, settings)
    if payload is None:
        if last is None or last.status == "retracted":
            return None  # never published, or already retracted
        # Its articles now belong elsewhere (e.g. after `mnp recluster`).
        former = [a["article_id"] for a in last.payload.get("articles", [])]
        superseded_by = sorted(
            {
                c
                for c in (
                    await conn.execute(
                        select(Article.cluster_id).where(
                            Article.id.in_(former or [-1]),
                            Article.cluster_id.is_not(None),
                            Article.cluster_id != cluster_id,
                        )
                    )
                ).scalars()
            }
        )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "event_id": cluster_id,
            "status": "retracted",
            "superseded_by": superseded_by,
        }

    digest = content_hash(payload)
    if last is not None and last.content_hash == digest:
        return None
    row = (
        await conn.execute(
            insert(FeedRevision)
            .values(
                event_id=cluster_id,
                revision=(last.revision + 1) if last else 1,
                status=payload["status"],
                available_at=func.clock_timestamp(),
                content_hash=digest,
                payload=payload,
            )
            .returning(FeedRevision)
        )
    ).one()
    return row


async def rebuild_all(engine: AsyncEngine) -> int:
    """Queue a feed update for every event that has, or had, visible articles."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%f")
    async with engine.begin() as conn:
        rows = await conn.execute(
            select(Article.cluster_id)
            .where(Article.cluster_id.is_not(None), Article.is_backfill.is_(False))
            .union(select(FeedRevision.event_id))
        )
        ids = sorted(set(rows.scalars()))
        for cluster_id in ids:
            await enqueue_feed(conn, cluster_id, f"rebuild:{stamp}:{cluster_id}")
    return len(ids)


async def handle_feed_job(conn: AsyncConnection, payload: dict[str, Any]) -> None:
    await update_event(conn, int(payload["cluster_id"]))
