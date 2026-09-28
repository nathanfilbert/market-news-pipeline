"""Aggregate and detail queries for the dashboard (read-only).

Article lists and details reuse mnp.outputs.queries so filters mean the same as in `mnp news`
and the API. Dates are grouped in UTC.
"""

from collections import Counter
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.models import (
    Article,
    ArticleAsset,
    ArticleVersion,
    Asset,
    Classification,
    Cluster,
    RawItem,
    Source,
)

NOUL_FIELDS = ["is_market_relevant_prob", "is_new_information_prob", "is_promotional_prob"]
SCORE_FIELDS = ["sentiment", "impact", "urgency"]


def _utc_day(column):
    return func.date(func.timezone("UTC", column))


def _first_versions():
    return (
        select(ArticleVersion.article_id, ArticleVersion.published_at, ArticleVersion.raw_item_id)
        .where(ArticleVersion.version_no == 1)
        .subquery("first")
    )


def _latest_versions():
    return (
        select(ArticleVersion.id, ArticleVersion.article_id)
        .ext(distinct_on(ArticleVersion.article_id))
        .order_by(ArticleVersion.article_id, ArticleVersion.version_no.desc())
        .subquery("latest")
    )


async def source_stats(conn: AsyncConnection) -> dict[str, dict[str, Any]]:
    """Per source: raw items, visible/backfill articles, published range."""
    raw = dict(
        (
            await conn.execute(
                select(Source.name, func.count(RawItem.id))
                .outerjoin(RawItem, RawItem.source_id == Source.id)
                .group_by(Source.name)
            )
        ).all()
    )
    first = _first_versions()
    rows = (
        await conn.execute(
            select(
                Source.name,
                func.count().filter(Article.is_backfill.is_(False)).label("articles"),
                func.count().filter(Article.is_backfill).label("backfill"),
                func.min(first.c.published_at)
                .filter(Article.is_backfill.is_(False))
                .label("oldest"),
                func.max(first.c.published_at).label("newest"),
            )
            .join(RawItem, RawItem.source_id == Source.id)
            .join(first, first.c.raw_item_id == RawItem.id)
            .join(Article, Article.id == first.c.article_id)
            .group_by(Source.name)
        )
    ).all()
    stats = {name: {"raw_items": n, "articles": 0, "backfill": 0} for name, n in raw.items()}
    for r in rows:
        stats[r.name] |= {
            "articles": r.articles,
            "backfill": r.backfill,
            "oldest": r.oldest,
            "newest": r.newest,
        }
    return stats


async def articles_per_day(
    conn: AsyncConnection, since: datetime, source: str | None = None, by: str = "first_seen"
) -> list[dict[str, Any]]:
    """Visible (non-backfill) articles per UTC day and source, by first-seen or publish date."""
    first = _first_versions()
    when = Article.first_seen_at if by == "first_seen" else first.c.published_at
    stmt = (
        select(_utc_day(when).label("day"), Source.name.label("source"), func.count().label("n"))
        .select_from(Article)
        .join(first, first.c.article_id == Article.id)
        .join(RawItem, RawItem.id == first.c.raw_item_id)
        .join(Source, Source.id == RawItem.source_id)
        .where(Article.is_backfill.is_(False), when >= since)
        .group_by("day", Source.name)
        .order_by("day")
    )
    if source:
        stmt = stmt.where(Source.name == source)
    return [dict(r._mapping) for r in (await conn.execute(stmt)).all()]


def _latest_classifications(question_set: str, classifier: str, since: datetime | None):
    """Classifications of each visible article's latest version."""
    latest = _latest_versions()
    stmt = (
        select(Classification, Article.id.label("article_id"), ArticleVersion.headline)
        .join(latest, latest.c.id == Classification.article_version_id)
        .join(Article, Article.id == latest.c.article_id)
        .join(ArticleVersion, ArticleVersion.id == latest.c.id)
        .where(
            Classification.question_set_version == question_set,
            Classification.classifier == classifier,
            Article.is_backfill.is_(False),
        )
    )
    if since:
        stmt = stmt.where(Article.first_seen_at >= since)
    return stmt


async def classification_summary(
    conn: AsyncConnection,
    question_set: str,
    classifier: str = "jev",
    since: datetime | None = None,
) -> dict[str, Any]:
    """Distributions of the denormalized answers, for charts."""
    rows = (await conn.execute(_latest_classifications(question_set, classifier, since))).all()

    def histogram(values: list[float], low: float, high: float, bins: int = 10) -> list[int]:
        counts = [0] * bins
        for v in values:
            if v is not None:
                i = int((v - low) / (high - low) * bins)
                counts[min(max(i, 0), bins - 1)] += 1
        return counts

    return {
        "total": len(rows),
        "event_types": Counter(r.event_type for r in rows).most_common(),
        "domains": Counter(r.domain for r in rows).most_common(),
        "scatter": [
            {"x": r.sentiment, "y": r.impact, "id": r.article_id, "label": r.headline[:90]}
            for r in rows
            if r.sentiment is not None and r.impact is not None
        ],
        "histograms": {
            **{f: histogram([getattr(r, f) for r in rows], 0, 1) for f in NOUL_FIELDS},
            "sentiment": histogram([r.sentiment for r in rows], -1, 1),
            "impact": histogram([r.impact for r in rows], 0, 1),
            "urgency": histogram([r.urgency for r in rows], 0, 1),
        },
    }


async def question_sets_in_use(conn: AsyncConnection) -> list[dict[str, Any]]:
    rows = await conn.execute(
        select(
            Classification.question_set_version,
            Classification.classifier,
            func.count(),
            func.max(Classification.classified_at),
        )
        .group_by(Classification.question_set_version, Classification.classifier)
        .order_by(Classification.question_set_version)
    )
    return [
        {"question_set": qs, "classifier": c, "count": n, "latest": at}
        for qs, c, n, at in rows.all()
    ]


async def compare_question_sets(
    conn: AsyncConnection, a: str, b: str, classifier: str = "jev", limit: int = 25
) -> dict[str, Any]:
    """Versions classified under both sets: agreement and the biggest disagreements."""
    ca, cb = Classification.__table__.alias("a"), Classification.__table__.alias("b")
    rows = (
        await conn.execute(
            select(
                ca,
                cb.c.event_type.label("b_event_type"),
                *(cb.c[f].label(f"b_{f}") for f in NOUL_FIELDS + SCORE_FIELDS),
                ArticleVersion.headline,
                ArticleVersion.article_id,
            )
            .join(
                cb,
                and_(
                    cb.c.article_version_id == ca.c.article_version_id,
                    cb.c.classifier == ca.c.classifier,
                ),
            )
            .join(ArticleVersion, ArticleVersion.id == ca.c.article_version_id)
            .where(
                ca.c.question_set_version == a,
                cb.c.question_set_version == b,
                ca.c.classifier == classifier,
            )
        )
    ).all()
    if not rows:
        return {"count": 0}
    fields = NOUL_FIELDS + SCORE_FIELDS
    mean_abs_diff = {
        f: sum(abs((getattr(r, f"b_{f}") or 0) - (getattr(r, f) or 0)) for r in rows) / len(rows)
        for f in fields
    }

    def spread(r) -> float:
        return sum(abs((getattr(r, f"b_{f}") or 0) - (getattr(r, f) or 0)) for f in fields)

    changed = sorted(rows, key=spread, reverse=True)[:limit]
    return {
        "count": len(rows),
        "event_type_agreement": sum(r.event_type == r.b_event_type for r in rows) / len(rows),
        "mean_abs_diff": mean_abs_diff,
        "biggest_changes": [
            {
                "article_id": r.article_id,
                "headline": r.headline,
                "event_type": (r.event_type, r.b_event_type),
                **{f: (getattr(r, f), getattr(r, f"b_{f}")) for f in fields},
            }
            for r in changed
        ],
    }


async def source_raw_items(conn: AsyncConnection, source: str, limit: int = 25) -> list[Any]:
    return (
        await conn.execute(
            select(
                RawItem.id,
                RawItem.fetched_at,
                RawItem.external_id,
                RawItem.url,
                RawItem.payload["format"].astext.label("format"),
            )
            .join(Source, Source.id == RawItem.source_id)
            .where(Source.name == source)
            .order_by(RawItem.id.desc())
            .limit(limit)
        )
    ).all()


async def source_row(conn: AsyncConnection, name: str) -> Any | None:
    return (await conn.execute(select(Source).where(Source.name == name))).one_or_none()


async def raw_item_outputs(conn: AsyncConnection, raw_item_id: int) -> list[Any]:
    """Article versions produced from a raw item."""
    return (
        await conn.execute(
            select(ArticleVersion.article_id, ArticleVersion.version_no, ArticleVersion.headline)
            .where(ArticleVersion.raw_item_id == raw_item_id)
            .order_by(ArticleVersion.id)
        )
    ).all()


async def classification_detail(conn: AsyncConnection, classification_id: int) -> Any | None:
    return (
        await conn.execute(
            select(
                Classification,
                ArticleVersion.article_id,
                ArticleVersion.version_no,
                ArticleVersion.headline,
            )
            .join(ArticleVersion, ArticleVersion.id == Classification.article_version_id)
            .where(Classification.id == classification_id)
        )
    ).one_or_none()


async def classification_assets(conn: AsyncConnection, classification_id: int) -> list[Any]:
    return (
        await conn.execute(
            select(
                Asset.symbol, Asset.name, ArticleAsset.relevance_prob, ArticleAsset.candidate_via
            )
            .join(Asset, Asset.id == ArticleAsset.asset_id)
            .where(ArticleAsset.classification_id == classification_id)
            .order_by(ArticleAsset.relevance_prob.desc())
        )
    ).all()


async def clusters(
    conn: AsyncConnection, since: datetime, min_size: int = 2, limit: int = 100
) -> list[Any]:
    """Recent multi-article clusters with their sources and representative headline."""
    first = _first_versions()
    members = (
        select(
            Article.cluster_id,
            func.count().label("size"),
            func.array_agg(func.distinct(Source.name)).label("sources"),
            func.max(Article.first_seen_at).label("last_seen"),
        )
        .join(first, first.c.article_id == Article.id)
        .join(RawItem, RawItem.id == first.c.raw_item_id)
        .join(Source, Source.id == RawItem.source_id)
        .where(Article.cluster_id.is_not(None))
        .group_by(Article.cluster_id)
        .having(func.count() >= min_size)
        .subquery("members")
    )
    rep = ArticleVersion.__table__.alias("rep")
    return (
        await conn.execute(
            select(
                Cluster.id,
                Cluster.first_seen_at,
                members.c.size,
                members.c.sources,
                members.c.last_seen,
                rep.c.headline,
            )
            .join(members, members.c.cluster_id == Cluster.id)
            .join(
                rep,
                and_(rep.c.article_id == Cluster.representative_article_id, rep.c.version_no == 1),
            )
            .where(members.c.last_seen >= since)
            .order_by(members.c.last_seen.desc())
            .limit(limit)
        )
    ).all()


def days_between(since: datetime, until: datetime) -> list[str]:
    return [(since + timedelta(days=i)).date().isoformat() for i in range((until - since).days + 1)]
