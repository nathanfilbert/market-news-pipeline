"""Near-duplicate clustering (v1): headline trigram similarity within a time window.

Only articles from *other* sources are matched, and never backfill articles (old news first
seen now), which also get a cluster of their own: first_seen_at says nothing about when their
story happened. Within one source, similar headlines are
almost always distinct events from a template ("Federal Reserve Board announces approval of
application by <bank>"), and merging them would suppress per-cluster alerts.

Kept in one module so it can be replaced by embedding similarity later.
"""

from datetime import datetime, timedelta

from sqlalchemy import func, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.models import Article, ArticleVersion, Cluster, RawItem

# Serializes cluster assignment so two concurrent workers can't both open a cluster for the
# same story. Arbitrary constant, unique within this database.
_CLUSTER_LOCK_KEY = 7_214_001


async def assign_cluster(
    conn: AsyncConnection,
    *,
    article_id: int,
    source_id: int,
    headline: str,
    seen_at: datetime,
    threshold: float,
    window: timedelta,
    isolated: bool = False,
) -> int:
    """Put a new article into the most similar recent cluster, or a new one. Returns its id."""
    await conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _CLUSTER_LOCK_KEY})

    match = None
    if headline and not isolated:
        # `%` uses the trigram index with this threshold; similarity() re-checks and ranks.
        await conn.execute(
            text("SELECT set_config('pg_trgm.similarity_threshold', :t, true)"),
            {"t": str(threshold)},
        )
        similarity = func.similarity(ArticleVersion.headline, headline)
        match = (
            await conn.execute(
                select(Article.cluster_id)
                .join(ArticleVersion, ArticleVersion.article_id == Article.id)
                .join(RawItem, RawItem.id == ArticleVersion.raw_item_id)
                .where(
                    ArticleVersion.headline.op("%")(headline),
                    similarity >= threshold,
                    Article.id != article_id,
                    RawItem.source_id != source_id,
                    Article.cluster_id.is_not(None),
                    Article.is_backfill.is_(False),
                    Article.first_seen_at.between(seen_at - window, seen_at + window),
                )
                .order_by(similarity.desc(), Article.first_seen_at)
                .limit(1)
            )
        ).scalar_one_or_none()

    if match is None:
        cluster_id = (
            await conn.execute(
                insert(Cluster)
                .values(first_seen_at=seen_at, representative_article_id=article_id)
                .returning(Cluster.id)
            )
        ).scalar_one()
    else:
        cluster_id = match
        await conn.execute(
            update(Cluster)
            .where(Cluster.id == cluster_id)
            .values(first_seen_at=func.least(Cluster.first_seen_at, seen_at))
        )
    await conn.execute(
        update(Article).where(Article.id == article_id).values(cluster_id=cluster_id)
    )
    return cluster_id
