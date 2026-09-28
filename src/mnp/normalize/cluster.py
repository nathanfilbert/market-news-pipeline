"""Near-duplicate clustering: which story (cluster) a new article belongs to.

Two methods (Settings.cluster_method):

- embedding (default, v1.2): cosine similarity of headline+summary embeddings. At or above
  `cluster_join_similarity` the article joins; between `cluster_confirm_similarity` and that, Jev
  answers "same event?" (join if yes-probability >= `cluster_confirm_min_prob`); if Jev is
  unavailable, `cluster_fallback_similarity` decides.
- trigram (v1): pg_trgm headline similarity; kept for comparison (`mnp cluster-eval`).

Both only match articles from *other* sources. Within one source, similar headlines are almost
always distinct events from a template ("Federal Reserve Board announces approval of
application by <bank>"), and merging them would hide events. Backfill articles (old news first
seen now) get a cluster of their own and are never matched: first_seen_at says nothing about
when their story happened. Time windows are anchored on publish time where known.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
from sqlalchemy import and_, func, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.classify.base import ClassifierUnavailable
from mnp.config import Settings
from mnp.feed.build import enqueue_feed
from mnp.models import Article, ArticleEmbedding, ArticleVersion, Cluster, RawItem, Source
from mnp.normalize.same_event import SameEventJudge, judge_view

log = logging.getLogger(__name__)
MAX_JUDGED_CLUSTERS = 2

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

    return await _join_or_create(
        conn, article_id, seen_at, match, "isolated" if isolated else "trigram"
    )


async def _join_or_create(
    conn: AsyncConnection,
    article_id: int,
    seen_at: datetime,
    cluster_id: int | None,
    method: str,
) -> int:
    if cluster_id is None:
        cluster_id = (
            await conn.execute(
                insert(Cluster)
                .values(first_seen_at=seen_at, representative_article_id=article_id)
                .returning(Cluster.id)
            )
        ).scalar_one()
    else:
        await conn.execute(
            update(Cluster)
            .where(Cluster.id == cluster_id)
            .values(first_seen_at=func.least(Cluster.first_seen_at, seen_at))
        )
    await conn.execute(
        update(Article)
        .where(Article.id == article_id)
        .values(cluster_id=cluster_id, clustered_at=func.now(), cluster_method=method)
    )
    if method != "isolated":  # backfill (old news) never enters the feed
        await enqueue_feed(conn, cluster_id, f"join:{article_id}:{cluster_id}")
    return cluster_id


@dataclass(frozen=True, slots=True)
class ClusterDecision:
    cluster_id: int
    joined: bool  # False: a new cluster was created
    similarity: float | None = None  # best candidate's cosine similarity
    judged: list[float] | None = None  # Jev yes-probabilities asked, in order
    fallback: bool = False  # Jev was unavailable; the fallback threshold decided


async def assign_cluster_by_embedding(
    conn: AsyncConnection,
    *,
    article_id: int,
    source_id: int,
    vector: np.ndarray,
    view: dict,
    anchor: datetime,
    model: str,
    settings: Settings,
    judge: SameEventJudge | None,
) -> ClusterDecision:
    """Put a new (non-backfill) article into the best matching cluster, or a new one."""
    await conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _CLUSTER_LOCK_KEY})
    window = timedelta(hours=settings.cluster_window_hours)
    when = func.coalesce(ArticleVersion.published_at, Article.first_seen_at)
    rows = (
        await conn.execute(
            select(
                Article.cluster_id,
                ArticleEmbedding.vector,
                ArticleVersion.headline,
                ArticleVersion.summary,
                Source.name.label("source"),
            )
            .join(ArticleVersion, ArticleVersion.article_id == Article.id)
            .join(
                ArticleEmbedding,
                and_(
                    ArticleEmbedding.article_version_id == ArticleVersion.id,
                    ArticleEmbedding.model == model,
                ),
            )
            .join(RawItem, RawItem.id == ArticleVersion.raw_item_id)
            .join(Source, Source.id == RawItem.source_id)
            .where(
                Article.id != article_id,
                Article.cluster_id.is_not(None),
                Article.is_backfill.is_(False),
                RawItem.source_id != source_id,
                when.between(anchor - window, anchor + window),
            )
        )
    ).all()

    best: dict[int, tuple[float, dict]] = {}  # cluster -> (similarity, candidate view)
    if rows:
        sims = np.array([r.vector for r in rows], dtype=np.float32) @ vector
        for r, sim in zip(rows, sims.tolist(), strict=True):
            if r.cluster_id not in best or sim > best[r.cluster_id][0]:
                best[r.cluster_id] = (sim, judge_view(r.source, r.headline, r.summary))
    ranked = sorted(best.items(), key=lambda kv: kv[1][0], reverse=True)

    chosen: int | None = None
    judged: list[float] = []
    fallback = False
    for cluster_id, (sim, other) in ranked:
        if sim >= settings.cluster_join_similarity:
            chosen = cluster_id
            break
        if sim < settings.cluster_confirm_similarity or len(judged) >= MAX_JUDGED_CLUSTERS:
            break
        prob = None
        if judge is not None and not fallback:
            try:
                prob = await judge.same_event(view, other)
            except ClassifierUnavailable as exc:
                log.warning("same-event check unavailable, using fallback threshold: %s", exc)
                fallback = True
        if prob is not None:
            judged.append(prob)
            if prob >= settings.cluster_confirm_min_prob:
                chosen = cluster_id
                break
        else:
            fallback = True
            if sim >= settings.cluster_fallback_similarity:
                chosen = cluster_id
            break

    cluster_id = await _join_or_create(conn, article_id, anchor, chosen, "embedding")
    return ClusterDecision(
        cluster_id=cluster_id,
        joined=chosen is not None,
        similarity=ranked[0][1][0] if ranked else None,
        judged=judged or None,
        fallback=fallback,
    )
