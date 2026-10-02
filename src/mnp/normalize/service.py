"""The normalize job: raw item -> article (by canonical URL) -> version (by content hash).

Sentiment payloads (mnp.sentiment) become sentiment readings instead of articles. Items not
in their source's language (mnp.normalize.language) are skipped: the raw item stays, no
article is made.
"""

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import numpy as np
from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.classify.service import enqueue_classify
from mnp.config import Settings, get_settings
from mnp.feed.build import enqueue_feed
from mnp.jobs import PermanentJobError
from mnp.models import Article, ArticleEmbedding, ArticleVersion, RawItem, Source
from mnp.normalize.canonical_url import canonicalize_url
from mnp.normalize.cluster import assign_cluster, assign_cluster_by_embedding
from mnp.normalize.embeddings import embed_texts, embedding_text, get_embedder
from mnp.normalize.hashing import content_hash
from mnp.normalize.item import UnparseableItem, parse_payload
from mnp.normalize.language import is_off_language
from mnp.normalize.same_event import SameEventJudge, default_judge, judge_view
from mnp.sentiment import store_sentiment_reading

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class NormalizeOutcome:
    article_id: int
    new_article: bool
    version_no: int | None  # None: content already stored, nothing written
    cluster_id: int | None


_DEFAULT: Any = object()  # judge not given: use default_judge(settings)


async def normalize_raw_item(
    conn: AsyncConnection,
    raw_item_id: int,
    settings: Settings | None = None,
    judge: SameEventJudge | None = _DEFAULT,
) -> NormalizeOutcome | None:
    """Idempotent: running it again for the same raw item writes nothing.

    Backfill articles (published long before we first saw them) are stored but not classified,
    and get a cluster of their own. Returns None for an item not in its source's language.
    """
    settings = settings or get_settings()
    raw = (
        await conn.execute(
            select(
                RawItem.payload,
                RawItem.url,
                RawItem.external_id,
                RawItem.fetched_at,
                RawItem.source_id,
                Source.name.label("source_name"),
                Source.language,
            )
            .join(Source, Source.id == RawItem.source_id)
            .where(RawItem.id == raw_item_id)
        )
    ).one_or_none()
    if raw is None:
        raise PermanentJobError(f"raw item {raw_item_id} not found")
    try:
        parsed = parse_payload(raw.payload)
    except UnparseableItem as exc:
        raise PermanentJobError(str(exc)) from exc
    if detected := is_off_language(
        f"{parsed.headline or ''} {parsed.summary or ''}", parsed.language or raw.language
    ):
        log.info("skipping raw item %d from %s: in %s", raw_item_id, raw.source_name, detected)
        return None

    ext_id = raw.external_id or ""
    url = parsed.url or raw.url or (ext_id if ext_id.startswith(("http://", "https://")) else None)
    if not url:
        raise PermanentJobError("item has no URL")
    canonical_url = canonicalize_url(url)
    headline = parsed.headline or ""
    chash = content_hash(
        headline=headline, summary=parsed.summary, body=parsed.body, canonical_url=canonical_url
    )

    # Old news we are only now seeing: decided when the article is first created.
    is_backfill = parsed.published_at is not None and (
        raw.fetched_at - parsed.published_at > timedelta(days=settings.backfill_after_days)
    )
    article_id = (
        await conn.execute(
            pg_insert(Article)
            .values(
                canonical_url=canonical_url,
                first_seen_at=raw.fetched_at,
                is_backfill=is_backfill,
            )
            .on_conflict_do_nothing()
            .returning(Article.id)
        )
    ).scalar_one_or_none()
    new_article = article_id is not None
    if not new_article:
        # Row lock serializes version numbering for this article.
        article_id, is_backfill = (
            await conn.execute(
                select(Article.id, Article.is_backfill)
                .where(Article.canonical_url == canonical_url)
                .with_for_update()
            )
        ).one()
        await conn.execute(
            update(Article)
            .where(Article.id == article_id)
            .values(first_seen_at=func.least(Article.first_seen_at, raw.fetched_at))
        )

    exists = (
        await conn.execute(
            select(ArticleVersion.id).where(
                ArticleVersion.article_id == article_id, ArticleVersion.content_hash == chash
            )
        )
    ).first()
    if exists:
        return NormalizeOutcome(article_id, new_article=False, version_no=None, cluster_id=None)

    version_no = (
        await conn.execute(
            select(func.coalesce(func.max(ArticleVersion.version_no), 0) + 1).where(
                ArticleVersion.article_id == article_id
            )
        )
    ).scalar_one()
    version_id = (
        await conn.execute(
            insert(ArticleVersion)
            .values(
                article_id=article_id,
                version_no=version_no,
                content_hash=chash,
                headline=headline,
                summary=parsed.summary,
                body=parsed.body,
                author=parsed.author,
                language=parsed.language or raw.language,
                published_at=parsed.published_at,
                received_at=raw.fetched_at,
                raw_item_id=raw_item_id,
            )
            .returning(ArticleVersion.id)
        )
    ).scalar_one()
    if not is_backfill:
        await enqueue_classify(conn, [version_id], settings.question_set)
    if not new_article:  # e.g. an edited headline: the event's feed record changes
        cluster = (
            await conn.execute(select(Article.cluster_id).where(Article.id == article_id))
        ).scalar_one()
        await enqueue_feed(conn, cluster, f"version:{version_id}")

    vector = None
    if settings.cluster_method == "embedding":
        vector = await store_embedding(conn, version_id, headline, parsed.summary, settings)

    cluster_id = None
    if new_article:
        anchor = parsed.published_at or raw.fetched_at
        if vector is not None and not is_backfill:
            decision = await assign_cluster_by_embedding(
                conn,
                article_id=article_id,
                source_id=raw.source_id,
                vector=vector,
                view=judge_view(raw.source_name, headline, parsed.summary),
                anchor=anchor,
                model=get_embedder(settings.embedding_model, settings.embedding_cache_dir).name,
                settings=settings,
                judge=default_judge(settings) if judge is _DEFAULT else judge,
            )
            cluster_id = decision.cluster_id
        else:
            cluster_id = await assign_cluster(
                conn,
                article_id=article_id,
                source_id=raw.source_id,
                headline=headline,
                seen_at=raw.fetched_at,
                threshold=settings.cluster_similarity_threshold,
                window=timedelta(hours=settings.cluster_window_hours),
                isolated=is_backfill,
            )
    return NormalizeOutcome(article_id, new_article, version_no, cluster_id)


async def store_embedding(
    conn: AsyncConnection,
    version_id: int,
    headline: str,
    summary: str | None,
    settings: Settings,
) -> np.ndarray:
    """Embed a version's headline + summary and store it (idempotent)."""
    embedder = get_embedder(settings.embedding_model, settings.embedding_cache_dir)
    [vector] = await embed_texts(embedder, [embedding_text(headline, summary)])
    await conn.execute(
        pg_insert(ArticleEmbedding)
        .values(article_version_id=version_id, model=embedder.name, vector=vector.tolist())
        .on_conflict_do_nothing()
    )
    return vector


async def handle_normalize_job(conn: AsyncConnection, payload: dict[str, Any]) -> None:
    raw_item_id = int(payload["raw_item_id"])
    if not await store_sentiment_reading(conn, raw_item_id):
        await normalize_raw_item(conn, raw_item_id)
