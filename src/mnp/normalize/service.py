"""The normalize job: raw item -> article (by canonical URL) -> version (by content hash)."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.classify.service import enqueue_classify
from mnp.config import Settings, get_settings
from mnp.jobs import PermanentJobError
from mnp.models import Article, ArticleVersion, RawItem, Source
from mnp.normalize.canonical_url import canonicalize_url
from mnp.normalize.cluster import assign_cluster
from mnp.normalize.hashing import content_hash
from mnp.normalize.item import UnparseableItem, parse_payload


@dataclass(frozen=True, slots=True)
class NormalizeOutcome:
    article_id: int
    new_article: bool
    version_no: int | None  # None: content already stored, nothing written
    cluster_id: int | None


async def normalize_raw_item(
    conn: AsyncConnection, raw_item_id: int, settings: Settings | None = None
) -> NormalizeOutcome:
    """Idempotent: running it again for the same raw item writes nothing.

    Backfill articles (published long before we first saw them) are stored but not classified,
    and get a cluster of their own.
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

    cluster_id = None
    if new_article:
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


async def handle_normalize_job(conn: AsyncConnection, payload: dict[str, Any]) -> None:
    await normalize_raw_item(conn, int(payload["raw_item_id"]))
