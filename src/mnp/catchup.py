"""`mnp catch-up`: a one-shot counterpart to `mnp run` for history and gaps.

Fetches everything each source currently offers (paging back where the source supports it),
processes the normalize and classify queues to completion, then reports coverage per source so
remaining gaps are visible. Feeds only hold their latest items, so how far back this reaches
depends on the source: about 2-3 days for the crypto feeds, weeks for official and CNBC feeds.

Articles published inside the requested window are wanted history: any that were flagged as
backfill (old news at first sight) are un-flagged and classified. Older ones stay flagged.
Safe to run while `mnp run` is running.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from mnp.classify.assets import load_assets, sync_assets
from mnp.classify.jev import JevClassifier
from mnp.classify.service import ClassifyHandler, enqueue_classify
from mnp.collectors.base import CollectorUnavailable, make_http_client
from mnp.collectors.service import CollectResult, collect_history, make_collector, sync_sources
from mnp.config import Settings, SourceConfig, load_sources
from mnp.jobs import CLASSIFY, NORMALIZE, JobStats, run_pending
from mnp.models import Article, ArticleVersion, RawItem, Source
from mnp.normalize.service import handle_normalize_job

log = logging.getLogger(__name__)


@dataclass
class SourceCoverage:
    source: str
    articles: int
    oldest: datetime | None
    empty_days: list[date]  # days in the window with no articles published


@dataclass
class CatchUpReport:
    since: datetime
    fetched: list[CollectResult] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    normalize: JobStats = field(default_factory=JobStats)
    adopted: int = 0
    classify: JobStats | None = None  # None: not run (no JEV_API_KEY or --no-classify)
    coverage: list[SourceCoverage] = field(default_factory=list)


def _first_versions():
    return (
        select(ArticleVersion.article_id, ArticleVersion.published_at, ArticleVersion.raw_item_id)
        .where(ArticleVersion.version_no == 1)
        .subquery("first")
    )


async def adopt_history(engine: AsyncEngine, since: datetime, question_set: str) -> int:
    """Un-flag backfill articles first published at or after `since`; queue them to classify."""
    first = _first_versions()
    async with engine.begin() as conn:
        adopted = list(
            (
                await conn.execute(
                    update(Article)
                    .where(
                        Article.is_backfill,
                        Article.id == first.c.article_id,
                        first.c.published_at >= since,
                    )
                    .values(is_backfill=False)
                    .returning(Article.id)
                )
            ).scalars()
        )
        if adopted:
            latest = (
                select(func.max(ArticleVersion.id))
                .where(ArticleVersion.article_id.in_(adopted))
                .group_by(ArticleVersion.article_id)
            )
            version_ids = list((await conn.execute(latest)).scalars())
            await enqueue_classify(conn, version_ids, question_set)
    return len(adopted)


async def coverage(
    engine: AsyncEngine, since: datetime, sources: list[str], now: datetime | None = None
) -> list[SourceCoverage]:
    """Per source: visible articles published in the window, the oldest, and empty days."""
    now = now or datetime.now(UTC)
    first = _first_versions()
    day = func.date(func.timezone("UTC", first.c.published_at))
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                select(
                    Source.name,
                    day.label("day"),
                    func.count().label("n"),
                    func.min(first.c.published_at).label("oldest"),
                )
                .join(RawItem, RawItem.source_id == Source.id)
                .join(first, first.c.raw_item_id == RawItem.id)
                .join(
                    Article, and_(Article.id == first.c.article_id, Article.is_backfill.is_(False))
                )
                .where(Source.name.in_(sources), first.c.published_at >= since)
                .group_by(Source.name, day)
            )
        ).all()
    days = [(since + timedelta(days=i)).date() for i in range((now.date() - since.date()).days + 1)]
    result = []
    for name in sources:
        mine = [r for r in rows if r.name == name]
        covered = {r.day for r in mine}
        result.append(
            SourceCoverage(
                source=name,
                articles=sum(r.n for r in mine),
                oldest=min((r.oldest for r in mine), default=None),
                empty_days=[d for d in days if d not in covered],
            )
        )
    return result


async def catch_up(
    settings: Settings,
    engine: AsyncEngine,
    *,
    since: datetime,
    source_names: list[str] | None = None,
    classify: bool = True,
) -> CatchUpReport:
    report = CatchUpReport(since=since)
    configs = load_sources()
    ids = await sync_sources(engine, configs)
    await sync_assets(engine, load_assets())
    selected: list[SourceConfig] = [
        c for c in configs if (c.name in source_names if source_names is not None else c.enabled)
    ]

    async with make_http_client(settings) as client:
        jobs = []
        for cfg in selected:
            try:
                jobs.append((ids[cfg.name], make_collector(cfg, client, settings)))
            except CollectorUnavailable as exc:
                report.skipped[cfg.name] = str(exc)
        report.fetched = list(
            await asyncio.gather(*(collect_history(engine, i, c, since) for i, c in jobs))
        )

        report.normalize = await run_pending(engine, NORMALIZE, handle_normalize_job)
        report.adopted = await adopt_history(engine, since, settings.question_set)

        if classify and settings.jev_api_key is not None:
            classifier = JevClassifier(
                client,
                settings.jev_api_key.get_secret_value(),
                model=settings.jev_model,
                url=settings.jev_url,
            )
            report.classify = await run_pending(engine, CLASSIFY, ClassifyHandler(classifier))

    report.coverage = await coverage(engine, since, [c.name for c in selected])
    return report
