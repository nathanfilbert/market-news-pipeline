"""Rebuild clusters for stored articles, and evaluate clustering on labelled pairs.

Clusters are derived data: `recluster` clears the assignments in scope and replays the
articles in publish-time order, as if the pipeline had seen them live, so each article can
only join a story that was already there. Every assignment records `clustered_at` (now) and
`cluster_method`, so consumers replaying history can tell rebuilt assignments from live ones.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import yaml
from sqlalchemy import delete, exists, func, literal, select, text, update
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.ext.asyncio import AsyncEngine

from mnp.config import Settings
from mnp.models import Article, ArticleEmbedding, ArticleVersion, Cluster, RawItem, Source
from mnp.normalize.cluster import (
    _CLUSTER_LOCK_KEY,
    assign_cluster,
    assign_cluster_by_embedding,
)
from mnp.normalize.embeddings import embed_texts, embedding_text, get_embedder
from mnp.normalize.same_event import SameEventJudge, judge_view

log = logging.getLogger(__name__)
EMBED_BATCH = 256


@dataclass
class ReclusterReport:
    articles: int = 0
    embedded: int = 0
    joined: int = 0
    new_clusters: int = 0
    judged: int = 0
    fallbacks: int = 0
    multi_source_clusters: int = 0


def _latest_versions():
    return (
        select(
            ArticleVersion.id,
            ArticleVersion.article_id,
            ArticleVersion.headline,
            ArticleVersion.summary,
            ArticleVersion.raw_item_id,
        )
        .ext(distinct_on(ArticleVersion.article_id))
        .order_by(ArticleVersion.article_id, ArticleVersion.version_no.desc())
        .subquery("latest")
    )


async def ensure_embeddings(engine: AsyncEngine, settings: Settings) -> int:
    """Embed every article version that has no vector for the configured model yet."""
    embedder = get_embedder(settings.embedding_model, settings.embedding_cache_dir)
    total = 0
    while True:
        async with engine.begin() as conn:
            missing = (
                await conn.execute(
                    select(ArticleVersion.id, ArticleVersion.headline, ArticleVersion.summary)
                    .where(
                        ~exists().where(
                            ArticleEmbedding.article_version_id == ArticleVersion.id,
                            ArticleEmbedding.model == embedder.name,
                        )
                    )
                    .limit(EMBED_BATCH)
                )
            ).all()
            if not missing:
                return total
            vectors = await embed_texts(
                embedder, [embedding_text(r.headline, r.summary) for r in missing]
            )
            await conn.execute(
                ArticleEmbedding.__table__.insert(),
                [
                    {"article_version_id": r.id, "model": embedder.name, "vector": v.tolist()}
                    for r, v in zip(missing, vectors, strict=True)
                ],
            )
            total += len(missing)


async def recluster(
    engine: AsyncEngine,
    settings: Settings,
    *,
    since: datetime | None = None,
    judge: SameEventJudge | None = None,
) -> ReclusterReport:
    report = ReclusterReport()
    method = settings.cluster_method
    if method == "embedding":
        report.embedded = await ensure_embeddings(engine, settings)
    model = get_embedder(settings.embedding_model, settings.embedding_cache_dir).name

    latest = _latest_versions()
    first = (
        select(ArticleVersion.article_id, ArticleVersion.published_at, ArticleVersion.raw_item_id)
        .where(ArticleVersion.version_no == 1)
        .subquery("first")
    )
    anchor = func.coalesce(first.c.published_at, Article.first_seen_at).label("anchor")
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _CLUSTER_LOCK_KEY})
        stmt = (
            select(
                Article.id,
                Article.is_backfill,
                Article.first_seen_at,
                anchor,
                RawItem.source_id,
                Source.name.label("source"),
                latest.c.headline,
                latest.c.summary,
                ArticleEmbedding.vector,
            )
            .join(first, first.c.article_id == Article.id)
            .join(RawItem, RawItem.id == first.c.raw_item_id)
            .join(Source, Source.id == RawItem.source_id)
            .join(latest, latest.c.article_id == Article.id)
            .outerjoin(
                ArticleEmbedding,
                (ArticleEmbedding.article_version_id == latest.c.id)
                & (ArticleEmbedding.model == literal(model)),
            )
            .order_by(anchor, Article.id)
        )
        if since is not None:
            stmt = stmt.where(anchor >= since)
        articles = (await conn.execute(stmt)).all()
        ids = [a.id for a in articles]
        report.articles = len(ids)
        if not ids:
            return report

        await conn.execute(
            update(Article)
            .where(Article.id.in_(ids))
            .values(cluster_id=None, clustered_at=None, cluster_method=None)
        )
        await conn.execute(delete(Cluster).where(~exists().where(Article.cluster_id == Cluster.id)))

        for a in articles:
            if a.is_backfill or method == "trigram" or a.vector is None:
                await assign_cluster(
                    conn,
                    article_id=a.id,
                    source_id=a.source_id,
                    headline=a.headline,
                    seen_at=a.anchor,
                    threshold=settings.cluster_similarity_threshold,
                    window=_window(settings),
                    isolated=a.is_backfill,
                )
                continue
            decision = await assign_cluster_by_embedding(
                conn,
                article_id=a.id,
                source_id=a.source_id,
                vector=np.array(a.vector, dtype=np.float32),
                view=judge_view(a.source, a.headline, a.summary),
                anchor=a.anchor,
                model=model,
                settings=settings,
                judge=judge,
            )
            report.joined += decision.joined
            report.judged += len(decision.judged or [])
            report.fallbacks += decision.fallback
        report.new_clusters = report.articles - report.joined

        report.multi_source_clusters = (
            await conn.execute(
                select(func.count()).select_from(
                    select(Article.cluster_id)
                    .where(Article.id.in_(ids), Article.cluster_id.is_not(None))
                    .group_by(Article.cluster_id)
                    .having(func.count() > 1)
                    .subquery()
                )
            )
        ).scalar_one()
    return report


def _window(settings: Settings) -> timedelta:
    return timedelta(hours=settings.cluster_window_hours)


# --- evaluation ------------------------------------------------------------------------------


@dataclass
class PairScore:
    a: str
    b: str
    same: bool
    embedding: float
    trigram: float
    jev: float | None = None


@dataclass
class EvalReport:
    pairs: list[PairScore] = field(default_factory=list)
    missing: int = 0  # labelled pairs whose articles aren't in this database

    @property
    def positives(self) -> int:
        return sum(p.same for p in self.pairs)

    def rate(self, predicted: list[bool]) -> tuple[float, float, int, int]:
        """(recall, precision, true positives, predicted positives)."""
        tp = sum(p.same and hit for p, hit in zip(self.pairs, predicted, strict=True))
        n = sum(predicted)
        return (tp / self.positives if self.positives else 0.0, tp / n if n else 1.0, tp, n)


def load_pairs(path: Path) -> list[dict[str, Any]]:
    data = yaml.safe_load(path.read_text()) or {}
    return [p for p in data.get("pairs", []) if p.get("label") in ("same", "related", "different")]


async def evaluate_pairs(
    engine: AsyncEngine,
    settings: Settings,
    pairs: list[dict[str, Any]],
    judge: SameEventJudge | None = None,
) -> EvalReport:
    """Score labelled pairs with both methods (and the same-event judge, if given)."""
    latest = _latest_versions()
    urls = {p["a"] for p in pairs} | {p["b"] for p in pairs}
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                select(
                    Article.canonical_url,
                    latest.c.headline,
                    latest.c.summary,
                    Source.name.label("source"),
                )
                .join(latest, latest.c.article_id == Article.id)
                .join(RawItem, RawItem.id == latest.c.raw_item_id)
                .join(Source, Source.id == RawItem.source_id)
                .where(Article.canonical_url.in_(urls))
            )
        ).all()
        found = {r.canonical_url: r for r in rows}
        present = [p for p in pairs if p["a"] in found and p["b"] in found]
        trigram = [
            (
                await conn.execute(
                    select(func.similarity(found[p["a"]].headline, found[p["b"]].headline))
                )
            ).scalar_one()
            for p in present
        ]
    embedder = get_embedder(settings.embedding_model, settings.embedding_cache_dir)
    order = sorted(found)
    vectors = await embed_texts(
        embedder, [embedding_text(found[u].headline, found[u].summary) for u in order]
    )
    index = {u: i for i, u in enumerate(order)}
    report = EvalReport(missing=len(pairs) - len(present))
    for p, tri in zip(present, trigram, strict=True):
        sim = float(vectors[index[p["a"]]] @ vectors[index[p["b"]]])
        score = PairScore(p["a"], p["b"], p["label"] == "same", sim, float(tri))
        if judge is not None:
            a, b = found[p["a"]], found[p["b"]]
            score.jev = await judge.same_event(
                judge_view(a.source, a.headline, a.summary),
                judge_view(b.source, b.headline, b.summary),
            )
        report.pairs.append(score)
    return report


def hybrid_predictions(report: EvalReport, settings: Settings) -> list[bool]:
    """What the embedding method would decide for each pair with the current settings."""
    out = []
    for p in report.pairs:
        if p.embedding >= settings.cluster_join_similarity:
            out.append(True)
        elif p.embedding < settings.cluster_confirm_similarity:
            out.append(False)
        elif p.jev is not None:
            out.append(p.jev >= settings.cluster_confirm_min_prob)
        else:
            out.append(p.embedding >= settings.cluster_fallback_similarity)
    return out
