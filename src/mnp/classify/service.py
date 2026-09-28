"""The classify job: one classification per (article version, classifier, question set)."""

from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from mnp.classify.assets import AssetMatcher, load_matcher
from mnp.classify.base import ArticleState, Classifier, ClassifierError, check_answers
from mnp.classify.questions import QuestionSet, QuestionSetError, cached_question_set
from mnp.config import get_settings
from mnp.feed.build import enqueue_feed
from mnp.jobs import CLASSIFY, PermanentJobError, enqueue
from mnp.models import (
    Article,
    ArticleAsset,
    ArticleVersion,
    Classification,
    RawItem,
    Source,
)
from mnp.normalize.item import UnparseableItem, parse_payload


def classify_job(article_version_id: int, question_set: str) -> tuple[str, dict[str, Any]]:
    """(dedupe_key, payload) for a classify job."""
    return (
        f"{article_version_id}:{question_set}",
        {"article_version_id": article_version_id, "question_set": question_set},
    )


async def enqueue_classify(
    conn: AsyncConnection, article_version_ids: list[int], question_set: str
) -> int:
    return await enqueue(
        conn, CLASSIFY, (classify_job(i, question_set) for i in article_version_ids)
    )


class ClassifyHandler:
    """Job handler. Holds the classifier and caches question sets and the asset matcher."""

    def __init__(self, classifier: Classifier, config_dir: Path | None = None) -> None:
        self.classifier = classifier
        self.config_dir = config_dir or get_settings().config_dir
        self._matcher: AssetMatcher | None = None
        self.classified: list[int] = []  # classification ids written by this handler

    def question_set(self, version: str) -> QuestionSet:
        try:
            return cached_question_set(version, self.config_dir)
        except QuestionSetError as exc:
            raise PermanentJobError(str(exc)) from exc

    async def __call__(self, conn: AsyncConnection, payload: dict[str, Any]) -> None:
        version_id = int(payload["article_version_id"])
        qs = self.question_set(payload["question_set"])

        done = (
            await conn.execute(
                select(Classification.id).where(
                    Classification.article_version_id == version_id,
                    Classification.classifier == self.classifier.name,
                    Classification.question_set_version == qs.version,
                )
            )
        ).first()
        if done:
            return

        row = (
            await conn.execute(
                select(
                    ArticleVersion.headline,
                    ArticleVersion.summary,
                    ArticleVersion.body,
                    ArticleVersion.published_at,
                    ArticleVersion.content_hash,
                    RawItem.payload,
                    Source.name.label("source_name"),
                    Source.category.label("source_category"),
                )
                .join(RawItem, RawItem.id == ArticleVersion.raw_item_id)
                .join(Source, Source.id == RawItem.source_id)
                .where(ArticleVersion.id == version_id)
            )
        ).one_or_none()
        if row is None:
            raise PermanentJobError(f"article version {version_id} not found")

        try:
            tags = parse_payload(row.payload).tags
        except UnparseableItem:
            tags = ()
        if self._matcher is None:
            self._matcher = await load_matcher(conn)
        candidates = self._matcher.candidates(f"{row.headline}\n{row.summary or ''}", tags)
        questions = qs.questions_for(candidates)
        state = ArticleState(
            headline=row.headline,
            summary=row.summary,
            body=row.body,
            source_name=row.source_name,
            source_category=row.source_category,
            published_at=row.published_at,
        )

        result = await self.classifier.classify(state, questions)
        try:
            check_answers(questions, result.answers)
            columns = qs.denormalize(result.answers)
        except (ClassifierError, KeyError, TypeError, ValueError) as exc:
            raise ClassifierError(f"unusable classifier response: {exc}") from exc

        classification_id = (
            await conn.execute(
                insert(Classification)
                .values(
                    article_version_id=version_id,
                    content_hash=row.content_hash,
                    classifier=self.classifier.name,
                    model_version=result.model_version,
                    question_set_version=qs.version,
                    results={
                        "response": result.raw,
                        "state": state.to_json(),
                        "asset_candidates": [
                            {"symbol": c.asset.symbol, "via": c.via} for c in candidates
                        ],
                    },
                    latency_ms=result.latency_ms,
                    **columns,
                )
                .on_conflict_do_nothing()
                .returning(Classification.id)
            )
        ).scalar_one_or_none()
        if classification_id is None:
            return  # classified concurrently
        cluster = (
            await conn.execute(
                select(Article.cluster_id)
                .join(ArticleVersion, ArticleVersion.article_id == Article.id)
                .where(ArticleVersion.id == version_id)
            )
        ).scalar_one_or_none()
        await enqueue_feed(conn, cluster, f"classification:{classification_id}")

        if candidates:
            await conn.execute(
                insert(ArticleAsset).values(
                    [
                        {
                            "article_version_id": version_id,
                            "asset_id": c.asset.id,
                            "classification_id": classification_id,
                            "candidate_via": c.via,
                            "relevance_prob": result.answers[qs.asset_question_id(c.asset.symbol)][
                                "noul"
                            ],
                        }
                        for c in candidates
                    ]
                )
            )
        self.classified.append(classification_id)
