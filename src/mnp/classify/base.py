"""Classifier interface. The pipeline depends only on this, so models can be swapped."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from mnp.jobs import RetryableJobError

# Keep the state focused: Jev's accuracy drops with long, mostly irrelevant input.
MAX_BODY_CHARS = 20_000

Question = dict[str, Any]  # Jev question shape: {"type", "instructions", "criteria"}
Answer = dict[str, Any]  # Jev answer shape: {"type", "noul"} / {"type", "choice", ...} / ...


@dataclass(frozen=True, slots=True)
class ArticleState:
    headline: str
    summary: str | None
    body: str | None
    source_name: str
    source_category: str
    published_at: datetime | None

    def to_json(self) -> dict[str, Any]:
        return {
            "headline": self.headline,
            "summary": self.summary,
            "body": self.body[:MAX_BODY_CHARS] if self.body else None,
            "source": {"name": self.source_name, "category": self.source_category},
            "published_at": self.published_at.isoformat() if self.published_at else None,
        }


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    model_version: str
    answers: dict[str, Answer]  # keyed by question id
    raw: dict[str, Any]  # the full response, stored as-is
    latency_ms: int


class ClassifierError(Exception):
    """The classifier rejected the request or returned something unusable."""


class ClassifierUnavailable(RetryableJobError):
    """Outage, overload, rate limit or bad credentials: retry later, never give up."""


class Classifier(Protocol):
    name: str

    async def classify(
        self, state: ArticleState, questions: Mapping[str, Question]
    ) -> ClassificationResult: ...


def check_answers(questions: Mapping[str, Question], answers: Mapping[str, Answer]) -> None:
    """Every question must come back with an answer of the matching type."""
    for qid, question in questions.items():
        answer = answers.get(qid)
        if not isinstance(answer, dict) or answer.get("type") != question["type"]:
            raise ClassifierError(f"missing or mistyped answer for {qid!r}: {answer!r}")
        key = {"noul": "noul", "choice": "choice", "score": "score"}[question["type"]]
        if answer.get(key) is None:
            raise ClassifierError(f"answer for {qid!r} has no {key!r}: {answer!r}")
