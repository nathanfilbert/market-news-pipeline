"""Versioned question sets (config/questions/<version>.yaml)."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mnp.classify.base import Answer, Question
from mnp.config import get_settings, load_yaml

if TYPE_CHECKING:
    from mnp.classify.assets import AssetCandidate

# Questions every set must define: their answers are denormalized into classifications columns.
REQUIRED = {
    "event_type": "choice",
    "domain": "choice",
    "is_market_relevant": "noul",
    "is_new_information": "noul",
    "is_promotional": "noul",
    "sentiment": "score",
    "impact": "score",
    "urgency": "score",
}
MAX_CHOICE_OPTIONS = 255
MAX_SCORE_LEVELS = 10


class QuestionSetError(ValueError):
    pass


@dataclass(frozen=True)
class QuestionSet:
    version: str
    questions: dict[str, Question]  # fixed questions, in API shape
    ranges: dict[str, tuple[float, float]]  # score question id -> numeric range
    asset_prefix: str
    asset_instructions: str
    asset_criteria: dict[str, str]

    def questions_for(self, candidates: Sequence["AssetCandidate"]) -> dict[str, Question]:
        """The fixed questions plus one yes/no question per candidate asset."""
        questions = dict(self.questions)
        for c in candidates:
            questions[self.asset_question_id(c.asset.symbol)] = {
                "type": "noul",
                "instructions": {
                    "asset": {"name": c.asset.name, "symbol": c.asset.symbol},
                    "question": self.asset_instructions,
                },
                "criteria": self.asset_criteria,
            }
        return questions

    def asset_question_id(self, symbol: str) -> str:
        return f"{self.asset_prefix}{symbol}"

    def scaled(self, qid: str, answer: Answer) -> float:
        """A Score answer's position on its levels, mapped linearly onto the question's range."""
        levels = len(self.questions[qid]["criteria"])
        low, high = self.ranges[qid]
        return low + (float(answer["score"]) / (levels - 1)) * (high - low)

    def denormalize(self, answers: Mapping[str, Answer]) -> dict[str, Any]:
        event = answers["event_type"]
        return {
            "event_type": event["choice"],
            "event_type_prob": event["probabilities"][event["choice"]],
            "domain": answers["domain"]["choice"],
            "is_market_relevant_prob": answers["is_market_relevant"]["noul"],
            "is_new_information_prob": answers["is_new_information"]["noul"],
            "is_promotional_prob": answers["is_promotional"]["noul"],
            "sentiment": self.scaled("sentiment", answers["sentiment"]),
            "impact": self.scaled("impact", answers["impact"]),
            "urgency": self.scaled("urgency", answers["urgency"]),
        }


def _validate_question(qid: str, q: dict[str, Any]) -> None:
    kind = q.get("type")
    if kind not in ("noul", "choice", "score"):
        raise QuestionSetError(f"{qid}: unknown type {kind!r}")
    if not q.get("instructions"):
        raise QuestionSetError(f"{qid}: instructions are required")
    criteria = q.get("criteria")
    if kind == "choice" and not (isinstance(criteria, dict) and 2 <= len(criteria) <= 255):
        raise QuestionSetError(f"{qid}: a choice needs 2-{MAX_CHOICE_OPTIONS} options")
    if kind == "score":
        if not (isinstance(criteria, list) and 2 <= len(criteria) <= MAX_SCORE_LEVELS):
            raise QuestionSetError(f"{qid}: a score needs 2-{MAX_SCORE_LEVELS} levels")
        rng = q.get("range")
        if not (isinstance(rng, list) and len(rng) == 2 and rng[0] < rng[1]):
            raise QuestionSetError(f"{qid}: a score needs `range: [low, high]`")


def load_question_set(version: str, config_dir: Path | None = None) -> QuestionSet:
    path = (config_dir or get_settings().config_dir) / "questions" / f"{version}.yaml"
    if not path.exists():
        raise QuestionSetError(f"question set {version!r} not found at {path}")
    data = load_yaml(path)
    if data.get("version") != version:
        raise QuestionSetError(f"{path} declares version {data.get('version')!r}")

    raw_questions: dict[str, dict[str, Any]] = data.get("questions") or {}
    for qid, q in raw_questions.items():
        _validate_question(qid, q)
    for qid, kind in REQUIRED.items():
        if raw_questions.get(qid, {}).get("type") != kind:
            raise QuestionSetError(f"{version}: required {kind} question {qid!r} is missing")

    asset = data.get("asset_question") or {}
    if not asset.get("instructions") or not asset.get("id_prefix"):
        raise QuestionSetError(f"{version}: asset_question needs id_prefix and instructions")

    return QuestionSet(
        version=version,
        questions={
            qid: {k: v for k, v in q.items() if k != "range"} for qid, q in raw_questions.items()
        },
        ranges={
            qid: (float(q["range"][0]), float(q["range"][1]))
            for qid, q in raw_questions.items()
            if q["type"] == "score"
        },
        asset_prefix=asset["id_prefix"],
        asset_instructions=asset["instructions"],
        asset_criteria=asset.get("criteria") or {},
    )


@cache
def cached_question_set(version: str, config_dir: Path) -> QuestionSet:
    return load_question_set(version, config_dir)
