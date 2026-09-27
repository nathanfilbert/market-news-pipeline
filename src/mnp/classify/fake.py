"""Deterministic stand-in for Jev in tests: keyword rules, Jev-shaped answers."""

import re
from collections.abc import Callable, Mapping
from typing import Any

from mnp.classify.base import Answer, ArticleState, ClassificationResult, Question

_EVENT_RULES = [
    (r"\b(hack|exploit|drain|stolen|breach)", "hack_exploit"),
    (r"\bdelist", "exchange_delisting"),
    (r"\blist(s|ing|ed)?\b", "exchange_listing"),
    (r"\bETFs?\b", "etf_institutional_flows"),
    (r"\b(CPI|inflation|payrolls|jobs report)\b", "macro_data_release"),
    (r"\b(Fed|FOMC|rate cut|rate hike|central bank)\b", "monetary_policy"),
    (r"\b(opinion|why|how)\b", "opinion_analysis"),
]
# (sentiment level of 5, impact level of 4, urgency level of 4)
_LEVELS = {
    "hack_exploit": (0, 3, 3),
    "exchange_delisting": (1, 2, 2),
    "exchange_listing": (4, 2, 2),
    "etf_institutional_flows": (3, 2, 1),
    "macro_data_release": (2, 2, 3),
    "monetary_policy": (2, 3, 3),
    "opinion_analysis": (2, 0, 0),
}


def _choice(options: list[str], chosen: str) -> Answer:
    rest = (0.1 / (len(options) - 1)) if len(options) > 1 else 0.0
    probs = {o: (0.9 if o == chosen else rest) for o in options}
    return {"type": "choice", "choice": chosen, "probabilities": probs, "confidence": 0.85}


def _score(levels: int, level: int) -> Answer:
    probs = {str(i): (1.0 if i == level else 0.0) for i in range(levels)}
    legend = {str(i): f"level {i}" for i in range(levels)}
    return {
        "type": "score",
        "score": float(level),
        "legend": legend,
        "probabilities": probs,
        "confidence": 1.0,
    }


class FakeClassifier:
    name = "fake"

    def __init__(
        self,
        overrides: Callable[[ArticleState], dict[str, Answer]] | None = None,
        model_version: str = "fake-1.0",
    ) -> None:
        self.overrides = overrides
        self.model_version = model_version
        self.calls: list[tuple[ArticleState, dict[str, Question]]] = []

    async def classify(
        self, state: ArticleState, questions: Mapping[str, Question]
    ) -> ClassificationResult:
        self.calls.append((state, dict(questions)))
        text = f"{state.headline} {state.summary or ''}"
        event = next((e for pattern, e in _EVENT_RULES if re.search(pattern, text, re.I)), "other")
        sentiment, impact, urgency = _LEVELS.get(event, (2, 0, 1))
        answers: dict[str, Answer] = {}
        for qid, q in questions.items():
            answers[qid] = self._answer(qid, q, text, event, sentiment, impact, urgency)
        if self.overrides:
            answers.update(self.overrides(state))
        raw: dict[str, Any] = {"model": self.model_version, "answers": answers, "usage": {}}
        return ClassificationResult(self.model_version, answers, raw, latency_ms=1)

    @staticmethod
    def _answer(qid, q, text, event, sentiment, impact, urgency) -> Answer:
        if q["type"] == "choice":
            options = list(q["criteria"])
            if qid == "event_type":
                return _choice(options, event if event in options else options[-1])
            crypto = re.search(r"\b(bitcoin|ether|crypto|token|blockchain|stablecoin)", text, re.I)
            return _choice(options, "crypto" if crypto and "crypto" in options else options[-1])
        if q["type"] == "score":
            levels = len(q["criteria"])
            level = {"sentiment": sentiment, "impact": impact, "urgency": urgency}.get(qid, 0)
            return _score(levels, min(level, levels - 1))
        if isinstance(q["instructions"], dict) and "asset" in q["instructions"]:
            asset = q["instructions"]["asset"]
            mentioned = re.search(
                rf"\b({re.escape(asset['symbol'])}|{re.escape(asset['name'])})\b", text, re.I
            )
            return {"type": "noul", "noul": 0.95 if mentioned else 0.1}
        value = {
            "is_market_relevant": 0.3 if event in ("other", "opinion_analysis") else 0.9,
            "is_new_information": 0.2 if event == "opinion_analysis" else 0.8,
            "is_promotional": 0.9 if re.search(r"sponsored|press release", text, re.I) else 0.05,
        }.get(qid, 0.5)
        return {"type": "noul", "noul": value}
