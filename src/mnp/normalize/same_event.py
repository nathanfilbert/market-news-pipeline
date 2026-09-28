"""Ask Jev whether two articles report the same event (for borderline cluster matches)."""

from typing import Any, Protocol

from mnp.classify.base import ClassifierError
from mnp.classify.jev import JevClassifier
from mnp.collectors.base import make_http_client
from mnp.config import Settings

# Wording tested on labelled pairs (2026-09-28): yes >= 0.7 gave 92% recall, 92% precision alone.
QUESTION = {
    "type": "noul",
    "instructions": (
        "Do `article_a` and `article_b` report the same news event: the same development, "
        "not just the same topic or story?"
    ),
    "criteria": {
        "true": (
            "Both report the same development, even if worded differently or with slightly "
            "different figures (e.g. two outlets reporting the same hack, lawsuit, filing, "
            "ruling or announcement)."
        ),
        "false": (
            "Different events, or different developments in one story: a follow-up action "
            "(e.g. an issuer freezing a hacker's funds after a hack), an update from a different "
            "day, general market commentary, or a roundup covering several items."
        ),
    },
}


def judge_view(source: str, headline: str, summary: str | None) -> dict[str, Any]:
    return {"source": source, "headline": headline, "summary": (summary or "")[:600]}


class SameEventJudge(Protocol):
    async def same_event(self, a: dict[str, Any], b: dict[str, Any]) -> float:
        """Probability that `a` and `b` report the same event. Raises ClassifierUnavailable."""
        ...


class JevSameEventJudge:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def same_event(self, a: dict[str, Any], b: dict[str, Any]) -> float:
        s = self.settings
        async with make_http_client(s) as client:
            jev = JevClassifier(
                client, s.jev_api_key.get_secret_value(), model=s.jev_model, url=s.jev_url
            )
            result = await jev.ask({"article_a": a, "article_b": b}, {"same_event": QUESTION})
        answer = result.answers.get("same_event") or {}
        if answer.get("type") != "noul" or answer.get("noul") is None:
            raise ClassifierError(f"unexpected same_event answer: {answer!r}")
        return float(answer["noul"])


def default_judge(settings: Settings) -> SameEventJudge | None:
    if settings.cluster_confirm_with_jev and settings.jev_api_key is not None:
        return JevSameEventJudge(settings)
    return None
