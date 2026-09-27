from pathlib import Path

import pytest
import yaml

from mnp.classify.assets import AssetCandidate, AssetRef
from mnp.classify.questions import QuestionSetError, load_question_set
from mnp.config import PROJECT_ROOT

CONFIG = PROJECT_ROOT / "config"
BTC = AssetRef(1, "BTC", "Bitcoin", "crypto", ("Bitcoin",), (), False)


def test_repo_question_set_v1_0():
    qs = load_question_set("v1.0", CONFIG)
    assert qs.version == "v1.0"
    assert set(qs.questions) == {
        "event_type", "domain", "is_market_relevant", "is_new_information",
        "is_promotional", "sentiment", "impact", "urgency",
    }  # fmt: skip
    assert len(qs.questions["event_type"]["criteria"]) == 17
    assert qs.ranges == {"sentiment": (-1.0, 1.0), "impact": (0.0, 1.0), "urgency": (0.0, 1.0)}
    assert all("range" not in q for q in qs.questions.values())  # not sent to Jev


def test_asset_questions_are_added_per_candidate():
    qs = load_question_set("v1.0", CONFIG)
    questions = qs.questions_for([AssetCandidate(BTC, "alias_match")])
    about = questions["about_BTC"]
    assert about["type"] == "noul"
    assert about["instructions"]["asset"] == {"name": "Bitcoin", "symbol": "BTC"}
    assert "`asset`" in about["instructions"]["question"]
    assert len(questions) == len(qs.questions) + 1


@pytest.mark.parametrize(
    ("qid", "score", "expected"),
    [("sentiment", 0, -1.0), ("sentiment", 2, 0.0), ("sentiment", 4, 1.0), ("sentiment", 3, 0.5),
     ("impact", 0, 0.0), ("impact", 3, 1.0), ("urgency", 1.5, 0.5)],
)  # fmt: skip
def test_score_answers_map_onto_range(qid, score, expected):
    qs = load_question_set("v1.0", CONFIG)
    assert qs.scaled(qid, {"score": score}) == pytest.approx(expected)


def _write(tmp_path: Path, version: str, mutate) -> Path:
    data = yaml.safe_load((CONFIG / "questions" / "v1.0.yaml").read_text())
    data["version"] = version
    mutate(data)
    (tmp_path / "questions").mkdir(exist_ok=True)
    (tmp_path / "questions" / f"{version}.yaml").write_text(yaml.safe_dump(data))
    return tmp_path


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda d: d["questions"].pop("impact"), "required score question 'impact'"),
        (lambda d: d["questions"]["sentiment"].pop("range"), "range"),
        (lambda d: d["questions"]["domain"].update(criteria={"only": None}), "2-255 options"),
        (lambda d: d["questions"]["urgency"].update(criteria=["x"] * 11), "2-10 levels"),
        (lambda d: d["questions"]["event_type"].update(type="list"), "unknown type"),
        (lambda d: d.update(version="v9"), "declares version"),
    ],
)
def test_invalid_question_sets(tmp_path, mutate, message):
    config = _write(tmp_path, "v1.1", mutate)
    with pytest.raises(QuestionSetError, match=message):
        load_question_set("v1.1", config)


def test_unknown_version():
    with pytest.raises(QuestionSetError, match="not found"):
        load_question_set("v0.0", CONFIG)
