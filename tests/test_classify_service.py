import shutil

import pytest
import yaml
from sqlalchemy import func, select, update

from mnp.classify.assets import load_assets, sync_assets
from mnp.classify.base import ClassifierUnavailable
from mnp.classify.fake import FakeClassifier
from mnp.classify.service import ClassifyHandler, enqueue_classify
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.config import PROJECT_ROOT
from mnp.jobs import CLASSIFY, NORMALIZE, run_pending
from mnp.models import ArticleAsset, ArticleVersion, Asset, Classification, Job
from mnp.normalize.service import handle_normalize_job
from tests.conftest import source

pytestmark = pytest.mark.db


@pytest.fixture
async def normalized(engine, server, client):
    """Two RSS polls (the second edits a headline) collected and normalized."""
    await sync_assets(engine, load_assets(PROJECT_ROOT / "config" / "assets.yaml"))
    ids = await sync_sources(engine, [source("a")])
    collector = RssCollector(source("a"), client)
    server.serve("a", fixture="rss/wordpress.xml")
    await collect_once(engine, ids["a"], collector)
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    return server, ids, collector


async def classify(engine, classifier=None, **kwargs):
    handler = ClassifyHandler(classifier or FakeClassifier(), PROJECT_ROOT / "config")
    stats = await run_pending(engine, CLASSIFY, handler, **kwargs)
    return handler, stats


async def count(engine, model, *where) -> int:
    async with engine.connect() as conn:
        return (await conn.execute(select(func.count()).select_from(model).where(*where))).scalar()


async def test_normalize_queues_one_classify_job_per_version(engine, normalized):
    async with engine.connect() as conn:
        keys = sorted(
            (await conn.execute(select(Job.dedupe_key).where(Job.kind == CLASSIFY))).scalars()
        )
    assert keys == ["1:v1.0", "2:v1.0", "3:v1.0"]


async def test_each_version_gets_exactly_one_classification(engine, normalized):
    _, stats = await classify(engine)
    assert stats.done == 3
    assert await count(engine, Classification) == 3

    # Re-running: queue is empty, and forcing the jobs back in is still a no-op.
    assert (await classify(engine))[1].processed == 0
    async with engine.begin() as conn:
        await conn.execute(update(Job).where(Job.kind == CLASSIFY).values(status="pending"))
    fake = FakeClassifier()
    await classify(engine, fake)
    assert fake.calls == []  # already classified: the classifier isn't called again
    assert await count(engine, Classification) == 3


async def test_full_results_and_denormalized_columns_are_stored(engine, normalized):
    await classify(engine)
    async with engine.connect() as conn:
        c = (
            await conn.execute(
                select(Classification)
                .join(ArticleVersion, ArticleVersion.id == Classification.article_version_id)
                .where(ArticleVersion.headline.like("Protocol X%"))
            )
        ).one()
    assert (c.classifier, c.model_version, c.question_set_version) == ("fake", "fake-1.0", "v1.0")
    assert c.event_type == "hack_exploit"
    assert c.event_type_prob == pytest.approx(0.9)
    assert c.sentiment == -1.0 and c.impact == 1.0
    answers = c.results["response"]["answers"]
    # every probability kept, not just the argmax
    assert len(answers["event_type"]["probabilities"]) == 17
    assert set(answers["sentiment"]["probabilities"]) == {"0", "1", "2", "3", "4"}
    assert c.results["state"]["headline"].startswith("Protocol X")
    assert c.content_hash


async def test_asset_candidates_are_confirmed_and_stored(engine, normalized, server):
    # Add a feed item that names assets.
    server.serve(
        "a",
        content=b"""<rss version="2.0"><channel><item><title>Bitcoin and Chainlink jump as SEC
        drops case; LINK link</title><link>https://a.example.com/x</link><guid>x</guid>
        <category>ETH</category></item></channel></rss>""",
    )
    _, ids, collector = normalized
    await collect_once(engine, ids["a"], collector)
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    await classify(engine)

    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                select(Asset.symbol, ArticleAsset.candidate_via, ArticleAsset.relevance_prob)
                .join(Asset, Asset.id == ArticleAsset.asset_id)
                .order_by(Asset.symbol)
            )
        ).all()
    assert [(r.symbol, r.candidate_via) for r in rows] == [
        ("BTC", "alias_match"),
        ("ETH", "source_tag"),
        ("LINK", "alias_match"),
        ("SEC", "alias_match"),
    ]
    assert dict((r.symbol, r.relevance_prob) for r in rows)["BTC"] == 0.95


async def test_edited_version_gets_its_own_classification(engine, normalized):
    server, ids, collector = normalized
    await classify(engine)
    server.serve("a", fixture="rss/wordpress_edited.xml")
    await collect_once(engine, ids["a"], collector)
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    await classify(engine)
    assert await count(engine, Classification) == 4


async def test_reclassify_with_a_new_question_set_keeps_both(engine, normalized, tmp_path):
    config = tmp_path / "config"
    shutil.copytree(PROJECT_ROOT / "config", config)
    data = yaml.safe_load((config / "questions" / "v1.0.yaml").read_text())
    data["version"] = "v1.1"
    data["questions"]["impact"]["instructions"] += " Consider the whole market."
    (config / "questions" / "v1.1.yaml").write_text(yaml.safe_dump(data))

    await classify(engine)
    async with engine.begin() as conn:
        ids = list((await conn.execute(select(ArticleVersion.id))).scalars())
        assert await enqueue_classify(conn, ids, "v1.1") == 3
    handler = ClassifyHandler(FakeClassifier(), config)
    await run_pending(engine, CLASSIFY, handler)

    async with engine.connect() as conn:
        per_set = dict(
            (
                await conn.execute(
                    select(Classification.question_set_version, func.count()).group_by(
                        Classification.question_set_version
                    )
                )
            ).all()
        )
    assert per_set == {"v1.0": 3, "v1.1": 3}


class FlakyClassifier(FakeClassifier):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    async def classify(self, state, questions):
        if self.failures:
            self.failures -= 1
            raise ClassifierUnavailable("Jev HTTP 529: overloaded", retry_after=120)
        return await super().classify(state, questions)


async def test_outage_retries_with_backoff_and_loses_nothing(engine, normalized):
    flaky = FlakyClassifier(failures=6)
    for _ in range(2):  # more failures than max_attempts allows for ordinary errors
        _, stats = await classify(engine, flaky, max_attempts=2)
        async with engine.begin() as conn:
            pending = (
                await conn.execute(select(Job).where(Job.kind == CLASSIFY).order_by(Job.id))
            ).all()
            assert {j.status for j in pending} == {"pending"}
            assert all(j.run_after > j.created_at for j in pending)  # backed off
            await conn.execute(update(Job).values(run_after=Job.created_at))  # "time passes"
    assert stats.failed == 0
    assert await count(engine, Classification) == 0

    _, stats = await classify(engine, flaky)  # Jev is back
    assert stats.done == 3
    assert await count(engine, Classification) == 3


async def test_unusable_response_fails_after_max_attempts(engine, normalized):
    broken = FakeClassifier(overrides=lambda state: {"impact": {"type": "noul", "noul": 1}})
    _, stats = await classify(engine, broken, max_attempts=1)
    assert stats.failed == 3
    async with engine.connect() as conn:
        errors = (await conn.execute(select(Job.last_error).where(Job.kind == CLASSIFY))).scalars()
        assert all("mistyped answer for 'impact'" in e for e in errors)
    assert await count(engine, Classification) == 0
