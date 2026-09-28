"""v1.2: embedding clustering with a same-event check, recluster, and evaluation."""

import json
import math

import httpx
import numpy as np
import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from mnp import cli
from mnp.classify.base import ClassifierError, ClassifierUnavailable
from mnp.collectors import base
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.config import Settings, get_settings
from mnp.jobs import NORMALIZE, run_pending
from mnp.models import Article, ArticleEmbedding, ArticleVersion
from mnp.normalize import same_event, service
from mnp.normalize.embeddings import DIMS, HashingEmbedder, embedding_text
from mnp.normalize.recluster import evaluate_pairs, hybrid_predictions, recluster
from mnp.normalize.same_event import JevSameEventJudge
from mnp.normalize.service import handle_normalize_job
from tests.conftest import jev_response, source

LISTING_A = "news.example.com/markets/2026/09/27/example-exchange-lists-token"
LISTING_B = "second.example.org/2026/09/27/example-exchange-token-perps"


def test_hashing_embedder():
    e = HashingEmbedder()
    v = e.embed(["Bitget hacked for $350M", "Bitget hacked for $350M", "Fed holds rates"])
    assert v.shape == (3, DIMS)
    assert np.allclose(np.linalg.norm(v, axis=1), 1)
    assert v[0] @ v[1] == pytest.approx(1) and v[0] @ v[2] < 0.3
    assert embedding_text("Headline", "x" * 1000) == "Headline. " + "x" * 300


class ScriptedEmbedder:
    """Listing A and B get vectors with a chosen cosine; everything else is orthogonal."""

    name = "scripted"

    def __init__(self, similarity: float) -> None:
        self.similarity = similarity
        self.others = 2

    def embed(self, texts):
        out = np.zeros((len(texts), DIMS), dtype=np.float32)
        for i, t in enumerate(texts):
            if "Example Exchange said it will list TOKEN" in t:  # listing A (source a)
                out[i, 0] = 1
            elif "The exchange will open TOKEN" in t:  # listing B (source b)
                out[i, 0], out[i, 1] = self.similarity, math.sqrt(1 - self.similarity**2)
            else:
                out[i, self.others] = 1
                self.others += 1
        return out


class FakeJudge:
    def __init__(self, answer):
        self.answer = answer
        self.calls = []

    async def same_event(self, a, b):
        self.calls.append((a["headline"], b["headline"]))
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


@pytest.fixture
def production_thresholds(monkeypatch):
    for key, value in {
        "CLUSTER_JOIN_SIMILARITY": "0.88",
        "CLUSTER_CONFIRM_SIMILARITY": "0.78",
        "CLUSTER_FALLBACK_SIMILARITY": "0.84",
        "CLUSTER_CONFIRM_MIN_PROB": "0.7",
    }.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()


async def run_pipeline(engine, server, client, monkeypatch, similarity, judge):
    embedder = ScriptedEmbedder(similarity)
    monkeypatch.setattr(service, "get_embedder", lambda *a: embedder)
    monkeypatch.setattr(service, "default_judge", lambda s: judge)
    ids = await sync_sources(engine, [source("a"), source("b")])
    server.serve("a", fixture="rss/wordpress.xml")
    server.serve("b", fixture="rss/second_source.xml")
    for name in ("a", "b"):
        await collect_once(engine, ids[name], RssCollector(source(name), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)


async def same_cluster(engine) -> bool:
    async with engine.connect() as conn:
        clusters = [
            (
                await conn.execute(
                    select(Article.cluster_id).where(Article.canonical_url.contains(url))
                )
            ).scalar_one()
            for url in (LISTING_A, LISTING_B)
        ]
    return clusters[0] == clusters[1]


@pytest.mark.db
@pytest.mark.parametrize(
    ("similarity", "answer", "joined", "judged"),
    [
        (0.92, 0.1, True, 0),  # clearly similar: joins without asking
        (0.82, 0.9, True, 1),  # borderline, Jev says same event
        (0.82, 0.3, False, 1),  # borderline, Jev says different
        (0.70, 0.9, False, 0),  # clearly different: never asks
        (0.82, ClassifierUnavailable("Jev HTTP 529"), False, 1),  # outage: 0.82 < fallback 0.84
        (0.86, ClassifierUnavailable("Jev HTTP 529"), True, 1),  # outage: 0.86 >= fallback
        (0.86, None, True, 0),  # no judge configured: fallback threshold
    ],
)
async def test_embedding_decisions(
    engine, server, client, monkeypatch, production_thresholds, similarity, answer, joined, judged
):
    judge = None if answer is None else FakeJudge(answer)
    await run_pipeline(engine, server, client, monkeypatch, similarity, judge)
    assert await same_cluster(engine) is joined
    assert len(judge.calls if judge else []) == judged


@pytest.mark.db
async def test_embeddings_and_provenance_are_stored(engine, server, client, monkeypatch):
    await run_pipeline(engine, server, client, monkeypatch, 0.95, None)
    async with engine.connect() as conn:
        versions = (await conn.execute(select(func.count()).select_from(ArticleVersion))).scalar()
        embedded = (await conn.execute(select(func.count()).select_from(ArticleEmbedding))).scalar()
        rows = (await conn.execute(select(Article.cluster_method, Article.clustered_at))).all()
    assert embedded == versions == 5
    assert {m for m, _ in rows} == {"embedding"}
    assert all(at is not None for _, at in rows)


@pytest.mark.db
async def test_recluster_is_repeatable_and_marks_provenance(engine, server, client, monkeypatch):
    await run_pipeline(engine, server, client, monkeypatch, 0.95, None)

    async def membership():
        async with engine.connect() as conn:
            rows = (await conn.execute(select(Article.canonical_url, Article.cluster_id))).all()
        groups: dict[int, set[str]] = {}
        for url, cluster in rows:
            groups.setdefault(cluster, set()).add(url)
        return sorted(sorted(g) for g in groups.values())

    before = await membership()
    settings = get_settings()
    first = await recluster(engine, settings)
    second = await recluster(engine, settings)
    assert await membership() == before
    assert (first.articles, first.joined, first.multi_source_clusters) == (5, 1, 1)
    assert second.joined == first.joined and second.embedded == 0
    async with engine.connect() as conn:
        methods = set((await conn.execute(select(Article.cluster_method))).scalars())
    assert methods == {"embedding"}


@pytest.mark.db
async def test_recluster_embeds_missing_vectors_and_supports_trigram(
    engine, server, client, monkeypatch
):
    monkeypatch.setenv("CLUSTER_METHOD", "trigram")
    get_settings.cache_clear()
    ids = await sync_sources(engine, [source("a"), source("b")])
    server.serve("a", fixture="rss/wordpress.xml")
    server.serve("b", fixture="rss/second_source.xml")
    for name in ("a", "b"):
        await collect_once(engine, ids[name], RssCollector(source(name), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    async with engine.connect() as conn:
        assert (
            await conn.execute(select(func.count()).select_from(ArticleEmbedding))
        ).scalar() == 0

    monkeypatch.setenv("CLUSTER_METHOD", "embedding")
    get_settings.cache_clear()
    report = await recluster(engine, get_settings())
    assert report.embedded == 5 and report.multi_source_clusters == 1


@pytest.mark.db
async def test_evaluate_pairs(engine, server, client, monkeypatch):
    await run_pipeline(engine, server, client, monkeypatch, 0.95, None)
    monkeypatch.setattr("mnp.normalize.recluster.get_embedder", lambda *a: ScriptedEmbedder(0.95))
    pairs = [
        {"a": f"https://{LISTING_A}", "b": f"https://{LISTING_B}", "label": "same"},
        {
            "a": f"https://{LISTING_A}",
            "b": "https://second.example.org/2026/09/27/minutes",
            "label": "different",
        },
        {"a": "https://missing.example/a", "b": "https://missing.example/b", "label": "same"},
    ]
    report = await evaluate_pairs(engine, get_settings(), pairs, judge=FakeJudge(0.9))
    assert report.missing == 1 and report.positives == 1
    assert [round(p.embedding, 2) for p in report.pairs] == [0.95, 0.0]
    assert report.rate(hybrid_predictions(report, get_settings()))[:2] == (1.0, 1.0)


async def test_jev_same_event_judge_request(monkeypatch):
    seen = []

    def handler(request):
        seen.append(json.loads(request.content))
        return jev_response(request)

    real = base.make_http_client
    monkeypatch.setattr(
        same_event, "make_http_client", lambda s: real(s, transport=httpx.MockTransport(handler))
    )
    judge = JevSameEventJudge(Settings(_env_file=None, jev_api_key="sk-test"))
    prob = await judge.same_event({"headline": "A"}, {"headline": "B"})
    assert prob == 0.8  # mock answers every noul with 0.8
    [body] = seen
    assert body["state"] == {"article_a": {"headline": "A"}, "article_b": {"headline": "B"}}
    assert body["questions"]["same_event"]["type"] == "noul"

    def bad(request):
        return httpx.Response(200, json={"model": "m", "answers": {"same_event": {"type": "x"}}})

    monkeypatch.setattr(
        same_event, "make_http_client", lambda s: real(s, transport=httpx.MockTransport(bad))
    )
    with pytest.raises(ClassifierError):
        await judge.same_event({}, {})


@pytest.mark.db
def test_cli_recluster_and_eval(sync_engine, database_url, monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", database_url)
    get_settings.cache_clear()
    runner = CliRunner()
    result = runner.invoke(cli.app, ["recluster", "--no-jev"])
    assert result.exit_code == 0, result.output
    assert "recluster (embedding): 0 articles" in result.stdout

    pairs = tmp_path / "pairs.yaml"
    pairs.write_text("pairs:\n  - {label: same, a: 'https://x/1', b: 'https://x/2'}\n")
    result = runner.invoke(cli.app, ["cluster-eval", "--pairs", str(pairs)])
    assert result.exit_code == 0, result.output
    assert "0 pairs (0 same), 1 not in this database" in result.stdout
