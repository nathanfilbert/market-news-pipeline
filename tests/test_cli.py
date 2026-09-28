import json
import shutil

import httpx
import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from mnp import __version__, cli
from mnp.collectors import base
from mnp.config import PROJECT_ROOT, get_settings
from mnp.models import Classification, RawItem
from tests.conftest import fixture_bytes, jev_response

runner = CliRunner()


def test_version():
    result = runner.invoke(cli.app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


@pytest.fixture
def cli_env(tmp_path, monkeypatch, database_url):
    """Point the CLI at the test database, a temp sources.yaml, and a mock feed server."""
    shutil.copytree(PROJECT_ROOT / "config" / "questions", tmp_path / "questions")
    shutil.copy(PROJECT_ROOT / "config" / "assets.yaml", tmp_path / "assets.yaml")
    (tmp_path / "sources.yaml").write_text(
        """
- {name: good, kind: rss, url: "https://good.example.com/rss", category: crypto, reputation: 1}
- {name: bad, kind: rss, url: "https://bad.example.com/rss", category: crypto, reputation: 0.5}
- {name: fh, kind: finnhub, url: "https://fh.example.com/news", category: crypto, reputation: 0.5}
"""
    )
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("FINNHUB_API_KEY", "")  # overrides a real key in a local .env
    get_settings.cache_clear()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "bad.example.com":
            return httpx.Response(502)
        return httpx.Response(200, content=fixture_bytes("rss/wordpress.xml"))

    def handler_with_jev(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.typesafe.ai":
            return jev_response(request)
        return handler(request)

    real = base.make_http_client
    monkeypatch.setattr(
        cli,
        "make_http_client",
        lambda s: real(s, transport=httpx.MockTransport(handler_with_jev)),
    )
    yield tmp_path
    get_settings.cache_clear()


def count_raw(sync_engine) -> int:
    with sync_engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(RawItem)).scalar_one()


@pytest.mark.db
def test_collect_once_twice(cli_env, sync_engine):
    first = runner.invoke(cli.app, ["collect", "--once", "--source", "good"])
    second = runner.invoke(cli.app, ["collect", "--once", "--source", "good"])
    assert first.exit_code == 0, first.output
    assert "good: 3 received, 3 new, ok" in first.stdout
    assert "good: 3 received, 0 new, ok" in second.stdout
    assert count_raw(sync_engine) == 3


@pytest.mark.db
def test_collect_all_reports_failures_but_runs_others(cli_env, sync_engine):
    result = runner.invoke(cli.app, ["collect", "--once"])
    assert result.exit_code == 1
    assert "good: 3 received, 3 new, ok" in result.stdout
    assert "bad: 0 received, 0 new, FAILED (CollectorError: HTTP 502" in result.stdout
    assert "fh: skipped (FINNHUB_API_KEY not set)" in result.stdout
    assert count_raw(sync_engine) == 3


def test_collect_unknown_source(cli_env):
    result = runner.invoke(cli.app, ["collect", "--once", "--source", "nope"])
    assert result.exit_code == 2
    assert "unknown source(s): nope" in result.output


@pytest.mark.db
def test_classify_requires_jev_key(cli_env, sync_engine, monkeypatch):
    monkeypatch.setenv("JEV_API_KEY", "")
    get_settings.cache_clear()
    result = runner.invoke(cli.app, ["classify"])
    assert result.exit_code == 2
    assert "JEV_API_KEY is not set" in result.output


@pytest.mark.db
def test_collect_normalize_classify_show(cli_env, sync_engine, monkeypatch):
    monkeypatch.setenv("JEV_API_KEY", "sk-test")
    monkeypatch.setenv("CONFIG_DIR", str(cli_env))
    get_settings.cache_clear()
    runner.invoke(cli.app, ["collect", "--once", "--source", "good"])
    runner.invoke(cli.app, ["normalize"])
    result = runner.invoke(cli.app, ["classify", "--show"])

    assert result.exit_code == 0, result.output
    assert "classify: 3 done, 0 retrying, 0 failed; 3 new classifications" in result.stdout
    assert "[good] Protocol X patches bug after $12M exploit" in result.stdout
    assert "jev-1.13.0" in result.stdout
    with sync_engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(Classification)).scalar() == 3


def test_reclassify_rejects_unknown_question_set(cli_env):
    result = runner.invoke(cli.app, ["reclassify", "--question-set", "v9.9"])
    assert result.exit_code == 2
    assert "question set 'v9.9' not found" in result.output


@pytest.mark.db
def test_news_command(cli_env, sync_engine, monkeypatch):
    monkeypatch.setenv("JEV_API_KEY", "sk-test")
    monkeypatch.setenv("CONFIG_DIR", str(cli_env))
    get_settings.cache_clear()
    runner.invoke(cli.app, ["collect", "--once", "--source", "good"])
    runner.invoke(cli.app, ["normalize"])
    runner.invoke(cli.app, ["classify"])

    result = runner.invoke(cli.app, ["news", "--since", "1h", "--source", "good"])
    assert result.exit_code == 0, result.output
    assert "[good]" in result.stdout
    assert "Protocol X patches bug after $12M exploit" in result.stdout
    assert "https://news.example.com/tech/2026/09/27/protocol-x-exploit" in result.stdout

    as_json = runner.invoke(cli.app, ["news", "--json", "--limit", "1"])
    record = json.loads(as_json.stdout.splitlines()[0])
    assert record["classification"]["model_version"] == "jev-1.13.0"

    none = runner.invoke(cli.app, ["news", "--event-type", "exchange_delisting"])
    assert "no matching articles" in none.stdout
    bad = runner.invoke(cli.app, ["news", "--since", "soonish"])
    assert bad.exit_code == 2
