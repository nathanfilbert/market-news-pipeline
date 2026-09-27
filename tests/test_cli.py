import httpx
import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from mnp import __version__, cli
from mnp.collectors import base
from mnp.config import get_settings
from mnp.models import RawItem
from tests.conftest import fixture_bytes

runner = CliRunner()


def test_version():
    result = runner.invoke(cli.app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


@pytest.fixture
def cli_env(tmp_path, monkeypatch, database_url):
    """Point the CLI at the test database, a temp sources.yaml, and a mock feed server."""
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

    real = base.make_http_client
    monkeypatch.setattr(
        cli, "make_http_client", lambda s: real(s, transport=httpx.MockTransport(handler))
    )
    yield
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
