"""M6: `mnp run` wiring, supervision, JSON logs and graceful shutdown."""

import asyncio
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest
from sqlalchemy import func, select

from mnp import runner
from mnp.collectors import base
from mnp.config import PROJECT_ROOT, get_settings
from mnp.logs import JsonFormatter
from mnp.models import Article, Classification, RawItem
from tests.conftest import fixture_bytes, jev_response


def test_json_formatter_includes_extras_and_exceptions():
    record = logging.LogRecord("mnp.test", logging.WARNING, __file__, 1, "hello %s", ("x",), None)
    record.source = "coindesk"
    record.new = 3
    entry = json.loads(JsonFormatter().format(record))
    assert entry["msg"] == "hello x"
    assert entry["level"] == "warning"
    assert entry["logger"] == "mnp.test"
    assert (entry["source"], entry["new"]) == ("coindesk", 3)
    assert entry["ts"].endswith("+00:00")

    try:
        raise ValueError("boom")
    except ValueError:
        import sys as _sys

        record.exc_info = _sys.exc_info()
    assert "ValueError: boom" in json.loads(JsonFormatter().format(record))["exc"]


async def test_supervise_restarts_a_crashing_loop(monkeypatch):
    monkeypatch.setattr(runner, "RESTART_BACKOFF_MAX", 0.01)
    stop = asyncio.Event()
    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("bug")
        stop.set()

    await asyncio.wait_for(runner.supervise("flaky", flaky, stop), timeout=5)
    assert len(calls) == 3


@pytest.fixture
def run_env(tmp_path, monkeypatch, database_url):
    """Config with two mock RSS sources, a mock Jev, and fast worker polling."""
    config = tmp_path / "config"
    shutil.copytree(PROJECT_ROOT / "config" / "questions", config / "questions")
    shutil.copy(PROJECT_ROOT / "config" / "assets.yaml", config / "assets.yaml")
    (config / "sources.yaml").write_text(
        """
- {name: a, kind: rss, url: "https://a.example.com/rss", category: crypto, reputation: 0.9}
- {name: b, kind: rss, url: "https://b.example.com/rss", category: crypto, reputation: 0.5}
- {name: fh, kind: finnhub, url: "https://fh.example.com/news", category: markets, reputation: 0.5}
"""
    )
    monkeypatch.setenv("CONFIG_DIR", str(config))
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("JEV_API_KEY", "sk-test")
    monkeypatch.setenv("FINNHUB_API_KEY", "")
    get_settings.cache_clear()
    monkeypatch.setattr(runner, "WORKER_IDLE_SECONDS", 0.05)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.typesafe.ai":
            return jev_response(request)
        if request.url.host == "a.example.com":
            return httpx.Response(200, content=fixture_bytes("rss/wordpress.xml"))
        return httpx.Response(200, content=fixture_bytes("rss/second_source.xml"))

    real = base.make_http_client
    monkeypatch.setattr(
        runner, "make_http_client", lambda s: real(s, transport=httpx.MockTransport(handler))
    )
    yield config
    get_settings.cache_clear()


async def _count(engine, model) -> int:
    async with engine.connect() as conn:
        return (await conn.execute(select(func.count()).select_from(model))).scalar()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.db
async def test_run_collects_normalizes_classifies_and_serves_health(run_env, engine, caplog):
    caplog.set_level(logging.INFO)
    port = _free_port()
    stop = asyncio.Event()
    task = asyncio.create_task(
        runner.run(get_settings(), engine, port=port, stop=stop, install_signal_handlers=False)
    )
    deadline = time.monotonic() + 15
    while await _count(engine, Classification) < 5 and time.monotonic() < deadline:
        assert not task.done(), task.exception()
        await asyncio.sleep(0.1)

    async with httpx.AsyncClient() as http:
        health = (await http.get(f"http://127.0.0.1:{port}/health")).json()
    stop.set()
    await asyncio.wait_for(task, timeout=10)

    assert await _count(engine, RawItem) == 5
    assert await _count(engine, Article) == 5
    assert await _count(engine, Classification) == 5
    statuses = {s["name"]: s["status"] for s in health["sources"]}
    assert statuses["a"] == statuses["b"] == "ok"
    assert statuses["fh"] == "never_run"  # no FINNHUB_API_KEY: skipped, and /health says so
    assert "source fh skipped: FINNHUB_API_KEY not set" in caplog.text
    assert "running 2 collectors (a, b), workers: normalize, classify" in caplog.text
    assert "stopped" in caplog.text


@pytest.mark.db
def test_sigterm_shuts_down_cleanly_with_json_logs(tmp_path, database_url):
    config = tmp_path / "config"
    shutil.copytree(PROJECT_ROOT / "config" / "questions", config / "questions")
    shutil.copy(PROJECT_ROOT / "config" / "assets.yaml", config / "assets.yaml")
    (config / "sources.yaml").write_text("[]\n")  # nothing to fetch: no network in tests
    env = os.environ | {
        "CONFIG_DIR": str(config),
        "DATABASE_URL": database_url,
        "JEV_API_KEY": "",
        "FINNHUB_API_KEY": "",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "mnp.cli", "run", "--no-api"],
        env=env,
        stderr=subprocess.PIPE,
        text=True,
        cwd=tmp_path,  # no .env here
    )
    lines = []
    try:
        for line in proc.stderr:
            lines.append(json.loads(line))
            if lines[-1]["msg"].startswith("running"):
                break
        proc.send_signal(signal.SIGTERM)
        proc.send_signal(signal.SIGTERM)  # duplicate delivery (process group + forwarding)
        lines += [json.loads(line) for line in proc.stderr]
        assert proc.wait(timeout=15) == 0
    finally:
        proc.kill()
    messages = [entry["msg"] for entry in lines]
    assert "running 0 collectors (), workers: normalize" in messages
    assert "SIGTERM received: shutting down gracefully" in messages
    assert "SIGTERM received again: cancelling now" not in messages
    assert messages[-1] == "stopped"
    assert all({"ts", "level", "logger", "msg"} <= entry.keys() for entry in lines)
