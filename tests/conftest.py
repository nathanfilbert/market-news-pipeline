import json
import os
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from mnp.collectors.base import make_http_client
from mnp.config import PROJECT_ROOT, Settings, SourceConfig, get_settings
from mnp.models import Base

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_bytes(relpath: str) -> bytes:
    return (FIXTURES / relpath).read_bytes()


@pytest.fixture(autouse=True)
def _no_backfill_by_default(monkeypatch):
    """Fixture feeds carry fixed 2026 dates; don't let them age into backfill as time passes.

    Tests of the backfill rule set BACKFILL_AFTER_DAYS themselves.
    """
    monkeypatch.setenv("BACKFILL_AFTER_DAYS", "100000")
    # Offline embeddings; thresholds suited to hashed word overlap. No Jev calls when
    # clustering (tests that exercise the same-event check pass their own judge).
    monkeypatch.setenv("EMBEDDING_MODEL", "hashing")
    monkeypatch.setenv("CLUSTER_JOIN_SIMILARITY", "0.6")
    monkeypatch.setenv("CLUSTER_CONFIRM_SIMILARITY", "0.4")
    monkeypatch.setenv("CLUSTER_FALLBACK_SIMILARITY", "0.5")
    monkeypatch.setenv("CLUSTER_CONFIRM_WITH_JEV", "false")
    monkeypatch.setenv("DISPLAY_TIMEZONE", "UTC")  # results don't depend on the machine's zone
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def alembic_config(database_url: str) -> Config:
    cfg = Config(PROJECT_ROOT / "alembic.ini")
    cfg.attributes["database_url"] = database_url
    return cfg


@pytest.fixture(scope="session")
def database_url() -> str:
    """A dedicated test database (default: `mnp_test` next to DATABASE_URL), migrated to head."""
    url = make_url(
        os.environ.get("TEST_DATABASE_URL")
        or make_url(get_settings().database_url).set(database="mnp_test")
    )
    admin = create_engine(
        url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    try:
        with admin.connect() as conn:
            exists = conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database}
            )
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    except Exception as exc:
        # In CI the database is expected to be there; a silent skip would hide a broken setup.
        if os.environ.get("CI"):
            pytest.fail(f"database unavailable in CI: {exc}")
        pytest.skip(f"database unavailable (run `docker compose up -d`): {exc}")
    finally:
        admin.dispose()

    rendered = url.render_as_string(hide_password=False)
    command.upgrade(alembic_config(rendered), "head")
    return rendered


TRUNCATE_ALL = text(
    f"TRUNCATE {', '.join(t.name for t in Base.metadata.sorted_tables)} RESTART IDENTITY CASCADE"
)


@pytest.fixture
async def engine(database_url):
    """Async engine on the test database, with every table emptied first."""
    eng = create_async_engine(database_url, poolclass=NullPool)
    async with eng.begin() as conn:
        await conn.execute(TRUNCATE_ALL)
    yield eng
    await eng.dispose()


@pytest.fixture
def sync_engine(database_url):
    """Sync engine on the emptied test database, for tests that drive asyncio.run() themselves."""
    eng = create_engine(database_url, poolclass=NullPool)
    with eng.begin() as conn:
        conn.execute(TRUNCATE_ALL)
    yield eng
    eng.dispose()


def source(name: str, **kwargs) -> SourceConfig:
    """An RSS source served by FeedServer at https://<name>.example.com/rss.xml."""
    return SourceConfig(
        **{
            "name": name,
            "kind": "rss",
            "url": f"https://{name}.example.com/rss.xml",
            "category": "crypto",
            "reputation": 0.5,
            **kwargs,
        }
    )


class FeedServer:
    """Mock transport serving one response per host; responses can be swapped between polls."""

    def __init__(self) -> None:
        self.responses: dict[str, httpx.Response] = {}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        r = self.responses[request.url.host]
        return httpx.Response(r.status_code, headers=r.headers, content=r.content)

    def serve(
        self,
        name: str,
        status: int = 200,
        fixture: str | None = None,
        content: bytes = b"",
        **headers: str,
    ) -> None:
        content = fixture_bytes(fixture) if fixture else content
        self.responses[f"{name}.example.com"] = httpx.Response(
            status, content=content, headers=headers
        )


@pytest.fixture
def server():
    return FeedServer()


@pytest.fixture
async def client(server):
    transport = httpx.MockTransport(server)
    async with make_http_client(Settings(_env_file=None), transport=transport) as c:
        yield c


def jev_response(request: httpx.Request, *, model: str = "jev-1.13.0") -> httpx.Response:
    """Mock Jev endpoint: answers every question in the request, in the documented shape."""
    body = json.loads(request.content)
    answers = {}
    for qid, q in body["questions"].items():
        if q["type"] == "noul":
            answers[qid] = {"type": "noul", "noul": 0.8}
        elif q["type"] == "choice":
            options = list(q["criteria"])
            probs = {
                o: (0.7 if i == 0 else 0.3 / max(1, len(options) - 1))
                for i, o in enumerate(options)
            }
            answers[qid] = {
                "type": "choice",
                "choice": options[0],
                "probabilities": probs,
                "confidence": 0.6,
            }
        else:
            levels = len(q["criteria"])
            answers[qid] = {
                "type": "score",
                "score": levels - 1,
                "legend": {str(i): c for i, c in enumerate(q["criteria"])},
                "probabilities": {str(i): float(i == levels - 1) for i in range(levels)},
                "confidence": 1.0,
            }
    return httpx.Response(
        200,
        json={
            "model": model,
            "answers": answers,
            "usage": {"input_tokens": 900, "output_tokens": 40},
        },
    )
