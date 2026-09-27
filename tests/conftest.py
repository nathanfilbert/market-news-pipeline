import os
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from mnp.config import PROJECT_ROOT, get_settings
from mnp.models import Base

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_bytes(relpath: str) -> bytes:
    return (FIXTURES / relpath).read_bytes()


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
