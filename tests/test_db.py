import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import text

from mnp.models import Base
from tests.conftest import alembic_config

pytestmark = pytest.mark.db


def test_migrations_downgrade_and_upgrade(database_url):
    cfg = alembic_config(database_url)
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")


async def test_models_match_migrations(engine):
    async with engine.connect() as conn:
        diff = await conn.run_sync(
            lambda sync_conn: compare_metadata(MigrationContext.configure(sync_conn), Base.metadata)
        )
    assert diff == []


async def test_pg_trgm_enabled(engine):
    async with engine.connect() as conn:
        sim = (
            await conn.execute(text("SELECT similarity('bitcoin etf', 'bitcoin etfs')"))
        ).scalar()
    assert sim > 0.6
