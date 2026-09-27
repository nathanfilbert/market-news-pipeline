import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from mnp.config import PROJECT_ROOT

pytestmark = pytest.mark.db


def test_migrated_to_head(db_engine):
    head = ScriptDirectory.from_config(Config(PROJECT_ROOT / "alembic.ini")).get_current_head()
    with db_engine.connect() as conn:
        current = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert current == head


def test_pg_trgm_enabled(db_engine):
    with db_engine.connect() as conn:
        sim = conn.execute(text("SELECT similarity('bitcoin etf', 'bitcoin etfs')")).scalar_one()
    assert sim > 0.6
