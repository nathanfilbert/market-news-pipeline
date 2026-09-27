import pytest
from sqlalchemy import text

from mnp.db import get_engine


@pytest.fixture(scope="session")
def db_engine():
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"database unavailable (run `docker compose up -d`): {exc}")
    return engine
