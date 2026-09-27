import os

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
        # In CI the database is expected to be there; a silent skip would hide a broken setup.
        if os.environ.get("CI"):
            pytest.fail(f"database unavailable in CI: {exc}")
        pytest.skip(f"database unavailable (run `docker compose up -d`): {exc}")
    return engine
