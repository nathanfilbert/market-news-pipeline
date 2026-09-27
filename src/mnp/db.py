"""Database engine helpers."""

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from mnp.config import get_settings


def make_engine(url: str | None = None) -> AsyncEngine:
    return create_async_engine(url or get_settings().database_url, pool_pre_ping=True)
