"""SQLAlchemy models. Tables are added milestone by milestone (see docs/v1-plan.md §5)."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Double,
    ForeignKey,
    Identity,
    MetaData,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Deterministic constraint names so Alembic autogenerate produces stable migrations.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

Timestamp = DateTime(timezone=True)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Source(Base):
    __tablename__ = "sources"
    __table_args__ = (CheckConstraint("reputation BETWEEN 0 AND 1", name="reputation_range"),)

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(Text, unique=True)
    kind: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    category: Mapped[str] = mapped_column(Text)
    reputation: Mapped[float] = mapped_column(Double)
    enabled: Mapped[bool] = mapped_column(server_default=text("true"))
    poll_seconds: Mapped[int]


class SourceState(Base):
    __tablename__ = "source_state"

    source_id: Mapped[int] = mapped_column(
        ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True
    )
    # Collector-specific resume state: ETag / Last-Modified / cursor.
    checkpoint: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    last_success_at: Mapped[datetime | None] = mapped_column(Timestamp)
    last_error_at: Mapped[datetime | None] = mapped_column(Timestamp)
    last_error: Mapped[str | None] = mapped_column(Text)
    consecutive_failures: Mapped[int] = mapped_column(server_default=text("0"))


class RawItem(Base):
    """Append-only: rows are never updated or deleted (enforced by a trigger)."""

    __tablename__ = "raw_items"
    __table_args__ = (UniqueConstraint("source_id", "payload_sha256"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    fetched_at: Mapped[datetime] = mapped_column(Timestamp)
    external_id: Mapped[str | None] = mapped_column(Text)
    url: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Hex sha256 of the raw bytes as received (for RSS: the item's bytes in the feed).
    payload_sha256: Mapped[str] = mapped_column(Text)
