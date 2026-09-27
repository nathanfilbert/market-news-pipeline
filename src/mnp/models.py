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
    Index,
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
    # Used for articles whose item doesn't declare a language.
    language: Mapped[str] = mapped_column(Text, server_default=text("'en'"))


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


class Cluster(Base):
    """Near-duplicate story group (see mnp.normalize.cluster)."""

    __tablename__ = "clusters"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    first_seen_at: Mapped[datetime] = mapped_column(Timestamp)
    representative_article_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("articles.id", use_alter=True),  # FK added after articles exists
    )


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    canonical_url: Mapped[str] = mapped_column(Text, unique=True)
    first_seen_at: Mapped[datetime] = mapped_column(Timestamp)
    cluster_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("clusters.id"), index=True
    )


class ArticleVersion(Base):
    __tablename__ = "article_versions"
    __table_args__ = (
        UniqueConstraint("article_id", "content_hash"),
        UniqueConstraint("article_id", "version_no"),
        Index(
            "ix_article_versions_headline_trgm",
            "headline",
            postgresql_using="gin",
            postgresql_ops={"headline": "gin_trgm_ops"},
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    article_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("articles.id"))
    version_no: Mapped[int]
    # sha256 of normalized headline + summary + body + canonical_url (mnp.normalize.hashing).
    content_hash: Mapped[str] = mapped_column(Text)
    headline: Mapped[str] = mapped_column(Text)
    summary: Mapped[str | None] = mapped_column(Text)
    body: Mapped[str | None] = mapped_column(Text)
    author: Mapped[str | None] = mapped_column(Text)
    language: Mapped[str] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(Timestamp)  # from the source, as given
    received_at: Mapped[datetime] = mapped_column(Timestamp)  # when we fetched this content
    raw_item_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_items.id"), index=True)


class Job(Base):
    """Work queue between stages, consumed with SELECT … FOR UPDATE SKIP LOCKED."""

    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("kind", "dedupe_key"),
        CheckConstraint("status IN ('pending', 'done', 'failed')", name="status_valid"),
        Index(
            "ix_jobs_pending",
            "kind",
            "run_after",
            postgresql_where=text("status = 'pending'"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    kind: Mapped[str] = mapped_column(Text)
    dedupe_key: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    status: Mapped[str] = mapped_column(Text, server_default=text("'pending'"))
    attempts: Mapped[int] = mapped_column(server_default=text("0"))
    run_after: Mapped[datetime] = mapped_column(Timestamp, server_default=text("now()"))
    locked_at: Mapped[datetime | None] = mapped_column(Timestamp)  # start of the latest attempt
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(Timestamp, server_default=text("now()"))
    finished_at: Mapped[datetime | None] = mapped_column(Timestamp)
