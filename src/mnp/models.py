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
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, REAL
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
    # Published long before we first saw it (see Settings.backfill_after_days).
    is_backfill: Mapped[bool] = mapped_column(server_default=text("false"))
    # When and how cluster_id was assigned. Consumers replaying history must not use a
    # cluster assignment before clustered_at (e.g. after `mnp recluster`).
    clustered_at: Mapped[datetime | None] = mapped_column(Timestamp)
    cluster_method: Mapped[str | None] = mapped_column(Text)  # embedding | trigram | isolated


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


class Asset(Base):
    """Taggable asset, synced from config/assets.yaml (see mnp.classify.assets for matching)."""

    __tablename__ = "assets"
    __table_args__ = (
        CheckConstraint("kind IN ('crypto', 'equity', 'index', 'macro')", name="kind_valid"),
    )

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, unique=True)
    name: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(Text)
    aliases: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default=text("'{}'"))
    exact_aliases: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default=text("'{}'"))
    # The symbol is a common word (LINK, NEAR): it can't make the asset a candidate on its own.
    ambiguous: Mapped[bool] = mapped_column(server_default=text("false"))
    enabled: Mapped[bool] = mapped_column(server_default=text("true"))


class Classification(Base):
    __tablename__ = "classifications"
    __table_args__ = (UniqueConstraint("article_version_id", "classifier", "question_set_version"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    article_version_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("article_versions.id"), index=True
    )
    content_hash: Mapped[str] = mapped_column(Text)
    classifier: Mapped[str] = mapped_column(Text)  # 'jev'
    model_version: Mapped[str] = mapped_column(Text)  # as reported by the API, e.g. jev-1.13.0
    question_set_version: Mapped[str] = mapped_column(Text)  # e.g. v1.1
    # Full classifier output (every probability), plus the state and asset candidates sent.
    results: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # Denormalized from `results` for querying.
    event_type: Mapped[str | None] = mapped_column(Text, index=True)
    event_type_prob: Mapped[float | None] = mapped_column(Double)
    domain: Mapped[str | None] = mapped_column(Text)
    is_market_relevant_prob: Mapped[float | None] = mapped_column(Double)
    is_new_information_prob: Mapped[float | None] = mapped_column(Double)
    is_promotional_prob: Mapped[float | None] = mapped_column(Double)
    sentiment: Mapped[float | None] = mapped_column(Double)  # -1 (bearish) .. +1 (bullish)
    impact: Mapped[float | None] = mapped_column(Double)  # 0 .. 1
    urgency: Mapped[float | None] = mapped_column(Double)  # 0 .. 1
    latency_ms: Mapped[int]
    classified_at: Mapped[datetime] = mapped_column(Timestamp, server_default=text("now()"))


class ArticleAsset(Base):
    __tablename__ = "article_assets"
    __table_args__ = (
        CheckConstraint(
            "candidate_via IN ('alias_match', 'source_tag')", name="candidate_via_valid"
        ),
    )

    article_version_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("article_versions.id"), primary_key=True
    )
    asset_id: Mapped[int] = mapped_column(ForeignKey("assets.id"), primary_key=True, index=True)
    classification_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("classifications.id"), primary_key=True
    )
    candidate_via: Mapped[str] = mapped_column(Text)
    relevance_prob: Mapped[float] = mapped_column(Double)  # Jev: "is this article about <asset>?"


class ArticleEmbedding(Base):
    """Embedding of an article version's headline + summary (mnp.normalize.embeddings)."""

    __tablename__ = "article_embeddings"

    article_version_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("article_versions.id"), primary_key=True
    )
    model: Mapped[str] = mapped_column(Text, primary_key=True)
    vector: Mapped[list[float]] = mapped_column(ARRAY(REAL))
    created_at: Mapped[datetime] = mapped_column(Timestamp, server_default=text("now()"))


class FeedRevision(Base):
    """Append-only log of event revisions: the trading feed (docs/feed-v1.md).

    `id` is the cursor: revisions are written one at a time (under a lock) in id order, so
    "everything after cursor X" never misses or repeats one. Rows are never updated.
    """

    __tablename__ = "feed_revisions"
    __table_args__ = (
        UniqueConstraint("event_id", "revision"),
        CheckConstraint("status IN ('active', 'retracted')", name="status_valid"),
        Index("ix_feed_revisions_event_id_id", "event_id", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[int] = mapped_column(BigInteger)  # the cluster id; no FK (clusters can go)
    revision: Mapped[int]
    status: Mapped[str] = mapped_column(Text)
    available_at: Mapped[datetime] = mapped_column(Timestamp, index=True)
    content_hash: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)


class SentimentReading(Base):
    """One value of an external sentiment series (mnp.sentiment), e.g. a day of Fear & Greed.

    asset_id NULL means market-wide. A revised value for the same point replaces the row;
    every value received stays in raw_items.
    """

    __tablename__ = "sentiment_readings"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "metric",
            "asset_id",
            "observed_at",
            postgresql_nulls_not_distinct=True,
        ),
        Index("ix_sentiment_readings_metric_observed_at", "metric", "observed_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    metric: Mapped[str] = mapped_column(Text)  # e.g. crypto_fear_greed
    asset_id: Mapped[int | None] = mapped_column(ForeignKey("assets.id"))
    observed_at: Mapped[datetime] = mapped_column(Timestamp)  # the time the value is for
    value: Mapped[float] = mapped_column(Double)  # on the metric's own scale (mnp.sentiment)
    label: Mapped[str | None] = mapped_column(Text)  # the source's wording, e.g. "Greed"
    raw_item_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("raw_items.id"))
    received_at: Mapped[datetime] = mapped_column(Timestamp)  # when we fetched this value
