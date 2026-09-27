"""articles, article_versions, clusters, jobs

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sources", sa.Column("language", sa.Text(), server_default=sa.text("'en'"), nullable=False)
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("status", sa.Text(), server_default=sa.text("'pending'"), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "run_after", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'done', 'failed')", name=op.f("ck_jobs_status_valid")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
        sa.UniqueConstraint("kind", "dedupe_key", name=op.f("uq_jobs_kind_dedupe_key")),
    )
    op.create_index(
        "ix_jobs_pending",
        "jobs",
        ["kind", "run_after"],
        postgresql_where=sa.text("status = 'pending'"),
    )

    # clusters <-> articles reference each other; the clusters FK is added after both exist.
    op.create_table(
        "clusters",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("representative_article_id", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_clusters")),
    )
    op.create_table(
        "articles",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cluster_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["cluster_id"], ["clusters.id"], name=op.f("fk_articles_cluster_id_clusters")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_articles")),
        sa.UniqueConstraint("canonical_url", name=op.f("uq_articles_canonical_url")),
    )
    op.create_index(op.f("ix_articles_cluster_id"), "articles", ["cluster_id"])
    op.create_foreign_key(
        op.f("fk_clusters_representative_article_id_articles"),
        "clusters",
        "articles",
        ["representative_article_id"],
        ["id"],
    )

    op.create_table(
        "article_versions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("article_id", sa.BigInteger(), nullable=False),
        sa.Column("version_no", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("author", sa.Text(), nullable=True),
        sa.Column("language", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_item_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["article_id"], ["articles.id"], name=op.f("fk_article_versions_article_id_articles")
        ),
        sa.ForeignKeyConstraint(
            ["raw_item_id"],
            ["raw_items.id"],
            name=op.f("fk_article_versions_raw_item_id_raw_items"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_article_versions")),
        sa.UniqueConstraint(
            "article_id", "content_hash", name=op.f("uq_article_versions_article_id_content_hash")
        ),
        sa.UniqueConstraint(
            "article_id", "version_no", name=op.f("uq_article_versions_article_id_version_no")
        ),
    )
    op.create_index(
        "ix_article_versions_headline_trgm",
        "article_versions",
        ["headline"],
        postgresql_using="gin",
        postgresql_ops={"headline": "gin_trgm_ops"},
    )
    op.create_index(op.f("ix_article_versions_raw_item_id"), "article_versions", ["raw_item_id"])

    # Queue raw items collected before this migration for normalization.
    op.execute(
        """
        INSERT INTO jobs (kind, dedupe_key, payload)
        SELECT 'normalize', id::text, jsonb_build_object('raw_item_id', id)
        FROM raw_items ORDER BY id
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_table("article_versions")
    op.drop_constraint(
        op.f("fk_clusters_representative_article_id_articles"), "clusters", type_="foreignkey"
    )
    op.drop_table("articles")
    op.drop_table("clusters")
    op.drop_table("jobs")
    op.drop_column("sources", "language")
