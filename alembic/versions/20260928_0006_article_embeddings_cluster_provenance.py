"""article_embeddings; articles.clustered_at and cluster_method

Revision ID: 0006
Revises: 0005
Create Date: 2026-09-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "article_embeddings",
        sa.Column("article_version_id", sa.BigInteger(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("vector", postgresql.ARRAY(postgresql.REAL()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["article_version_id"],
            ["article_versions.id"],
            name=op.f("fk_article_embeddings_article_version_id_article_versions"),
        ),
        sa.PrimaryKeyConstraint("article_version_id", "model", name=op.f("pk_article_embeddings")),
    )
    op.add_column("articles", sa.Column("clustered_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("articles", sa.Column("cluster_method", sa.Text(), nullable=True))
    # Existing clusters came from the trigram method at normalize time, shortly after first sight.
    op.execute(
        """
        UPDATE articles
        SET clustered_at = first_seen_at,
            cluster_method = CASE WHEN is_backfill THEN 'isolated' ELSE 'trigram' END
        WHERE cluster_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_column("articles", "cluster_method")
    op.drop_column("articles", "clustered_at")
    op.drop_table("article_embeddings")
