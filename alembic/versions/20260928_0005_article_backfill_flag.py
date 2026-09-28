"""articles.is_backfill

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "articles",
        sa.Column("is_backfill", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    # Flag stored articles whose first version was published over 7 days (the default
    # BACKFILL_AFTER_DAYS) before we first saw them. Their existing clusters and
    # classifications are left as they are.
    op.execute(
        """
        UPDATE articles a SET is_backfill = true
        FROM article_versions v
        WHERE v.article_id = a.id AND v.version_no = 1
          AND v.published_at < a.first_seen_at - interval '7 days'
        """
    )


def downgrade() -> None:
    op.drop_column("articles", "is_backfill")
