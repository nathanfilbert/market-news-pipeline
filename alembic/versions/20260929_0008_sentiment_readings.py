"""sentiment_readings (external sentiment series, e.g. Crypto Fear & Greed)

Revision ID: 0008
Revises: 0007
Create Date: 2026-09-29

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: Union[str, None] = "0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sentiment_readings",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("metric", sa.Text(), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Double(), nullable=False),
        sa.Column("label", sa.Text(), nullable=True),
        sa.Column("raw_item_id", sa.BigInteger(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["asset_id"], ["assets.id"], name=op.f("fk_sentiment_readings_asset_id_assets")
        ),
        sa.ForeignKeyConstraint(
            ["raw_item_id"],
            ["raw_items.id"],
            name=op.f("fk_sentiment_readings_raw_item_id_raw_items"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"], ["sources.id"], name=op.f("fk_sentiment_readings_source_id_sources")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sentiment_readings")),
        sa.UniqueConstraint(
            "source_id",
            "metric",
            "asset_id",
            "observed_at",
            name=op.f("uq_sentiment_readings_source_id_metric_asset_id_observed_at"),
            postgresql_nulls_not_distinct=True,
        ),
    )
    op.create_index(
        "ix_sentiment_readings_metric_observed_at",
        "sentiment_readings",
        ["metric", "observed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("sentiment_readings")
