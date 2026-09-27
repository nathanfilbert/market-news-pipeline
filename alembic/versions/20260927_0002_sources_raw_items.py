"""sources, source_state, raw_items

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), sa.Identity(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("reputation", sa.Double(), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("poll_seconds", sa.Integer(), nullable=False),
        sa.CheckConstraint("reputation BETWEEN 0 AND 1", name=op.f("ck_sources_reputation_range")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sources")),
        sa.UniqueConstraint("name", name=op.f("uq_sources_name")),
    )
    op.create_table(
        "source_state",
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column(
            "checkpoint",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_state_source_id_sources"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("source_id", name=op.f("pk_source_state")),
    )
    op.create_table(
        "raw_items",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload_sha256", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_id"], ["sources.id"], name=op.f("fk_raw_items_source_id_sources")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_raw_items")),
        sa.UniqueConstraint(
            "source_id", "payload_sha256", name=op.f("uq_raw_items_source_id_payload_sha256")
        ),
    )
    # raw_items is append-only (docs/v1-plan.md §10).
    op.execute(
        """
        CREATE FUNCTION raw_items_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'raw_items is append-only (% not allowed)', TG_OP;
        END $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER raw_items_immutable BEFORE UPDATE OR DELETE ON raw_items
        FOR EACH ROW EXECUTE FUNCTION raw_items_immutable()
        """
    )


def downgrade() -> None:
    op.drop_table("raw_items")
    op.execute("DROP FUNCTION raw_items_immutable()")
    op.drop_table("source_state")
    op.drop_table("sources")
