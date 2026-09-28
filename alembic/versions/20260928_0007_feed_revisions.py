"""feed_revisions (trading feed v1)

Revision ID: 0007
Revises: 0006
Create Date: 2026-09-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "feed_revisions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("event_id", sa.BigInteger(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.CheckConstraint(
            "status IN ('active', 'retracted')", name=op.f("ck_feed_revisions_status_valid")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_feed_revisions")),
        sa.UniqueConstraint(
            "event_id", "revision", name=op.f("uq_feed_revisions_event_id_revision")
        ),
    )
    op.create_index(
        "ix_feed_revisions_event_id_id", "feed_revisions", ["event_id", "id"], unique=False
    )
    op.create_index(
        op.f("ix_feed_revisions_available_at"), "feed_revisions", ["available_at"], unique=False
    )
    # The feed is append-only, like raw_items.
    op.execute(
        """
        CREATE FUNCTION feed_revisions_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'feed_revisions is append-only (% not allowed)', TG_OP;
        END $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER feed_revisions_immutable BEFORE UPDATE OR DELETE ON feed_revisions
        FOR EACH ROW EXECUTE FUNCTION feed_revisions_immutable()
        """
    )
    # Publish the events that already exist.
    op.execute(
        """
        INSERT INTO jobs (kind, dedupe_key, payload)
        SELECT 'feed', 'init\\:' || c.id, jsonb_build_object('cluster_id', c.id)
        FROM clusters c
        WHERE EXISTS (SELECT 1 FROM articles a WHERE a.cluster_id = c.id AND NOT a.is_backfill)
        ORDER BY c.first_seen_at
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_table("feed_revisions")
    op.execute("DROP FUNCTION feed_revisions_immutable()")
