"""assets, classifications, article_assets

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '0004'
down_revision: Union[str, None] = '0003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('assets',
    sa.Column('id', sa.Integer(), sa.Identity(always=False), nullable=False),
    sa.Column('symbol', sa.Text(), nullable=False),
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('kind', sa.Text(), nullable=False),
    sa.Column('aliases', postgresql.ARRAY(sa.Text()), server_default=sa.text("'{}'"), nullable=False),
    sa.Column('exact_aliases', postgresql.ARRAY(sa.Text()), server_default=sa.text("'{}'"), nullable=False),
    sa.Column('ambiguous', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    sa.Column('enabled', sa.Boolean(), server_default=sa.text('true'), nullable=False),
    sa.CheckConstraint("kind IN ('crypto', 'equity', 'index', 'macro')", name=op.f('ck_assets_kind_valid')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_assets')),
    sa.UniqueConstraint('symbol', name=op.f('uq_assets_symbol'))
    )
    op.create_table('classifications',
    sa.Column('id', sa.BigInteger(), sa.Identity(always=False), nullable=False),
    sa.Column('article_version_id', sa.BigInteger(), nullable=False),
    sa.Column('content_hash', sa.Text(), nullable=False),
    sa.Column('classifier', sa.Text(), nullable=False),
    sa.Column('model_version', sa.Text(), nullable=False),
    sa.Column('question_set_version', sa.Text(), nullable=False),
    sa.Column('results', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('event_type', sa.Text(), nullable=True),
    sa.Column('event_type_prob', sa.Double(), nullable=True),
    sa.Column('domain', sa.Text(), nullable=True),
    sa.Column('is_market_relevant_prob', sa.Double(), nullable=True),
    sa.Column('is_new_information_prob', sa.Double(), nullable=True),
    sa.Column('is_promotional_prob', sa.Double(), nullable=True),
    sa.Column('sentiment', sa.Double(), nullable=True),
    sa.Column('impact', sa.Double(), nullable=True),
    sa.Column('urgency', sa.Double(), nullable=True),
    sa.Column('latency_ms', sa.Integer(), nullable=False),
    sa.Column('classified_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['article_version_id'], ['article_versions.id'], name=op.f('fk_classifications_article_version_id_article_versions')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_classifications')),
    sa.UniqueConstraint('article_version_id', 'classifier', 'question_set_version', name=op.f('uq_classifications_article_version_id_classifier_question_set_version'))
    )
    op.create_index(op.f('ix_classifications_article_version_id'), 'classifications', ['article_version_id'], unique=False)
    op.create_index(op.f('ix_classifications_event_type'), 'classifications', ['event_type'], unique=False)
    op.create_table('article_assets',
    sa.Column('article_version_id', sa.BigInteger(), nullable=False),
    sa.Column('asset_id', sa.Integer(), nullable=False),
    sa.Column('classification_id', sa.BigInteger(), nullable=False),
    sa.Column('candidate_via', sa.Text(), nullable=False),
    sa.Column('relevance_prob', sa.Double(), nullable=False),
    sa.CheckConstraint("candidate_via IN ('alias_match', 'source_tag')", name=op.f('ck_article_assets_candidate_via_valid')),
    sa.ForeignKeyConstraint(['article_version_id'], ['article_versions.id'], name=op.f('fk_article_assets_article_version_id_article_versions')),
    sa.ForeignKeyConstraint(['asset_id'], ['assets.id'], name=op.f('fk_article_assets_asset_id_assets')),
    sa.ForeignKeyConstraint(['classification_id'], ['classifications.id'], name=op.f('fk_article_assets_classification_id_classifications')),
    sa.PrimaryKeyConstraint('article_version_id', 'asset_id', 'classification_id', name=op.f('pk_article_assets'))
    )
    op.create_index(op.f('ix_article_assets_asset_id'), 'article_assets', ['asset_id'], unique=False)

    # Queue existing article versions for classification with question set v1.0.
    # ("\\:" escapes the colon, which text() would otherwise read as a bind parameter.)
    op.execute(
        """
        INSERT INTO jobs (kind, dedupe_key, payload)
        SELECT 'classify', id || '\\:v1.0',
               jsonb_build_object('article_version_id', id, 'question_set', 'v1.0')
        FROM article_versions ORDER BY id
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_article_assets_asset_id'), table_name='article_assets')
    op.drop_table('article_assets')
    op.drop_index(op.f('ix_classifications_event_type'), table_name='classifications')
    op.drop_index(op.f('ix_classifications_article_version_id'), table_name='classifications')
    op.drop_table('classifications')
    op.drop_table('assets')
