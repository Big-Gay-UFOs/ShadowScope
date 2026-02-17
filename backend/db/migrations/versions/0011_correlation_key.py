"""Add correlation_key for idempotent rebuilds

Revision ID: 0011_correlation_key
Revises: 0010_correlation_links_constraints
Create Date: 2026-02-17
"""

from alembic import op
import sqlalchemy as sa

revision = "0011_correlation_key"
down_revision = "0010_correlation_links_constraints"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.add_column("correlations", sa.Column("correlation_key", sa.Text(), nullable=True))

    # Backfill existing rows to avoid duplicates:
    # If lanes_hit includes lane + entity_id, build a deterministic key; otherwise fall back to legacy:<id>
    op.execute(
        """
        UPDATE correlations
        SET correlation_key =
            CASE
              WHEN lanes_hit IS NOT NULL
                   AND (lanes_hit->>'lane') IS NOT NULL
                   AND (lanes_hit->>'entity_id') IS NOT NULL
              THEN (lanes_hit->>'lane') || '|USAspending|' || window_days::text || '|entity:' || (lanes_hit->>'entity_id')
              ELSE 'legacy:' || id::text
            END
        WHERE correlation_key IS NULL;
        """
    )

    op.create_unique_constraint("uq_correlations_key", "correlations", ["correlation_key"])


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.drop_constraint("uq_correlations_key", "correlations", type_="unique")
    op.drop_column("correlations", "correlation_key")