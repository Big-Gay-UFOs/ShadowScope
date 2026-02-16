"""Correlation links constraints and indexes

Revision ID: 0010_correlation_links_constraints
Revises: 0009_lead_max_items
Create Date: 2026-02-16
"""

from alembic import op
import sqlalchemy as sa

revision = "0010_correlation_links_constraints"
down_revision = "0009_lead_max_items"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # Clean any legacy/null rows before making columns NOT NULL
    op.execute("DELETE FROM correlation_links WHERE correlation_id IS NULL OR event_id IS NULL;")

    op.alter_column("correlation_links", "correlation_id", existing_type=sa.Integer(), nullable=False)
    op.alter_column("correlation_links", "event_id", existing_type=sa.Integer(), nullable=False)

    op.create_foreign_key(
        "fk_correlation_links_correlation_id",
        "correlation_links",
        "correlations",
        ["correlation_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_correlation_links_event_id",
        "correlation_links",
        "events",
        ["event_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_unique_constraint(
        "uq_correlation_links_corr_event",
        "correlation_links",
        ["correlation_id", "event_id"],
    )

    op.create_index(
        "ix_correlation_links_correlation_id",
        "correlation_links",
        ["correlation_id"],
    )
    op.create_index(
        "ix_correlation_links_event_id",
        "correlation_links",
        ["event_id"],
    )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.drop_index("ix_correlation_links_event_id", table_name="correlation_links")
    op.drop_index("ix_correlation_links_correlation_id", table_name="correlation_links")
    op.drop_constraint("uq_correlation_links_corr_event", "correlation_links", type_="unique")
    op.drop_constraint("fk_correlation_links_event_id", "correlation_links", type_="foreignkey")
    op.drop_constraint("fk_correlation_links_correlation_id", "correlation_links", type_="foreignkey")

    op.alter_column("correlation_links", "event_id", existing_type=sa.Integer(), nullable=True)
    op.alter_column("correlation_links", "correlation_id", existing_type=sa.Integer(), nullable=True)