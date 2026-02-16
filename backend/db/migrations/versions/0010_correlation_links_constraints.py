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


def _drop_constraint_if_exists(table: str, name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = '{name}'
                  AND conrelid = '{table}'::regclass
            ) THEN
                EXECUTE 'ALTER TABLE {table} DROP CONSTRAINT {name}';
            END IF;
        END$$;
        """
    )


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # Clean any legacy/null rows before making columns NOT NULL
    op.execute("DELETE FROM correlation_links WHERE correlation_id IS NULL OR event_id IS NULL;")

    # Drop any existing FK/unique constraints that may already exist from earlier migrations
    _drop_constraint_if_exists("correlation_links", "correlation_links_correlation_id_fkey")
    _drop_constraint_if_exists("correlation_links", "correlation_links_event_id_fkey")
    _drop_constraint_if_exists("correlation_links", "fk_correlation_links_correlation_id")
    _drop_constraint_if_exists("correlation_links", "fk_correlation_links_event_id")
    _drop_constraint_if_exists("correlation_links", "uq_correlation_links_corr_event")

    # Drop supporting indexes if they exist (safe for re-apply in dev)
    op.execute("DROP INDEX IF EXISTS ix_correlation_links_correlation_id;")
    op.execute("DROP INDEX IF EXISTS ix_correlation_links_event_id;")
    op.execute("DROP INDEX IF EXISTS uq_correlation_links_corr_event;")

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

    # Drop our constraints/indexes if present
    op.execute("DROP INDEX IF EXISTS ix_correlation_links_event_id;")
    op.execute("DROP INDEX IF EXISTS ix_correlation_links_correlation_id;")
    _drop_constraint_if_exists("correlation_links", "uq_correlation_links_corr_event")
    _drop_constraint_if_exists("correlation_links", "fk_correlation_links_event_id")
    _drop_constraint_if_exists("correlation_links", "fk_correlation_links_correlation_id")

    # Restore the original-style FK names (non-cascade)
    _drop_constraint_if_exists("correlation_links", "correlation_links_correlation_id_fkey")
    _drop_constraint_if_exists("correlation_links", "correlation_links_event_id_fkey")

    op.create_foreign_key(
        "correlation_links_correlation_id_fkey",
        "correlation_links",
        "correlations",
        ["correlation_id"],
        ["id"],
    )
    op.create_foreign_key(
        "correlation_links_event_id_fkey",
        "correlation_links",
        "events",
        ["event_id"],
        ["id"],
    )

    op.alter_column("correlation_links", "event_id", existing_type=sa.Integer(), nullable=True)
    op.alter_column("correlation_links", "correlation_id", existing_type=sa.Integer(), nullable=True)