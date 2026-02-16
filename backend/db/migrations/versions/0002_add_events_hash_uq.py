"""Add unique constraint on events.hash

Revision ID: 0002_add_events_hash_uq
Revises: 0001_create_core_tables
Create Date: 2026-02-12
"""

from alembic import op

revision = "0002_add_events_hash_uq"
down_revision = "0001_create_core_tables"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        # Dev/tests may use SQLite; this migration is Postgres-specific.
        return

    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'uq_events_hash'
                  AND conrelid = 'events'::regclass
            ) THEN
                ALTER TABLE events ADD CONSTRAINT uq_events_hash UNIQUE (hash);
            END IF;
        END$$;
        """
    )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'uq_events_hash'
                  AND conrelid = 'events'::regclass
            ) THEN
                ALTER TABLE events DROP CONSTRAINT uq_events_hash;
            END IF;
        END$$;
        """
    )
