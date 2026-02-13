"""Drop duplicate events.hash unique constraint (events_hash_key).

Revision ID: 0003_drop_hash_key
Revises: 0002_add_events_hash_uq
Create Date: 2026-02-13
"""
from __future__ import annotations

from alembic import op

revision = "0003_drop_hash_key"
down_revision = "0002_add_events_hash_uq"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # Remove redundant constraint created by older create_all / model unique=True behavior.
    op.execute("ALTER TABLE events DROP CONSTRAINT IF EXISTS events_hash_key;")

    # Ensure canonical constraint exists (some restored DBs may only have events_hash_key).
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_events_hash' AND conrelid = 'events'::regclass
            ) THEN
                ALTER TABLE events ADD CONSTRAINT uq_events_hash UNIQUE (hash);
            END IF;
        END$$;
        """
    )
    # Safety: if an index existed without a constraint (rare), drop it too.
    op.execute("DROP INDEX IF EXISTS events_hash_key;")


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # Restore the previous (redundant) constraint to keep downgrade reversible.
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'events_hash_key' AND conrelid = 'events'::regclass
            ) THEN
                ALTER TABLE events ADD CONSTRAINT events_hash_key UNIQUE (hash);
            END IF;
        END$$;
        """
    )
