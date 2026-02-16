"""Rename lead_snapshots."limit" column to max_items.

Revision ID: 0009_lead_max_items
Revises: 0008_lead_snapshots
Create Date: 2026-02-16
"""
from __future__ import annotations

from alembic import op

revision = "0009_lead_max_items"
down_revision = "0008_lead_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='lead_snapshots' AND column_name='limit'
          )
          AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='lead_snapshots' AND column_name='max_items'
          ) THEN
            ALTER TABLE lead_snapshots RENAME COLUMN "limit" TO max_items;
          END IF;
        END$$;
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='lead_snapshots' AND column_name='max_items'
          )
          AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='lead_snapshots' AND column_name='limit'
          ) THEN
            ALTER TABLE lead_snapshots RENAME COLUMN max_items TO "limit";
          END IF;
        END$$;
        """
    )