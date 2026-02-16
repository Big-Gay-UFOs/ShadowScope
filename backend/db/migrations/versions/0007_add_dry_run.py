"""Add dry_run column to analysis_runs.

Revision ID: 0007_add_dry_run
Revises: 0006_analysis_runs
Create Date: 2026-02-15
"""
from __future__ import annotations

from alembic import op

revision = "0007_add_dry_run"
down_revision = "0006_analysis_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        ALTER TABLE analysis_runs
        ADD COLUMN IF NOT EXISTS dry_run boolean NOT NULL DEFAULT false;
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute("ALTER TABLE analysis_runs DROP COLUMN IF EXISTS dry_run;")