"""Default lead snapshots to v2 scoring.

Revision ID: 0013_lead_snapshots_default_v2
Revises: 0012_event_promoted_fields
Create Date: 2026-03-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0013_lead_snapshots_default_v2"
down_revision = "0012_event_promoted_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("lead_snapshots") as batch_op:
        batch_op.alter_column(
            "scoring_version",
            existing_type=sa.String(length=32),
            existing_nullable=False,
            server_default="v2",
        )


def downgrade() -> None:
    with op.batch_alter_table("lead_snapshots") as batch_op:
        batch_op.alter_column(
            "scoring_version",
            existing_type=sa.String(length=32),
            existing_nullable=False,
            server_default="v1",
        )
