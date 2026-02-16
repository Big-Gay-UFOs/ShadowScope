"""Add lead snapshot tables.

Revision ID: 0008_lead_snapshots
Revises: 0007_add_dry_run
Create Date: 2026-02-16
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0008_lead_snapshots"
down_revision = "0007_add_dry_run"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "lead_snapshots",
        sa.Column("id", sa.Integer, primary_key=True),

        # optional linkage to an analysis run (recommended)
        sa.Column("analysis_run_id", sa.Integer, sa.ForeignKey("analysis_runs.id", ondelete="SET NULL"), nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),

        # snapshot parameters / provenance
        sa.Column("source", sa.String(32), nullable=True),
        sa.Column("min_score", sa.Integer, nullable=False, server_default="1"),
        sa.Column("limit", sa.Integer, nullable=False, server_default="200"),
        sa.Column("scoring_version", sa.String(32), nullable=False, server_default="v1"),
        sa.Column("notes", sa.Text, nullable=True),
    )

    op.create_index("ix_lead_snapshots_created_at", "lead_snapshots", ["created_at"])
    op.create_index("ix_lead_snapshots_analysis_run", "lead_snapshots", ["analysis_run_id"])

    op.create_table(
        "lead_snapshot_items",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("lead_snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_id", sa.Integer, sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False),

        # stable identifier for delta comparisons
        sa.Column("event_hash", sa.String(64), nullable=False),

        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("score", sa.Integer, nullable=False),
        sa.Column("score_details", sa.JSON, nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_index("ix_lead_items_snapshot_rank", "lead_snapshot_items", ["snapshot_id", "rank"])
    op.create_index("ix_lead_items_event_hash", "lead_snapshot_items", ["event_hash"])
    op.create_index("ix_lead_items_score", "lead_snapshot_items", ["score"])

    op.create_unique_constraint("uq_lead_items_snapshot_event", "lead_snapshot_items", ["snapshot_id", "event_hash"])


def downgrade() -> None:
    op.drop_constraint("uq_lead_items_snapshot_event", "lead_snapshot_items", type_="unique")
    op.drop_index("ix_lead_items_score", table_name="lead_snapshot_items")
    op.drop_index("ix_lead_items_event_hash", table_name="lead_snapshot_items")
    op.drop_index("ix_lead_items_snapshot_rank", table_name="lead_snapshot_items")
    op.drop_table("lead_snapshot_items")

    op.drop_index("ix_lead_snapshots_analysis_run", table_name="lead_snapshots")
    op.drop_index("ix_lead_snapshots_created_at", table_name="lead_snapshots")
    op.drop_table("lead_snapshots")