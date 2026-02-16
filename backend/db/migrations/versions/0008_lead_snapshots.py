"""Add lead snapshot tables.

Revision ID: 0008_lead_snapshots
Revises: 0007_add_dry_run
Create Date: 2026-02-16
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0008_lead_snapshots"
down_revision = "0007_add_dry_run"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    # Create tables only if missing (prevents DuplicateTable on drifted DBs)
    if not insp.has_table("lead_snapshots"):
        op.create_table(
            "lead_snapshots",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("analysis_run_id", sa.Integer, sa.ForeignKey("analysis_runs.id", ondelete="SET NULL"), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("source", sa.String(32), nullable=True),
            sa.Column("min_score", sa.Integer, nullable=False, server_default="1"),
            sa.Column("limit", sa.Integer, nullable=False, server_default="200"),
            sa.Column("scoring_version", sa.String(32), nullable=False, server_default="v1"),
            sa.Column("notes", sa.Text, nullable=True),
        )

    if not insp.has_table("lead_snapshot_items"):
        op.create_table(
            "lead_snapshot_items",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("lead_snapshots.id", ondelete="CASCADE"), nullable=False),
            sa.Column("event_id", sa.Integer, sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False),
            sa.Column("event_hash", sa.String(64), nullable=False),
            sa.Column("rank", sa.Integer, nullable=False),
            sa.Column("score", sa.Integer, nullable=False),
            sa.Column("score_details", sa.JSON, nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        )

    # Postgres-safe idempotent indexes/constraint
    if bind.dialect.name == "postgresql":
        op.execute("CREATE INDEX IF NOT EXISTS ix_lead_snapshots_created_at ON lead_snapshots (created_at);")
        op.execute("CREATE INDEX IF NOT EXISTS ix_lead_snapshots_analysis_run ON lead_snapshots (analysis_run_id);")

        op.execute("CREATE INDEX IF NOT EXISTS ix_lead_items_snapshot_rank ON lead_snapshot_items (snapshot_id, rank);")
        op.execute("CREATE INDEX IF NOT EXISTS ix_lead_items_event_hash ON lead_snapshot_items (event_hash);")
        op.execute("CREATE INDEX IF NOT EXISTS ix_lead_items_score ON lead_snapshot_items (score);")

        op.execute(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname='uq_lead_items_snapshot_event'
              ) THEN
                ALTER TABLE lead_snapshot_items
                  ADD CONSTRAINT uq_lead_items_snapshot_event UNIQUE (snapshot_id, event_hash);
              END IF;
            END$$;
            """
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE lead_snapshot_items DROP CONSTRAINT IF EXISTS uq_lead_items_snapshot_event;")
        op.execute("DROP INDEX IF EXISTS ix_lead_items_score;")
        op.execute("DROP INDEX IF EXISTS ix_lead_items_event_hash;")
        op.execute("DROP INDEX IF EXISTS ix_lead_items_snapshot_rank;")
        op.execute("DROP TABLE IF EXISTS lead_snapshot_items;")
        op.execute("DROP INDEX IF EXISTS ix_lead_snapshots_analysis_run;")
        op.execute("DROP INDEX IF EXISTS ix_lead_snapshots_created_at;")
        op.execute("DROP TABLE IF EXISTS lead_snapshots;")