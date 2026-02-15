"""Add ingest run tracking table.

Revision ID: 0004_ingest_runs
Revises: 0003_drop_hash_key
Create Date: 2026-02-13
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_ingest_runs"
down_revision = "0003_drop_hash_key"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingest_runs",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),

        sa.Column("days", sa.Integer, nullable=True),
        sa.Column("start_page", sa.Integer, nullable=True),
        sa.Column("pages", sa.Integer, nullable=True),
        sa.Column("page_size", sa.Integer, nullable=True),
        sa.Column("max_records", sa.Integer, nullable=True),

        sa.Column("fetched", sa.Integer, nullable=False, server_default="0"),
        sa.Column("normalized", sa.Integer, nullable=False, server_default="0"),
        sa.Column("inserted", sa.Integer, nullable=False, server_default="0"),

        sa.Column("snapshot_dir", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
    )
    op.create_index("ix_ingest_runs_source_started", "ingest_runs", ["source", "started_at"])


def downgrade() -> None:
    op.drop_index("ix_ingest_runs_source_started", table_name="ingest_runs")
    op.drop_table("ingest_runs")