"""Add analysis_runs table.

Revision ID: 0006_analysis_runs
Revises: 0005_alembic_ver_text
Create Date: 2026-02-15
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0006_analysis_runs"
down_revision = "0005_alembic_ver_text"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analysis_runs",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("analysis_type", sa.String(32), nullable=False),  # e.g. "ontology_apply"
        sa.Column("status", sa.String(16), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),

        sa.Column("source", sa.String(32), nullable=True),
        sa.Column("days", sa.Integer, nullable=True),

        sa.Column("ontology_version", sa.String(32), nullable=True),
        sa.Column("ontology_hash", sa.String(64), nullable=True),

        sa.Column("dry_run", sa.Boolean, nullable=False, server_default=sa.text("false")),

        sa.Column("scanned", sa.Integer, nullable=False, server_default="0"),
        sa.Column("updated", sa.Integer, nullable=False, server_default="0"),
        sa.Column("unchanged", sa.Integer, nullable=False, server_default="0"),

        sa.Column("error", sa.Text, nullable=True),
    )
    op.create_index("ix_analysis_runs_type_started", "analysis_runs", ["analysis_type", "started_at"])
    op.create_index("ix_analysis_runs_source_started", "analysis_runs", ["source", "started_at"])


def downgrade() -> None:
    op.drop_index("ix_analysis_runs_source_started", table_name="analysis_runs")
    op.drop_index("ix_analysis_runs_type_started", table_name="analysis_runs")
    op.drop_table("analysis_runs")