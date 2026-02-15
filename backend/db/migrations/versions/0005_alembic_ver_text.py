"""Widen alembic_version.version_num.

Revision ID: 0005_alembic_ver_text
Revises: 0004_ingest_runs
Create Date: 2026-02-15
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005_alembic_ver_text"
down_revision = "0004_ingest_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.Text(),
        existing_nullable=False,
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.Text(),
        type_=sa.String(length=32),
        existing_nullable=False,
    )