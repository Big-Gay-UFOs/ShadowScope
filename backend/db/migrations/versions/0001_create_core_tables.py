"""Create initial entities, events, correlations tables"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_create_core_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "entities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("uei", sa.String(), nullable=True),
        sa.Column("cage", sa.String(), nullable=True),
        sa.Column("parent", sa.String(), nullable=True),
        sa.Column("type", sa.String(), nullable=True),
        sa.Column("sponsor", sa.String(), nullable=True),
        sa.Column("sites_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("entity_id", sa.Integer(), sa.ForeignKey("entities.id"), nullable=True),
        sa.Column("category", sa.String(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("doc_id", sa.String(), nullable=True),
        sa.Column("keywords", sa.JSON(), nullable=True),
        sa.Column("clauses", sa.JSON(), nullable=True),
        sa.Column("place_text", sa.Text(), nullable=True),
        sa.Column("snippet", sa.Text(), nullable=True),
        sa.Column("raw_json", sa.JSON(), nullable=True),
        sa.Column("hash", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("hash", name="uq_events_hash"),
    )

    op.create_table(
        "correlations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("score", sa.String(), nullable=False),
        sa.Column("window_days", sa.Integer(), nullable=False),
        sa.Column("radius_km", sa.Float(), nullable=False),
        sa.Column("lanes_hit", sa.JSON(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("rationale", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "correlation_links",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "correlation_id",
            sa.Integer(),
            sa.ForeignKey("correlations.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column(
            "event_id",
            sa.Integer(),
            sa.ForeignKey("events.id", ondelete="CASCADE"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_table("correlation_links")
    op.drop_table("correlations")
    op.drop_table("events")
    op.drop_table("entities")
