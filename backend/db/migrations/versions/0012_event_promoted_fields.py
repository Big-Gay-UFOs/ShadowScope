"""Promote high-value normalized event fields

Revision ID: 0012_event_promoted_fields
Revises: 0011_correlation_key
Create Date: 2026-03-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0012_event_promoted_fields"
down_revision = "0011_correlation_key"
branch_labels = None
depends_on = None


def _add_col(name: str, col_type: sa.types.TypeEngine) -> None:
    op.add_column("events", sa.Column(name, col_type, nullable=True))


def _create_idx(name: str, col: str) -> None:
    op.create_index(name, "events", [col], unique=False)


def _drop_idx(name: str) -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(f"DROP INDEX IF EXISTS {name};")
    else:
        op.execute(f"DROP INDEX IF EXISTS {name}")


def upgrade() -> None:
    _add_col("award_id", sa.String(length=128))
    _add_col("generated_unique_award_id", sa.String(length=128))
    _add_col("piid", sa.String(length=128))
    _add_col("fain", sa.String(length=128))
    _add_col("uri", sa.String(length=256))
    _add_col("transaction_id", sa.String(length=128))
    _add_col("modification_number", sa.String(length=64))
    _add_col("source_record_id", sa.String(length=128))

    _add_col("recipient_name", sa.Text())
    _add_col("recipient_uei", sa.String(length=64))
    _add_col("recipient_parent_uei", sa.String(length=64))
    _add_col("recipient_duns", sa.String(length=32))
    _add_col("recipient_cage_code", sa.String(length=32))

    _add_col("awarding_agency_code", sa.String(length=64))
    _add_col("awarding_agency_name", sa.Text())
    _add_col("funding_agency_code", sa.String(length=64))
    _add_col("funding_agency_name", sa.Text())
    _add_col("contracting_office_code", sa.String(length=64))
    _add_col("contracting_office_name", sa.Text())

    _add_col("psc_code", sa.String(length=32))
    _add_col("psc_description", sa.Text())
    _add_col("naics_code", sa.String(length=32))
    _add_col("naics_description", sa.Text())
    _add_col("notice_award_type", sa.String(length=128))

    _add_col("place_of_performance_city", sa.String(length=128))
    _add_col("place_of_performance_state", sa.String(length=32))
    _add_col("place_of_performance_country", sa.String(length=32))
    _add_col("place_of_performance_zip", sa.String(length=32))

    _add_col("solicitation_number", sa.String(length=128))
    _add_col("notice_id", sa.String(length=128))
    _add_col("document_id", sa.String(length=128))

    _create_idx("ix_events_award_id", "award_id")
    _create_idx("ix_events_generated_unique_award_id", "generated_unique_award_id")
    _create_idx("ix_events_piid", "piid")
    _create_idx("ix_events_fain", "fain")
    _create_idx("ix_events_uri", "uri")
    _create_idx("ix_events_transaction_id", "transaction_id")
    _create_idx("ix_events_modification_number", "modification_number")
    _create_idx("ix_events_source_record_id", "source_record_id")

    _create_idx("ix_events_recipient_uei", "recipient_uei")
    _create_idx("ix_events_recipient_parent_uei", "recipient_parent_uei")
    _create_idx("ix_events_recipient_duns", "recipient_duns")
    _create_idx("ix_events_recipient_cage_code", "recipient_cage_code")

    _create_idx("ix_events_awarding_agency_code", "awarding_agency_code")
    _create_idx("ix_events_funding_agency_code", "funding_agency_code")
    _create_idx("ix_events_contracting_office_code", "contracting_office_code")

    _create_idx("ix_events_psc_code", "psc_code")
    _create_idx("ix_events_naics_code", "naics_code")
    _create_idx("ix_events_notice_award_type", "notice_award_type")

    _create_idx("ix_events_place_of_performance_state", "place_of_performance_state")
    _create_idx("ix_events_place_of_performance_country", "place_of_performance_country")
    _create_idx("ix_events_place_of_performance_zip", "place_of_performance_zip")

    _create_idx("ix_events_solicitation_number", "solicitation_number")
    _create_idx("ix_events_notice_id", "notice_id")
    _create_idx("ix_events_document_id", "document_id")


def downgrade() -> None:
    _drop_idx("ix_events_document_id")
    _drop_idx("ix_events_notice_id")
    _drop_idx("ix_events_solicitation_number")

    _drop_idx("ix_events_place_of_performance_zip")
    _drop_idx("ix_events_place_of_performance_country")
    _drop_idx("ix_events_place_of_performance_state")

    _drop_idx("ix_events_notice_award_type")
    _drop_idx("ix_events_naics_code")
    _drop_idx("ix_events_psc_code")

    _drop_idx("ix_events_contracting_office_code")
    _drop_idx("ix_events_funding_agency_code")
    _drop_idx("ix_events_awarding_agency_code")

    _drop_idx("ix_events_recipient_cage_code")
    _drop_idx("ix_events_recipient_duns")
    _drop_idx("ix_events_recipient_parent_uei")
    _drop_idx("ix_events_recipient_uei")

    _drop_idx("ix_events_source_record_id")
    _drop_idx("ix_events_modification_number")
    _drop_idx("ix_events_transaction_id")
    _drop_idx("ix_events_uri")
    _drop_idx("ix_events_fain")
    _drop_idx("ix_events_piid")
    _drop_idx("ix_events_generated_unique_award_id")
    _drop_idx("ix_events_award_id")

    op.drop_column("events", "document_id")
    op.drop_column("events", "notice_id")
    op.drop_column("events", "solicitation_number")

    op.drop_column("events", "place_of_performance_zip")
    op.drop_column("events", "place_of_performance_country")
    op.drop_column("events", "place_of_performance_state")
    op.drop_column("events", "place_of_performance_city")

    op.drop_column("events", "notice_award_type")
    op.drop_column("events", "naics_description")
    op.drop_column("events", "naics_code")
    op.drop_column("events", "psc_description")
    op.drop_column("events", "psc_code")

    op.drop_column("events", "contracting_office_name")
    op.drop_column("events", "contracting_office_code")
    op.drop_column("events", "funding_agency_name")
    op.drop_column("events", "funding_agency_code")
    op.drop_column("events", "awarding_agency_name")
    op.drop_column("events", "awarding_agency_code")

    op.drop_column("events", "recipient_cage_code")
    op.drop_column("events", "recipient_duns")
    op.drop_column("events", "recipient_parent_uei")
    op.drop_column("events", "recipient_uei")
    op.drop_column("events", "recipient_name")

    op.drop_column("events", "source_record_id")
    op.drop_column("events", "modification_number")
    op.drop_column("events", "transaction_id")
    op.drop_column("events", "uri")
    op.drop_column("events", "fain")
    op.drop_column("events", "piid")
    op.drop_column("events", "generated_unique_award_id")
    op.drop_column("events", "award_id")
