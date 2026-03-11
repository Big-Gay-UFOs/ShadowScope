import os

from sqlalchemy import inspect

from backend.db.models import get_engine
from backend.db.ops import sync_database


def test_sync_database_adds_promoted_event_fields(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'migrations.db').as_posix()}"

    os.environ["DATABASE_URL"] = db_url
    status = sync_database()
    assert status in {"upgraded", "stamped"}

    inspector = inspect(get_engine(db_url))
    cols = {c["name"] for c in inspector.get_columns("events")}

    expected = {
        "award_id",
        "generated_unique_award_id",
        "piid",
        "fain",
        "uri",
        "transaction_id",
        "modification_number",
        "source_record_id",
        "recipient_name",
        "recipient_uei",
        "recipient_parent_uei",
        "recipient_duns",
        "recipient_cage_code",
        "awarding_agency_code",
        "awarding_agency_name",
        "funding_agency_code",
        "funding_agency_name",
        "contracting_office_code",
        "contracting_office_name",
        "psc_code",
        "psc_description",
        "naics_code",
        "naics_description",
        "notice_award_type",
        "place_of_performance_city",
        "place_of_performance_state",
        "place_of_performance_country",
        "place_of_performance_zip",
        "solicitation_number",
        "notice_id",
        "document_id",
    }

    assert expected.issubset(cols)
