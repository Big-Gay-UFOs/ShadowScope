from __future__ import annotations

from datetime import datetime

from backend.connectors.usaspending import SOURCE_NAME, normalize_awards


def test_normalize_awards_handles_date_formats_and_missing_fields():
    records = [
        {
            "generated_unique_award_id": "AWARD-1",
            "Award ID": "AID-1",
            "Action Date": "2024-02-29",
            "Description": "Test award",
            "Recipient Name": "ACME FEDERAL",
            "Recipient UEI": "uei123",
            "naicsCode": "541330",
            "naicsDescription": "Engineering Services",
            "awarding_agency_code": "DOE",
        },
        {
            "generated_unique_award_id": "AWARD-2",
            "action_date": "03/01/2024",
            "Place of Performance": "Los Alamos, NM",
            "piid": "PIID-22",
            "fain": "FAIN-9",
            "uri": "uri-abc",
            "modification_number": "0001",
            "place_of_performance_state": "NM",
            "place_of_performance_country": "USA",
        },
        {
            "Award ID": "AWARD-3",
            "action_date": "20240302",
            "Description": None,
            "document_id": "DOC-3",
        },
        {
            "internal_id": "abc123",
            "action_date": "not-a-date",
        },
    ]

    events = normalize_awards(records)
    assert len(events) == 4

    for event in events:
        assert event["category"] == "procurement"
        assert event["source"] == SOURCE_NAME
        assert event["hash"]
        assert isinstance(event["keywords"], list)
        assert isinstance(event["clauses"], list)
        assert event["raw_json"]

    # The first three records have parseable dates
    for parsed in events[:3]:
        assert isinstance(parsed["occurred_at"], datetime)

    # Canonical promoted fields are populated and merged into raw_json.
    first = events[0]
    assert first["award_id"] == "AID-1"
    assert first["generated_unique_award_id"] == "AWARD-1"
    assert first["recipient_name"] == "ACME FEDERAL"
    assert first["recipient_uei"] == "UEI123"
    assert first["naics_code"] == "541330"
    assert first["awarding_agency_code"] == "DOE"
    assert first["source_record_id"] == "AWARD-1"
    assert first["raw_json"].get("award_id") == "AID-1"
    assert first["raw_json"].get("recipient_uei") == "UEI123"

    second = events[1]
    assert second["piid"] == "PIID-22"
    assert second["fain"] == "FAIN-9"
    assert second["uri"] == "uri-abc"
    assert second["modification_number"] == "0001"
    assert second["document_id"] == "AWARD-2"

    # When no stable award identifier exists, the URL is omitted
    assert events[3]["source_url"] is None

    # When an award id exists, it is used in the source URL
    assert events[2]["source_url"] == "https://www.usaspending.gov/award/AWARD-3"
