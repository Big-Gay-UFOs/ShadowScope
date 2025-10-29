from __future__ import annotations

from datetime import datetime

from backend.connectors.usaspending import SOURCE_NAME, normalize_awards


def test_normalize_awards_handles_date_formats_and_missing_fields():
    records = [
        {
            "generated_unique_award_id": "AWARD-1",
            "Action Date": "2024-02-29",
            "Description": "Test award",
        },
        {
            "generated_unique_award_id": "AWARD-2",
            "action_date": "03/01/2024",
            "Place of Performance": "Los Alamos, NM",
        },
        {
            "Award ID": "AWARD-3",
            "action_date": "20240302",
            "Description": None,
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

    # When no stable award identifier exists, the URL is omitted
    assert events[3]["source_url"] is None

    # When an award id exists, it is used in the source URL
    assert events[2]["source_url"] == "https://www.usaspending.gov/award/AWARD-3"
