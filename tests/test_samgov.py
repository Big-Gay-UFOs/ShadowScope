from __future__ import annotations

import hashlib
from datetime import datetime

from backend.connectors.samgov import SOURCE_NAME, normalize_opportunities


def test_normalize_opportunities_handles_date_formats_and_missing_fields():
    records = [
        {
            "noticeId": "N1",
            "postedDate": "2018-05-04",
            "title": "Historic Office Renovation",
            "uiLink": "https://beta.sam.gov/opp/N1/view",
            "placeOfPerformance": {
                "streetAddress": "517 E Wisconsin Ave",
                "city": {"name": "Milwaukee"},
                "state": {"code": "WI"},
                "zip": "53202",
                "country": {"code": "USA"},
            },
            "award": {"awardee": {"name": "ACME INC", "ueiSAM": "UEI123"}},
        },
        {
            "noticeId": "N2",
            "postedDate": "2018-05-04 12:34:56",
            "title": None,
            "uiLink": "null",
            "placeOfPerformance": None,
            "officeAddress": {"city": "WASHINGTON", "state": "DC", "zipcode": "20405", "countryCode": "USA"},
        },
        {
            # No noticeId => fallback hash based on the full record
            "postedDate": "not-a-date",
            "title": "Missing notice id sample",
        },
    ]

    events = normalize_opportunities(records)
    assert len(events) == 3

    for event in events:
        assert event["category"] == "procurement"
        assert event["source"] == SOURCE_NAME
        assert event["hash"]
        assert isinstance(event["keywords"], list)
        assert isinstance(event["clauses"], list)
        assert event["raw_json"]

    # First two records have parseable dates
    assert isinstance(events[0]["occurred_at"], datetime)
    assert isinstance(events[1]["occurred_at"], datetime)
    assert events[2]["occurred_at"] is None

    # Hash strategy uses a stable SOURCE_NAME prefix for noticeId rows
    expected = hashlib.sha256(f"{SOURCE_NAME}:N1".encode("utf-8")).hexdigest()
    assert events[0]["hash"] == expected

    # UI link normalization beta.sam.gov -> sam.gov
    assert events[0]["source_url"] == "https://sam.gov/opp/N1/view"

    # When uiLink is null, use a stable fallback
    assert events[1]["source_url"] == "https://sam.gov/opp/N2/view"

    # Awardee identifiers copied into canonical keys for later entity-linking
    assert events[0]["raw_json"].get("Recipient Name") == "ACME INC"
    assert events[0]["raw_json"].get("Recipient UEI") == "UEI123"
