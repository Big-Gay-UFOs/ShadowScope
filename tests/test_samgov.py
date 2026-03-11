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
            "noticeType": "Sources Sought",
            "solicitationNumber": "DOE-RFP-001",
            "naicsCode": "541330",
            "naicsDescription": "Engineering Services",
            "classificationCode": "R425",
            "typeOfSetAside": "SBA",
            "typeOfSetAsideDescription": "Total Small Business Set-Aside",
            "responseDeadLine": "2026-03-15",
            "fullParentPathName": "Department of Energy.Office of Science",
            "fullParentPathCode": "DOE.SCI",
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
            # International officeAddress uses countryCode (common SAM shape)
            "noticeId": "N3",
            "postedDate": "2018-05-04",
            "title": "International notice",
            "placeOfPerformance": None,
            "officeAddress": {"city": "OTTAWA", "countryCode": "CAN"},
        },
        {
            # No noticeId => fallback hash based on the full record
            "postedDate": "not-a-date",
            "title": "Missing notice id sample",
        },
    ]

    events = normalize_opportunities(records)
    assert len(events) == 4

    for event in events:
        assert event["category"] == "procurement"
        assert event["source"] == SOURCE_NAME
        assert event["hash"]
        assert isinstance(event["keywords"], list)
        assert isinstance(event["clauses"], list)
        assert event["raw_json"]

    # First three records have parseable dates
    assert isinstance(events[0]["occurred_at"], datetime)
    assert isinstance(events[1]["occurred_at"], datetime)
    assert isinstance(events[2]["occurred_at"], datetime)
    assert events[3]["occurred_at"] is None

    # Hash strategy uses a stable SOURCE_NAME prefix for noticeId rows
    expected = hashlib.sha256(f"{SOURCE_NAME}:N1".encode("utf-8")).hexdigest()
    assert events[0]["hash"] == expected

    # UI link normalization beta.sam.gov -> sam.gov
    assert events[0]["source_url"] == "https://sam.gov/opp/N1/view"

    # When uiLink is null, use a stable fallback
    assert events[1]["source_url"] == "https://sam.gov/opp/N2/view"

    # International officeAddress.countryCode should be preserved in place_text
    assert events[2]["place_text"] is not None
    assert "OTTAWA" in events[2]["place_text"]
    assert "CAN" in events[2]["place_text"]

    # Promoted normalized fields are filled for lanes/filters.
    e0 = events[0]
    assert e0["notice_id"] == "N1"
    assert e0["document_id"] == "N1"
    assert e0["source_record_id"] == "N1"
    assert e0["solicitation_number"] == "DOE-RFP-001"
    assert e0["naics_code"] == "541330"
    assert e0["naics_description"] == "Engineering Services"
    assert e0["psc_code"] == "R425"
    assert e0["awarding_agency_code"] == "DOE.SCI"
    assert e0["recipient_name"] == "ACME INC"
    assert e0["recipient_uei"] == "UEI123"
    assert e0["place_of_performance_state"] == "WI"
    assert e0["place_of_performance_country"] == "USA"

    # Awardee identifiers copied into canonical keys for later entity-linking
    assert events[0]["raw_json"].get("Recipient Name") == "ACME INC"
    assert events[0]["raw_json"].get("Recipient UEI") == "UEI123"

    # SAM context contract fields are normalized into canonical sam_* keys.
    raw0 = events[0]["raw_json"]
    assert raw0.get("sam_notice_type") == "sources sought"
    assert raw0.get("sam_solicitation_number") == "DOE-RFP-001"
    assert raw0.get("sam_naics_code") == "541330"
    assert raw0.get("sam_set_aside_code") == "SBA"
    assert raw0.get("sam_agency_path_code") == "DOE.SCI"
    assert raw0.get("sam_response_deadline", "").startswith("2026-03-15")

    # Canonical keys are merged into raw_json for compatibility/debugging.
    assert raw0.get("notice_id") == "N1"
    assert raw0.get("document_id") == "N1"
    assert raw0.get("naics_code") == "541330"
    assert raw0.get("awarding_agency_code") == "DOE.SCI"
