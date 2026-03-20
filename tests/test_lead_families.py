from collections import Counter
from datetime import datetime, timezone

import pytest

from backend.db.models import Correlation, CorrelationLink, Event, ensure_schema, get_session_factory
from backend.services.lead_families import classify_lead_families, summarize_lead_family_distribution
from backend.services.leads import compute_leads


def _make_event(*, event_hash: str, pack: str, rule: str, created_at: datetime) -> Event:
    return Event(
        category="notice",
        source="SAM.gov",
        hash=event_hash,
        snippet=f"{pack} {rule}",
        doc_id=f"{event_hash}-doc",
        source_url=f"http://example.com/{event_hash}",
        raw_json={},
        keywords=[f"{pack}:{rule}"],
        clauses=[
            {
                "pack": pack,
                "rule": rule,
                "weight": 2,
                "field": "snippet",
                "match": f"{pack}:{rule}",
            }
        ],
        created_at=created_at,
    )


def _attach_lane(db, *, event_id: int, lane: str, suffix: str) -> None:
    correlation = Correlation(
        correlation_key=f"{lane}|SAM.gov|30|{suffix}",
        score="5",
        window_days=30,
        radius_km=0.0,
        lanes_hit={"lane": lane, "event_count": 2},
    )
    db.add(correlation)
    db.commit()
    db.refresh(correlation)
    db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(event_id)))
    db.commit()


def _common_linked_source_summary() -> list[dict[str, object]]:
    return [
        {
            "source": "USAspending",
            "linked_event_count": 1,
            "lanes": ["sam_usaspending_candidate_join"],
            "sample_event_ids": [211],
            "sample_doc_ids": ["usa-linked-doc"],
        }
    ]


def _common_linked_records() -> dict[int, list[dict[str, object]]]:
    return {
        902: [
            {
                "event_id": 211,
                "source": "USAspending",
                "doc_id": "usa-linked-doc",
                "award_id": "AWD-211",
                "solicitation_number": None,
                "source_url": "http://example.com/usa/211",
                "recipient_name": "Linked Support Vendor",
                "recipient_uei": "UEI-LINKED-211",
                "agency": "Department of Energy (DOE)",
                "place_region": "CA, USA",
                "lane": "sam_usaspending_candidate_join",
            }
        ]
    }


def _fixture_details(
    *,
    clauses: list[dict[str, object]],
    correlations: list[dict[str, object]],
    event_context: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "matched_ontology_rules": [
            f"{clause['pack']}:{clause['rule']}"
            for clause in clauses
            if clause.get("pack") and clause.get("rule")
        ],
        "matched_ontology_clauses": clauses,
        "contributing_correlations": correlations,
        "top_positive_signals": [
            {
                "signal_type": "clause",
                "pack": clause.get("pack"),
                "rule": clause.get("rule"),
                "field": clause.get("field"),
                "match": clause.get("match"),
                "contribution": max(int(clause.get("weight") or 1) + 1, 1),
                "bucket": "proxy_relevance",
            }
            for clause in clauses
        ],
        "event_context": {
            "source": "SAM.gov",
            "recipient_name": "Fixture Mission Support LLC",
            "recipient_uei": "UEI-FIXTURE-001",
            "awarding_agency_code": "DOE",
            "awarding_agency_name": "Department of Energy",
            "psc_code": "R499",
            "naics_code": "541330",
            "place_region": "CA, USA",
            "solicitation_number": "FIXTURE-001",
            "doc_id": "fixture-doc-001",
            **(event_context or {}),
        },
    }


def _generic_lineage_join() -> dict[str, object]:
    return {
        "lane": "sam_usaspending_candidate_join",
        "correlation_id": 902,
        "evidence_types": ["awarding_agency", "naics", "place_region"],
        "candidate_join_evidence": [],
        "matched_values": {},
    }


def _strong_lineage_join() -> dict[str, object]:
    return {
        "lane": "sam_usaspending_candidate_join",
        "correlation_id": 902,
        "evidence_types": ["identifier_exact", "contract_family", "recipient_uei"],
        "candidate_join_evidence": [],
        "matched_values": {},
    }


def _classification_fixtures() -> dict[str, dict[str, object]]:
    return {
        "facility_security_hardening": {
            "details": _fixture_details(
                clauses=[
                    {
                        "pack": "sam_proxy_secure_compartmented_facility_engineering",
                        "rule": "icd705_scif_sapf_facility_upgrade_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "SCIF upgrade",
                    },
                    {
                        "pack": "sam_proxy_procurement_continuity_classified_followon",
                        "rule": "sole_source_follow_on_classified_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "follow-on",
                    },
                ],
                correlations=[
                    {"lane": "same_agency", "correlation_id": 901},
                    _generic_lineage_join(),
                ],
                event_context={"notice_award_type": "Solicitation"},
            ),
            "expected_primary": "facility_security_hardening",
            "expected_secondaries": {"vendor_network_contract_lineage"},
        },
        "undersea_recovery_salvage": {
            "details": _fixture_details(
                clauses=[
                    {
                        "pack": "sam_proxy_maritime_remote_recovery_systems",
                        "rule": "rov_lars_heavy_lift_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "ROV LARS heavy lift",
                    },
                    {
                        "pack": "sam_proxy_procurement_continuity_classified_followon",
                        "rule": "sole_source_follow_on_classified_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "follow-on",
                    },
                ],
                correlations=[
                    {"lane": "same_place_region", "correlation_id": 901},
                    _generic_lineage_join(),
                ],
                event_context={"place_region": "HI, USA"},
            ),
            "expected_primary": "undersea_recovery_salvage",
            "expected_secondaries": {"vendor_network_contract_lineage"},
        },
        "range_test_infrastructure": {
            "details": _fixture_details(
                clauses=[
                    {
                        "pack": "sam_dod_flight_test_range_instrumentation",
                        "rule": "edwards_412th_plant42_range_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "Edwards AFB Plant 42",
                    },
                    {
                        "pack": "sam_proxy_procurement_continuity_classified_followon",
                        "rule": "sole_source_follow_on_classified_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "follow-on",
                    },
                ],
                correlations=[
                    {"lane": "same_place_region", "correlation_id": 901},
                    _generic_lineage_join(),
                ],
                event_context={"place_region": "CA, USA"},
            ),
            "expected_primary": "range_test_infrastructure",
            "expected_secondaries": {"vendor_network_contract_lineage"},
        },
        "vendor_network_contract_lineage": {
            "details": _fixture_details(
                clauses=[
                    {
                        "pack": "sam_proxy_procurement_continuity_classified_followon",
                        "rule": "sole_source_follow_on_classified_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "follow-on",
                    }
                ],
                correlations=[
                    {"lane": "same_contract_id", "correlation_id": 901},
                    _strong_lineage_join(),
                ],
                event_context={"award_id": "AWD-901", "piid": "PIID-901"},
            ),
            "expected_primary": "vendor_network_contract_lineage",
            "expected_secondaries": set(),
        },
        "ambiguous_multifamily": {
            "details": _fixture_details(
                clauses=[
                    {
                        "pack": "sam_proxy_secure_compartmented_facility_engineering",
                        "rule": "icd705_scif_sapf_facility_upgrade_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "SCIF upgrade",
                    },
                    {
                        "pack": "sam_proxy_classified_contract_security_admin",
                        "rule": "dd254_classification_guide_contract_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "DD254",
                    },
                    {
                        "pack": "sam_proxy_procurement_continuity_classified_followon",
                        "rule": "sole_source_follow_on_classified_context",
                        "weight": 2,
                        "field": "snippet",
                        "match": "follow-on",
                    },
                ],
                correlations=[
                    {"lane": "same_doc_id", "correlation_id": 901},
                    _generic_lineage_join(),
                ],
                event_context={"document_id": "DOC-AMB-001"},
            ),
            "expected_primary": "facility_security_hardening",
            "expected_secondaries": {
                "compartmented_support_intel",
                "vendor_network_contract_lineage",
            },
        },
    }


@pytest.mark.parametrize(
    ("pack", "rule", "lane", "expected_family"),
    [
        (
            "sam_proxy_secure_compartmented_facility_engineering",
            "icd705_scif_sapf_facility_upgrade_context",
            "same_agency",
            "facility_security_hardening",
        ),
        (
            "sam_proxy_materials_exploitation_forensics",
            "materials_forensic_lab_context",
            "kw_pair",
            "exploitation_materials_handling",
        ),
        (
            "sam_proxy_maritime_remote_recovery_systems",
            "rov_lars_heavy_lift_context",
            "same_place_region",
            "undersea_recovery_salvage",
        ),
        (
            "sam_proxy_classified_contract_security_admin",
            "dd254_classification_guide_contract_context",
            "same_doc_id",
            "compartmented_support_intel",
        ),
        (
            "sam_dod_flight_test_range_instrumentation",
            "edwards_412th_plant42_range_context",
            "same_place_region",
            "range_test_infrastructure",
        ),
        (
            "sam_proxy_procurement_continuity_classified_followon",
            "sole_source_follow_on_classified_context",
            "same_contract_id",
            "vendor_network_contract_lineage",
        ),
    ],
)
def test_compute_leads_assigns_expected_primary_family(tmp_path, pack: str, rule: str, lane: str, expected_family: str):
    db_url = f"sqlite:///{(tmp_path / f'{expected_family}.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        event = _make_event(
            event_hash=f"{expected_family}-event",
            pack=pack,
            rule=rule,
            created_at=now,
        )
        db.add(event)
        db.commit()
        db.refresh(event)
        _attach_lane(db, event_id=int(event.id), lane=lane, suffix=f"{expected_family}-lane")

    with SessionFactory() as db:
        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=0,
            limit=10,
            scan_limit=50,
            scoring_version="v2",
        )

    assert scanned == 1
    assert len(ranked) == 1

    score, event, details = ranked[0]
    assert score >= 0
    assert event.hash == f"{expected_family}-event"
    assert details["lead_family"] == expected_family
    assert details["lead_family_assignments"][0]["family"] == expected_family
    assert details["lead_family_assignments"][0]["ontology_matches"]
    assert any(match.get("kind") == "lane" and match.get("lane") == lane for match in details["lead_family_assignments"][0]["corroboration_matches"])
    assert lane in details["corroboration_summary"]["correlation_types_hit"]


@pytest.mark.parametrize(
    ("fixture_name", "expected_primary", "expected_secondaries"),
    [
        ("facility_security_hardening", "facility_security_hardening", {"vendor_network_contract_lineage"}),
        ("undersea_recovery_salvage", "undersea_recovery_salvage", {"vendor_network_contract_lineage"}),
        ("range_test_infrastructure", "range_test_infrastructure", {"vendor_network_contract_lineage"}),
        ("vendor_network_contract_lineage", "vendor_network_contract_lineage", set()),
        (
            "ambiguous_multifamily",
            "facility_security_hardening",
            {"compartmented_support_intel", "vendor_network_contract_lineage"},
        ),
    ],
)
def test_classify_lead_families_preserves_specialized_primary_candidates(
    fixture_name: str,
    expected_primary: str,
    expected_secondaries: set[str],
):
    fixture = _classification_fixtures()[fixture_name]
    details = classify_lead_families(
        details=dict(fixture["details"]),
        linked_source_summary=_common_linked_source_summary(),
        linked_records_by_correlation=_common_linked_records(),
    )

    assert details["lead_family"] == expected_primary
    assert expected_secondaries.issubset(set(details["secondary_lead_families"]))
    assert details["lead_family_assignments"]
    assert details["lead_family_selection"]["primary_family"] == expected_primary
    assert details["lead_family_assignments"][0]["selection"]["selection_score"] >= details["lead_family_assignments"][0]["score"]
    if expected_secondaries:
        assignment_families = {item["family"] for item in details["lead_family_assignments"]}
        assert expected_secondaries.issubset(assignment_families)


def test_mixed_fixture_set_does_not_collapse_to_vendor_network_contract_lineage():
    fixtures = _classification_fixtures()
    rows = []
    primary_counts: Counter[str] = Counter()

    for rank, (fixture_name, fixture) in enumerate(fixtures.items(), start=1):
        details = classify_lead_families(
            details=dict(fixture["details"]),
            linked_source_summary=_common_linked_source_summary(),
            linked_records_by_correlation=_common_linked_records(),
        )
        primary_counts[str(details["lead_family"])] += 1
        rows.append(
            {
                "rank": rank,
                "score": 100 - rank,
                "event_id": rank,
                "lead_family": details["lead_family"],
                "secondary_lead_families": details["secondary_lead_families"],
            }
        )

    assert primary_counts["vendor_network_contract_lineage"] == 1
    assert set(primary_counts) == {
        "facility_security_hardening",
        "undersea_recovery_salvage",
        "range_test_infrastructure",
        "vendor_network_contract_lineage",
    }

    distribution = summarize_lead_family_distribution(rows)
    primary = {row["lead_family"]: row["count"] for row in distribution["primary"]}
    secondary = {row["lead_family"]: row["count"] for row in distribution["secondary"]}

    assert primary == {
        "facility_security_hardening": 2,
        "undersea_recovery_salvage": 1,
        "range_test_infrastructure": 1,
        "vendor_network_contract_lineage": 1,
    }
    assert secondary["vendor_network_contract_lineage"] == 4
    assert distribution["ambiguous_items"] == 4
