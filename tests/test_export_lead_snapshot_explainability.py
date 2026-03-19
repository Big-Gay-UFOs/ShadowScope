import json
from datetime import datetime, timezone

from backend.db.models import Correlation, CorrelationLink, Event, LeadSnapshot, LeadSnapshotItem, ensure_schema, get_session_factory
from backend.services.explainability import load_event_linked_source_summary
from backend.services.export_leads import export_lead_snapshot
from backend.services.lead_families import classify_lead_families



def test_export_lead_snapshot_includes_explainability(tmp_path):
    db_path = tmp_path / "explain.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        event = Event(
            category="award",
            source="USAspending",
            hash="ev_x_1",
            snippet="s1",
            place_text="p1",
            doc_id="d1",
            source_url="http://x/1",
            raw_json={},
            keywords=["k1"],
            clauses=[{"pack": "focus", "rule": "signal", "weight": 5, "field": "snippet", "match": "s1"}],
        )
        db.add(event)
        db.commit()
        db.refresh(event)

        correlation = Correlation(
            correlation_key="kw_pair|USAspending|30|pair:aaaaaaaaaaaaaaaa",
            score="0.577400",
            window_days=30,
            radius_km=0.0,
            lanes_hit={
                "lane": "kw_pair",
                "keyword_1": "alpha",
                "keyword_2": "beta",
                "event_count": 3,
                "c12": 3,
                "keyword_1_df": 3,
                "keyword_2_df": 3,
                "total_events": 9,
                "score_signal": 0.5774,
                "score_kind": "npmi",
                "score_secondary": 1.4142,
                "score_secondary_kind": "log_odds",
            },
        )
        db.add(correlation)
        db.commit()
        db.refresh(correlation)

        db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(event.id)))
        db.commit()

        snapshot = LeadSnapshot(source="USAspending", min_score=1, scoring_version="v2")
        db.add(snapshot)
        db.commit()
        db.refresh(snapshot)

        db.add(
            LeadSnapshotItem(
                snapshot_id=int(snapshot.id),
                event_id=int(event.id),
                event_hash=event.hash,
                rank=1,
                score=12,
                score_details={
                    "scoring_version": "v2",
                    "clause_score": 5,
                    "keyword_score": 0,
                    "entity_bonus": 0,
                    "pair_bonus": 6,
                    "pair_count": 1,
                    "pair_count_total": 1,
                    "pair_strength": 0.5774,
                    "pair_signal_total": 0.5774,
                },
            )
        )
        db.commit()

    out_dir = tmp_path / "out"
    result = export_lead_snapshot(snapshot_id=int(snapshot.id), database_url=db_url, output=out_dir)
    payload = json.loads(result["json"].read_text(encoding="utf-8"))
    assert payload["count"] == 1
    item = payload["items"][0]
    assert "why_summary" in item
    assert item["scoring_version"] == "v2"
    assert item["pair_bonus_applied"] == 6
    assert item["noise_penalty_applied"] == 0
    assert "contributing_correlations_json" in item
    correlations = json.loads(item["contributing_correlations_json"])
    assert correlations and correlations[0]["lane"] == "kw_pair"
    assert "matched_ontology_rules_json" in item
    assert json.loads(item["matched_ontology_rules_json"]) == ["focus:signal"]
    pairs = json.loads(item["top_kw_pairs_json"])
    assert pairs and pairs[0]["keyword_1"] == "alpha"
    assert pairs[0]["score_signal"] == 0.5774
    assert pairs[0]["score_secondary"] == 1.4142


def test_export_lead_snapshot_preserves_v3_subscore_explainability(tmp_path):
    db_path = tmp_path / "explain_v3.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        event = Event(
            category="notice",
            source="SAM.gov",
            hash="ev_v3_1",
            snippet="Secure facility modernization with DD254 support",
            place_text="CA",
            doc_id="secure-1",
            source_url="http://x/v3/1",
            raw_json={},
            keywords=[
                "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context",
                "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            ],
            clauses=[
                {
                    "pack": "sam_proxy_secure_compartmented_facility_engineering",
                    "rule": "icd705_scif_sapf_facility_upgrade_context",
                    "weight": 1,
                    "field": "snippet",
                    "match": "secure facility modernization",
                }
            ],
        )
        db.add(event)
        db.commit()
        db.refresh(event)

        snapshot = LeadSnapshot(source="SAM.gov", min_score=1, scoring_version="v3")
        db.add(snapshot)
        db.commit()
        db.refresh(snapshot)

        db.add(
            LeadSnapshotItem(
                snapshot_id=int(snapshot.id),
                event_id=int(event.id),
                event_hash=event.hash,
                rank=1,
                score=22,
                score_details={
                    "scoring_version": "v3",
                    "proxy_relevance_score": 12,
                    "investigability_score": 4,
                    "corroboration_score": 5,
                    "structural_context_score": 3,
                    "noise_penalty": 2,
                    "total_score": 22,
                    "top_positive_signals": [
                        {
                            "label": "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context",
                            "bucket": "proxy_relevance",
                            "signal_type": "clause",
                            "contribution": 6,
                        }
                    ],
                    "top_suppressors": [
                        {
                            "label": "operational_noise_terms:admin_facility_ops_noise",
                            "signal_type": "keyword",
                            "penalty": 2,
                        }
                    ],
                    "corroboration_sources": [
                        {
                            "label": "kw_pair corroboration",
                            "bucket": "corroboration",
                            "signal_type": "correlation",
                            "contribution": 5,
                            "lane": "kw_pair",
                        }
                    ],
                    "subscore_math": {
                        "formula": "proxy_relevance_score + investigability_score + corroboration_score + structural_context_score - noise_penalty",
                        "proxy_relevance_score": 12,
                        "investigability_score": 4,
                        "corroboration_score": 5,
                        "structural_context_score": 3,
                        "noise_penalty": 2,
                        "total_score": 22,
                    },
                },
            )
        )
        db.commit()

    out_dir = tmp_path / "out_v3"
    result = export_lead_snapshot(snapshot_id=int(snapshot.id), database_url=db_url, output=out_dir)
    payload = json.loads(result["json"].read_text(encoding="utf-8"))
    item = payload["items"][0]

    assert item["scoring_version"] == "v3"
    assert item["proxy_relevance_score"] == 12
    assert item["corroboration_score"] == 5
    assert "signals:" in item["why_summary"]
    assert json.loads(item["top_positive_signals_json"])[0]["contribution"] == 6
    assert json.loads(item["top_suppressors_json"])[0]["penalty"] == 2
    assert json.loads(item["corroboration_sources_json"])[0]["lane"] == "kw_pair"
    assert json.loads(item["subscore_math_json"])["total_score"] == 22


def test_linked_source_summary_orders_records_before_sampling(tmp_path):
    db_path = tmp_path / "linked_source_order.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        target_event = Event(
            category="notice",
            source="SAM.gov",
            hash="target-link-order",
            snippet="classified follow-on support",
            doc_id="target-doc",
            solicitation_number="ORDER-001",
            source_url="http://example.com/target-link-order",
            raw_json={},
            keywords=[],
            clauses=[],
            created_at=now,
        )
        db.add(target_event)
        db.commit()
        db.refresh(target_event)

        linked_events = []
        for idx in [6, 2, 5, 1, 4, 3]:
            linked_event = Event(
                category="award",
                source="USAspending",
                hash=f"linked-{idx}",
                snippet=f"linked award {idx}",
                doc_id=f"usa-doc-{idx}",
                award_id=f"AWARD-{idx}",
                source_url=f"http://example.com/linked/{idx}",
                raw_json={},
                keywords=[],
                clauses=[],
                created_at=now,
            )
            db.add(linked_event)
            db.commit()
            db.refresh(linked_event)
            linked_events.append(linked_event)

        correlation = Correlation(
            correlation_key="sam_usaspending_candidate_join|SAM.gov|365|order-001",
            score="65",
            window_days=365,
            radius_km=0.0,
            lanes_hit={
                "lane": "sam_usaspending_candidate_join",
                "event_count": 7,
                "score_signal": 65,
            },
        )
        db.add(correlation)
        db.commit()
        db.refresh(correlation)

        db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(target_event.id)))
        for linked_event in linked_events:
            db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(linked_event.id)))
        db.commit()

        target_event_id = int(target_event.id)
        correlation_id = int(correlation.id)
        context = load_event_linked_source_summary(db, event_ids=[int(target_event.id)])

    event_context = context[target_event_id]
    linked_records = event_context["linked_records_by_correlation"][correlation_id]
    assert [record["doc_id"] for record in linked_records] == [
        "usa-doc-1",
        "usa-doc-2",
        "usa-doc-3",
        "usa-doc-4",
        "usa-doc-5",
        "usa-doc-6",
    ]

    source_summary = event_context["linked_source_summary"][0]
    assert source_summary["source"] == "USAspending"
    assert source_summary["sample_doc_ids"] == [
        "usa-doc-1",
        "usa-doc-2",
        "usa-doc-3",
        "usa-doc-4",
        "usa-doc-5",
    ]

    details = classify_lead_families(
        details={
            "matched_ontology_clauses": [
                {
                    "pack": "sam_proxy_procurement_continuity_classified_followon",
                    "rule": "sole_source_follow_on_classified_context",
                    "weight": 2,
                    "field": "snippet",
                    "match": "classified follow-on",
                }
            ],
            "contributing_correlations": [
                {
                    "correlation_id": correlation_id,
                    "lane": "sam_usaspending_candidate_join",
                    "score_signal": 65,
                    "confidence_score": 65,
                    "evidence_types": ["identifier_exact"],
                    "candidate_join_evidence": [{"kind": "identifier_exact", "value": "ORDER-001"}],
                }
            ],
        },
        linked_source_summary=event_context["linked_source_summary"],
        linked_records_by_correlation=event_context["linked_records_by_correlation"],
    )
    candidate_join = details["corroboration_summary"]["candidate_join_evidence"][0]
    assert [record["doc_id"] for record in candidate_join["linked_records"]] == [
        "usa-doc-1",
        "usa-doc-2",
        "usa-doc-3",
    ]


def test_linked_source_summary_sampling_ignores_correlation_insert_order(tmp_path):
    db_path = tmp_path / "linked_source_multi_corr.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        target_event = Event(
            category="notice",
            source="SAM.gov",
            hash="target-link-multi-corr",
            snippet="classified follow-on support",
            doc_id="target-multi-doc",
            solicitation_number="ORDER-MULTI-001",
            source_url="http://example.com/target-link-multi-corr",
            raw_json={},
            keywords=[],
            clauses=[],
            created_at=now,
        )
        db.add(target_event)
        db.commit()
        db.refresh(target_event)

        linked_events: dict[int, Event] = {}
        for idx in [6, 2, 5, 1, 4, 3]:
            linked_event = Event(
                category="award",
                source="USAspending",
                hash=f"linked-multi-{idx}",
                snippet=f"linked multi award {idx}",
                doc_id=f"usa-doc-{idx}",
                award_id=f"AWARD-MULTI-{idx}",
                source_url=f"http://example.com/linked/multi/{idx}",
                raw_json={},
                keywords=[],
                clauses=[],
                created_at=now,
            )
            db.add(linked_event)
            db.commit()
            db.refresh(linked_event)
            linked_events[idx] = linked_event

        first_correlation = Correlation(
            correlation_key="sam_usaspending_candidate_join|SAM.gov|365|order-multi-a",
            score="65",
            window_days=365,
            radius_km=0.0,
            lanes_hit={
                "lane": "sam_usaspending_candidate_join",
                "event_count": 4,
                "score_signal": 65,
            },
        )
        second_correlation = Correlation(
            correlation_key="sam_usaspending_candidate_join|SAM.gov|365|order-multi-b",
            score="64",
            window_days=365,
            radius_km=0.0,
            lanes_hit={
                "lane": "sam_usaspending_candidate_join",
                "event_count": 4,
                "score_signal": 64,
            },
        )
        db.add_all([first_correlation, second_correlation])
        db.commit()
        db.refresh(first_correlation)
        db.refresh(second_correlation)

        db.add(CorrelationLink(correlation_id=int(first_correlation.id), event_id=int(target_event.id)))
        for idx in [6, 2, 4]:
            db.add(CorrelationLink(correlation_id=int(first_correlation.id), event_id=int(linked_events[idx].id)))
        db.add(CorrelationLink(correlation_id=int(second_correlation.id), event_id=int(target_event.id)))
        for idx in [5, 1, 3]:
            db.add(CorrelationLink(correlation_id=int(second_correlation.id), event_id=int(linked_events[idx].id)))
        db.commit()

        target_event_id = int(target_event.id)
        first_correlation_id = int(first_correlation.id)
        second_correlation_id = int(second_correlation.id)
        context = load_event_linked_source_summary(db, event_ids=[target_event_id])

    event_context = context[target_event_id]
    source_summary = event_context["linked_source_summary"][0]
    assert source_summary["sample_doc_ids"] == [
        "usa-doc-1",
        "usa-doc-2",
        "usa-doc-3",
        "usa-doc-4",
        "usa-doc-5",
    ]

    assert [record["doc_id"] for record in event_context["linked_records_by_correlation"][first_correlation_id]] == [
        "usa-doc-2",
        "usa-doc-4",
        "usa-doc-6",
    ]
    assert [record["doc_id"] for record in event_context["linked_records_by_correlation"][second_correlation_id]] == [
        "usa-doc-1",
        "usa-doc-3",
        "usa-doc-5",
    ]
