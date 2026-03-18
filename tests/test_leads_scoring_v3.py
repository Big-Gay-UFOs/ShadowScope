from datetime import datetime, timedelta, timezone

from backend.db.models import Correlation, CorrelationLink, Entity, Event, ensure_schema, get_session_factory
from backend.services.leads import compute_leads


def _clause(pack: str, rule: str, *, weight: int = 1, field: str = "snippet", match: str | None = None) -> dict:
    return {
        "pack": pack,
        "rule": rule,
        "weight": int(weight),
        "field": field,
        "match": match or rule.replace("_", " "),
    }


def _seed_correlation(
    db,
    *,
    event_id: int,
    correlation_key: str,
    lane: str,
    score: float,
    event_count: int,
    lanes_hit: dict | None = None,
    summary: str | None = None,
    rationale: str | None = None,
) -> None:
    payload = {"lane": lane, "event_count": int(event_count)}
    if isinstance(lanes_hit, dict):
        payload.update(lanes_hit)
    correlation = (
        db.query(Correlation)
        .filter(Correlation.correlation_key == correlation_key)
        .one_or_none()
    )
    if correlation is None:
        correlation = Correlation(
            correlation_key=correlation_key,
            score=f"{float(score):.6f}",
            window_days=30,
            radius_km=0.0,
            lanes_hit=payload,
            summary=summary,
            rationale=rationale,
        )
        db.add(correlation)
        db.commit()
        db.refresh(correlation)
    else:
        correlation.score = f"{float(score):.6f}"
        correlation.window_days = 30
        correlation.radius_km = 0.0
        correlation.lanes_hit = payload
        correlation.summary = summary
        correlation.rationale = rationale
        db.commit()

    existing_link = (
        db.query(CorrelationLink)
        .filter(
            CorrelationLink.correlation_id == int(correlation.id),
            CorrelationLink.event_id == int(event_id),
        )
        .one_or_none()
    )
    if existing_link is None:
        db.add(CorrelationLink(correlation_id=int(correlation.id), event_id=int(event_id)))
        db.commit()


def _seed_v3_fixture(db) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    routine_entity = Entity(name="Routine Facilities Vendor")
    db.add(routine_entity)
    db.commit()
    db.refresh(routine_entity)

    secure = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_secure_facility",
        created_at=now - timedelta(hours=4),
        snippet="ICD-705 secure facility upgrade with DD254 and SAP security support.",
        source_url="https://example.com/secure",
        doc_id="SECURE-1",
        solicitation_number="SOL-SECURE-1",
        awarding_agency_name="Department of the Air Force",
        recipient_uei="UEI-SECURE-1",
        naics_code="541715",
        psc_code="R425",
        place_of_performance_state="CA",
        place_of_performance_country="USA",
        notice_award_type="Solicitation",
        raw_json={},
        keywords=[
            "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context",
            "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            "sam_dod_program_protection_sap:afosi_program_security_context",
        ],
        clauses=[
            _clause(
                "sam_proxy_secure_compartmented_facility_engineering",
                "icd705_scif_sapf_facility_upgrade_context",
            ),
            _clause(
                "sam_proxy_classified_contract_security_admin",
                "dd254_classification_guide_contract_context",
            ),
            _clause("sam_dod_program_protection_sap", "afosi_program_security_context"),
        ],
        place_text="Edwards AFB, CA",
    )
    materials = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_materials_exploitation",
        created_at=now - timedelta(hours=3),
        snippet="Materials exploitation, trace chemistry, and inert sample handling support.",
        source_url="https://example.com/materials",
        doc_id="MAT-1",
        solicitation_number="SOL-MAT-1",
        awarding_agency_name="Air Force Research Laboratory",
        recipient_uei="UEI-MAT-1",
        naics_code="541715",
        psc_code="B599",
        place_of_performance_state="OH",
        place_of_performance_country="USA",
        notice_award_type="Sources Sought",
        raw_json={},
        keywords=[
            "sam_proxy_materials_exploitation_forensics:materials_forensic_lab_context",
            "sam_proxy_advanced_metrology_trace_analysis:surface_trace_chemistry_context",
            "sam_proxy_controlled_sample_containment_storage:glovebox_inert_sample_handling_context",
        ],
        clauses=[
            _clause("sam_proxy_materials_exploitation_forensics", "materials_forensic_lab_context"),
            _clause("sam_proxy_advanced_metrology_trace_analysis", "surface_trace_chemistry_context"),
            _clause("sam_proxy_controlled_sample_containment_storage", "glovebox_inert_sample_handling_context"),
        ],
        place_text="Wright-Patterson AFB, OH",
    )
    undersea = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_undersea_proxy",
        created_at=now - timedelta(hours=2),
        snippet="Sonar, magnetometer, and secure debris recovery support for undersea search operations.",
        source_url="https://example.com/undersea",
        doc_id="SEA-1",
        solicitation_number="SOL-SEA-1",
        awarding_agency_name="Department of the Navy",
        recipient_uei="UEI-SEA-1",
        naics_code="541330",
        psc_code="R425",
        place_of_performance_state="VA",
        place_of_performance_country="USA",
        notice_award_type="Combined Synopsis/Solicitation",
        raw_json={},
        keywords=[
            "sam_proxy_maritime_remote_recovery_systems:sonar_magnetometer_search_context",
            "sam_proxy_recovery_chain_support:component_fragment_debris_recovery_context",
            "sam_dod_intel_recovery_undersea_support:intel_org_support_context",
        ],
        clauses=[
            _clause("sam_proxy_maritime_remote_recovery_systems", "sonar_magnetometer_search_context"),
            _clause("sam_proxy_recovery_chain_support", "component_fragment_debris_recovery_context"),
            _clause("sam_dod_intel_recovery_undersea_support", "intel_org_support_context"),
        ],
        place_text="Norfolk, VA",
    )
    routine = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_routine_noise",
        entity_id=int(routine_entity.id),
        created_at=now - timedelta(hours=1),
        snippet="NSN janitorial supply purchase with generic UAP chatter and admin facility support.",
        source_url="https://example.com/routine",
        doc_id="ROUTINE-1",
        solicitation_number="SOL-ROUTINE-1",
        awarding_agency_name="Department of the Air Force",
        recipient_uei="UEI-ROUTINE-1",
        naics_code="561720",
        psc_code="S201",
        place_of_performance_state="NM",
        place_of_performance_country="USA",
        notice_award_type="Solicitation",
        raw_json={},
        keywords=[
            "operational_noise_terms:nsn_line_item_commodity_noise",
            "operational_noise_terms:admin_facility_ops_noise",
            "sam_proxy_noise_expansion:generic_facility_maintenance_noise",
            "operational_noise_terms:explicit_uap_lore_noise_terms",
        ],
        clauses=[
            _clause("operational_noise_terms", "nsn_line_item_commodity_noise"),
            _clause("operational_noise_terms", "admin_facility_ops_noise"),
            _clause("sam_proxy_noise_expansion", "generic_facility_maintenance_noise"),
            _clause("operational_noise_terms", "explicit_uap_lore_noise_terms"),
        ],
        place_text="Kirtland AFB, NM",
    )

    db.add_all([secure, materials, undersea, routine])
    db.commit()
    db.refresh(secure)
    db.refresh(materials)
    db.refresh(undersea)
    db.refresh(routine)

    _seed_correlation(
        db,
        event_id=int(secure.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3secure1111111",
        lane="kw_pair",
        score=0.55,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context",
            "keyword_2": "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            "c12": 4,
            "keyword_1_df": 4,
            "keyword_2_df": 4,
            "total_events": 20,
            "score_signal": 0.55,
        },
    )
    _seed_correlation(
        db,
        event_id=int(secure.id),
        correlation_key="same_keyword|SAM.gov|30|kw:sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context",
        lane="same_keyword",
        score=3,
        event_count=3,
    )
    _seed_correlation(
        db,
        event_id=int(secure.id),
        correlation_key="same_sam_naics|SAM.gov|30|naics:541715",
        lane="same_sam_naics",
        score=4,
        event_count=4,
    )

    _seed_correlation(
        db,
        event_id=int(materials.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3mat11111111111",
        lane="kw_pair",
        score=0.6,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_proxy_materials_exploitation_forensics:materials_forensic_lab_context",
            "keyword_2": "sam_proxy_advanced_metrology_trace_analysis:surface_trace_chemistry_context",
            "c12": 4,
            "keyword_1_df": 4,
            "keyword_2_df": 4,
            "total_events": 20,
            "score_signal": 0.6,
        },
    )
    _seed_correlation(
        db,
        event_id=int(materials.id),
        correlation_key="same_keyword|SAM.gov|30|kw:sam_proxy_materials_exploitation_forensics:materials_forensic_lab_context",
        lane="same_keyword",
        score=3,
        event_count=3,
    )
    _seed_correlation(
        db,
        event_id=int(materials.id),
        correlation_key="same_sam_naics|SAM.gov|30|naics:541715",
        lane="same_sam_naics",
        score=5,
        event_count=5,
    )

    _seed_correlation(
        db,
        event_id=int(undersea.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3sea11111111111",
        lane="kw_pair",
        score=0.58,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_proxy_maritime_remote_recovery_systems:sonar_magnetometer_search_context",
            "keyword_2": "sam_proxy_recovery_chain_support:component_fragment_debris_recovery_context",
            "c12": 4,
            "keyword_1_df": 4,
            "keyword_2_df": 4,
            "total_events": 20,
            "score_signal": 0.58,
        },
    )
    _seed_correlation(
        db,
        event_id=int(undersea.id),
        correlation_key="same_keyword|SAM.gov|30|kw:sam_proxy_maritime_remote_recovery_systems:sonar_magnetometer_search_context",
        lane="same_keyword",
        score=3,
        event_count=3,
    )
    _seed_correlation(
        db,
        event_id=int(undersea.id),
        correlation_key="same_sam_naics|SAM.gov|30|naics:541330",
        lane="same_sam_naics",
        score=4,
        event_count=4,
    )

    _seed_correlation(
        db,
        event_id=int(routine.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3noise111111111",
        lane="kw_pair",
        score=0.9,
        event_count=4,
        lanes_hit={
            "keyword_1": "operational_noise_terms:explicit_uap_lore_noise_terms",
            "keyword_2": "operational_noise_terms:admin_facility_ops_noise",
            "c12": 4,
            "keyword_1_df": 4,
            "keyword_2_df": 4,
            "total_events": 20,
            "score_signal": 0.9,
        },
    )
    _seed_correlation(
        db,
        event_id=int(routine.id),
        correlation_key="same_keyword|SAM.gov|30|kw:operational_noise_terms:explicit_uap_lore_noise_terms",
        lane="same_keyword",
        score=5,
        event_count=5,
    )
    _seed_correlation(
        db,
        event_id=int(routine.id),
        correlation_key=f"same_entity|SAM.gov|30|entity:{int(routine_entity.id)}",
        lane="same_entity",
        score=4,
        event_count=4,
    )
    _seed_correlation(
        db,
        event_id=int(routine.id),
        correlation_key="same_sam_naics|SAM.gov|30|naics:561720",
        lane="same_sam_naics",
        score=6,
        event_count=6,
    )

    return {
        "secure": int(secure.id),
        "materials": int(materials.id),
        "undersea": int(undersea.id),
        "routine": int(routine.id),
    }


def test_compute_leads_v3_ranks_proxy_examples_above_routine_noise(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_order.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        _seed_v3_fixture(db)
        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=10,
            scan_limit=50,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    assert scanned == 4
    assert len(ranked) == 4

    ordered_hashes = [event.hash for _score, event, _details in ranked]
    ordered_scores = {event.hash: score for score, event, _details in ranked}
    proxy_hashes = {
        "v3_secure_facility",
        "v3_materials_exploitation",
        "v3_undersea_proxy",
    }

    assert set(ordered_hashes[:3]) == proxy_hashes
    assert ordered_hashes[-1] == "v3_routine_noise"
    assert all(ordered_scores[name] > ordered_scores["v3_routine_noise"] for name in proxy_hashes)


def test_compute_leads_v3_score_details_explain_subscores_and_suppressors(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_details.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        _seed_v3_fixture(db)
        ranked, _scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=10,
            scan_limit=50,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    by_hash = {event.hash: (score, details) for score, event, details in ranked}
    secure_score, secure_details = by_hash["v3_secure_facility"]
    routine_score, routine_details = by_hash["v3_routine_noise"]

    assert secure_details["scoring_version"] == "v3"
    assert secure_details["total_score"] == secure_score
    assert secure_details["proxy_relevance_score"] > 0
    assert secure_details["investigability_score"] > 0
    assert secure_details["corroboration_score"] > 0
    assert secure_details["structural_context_score"] > 0
    assert secure_details["noise_penalty"] == 0
    assert secure_details["pair_bonus"] > 0
    assert secure_details["top_positive_signals"]
    assert secure_details["corroboration_sources"]
    assert secure_details["subscore_math"]["formula"] == (
        "proxy_relevance_score + investigability_score + corroboration_score + structural_context_score - noise_penalty"
    )
    assert secure_details["subscore_math"]["total_score"] == secure_score

    assert routine_details["scoring_version"] == "v3"
    assert routine_details["pair_bonus"] == 0
    assert routine_details["noise_penalty"] > 0
    assert routine_details["top_suppressors"]
    assert any(item.get("is_lore") for item in routine_details["top_suppressors"])
    assert routine_score < secure_score


def test_compute_leads_v3_counts_starter_context_ontology_as_structural_signal(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_starter_context.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        starter = Event(
            category="award",
            source="USAspending",
            hash="v3_starter_context",
            created_at=now - timedelta(hours=2),
            snippet="Base period of performance with option year support services.",
            source_url="https://example.com/starter",
            doc_id="STARTER-1",
            award_id="AWARD-STARTER-1",
            awarding_agency_name="Department of Energy",
            recipient_uei="UEI-STARTER-1",
            naics_code="541611",
            psc_code="R408",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            raw_json={},
            keywords=["procurement_lifecycle:period_of_performance"],
            clauses=[_clause("procurement_lifecycle", "period_of_performance")],
            place_text="Arlington, VA",
        )
        blank = Event(
            category="award",
            source="USAspending",
            hash="v3_metadata_only",
            created_at=now - timedelta(hours=1),
            snippet="Generic support services.",
            source_url="https://example.com/blank",
            doc_id="BLANK-1",
            award_id="AWARD-BLANK-1",
            awarding_agency_name="Department of Energy",
            recipient_uei="UEI-BLANK-1",
            naics_code="541611",
            psc_code="R408",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            raw_json={},
            keywords=[],
            clauses=[],
            place_text="Arlington, VA",
        )
        db.add_all([starter, blank])
        db.commit()

        ranked, scanned = compute_leads(
            db,
            source="USAspending",
            min_score=-10,
            limit=10,
            scan_limit=50,
            scoring_version="v3",
        )

    assert scanned == 2
    by_hash = {event.hash: (score, details) for score, event, details in ranked}
    starter_score, starter_details = by_hash["v3_starter_context"]
    blank_score, blank_details = by_hash["v3_metadata_only"]

    assert starter_details["context_ontology_score"] > 0
    assert starter_details["structural_context_score"] >= blank_details["structural_context_score"]
    assert any(signal.get("bucket") == "structural_context" for signal in starter_details["top_positive_signals"])
    assert starter_score > blank_score


def test_compute_leads_v3_negative_proxy_weights_reduce_score(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_negative_weight.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        positive = Event(
            category="notice",
            source="SAM.gov",
            hash="v3_positive_proxy_weight",
            created_at=now - timedelta(hours=2),
            snippet="Secure facility upgrade support.",
            source_url="https://example.com/pos",
            doc_id="POS-1",
            solicitation_number="SOL-POS-1",
            awarding_agency_name="Department of the Air Force",
            recipient_uei="UEI-POS-1",
            raw_json={},
            keywords=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            clauses=[
                _clause(
                    "sam_proxy_secure_compartmented_facility_engineering",
                    "icd705_scif_sapf_facility_upgrade_context",
                    weight=1,
                )
            ],
        )
        negative = Event(
            category="notice",
            source="SAM.gov",
            hash="v3_negative_proxy_weight",
            created_at=now - timedelta(hours=1),
            snippet="Secure facility upgrade support.",
            source_url="https://example.com/neg",
            doc_id="NEG-1",
            solicitation_number="SOL-NEG-1",
            awarding_agency_name="Department of the Air Force",
            recipient_uei="UEI-NEG-1",
            raw_json={},
            keywords=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            clauses=[
                _clause(
                    "sam_proxy_secure_compartmented_facility_engineering",
                    "icd705_scif_sapf_facility_upgrade_context",
                    weight=-4,
                )
            ],
        )
        db.add_all([positive, negative])
        db.commit()

        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-20,
            limit=10,
            scan_limit=50,
            scoring_version="v3",
        )

    assert scanned == 2
    by_hash = {event.hash: (score, details) for score, event, details in ranked}
    positive_score, positive_details = by_hash["v3_positive_proxy_weight"]
    negative_score, negative_details = by_hash["v3_negative_proxy_weight"]

    assert positive_details["proxy_relevance_score"] > 0
    assert negative_details["proxy_relevance_score"] == 0
    assert negative_details["noise_penalty"] > 0
    assert any(
        signal.get("pack") == "sam_proxy_secure_compartmented_facility_engineering"
        for signal in negative_details["top_suppressors"]
    )
    assert negative_score < positive_score
