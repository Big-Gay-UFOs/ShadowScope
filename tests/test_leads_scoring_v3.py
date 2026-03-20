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


def test_compute_leads_v3_gates_context_only_rows_below_proxy_backed_leads(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_context_gate.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        proxy = Event(
            category="notice",
            source="SAM.gov",
            hash="v3_proxy_rank_gate",
            created_at=now - timedelta(hours=2),
            snippet="ICD-705 secure facility upgrade with DD254 support requirements.",
            source_url="https://example.com/proxy-rank-gate",
            doc_id="PROXY-GATE-1",
            solicitation_number="SOL-PROXY-GATE-1",
            awarding_agency_name="Department of the Air Force",
            recipient_uei="UEI-PROXY-GATE-1",
            naics_code="541715",
            psc_code="R425",
            place_of_performance_state="CA",
            place_of_performance_country="USA",
            notice_award_type="Solicitation",
            raw_json={},
            keywords=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            clauses=[_clause("sam_proxy_secure_compartmented_facility_engineering", "icd705_scif_sapf_facility_upgrade_context")],
            place_text="Edwards AFB, CA",
        )
        context_only = Event(
            category="notice",
            source="SAM.gov",
            hash="v3_context_only_gate",
            created_at=now - timedelta(hours=1),
            snippet="Routine support services solicitation with period of performance, agency, NAICS, and place data.",
            source_url="https://example.com/context-only-gate",
            doc_id="CONTEXT-GATE-1",
            solicitation_number="SOL-CONTEXT-GATE-1",
            awarding_agency_name="Department of the Air Force",
            recipient_uei="UEI-CONTEXT-GATE-1",
            naics_code="541611",
            psc_code="R408",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            notice_award_type="Solicitation",
            raw_json={},
            keywords=[],
            clauses=[],
            place_text="Arlington, VA",
        )
        db.add_all([proxy, context_only])
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
    proxy_score, proxy_details = by_hash["v3_proxy_rank_gate"]
    context_score, context_details = by_hash["v3_context_only_gate"]

    assert proxy_score > context_score
    assert proxy_details["ranking_tier"] in {"highest", "review", "candidate"}
    assert proxy_details["proxy_relevance_score"] > 0
    assert context_details["ranking_tier"] == "context_only"
    assert context_details["proxy_relevance_score"] == 0
    assert context_details["top_rank_gate_penalty"] > 0
    assert context_details["structural_context_score"] > 0


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


def test_compute_leads_v3_non_sam_events_do_not_get_source_metadata_boosts(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_source_gate.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        sam_event = Event(
            category="notice",
            source="SAM.gov",
            hash="v3_sam_metadata_boost",
            created_at=now - timedelta(hours=2),
            snippet="Secure facility upgrade support.",
            source_url="https://example.com/sam",
            doc_id="SAM-BOOST-1",
            award_id="AWARD-SAM-BOOST-1",
            solicitation_number="SOL-SAM-BOOST-1",
            awarding_agency_name="Department of the Air Force",
            recipient_name="Acme Federal",
            recipient_uei="UEI-SAM-BOOST-1",
            naics_code="541715",
            psc_code="R425",
            place_of_performance_state="CA",
            place_of_performance_country="USA",
            notice_award_type="Solicitation",
            raw_json={},
            keywords=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            clauses=[
                _clause(
                    "sam_proxy_secure_compartmented_facility_engineering",
                    "icd705_scif_sapf_facility_upgrade_context",
                )
            ],
            place_text="Edwards AFB, CA",
        )
        usa_event = Event(
            category="notice",
            source="USAspending",
            hash="v3_usaspending_no_metadata_boost",
            created_at=now - timedelta(hours=1),
            snippet="Secure facility upgrade support.",
            source_url="https://example.com/usa",
            doc_id="USA-BOOST-1",
            award_id="AWARD-USA-BOOST-1",
            solicitation_number="SOL-USA-BOOST-1",
            awarding_agency_name="Department of the Air Force",
            recipient_name="Acme Federal",
            recipient_uei="UEI-USA-BOOST-1",
            naics_code="541715",
            psc_code="R425",
            place_of_performance_state="CA",
            place_of_performance_country="USA",
            notice_award_type="Award Notice",
            raw_json={},
            keywords=["sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context"],
            clauses=[
                _clause(
                    "sam_proxy_secure_compartmented_facility_engineering",
                    "icd705_scif_sapf_facility_upgrade_context",
                )
            ],
            place_text="Edwards AFB, CA",
        )
        db.add_all([sam_event, usa_event])
        db.commit()

        ranked, scanned = compute_leads(
            db,
            min_score=-20,
            limit=10,
            scan_limit=50,
            scoring_version="v3",
        )

    assert scanned == 2
    by_hash = {event.hash: (score, details) for score, event, details in ranked}
    sam_score, sam_details = by_hash["v3_sam_metadata_boost"]
    usa_score, usa_details = by_hash["v3_usaspending_no_metadata_boost"]

    assert sam_details["investigability_score"] == usa_details["investigability_score"] + 1
    assert sam_details["structural_context_score"] > usa_details["structural_context_score"]
    assert usa_details["structural_context_score"] == 0
    assert sam_score > usa_score


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


def _seed_v3_rank_regression_fixture(db) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    routine_entity = Entity(name="Routine Ops LLC")
    db.add(routine_entity)
    db.commit()
    db.refresh(routine_entity)

    secure = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_reg_secure_facility",
        created_at=now - timedelta(hours=7),
        snippet="ICD-705 SCIF modernization with DD254 program protection and secure power upgrades.",
        source_url="https://example.com/reg/secure",
        doc_id="REG-SEC-1",
        solicitation_number="REG-SOL-SEC-1",
        awarding_agency_name="Department of the Air Force",
        recipient_uei="UEI-REG-SEC-1",
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
            _clause("sam_proxy_secure_compartmented_facility_engineering", "icd705_scif_sapf_facility_upgrade_context"),
            _clause("sam_proxy_classified_contract_security_admin", "dd254_classification_guide_contract_context"),
            _clause("sam_dod_program_protection_sap", "afosi_program_security_context"),
        ],
        place_text="Edwards AFB, CA",
    )
    range_evt = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_reg_range_test",
        created_at=now - timedelta(hours=6),
        snippet="Edwards range telemetry instrumentation and optical tracking integration support.",
        source_url="https://example.com/reg/range",
        doc_id="REG-RNG-1",
        solicitation_number="REG-SOL-RNG-1",
        awarding_agency_name="Department of the Air Force",
        recipient_uei="UEI-REG-RNG-1",
        naics_code="541715",
        psc_code="R425",
        place_of_performance_state="CA",
        place_of_performance_country="USA",
        notice_award_type="Sources Sought",
        raw_json={},
        keywords=[
            "sam_dod_flight_test_range_instrumentation:edwards_412th_plant42_range_context",
            "sam_dod_flight_test_range_instrumentation:site_range_anchor_support_context",
            "sam_proxy_optical_tracking_transient_collection:optical_ir_tracking_context",
        ],
        clauses=[
            _clause("sam_dod_flight_test_range_instrumentation", "edwards_412th_plant42_range_context"),
            _clause("sam_dod_flight_test_range_instrumentation", "site_range_anchor_support_context"),
            _clause("sam_proxy_optical_tracking_transient_collection", "optical_ir_tracking_context"),
        ],
        place_text="Edwards AFB, CA",
    )
    undersea = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_reg_undersea",
        created_at=now - timedelta(hours=5),
        snippet="Undersea recovery support with sonar magnetometer search and debris handling.",
        source_url="https://example.com/reg/undersea",
        doc_id="REG-SEA-1",
        solicitation_number="REG-SOL-SEA-1",
        awarding_agency_name="Department of the Navy",
        recipient_uei="UEI-REG-SEA-1",
        naics_code="541330",
        psc_code="R425",
        place_of_performance_state="VA",
        place_of_performance_country="USA",
        notice_award_type="Combined Synopsis/Solicitation",
        raw_json={},
        keywords=[
            "sam_dod_intel_recovery_undersea_support:undersea_recovery_support_context",
            "sam_proxy_maritime_remote_recovery_systems:sonar_magnetometer_search_context",
            "sam_proxy_recovery_chain_support:component_fragment_debris_recovery_context",
        ],
        clauses=[
            _clause("sam_dod_intel_recovery_undersea_support", "undersea_recovery_support_context"),
            _clause("sam_proxy_maritime_remote_recovery_systems", "sonar_magnetometer_search_context"),
            _clause("sam_proxy_recovery_chain_support", "component_fragment_debris_recovery_context"),
        ],
        place_text="Norfolk, VA",
    )
    software = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_reg_software_support",
        created_at=now - timedelta(hours=4),
        snippet="Follow-on IDIQ mission systems integration support with DD254 annex and incumbent transition.",
        source_url="https://example.com/reg/software",
        doc_id="REG-SW-1",
        solicitation_number="REG-SOL-SW-1",
        awarding_agency_name="Department of the Air Force",
        recipient_name="Mission Systems Group",
        recipient_uei="UEI-REG-SW-1",
        naics_code="541511",
        psc_code="D308",
        place_of_performance_state="VA",
        place_of_performance_country="USA",
        notice_award_type="Solicitation",
        raw_json={},
        keywords=[
            "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
            "sam_proxy_procurement_continuity_classified_followon:idiq_task_order_secure_support_context",
            "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            "sam_dod_advanced_aerospace_support:advanced_air_vehicle_integration_context",
        ],
        clauses=[
            _clause("sam_proxy_procurement_continuity_classified_followon", "sole_source_follow_on_classified_context"),
            _clause("sam_proxy_procurement_continuity_classified_followon", "idiq_task_order_secure_support_context"),
            _clause("sam_proxy_classified_contract_security_admin", "dd254_classification_guide_contract_context"),
            _clause("sam_dod_advanced_aerospace_support", "advanced_air_vehicle_integration_context"),
        ],
        place_text="Arlington, VA",
    )
    starter_pair = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_reg_starter_pair_noise",
        entity_id=int(routine_entity.id),
        created_at=now - timedelta(hours=3),
        snippet="Sources sought follow-on admin support with security coordination and office transition services.",
        source_url="https://example.com/reg/starter-pair",
        doc_id="REG-ST-1",
        solicitation_number="REG-SOL-ST-1",
        awarding_agency_name="Department of the Air Force",
        recipient_name="Routine Ops LLC",
        recipient_uei="UEI-REG-ST-1",
        naics_code="541611",
        psc_code="R499",
        place_of_performance_state="NM",
        place_of_performance_country="USA",
        notice_award_type="Sources Sought",
        raw_json={},
        keywords=[
            "sam_procurement_starter:naics_context",
            "sam_procurement_starter:response_deadline_present",
            "sam_procurement_starter:solicitation_number_present",
            "sam_procurement_starter:agency_path_code_present",
            "sam_proxy_classified_contract_security_admin:visit_authorization_courier_access_context",
        ],
        clauses=[
            _clause("sam_procurement_starter", "naics_context", weight=2),
            _clause("sam_procurement_starter", "response_deadline_present", weight=1),
            _clause("sam_procurement_starter", "solicitation_number_present", weight=1),
            _clause("sam_procurement_starter", "agency_path_code_present", weight=1),
            _clause("sam_proxy_classified_contract_security_admin", "visit_authorization_courier_access_context"),
        ],
        place_text="Kirtland AFB, NM",
    )
    routine = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_reg_routine_noise",
        entity_id=int(routine_entity.id),
        created_at=now - timedelta(hours=2),
        snippet="NSN janitorial office furniture and grounds maintenance procurement with admin facility support.",
        source_url="https://example.com/reg/routine",
        doc_id="REG-NOISE-1",
        solicitation_number="REG-SOL-NOISE-1",
        awarding_agency_name="Department of the Air Force",
        recipient_name="Routine Ops LLC",
        recipient_uei="UEI-REG-NOISE-1",
        naics_code="561210",
        psc_code="S201",
        place_of_performance_state="NM",
        place_of_performance_country="USA",
        notice_award_type="Solicitation",
        raw_json={},
        keywords=[
            "sam_procurement_starter:naics_context",
            "sam_procurement_starter:response_deadline_present",
            "sam_procurement_starter:solicitation_number_present",
            "operational_noise_terms:nsn_line_item_commodity_noise",
            "operational_noise_terms:admin_facility_ops_noise",
            "sam_proxy_noise_expansion:generic_facility_maintenance_noise",
        ],
        clauses=[
            _clause("sam_procurement_starter", "naics_context", weight=2),
            _clause("sam_procurement_starter", "response_deadline_present", weight=1),
            _clause("sam_procurement_starter", "solicitation_number_present", weight=1),
            _clause("operational_noise_terms", "nsn_line_item_commodity_noise"),
            _clause("operational_noise_terms", "admin_facility_ops_noise"),
            _clause("sam_proxy_noise_expansion", "generic_facility_maintenance_noise"),
        ],
        place_text="Kirtland AFB, NM",
    )
    ambiguous = Event(
        category="notice",
        source="SAM.gov",
        hash="v3_reg_ambiguous_mid",
        created_at=now - timedelta(hours=1),
        snippet="Follow-on option period support with one secure facility reference and limited agency context.",
        source_url="https://example.com/reg/ambiguous",
        doc_id="REG-AMB-1",
        solicitation_number="REG-SOL-AMB-1",
        awarding_agency_name="Department of the Air Force",
        recipient_uei="UEI-REG-AMB-1",
        naics_code="541611",
        psc_code="R499",
        place_of_performance_state="VA",
        place_of_performance_country="USA",
        notice_award_type="Presolicitation",
        raw_json={},
        keywords=[
            "sam_procurement_starter:solicitation_number_present",
            "sam_proxy_procurement_continuity_classified_followon:classified_annex_continuity_context",
        ],
        clauses=[
            _clause("sam_procurement_starter", "solicitation_number_present", weight=1),
            _clause("sam_proxy_procurement_continuity_classified_followon", "classified_annex_continuity_context"),
        ],
        place_text="Arlington, VA",
    )

    db.add_all([secure, range_evt, undersea, software, starter_pair, routine, ambiguous])
    db.commit()
    for event in [secure, range_evt, undersea, software, starter_pair, routine, ambiguous]:
        db.refresh(event)

    _seed_correlation(
        db,
        event_id=int(secure.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3regsecure",
        lane="kw_pair",
        score=0.62,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context",
            "keyword_2": "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            "score_signal": 0.62,
        },
        summary="secure proxy pair",
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
        correlation_key="same_doc_id|SAM.gov|30|doc:reg-sec-1",
        lane="same_doc_id",
        score=3,
        event_count=3,
    )

    _seed_correlation(
        db,
        event_id=int(range_evt.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3regrange",
        lane="kw_pair",
        score=0.58,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_dod_flight_test_range_instrumentation:edwards_412th_plant42_range_context",
            "keyword_2": "sam_proxy_optical_tracking_transient_collection:optical_ir_tracking_context",
            "score_signal": 0.58,
        },
        summary="range proxy pair",
    )
    _seed_correlation(
        db,
        event_id=int(range_evt.id),
        correlation_key="same_keyword|SAM.gov|30|kw:sam_dod_flight_test_range_instrumentation:edwards_412th_plant42_range_context",
        lane="same_keyword",
        score=3,
        event_count=3,
    )
    _seed_correlation(
        db,
        event_id=int(range_evt.id),
        correlation_key="same_place_region|SAM.gov|30|place:reg-range-ca",
        lane="same_place_region",
        score=3,
        event_count=3,
    )

    _seed_correlation(
        db,
        event_id=int(undersea.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3regundersea",
        lane="kw_pair",
        score=0.59,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_dod_intel_recovery_undersea_support:undersea_recovery_support_context",
            "keyword_2": "sam_proxy_maritime_remote_recovery_systems:sonar_magnetometer_search_context",
            "score_signal": 0.59,
        },
        summary="undersea proxy pair",
    )
    _seed_correlation(
        db,
        event_id=int(undersea.id),
        correlation_key="same_keyword|SAM.gov|30|kw:sam_dod_intel_recovery_undersea_support:undersea_recovery_support_context",
        lane="same_keyword",
        score=3,
        event_count=3,
    )
    _seed_correlation(
        db,
        event_id=int(undersea.id),
        correlation_key="same_place_region|SAM.gov|30|place:reg-undersea-va",
        lane="same_place_region",
        score=3,
        event_count=3,
    )

    _seed_correlation(
        db,
        event_id=int(software.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3regsoftware",
        lane="kw_pair",
        score=0.61,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
            "keyword_2": "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context",
            "score_signal": 0.61,
        },
        summary="software proxy pair",
    )
    _seed_correlation(
        db,
        event_id=int(software.id),
        correlation_key="same_keyword|SAM.gov|30|kw:sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
        lane="same_keyword",
        score=3,
        event_count=3,
    )
    _seed_correlation(
        db,
        event_id=int(software.id),
        correlation_key="sam_usaspending_candidate_join|SAM.gov|365|reg-sw-1",
        lane="sam_usaspending_candidate_join",
        score=65,
        event_count=3,
        lanes_hit={
            "score_signal": 65,
            "confidence_score": 65,
            "evidence_types": ["identifier_exact", "contract_family"],
        },
        summary="cross-source incumbent lineage",
    )

    _seed_correlation(
        db,
        event_id=int(starter_pair.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3regstarter",
        lane="kw_pair",
        score=0.74,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_procurement_starter:naics_context",
            "keyword_2": "sam_procurement_starter:response_deadline_present",
            "score_signal": 0.74,
        },
        summary="starter structural pair",
    )
    _seed_correlation(
        db,
        event_id=int(starter_pair.id),
        correlation_key="same_sam_naics|SAM.gov|30|naics:v3regstarter",
        lane="same_sam_naics",
        score=6,
        event_count=6,
    )
    _seed_correlation(
        db,
        event_id=int(starter_pair.id),
        correlation_key="same_agency|SAM.gov|30|agency:v3regstarter",
        lane="same_agency",
        score=5,
        event_count=5,
    )

    _seed_correlation(
        db,
        event_id=int(routine.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3regroutine",
        lane="kw_pair",
        score=0.72,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_procurement_starter:naics_context",
            "keyword_2": "sam_procurement_starter:response_deadline_present",
            "score_signal": 0.72,
        },
        summary="routine structural pair",
    )
    _seed_correlation(
        db,
        event_id=int(routine.id),
        correlation_key="same_entity|SAM.gov|30|entity:v3regroutine",
        lane="same_entity",
        score=4,
        event_count=4,
    )
    _seed_correlation(
        db,
        event_id=int(routine.id),
        correlation_key="same_sam_naics|SAM.gov|30|naics:v3regroutine",
        lane="same_sam_naics",
        score=5,
        event_count=5,
    )

    _seed_correlation(
        db,
        event_id=int(ambiguous.id),
        correlation_key="kw_pair|SAM.gov|30|pair:v3regambiguous",
        lane="kw_pair",
        score=0.41,
        event_count=4,
        lanes_hit={
            "keyword_1": "sam_procurement_starter:solicitation_number_present",
            "keyword_2": "sam_proxy_procurement_continuity_classified_followon:classified_annex_continuity_context",
            "score_signal": 0.41,
        },
        summary="mixed weak pair",
    )
    _seed_correlation(
        db,
        event_id=int(ambiguous.id),
        correlation_key="same_sam_naics|SAM.gov|30|naics:v3regambiguous",
        lane="same_sam_naics",
        score=4,
        event_count=4,
    )

    return {
        "secure": int(secure.id),
        "range": int(range_evt.id),
        "undersea": int(undersea.id),
        "software": int(software.id),
        "starter_pair": int(starter_pair.id),
        "routine": int(routine.id),
        "ambiguous": int(ambiguous.id),
    }


def test_compute_leads_v3_proxy_rich_regression_fixture_outranks_starter_and_routine_noise(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_regression.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        _seed_v3_rank_regression_fixture(db)
        ranked, scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=20,
            scan_limit=100,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    assert scanned == 7
    ordered_hashes = [event.hash for _score, event, _details in ranked]
    ordered_scores = {event.hash: score for score, event, _details in ranked}
    strong_hashes = {
        "v3_reg_secure_facility",
        "v3_reg_range_test",
        "v3_reg_undersea",
        "v3_reg_software_support",
    }

    assert set(ordered_hashes[:4]) == strong_hashes
    assert ordered_hashes[4] == "v3_reg_ambiguous_mid"
    assert ordered_hashes[5] == "v3_reg_starter_pair_noise"
    assert ordered_hashes[6] == "v3_reg_routine_noise"
    assert min(ordered_scores[name] for name in strong_hashes) > ordered_scores["v3_reg_ambiguous_mid"]
    assert ordered_scores["v3_reg_ambiguous_mid"] > ordered_scores["v3_reg_starter_pair_noise"] > ordered_scores["v3_reg_routine_noise"]


def test_compute_leads_v3_caps_starter_only_pair_bonus_and_exposes_pair_quality(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_pair_caps.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        _seed_v3_rank_regression_fixture(db)
        ranked, _scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=20,
            scan_limit=100,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    by_hash = {event.hash: (score, details) for score, event, details in ranked}
    starter_score, starter_details = by_hash["v3_reg_starter_pair_noise"]
    software_score, software_details = by_hash["v3_reg_software_support"]

    assert starter_details["pair_bonus_raw"] > starter_details["pair_bonus_applied"]
    assert starter_details["pair_bonus_applied"] == 1
    assert starter_details["pair_bonus_quality_cap"] == 1
    assert starter_details["starter_only_pair_count"] == 1
    assert starter_details["pair_quality_counts"]["starter_only"] == 1
    assert software_details["pair_bonus_applied"] > starter_details["pair_bonus_applied"]
    assert software_details["pair_bonus_quality_cap"] > starter_details["pair_bonus_quality_cap"]
    assert starter_score < software_score


def test_compute_leads_v3_keeps_classified_visit_rules_out_of_routine_access_control_hints(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_visit_rules.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        _seed_v3_rank_regression_fixture(db)
        ranked, _scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=20,
            scan_limit=100,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    by_hash = {event.hash: details for _score, event, details in ranked}
    starter_details = by_hash["v3_reg_starter_pair_noise"]

    assert "site_security_access_control" not in starter_details["classification_tags"]
    assert "site_security_access_control" not in starter_details["routine_noise_tags"]


def test_compute_leads_v3_ignores_starter_only_pairs_for_family_relevance_bonus(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_family_pair_guard.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        event = Event(
            category="notice",
            source="SAM.gov",
            hash="v3_family_pair_guard",
            created_at=now,
            snippet="IDIQ follow-on task order continuity support with secure admin handling.",
            source_url="https://example.com/reg/family-pair-guard",
            doc_id="REG-FAM-1",
            solicitation_number="REG-SOL-FAM-1",
            awarding_agency_name="Department of the Air Force",
            recipient_name="Continuity Ops LLC",
            recipient_uei="UEI-REG-FAM-1",
            naics_code="541611",
            psc_code="R499",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            notice_award_type="Sources Sought",
            raw_json={},
            keywords=[
                "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context",
                "sam_procurement_starter:idiq_vehicle",
                "sam_procurement_starter:task_or_delivery_order",
            ],
            clauses=[
                _clause(
                    "sam_proxy_procurement_continuity_classified_followon",
                    "sole_source_follow_on_classified_context",
                ),
                _clause("sam_procurement_starter", "idiq_vehicle", weight=1),
                _clause("sam_procurement_starter", "task_or_delivery_order", weight=1),
            ],
            place_text="Arlington, VA",
        )
        db.add(event)
        db.commit()
        db.refresh(event)

        _seed_correlation(
            db,
            event_id=int(event.id),
            correlation_key="kw_pair|SAM.gov|30|pair:v3familypairguard",
            lane="kw_pair",
            score=0.66,
            event_count=4,
            lanes_hit={
                "keyword_1": "sam_procurement_starter:idiq_vehicle",
                "keyword_2": "sam_procurement_starter:task_or_delivery_order",
                "score_signal": 0.66,
            },
            summary="starter lineage pair",
        )

        ranked, _scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=10,
            scan_limit=20,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    by_hash = {event.hash: (score, details) for score, event, details in ranked}
    score, details = by_hash["v3_family_pair_guard"]
    family_rows = {
        row["family"]: row
        for row in details["family_relevant_families"]
    }

    assert score > 0
    assert details["pair_quality_counts"]["starter_only"] == 1
    assert details["pair_bonus_applied"] == 1
    assert details["family_relevance_bonus"] == 0
    assert family_rows["vendor_network_contract_lineage"]["pair_count"] == 0
    assert family_rows["vendor_network_contract_lineage"]["bonus"] == 0


def test_compute_leads_v3_regression_fixture_improves_score_spread_without_forcing_ambiguous_extremes(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_spread.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        _seed_v3_rank_regression_fixture(db)
        ranked, _scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=20,
            scan_limit=100,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    by_hash = {event.hash: score for score, event, _details in ranked}
    strong_scores = [
        by_hash["v3_reg_secure_facility"],
        by_hash["v3_reg_range_test"],
        by_hash["v3_reg_undersea"],
        by_hash["v3_reg_software_support"],
    ]
    ambiguous_score = by_hash["v3_reg_ambiguous_mid"]
    starter_score = by_hash["v3_reg_starter_pair_noise"]

    assert len(set(strong_scores)) >= 3
    assert max(strong_scores) - min(strong_scores) >= 2
    assert max(strong_scores) > ambiguous_score > starter_score


def test_compute_leads_v3_regression_fixture_keeps_suppressors_and_corroboration_visible(tmp_path):
    db_url = f"sqlite:///{(tmp_path / 'leads_scoring_v3_explainability_regression.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        _seed_v3_rank_regression_fixture(db)
        ranked, _scanned = compute_leads(
            db,
            source="SAM.gov",
            min_score=-50,
            limit=20,
            scan_limit=100,
            scoring_version="v3",
            pair_signal_threshold=0.15,
            pair_event_count_threshold=2,
        )

    by_hash = {event.hash: (score, details) for score, event, details in ranked}
    secure_score, secure_details = by_hash["v3_reg_secure_facility"]
    routine_score, routine_details = by_hash["v3_reg_routine_noise"]

    assert secure_details["corroboration_sources"]
    assert secure_details["cross_lane_bonus"] > 0
    assert secure_details["family_relevance_bonus"] > 0
    assert secure_details["family_relevant_families"]
    assert secure_details["subscore_math"]["components"]["pair_bonus_quality_cap"] >= secure_details["pair_bonus_applied"]

    assert routine_details["top_suppressors"]
    assert routine_details["routine_noise_surcharge"] > 0
    assert routine_details["routine_noise_hit_count"] >= 2
    assert routine_details["noise_penalty"] > routine_details["noise_penalty_core"]
    assert routine_details["weak_proxy_context_cap_applied"] is True
    assert routine_score < secure_score
