from __future__ import annotations

from collections import Counter
from typing import Any


LEAD_FAMILY_TAXONOMY: dict[str, dict[str, Any]] = {
    "facility_security_hardening": {
        "label": "Facility Security Hardening",
        "min_total_score": 4,
        "min_ontology_score": 2,
        "min_corroboration_score": 1,
        "pack_weights": {
            "sam_proxy_secure_compartmented_facility_engineering": 3,
            "sam_proxy_classified_contract_security_admin": 2,
            "sam_dod_program_protection_sap": 2,
            "sam_dod_hardened_subsurface_infrastructure": 2,
        },
        "rule_weights": {
            "sam_proxy_secure_compartmented_facility_engineering:icd705_scif_sapf_facility_upgrade_context": 3,
            "sam_proxy_secure_compartmented_facility_engineering:tempest_emanations_shielding_context": 3,
            "sam_proxy_secure_compartmented_facility_engineering:secure_power_env_control_hardening_context": 2,
            "sam_dod_program_protection_sap:sap_facility_modernization_context": 3,
            "sam_dod_program_protection_sap:afosi_program_security_context": 2,
            "sam_dod_hardened_subsurface_infrastructure:subsurface_shaft_tunneling_context": 2,
            "sam_dod_hardened_subsurface_infrastructure:hardened_portal_life_support_context": 2,
            "sam_dod_hardened_subsurface_infrastructure:site_hardened_infrastructure_pair_context": 3,
            "sam_proxy_classified_contract_security_admin:comsec_type1_secure_comms_context": 2,
        },
        "lane_weights": {
            "same_doc_id": 2,
            "same_contract_id": 2,
            "same_agency": 1,
            "same_place_region": 1,
            "same_psc": 1,
            "same_naics": 1,
            "same_sam_naics": 1,
            "same_keyword": 1,
            "kw_pair": 1,
        },
        "candidate_evidence_weights": {
            "identifier_exact": 2,
            "contract_family": 2,
            "awarding_agency": 1,
            "funding_agency": 1,
            "place_region": 1,
            "psc": 1,
            "naics": 1,
        },
        "source_weights": {
            "SAM.gov": 1,
            "USAspending": 1,
        },
    },
    "exploitation_materials_handling": {
        "label": "Exploitation Materials Handling",
        "min_total_score": 4,
        "min_ontology_score": 2,
        "min_corroboration_score": 1,
        "pack_weights": {
            "sam_proxy_materials_exploitation_forensics": 3,
            "sam_proxy_recovery_chain_support": 2,
            "sam_proxy_controlled_sample_containment_storage": 3,
            "sam_proxy_advanced_metrology_trace_analysis": 2,
            "sam_proxy_lab_office_anchor_expansion": 1,
            "sam_dod_program_protection_sap": 2,
            "sam_dod_intel_recovery_undersea_support": 1,
        },
        "rule_weights": {
            "sam_proxy_materials_exploitation_forensics:materials_forensic_lab_context": 3,
            "sam_proxy_materials_exploitation_forensics:recovered_or_foreign_material_exploitation_context": 3,
            "sam_proxy_materials_exploitation_forensics:advanced_material_signature_context": 2,
            "sam_proxy_recovery_chain_support:chain_of_custody_secure_material_handling_context": 3,
            "sam_proxy_controlled_sample_containment_storage:glovebox_inert_sample_handling_context": 3,
            "sam_proxy_controlled_sample_containment_storage:contamination_control_sample_archive_context": 2,
            "sam_proxy_controlled_sample_containment_storage:evidence_preservation_secure_storage_context": 2,
            "sam_proxy_advanced_metrology_trace_analysis:surface_trace_chemistry_context": 2,
            "sam_proxy_advanced_metrology_trace_analysis:structural_mechanical_metrology_context": 2,
            "sam_proxy_advanced_metrology_trace_analysis:electron_microstructure_context": 2,
            "sam_proxy_lab_office_anchor_expansion:lab_secure_materials_anchor_context": 2,
            "sam_dod_program_protection_sap:doe_nnsa_material_handling_context": 3,
            "sam_dod_program_protection_sap:doe_secure_transport_response_context": 2,
            "sam_dod_intel_recovery_undersea_support:sensitive_recovery_exploitation_support": 2,
        },
        "lane_weights": {
            "same_doc_id": 2,
            "same_contract_id": 2,
            "same_agency": 1,
            "same_place_region": 1,
            "same_psc": 1,
            "same_naics": 1,
            "same_keyword": 1,
            "kw_pair": 1,
        },
        "candidate_evidence_weights": {
            "identifier_exact": 2,
            "contract_family": 2,
            "recipient_uei": 2,
            "recipient_name": 2,
            "awarding_agency": 1,
            "funding_agency": 1,
            "psc": 1,
            "naics": 1,
        },
        "source_weights": {
            "SAM.gov": 1,
            "USAspending": 1,
        },
    },
    "undersea_recovery_salvage": {
        "label": "Undersea Recovery Salvage",
        "min_total_score": 4,
        "min_ontology_score": 2,
        "min_corroboration_score": 1,
        "pack_weights": {
            "sam_dod_intel_recovery_undersea_support": 3,
            "sam_proxy_maritime_remote_recovery_systems": 3,
            "sam_proxy_recovery_chain_support": 1,
            "sam_proxy_operator_site_program_pairs": 1,
        },
        "rule_weights": {
            "sam_dod_intel_recovery_undersea_support:undersea_recovery_support_context": 3,
            "sam_dod_intel_recovery_undersea_support:undersea_precision_capability_pair_context": 3,
            "sam_dod_intel_recovery_undersea_support:deep_submergence_platform_support_context": 3,
            "sam_dod_intel_recovery_undersea_support:sensitive_recovery_exploitation_support": 2,
            "sam_proxy_maritime_remote_recovery_systems:sonar_magnetometer_search_context": 3,
            "sam_proxy_maritime_remote_recovery_systems:rov_lars_heavy_lift_context": 3,
            "sam_proxy_maritime_remote_recovery_systems:maritime_recovery_object_context": 2,
            "sam_proxy_recovery_chain_support:component_fragment_debris_recovery_context": 1,
        },
        "lane_weights": {
            "same_doc_id": 2,
            "same_contract_id": 2,
            "same_agency": 1,
            "same_place_region": 1,
            "same_psc": 1,
            "same_naics": 1,
            "same_keyword": 1,
            "kw_pair": 1,
        },
        "candidate_evidence_weights": {
            "identifier_exact": 2,
            "contract_family": 2,
            "awarding_agency": 1,
            "place_region": 1,
            "psc": 1,
            "naics": 1,
        },
        "source_weights": {
            "SAM.gov": 1,
            "USAspending": 1,
        },
    },
    "compartmented_support_intel": {
        "label": "Compartmented Support Intel",
        "min_total_score": 4,
        "min_ontology_score": 2,
        "min_corroboration_score": 1,
        "pack_weights": {
            "sam_proxy_classified_contract_security_admin": 3,
            "sam_dod_program_protection_sap": 2,
            "sam_dod_intel_recovery_undersea_support": 2,
            "sam_proxy_signature_phenomenology_measurement": 1,
        },
        "rule_weights": {
            "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context": 3,
            "sam_proxy_classified_contract_security_admin:comsec_type1_secure_comms_context": 2,
            "sam_proxy_classified_contract_security_admin:visit_authorization_courier_access_context": 2,
            "sam_dod_program_protection_sap:afosi_program_security_context": 2,
            "sam_dod_program_protection_sap:program_protection_plan_support": 2,
            "sam_dod_program_protection_sap:saf_org_code_program_protection_context": 2,
            "sam_dod_intel_recovery_undersea_support:intel_org_support_context": 3,
            "sam_proxy_signature_phenomenology_measurement:masint_signature_phenomenology_context": 2,
            "sam_proxy_signature_phenomenology_measurement:eoir_multispectral_collection_context": 1,
        },
        "lane_weights": {
            "same_doc_id": 2,
            "same_contract_id": 2,
            "same_agency": 1,
            "same_psc": 1,
            "same_naics": 1,
            "same_keyword": 1,
            "kw_pair": 1,
        },
        "candidate_evidence_weights": {
            "identifier_exact": 2,
            "contract_family": 2,
            "recipient_uei": 1,
            "recipient_name": 1,
            "awarding_agency": 1,
            "funding_agency": 1,
        },
        "source_weights": {
            "SAM.gov": 1,
            "USAspending": 1,
        },
    },
    "range_test_infrastructure": {
        "label": "Range Test Infrastructure",
        "min_total_score": 4,
        "min_ontology_score": 2,
        "min_corroboration_score": 1,
        "pack_weights": {
            "sam_dod_flight_test_range_instrumentation": 3,
            "sam_proxy_optical_tracking_transient_collection": 2,
            "sam_proxy_signature_phenomenology_measurement": 2,
            "sam_proxy_lab_office_anchor_expansion": 1,
            "sam_dod_advanced_aerospace_support": 1,
        },
        "rule_weights": {
            "sam_dod_flight_test_range_instrumentation:edwards_412th_plant42_range_context": 3,
            "sam_dod_flight_test_range_instrumentation:nawcwd_china_lake_instrumentation_context": 3,
            "sam_dod_flight_test_range_instrumentation:range_telemetry_support_services": 2,
            "sam_dod_flight_test_range_instrumentation:site_range_anchor_support_context": 3,
            "sam_dod_flight_test_range_instrumentation:maritime_range_anchor_support_context": 2,
            "sam_dod_flight_test_range_instrumentation:operator_site_pair_support_context": 2,
            "sam_proxy_optical_tracking_transient_collection:optical_ir_tracking_context": 2,
            "sam_proxy_optical_tracking_transient_collection:high_speed_photometry_spectrograph_context": 2,
            "sam_proxy_optical_tracking_transient_collection:trajectory_reconstruction_timing_context": 2,
            "sam_proxy_signature_phenomenology_measurement:chamber_or_signature_measurement_facility_context": 2,
            "sam_proxy_signature_phenomenology_measurement:eoir_multispectral_collection_context": 2,
            "sam_proxy_lab_office_anchor_expansion:aedc_naval_test_anchor_context": 2,
            "sam_dod_advanced_aerospace_support:low_observable_rcs_support_context": 1,
        },
        "lane_weights": {
            "same_doc_id": 2,
            "same_contract_id": 2,
            "same_agency": 1,
            "same_place_region": 1,
            "same_psc": 1,
            "same_naics": 1,
            "same_sam_naics": 1,
            "same_keyword": 1,
            "kw_pair": 1,
        },
        "candidate_evidence_weights": {
            "identifier_exact": 2,
            "contract_family": 2,
            "awarding_agency": 1,
            "place_region": 1,
            "psc": 1,
            "naics": 1,
        },
        "source_weights": {
            "SAM.gov": 1,
            "USAspending": 1,
        },
    },
    "vendor_network_contract_lineage": {
        "label": "Vendor Network Contract Lineage",
        "min_total_score": 4,
        "min_ontology_score": 1,
        "min_corroboration_score": 2,
        "pack_weights": {
            "sam_proxy_procurement_continuity_classified_followon": 3,
            "sam_proxy_operator_site_program_pairs": 2,
            "sam_proxy_classified_contract_security_admin": 1,
            "sam_procurement_starter": 1,
        },
        "rule_weights": {
            "sam_proxy_procurement_continuity_classified_followon:sole_source_follow_on_classified_context": 3,
            "sam_proxy_procurement_continuity_classified_followon:idiq_task_order_secure_support_context": 2,
            "sam_proxy_procurement_continuity_classified_followon:classified_annex_continuity_context": 2,
            "sam_proxy_operator_site_program_pairs:operator_site_pair_proxy_context": 2,
            "sam_proxy_operator_site_program_pairs:office_site_pair_proxy_context": 2,
            "sam_proxy_classified_contract_security_admin:dd254_classification_guide_contract_context": 1,
            "sam_procurement_starter:idiq_vehicle": 1,
            "sam_procurement_starter:task_or_delivery_order": 1,
        },
        "lane_weights": {
            "same_award_id": 3,
            "same_contract_id": 3,
            "same_doc_id": 2,
            "same_entity": 2,
            "same_uei": 2,
            "same_agency": 1,
            "same_psc": 1,
            "same_naics": 1,
            "same_place_region": 1,
            "same_keyword": 1,
            "kw_pair": 1,
            "sam_usaspending_candidate_join": 2,
        },
        "candidate_evidence_weights": {
            "identifier_exact": 3,
            "contract_family": 3,
            "recipient_uei": 2,
            "recipient_name": 2,
            "awarding_agency": 1,
            "funding_agency": 1,
            "place_region": 1,
            "psc": 1,
            "naics": 1,
        },
        "source_weights": {
            "USAspending": 1,
            "SAM.gov": 1,
        },
    },
}


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_key(value: Any) -> str:
    return _norm_text(value).lower()


def _norm_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _build_ontology_index(details: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    for clause in _norm_list(details.get("matched_ontology_clauses")):
        if not isinstance(clause, dict):
            continue
        pack = _norm_key(clause.get("pack"))
        rule = _norm_key(clause.get("rule"))
        if not pack:
            continue
        rule_key = f"{pack}:{rule}" if rule else pack
        if rule_key in seen:
            continue
        seen.add(rule_key)
        items.append(
            {
                "pack": pack,
                "rule": rule,
                "rule_key": rule_key,
                "weight": max(_safe_int(clause.get("weight"), default=1), 1),
                "field": _norm_text(clause.get("field")) or None,
                "match": _norm_text(clause.get("match")) or None,
            }
        )

    for rule_key in _norm_list(details.get("matched_ontology_rules")):
        text = _norm_key(rule_key)
        if not text or text in seen:
            continue
        pack, _, rule = text.partition(":")
        if not pack:
            continue
        seen.add(text)
        items.append(
            {
                "pack": pack,
                "rule": rule,
                "rule_key": text,
                "weight": 1,
                "field": None,
                "match": None,
            }
        )

    return items


def _lane_counter(details: dict[str, Any]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for correlation in _norm_list(details.get("contributing_correlations")):
        if not isinstance(correlation, dict):
            continue
        lane = _norm_text(correlation.get("lane"))
        if lane:
            counter[lane] += 1
    return counter


def _build_candidate_join_evidence(
    *,
    details: dict[str, Any],
    linked_records_by_correlation: dict[int, list[dict[str, Any]]] | None,
) -> list[dict[str, Any]]:
    linked_lookup = linked_records_by_correlation or {}
    items: list[dict[str, Any]] = []
    for correlation in _norm_list(details.get("contributing_correlations")):
        if not isinstance(correlation, dict):
            continue
        if _norm_text(correlation.get("lane")) != "sam_usaspending_candidate_join":
            continue
        correlation_id = _safe_int(correlation.get("correlation_id"), default=0)
        linked_records = [dict(item) for item in linked_lookup.get(correlation_id, []) if isinstance(item, dict)]
        linked_sources = sorted({_norm_text(item.get("source")) for item in linked_records if _norm_text(item.get("source"))})
        items.append(
            {
                "correlation_id": correlation_id or None,
                "status": "candidate",
                "score_signal": correlation.get("score_signal"),
                "confidence_score": correlation.get("confidence_score", correlation.get("score_signal")),
                "likely_incumbent": bool(correlation.get("likely_incumbent")),
                "time_delta_days": correlation.get("time_delta_days"),
                "evidence_types": [str(item) for item in _norm_list(correlation.get("evidence_types")) if _norm_text(item)],
                "evidence": [dict(item) for item in _norm_list(correlation.get("candidate_join_evidence")) if isinstance(item, dict)],
                "matched_values": dict(correlation.get("matched_values") or {}) if isinstance(correlation.get("matched_values"), dict) else {},
                "linked_sources": linked_sources,
                "linked_records": linked_records[:3],
            }
        )
    return items


def build_corroboration_summary(
    *,
    details: dict[str, Any],
    linked_source_summary: list[dict[str, Any]] | None = None,
    linked_records_by_correlation: dict[int, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    lane_counts = _lane_counter(details)
    candidate_join_evidence = _build_candidate_join_evidence(
        details=details,
        linked_records_by_correlation=linked_records_by_correlation,
    )
    return {
        "correlation_types_hit": sorted(lane_counts.keys()),
        "correlation_type_counts": {lane: int(count) for lane, count in sorted(lane_counts.items())},
        "candidate_join_evidence": candidate_join_evidence,
        "linked_source_summary": [dict(item) for item in _norm_list(linked_source_summary) if isinstance(item, dict)],
    }


def _family_ontology_matches(
    *,
    family_spec: dict[str, Any],
    ontology_index: list[dict[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    pack_weights = {
        _norm_key(key): _safe_int(value, default=0)
        for key, value in dict(family_spec.get("pack_weights") or {}).items()
    }
    rule_weights = {
        _norm_key(key): _safe_int(value, default=0)
        for key, value in dict(family_spec.get("rule_weights") or {}).items()
    }

    score = 0
    matches: list[dict[str, Any]] = []
    for item in ontology_index:
        rule_key = _norm_key(item.get("rule_key"))
        pack = _norm_key(item.get("pack"))
        configured_weight = rule_weights.get(rule_key)
        if configured_weight is None:
            configured_weight = pack_weights.get(pack)
        if configured_weight is None or configured_weight <= 0:
            continue
        contribution = int(configured_weight) + max(min(_safe_int(item.get("weight"), default=1) - 1, 2), 0)
        score += contribution
        matches.append(
            {
                "pack": item.get("pack"),
                "rule": item.get("rule"),
                "weight": contribution,
                "field": item.get("field"),
                "match": item.get("match"),
            }
        )

    matches.sort(
        key=lambda item: (
            -_safe_int(item.get("weight"), default=0),
            _norm_text(item.get("pack")),
            _norm_text(item.get("rule")),
        )
    )
    return score, matches


def _family_corroboration_matches(
    *,
    family_spec: dict[str, Any],
    details: dict[str, Any],
    corroboration_summary: dict[str, Any],
) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
    lane_weights = {
        _norm_text(key): _safe_int(value, default=0)
        for key, value in dict(family_spec.get("lane_weights") or {}).items()
        if _safe_int(value, default=0) > 0
    }
    candidate_weights = {
        _norm_text(key): _safe_int(value, default=0)
        for key, value in dict(family_spec.get("candidate_evidence_weights") or {}).items()
        if _safe_int(value, default=0) > 0
    }
    source_weights = {
        _norm_text(key): _safe_int(value, default=0)
        for key, value in dict(family_spec.get("source_weights") or {}).items()
        if _safe_int(value, default=0) > 0
    }

    lane_counts = _lane_counter(details)
    score = 0
    matches: list[dict[str, Any]] = []
    source_matches: list[dict[str, Any]] = []

    for lane, count in sorted(lane_counts.items()):
        weight = lane_weights.get(lane, 0)
        if weight <= 0:
            continue
        contribution = int(weight)
        score += contribution
        matches.append(
            {
                "kind": "lane",
                "lane": lane,
                "weight": contribution,
                "count": int(count),
            }
        )

    for candidate in _norm_list(corroboration_summary.get("candidate_join_evidence")):
        if not isinstance(candidate, dict):
            continue
        candidate_score = 0
        evidence_types = [_norm_text(item) for item in _norm_list(candidate.get("evidence_types")) if _norm_text(item)]
        for evidence_type in evidence_types:
            candidate_score += candidate_weights.get(evidence_type, 0)
        if candidate_score <= 0:
            continue
        score += candidate_score
        matches.append(
            {
                "kind": "candidate_join",
                "lane": "sam_usaspending_candidate_join",
                "correlation_id": candidate.get("correlation_id"),
                "weight": candidate_score,
                "evidence_types": evidence_types,
                "status": "candidate",
                "likely_incumbent": bool(candidate.get("likely_incumbent")),
            }
        )

    for source_item in _norm_list(corroboration_summary.get("linked_source_summary")):
        if not isinstance(source_item, dict):
            continue
        source = _norm_text(source_item.get("source"))
        source_weight = source_weights.get(source, 0)
        if source_weight <= 0:
            continue
        score += source_weight
        match = {
            "source": source,
            "weight": int(source_weight),
            "linked_event_count": _safe_int(source_item.get("linked_event_count"), default=0),
            "lanes": [str(item) for item in _norm_list(source_item.get("lanes")) if _norm_text(item)],
            "sample_event_ids": [int(item) for item in _norm_list(source_item.get("sample_event_ids")) if _safe_int(item, default=0) > 0],
            "sample_doc_ids": [str(item) for item in _norm_list(source_item.get("sample_doc_ids")) if _norm_text(item)],
        }
        source_matches.append(match)
        matches.append({"kind": "linked_source", **match})

    matches.sort(
        key=lambda item: (
            -_safe_int(item.get("weight"), default=0),
            _norm_text(item.get("kind")),
            _norm_text(item.get("lane") or item.get("source")),
        )
    )
    return score, matches, source_matches


def _assignment_rationale(
    *,
    label: str,
    ontology_matches: list[dict[str, Any]],
    corroboration_matches: list[dict[str, Any]],
) -> str:
    ontology_bits = [
        ":".join(filter(None, [_norm_text(item.get("pack")), _norm_text(item.get("rule"))]))
        for item in ontology_matches[:3]
    ]
    corroboration_bits: list[str] = []
    for item in corroboration_matches[:4]:
        kind = _norm_text(item.get("kind"))
        if kind == "lane":
            corroboration_bits.append(_norm_text(item.get("lane")))
        elif kind == "candidate_join":
            corroboration_bits.append("candidate_join:" + ",".join(item.get("evidence_types") or []))
        elif kind == "linked_source":
            corroboration_bits.append("linked_source:" + _norm_text(item.get("source")))
    parts = [label]
    if ontology_bits:
        parts.append("ontology=" + ", ".join([bit for bit in ontology_bits if bit]))
    if corroboration_bits:
        parts.append("corroboration=" + ", ".join([bit for bit in corroboration_bits if bit]))
    return " | ".join(parts)


def classify_lead_families(
    *,
    details: dict[str, Any],
    linked_source_summary: list[dict[str, Any]] | None = None,
    linked_records_by_correlation: dict[int, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    enriched = dict(details or {})
    corroboration_summary = build_corroboration_summary(
        details=enriched,
        linked_source_summary=linked_source_summary,
        linked_records_by_correlation=linked_records_by_correlation,
    )
    ontology_index = _build_ontology_index(enriched)

    assignments: list[dict[str, Any]] = []
    for family, spec in LEAD_FAMILY_TAXONOMY.items():
        ontology_score, ontology_matches = _family_ontology_matches(
            family_spec=spec,
            ontology_index=ontology_index,
        )
        corroboration_score, corroboration_matches, source_matches = _family_corroboration_matches(
            family_spec=spec,
            details=enriched,
            corroboration_summary=corroboration_summary,
        )
        total_score = int(ontology_score + corroboration_score)
        if ontology_score < _safe_int(spec.get("min_ontology_score"), default=1):
            continue
        if corroboration_score < _safe_int(spec.get("min_corroboration_score"), default=0):
            continue
        if total_score < _safe_int(spec.get("min_total_score"), default=1):
            continue
        assignments.append(
            {
                "family": family,
                "label": spec.get("label") or family.replace("_", " "),
                "score": total_score,
                "ontology_score": ontology_score,
                "corroboration_score": corroboration_score,
                "ontology_matches": ontology_matches[:6],
                "corroboration_matches": corroboration_matches[:8],
                "linked_source_summary": source_matches[:4],
                "rationale": _assignment_rationale(
                    label=str(spec.get("label") or family.replace("_", " ")),
                    ontology_matches=ontology_matches,
                    corroboration_matches=corroboration_matches,
                ),
            }
        )

    assignments.sort(
        key=lambda item: (
            -_safe_int(item.get("score"), default=0),
            -_safe_int(item.get("ontology_score"), default=0),
            -_safe_int(item.get("corroboration_score"), default=0),
            _norm_text(item.get("family")),
        )
    )

    primary = assignments[0] if assignments else None
    secondary = [str(item.get("family")) for item in assignments[1:]]

    enriched["corroboration_summary"] = corroboration_summary
    enriched["lead_family"] = None if primary is None else primary.get("family")
    enriched["lead_family_label"] = None if primary is None else primary.get("label")
    enriched["secondary_lead_families"] = secondary
    enriched["lead_family_assignments"] = assignments
    return enriched


def lead_matches_family(details: dict[str, Any], family: str | None) -> bool:
    family_key = _norm_key(family)
    if not family_key:
        return True
    if _norm_key(details.get("lead_family")) == family_key:
        return True
    return family_key in {_norm_key(item) for item in _norm_list(details.get("secondary_lead_families"))}


def lead_family_label(family: str | None) -> str | None:
    family_key = _norm_key(family)
    if not family_key:
        return None
    spec = LEAD_FAMILY_TAXONOMY.get(family_key)
    if spec:
        return str(spec.get("label") or family_key.replace("_", " "))
    return family_key.replace("_", " ")


def _summary_family(item: dict[str, Any], *, lead_family_filter: str | None = None) -> str | None:
    primary = _norm_text(item.get("lead_family")) or None
    family_key = _norm_key(lead_family_filter)
    if not family_key:
        return primary
    if _norm_key(primary) == family_key:
        return primary
    for secondary in _norm_list(item.get("secondary_lead_families")):
        secondary_text = _norm_text(secondary)
        if _norm_key(secondary_text) == family_key:
            return secondary_text or family_key
    return primary


def summarize_lead_family_groups(
    items: list[dict[str, Any]],
    *,
    lead_family_filter: str | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str | None, dict[str, Any]] = {}
    for item in items:
        family = _summary_family(item, lead_family_filter=lead_family_filter)
        bucket = grouped.setdefault(
            family,
            {
                "lead_family": family,
                "label": lead_family_label(family) or "Unassigned",
                "count": 0,
                "top_score": None,
                "top_rank": None,
                "sample_event_ids": [],
            },
        )
        bucket["count"] += 1
        score = item.get("score")
        if bucket["top_score"] is None or _safe_int(score, default=0) > _safe_int(bucket["top_score"], default=0):
            bucket["top_score"] = _safe_int(score, default=0)
        rank = item.get("rank")
        if rank is not None:
            if bucket["top_rank"] is None or _safe_int(rank, default=0) < _safe_int(bucket["top_rank"], default=0):
                bucket["top_rank"] = _safe_int(rank, default=0)
        event_id = _safe_int(item.get("event_id"), default=0)
        if event_id > 0 and event_id not in bucket["sample_event_ids"] and len(bucket["sample_event_ids"]) < 5:
            bucket["sample_event_ids"].append(event_id)

    rows = list(grouped.values())
    rows.sort(
        key=lambda item: (
            1 if item.get("lead_family") is None else 0,
            -(item.get("count") or 0),
            _safe_int(item.get("top_rank"), default=10**9),
            _norm_text(item.get("lead_family")),
        )
    )
    return rows


__all__ = [
    "LEAD_FAMILY_TAXONOMY",
    "classify_lead_families",
    "lead_family_label",
    "lead_matches_family",
    "summarize_lead_family_groups",
]
