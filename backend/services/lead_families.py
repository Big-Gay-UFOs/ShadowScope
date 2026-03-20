from __future__ import annotations

import json
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
        "min_ontology_score": 2,
        "min_corroboration_score": 2,
        "scope": "fallback",
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
            "sam_usaspending_candidate_join": 2,
        },
        "specific_lanes": [
            "same_award_id",
            "same_contract_id",
            "same_doc_id",
            "same_entity",
            "same_uei",
            "sam_usaspending_candidate_join",
        ],
        "candidate_evidence_weights": {
            "identifier_exact": 3,
            "contract_family": 3,
            "recipient_uei": 2,
            "recipient_name": 2,
        },
        "specific_candidate_evidence_types": [
            "identifier_exact",
            "contract_family",
            "recipient_uei",
            "recipient_name",
        ],
        "source_weights": {},
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


def _norm_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _unique_texts(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _norm_text(value)
        if not text:
            continue
        key = _norm_key(text)
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


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

    # Some scoring paths preserve high-signal keyword/clause matches only in top_positive_signals.
    for signal in _norm_list(details.get("top_positive_signals")):
        if not isinstance(signal, dict):
            continue
        signal_type = _norm_key(signal.get("signal_type"))
        if signal_type not in {"clause", "keyword"}:
            continue
        pack = _norm_key(signal.get("pack"))
        rule = _norm_key(signal.get("rule"))
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
                "weight": max(min(_safe_int(signal.get("contribution"), default=1), 3), 1),
                "field": _norm_text(signal.get("field")) or None,
                "match": _norm_text(signal.get("match")) or None,
                "source": "score_signal",
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


def _best_context_agency(event_context: dict[str, Any]) -> str | None:
    candidates = (
        (event_context.get("awarding_agency_name"), event_context.get("awarding_agency_code")),
        (event_context.get("funding_agency_name"), event_context.get("funding_agency_code")),
        (event_context.get("contracting_office_name"), event_context.get("contracting_office_code")),
    )
    for name, code in candidates:
        name_text = _norm_text(name)
        code_text = _norm_text(code)
        if name_text and code_text:
            return f"{name_text} ({code_text})"
        if name_text or code_text:
            return name_text or code_text
    return None


def _build_family_context(details: dict[str, Any]) -> dict[str, Any]:
    event_context = _norm_dict(details.get("event_context"))
    identifiers = _unique_texts(
        [
            event_context.get("doc_id"),
            event_context.get("solicitation_number"),
            event_context.get("notice_id"),
            event_context.get("document_id"),
            event_context.get("award_id"),
            event_context.get("piid"),
            event_context.get("generated_unique_award_id"),
            event_context.get("source_record_id"),
        ]
    )
    score_profile = {
        key: _safe_int(details.get(key), default=0)
        for key in (
            "proxy_relevance_score",
            "investigability_score",
            "corroboration_score",
            "structural_context_score",
            "noise_penalty",
            "total_score",
        )
        if key in details
    }
    return {
        "source": _norm_text(event_context.get("source")),
        "agency": _best_context_agency(event_context),
        "vendor": _norm_text(event_context.get("recipient_name")),
        "vendor_uei": _norm_text(event_context.get("recipient_uei")),
        "entity_id": _safe_int(event_context.get("entity_id"), default=0) or None,
        "place_region": _norm_text(event_context.get("place_region")),
        "psc_code": _norm_text(event_context.get("psc_code")),
        "naics_code": _norm_text(event_context.get("naics_code")),
        "notice_award_type": _norm_text(event_context.get("notice_award_type")),
        "traceable_identifiers": identifiers[:5],
        "score_profile": score_profile,
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
) -> tuple[int, int, int, list[dict[str, Any]], list[dict[str, Any]]]:
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
    specific_lanes = {_norm_text(item) for item in _norm_list(family_spec.get("specific_lanes")) if _norm_text(item)}
    if not specific_lanes:
        specific_lanes = set(lane_weights.keys())
    specific_candidate_types = {
        _norm_text(item)
        for item in _norm_list(family_spec.get("specific_candidate_evidence_types"))
        if _norm_text(item)
    }
    if not specific_candidate_types and (
        "specific_candidate_evidence_types" not in family_spec
        and "specific_candidate_evidence_weights" not in family_spec
    ):
        specific_candidate_types = set(candidate_weights.keys())

    lane_counts = _lane_counter(details)
    score = 0
    specific_score = 0
    context_score = 0
    matches: list[dict[str, Any]] = []
    source_matches: list[dict[str, Any]] = []

    for lane, count in sorted(lane_counts.items()):
        weight = lane_weights.get(lane, 0)
        if weight <= 0:
            continue
        contribution = int(weight)
        score += contribution
        is_specific = lane in specific_lanes
        if is_specific:
            specific_score += contribution
        else:
            context_score += contribution
        matches.append(
            {
                "kind": "lane",
                "lane": lane,
                "weight": contribution,
                "count": int(count),
                "specific": bool(is_specific),
            }
        )

    for candidate in _norm_list(corroboration_summary.get("candidate_join_evidence")):
        if not isinstance(candidate, dict):
            continue
        candidate_score = 0
        specific_candidate_score = 0
        evidence_types = [_norm_text(item) for item in _norm_list(candidate.get("evidence_types")) if _norm_text(item)]
        specific_evidence_types: list[str] = []
        context_evidence_types: list[str] = []
        for evidence_type in evidence_types:
            weight = candidate_weights.get(evidence_type, 0)
            candidate_score += weight
            if weight <= 0:
                continue
            if evidence_type in specific_candidate_types:
                specific_candidate_score += weight
                specific_evidence_types.append(evidence_type)
            else:
                context_evidence_types.append(evidence_type)
        if candidate_score <= 0:
            continue
        score += candidate_score
        specific_score += specific_candidate_score
        context_score += max(candidate_score - specific_candidate_score, 0)
        matches.append(
            {
                "kind": "candidate_join",
                "lane": "sam_usaspending_candidate_join",
                "correlation_id": candidate.get("correlation_id"),
                "weight": candidate_score,
                "specific_weight": specific_candidate_score,
                "context_weight": max(candidate_score - specific_candidate_score, 0),
                "evidence_types": evidence_types,
                "specific_evidence_types": specific_evidence_types,
                "context_evidence_types": context_evidence_types,
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
        context_score += source_weight
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
            -_safe_int(item.get("specific_weight"), default=0),
            _norm_text(item.get("kind")),
            _norm_text(item.get("lane") or item.get("source")),
        )
    )
    return score, specific_score, context_score, matches, source_matches


def _assignment_rationale(
    *,
    label: str,
    ontology_matches: list[dict[str, Any]],
    corroboration_matches: list[dict[str, Any]],
    selection: dict[str, Any] | None = None,
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
    if isinstance(selection, dict):
        score_bits = [
            f"selection={_safe_int(selection.get('selection_score'), default=0)}",
            f"total={_safe_int(selection.get('total_score'), default=0)}",
            f"ontology={_safe_int(selection.get('ontology_score'), default=0)}",
            f"corroboration={_safe_int(selection.get('corroboration_score'), default=0)}",
        ]
        parts.append("score=" + ", ".join(score_bits))
    if ontology_bits:
        parts.append("ontology=" + ", ".join([bit for bit in ontology_bits if bit]))
    if corroboration_bits:
        parts.append("corroboration=" + ", ".join([bit for bit in corroboration_bits if bit]))
    return " | ".join(parts)


def _family_selection_breakdown(
    *,
    family_spec: dict[str, Any],
    total_score: int,
    ontology_score: int,
    corroboration_score: int,
    specific_corroboration_score: int,
    context_corroboration_score: int,
    ontology_matches: list[dict[str, Any]],
    corroboration_matches: list[dict[str, Any]],
) -> dict[str, Any]:
    scope = _norm_key(family_spec.get("scope")) or "specialized"
    rule_match_count = len({_norm_key(item.get("rule_key") or f"{item.get('pack')}:{item.get('rule')}") for item in ontology_matches})
    pack_match_count = len({_norm_key(item.get("pack")) for item in ontology_matches if _norm_key(item.get("pack"))})
    ontology_bonus = min(int(ontology_score), 3) + min(rule_match_count, 2)
    specificity_bonus = min(int(specific_corroboration_score), 3)
    fallback_penalty = min(int(context_corroboration_score), 3) if scope == "fallback" else 0
    selection_score = int(total_score + ontology_bonus + specificity_bonus - fallback_penalty)
    return {
        "scope": scope,
        "selection_score": selection_score,
        "total_score": int(total_score),
        "ontology_score": int(ontology_score),
        "corroboration_score": int(corroboration_score),
        "specific_corroboration_score": int(specific_corroboration_score),
        "context_corroboration_score": int(context_corroboration_score),
        "ontology_bonus": int(ontology_bonus),
        "specificity_bonus": int(specificity_bonus),
        "fallback_penalty": int(fallback_penalty),
        "rule_match_count": int(rule_match_count),
        "pack_match_count": int(pack_match_count),
        "corroboration_match_count": len(corroboration_matches),
    }


def _lead_family_selection_summary(assignments: list[dict[str, Any]]) -> dict[str, Any]:
    primary = assignments[0] if assignments else None
    runner_up = assignments[1] if len(assignments) > 1 else None
    return {
        "primary_family": None if primary is None else primary.get("family"),
        "primary_label": None if primary is None else primary.get("label"),
        "primary_selection_score": None
        if primary is None
        else _safe_int(_norm_dict(primary.get("selection")).get("selection_score"), default=0),
        "runner_up_family": None if runner_up is None else runner_up.get("family"),
        "runner_up_label": None if runner_up is None else runner_up.get("label"),
        "runner_up_selection_score": None
        if runner_up is None
        else _safe_int(_norm_dict(runner_up.get("selection")).get("selection_score"), default=0),
        "selection_margin": (
            None
            if primary is None or runner_up is None
            else _safe_int(_norm_dict(primary.get("selection")).get("selection_score"), default=0)
            - _safe_int(_norm_dict(runner_up.get("selection")).get("selection_score"), default=0)
        ),
        "considered_family_count": len(assignments),
        "considered_families": [
            {
                "family": assignment.get("family"),
                "label": assignment.get("label"),
                "selection_score": _safe_int(_norm_dict(assignment.get("selection")).get("selection_score"), default=0),
                "score": _safe_int(assignment.get("score"), default=0),
            }
            for assignment in assignments[:5]
        ],
    }


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
    family_context = _build_family_context(enriched)

    assignments: list[dict[str, Any]] = []
    for family, spec in LEAD_FAMILY_TAXONOMY.items():
        ontology_score, ontology_matches = _family_ontology_matches(
            family_spec=spec,
            ontology_index=ontology_index,
        )
        (
            corroboration_score,
            specific_corroboration_score,
            context_corroboration_score,
            corroboration_matches,
            source_matches,
        ) = _family_corroboration_matches(
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
        selection = _family_selection_breakdown(
            family_spec=spec,
            total_score=total_score,
            ontology_score=ontology_score,
            corroboration_score=corroboration_score,
            specific_corroboration_score=specific_corroboration_score,
            context_corroboration_score=context_corroboration_score,
            ontology_matches=ontology_matches,
            corroboration_matches=corroboration_matches,
        )
        assignments.append(
            {
                "family": family,
                "label": spec.get("label") or family.replace("_", " "),
                "score": total_score,
                "ontology_score": ontology_score,
                "corroboration_score": corroboration_score,
                "score_breakdown": {
                    "total_score": total_score,
                    "ontology_score": ontology_score,
                    "corroboration_score": corroboration_score,
                    "specific_corroboration_score": specific_corroboration_score,
                    "context_corroboration_score": context_corroboration_score,
                },
                "selection": selection,
                "ontology_matches": ontology_matches[:6],
                "corroboration_matches": corroboration_matches[:8],
                "linked_source_summary": source_matches[:4],
                "context_summary": family_context,
                "rationale": _assignment_rationale(
                    label=str(spec.get("label") or family.replace("_", " ")),
                    ontology_matches=ontology_matches,
                    corroboration_matches=corroboration_matches,
                    selection=selection,
                ),
            }
        )

    assignments.sort(
        key=lambda item: (
            -_safe_int(_norm_dict(item.get("selection")).get("selection_score"), default=0),
            -_safe_int(item.get("score"), default=0),
            -_safe_int(item.get("ontology_score"), default=0),
            -_safe_int(_norm_dict(item.get("selection")).get("specific_corroboration_score"), default=0),
            -_safe_int(item.get("corroboration_score"), default=0),
            _norm_text(item.get("family")),
        )
    )

    primary = assignments[0] if assignments else None
    secondary = [str(item.get("family")) for item in assignments[1:]]
    selection_summary = _lead_family_selection_summary(assignments)

    enriched["corroboration_summary"] = corroboration_summary
    enriched["lead_family_context"] = family_context
    enriched["lead_family"] = None if primary is None else primary.get("family")
    enriched["lead_family_label"] = None if primary is None else primary.get("label")
    enriched["secondary_lead_families"] = secondary
    enriched["lead_family_assignments"] = assignments
    enriched["lead_family_selection"] = selection_summary
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


def _summary_secondary_families(item: dict[str, Any]) -> list[Any]:
    secondaries = _norm_list(item.get("secondary_lead_families"))
    if secondaries:
        return secondaries
    payload = item.get("secondary_lead_families_json")
    if not isinstance(payload, str) or not payload.strip():
        return []
    try:
        decoded = json.loads(payload)
    except Exception:
        return []
    return decoded if isinstance(decoded, list) else []


def _distribution_secondary_families(
    item: dict[str, Any],
    *,
    primary: str | None,
    lead_family_filter: str | None = None,
) -> list[str]:
    secondaries = [text for text in _unique_texts(_summary_secondary_families(item))]
    family_key = _norm_key(lead_family_filter)
    if not family_key:
        return secondaries

    primary_key = _norm_key(primary)
    return [
        secondary
        for secondary in secondaries
        if _norm_key(secondary) == family_key and _norm_key(secondary) != primary_key
    ]


def _summary_family(item: dict[str, Any], *, lead_family_filter: str | None = None) -> str | None:
    primary = _norm_text(item.get("lead_family")) or None
    family_key = _norm_key(lead_family_filter)
    if not family_key:
        return primary
    if _norm_key(primary) == family_key:
        return primary
    for secondary in _summary_secondary_families(item):
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


def summarize_lead_family_distribution(
    items: list[dict[str, Any]],
    *,
    lead_family_filter: str | None = None,
) -> dict[str, Any]:
    total_items = len(items)
    primary_rows = summarize_lead_family_groups(items, lead_family_filter=lead_family_filter)
    for row in primary_rows:
        row["share_pct"] = round((100.0 * int(row.get("count") or 0) / total_items), 1) if total_items else 0.0

    secondary_counts: Counter[str] = Counter()
    assignment_counts: Counter[str] = Counter()
    ambiguous_items = 0
    unassigned_items = 0

    for item in items:
        primary = _summary_family(item, lead_family_filter=lead_family_filter)
        if primary:
            assignment_counts[primary] += 1
        else:
            unassigned_items += 1

        raw_secondaries = [text for text in _unique_texts(_summary_secondary_families(item))]
        if raw_secondaries:
            ambiguous_items += 1
        secondaries = _distribution_secondary_families(
            item,
            primary=primary,
            lead_family_filter=lead_family_filter,
        )
        seen = {_norm_key(primary)} if primary else set()
        for secondary in secondaries:
            secondary_counts[secondary] += 1
            key = _norm_key(secondary)
            if key in seen:
                continue
            seen.add(key)
            assignment_counts[secondary] += 1

    def _counter_rows(counter: Counter[str]) -> list[dict[str, Any]]:
        rows = [
            {
                "lead_family": family,
                "label": lead_family_label(family) or "Unassigned",
                "count": int(count),
                "share_pct": round((100.0 * int(count) / total_items), 1) if total_items else 0.0,
            }
            for family, count in counter.items()
        ]
        rows.sort(
            key=lambda item: (
                -(item.get("count") or 0),
                _norm_text(item.get("lead_family")),
            )
        )
        return rows

    return {
        "total_items": total_items,
        "ambiguous_items": ambiguous_items,
        "unassigned_items": unassigned_items,
        "primary": primary_rows,
        "secondary": _counter_rows(secondary_counts),
        "any_assignment": _counter_rows(assignment_counts),
    }


__all__ = [
    "LEAD_FAMILY_TAXONOMY",
    "classify_lead_families",
    "lead_family_label",
    "lead_matches_family",
    "summarize_lead_family_distribution",
    "summarize_lead_family_groups",
]
