"""SAM.gov context normalization helpers.

This module defines a focused SAM context contract for research pivots.
We intentionally keep the set small and high-signal:

- agency/office hierarchy signals
- notice/solicitation metadata
- NAICS/set-aside/procurement classification
- key timeline dates
- place-of-performance region pivots

The extracted keys are persisted into Event.raw_json as canonical `sam_*` fields
so operators can filter/export consistently without depending on every upstream
SAM field shape variant.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional


SAM_CONTEXT_FIELD_DESCRIPTIONS: Dict[str, str] = {
    "sam_agency_path_name": "Agency hierarchy name path (for org pivots)",
    "sam_agency_path_code": "Agency hierarchy code path (for stable org joins)",
    "sam_organization_type": "SAM organization type label",
    "sam_office_name": "Office/sub-tier label",
    "sam_office_code": "Office/sub-tier code",
    "sam_notice_type": "Notice type text (normalized)",
    "sam_notice_type_code": "Notice type code",
    "sam_solicitation_number": "Solicitation identifier",
    "sam_classification_code": "Procurement classification code",
    "sam_naics_code": "NAICS code",
    "sam_naics_description": "NAICS description text",
    "sam_set_aside_code": "Set-aside code",
    "sam_set_aside_description": "Set-aside description text",
    "sam_posted_date": "Posted date/time (normalized text)",
    "sam_response_deadline": "Response deadline date/time (normalized text)",
    "sam_archive_date": "Archive date/time (normalized text)",
    "sam_place_state_code": "Place-of-performance state code",
    "sam_place_country_code": "Place-of-performance country code",
    "sam_active": "Opportunity active flag when provided by source",
}

SAM_CONTEXT_FIELDS: tuple[str, ...] = tuple(SAM_CONTEXT_FIELD_DESCRIPTIONS.keys())
_SAM_CONTEXT_FIELD_SET = set(SAM_CONTEXT_FIELDS)


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    cleaned = " ".join(value.strip().split())
    if not cleaned or cleaned.lower() == "null":
        return None
    return cleaned


def _normalize_code(value: Any) -> Optional[str]:
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    return cleaned.upper()


def _normalize_naics_code(value: Any) -> Optional[str]:
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    compact = re.sub(r"[^A-Za-z0-9]", "", cleaned)
    if not compact:
        return None
    return compact.upper()


def _normalize_label(value: Any) -> Optional[str]:
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    return cleaned.lower()


def _normalize_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    cleaned = _clean_str(value)
    if not cleaned:
        return None
    lowered = cleaned.lower()
    if lowered in {"true", "1", "yes", "y", "active"}:
        return True
    if lowered in {"false", "0", "no", "n", "inactive"}:
        return False
    return None


def _normalize_datetime_text(value: Any) -> Optional[str]:
    cleaned = _clean_str(value)
    if not cleaned:
        return None

    fmts = (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(cleaned, fmt).isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return cleaned


def _dig(record: Mapping[str, Any], dotted_path: str) -> Any:
    cur: Any = record
    for part in dotted_path.split("."):
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(part)
    return cur


def _first_present(record: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if "." in key:
            value = _dig(record, key)
        else:
            value = record.get(key)
        cleaned = _clean_str(value) if isinstance(value, str) else value
        if cleaned is None:
            continue
        if isinstance(cleaned, str) and not cleaned:
            continue
        return cleaned
    return None


def _extract_code_name(value: Any) -> tuple[Optional[str], Optional[str]]:
    if isinstance(value, Mapping):
        code = _normalize_code(
            value.get("code")
            or value.get("id")
            or value.get("value")
            or value.get("key")
        )
        name = _clean_str(value.get("name") or value.get("description") or value.get("label"))
        return code, name
    cleaned = _clean_str(value)
    if not cleaned:
        return None, None
    return _normalize_code(cleaned), None


def extract_sam_context_fields(record: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Extract canonical high-value SAM context fields from an opportunity record."""
    raw: Mapping[str, Any] = record if isinstance(record, Mapping) else {}

    agency_path_name = _clean_str(
        _first_present(
            raw,
            (
                "sam_agency_path_name",
                "fullParentPathName",
                "parentPathName",
                "organization",
                "organizationName",
                "agency.name",
                "agency",
            ),
        )
    )
    agency_path_code = _normalize_code(
        _first_present(
            raw,
            (
                "sam_agency_path_code",
                "fullParentPathCode",
                "parentPathCode",
                "organizationCode",
                "agency.code",
            ),
        )
    )
    org_type = _clean_str(
        _first_present(
            raw,
            (
                "sam_organization_type",
                "organizationType",
                "organization_type",
                "officeType",
            ),
        )
    )

    office_name = _clean_str(
        _first_present(
            raw,
            (
                "sam_office_name",
                "officeName",
                "office.name",
                "office",
                "subTier",
                "subTierName",
                "officeAddress.office",
                "officeAddress.officeName",
            ),
        )
    )
    office_code = _normalize_code(
        _first_present(
            raw,
            (
                "sam_office_code",
                "officeCode",
                "office.code",
                "subTierCode",
                "officeAddress.officeCode",
            ),
        )
    )

    notice_type = _normalize_label(
        _first_present(
            raw,
            (
                "sam_notice_type",
                "noticeType",
                "noticeTypeDescription",
                "typeOfNotice",
                "baseType",
                "type",
            ),
        )
    )
    notice_type_code = _normalize_code(
        _first_present(
            raw,
            (
                "sam_notice_type_code",
                "noticeTypeCode",
                "typeCode",
                "baseTypeCode",
            ),
        )
    )
    solicitation_number = _clean_str(
        _first_present(
            raw,
            (
                "sam_solicitation_number",
                "solicitationNumber",
                "solicitationNumberLegacy",
                "solicitation",
                "solicitationId",
            ),
        )
    )
    classification_code = _normalize_code(
        _first_present(
            raw,
            (
                "sam_classification_code",
                "classificationCode",
                "classification.code",
            ),
        )
    )

    naics_raw = _first_present(
        raw,
        (
            "sam_naics_code",
            "naicsCode",
            "naics",
            "naics.code",
            "classification.naicsCode",
        ),
    )
    naics_code, naics_name_from_obj = _extract_code_name(naics_raw)
    if not naics_code:
        naics_code = _normalize_naics_code(naics_raw)
    else:
        naics_code = _normalize_naics_code(naics_code)
    naics_description = _clean_str(
        _first_present(
            raw,
            (
                "sam_naics_description",
                "naicsDescription",
                "naicsDesc",
                "naicsCodeDescription",
                "naics.description",
                "classification.naicsDescription",
            ),
        )
        or naics_name_from_obj
    )

    set_aside_raw = _first_present(
        raw,
        (
            "sam_set_aside_code",
            "typeOfSetAside",
            "typeOfSetAsideCode",
            "setAsideCode",
            "setAside",
            "setAsideType",
        ),
    )
    set_aside_code, set_aside_name_from_obj = _extract_code_name(set_aside_raw)
    set_aside_description = _clean_str(
        _first_present(
            raw,
            (
                "sam_set_aside_description",
                "typeOfSetAsideDescription",
                "setAsideDescription",
                "setAsideDesc",
                "setAsideText",
            ),
        )
        or set_aside_name_from_obj
    )

    posted_date = _normalize_datetime_text(
        _first_present(raw, ("sam_posted_date", "postedDate", "posted_date"))
    )
    response_deadline = _normalize_datetime_text(
        _first_present(
            raw,
            (
                "sam_response_deadline",
                "responseDeadLine",
                "responseDeadline",
                "response_date",
                "responseDate",
            ),
        )
    )
    archive_date = _normalize_datetime_text(
        _first_present(raw, ("sam_archive_date", "archiveDate", "archive_date"))
    )

    place_state = _normalize_code(
        _first_present(
            raw,
            (
                "sam_place_state_code",
                "placeOfPerformance.state.code",
                "placeOfPerformance.state",
                "placeOfPerformance.stateCode",
            ),
        )
    )
    place_country = _normalize_code(
        _first_present(
            raw,
            (
                "sam_place_country_code",
                "placeOfPerformance.country.code",
                "placeOfPerformance.countryCode",
                "placeOfPerformance.country",
            ),
        )
    )
    active_flag = _normalize_bool(_first_present(raw, ("sam_active", "active", "isActive")))

    return {
        "sam_agency_path_name": agency_path_name,
        "sam_agency_path_code": agency_path_code,
        "sam_organization_type": org_type,
        "sam_office_name": office_name,
        "sam_office_code": office_code,
        "sam_notice_type": notice_type,
        "sam_notice_type_code": notice_type_code,
        "sam_solicitation_number": solicitation_number,
        "sam_classification_code": classification_code,
        "sam_naics_code": naics_code,
        "sam_naics_description": naics_description,
        "sam_set_aside_code": set_aside_code,
        "sam_set_aside_description": set_aside_description,
        "sam_posted_date": posted_date,
        "sam_response_deadline": response_deadline,
        "sam_archive_date": archive_date,
        "sam_place_state_code": place_state,
        "sam_place_country_code": place_country,
        "sam_active": active_flag,
    }


def merge_sam_context_fields(raw_json: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge canonical SAM fields into raw_json, filling only missing/empty values."""
    merged: Dict[str, Any] = dict(raw_json or {})
    for key, value in context.items():
        if key not in _SAM_CONTEXT_FIELD_SET:
            continue
        if value in (None, "", [], {}):
            continue
        cur = merged.get(key)
        if cur in (None, "", [], {}):
            merged[key] = value
    return merged


__all__ = [
    "SAM_CONTEXT_FIELD_DESCRIPTIONS",
    "SAM_CONTEXT_FIELDS",
    "extract_sam_context_fields",
    "merge_sam_context_fields",
]
