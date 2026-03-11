"""Canonical event field extraction helpers for procurement sources."""
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional


CANONICAL_EVENT_FIELD_KEYS: tuple[str, ...] = (
    "award_id",
    "generated_unique_award_id",
    "piid",
    "fain",
    "uri",
    "transaction_id",
    "modification_number",
    "source_record_id",
    "recipient_name",
    "recipient_uei",
    "recipient_parent_uei",
    "recipient_duns",
    "recipient_cage_code",
    "awarding_agency_code",
    "awarding_agency_name",
    "funding_agency_code",
    "funding_agency_name",
    "contracting_office_code",
    "contracting_office_name",
    "psc_code",
    "psc_description",
    "naics_code",
    "naics_description",
    "notice_award_type",
    "place_of_performance_city",
    "place_of_performance_state",
    "place_of_performance_country",
    "place_of_performance_zip",
    "solicitation_number",
    "notice_id",
    "document_id",
)

_NULL_LIKE = {"", "null", "none", "n/a", "nan"}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip().lower() in _NULL_LIKE
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _clean_text(value: Any) -> Optional[str]:
    if _is_missing(value):
        return None
    if not isinstance(value, str):
        value = str(value)
    cleaned = " ".join(value.strip().split())
    if _is_missing(cleaned):
        return None
    return cleaned


def _clean_code(value: Any) -> Optional[str]:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    return cleaned.upper()


def _clean_notice_type(value: Any) -> Optional[str]:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    return cleaned.lower()


def _get_value_case_insensitive(record: Mapping[str, Any], key: str) -> Any:
    if key in record:
        return record.get(key)
    key_l = key.lower()
    for rk, rv in record.items():
        if str(rk).lower() == key_l:
            return rv
    return None


def _dig(record: Mapping[str, Any], dotted_path: str) -> Any:
    cur: Any = record
    for part in dotted_path.split("."):
        if not isinstance(cur, Mapping):
            return None
        cur = _get_value_case_insensitive(cur, part)
    return cur


def _first_present(record: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = _dig(record, key) if "." in key else _get_value_case_insensitive(record, key)
        if not _is_missing(value):
            return value
    return None


def _normalize_zip(value: Any) -> Optional[str]:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    return cleaned.upper()


def _normalize_naics_code(value: Any) -> Optional[str]:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    compact = "".join(ch for ch in cleaned if ch.isalnum())
    return compact.upper() if compact else None


def _extract_place_from_mapping(place_obj: Any) -> Dict[str, Optional[str]]:
    if not isinstance(place_obj, Mapping):
        return {
            "place_of_performance_city": None,
            "place_of_performance_state": None,
            "place_of_performance_country": None,
            "place_of_performance_zip": None,
        }

    city = _first_present(place_obj, ("city.name", "city", "cityName", "town"))
    state = _first_present(place_obj, ("state.code", "state", "stateCode", "region"))
    country = _first_present(place_obj, ("country.code", "countryCode", "country", "country_code"))
    zip_code = _first_present(place_obj, ("zip", "zipcode", "postalCode", "zipCode"))

    return {
        "place_of_performance_city": _clean_text(city),
        "place_of_performance_state": _clean_code(state),
        "place_of_performance_country": _clean_code(country),
        "place_of_performance_zip": _normalize_zip(zip_code),
    }


def _empty_canonical_fields() -> Dict[str, Optional[str]]:
    return {k: None for k in CANONICAL_EVENT_FIELD_KEYS}


def extract_usaspending_event_fields(record: Mapping[str, Any] | None) -> Dict[str, Optional[str]]:
    raw: Mapping[str, Any] = record if isinstance(record, Mapping) else {}
    out = _empty_canonical_fields()

    out["award_id"] = _clean_text(_first_present(raw, ("award_id", "Award ID", "awardId")))
    out["generated_unique_award_id"] = _clean_text(
        _first_present(raw, ("generated_unique_award_id", "Generated Unique Award ID"))
    )
    out["piid"] = _clean_text(_first_present(raw, ("piid", "PIID", "piid_code")))
    out["fain"] = _clean_text(_first_present(raw, ("fain", "FAIN")))
    out["uri"] = _clean_text(_first_present(raw, ("uri", "URI")))
    out["transaction_id"] = _clean_text(_first_present(raw, ("transaction_id", "Transaction ID", "transactionId")))
    out["modification_number"] = _clean_text(
        _first_present(raw, ("modification_number", "Modification Number", "modNumber"))
    )
    out["source_record_id"] = _clean_text(_first_present(raw, ("source_record_id", "internal_id", "internalId", "id")))

    out["recipient_name"] = _clean_text(
        _first_present(raw, ("recipient_name", "recipientName", "Recipient Name", "recipient"))
    )
    out["recipient_uei"] = _clean_code(_first_present(raw, ("recipient_uei", "Recipient UEI", "uei", "UEI")))
    out["recipient_parent_uei"] = _clean_code(
        _first_present(raw, ("recipient_parent_uei", "Recipient Parent UEI", "parent_uei"))
    )
    out["recipient_duns"] = _clean_code(
        _first_present(raw, ("recipient_duns", "Recipient DUNS Number", "duns", "DUNS"))
    )
    out["recipient_cage_code"] = _clean_code(
        _first_present(raw, ("recipient_cage_code", "Recipient CAGE Code", "cage_code", "CAGE"))
    )

    out["awarding_agency_code"] = _clean_code(
        _first_present(
            raw,
            (
                "awarding_agency_code",
                "Awarding Agency Code",
                "awardingAgencyCode",
                "awarding_toptier_agency_code",
            ),
        )
    )
    out["awarding_agency_name"] = _clean_text(
        _first_present(
            raw,
            (
                "awarding_agency_name",
                "Awarding Agency Name",
                "awardingAgencyName",
                "awarding_toptier_agency_name",
            ),
        )
    )
    out["funding_agency_code"] = _clean_code(
        _first_present(
            raw,
            (
                "funding_agency_code",
                "Funding Agency Code",
                "fundingAgencyCode",
                "funding_toptier_agency_code",
            ),
        )
    )
    out["funding_agency_name"] = _clean_text(
        _first_present(
            raw,
            (
                "funding_agency_name",
                "Funding Agency Name",
                "fundingAgencyName",
                "funding_toptier_agency_name",
            ),
        )
    )
    out["contracting_office_code"] = _clean_code(
        _first_present(raw, ("contracting_office_code", "Contracting Office Code", "awarding_office_code"))
    )
    out["contracting_office_name"] = _clean_text(
        _first_present(raw, ("contracting_office_name", "Contracting Office Name", "awarding_office_name"))
    )

    out["psc_code"] = _clean_code(
        _first_present(raw, ("psc_code", "PSC Code", "Product or Service Code", "product_or_service_code"))
    )
    out["psc_description"] = _clean_text(
        _first_present(raw, ("psc_description", "PSC Description", "product_or_service_code_description"))
    )
    out["naics_code"] = _normalize_naics_code(
        _first_present(raw, ("naics_code", "naicsCode", "naics", "NAICS"))
    )
    out["naics_description"] = _clean_text(
        _first_present(raw, ("naics_description", "naicsDescription", "NAICS Description"))
    )
    out["notice_award_type"] = _clean_notice_type(
        _first_present(raw, ("notice_award_type", "award_type", "award_type_code", "Award Type"))
    )

    out["solicitation_number"] = _clean_text(
        _first_present(raw, ("solicitation_number", "solicitationNumber", "solicitation"))
    )
    out["notice_id"] = _clean_text(_first_present(raw, ("notice_id", "noticeId", "notice_id")))
    out["document_id"] = _clean_text(_first_present(raw, ("document_id", "Document ID", "doc_id")))

    place_obj = _first_present(raw, ("place_of_performance", "placeOfPerformance"))
    place_fields = _extract_place_from_mapping(place_obj)
    for key, value in place_fields.items():
        if value:
            out[key] = value

    out["place_of_performance_city"] = out["place_of_performance_city"] or _clean_text(
        _first_present(raw, ("place_of_performance_city", "Place of Performance City", "place_of_performance_city_name"))
    )
    out["place_of_performance_state"] = out["place_of_performance_state"] or _clean_code(
        _first_present(raw, ("place_of_performance_state", "place_of_performance_state_code", "Place of Performance State"))
    )
    out["place_of_performance_country"] = out["place_of_performance_country"] or _clean_code(
        _first_present(raw, ("place_of_performance_country", "place_of_performance_country_code", "Place of Performance Country"))
    )
    out["place_of_performance_zip"] = out["place_of_performance_zip"] or _normalize_zip(
        _first_present(raw, ("place_of_performance_zip", "place_of_performance_zip5", "Place of Performance ZIP"))
    )

    if not out["document_id"]:
        out["document_id"] = out["notice_id"] or out["solicitation_number"] or out["award_id"] or out["generated_unique_award_id"]

    if not out["source_record_id"]:
        out["source_record_id"] = out["generated_unique_award_id"] or out["award_id"] or out["document_id"]

    return out


def extract_samgov_event_fields(record: Mapping[str, Any] | None) -> Dict[str, Optional[str]]:
    raw: Mapping[str, Any] = record if isinstance(record, Mapping) else {}
    out = extract_usaspending_event_fields(raw)

    notice_id = _clean_text(_first_present(raw, ("notice_id", "noticeId", "noticeid")))
    if notice_id:
        out["notice_id"] = notice_id
    out["document_id"] = out["document_id"] or notice_id
    out["source_record_id"] = out["source_record_id"] or notice_id

    out["notice_award_type"] = _clean_notice_type(
        _first_present(raw, ("notice_award_type", "noticeType", "noticeTypeDescription", "typeOfNotice", "baseType"))
    )
    out["solicitation_number"] = _clean_text(
        _first_present(raw, ("solicitation_number", "solicitationNumber", "solicitationId", "solicitation"))
    )

    out["awarding_agency_code"] = out["awarding_agency_code"] or _clean_code(
        _first_present(raw, ("awarding_agency_code", "fullParentPathCode", "parentPathCode", "organizationCode"))
    )
    out["awarding_agency_name"] = out["awarding_agency_name"] or _clean_text(
        _first_present(raw, ("awarding_agency_name", "fullParentPathName", "parentPathName", "organization"))
    )

    out["contracting_office_code"] = out["contracting_office_code"] or _clean_code(
        _first_present(raw, ("contracting_office_code", "officeCode", "subTierCode", "officeAddress.officeCode"))
    )
    out["contracting_office_name"] = out["contracting_office_name"] or _clean_text(
        _first_present(raw, ("contracting_office_name", "officeName", "subTier", "officeAddress.officeName"))
    )

    out["psc_code"] = out["psc_code"] or _clean_code(
        _first_present(raw, ("psc_code", "classificationCode", "classification.code"))
    )
    out["naics_code"] = out["naics_code"] or _normalize_naics_code(
        _first_present(raw, ("naics_code", "naicsCode", "naics", "naics.code"))
    )
    out["naics_description"] = out["naics_description"] or _clean_text(
        _first_present(raw, ("naics_description", "naicsDescription", "naicsDesc", "naics.description"))
    )

    place_obj = _first_present(raw, ("placeOfPerformance", "officeAddress", "place_of_performance"))
    place_fields = _extract_place_from_mapping(place_obj)
    for key, value in place_fields.items():
        if value:
            out[key] = value

    award = _first_present(raw, ("award",))
    if isinstance(award, Mapping):
        out["award_id"] = out["award_id"] or _clean_text(
            _first_present(award, ("awardId", "award_id", "id", "awardNumber"))
        )
        out["piid"] = out["piid"] or _clean_text(_first_present(award, ("piid", "contractNumber")))
        out["fain"] = out["fain"] or _clean_text(_first_present(award, ("fain",)))
        out["uri"] = out["uri"] or _clean_text(_first_present(award, ("uri", "URI")))
        out["modification_number"] = out["modification_number"] or _clean_text(
            _first_present(award, ("modNumber", "modificationNumber"))
        )

        awardee = _first_present(award, ("awardee", "recipient"))
        if isinstance(awardee, Mapping):
            out["recipient_name"] = out["recipient_name"] or _clean_text(
                _first_present(awardee, ("name", "legalBusinessName"))
            )
            out["recipient_uei"] = out["recipient_uei"] or _clean_code(
                _first_present(awardee, ("ueiSAM", "ueiSam", "uei"))
            )
            out["recipient_parent_uei"] = out["recipient_parent_uei"] or _clean_code(
                _first_present(awardee, ("parentUei", "parentUEI"))
            )
            out["recipient_duns"] = out["recipient_duns"] or _clean_code(_first_present(awardee, ("duns", "DUNS")))
            out["recipient_cage_code"] = out["recipient_cage_code"] or _clean_code(
                _first_present(awardee, ("cage", "cageCode", "CAGE"))
            )

    if not out["document_id"]:
        out["document_id"] = out["notice_id"] or out["solicitation_number"] or out["award_id"] or out["generated_unique_award_id"]

    return out


def merge_canonical_fields_into_raw(raw_json: Mapping[str, Any] | None, canonical_fields: Mapping[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(raw_json or {})
    for key in CANONICAL_EVENT_FIELD_KEYS:
        value = canonical_fields.get(key)
        if _is_missing(value):
            continue
        current = merged.get(key)
        if _is_missing(current):
            merged[key] = value
    return merged


__all__ = [
    "CANONICAL_EVENT_FIELD_KEYS",
    "extract_samgov_event_fields",
    "extract_usaspending_event_fields",
    "merge_canonical_fields_into_raw",
]



