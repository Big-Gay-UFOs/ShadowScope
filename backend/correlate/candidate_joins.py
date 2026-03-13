from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.correlate.correlate import (
    _clean_key_token,
    _event_value,
    _extract_naics_lane,
    _extract_place_region_lane,
    _extract_psc_lane,
    _is_blank,
)
from backend.db.models import Correlation, CorrelationLink, Event, get_session_factory


CANDIDATE_JOIN_LANE = "sam_usaspending_candidate_join"


def correlation_key_prefix(*, window_days: int, history_days: int) -> str:
    return f"{CANDIDATE_JOIN_LANE}|SAM.gov__USAspending|{int(window_days)}|hist{int(history_days)}|pair:"


def _ensure_utc_dt(value: Any) -> Optional[datetime]:
    if value is None or not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _display_token(value: Any) -> Optional[str]:
    if _is_blank(value):
        return None
    return " ".join(str(value).strip().split()) or None


def _normalize_join_identifier(value: Any) -> Optional[str]:
    if _is_blank(value):
        return None
    compact = "".join(ch for ch in str(value).upper() if ch.isalnum())
    return compact or None


def _normalize_join_name(value: Any) -> Optional[str]:
    if _is_blank(value):
        return None

    raw = str(value).upper().replace("&", " AND ")
    raw = re.sub(r"[^A-Z0-9]+", " ", raw)
    stop_words = {
        "A",
        "AN",
        "AND",
        "CO",
        "COMPANY",
        "CORP",
        "CORPORATION",
        "FEDERAL",
        "INC",
        "INCORPORATED",
        "L",
        "LC",
        "LLC",
        "LLP",
        "LTD",
        "LP",
        "OF",
        "PC",
        "PLC",
        "SERVICE",
        "SERVICES",
        "SYSTEM",
        "SYSTEMS",
        "TECHNOLOGIES",
        "TECHNOLOGY",
        "THE",
    }
    parts = [part for part in raw.split() if part and part not in stop_words]
    if not parts:
        parts = [part for part in raw.split() if part]
    if not parts:
        return None
    return " ".join(parts)


def _identifier_family_token(value: Any) -> Optional[str]:
    token = _normalize_join_identifier(value)
    if not token or len(token) < 8:
        return None

    match = re.match(r"^(.*?)(?:MOD\d{1,4}|P\d{1,4}|M\d{1,4}|A\d{1,4})$", token)
    if match:
        root = match.group(1)
        if len(root) >= 6:
            return root
    return None


def _make_match_value(
    *,
    sam_field: str,
    sam_value: Any,
    usaspending_field: str,
    usaspending_value: Any,
    match_kind: str = "exact",
) -> Dict[str, Any]:
    return {
        "sam_field": sam_field,
        "sam_value": _display_token(sam_value),
        "usaspending_field": usaspending_field,
        "usaspending_value": _display_token(usaspending_value),
        "match_kind": match_kind,
    }


def _evidence_entry(
    *,
    evidence_type: str,
    weight: int,
    matched_values: List[Dict[str, Any]],
    description: str,
) -> Dict[str, Any]:
    return {
        "type": evidence_type,
        "weight": int(weight),
        "matched_values": matched_values,
        "description": description,
    }


def _extract_candidate_join_features(ev: Event) -> Dict[str, Any]:
    identifier_specs: Tuple[Tuple[str, Tuple[str, ...], Tuple[str, ...]], ...] = (
        ("award_id", ("award_id",), ("award_id", "Award ID", "awardId")),
        (
            "generated_unique_award_id",
            ("generated_unique_award_id",),
            ("generated_unique_award_id", "Generated Unique Award ID"),
        ),
        ("piid", ("piid",), ("piid", "PIID", "piid_code")),
        ("fain", ("fain",), ("fain", "FAIN")),
        ("uri", ("uri",), ("uri", "URI")),
        (
            "solicitation_number",
            ("solicitation_number",),
            ("solicitation_number", "solicitationNumber", "solicitationId", "solicitation"),
        ),
        ("notice_id", ("notice_id",), ("notice_id", "noticeId", "noticeid")),
        ("document_id", ("document_id",), ("document_id", "Document ID", "doc_id")),
        ("source_record_id", ("source_record_id",), ("source_record_id", "internal_id", "internalId", "id")),
        ("doc_id", ("doc_id",), ("doc_id",)),
    )

    identifiers: List[Dict[str, Any]] = []
    seen_identifier_keys: set[tuple[str, str]] = set()
    for field_name, attrs, raw_keys in identifier_specs:
        raw_value = _event_value(ev, attrs, raw_keys)
        display = _display_token(raw_value)
        token = _normalize_join_identifier(display)
        if not display or not token:
            continue
        dedupe_key = (field_name, token)
        if dedupe_key in seen_identifier_keys:
            continue
        seen_identifier_keys.add(dedupe_key)
        identifiers.append(
            {
                "field": field_name,
                "display": display,
                "token": token,
                "family": _identifier_family_token(token),
            }
        )

    recipient_uei_display = _display_token(
        _event_value(ev, ("recipient_uei",), ("recipient_uei", "Recipient UEI", "uei", "UEI"))
    )
    recipient_uei = _clean_key_token(recipient_uei_display, upper=True)

    recipient_name_display = _display_token(
        _event_value(ev, ("recipient_name",), ("recipient_name", "recipientName", "Recipient Name", "recipient"))
    )
    recipient_name = _normalize_join_name(recipient_name_display)

    awarding_code_display = _display_token(
        _event_value(ev, ("awarding_agency_code",), ("awarding_agency_code", "fullParentPathCode"))
    )
    awarding_name_display = _display_token(
        _event_value(ev, ("awarding_agency_name",), ("awarding_agency_name", "fullParentPathName"))
    )
    funding_code_display = _display_token(_event_value(ev, ("funding_agency_code",), ("funding_agency_code",)))
    funding_name_display = _display_token(_event_value(ev, ("funding_agency_name",), ("funding_agency_name",)))

    awarding_matches: List[Dict[str, Any]] = []
    if awarding_code_display:
        awarding_matches.append(
            {
                "key": _clean_key_token(awarding_code_display, upper=True),
                "display": awarding_code_display,
                "match_kind": "code",
            }
        )
    if awarding_name_display:
        awarding_name = _normalize_join_name(awarding_name_display)
        if awarding_name:
            awarding_matches.append(
                {
                    "key": awarding_name,
                    "display": awarding_name_display,
                    "match_kind": "name",
                }
            )

    funding_matches: List[Dict[str, Any]] = []
    if funding_code_display:
        funding_matches.append(
            {
                "key": _clean_key_token(funding_code_display, upper=True),
                "display": funding_code_display,
                "match_kind": "code",
            }
        )
    if funding_name_display:
        funding_name = _normalize_join_name(funding_name_display)
        if funding_name:
            funding_matches.append(
                {
                    "key": funding_name,
                    "display": funding_name_display,
                    "match_kind": "name",
                }
            )

    psc_value, psc_meta = _extract_psc_lane(ev)
    naics_value, naics_meta = _extract_naics_lane(ev)
    place_region, place_meta = _extract_place_region_lane(ev)

    return {
        "ts": _ensure_utc_dt(ev.occurred_at or ev.created_at),
        "identifiers": identifiers,
        "identifier_tokens": {item["token"] for item in identifiers if item.get("token")},
        "identifier_families": {item["family"] for item in identifiers if item.get("family")},
        "recipient_uei": recipient_uei,
        "recipient_uei_display": recipient_uei_display,
        "recipient_name": recipient_name,
        "recipient_name_display": recipient_name_display,
        "awarding_matches": [item for item in awarding_matches if item.get("key")],
        "funding_matches": [item for item in funding_matches if item.get("key")],
        "psc_code": psc_value,
        "psc_description": psc_meta.get("psc_description") if isinstance(psc_meta, dict) else None,
        "naics_code": naics_value,
        "naics_description": naics_meta.get("naics_description") if isinstance(naics_meta, dict) else None,
        "place_region": place_region,
        "place_meta": place_meta if isinstance(place_meta, dict) else {},
    }


def _candidate_index_keys(features: Dict[str, Any]) -> List[Tuple[str, str]]:
    keys: List[Tuple[str, str]] = []
    for token in sorted(features.get("identifier_tokens") or []):
        keys.append(("identifier_exact", token))
    for family in sorted(features.get("identifier_families") or []):
        keys.append(("contract_family", family))

    recipient_uei = features.get("recipient_uei")
    if recipient_uei:
        keys.append(("recipient_uei", str(recipient_uei)))

    recipient_name = features.get("recipient_name")
    if recipient_name:
        keys.append(("recipient_name", str(recipient_name)))

    for item in features.get("awarding_matches") or []:
        if item.get("key"):
            keys.append(("awarding_agency", str(item["key"])))
    for item in features.get("funding_matches") or []:
        if item.get("key"):
            keys.append(("funding_agency", str(item["key"])))

    psc_code = features.get("psc_code")
    if psc_code:
        keys.append(("psc", str(psc_code)))

    naics_code = features.get("naics_code")
    if naics_code:
        keys.append(("naics", str(naics_code)))

    place_region = features.get("place_region")
    if place_region:
        keys.append(("place_region", str(place_region)))

    return keys


def _score_candidate_join_pair(
    *,
    sam_event: Event,
    sam_features: Dict[str, Any],
    usaspending_event: Event,
    usaspending_features: Dict[str, Any],
    history_days: int,
) -> Optional[Dict[str, Any]]:
    evidence: List[Dict[str, Any]] = []

    exact_identifier_matches: List[Dict[str, Any]] = []
    sam_identifiers = sam_features.get("identifiers") or []
    usa_identifiers = usaspending_features.get("identifiers") or []
    for sam_id in sam_identifiers:
        for usa_id in usa_identifiers:
            if sam_id.get("token") and sam_id.get("token") == usa_id.get("token"):
                exact_identifier_matches.append(
                    _make_match_value(
                        sam_field=str(sam_id.get("field")),
                        sam_value=sam_id.get("display"),
                        usaspending_field=str(usa_id.get("field")),
                        usaspending_value=usa_id.get("display"),
                    )
                )
    if exact_identifier_matches:
        evidence.append(
            _evidence_entry(
                evidence_type="identifier_exact",
                weight=70,
                matched_values=exact_identifier_matches,
                description="SAM opportunity and USAspending award share an exact normalized identifier.",
            )
        )
    else:
        family_matches: List[Dict[str, Any]] = []
        seen_family_pairs: set[tuple[str, str, str]] = set()
        for sam_id in sam_identifiers:
            sam_token = sam_id.get("token")
            sam_family = sam_id.get("family")
            for usa_id in usa_identifiers:
                usa_token = usa_id.get("token")
                usa_family = usa_id.get("family")
                family = None
                match_kind = "family"
                if sam_family and usa_family and sam_family == usa_family:
                    family = sam_family
                elif (
                    sam_token
                    and usa_token
                    and sam_token != usa_token
                    and min(len(str(sam_token)), len(str(usa_token))) >= 8
                    and (str(sam_token).startswith(str(usa_token)) or str(usa_token).startswith(str(sam_token)))
                ):
                    family = min((str(sam_token), str(usa_token)), key=len)
                    match_kind = "prefix_family"
                if not family:
                    continue
                pair_key = (str(sam_id.get("field")), str(usa_id.get("field")), str(family))
                if pair_key in seen_family_pairs:
                    continue
                seen_family_pairs.add(pair_key)
                family_matches.append(
                    _make_match_value(
                        sam_field=str(sam_id.get("field")),
                        sam_value=sam_id.get("display"),
                        usaspending_field=str(usa_id.get("field")),
                        usaspending_value=usa_id.get("display"),
                        match_kind=match_kind,
                    )
                )
        if family_matches:
            evidence.append(
                _evidence_entry(
                    evidence_type="contract_family",
                    weight=45,
                    matched_values=family_matches,
                    description="Identifiers share a contract-family root after deterministic normalization.",
                )
            )

    sam_uei = sam_features.get("recipient_uei")
    usa_uei = usaspending_features.get("recipient_uei")
    if sam_uei and sam_uei == usa_uei:
        evidence.append(
            _evidence_entry(
                evidence_type="recipient_uei",
                weight=30,
                matched_values=[
                    _make_match_value(
                        sam_field="recipient_uei",
                        sam_value=sam_features.get("recipient_uei_display") or sam_uei,
                        usaspending_field="recipient_uei",
                        usaspending_value=usaspending_features.get("recipient_uei_display") or usa_uei,
                    )
                ],
                description="Recipient UEI matches exactly.",
            )
        )

    sam_name = sam_features.get("recipient_name")
    usa_name = usaspending_features.get("recipient_name")
    if sam_name and sam_name == usa_name:
        evidence.append(
            _evidence_entry(
                evidence_type="recipient_name",
                weight=20,
                matched_values=[
                    _make_match_value(
                        sam_field="recipient_name",
                        sam_value=sam_features.get("recipient_name_display") or sam_name,
                        usaspending_field="recipient_name",
                        usaspending_value=usaspending_features.get("recipient_name_display") or usa_name,
                    )
                ],
                description="Recipient names match after deterministic normalization.",
            )
        )

    def _agency_matches(
        sam_items: List[Dict[str, Any]],
        usa_items: List[Dict[str, Any]],
        evidence_type: str,
        field_name: str,
        weight: int,
        description: str,
    ) -> None:
        matched: List[Dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for sam_item in sam_items:
            for usa_item in usa_items:
                if sam_item.get("key") and sam_item.get("key") == usa_item.get("key"):
                    dedupe = (
                        str(sam_item.get("match_kind")),
                        str(usa_item.get("match_kind")),
                        str(sam_item.get("key")),
                    )
                    if dedupe in seen:
                        continue
                    seen.add(dedupe)
                    matched.append(
                        _make_match_value(
                            sam_field=field_name,
                            sam_value=sam_item.get("display"),
                            usaspending_field=field_name,
                            usaspending_value=usa_item.get("display"),
                            match_kind=str(sam_item.get("match_kind") or usa_item.get("match_kind") or "exact"),
                        )
                    )
        if matched:
            evidence.append(
                _evidence_entry(
                    evidence_type=evidence_type,
                    weight=weight,
                    matched_values=matched,
                    description=description,
                )
            )

    _agency_matches(
        sam_features.get("awarding_matches") or [],
        usaspending_features.get("awarding_matches") or [],
        "awarding_agency",
        "awarding_agency",
        12,
        "Awarding agency aligns by code or normalized name.",
    )
    _agency_matches(
        sam_features.get("funding_matches") or [],
        usaspending_features.get("funding_matches") or [],
        "funding_agency",
        "funding_agency",
        8,
        "Funding agency aligns by code or normalized name.",
    )

    sam_psc = sam_features.get("psc_code")
    usa_psc = usaspending_features.get("psc_code")
    if sam_psc and sam_psc == usa_psc:
        evidence.append(
            _evidence_entry(
                evidence_type="psc",
                weight=10,
                matched_values=[
                    _make_match_value(
                        sam_field="psc_code",
                        sam_value=sam_psc,
                        usaspending_field="psc_code",
                        usaspending_value=usa_psc,
                    )
                ],
                description="PSC codes match exactly.",
            )
        )

    sam_naics = sam_features.get("naics_code")
    usa_naics = usaspending_features.get("naics_code")
    if sam_naics and sam_naics == usa_naics:
        evidence.append(
            _evidence_entry(
                evidence_type="naics",
                weight=10,
                matched_values=[
                    _make_match_value(
                        sam_field="naics_code",
                        sam_value=sam_naics,
                        usaspending_field="naics_code",
                        usaspending_value=usa_naics,
                    )
                ],
                description="NAICS codes match exactly.",
            )
        )

    sam_region = sam_features.get("place_region")
    usa_region = usaspending_features.get("place_region")
    if sam_region and sam_region == usa_region:
        evidence.append(
            _evidence_entry(
                evidence_type="place_region",
                weight=8,
                matched_values=[
                    _make_match_value(
                        sam_field="place_region",
                        sam_value=sam_region,
                        usaspending_field="place_region",
                        usaspending_value=usa_region,
                    )
                ],
                description="Place-of-performance region aligns.",
            )
        )

    sam_ts = sam_features.get("ts")
    usa_ts = usaspending_features.get("ts")
    time_delta_days: Optional[float] = None
    if sam_ts and usa_ts:
        delta_days = (sam_ts - usa_ts).total_seconds() / 86400.0
        if delta_days >= 0.0 and delta_days <= float(history_days):
            time_delta_days = round(delta_days, 1)
            if delta_days <= 90:
                weight = 10
            elif delta_days <= 180:
                weight = 7
            else:
                weight = 5
            evidence.append(
                _evidence_entry(
                    evidence_type="time_window",
                    weight=weight,
                    matched_values=[
                        {
                            "sam_occurred_at": sam_ts.isoformat(),
                            "usaspending_occurred_at": usa_ts.isoformat(),
                            "delta_days": time_delta_days,
                            "direction": "prior_award",
                        }
                    ],
                    description="USAspending action predates the SAM notice within the bounded lookback window.",
                )
            )

    if not evidence:
        return None

    score = min(100, sum(int(item.get("weight") or 0) for item in evidence))
    evidence_types = [str(item.get("type")) for item in evidence]
    likely_incumbent = (
        time_delta_days is not None
        and (
            "identifier_exact" in evidence_types
            or "contract_family" in evidence_types
            or (
                any(t in evidence_types for t in ("recipient_uei", "recipient_name"))
                and any(t in evidence_types for t in ("awarding_agency", "funding_agency", "psc", "naics", "place_region"))
            )
        )
    )

    sam_label = (
        sam_event.solicitation_number
        or sam_event.notice_id
        or sam_event.document_id
        or sam_event.doc_id
        or sam_event.hash
    )
    usa_label = (
        usaspending_event.award_id
        or usaspending_event.generated_unique_award_id
        or usaspending_event.piid
        or usaspending_event.doc_id
        or usaspending_event.hash
    )

    summary_bits = [f"SAM {sam_label}", f"USAspending {usa_label}", f"score {score}"]
    if likely_incumbent:
        summary_bits.append("likely incumbent-style")
    summary_bits.append("signals=" + ",".join(evidence_types))
    summary = " | ".join(summary_bits)

    rationale = " ; ".join(
        f"{item['type']} (+{item['weight']}): {item['description']}" for item in evidence
    )

    return {
        "score": int(score),
        "likely_incumbent": bool(likely_incumbent),
        "time_delta_days": time_delta_days,
        "evidence": evidence,
        "evidence_types": evidence_types,
        "matched_values": {
            str(item["type"]): item.get("matched_values") or []
            for item in evidence
        },
        "summary": summary,
        "rationale": rationale,
    }


def rebuild_sam_usaspending_candidate_joins(
    *,
    window_days: int = 30,
    history_days: int = 365,
    min_score: int = 45,
    max_matches_per_key: int = 25,
    max_candidates_per_sam: int = 10,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    window_days = int(window_days)
    history_days = int(history_days)
    min_score = int(min_score)
    max_matches_per_key = int(max_matches_per_key)
    max_candidates_per_sam = int(max_candidates_per_sam)

    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if history_days <= 0:
        raise ValueError("history_days must be > 0")
    if min_score <= 0:
        raise ValueError("min_score must be > 0")
    if max_matches_per_key < 1:
        raise ValueError("max_matches_per_key must be >= 1")
    if max_candidates_per_sam < 1:
        raise ValueError("max_candidates_per_sam must be >= 1")

    now = datetime.now(timezone.utc)
    sam_since = now - timedelta(days=window_days)
    usa_since = now - timedelta(days=history_days)
    key_prefix = correlation_key_prefix(window_days=window_days, history_days=history_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()
    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        sam_events = (
            db.query(Event)
            .filter(Event.source == "SAM.gov")
            .filter(ts >= sam_since)
            .order_by(Event.id.asc())
            .all()
        )
        usaspending_events = (
            db.query(Event)
            .filter(Event.source == "USAspending")
            .filter(ts >= usa_since)
            .order_by(Event.id.asc())
            .all()
        )

        sam_features = {int(ev.id): _extract_candidate_join_features(ev) for ev in sam_events}
        usa_features = {int(ev.id): _extract_candidate_join_features(ev) for ev in usaspending_events}
        usaspending_by_id = {int(ev.id): ev for ev in usaspending_events}

        usa_index: Dict[str, Dict[str, set[int]]] = {}
        for ev in usaspending_events:
            features = usa_features[int(ev.id)]
            for key_type, key_value in _candidate_index_keys(features):
                if not key_value:
                    continue
                type_bucket = usa_index.setdefault(key_type, {})
                type_bucket.setdefault(key_value, set()).add(int(ev.id))

        blocked_keys: Dict[str, set[str]] = {}
        blocked_key_counts: Dict[str, int] = {}
        for key_type, mapping in usa_index.items():
            blocked = {key for key, ids in mapping.items() if len(ids) > max_matches_per_key}
            blocked_keys[key_type] = blocked
            blocked_key_counts[key_type] = len(blocked)

        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        eligible_keys: set[str] = set()
        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0
        candidate_pairs_considered = 0
        candidate_pairs_above_threshold = 0
        candidate_pairs_trimmed = 0
        sam_events_with_candidates = 0
        likely_incumbent_count = 0
        rejected_common_keys: Dict[str, int] = {}
        top_scores: List[Dict[str, Any]] = []

        for sam_event in sam_events:
            features = sam_features[int(sam_event.id)]
            candidate_ids: set[int] = set()
            for key_type, key_value in _candidate_index_keys(features):
                if not key_value:
                    continue
                if key_value in blocked_keys.get(key_type, set()):
                    rejected_common_keys[key_type] = rejected_common_keys.get(key_type, 0) + 1
                    continue
                ids = (usa_index.get(key_type) or {}).get(key_value)
                if ids:
                    candidate_ids.update(ids)

            scored_candidates: List[Tuple[int, int, int, int, Dict[str, Any], Event]] = []
            for usa_event_id in sorted(candidate_ids):
                candidate_pairs_considered += 1
                usa_event = usaspending_by_id.get(int(usa_event_id))
                usa_feature = usa_features.get(int(usa_event_id))
                if usa_event is None or usa_feature is None:
                    continue
                candidate = _score_candidate_join_pair(
                    sam_event=sam_event,
                    sam_features=features,
                    usaspending_event=usa_event,
                    usaspending_features=usa_feature,
                    history_days=history_days,
                )
                if candidate is None:
                    continue
                if int(candidate.get("score") or 0) < min_score:
                    continue
                candidate_pairs_above_threshold += 1
                scored_candidates.append(
                    (
                        -int(candidate["score"]),
                        0 if candidate.get("likely_incumbent") else 1,
                        -len(candidate.get("evidence_types") or []),
                        int(usa_event.id),
                        candidate,
                        usa_event,
                    )
                )

            scored_candidates.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
            selected = scored_candidates[:max_candidates_per_sam]
            candidate_pairs_trimmed += max(0, len(scored_candidates) - len(selected))
            if selected:
                sam_events_with_candidates += 1

            for _score_ord, _incumbent_ord, _signal_ord, _usa_id, candidate, usa_event in selected:
                pair_material = f"{sam_event.hash}|{usa_event.hash}".encode("utf-8")
                pair_digest = hashlib.sha1(pair_material).hexdigest()[:20]
                key = f"{key_prefix}{pair_digest}"
                eligible_keys.add(key)

                if candidate.get("likely_incumbent"):
                    likely_incumbent_count += 1

                lanes_hit = {
                    "lane": CANDIDATE_JOIN_LANE,
                    "confidence_score": int(candidate["score"]),
                    "history_days": history_days,
                    "sam_event_id": int(sam_event.id),
                    "sam_event_hash": sam_event.hash,
                    "usaspending_event_id": int(usa_event.id),
                    "usaspending_event_hash": usa_event.hash,
                    "time_delta_days": candidate.get("time_delta_days"),
                    "likely_incumbent": bool(candidate.get("likely_incumbent")),
                    "evidence_types": candidate.get("evidence_types") or [],
                    "matched_values": candidate.get("matched_values") or {},
                    "evidence": candidate.get("evidence") or [],
                }

                if dry_run:
                    continue

                c = existing_by_key.get(key)
                if c is None:
                    c = Correlation(
                        correlation_key=key,
                        score=str(int(candidate["score"])),
                        window_days=window_days,
                        radius_km=0.0,
                        lanes_hit=lanes_hit,
                        summary=str(candidate.get("summary") or ""),
                        rationale=str(candidate.get("rationale") or ""),
                        created_at=now,
                    )
                    db.add(c)
                    db.flush()
                    correlations_created += 1
                else:
                    c.score = str(int(candidate["score"]))
                    c.window_days = window_days
                    c.radius_km = 0.0
                    c.lanes_hit = lanes_hit
                    c.summary = str(candidate.get("summary") or "")
                    c.rationale = str(candidate.get("rationale") or "")
                    c.created_at = now
                    correlations_updated += 1

                links_deleted += (
                    db.query(CorrelationLink)
                    .filter(CorrelationLink.correlation_id == int(c.id))
                    .delete(synchronize_session=False)
                )
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(sam_event.id)))
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(usa_event.id)))
                links_created += 2

                top_scores.append(
                    {
                        "score": int(candidate["score"]),
                        "sam_event_hash": sam_event.hash,
                        "usaspending_event_hash": usa_event.hash,
                        "likely_incumbent": bool(candidate.get("likely_incumbent")),
                        "evidence_types": candidate.get("evidence_types") or [],
                    }
                )

        if not dry_run:
            stale_keys = [key for key in existing_by_key.keys() if key not in eligible_keys]
            if stale_keys:
                correlations_deleted = (
                    db.query(Correlation)
                    .filter(Correlation.correlation_key.in_(stale_keys))
                    .delete(synchronize_session=False)
                )
            db.commit()

        top_scores.sort(
            key=lambda item: (
                -int(item.get("score") or 0),
                0 if item.get("likely_incumbent") else 1,
                str(item.get("sam_event_hash") or ""),
                str(item.get("usaspending_event_hash") or ""),
            )
        )

        return {
            "status": "ok",
            "dry_run": dry_run,
            "window_days": window_days,
            "history_days": history_days,
            "min_score": min_score,
            "max_matches_per_key": max_matches_per_key,
            "max_candidates_per_sam": max_candidates_per_sam,
            "sam_events_seen": len(sam_events),
            "usaspending_events_seen": len(usaspending_events),
            "sam_events_with_candidates": sam_events_with_candidates,
            "candidate_pairs_considered": candidate_pairs_considered,
            "candidate_pairs_above_threshold": candidate_pairs_above_threshold,
            "candidate_pairs_trimmed": candidate_pairs_trimmed,
            "blocked_key_counts": blocked_key_counts,
            "rejected_common_keys": rejected_common_keys,
            "likely_incumbent_count": likely_incumbent_count,
            "top_matches": top_scores[:20],
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()


__all__ = [
    "CANDIDATE_JOIN_LANE",
    "correlation_key_prefix",
    "rebuild_sam_usaspending_candidate_joins",
]
