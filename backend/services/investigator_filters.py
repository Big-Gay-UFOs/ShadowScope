from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import String, and_, cast, func, or_

from backend.db.models import Event


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def event_time_expr(model: Any = Event):
    return func.coalesce(model.occurred_at, model.created_at)


def event_place_region_label(event: Any) -> str | None:
    state = str(getattr(event, "place_of_performance_state", "") or "").strip().upper()
    country = str(getattr(event, "place_of_performance_country", "") or "").strip().upper()
    if state and country:
        return f"{state}, {country}"
    if state:
        return state
    if country:
        return country
    place_text = str(getattr(event, "place_text", "") or "").strip()
    return place_text or None


def investigator_event_filters_present(
    *,
    source: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    entity_id: int | None = None,
    keyword: str | None = None,
    agency: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    award_id: str | None = None,
    recipient_uei: str | None = None,
    place_region: str | None = None,
) -> bool:
    return any(
        [
            bool(str(source or "").strip()),
            date_from is not None,
            date_to is not None,
            entity_id is not None,
            bool(str(keyword or "").strip()),
            bool(str(agency or "").strip()),
            bool(str(psc or "").strip()),
            bool(str(naics or "").strip()),
            bool(str(award_id or "").strip()),
            bool(str(recipient_uei or "").strip()),
            bool(str(place_region or "").strip()),
        ]
    )


def investigator_event_conditions(
    *,
    model: Any = Event,
    source: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    entity_id: int | None = None,
    keyword: str | None = None,
    agency: str | None = None,
    psc: str | None = None,
    naics: str | None = None,
    award_id: str | None = None,
    recipient_uei: str | None = None,
    place_region: str | None = None,
) -> list[Any]:
    conditions: list[Any] = []

    source_value = str(source or "").strip()
    if source_value:
        conditions.append(model.source == source_value)

    ts_expr = event_time_expr(model)
    if date_from is not None:
        conditions.append(ts_expr >= date_from)
    if date_to is not None:
        conditions.append(ts_expr <= date_to)

    if entity_id is not None:
        conditions.append(model.entity_id == int(entity_id))

    keyword_value = str(keyword or "").strip()
    if keyword_value:
        kw_esc = _escape_like(keyword_value)
        conditions.append(cast(model.keywords, String).like(f'%"{kw_esc}"%', escape="\\"))

    award_value = str(award_id or "").strip()
    if award_value:
        conditions.append(
            or_(
                model.award_id == award_value,
                model.generated_unique_award_id == award_value,
            )
        )

    uei_value = str(recipient_uei or "").strip().upper()
    if uei_value:
        conditions.append(func.upper(model.recipient_uei) == uei_value)

    agency_value = str(agency or "").strip()
    if agency_value:
        agency_upper = agency_value.upper()
        agency_pattern = f"%{_escape_like(agency_value.lower())}%"
        conditions.append(
            or_(
                func.upper(model.awarding_agency_code) == agency_upper,
                func.upper(model.funding_agency_code) == agency_upper,
                func.upper(model.contracting_office_code) == agency_upper,
                func.lower(func.coalesce(cast(model.awarding_agency_name, String), "")).like(agency_pattern, escape="\\"),
                func.lower(func.coalesce(cast(model.funding_agency_name, String), "")).like(agency_pattern, escape="\\"),
                func.lower(func.coalesce(cast(model.contracting_office_name, String), "")).like(agency_pattern, escape="\\"),
            )
        )

    psc_value = str(psc or "").strip()
    if psc_value:
        psc_upper = psc_value.upper()
        psc_pattern = f"%{_escape_like(psc_value.lower())}%"
        conditions.append(
            or_(
                func.upper(model.psc_code) == psc_upper,
                func.lower(func.coalesce(cast(model.psc_description, String), "")).like(psc_pattern, escape="\\"),
            )
        )

    naics_value = str(naics or "").strip()
    if naics_value:
        naics_upper = naics_value.upper()
        naics_pattern = f"%{_escape_like(naics_value.lower())}%"
        conditions.append(
            or_(
                func.upper(model.naics_code) == naics_upper,
                func.lower(func.coalesce(cast(model.naics_description, String), "")).like(naics_pattern, escape="\\"),
            )
        )

    region_value = str(place_region or "").strip()
    if region_value:
        normalized = region_value.replace("|", ",").replace("/", ",")
        parts = [part.strip().upper() for part in normalized.split(",") if part.strip()]
        place_text_upper = func.upper(func.coalesce(cast(model.place_text, String), ""))
        if len(parts) >= 2:
            first = parts[0]
            second = parts[1]
            conditions.append(
                or_(
                    and_(
                        func.upper(func.coalesce(model.place_of_performance_state, "")) == first,
                        func.upper(func.coalesce(model.place_of_performance_country, "")) == second,
                    ),
                    and_(
                        func.upper(func.coalesce(model.place_of_performance_state, "")) == second,
                        func.upper(func.coalesce(model.place_of_performance_country, "")) == first,
                    ),
                    place_text_upper.like(f"%{_escape_like(region_value.upper())}%", escape="\\"),
                )
            )
        else:
            token = parts[0] if parts else region_value.upper()
            conditions.append(
                or_(
                    func.upper(func.coalesce(model.place_of_performance_state, "")) == token,
                    func.upper(func.coalesce(model.place_of_performance_country, "")) == token,
                    place_text_upper.like(f"%{_escape_like(token)}%", escape="\\"),
                )
            )

    return conditions


__all__ = [
    "event_place_region_label",
    "event_time_expr",
    "investigator_event_conditions",
    "investigator_event_filters_present",
]
