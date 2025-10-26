"""USAspending API connector for ShadowScope."""
from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import requests
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

BASE_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
DEFAULT_LIMIT = 200
MAX_LIMIT = 500
SOURCE_NAME = "USAspending"


class AwardFilter(BaseModel):
    """Input parameters for the USAspending search API."""

    since: date = Field(default=date(2008, 1, 1))
    limit: int = Field(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT)
    page: int = Field(default=1, ge=1)


class AwardEvent(BaseModel):
    category: str
    occurred_at: Optional[datetime]
    source: str
    source_url: Optional[str]
    doc_id: Optional[str]
    place_text: Optional[str]
    snippet: Optional[str]
    raw_json: Dict[str, Any]
    keywords: List[str] = Field(default_factory=list)
    clauses: List[str] = Field(default_factory=list)
    lat: Optional[float] = None
    lon: Optional[float] = None
    hash: str


def _build_request_payload(filters: AwardFilter) -> Dict[str, Any]:
    return {
        "fields": [
            "Award ID",
            "Recipient Name",
            "Action Date",
            "Award Amount",
            "Awarding Agency",
            "Funding Agency",
            "Description",
            "Place of Performance",
        ],
        "filters": {
            "award_type_codes": ["A", "B", "C", "D", "IDV"],
            "time_period": [
                {
                    "start_date": filters.since.isoformat(),
                    "end_date": date.today().isoformat(),
                }
            ],
        },
        "limit": filters.limit,
        "page": filters.page,
        "sort": "Action Date",
        "order": "desc",
    }


def fetch_awards(
    since: str = "2008-01-01",
    limit: int = 2000,
    session: Optional[requests.Session] = None,
) -> Iterable[Dict[str, Any]]:
    """Fetch awards from USAspending with automatic pagination."""
    sess = session or requests.Session()
    remaining = max(limit, 0)
    page = 1

    while remaining > 0:
        page_limit = min(MAX_LIMIT, remaining)
        filters = AwardFilter(since=date.fromisoformat(since), limit=page_limit, page=page)
        payload = _build_request_payload(filters)
        logger.debug("USAspending request payload: %s", payload)

        response = sess.post(BASE_URL, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        logger.info("Fetched %d awards from USAspending (page %d)", len(results), page)

        for record in results:
            yield record

        if len(results) < page_limit:
            break
        remaining -= len(results)
        page += 1


def normalize_awards(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize raw USAspending response rows into event dictionaries."""
    events: List[Dict[str, Any]] = []
    for record in records:
        unique_key = str(record.get("internal_id") or record.get("generated_unique_award_id") or record)
        digest = hashlib.sha256(unique_key.encode("utf-8")).hexdigest()
        action_date = record.get("Action Date") or record.get("action_date")
        place = record.get("Place of Performance") or record.get("place_of_performance")
        description = record.get("Description") or record.get("description")
        occurred_at = _parse_date(action_date)

        doc_identifier = record.get("piid") or record.get("Award ID") or record.get("generated_unique_award_id")
        event = AwardEvent(
            category="procurement",
            occurred_at=occurred_at,
            source=SOURCE_NAME,
            source_url="https://www.usaspending.gov/award/{}".format(record.get("generated_unique_award_id", "")),
            doc_id=str(doc_identifier) if doc_identifier else None,
            place_text=place,
            snippet=description,
            raw_json=record,
            hash=digest,
        )
        events.append(event.dict())
    return events


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        logger.debug("Unable to parse action date: %s", value)
        return None
