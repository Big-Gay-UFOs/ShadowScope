"""USAspending API connector for ShadowScope."""
from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import requests
import time
import random
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


def _post_with_retries(session, url, payload, timeout=60, max_retries=8, backoff_base=0.75):
    for attempt in range(max_retries + 1):
        try:
            resp = session.post(url, json=payload, timeout=timeout)
            # Retry on common transient statuses
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            return resp
        except requests.RequestException as exc:
            if attempt >= max_retries:
                raise
            sleep_s = min(60.0, backoff_base * (2 ** attempt)) + random.random()
            logger.warning("USAspending request failed (%s). Retry %d/%d in %.1fs", type(exc).__name__, attempt + 1, max_retries, sleep_s)
            time.sleep(sleep_s)

BASE_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

# Practical max page size for this endpoint. If you want 200, we do 2 pages of 100.
MAX_PAGE_SIZE = 100
DEFAULT_LIMIT = 100
MAX_LIMIT = 100  # USAspending spending_by_award per-page cap
SOURCE_NAME = "USAspending"


class AwardFilter(BaseModel):
    """Input parameters for the USAspending search API."""
    since: date = Field(default=date(2008, 1, 1))
    limit: int = Field(default=DEFAULT_LIMIT, ge=1, le=MAX_PAGE_SIZE)
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


def _build_request_payload(
    filters: AwardFilter,
    recipient_search_text: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Action Date",
            "Last Modified Date",
            "Description",
            "Place of Performance",
        ],
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [
                {
                    "date_type": "action_date",
                    "start_date": filters.since.isoformat(),
                    "end_date": date.today().isoformat(),
                }
            ],
        },
        "limit": filters.limit,
        "page": filters.page,
        "sort": "Last Modified Date",
        "order": "desc",
        "subawards": False,
    }

    # Optional narrowing (pre-filtering) so we don't pull the entire federal firehose.
    if recipient_search_text:
        payload["filters"]["recipient_search_text"] = recipient_search_text
    if keywords:
        payload["filters"]["keywords"] = keywords

    return payload


def fetch_awards_page(
    session: requests.Session,
    filters: AwardFilter,
    timeout: int = 60,
    recipient_search_text: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Fetch a single page of awards with the provided session."""
    payload = _build_request_payload(filters, recipient_search_text=recipient_search_text, keywords=keywords)
    logger.debug("USAspending request payload: %s", payload)

    response = _post_with_retries(session, BASE_URL, payload, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        logger.error("USAspending error %s: %s", response.status_code, response.text)
        raise

    return response.json()


def fetch_awards(
    since: str = "2008-01-01",
    limit: int = 2000,
    session: Optional[requests.Session] = None,
    max_pages: Optional[int] = None,
    recipient_search_text: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
) -> Iterable[Dict[str, Any]]:
    """Fetch awards from USAspending with automatic pagination."""
    sess = session or requests.Session()
    remaining = max(limit, 0)
    page = 1

    while remaining > 0 and (max_pages is None or page <= max_pages):
        # Force page size <= 100 to avoid 422s
        page_limit = min(MAX_PAGE_SIZE, remaining)

        filters = AwardFilter(
            since=date.fromisoformat(since),
            limit=page_limit,
            page=page,
        )

        data = fetch_awards_page(
            sess,
            filters,
            recipient_search_text=recipient_search_text,
            keywords=keywords,
        )

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

        action_date = (
            record.get("Action Date")
            or record.get("action_date")
            or record.get("Last Modified Date")
            or record.get("last_modified_date")
        )
        place = record.get("Place of Performance") or record.get("place_of_performance")
        description = record.get("Description") or record.get("description")
        occurred_at = _parse_date(action_date)

        doc_identifier = record.get("piid") or record.get("Award ID") or record.get("generated_unique_award_id")
        unique_award_id = record.get("generated_unique_award_id") or record.get("Award ID")

        source_url = f"https://www.usaspending.gov/award/{unique_award_id}" if unique_award_id else None

        event = AwardEvent(
            category="procurement",
            occurred_at=occurred_at,
            source=SOURCE_NAME,
            source_url=source_url,
            doc_id=str(doc_identifier) if doc_identifier else None,
            place_text=place,
            snippet=description,
            raw_json=record,
            hash=digest,
        )
        events.append(event.model_dump())
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
