"""SAM.gov Get Opportunities Public API connector for ShadowScope."""
from __future__ import annotations

import hashlib
import json
import os
import logging
import random
import time
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import requests
from pydantic import BaseModel, Field

from backend.connectors.samgov_context import extract_sam_context_fields, merge_sam_context_fields

logger = logging.getLogger(__name__)

BASE_URL = (os.getenv("SAM_API_BASE_URL") or "").strip() or "https://api.sam.gov/prod/opportunities/v2/search"
MAX_LIMIT = 1000
SOURCE_NAME = "SAM.gov"

# SHADOWSCOPE:RETRY_KNOBS:START
def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


DEFAULT_TIMEOUT = _env_int("SAM_API_TIMEOUT_SECONDS", 60)
DEFAULT_MAX_RETRIES = _env_int("SAM_API_MAX_RETRIES", 8)
DEFAULT_BACKOFF_BASE = _env_float("SAM_API_BACKOFF_BASE", 0.75)
# SHADOWSCOPE:RETRY_KNOBS:END



class SamGovError(RuntimeError):
    """Base error for SAM.gov connector failures."""


class SamGovAuthError(SamGovError):
    """Raised when the API key is missing/invalid/unauthorized."""


class OpportunityFilter(BaseModel):
    """Input parameters for the SAM.gov Get Opportunities v2 search endpoint."""

    posted_from: date
    posted_to: date
    limit: int = Field(default=100, ge=1, le=MAX_LIMIT)
    offset: int = Field(default=0, ge=0)
    title: Optional[str] = None


def _format_mmddyyyy(d: date) -> str:
    return d.strftime("%m/%d/%Y")


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value)
    v = value.strip()
    if not v or v.lower() == "null":
        return None
    return v


# SHADOWSCOPE:RETRY_AFTER:START
def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
    """Parse Retry-After header into seconds (delta-seconds or HTTP-date)."""
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    if v.isdigit():
        return float(v)
    try:
        from email.utils import parsedate_to_datetime
        from datetime import datetime, timezone

        dt = parsedate_to_datetime(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0.0, (dt - now).total_seconds())
    except Exception:
        return None
# SHADOWSCOPE:RETRY_AFTER:END

def _get_with_retries(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
) -> requests.Response:
    rate_limit_retries = 0
    retry_sleep_seconds_total = 0.0

    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)

            # Retry on common transient statuses
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)

            meta = {
                "attempts": int(attempt + 1),
                "retries": int(attempt),
                "rate_limit_retries": int(rate_limit_retries),
                "retry_sleep_seconds_total": round(float(retry_sleep_seconds_total), 3),
            }
            try:
                setattr(resp, "_shadow_retry_meta", meta)
            except Exception:
                pass
            return resp

        except requests.RequestException as exc:
            if attempt >= max_retries:
                raise
            sleep_s = None
            resp = getattr(exc, "response", None)
            if resp is not None and getattr(resp, "status_code", None) == 429:
                rate_limit_retries += 1
                ra = _retry_after_seconds(resp)
                if ra is not None:
                    sleep_s = ra
            if sleep_s is None:
                sleep_s = min(60.0, backoff_base * (2**attempt)) + random.random()
            retry_sleep_seconds_total += float(sleep_s)
            logger.warning(
                "SAM.gov request failed (%s). Retry %d/%d in %.1fs",
                type(exc).__name__,
                attempt + 1,
                max_retries,
                sleep_s,
            )
            time.sleep(sleep_s)

    raise SamGovError("Unexpected retry loop termination")

def fetch_opportunities_page(
    session: requests.Session,
    filters: OpportunityFilter,
    api_key: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    """Fetch a single page of opportunities."""

    params: Dict[str, Any] = {
        "api_key": api_key,
        "postedFrom": _format_mmddyyyy(filters.posted_from),
        "postedTo": _format_mmddyyyy(filters.posted_to),
        "limit": int(filters.limit),
        "offset": int(filters.offset),
    }
    if filters.title:
        params["title"] = filters.title

    resp = _get_with_retries(session, BASE_URL, params=params, timeout=timeout)
    request_meta = getattr(resp, "_shadow_retry_meta", None)
    text = resp.text or ""

    # SAM docs describe 404 as "No Data found"
    if resp.status_code == 404:
        return {
            "totalRecords": 0,
            "limit": int(filters.limit),
            "offset": int(filters.offset),
            "opportunitiesData": [],
            "_shadow_request_meta": request_meta if isinstance(request_meta, dict) else {},
        }

    lower = text.lower()
    if resp.status_code in (401, 403) or ("invalid api_key" in lower) or ("no api_key" in lower):
        raise SamGovAuthError("SAM.gov API key is missing/invalid/unauthorized (check SAM_API_KEY).")

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        logger.error("SAM.gov error %s: %s", resp.status_code, text[:2000])
        raise

    try:
        payload = resp.json()
    except ValueError as exc:
        logger.error("SAM.gov response was not JSON: %s", text[:2000])
        raise SamGovError("SAM.gov returned a non-JSON response") from exc

    if isinstance(payload, dict) and isinstance(request_meta, dict):
        payload["_shadow_request_meta"] = dict(request_meta)

    # Sometimes upstreams return an error blob even with 200s
    if isinstance(payload, dict) and payload.get("error"):
        msg = str(payload.get("error"))
        if "invalid api_key" in msg.lower() or "no api_key" in msg.lower():
            raise SamGovAuthError("SAM.gov API key is missing/invalid/unauthorized (check SAM_API_KEY).")
        raise SamGovError(msg)

    return payload

def _parse_posted_date(value: Optional[str]) -> Optional[datetime]:
    v = _clean_str(value)
    if not v:
        return None

    # Common formats seen in docs + real-world responses
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue

    # ISO-ish fallback (handle Z)
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        logger.debug("Unable to parse SAM.gov postedDate: %s", v)
        return None


def _render_location(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict) or not obj:
        return None

    street1 = _clean_str(obj.get("streetAddress"))
    street2 = _clean_str(obj.get("streetAddress2"))
    city = obj.get("city")
    city_name = _clean_str(city.get("name")) if isinstance(city, dict) else _clean_str(city)

    state = obj.get("state")
    state_code = None
    if isinstance(state, dict):
        state_code = _clean_str(state.get("code") or state.get("name"))
    else:
        state_code = _clean_str(state)

    zip_code = _clean_str(obj.get("zip") or obj.get("zipcode") or obj.get("postalCode"))

    country = obj.get("country")
    if country is None:
        country = obj.get("countryCode") or obj.get("country_code") or obj.get("countrycode")
    country_code = None
    if isinstance(country, dict):
        country_code = _clean_str(country.get("code") or country.get("name"))
    else:
        country_code = _clean_str(country)

    parts: List[str] = []
    if street1:
        parts.append(street1)
    if street2:
        parts.append(street2)

    locality: List[str] = []
    if city_name:
        locality.append(city_name)
    if state_code:
        locality.append(state_code)

    if zip_code:
        if locality:
            locality[-1] = f"{locality[-1]} {zip_code}".strip()
        else:
            locality.append(zip_code)

    if locality:
        parts.append(", ".join(locality))

    if country_code and country_code.upper() != "USA":
        parts.append(country_code)

    return ", ".join([p for p in parts if p])


def _normalize_source_url(notice_id: Optional[str], ui_link: Optional[str]) -> Optional[str]:
    ui = _clean_str(ui_link)
    if ui:
        # Old examples use beta.sam.gov; normalize to sam.gov for stability
        return ui.replace("beta.sam.gov", "sam.gov")
    if notice_id:
        return f"https://sam.gov/opp/{notice_id}/view"
    return None


def normalize_opportunities(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize raw SAM.gov opportunities into event dictionaries."""
    events: List[Dict[str, Any]] = []

    for record in records:
        notice_id = _clean_str(record.get("noticeId") or record.get("noticeid") or record.get("notice_id"))
        doc_id = notice_id

        if notice_id:
            unique_key = f"{SOURCE_NAME}:{notice_id}"
        else:
            unique_key = json.dumps(record, sort_keys=True, default=str)

        digest = hashlib.sha256(unique_key.encode("utf-8")).hexdigest()

        occurred_at = _parse_posted_date(_clean_str(record.get("postedDate") or record.get("posted_date")))
        snippet = _clean_str(record.get("title"))

        pop_text = _render_location(record.get("placeOfPerformance"))
        if not pop_text:
            pop_text = _render_location(record.get("officeAddress"))

        source_url = _normalize_source_url(notice_id, _clean_str(record.get("uiLink")))

        # Preserve full payload, but copy awardee identifiers into canonical keys if present.
        raw_json: Dict[str, Any] = dict(record)
        award = raw_json.get("award")
        if isinstance(award, dict):
            awardee = award.get("awardee")
            if isinstance(awardee, dict):
                a_name = _clean_str(awardee.get("name"))
                a_uei = _clean_str(awardee.get("ueiSAM") or awardee.get("ueiSam") or awardee.get("uei"))
                if a_name and not raw_json.get("Recipient Name"):
                    raw_json["Recipient Name"] = a_name
                if a_uei and not raw_json.get("Recipient UEI"):
                    raw_json["Recipient UEI"] = a_uei

        # SAM context contract: promote a focused set of high-value fields for
        # research pivots (agency path, notice metadata, NAICS/set-aside, dates).
        sam_ctx = extract_sam_context_fields(raw_json)
        raw_json = merge_sam_context_fields(raw_json, sam_ctx)

        events.append(
            {
                "category": "procurement",
                "occurred_at": occurred_at,
                "source": SOURCE_NAME,
                "source_url": source_url,
                "doc_id": doc_id,
                "place_text": pop_text,
                "snippet": snippet,
                "raw_json": raw_json,
                "keywords": [],
                "clauses": [],
                "lat": None,
                "lon": None,
                "hash": digest,
            }
        )

    return events


__all__ = [
    "BASE_URL",
    "DEFAULT_TIMEOUT",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_BACKOFF_BASE",
    "MAX_LIMIT",
    "SOURCE_NAME",
    "OpportunityFilter",
    "SamGovError",
    "SamGovAuthError",
    "fetch_opportunities_page",
    "normalize_opportunities",
]

