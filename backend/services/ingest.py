"""Ingestion workflows for live data sources."""
from __future__ import annotations

import json
import os
import logging
from datetime import date, timedelta, datetime, timezone
from typing import Dict, Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from backend.connectors import usaspending, samgov
from backend.db.models import EVENT_PROMOTED_FIELDS, Event, IngestRun, get_session_factory
from backend.runtime import RAW_SOURCES, ensure_runtime_directories

LOGGER = logging.getLogger("shadowscope.ingest")


def _normalize_sam_posted_date(value: object, *, field_name: str) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a date in YYYY-MM-DD format, not a datetime.")
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a date in YYYY-MM-DD format.") from exc


def resolve_sam_posted_window(
    *,
    days: Optional[int] = None,
    posted_from: Optional[object] = None,
    posted_to: Optional[object] = None,
    today: Optional[date] = None,
) -> dict[str, object]:
    resolved_posted_from = _normalize_sam_posted_date(posted_from, field_name="posted_from")
    resolved_posted_to = _normalize_sam_posted_date(posted_to, field_name="posted_to")
    has_explicit_range = resolved_posted_from is not None or resolved_posted_to is not None

    if has_explicit_range and days is not None:
        raise ValueError("Use either days or posted_from/posted_to, but not both.")

    if has_explicit_range:
        if resolved_posted_from is None or resolved_posted_to is None:
            raise ValueError("posted_from and posted_to must be provided together in YYYY-MM-DD format.")
        if resolved_posted_from > resolved_posted_to:
            raise ValueError("posted_from must be on or before posted_to.")
        return {
            "mode": "explicit_dates",
            "requested_days": None,
            "effective_days": None,
            "posted_from": resolved_posted_from,
            "posted_to": resolved_posted_to,
            "calendar_span_days": int((resolved_posted_to - resolved_posted_from).days),
        }

    resolved_days = 30 if days is None else int(days)
    if resolved_days < 1 or resolved_days > 365:
        raise ValueError("days must be between 1 and 365.")

    anchor_day = today or date.today()
    effective_posted_to = anchor_day
    effective_posted_from = effective_posted_to - timedelta(days=resolved_days)
    return {
        "mode": "days",
        "requested_days": resolved_days,
        "effective_days": resolved_days,
        "posted_from": effective_posted_from,
        "posted_to": effective_posted_to,
        "calendar_span_days": int((effective_posted_to - effective_posted_from).days),
    }


def serialize_sam_posted_window(window: dict[str, object]) -> dict[str, object]:
    posted_from = window.get("posted_from")
    posted_to = window.get("posted_to")
    return {
        "mode": window.get("mode"),
        "requested_days": window.get("requested_days"),
        "effective_days": window.get("effective_days"),
        "posted_from": posted_from.isoformat() if isinstance(posted_from, date) else posted_from,
        "posted_to": posted_to.isoformat() if isinstance(posted_to, date) else posted_to,
        "calendar_span_days": window.get("calendar_span_days"),
    }


def format_sam_posted_window_cli_args(window: dict[str, object]) -> list[str]:
    payload = serialize_sam_posted_window(window)
    if payload.get("mode") == "explicit_dates":
        return [
            f"--posted-from {payload.get('posted_from')}",
            f"--posted-to {payload.get('posted_to')}",
        ]
    return [f"--days {int(payload.get('effective_days') or 30)}"]


def describe_sam_posted_window(window: dict[str, object]) -> str:
    payload = serialize_sam_posted_window(window)
    mode = str(payload.get("mode") or "days")
    if mode == "explicit_dates":
        return f"{payload.get('posted_from')} -> {payload.get('posted_to')} (mode=explicit_dates)"
    return (
        f"{payload.get('posted_from')} -> {payload.get('posted_to')} "
        f"(mode=days, days={payload.get('effective_days')})"
    )


def append_sam_posted_window_note(notes: Optional[str], *, window: dict[str, object]) -> str:
    payload = serialize_sam_posted_window(window)
    audit_note = (
        f"SAM postedDate window {payload.get('posted_from')}..{payload.get('posted_to')} "
        f"(mode={payload.get('mode')}"
    )
    if payload.get("effective_days") is not None:
        audit_note += f", days={payload.get('effective_days')}"
    audit_note += ")"
    base = str(notes or "").strip()
    return f"{base} | {audit_note}" if base else audit_note


def _build_retrying_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "ShadowScope/0.1"})
    return session


def ingest_usaspending(
    days: int,
    pages: int,
    page_size: int = usaspending.MAX_LIMIT,
    max_records: Optional[int] = None,
    start_page: int = 1,
    recipient_search_text: Optional[List[str]] = None, keywords: Optional[List[str]] = None, database_url: Optional[str] = None,
) -> Dict[str, object]:
    """Ingest USAspending awards into the events table.

    Semantics:
      - pages: maximum pages to request (starting at start_page)
      - page_size: max records per page (capped to <= 100 by the upstream API)
      - max_records: optional total cap across all pages (defaults to pages * page_size)
    """
    ensure_runtime_directories()
    session = _build_retrying_session()

    since = date.today() - timedelta(days=max(days, 1))

    page_size = max(1, min(int(page_size), usaspending.MAX_LIMIT))
    max_total = int(max_records) if max_records is not None else pages * page_size

    total_fetched = 0
    normalized_total = 0
    inserted = 0

    snapshot_dir = RAW_SOURCES["usaspending"] / date.today().strftime("%Y%m%d")
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    SessionFactory = get_session_factory(database_url)
    db = SessionFactory()

    run = IngestRun(
        source="USAspending",
        status="running",
        days=days,
        start_page=start_page,
        pages=pages,
        page_size=page_size,
        max_records=max_total,
        snapshot_dir=str(snapshot_dir),
    )
    db.add(run)
    db.commit()  # ensure run id exists

    try:
        for page in range(start_page, start_page + pages):
            remaining = max_total - total_fetched
            if remaining <= 0:
                break

            page_limit = min(page_size, remaining)

            filters = usaspending.AwardFilter(since=since, limit=page_limit, page=page)
            data = usaspending.fetch_awards_page(session, filters, recipient_search_text=recipient_search_text, keywords=keywords)

            raw_path = snapshot_dir / f"page_{page}.json"
            raw_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

            results = data.get("results", [])
            LOGGER.info("Fetched %d USAspending rows (page %d)", len(results), page)

            total_fetched += len(results)
            normalized = usaspending.normalize_awards(results)
            normalized_total += len(normalized)

            if normalized:
                inserted += _upsert_events(db, normalized)
                db.commit()

            if len(results) < page_limit:
                break

        run.status = "success"
        run.fetched = total_fetched
        run.normalized = normalized_total
        run.inserted = inserted
        run.ended_at = datetime.now(timezone.utc)
        db.commit()

    except KeyboardInterrupt:
        # Ctrl+C should not leave the run 'running'

        db.rollback()

        run.status = "aborted"

        run.error = "KeyboardInterrupt"

        run.ended_at = datetime.now(timezone.utc)

        db.commit()

        raise


    except Exception as e:
        db.rollback()
        run.status = "failed"
        run.error = str(e)
        run.ended_at = datetime.now(timezone.utc)
        db.commit()
        raise
    finally:
        db.close()

    return {
        "run_id": run.id,
        "fetched": total_fetched,
        "normalized": normalized_total,
        "inserted": inserted,
        "snapshot_dir": snapshot_dir,
        "page_size": page_size,
        "max_total": max_total,
    }



def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return True
        if cleaned.lower() in {"null", "none", "n/a"}:
            return True
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False
def _upsert_events(session: Session, events):
    if not events:
        return 0

    dialect = session.bind.dialect.name  # type: ignore[attr-defined]

    inserted = 0
    if dialect == "postgresql":
        stmt = pg_insert(Event).values(events)
        stmt = stmt.on_conflict_do_nothing(index_elements=["hash"])
        result = session.execute(stmt.returning(Event.id))
        inserted = len(result.fetchall())
    else:
        for event in events:
            exists = session.query(Event.id).filter_by(hash=event["hash"]).first()
            if exists:
                continue
            session.add(Event(**event))
            session.flush()
            inserted += 1

    # Backfill missing fields on existing rows (do not touch keywords/clauses).
    hashes = [e.get("hash") for e in events if e.get("hash")]
    if hashes:
        rows = session.query(Event).filter(Event.hash.in_(hashes)).all()
        by_hash = {r.hash: r for r in rows}

        backfilled = 0
        for ev in events:
            h = ev.get("hash")
            row = by_hash.get(h)
            if row is None:
                continue

            changed = False

            # raw_json: fill missing keys only (safe merge)
            new_raw = ev.get("raw_json")
            if isinstance(new_raw, dict) and new_raw:
                cur_raw = row.raw_json
                if cur_raw is None:
                    row.raw_json = new_raw
                    changed = True
                elif isinstance(cur_raw, dict):
                    merged = dict(cur_raw)
                    for k, v in new_raw.items():
                        if k not in merged or merged.get(k) in (None, "", [], {}):
                            merged[k] = v
                    if merged != cur_raw:
                        row.raw_json = merged
                        changed = True

            if not row.doc_id and ev.get("doc_id"):
                row.doc_id = ev.get("doc_id")
                changed = True
            if not row.source_url and ev.get("source_url"):
                row.source_url = ev.get("source_url")
                changed = True
            if not row.snippet and ev.get("snippet"):
                row.snippet = ev.get("snippet")
                changed = True
            if not row.place_text and ev.get("place_text"):
                row.place_text = ev.get("place_text")
                changed = True
            if row.occurred_at is None and ev.get("occurred_at") is not None:
                row.occurred_at = ev.get("occurred_at")
                changed = True

            for field_name in EVENT_PROMOTED_FIELDS:
                new_value = ev.get(field_name)
                if _is_missing_value(new_value):
                    continue
                current_value = getattr(row, field_name, None)
                if _is_missing_value(current_value):
                    setattr(row, field_name, new_value)
                    changed = True

            if changed:
                backfilled += 1

        if backfilled:
            LOGGER.info("Backfilled %d existing events with missing fields", backfilled)

    return inserted

def ingest_sam_opportunities(
    api_key: Optional[str] = None,
    days: Optional[int] = None,
    posted_from: Optional[date] = None,
    posted_to: Optional[date] = None,
    pages: int = 1,
    page_size: int = 100,
    max_records: Optional[int] = None,
    start_page: int = 1,
    keywords: Optional[List[str]] = None,
    database_url: Optional[str] = None,
) -> Dict[str, object]:
    """Ingest SAM.gov opportunities into the events table.

    Semantics:
      - days: lookback window for postedDate when explicit dates are not supplied
      - posted_from / posted_to: explicit postedDate bounds (YYYY-MM-DD, inclusive)
      - pages: maximum pages to request (starting at start_page)
      - page_size: max records per page (capped to <= 1000 by the upstream API)
      - max_records: optional total cap across all pages (defaults to pages * page_size)
      - keywords: optional title search terms. If multiple are provided, we run one query per keyword and union results.
    """
    ensure_runtime_directories()
    date_window = resolve_sam_posted_window(days=days, posted_from=posted_from, posted_to=posted_to)
    resolved_posted_from = date_window["posted_from"]
    resolved_posted_to = date_window["posted_to"]
    resolved_days = date_window["effective_days"]

    # Back-compat: allow callers to pass api_key positionally,
    # but default to env var for operator ergonomics.
    if not api_key:
        api_key = os.getenv("SAM_API_KEY")

    if not api_key:
        LOGGER.info("SAM_API_KEY not provided; skipping SAM.gov ingest")
        return {
            "status": "skipped",
            "reason": "missing_api_key",
            "date_window": serialize_sam_posted_window(date_window),
        }

    session = _build_retrying_session()

    page_size = max(1, min(int(page_size), samgov.MAX_LIMIT))
    pages = max(1, int(pages))
    start_page = max(1, int(start_page))

    max_total = int(max_records) if max_records is not None else pages * page_size

    total_fetched = 0
    normalized_total = 0
    inserted = 0

    pages_attempted = 0
    pages_with_data = 0
    empty_pages = 0
    requests_total = 0
    requests_with_retries = 0
    retry_attempts_total = 0
    rate_limit_retries = 0
    retry_sleep_seconds_total = 0.0

    snapshot_dir = RAW_SOURCES["sam"] / date.today().strftime("%Y%m%d")
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    SessionFactory = get_session_factory(database_url)
    db = SessionFactory()

    run = IngestRun(
        source="SAM.gov",
        status="running",
        days=int(resolved_days) if resolved_days is not None else None,
        start_page=start_page,
        pages=pages,
        page_size=page_size,
        max_records=max_total,
        snapshot_dir=str(snapshot_dir),
    )
    db.add(run)
    db.commit()  # ensure run id exists

    try:
        terms: List[Optional[str]] = []
        if keywords:
            for k in keywords:
                if k is None:
                    continue
                ks = str(k).strip()
                if ks:
                    terms.append(ks)

        if not terms:
            terms = [None]

        if max_records is None:
            max_total = pages * page_size * len(terms)
            run.max_records = max_total

        for term_idx, term in enumerate(terms, start=1):
            for page in range(start_page, start_page + pages):
                remaining = max_total - total_fetched
                if remaining <= 0:
                    break

                page_limit = min(page_size, remaining)

                # SAM uses offset-based paging
                offset = (page - 1) * page_size

                filters = samgov.OpportunityFilter(
                    posted_from=resolved_posted_from,
                    posted_to=resolved_posted_to,
                    limit=page_limit,
                    offset=offset,
                    title=term,
                )
                data = samgov.fetch_opportunities_page(session, filters, api_key=api_key)
                pages_attempted += 1
                requests_total += 1
                req_meta = data.get("_shadow_request_meta") if isinstance(data, dict) else None
                if isinstance(req_meta, dict):
                    retries = int(req_meta.get("retries") or 0)
                    retry_attempts_total += retries
                    if retries > 0:
                        requests_with_retries += 1
                    rate_limit_retries += int(req_meta.get("rate_limit_retries") or 0)
                    retry_sleep_seconds_total += float(req_meta.get("retry_sleep_seconds_total") or 0.0)

                fname = f"page_{page}.json" if not term else f"kw{term_idx}_page_{page}.json"
                raw_path = snapshot_dir / fname
                raw_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

                results = data.get("opportunitiesData") or []
                if not isinstance(results, list):
                    results = []
                if len(results) > 0:
                    pages_with_data += 1
                else:
                    empty_pages += 1

                LOGGER.info("Fetched %d SAM.gov rows (page %d, offset %d)", len(results), page, offset)

                total_fetched += len(results)
                normalized = samgov.normalize_opportunities(results)
                normalized_total += len(normalized)

                if normalized:
                    inserted += _upsert_events(db, normalized)
                    db.commit()

                # Short page => no more data for this query
                if len(results) < page_limit:
                    break

            if (max_total - total_fetched) <= 0:
                break

        run.status = "success"
        run.fetched = total_fetched
        run.normalized = normalized_total
        run.inserted = inserted
        run.ended_at = datetime.now(timezone.utc)
        db.commit()

    except KeyboardInterrupt:
        # Ctrl+C should not leave the run 'running'

        db.rollback()

        run.status = "aborted"

        run.error = "KeyboardInterrupt"

        run.ended_at = datetime.now(timezone.utc)

        db.commit()

        raise


    except Exception as e:
        db.rollback()
        run.status = "failed"
        run.error = str(e)
        run.ended_at = datetime.now(timezone.utc)
        db.commit()
        raise
    finally:
        db.close()

    return {
        "status": "success",
        "run_id": run.id,
        "fetched": total_fetched,
        "normalized": normalized_total,
        "inserted": inserted,
        "snapshot_dir": snapshot_dir,
        "page_size": page_size,
        "max_total": max_total,
        "date_window": serialize_sam_posted_window(date_window),
        "paging": {
            "terms_requested": len(terms),
            "pages_requested_per_term": pages,
            "pages_requested_total": pages * len(terms),
            "pages_attempted": pages_attempted,
            "pages_with_data": pages_with_data,
            "empty_pages": empty_pages,
        },
        "request_diagnostics": {
            "requests_total": requests_total,
            "requests_with_retries": requests_with_retries,
            "retry_attempts_total": retry_attempts_total,
            "rate_limit_retries": rate_limit_retries,
            "retry_sleep_seconds_total": round(float(retry_sleep_seconds_total), 3),
        },
}


__all__ = [
    "append_sam_posted_window_note",
    "describe_sam_posted_window",
    "format_sam_posted_window_cli_args",
    "ingest_usaspending",
    "ingest_sam_opportunities",
    "resolve_sam_posted_window",
    "serialize_sam_posted_window",
]
