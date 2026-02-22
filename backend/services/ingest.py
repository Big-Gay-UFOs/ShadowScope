"""Ingestion workflows for live data sources."""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta, datetime, timezone
from typing import Dict, Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from backend.connectors import usaspending
from backend.db.models import Event, IngestRun, get_session_factory
from backend.runtime import RAW_SOURCES, ensure_runtime_directories

LOGGER = logging.getLogger("shadowscope.ingest")


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

            if changed:
                backfilled += 1

        if backfilled:
            LOGGER.info("Backfilled %d existing events with missing fields", backfilled)

    return inserted

def ingest_sam_opportunities(api_key: Optional[str]) -> Dict[str, object]:
    ensure_runtime_directories()
    if not api_key:
        LOGGER.info("SAM_API_KEY not provided; skipping SAM.gov ingest")
        return {"status": "skipped", "reason": "missing_api_key"}
    LOGGER.info("SAM.gov ingest placeholder executed (API integration pending)")
    return {"status": "placeholder"}


__all__ = ["ingest_usaspending", "ingest_sam_opportunities"]