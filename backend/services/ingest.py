"""Ingestion workflows for live data sources."""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from backend.connectors import usaspending
from backend.db.models import Event, get_session_factory
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
    limit: int,
    pages: int,
    database_url: Optional[str] = None,
) -> Dict[str, object]:
    ensure_runtime_directories()
    session = _build_retrying_session()
    since = date.today() - timedelta(days=max(days, 1))
    total_fetched = 0
    normalized_total = 0
    inserted = 0

    snapshot_dir = RAW_SOURCES["usaspending"] / date.today().strftime("%Y%m%d")
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    SessionFactory = get_session_factory(database_url)
    db = SessionFactory()
    try:
        for page in range(start_page, start_page + pages):
            if total_fetched >= limit:
                break
            page_limit = min(usaspending.MAX_LIMIT, limit - total_fetched)
            filters = usaspending.AwardFilter(since=since, limit=page_limit, page=page)
            data = usaspending.fetch_awards_page(session, filters)
            raw_path = snapshot_dir / f"page_{page}.json"
            raw_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            results = data.get("results", [])
            LOGGER.info("Fetched %d USAspending rows (page %d)", len(results), page)
            total_fetched += len(results)
            normalized = usaspending.normalize_awards(results)
            normalized_total += len(normalized)
            if not normalized:
                continue
            inserted += _upsert_events(db, normalized)
            db.commit()
            if len(results) < page_limit:
                break
        db.commit()
    finally:
        db.close()
    return {
        "fetched": total_fetched,
        "normalized": normalized_total,
        "inserted": inserted,
        "snapshot_dir": snapshot_dir,
    }


def _upsert_events(session: Session, events):
    if not events:
        return 0
    dialect = session.bind.dialect.name  # type: ignore[attr-defined]
    if dialect == "postgresql":
        stmt = pg_insert(Event).values(events)
        stmt = stmt.on_conflict_do_nothing(index_elements=["hash"])
        result = session.execute(stmt.returning(Event.id))
        return len(result.fetchall())
    inserted = 0
    for event in events:
        exists = session.query(Event.id).filter_by(hash=event["hash"]).first()
        if exists:
            continue
        session.add(Event(**event))
        session.flush()
        inserted += 1
    return inserted


def ingest_sam_opportunities(api_key: Optional[str]) -> Dict[str, object]:
    ensure_runtime_directories()
    if not api_key:
        LOGGER.info("SAM_API_KEY not provided; skipping SAM.gov ingest")
        return {"status": "skipped", "reason": "missing_api_key"}
    LOGGER.info("SAM.gov ingest placeholder executed (API integration pending)")
    return {"status": "placeholder"}


__all__ = ["ingest_usaspending", "ingest_sam_opportunities"]
