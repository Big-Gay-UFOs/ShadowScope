from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from backend.analysis.ontology import load_ontology, validate_ontology, summarize_ontology
from backend.analysis.tagger import compile_for_tagging, tag_fields
from backend.db.models import Event, AnalysisRun, get_session_factory


def _canon_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _canon_keywords(v: Any) -> list[str]:
    items = _canon_list(v)
    out = [str(x) for x in items if x is not None]
    return sorted(set(out))


def _canon_clauses(v: Any) -> list[dict]:
    items = _canon_list(v)
    out = []
    for x in items:
        if isinstance(x, dict):
            out.append(x)
    out_sorted = sorted(
        out,
        key=lambda c: (
            c.get("pack", ""),
            c.get("rule", ""),
            c.get("field", ""),
            str(c.get("match", "")),
        ),
    )
    return out_sorted


def apply_ontology_to_events(
    ontology_path: Path,
    days: int = 30,
    source: str = "USAspending",
    batch: int = 500,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    obj = load_ontology(ontology_path)
    errs = validate_ontology(obj)
    if errs:
        raise ValueError("Ontology invalid: " + "; ".join(errs))

    summary = summarize_ontology(obj)
    meta, rules = compile_for_tagging(obj)

    since = datetime.now(timezone.utc) - timedelta(days=max(int(days), 1))

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    analysis_run = AnalysisRun(
        analysis_type="ontology_apply",
        status="running",
        source=source,
        days=days,
        ontology_version=str(summary.get("version") or ""),
        ontology_hash=str(summary.get("hash") or ""),
    )
    db.add(analysis_run)
    db.commit()

    scanned = 0
    updated = 0
    unchanged = 0
    last_id = 0

    try:
        while True:
            q = (
                db.query(Event)
                .filter(Event.id > last_id)
                .filter(Event.source == source)
                .filter(or_(Event.created_at >= since, Event.occurred_at == None, Event.occurred_at >= since))  # noqa: E711
                .order_by(Event.id.asc())
                .limit(int(batch))
            )

            rows = q.all()
            if not rows:
                break

            for ev in rows:
                scanned += 1
                last_id = int(ev.id)

                fields = {
                    "snippet": ev.snippet,
                    "place_text": ev.place_text,
                    "doc_id": ev.doc_id,
                    "source_url": ev.source_url,
                }

                res = tag_fields(meta, rules, fields)

                new_keywords = _canon_keywords(res["keywords"])
                new_clauses = _canon_clauses(res["clauses"])

                old_keywords = _canon_keywords(ev.keywords)
                old_clauses = _canon_clauses(ev.clauses)

                if new_keywords == old_keywords and new_clauses == old_clauses:
                    unchanged += 1
                    continue

                updated += 1
                if not dry_run:
                    ev.keywords = new_keywords
                    ev.clauses = new_clauses

            if not dry_run:
                db.commit()

                analysis_run.scanned = scanned
        analysis_run.updated = updated
        analysis_run.unchanged = unchanged
        analysis_run.status = "success"
        analysis_run.ended_at = datetime.now(timezone.utc)
        db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "days": days,
            "since": since.isoformat(),
            "scanned": scanned,
            "updated": updated,
            "unchanged": unchanged,
            "ontology": summary,
        }

        except Exception as e:
        db.rollback()
        analysis_run.status = "failed"
        analysis_run.error = str(e)
        analysis_run.ended_at = datetime.now(timezone.utc)
        db.commit()
        raise
    finally:
        db.close()