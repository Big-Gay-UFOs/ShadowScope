from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink, Entity, Event, get_session_factory
from pathlib import Path


def export_correlations(
    *,
    out_path: str,
    source: Optional[str] = "USAspending",
    lane: Optional[str] = None,
    window_days: Optional[int] = None,
    min_score: Optional[int] = None,
    limit: int = 500,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    try:
        q = db.query(Correlation)

        if lane:
            q = q.filter(Correlation.correlation_key.like(f"{lane}|%"))

        if window_days is not None:
            q = q.filter(Correlation.window_days == int(window_days))

        if source:
            corr_ids = (
                db.query(CorrelationLink.correlation_id)
                .join(Event, Event.id == CorrelationLink.event_id)
                .filter(Event.source == source)
                .distinct()
            )
            q = q.filter(Correlation.id.in_(corr_ids))

        q = q.order_by(Correlation.id.desc()).limit(int(limit))
        rows = q.all()

        items: List[Dict[str, Any]] = []
        for c in rows:
            links = (
                db.query(Event, Entity)
                .join(CorrelationLink, CorrelationLink.event_id == Event.id)
                .outerjoin(Entity, Entity.id == Event.entity_id)
                .filter(CorrelationLink.correlation_id == c.id)
                .order_by(Event.id.asc())
                .all()
            )

            events = []
            for ev, ent in links:
                events.append(
                    {
                        "id": ev.id,
                        "hash": ev.hash,
                        "source": ev.source,
                        "doc_id": ev.doc_id,
                        "source_url": ev.source_url,
                        "occurred_at": ev.occurred_at.isoformat() if ev.occurred_at else None,
                        "created_at": ev.created_at.isoformat() if ev.created_at else None,
                        "snippet": ev.snippet,
                        "place_text": ev.place_text,
                        "entity": None
                        if ent is None
                        else {"id": ent.id, "name": ent.name, "uei": ent.uei},
                    }
                )

            items.append(
                {
                    "id": c.id,
                    "correlation_key": getattr(c, "correlation_key", None),
                    "score": c.score,
                    "window_days": c.window_days,
                    "radius_km": c.radius_km,
                    "lanes_hit": c.lanes_hit,
                    "summary": c.summary,
                    "rationale": c.rationale,
                    "created_at": c.created_at.isoformat() if c.created_at else None,
                    "event_count": len(events),
                    "events": events,
                }
            )

        if min_score is not None:
            ms = int(min_score)

            def _as_int(s: Any) -> Optional[int]:
                try:
                    return int(s)
                except Exception:
                    return None

            items = [it for it in items if (_as_int(it.get("score")) is not None and _as_int(it.get("score")) >= ms)]

        payload = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "lane": lane,
            "window_days": window_days,
            "min_score": min_score,
            "limit": limit,
            "count": len(items),
            "items": items,
        }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        return {"status": "ok", "out_path": out_path, "count": len(items)}
    finally:
        db.close()
def export_kw_pairs(
    *,
    database_url: str | None = None,
    output: Path | None = None,
    limit: int = 200,
    min_event_count: int = 2,
) -> dict:
    """
    Export kw_pair correlations to CSV + JSON.

    event_count is taken from lanes_hit["kw_pair"]["event_count"] when present,
    otherwise falls back to int(score) when score is numeric.
    """
    from datetime import datetime, timezone
    import csv
    import json
    from sqlalchemy import select

    from backend.db.models import Correlation, get_session_factory
    from backend.runtime import EXPORTS_DIR, ensure_runtime_directories

    ensure_runtime_directories()
    SessionFactory = get_session_factory(database_url)

    with SessionFactory() as db:
        rows = db.execute(select(Correlation).order_by(Correlation.id.desc())).scalars().all()

    items: list[dict] = []
    for c in rows:
        lh = c.lanes_hit or {}
        kw = None
        if isinstance(lh, dict):
            # Current schema: lanes_hit is a flat dict with lane == 'kw_pair'
            if lh.get('lane') == 'kw_pair':
                kw = lh
            else:
                # Back-compat: some legacy shapes may nest by lane name
                maybe = lh.get('kw_pair')
                if isinstance(maybe, dict):
                    kw = maybe
        if not isinstance(kw, dict):
            continue

        k1 = kw.get("keyword_1") or kw.get("k1")
        k2 = kw.get("keyword_2") or kw.get("k2")
        if not k1 or not k2:
            ck = c.correlation_key or ""
            parts = ck.split("|")
            if len(parts) >= 3 and parts[0] == "kw_pair":
                k1, k2 = parts[1], parts[2]

        ec = kw.get("event_count")
        if ec is None:
            try:
                ec = int(c.score) if c.score is not None else 0
            except Exception:
                ec = 0

        try:
            ec_i = int(ec)
        except Exception:
            ec_i = 0

        if ec_i < int(min_event_count):
            continue

        items.append(
            {
                "correlation_id": int(c.id),
                "correlation_key": c.correlation_key,
                "keyword_1": k1,
                "keyword_2": k2,
                "event_count": ec_i,
                "window_days": c.window_days,
                "score": c.score,
            }
        )

    items.sort(key=lambda x: (x["event_count"], x["correlation_id"]), reverse=True)
    items = items[: int(limit)]

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = "kw_pairs_" + ts
    export_dir = EXPORTS_DIR

    if output:
        output = output.expanduser()
        if output.suffix:
            export_dir = output.parent if output.parent else Path(".")
            export_dir.mkdir(parents=True, exist_ok=True)
            base = output.stem or base
        else:
            export_dir = output
            export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / (base + ".csv")
    json_path = export_dir / (base + ".json")

    if items:
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(items[0].keys()))
            w.writeheader()
            w.writerows(items)
    else:
        csv_path.write_text("", encoding="utf-8")

    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "limit": int(limit),
        "min_event_count": int(min_event_count),
        "count": len(items),
        "items": items,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"csv": csv_path, "json": json_path, "count": len(items)}

