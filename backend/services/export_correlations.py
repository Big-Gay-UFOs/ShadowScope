from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from backend.db.models import Correlation, CorrelationLink, Entity, Event, get_session_factory


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

        # python-side min_score filter (avoids cross-DB casting issues)
        if min_score is not None:
            ms = int(min_score)

            def _as_int(s: Any) -> Optional[int]:
                try:
                    return int(s)
                except Exception:
                    return None

            items = [it for it in items if (_as_int(it.get("score")) is not None and _as_int(it.get("score")) >= ms)]

        payload = {
            "exported_at": datetime.utcnow().isoformat() + "Z",
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