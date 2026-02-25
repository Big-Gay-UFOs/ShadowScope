import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
from backend.db.models import (
    Event, Correlation, CorrelationLink,
    ensure_schema, get_session_factory,
)
from backend.app import app


def test_api_correlations_min_event_count_respects_source(tmp_path):
    db_path = tmp_path / "api_corr.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    try:
        ev1 = Event(category="award", source="USAspending", hash="ev1", snippet="x", place_text="", doc_id="d1", source_url="http://x/1", raw_json={}, keywords=[], clauses=[])
        ev2 = Event(category="award", source="USAspending", hash="ev2", snippet="x", place_text="", doc_id="d2", source_url="http://x/2", raw_json={}, keywords=[], clauses=[])
        ev3 = Event(category="award", source="Other",      hash="ev3", snippet="x", place_text="", doc_id="d3", source_url="http://x/3", raw_json={}, keywords=[], clauses=[])
        db.add_all([ev1, ev2, ev3])
        db.commit()
        db.refresh(ev1); db.refresh(ev2); db.refresh(ev3)

        c1 = Correlation(correlation_key="kw_pair|a|b", score="3", window_days=30, radius_km=0.0, lanes_hit={"kw_pair": {"event_count": 3}})
        c2 = Correlation(correlation_key="kw_pair|c|d", score="1", window_days=30, radius_km=0.0, lanes_hit={"kw_pair": {"event_count": 1}})
        db.add_all([c1, c2])
        db.commit()
        db.refresh(c1); db.refresh(c2)

        # c1 links: 2 USAspending + 1 Other (total 3)
        db.add_all([
            CorrelationLink(correlation_id=c1.id, event_id=ev1.id),
            CorrelationLink(correlation_id=c1.id, event_id=ev2.id),
            CorrelationLink(correlation_id=c1.id, event_id=ev3.id),
            # c2 links: 1 USAspending
            CorrelationLink(correlation_id=c2.id, event_id=ev1.id),
        ])
        db.commit()
    finally:
        db.close()

    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as c:
        # Within USAspending, c1 has only 2 links -> should NOT match mec=3
        r = c.get("/api/correlations/?source=USAspending&min_event_count=3&limit=50")
        assert r.status_code == 200
        assert r.json()["total"] == 0

        # Within USAspending, mec=2 returns c1
        r = c.get("/api/correlations/?source=USAspending&min_event_count=2&limit=50")
        assert r.status_code == 200
        keys = [it["correlation_key"] for it in r.json()["items"]]
        assert "kw_pair|a|b" in keys
        assert "kw_pair|c|d" not in keys

        # With source blank, mec counts across all sources -> c1 matches mec=3
        r = c.get("/api/correlations/?source=&min_event_count=3&limit=50")
        assert r.status_code == 200
        keys = [it["correlation_key"] for it in r.json()["items"]]
        assert "kw_pair|a|b" in keys
