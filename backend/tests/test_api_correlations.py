from datetime import datetime, timedelta, timezone
from pathlib import Path
import importlib

from fastapi.testclient import TestClient

from backend.api.deps import get_db_session
from backend.db.models import Correlation, CorrelationLink, Entity, Event, ensure_schema, get_session_factory


def test_api_correlations_list_and_detail(tmp_path: Path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'api_corr.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    now = datetime.now(timezone.utc)

    ent = Entity(name="ACME INC", uei="UEI123")
    db.add(ent)
    db.flush()

    ev1 = Event(category="procurement", source="USAspending", hash="h1", entity_id=ent.id, created_at=now - timedelta(days=1))
    ev2 = Event(category="procurement", source="USAspending", hash="h2", entity_id=ent.id, created_at=now - timedelta(days=2))
    db.add_all([ev1, ev2])
    db.flush()

    corr = Correlation(score="2", window_days=30, radius_km=0.0, lanes_hit={"lane": "same_entity"}, summary="test", rationale="r", created_at=now)
    db.add(corr)
    db.flush()

    db.add_all(
        [
            CorrelationLink(correlation_id=corr.id, event_id=ev1.id),
            CorrelationLink(correlation_id=corr.id, event_id=ev2.id),
        ]
    )
    db.commit()
    db.close()

    # Ensure the app bootstraps against this sqlite DB in lifespan
    monkeypatch.setenv("DATABASE_URL", db_url)

    import backend.app as app_mod
    importlib.reload(app_mod)
    app = app_mod.app

    def override_get_db_session():
        s = SessionFactory()
        try:
            yield s
        finally:
            s.close()

    app.dependency_overrides[get_db_session] = override_get_db_session
    try:
        client = TestClient(app)

        r = client.get("/api/correlations/?source=USAspending&limit=50&offset=0")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        cid = body["items"][0]["id"]

        r2 = client.get(f"/api/correlations/{cid}")
        assert r2.status_code == 200
        d = r2.json()
        assert d["id"] == cid
        assert d["event_count"] == 2
        assert d["events"][0]["entity"]["uei"] == "UEI123"
    finally:
        app.dependency_overrides.clear()