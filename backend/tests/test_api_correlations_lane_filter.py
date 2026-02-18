from datetime import datetime, timezone
from pathlib import Path
import importlib

from fastapi.testclient import TestClient

from backend.api.deps import get_db_session
from backend.db.models import Correlation, ensure_schema, get_session_factory


def test_api_correlations_lane_filter(tmp_path: Path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'lane.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    now = datetime.now(timezone.utc)

    db.add_all(
        [
            Correlation(
                correlation_key="same_entity|USAspending|30|entity:1",
                score="2",
                window_days=30,
                radius_km=0.0,
                lanes_hit={"lane": "same_entity"},
                summary="e",
                rationale="r",
                created_at=now,
            ),
            Correlation(
                correlation_key="same_uei|USAspending|30|uei:UEI123",
                score="3",
                window_days=30,
                radius_km=0.0,
                lanes_hit={"lane": "same_uei"},
                summary="u",
                rationale="r",
                created_at=now,
            ),
        ]
    )
    db.commit()
    db.close()

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
        r = client.get("/api/correlations/?source=&lane=same_entity&limit=50&offset=0")
        assert r.status_code == 200
        assert r.json()["total"] == 1

        r2 = client.get("/api/correlations/?source=&lane=same_uei&limit=50&offset=0")
        assert r2.status_code == 200
        assert r2.json()["total"] == 1
    finally:
        app.dependency_overrides.clear()