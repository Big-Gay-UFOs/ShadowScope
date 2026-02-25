import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
from backend.db.models import Event, ensure_schema, get_session_factory
from backend.app import app


def test_api_leads_rejects_unbounded_params(tmp_path):
    db_path = tmp_path / "api_leads_bounds.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    try:
        db.add(Event(
            category="award",
            source="USAspending",
            hash="api_bounds_hash_1",
            snippet="x",
            place_text="",
            doc_id="d1",
            source_url="http://example.com",
            raw_json={},
            keywords=["k1"],
            clauses=[],
        ))
        db.commit()
    finally:
        db.close()

    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as c:
        assert c.get("/api/leads?scan_limit=-1").status_code == 400
        assert c.get("/api/leads?scan_limit=999999").status_code == 400
        assert c.get("/api/leads?limit=999999").status_code == 400
        assert c.get("/api/leads?scoring_version=bad").status_code == 400
