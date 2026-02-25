import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
from backend.db.models import Event, ensure_schema, get_session_factory
from backend.app import app


def test_api_leads_defaults_to_v2_and_supports_v1(tmp_path):
    db_path = tmp_path / "api_leads.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    try:
        ev = Event(
            category="award",
            source="USAspending",
            hash="api_test_hash_1",
            snippet="x",
            place_text="",
            doc_id="d1",
            source_url="http://example.com",
            raw_json={},
            keywords=["k1"],
            clauses=[],
        )
        db.add(ev)
        db.commit()
    finally:
        db.close()

    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as c:
        r = c.get("/api/leads?limit=10")
        assert r.status_code == 200
        j = r.json()
        assert j and j[0]["score_details"]["scoring_version"] == "v2"

        r2 = c.get("/api/leads?limit=10&scoring_version=v1")
        assert r2.status_code == 200
        j2 = r2.json()
        assert j2 and j2[0]["score_details"]["scoring_version"] == "v1"
