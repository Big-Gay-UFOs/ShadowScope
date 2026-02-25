import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
from backend.db.models import Event, ensure_schema, get_session_factory
from backend.app import app


def test_api_events_keyword_like_escapes_wildcards(tmp_path):
    db_path = tmp_path / "api_events_like_escape.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    try:
        # "_" wildcard case: without escaping, r_1 could match ra1
        db.add_all([
            Event(category="award", source="USAspending", hash="kw_under_exact", snippet="x", place_text="", doc_id="d_u1", source_url="http://x/u1", raw_json={}, keywords=["r_1"], clauses=[]),
            Event(category="award", source="USAspending", hash="kw_under_other", snippet="x", place_text="", doc_id="d_u2", source_url="http://x/u2", raw_json={}, keywords=["ra1"], clauses=[]),

            # "%" wildcard case: without escaping, r%1 could match rXX1
            Event(category="award", source="USAspending", hash="kw_pct_exact", snippet="x", place_text="", doc_id="d_p1", source_url="http://x/p1", raw_json={}, keywords=["r%1"], clauses=[]),
            Event(category="award", source="USAspending", hash="kw_pct_other", snippet="x", place_text="", doc_id="d_p2", source_url="http://x/p2", raw_json={}, keywords=["rXX1"], clauses=[]),
        ])
        db.commit()
    finally:
        db.close()

    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as c:
        r = c.get("/api/events", params={"limit": 50, "keyword": "r_1"})
        assert r.status_code == 200
        hashes = {e["hash"] for e in r.json()}
        assert "kw_under_exact" in hashes
        assert "kw_under_other" not in hashes

        r = c.get("/api/events", params={"limit": 50, "keyword": "r%1"})
        assert r.status_code == 200
        hashes = {e["hash"] for e in r.json()}
        assert "kw_pct_exact" in hashes
        assert "kw_pct_other" not in hashes
