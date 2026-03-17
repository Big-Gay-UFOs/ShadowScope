import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
from backend.db.models import Event, Entity, ensure_schema, get_session_factory
from backend.app import app


def test_api_events_filters(tmp_path):
    db_path = tmp_path / "api_events.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    try:
        ent = Entity(name="E1")
        db.add(ent)
        db.commit()
        db.refresh(ent)

        now = datetime.now(timezone.utc)

        e_old = Event(
            category="award",
            source="USAspending",
            hash="ev_old",
            snippet="old",
            place_text="Old region",
            doc_id="d_old",
            source_url="http://example.com/old",
            raw_json={},
            keywords=["needle"],
            clauses=[],
            created_at=now - timedelta(days=10),
        )
        db.add(e_old)
        db.commit()

        fillers = []
        for i in range(250):
            fillers.append(Event(
                category="award",
                source="USAspending",
                hash=f"ev_fill_{i}",
                snippet="fill",
                place_text="",
                doc_id=f"d_fill_{i}",
                source_url=f"http://example.com/fill/{i}",
                raw_json={},
                keywords=["noise"],
                clauses=[],
            ))

        e1 = Event(
            category="award",
            source="USAspending",
            hash="ev1",
            snippet="e1",
            place_text="Northern Virginia, USA",
            doc_id="d1",
            document_id="DOC-X",
            award_id="AWARD-X",
            piid="PIID-X",
            source_record_id="SRC-1",
            awarding_agency_code="DOE",
            awarding_agency_name="Department of Energy",
            naics_code="541330",
            naics_description="Engineering Services",
            psc_code="R425",
            psc_description="Engineering and Technical Services",
            notice_award_type="award",
            place_of_performance_state="VA",
            place_of_performance_country="USA",
            source_url="http://example.com/1",
            raw_json={},
            keywords=["alpha", "beta"],
            clauses=[],
            created_at=now - timedelta(hours=2),
        )

        e2 = Event(
            category="award",
            source="USAspending",
            hash="ev2",
            snippet="e2",
            place_text="Maryland, USA",
            doc_id="d2",
            piid="PIID-X",
            source_url="http://example.com/2",
            raw_json={},
            keywords=["beta"],
            clauses=[],
            entity_id=ent.id,
            created_at=now - timedelta(hours=1),
        )

        e3 = Event(
            category="award",
            source="Other",
            hash="ev3",
            snippet="e3",
            place_text="Other region",
            doc_id="d3",
            source_url="http://example.com/3",
            raw_json={},
            keywords=["gamma"],
            clauses=[],
            created_at=now - timedelta(minutes=30),
        )

        db.add_all(fillers + [e1, e2, e3])
        db.commit()
    finally:
        db.close()

    os.environ["DATABASE_URL"] = db_url

    with TestClient(app) as c:
        r = c.get("/api/events?limit=1&keyword=needle")
        assert r.status_code == 200
        j = r.json()
        assert j and j[0]["hash"] == "ev_old"

        r = c.get("/api/events?limit=100&source=USAspending")
        assert r.status_code == 200
        assert all(e["source"] == "USAspending" for e in r.json())

        r = c.get("/api/events?limit=100&exclude_source=USAspending")
        assert r.status_code == 200
        assert all(e["source"] != "USAspending" for e in r.json())

        r = c.get("/api/events?limit=100&keyword=alpha")
        assert r.status_code == 200
        assert all("alpha" in (e["keywords"] or []) for e in r.json())

        r = c.get("/api/events?limit=100&has_entity=true")
        assert r.status_code == 200
        assert all(e["entity_id"] is not None for e in r.json())

        r = c.get("/api/events?limit=1000&days=1")
        assert r.status_code == 200
        hashes = {e["hash"] for e in r.json()}
        assert "ev_old" not in hashes

        r = c.get("/api/events?limit=100&award_id=AWARD-X")
        assert r.status_code == 200
        assert {e["hash"] for e in r.json()} == {"ev1"}

        r = c.get("/api/events?limit=100&contract_id=PIID-X")
        assert r.status_code == 200
        assert {e["hash"] for e in r.json()} >= {"ev1", "ev2"}

        r = c.get("/api/events?limit=100&document_id=DOC-X")
        assert r.status_code == 200
        assert {e["hash"] for e in r.json()} == {"ev1"}

        r = c.get("/api/events?limit=100&naics=541330")
        assert r.status_code == 200
        assert {e["hash"] for e in r.json()} == {"ev1"}

        r = c.get("/api/events?limit=100&agency=Department of Energy")
        assert r.status_code == 200
        assert {e["hash"] for e in r.json()} == {"ev1"}

        r = c.get("/api/events?limit=100&psc=Engineering and Technical Services")
        assert r.status_code == 200
        assert {e["hash"] for e in r.json()} == {"ev1"}

        r = c.get("/api/events?limit=100&place_region=VA,USA")
        assert r.status_code == 200
        assert {e["hash"] for e in r.json()} == {"ev1"}

        r = c.get("/api/events", params={"limit": 100, "date_from": (now - timedelta(days=2)).isoformat()})
        assert r.status_code == 200
        hashes = {e["hash"] for e in r.json()}
        assert "ev_old" not in hashes

        r = c.get("/api/events", params={"limit": 10, "date_to": (now - timedelta(days=5)).isoformat()})
        assert r.status_code == 200
        assert [e["hash"] for e in r.json()] == ["ev_old"]

        r = c.get("/api/events", params={"limit": 1, "sort_by": "created_at", "sort_dir": "asc"})
        assert r.status_code == 200
        assert r.json()[0]["hash"] == "ev_old"
