from datetime import datetime, timezone

from backend.db.models import Correlation, Event, ensure_schema, get_session_factory
from backend.services.doctor import doctor_status


def test_doctor_status_basic(tmp_path):
    db_path = tmp_path / "doctor.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(category="award", source="USAspending", hash="h1", created_at=now, keywords=["pack:a", "pack:b"]),
                Event(category="award", source="USAspending", hash="h2", created_at=now, keywords=[]),
            ]
        )
        db.add(
            Correlation(
                correlation_key="kw_pair|USAspending|30|pair:aaaaaaaaaaaaaaaa",
                score="3",
                window_days=30,
                radius_km=0.0,
                lanes_hit={"lane": "kw_pair", "keyword_1": "a", "keyword_2": "b", "event_count": 3},
            )
        )
        db.commit()

    res = doctor_status(database_url=db_url, days=30, source="USAspending", scan_limit=100, max_keywords_per_event=10)

    assert res["db"]["status"] == "ok"
    assert res["counts"]["events_total"] == 2
    assert res["keywords"]["scanned_events"] == 2
    assert res["correlations"]["by_lane"]["kw_pair"] >= 1
