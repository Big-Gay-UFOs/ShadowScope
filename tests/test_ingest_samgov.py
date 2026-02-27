from __future__ import annotations

from pathlib import Path

from backend.connectors import samgov
from backend.db.models import Event, IngestRun, ensure_schema, get_session_factory
from backend.services import ingest as ingest_service


def test_ingest_samgov_is_idempotent_and_records_ingest_run(tmp_path: Path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'sam_ingest.db'}"
    ensure_schema(db_url)

    # Keep test artifacts out of repo data/ by redirecting snapshot root
    monkeypatch.setitem(ingest_service.RAW_SOURCES, "sam", tmp_path / "raw_sam")

    def fake_fetch(session, filters, api_key: str, timeout: int = 60):
        # Simulate two pages (offset 0 => 2 results, offset 2 => 1 result, then empty)
        if int(filters.offset) == 0:
            rows = [
                {"noticeId": "N1", "postedDate": "2018-05-04", "title": "t1"},
                {"noticeId": "N2", "postedDate": "2018-05-04", "title": "t2"},
            ]
        elif int(filters.offset) == 2:
            rows = [
                {"noticeId": "N3", "postedDate": "2018-05-04", "title": "t3"},
            ]
        else:
            rows = []
        return {
            "totalRecords": 3,
            "limit": int(filters.limit),
            "offset": int(filters.offset),
            "opportunitiesData": rows,
        }

    monkeypatch.setattr(samgov, "fetch_opportunities_page", fake_fetch)

    res1 = ingest_service.ingest_sam_opportunities(
        api_key="dummy",
        days=7,
        pages=5,
        page_size=2,
        start_page=1,
        database_url=db_url,
    )
    assert res1["status"] == "success"
    assert res1["inserted"] == 3
    assert res1["fetched"] == 3
    assert Path(res1["snapshot_dir"]).exists()

    # Second run should insert zero due to hash idempotency
    res2 = ingest_service.ingest_sam_opportunities(
        api_key="dummy",
        days=7,
        pages=5,
        page_size=2,
        start_page=1,
        database_url=db_url,
    )
    assert res2["status"] == "success"
    assert res2["inserted"] == 0

    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()
    try:
        assert db.query(Event).count() == 3
        runs = db.query(IngestRun).order_by(IngestRun.id.asc()).all()
        assert len(runs) == 2
        assert runs[0].source == "SAM.gov"
        assert runs[0].status == "success"
        assert runs[0].inserted == 3
        assert runs[1].status == "success"
        assert runs[1].inserted == 0
    finally:
        db.close()


def test_ingest_samgov_keywords_union_does_not_starve_later_terms(tmp_path: Path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'sam_kw.db'}"
    ensure_schema(db_url)

    monkeypatch.setitem(ingest_service.RAW_SOURCES, "sam", tmp_path / "raw_sam_kw")

    calls: list[str | None] = []

    def fake_fetch(session, filters, api_key: str, timeout: int = 60):
        calls.append(filters.title)

        if filters.title == "DOE":
            rows = [
                {"noticeId": "K1", "postedDate": "2018-05-04", "title": "doe-1"},
                {"noticeId": "K2", "postedDate": "2018-05-04", "title": "doe-2"},
            ]
        elif filters.title == "NNSA":
            rows = [
                {"noticeId": "K3", "postedDate": "2018-05-04", "title": "nnsa-1"},
            ]
        else:
            rows = []

        return {
            "totalRecords": len(rows),
            "limit": int(filters.limit),
            "offset": int(filters.offset),
            "opportunitiesData": rows,
        }

    monkeypatch.setattr(samgov, "fetch_opportunities_page", fake_fetch)

    res = ingest_service.ingest_sam_opportunities(
        api_key="dummy",
        days=7,
        pages=1,
        page_size=2,
        start_page=1,
        keywords=["DOE", "NNSA"],
        database_url=db_url,
    )

    assert res["status"] == "success"
    assert res["fetched"] == 3
    assert res["inserted"] == 3
    assert "DOE" in calls
    assert "NNSA" in calls
