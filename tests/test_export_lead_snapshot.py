import json

from backend.db.models import (
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    ensure_schema,
    get_session_factory,
)
from backend.services.export_leads import export_lead_snapshot


def test_export_lead_snapshot_writes_csv_and_json(tmp_path):
    db_path = tmp_path / "export_snap.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        e1 = Event(
            category="award",
            source="USAspending",
            hash="ev_export_1",
            snippet="s1",
            place_text="p1",
            doc_id="d1",
            source_url="http://x/1",
            raw_json={},
            keywords=["k1"],
            clauses=[],
        )
        e2 = Event(
            category="award",
            source="USAspending",
            hash="ev_export_2",
            snippet="s2",
            place_text="p2",
            doc_id="d2",
            source_url="http://x/2",
            raw_json={},
            keywords=["k2"],
            clauses=[],
        )
        db.add_all([e1, e2])
        db.commit()
        db.refresh(e1)
        db.refresh(e2)

        snap = LeadSnapshot(source="USAspending", min_score=1, scoring_version="v2")
        db.add(snap)
        db.commit()
        db.refresh(snap)

        db.add_all(
            [
                LeadSnapshotItem(
                    snapshot_id=int(snap.id),
                    event_id=int(e1.id),
                    event_hash=e1.hash,
                    rank=1,
                    score=10,
                    score_details={"scoring_version": "v2"},
                ),
                LeadSnapshotItem(
                    snapshot_id=int(snap.id),
                    event_id=int(e2.id),
                    event_hash=e2.hash,
                    rank=2,
                    score=9,
                    score_details={"scoring_version": "v2"},
                ),
            ]
        )
        db.commit()

    out_dir = tmp_path / "out"
    res = export_lead_snapshot(snapshot_id=int(snap.id), database_url=db_url, output=out_dir)

    assert res["csv"].exists()
    assert res["json"].exists()
    assert res["count"] == 2

    payload = json.loads(res["json"].read_text(encoding="utf-8"))
    assert payload["count"] == 2
    assert payload["scoring_version"] == "v2"
    assert payload["snapshot"]["scoring_version"] == "v2"

    lines = res["csv"].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1 + 2  # header + rows
    assert "snapshot_scoring_version" in lines[0]
