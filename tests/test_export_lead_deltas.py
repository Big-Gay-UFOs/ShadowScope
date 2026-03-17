import json

from backend.db.models import (
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    ensure_schema,
    get_session_factory,
)
from backend.services.export_leads import export_lead_deltas


def test_export_lead_deltas_writes_csv_and_json(tmp_path):
    db_path = tmp_path / "export_deltas.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        e1 = Event(category="award", source="USAspending", hash="ev_d_1", snippet="s1", place_text="p1", doc_id="d1", source_url="http://x/1", raw_json={}, keywords=["k1"], clauses=[{"pack": "focus", "rule": "one", "weight": 3}])
        e2 = Event(category="award", source="USAspending", hash="ev_d_2", snippet="s2", place_text="p2", doc_id="d2", source_url="http://x/2", raw_json={}, keywords=["k2"], clauses=[])
        e3 = Event(category="award", source="USAspending", hash="ev_d_3", snippet="s3", place_text="p3", doc_id="d3", source_url="http://x/3", raw_json={}, keywords=["k3"], clauses=[])
        db.add_all([e1, e2, e3])
        db.commit()
        db.refresh(e1); db.refresh(e2); db.refresh(e3)

        s1 = LeadSnapshot(source="USAspending", min_score=1, scoring_version="v2")
        s2 = LeadSnapshot(source="USAspending", min_score=1, scoring_version="v2")
        db.add_all([s1, s2])
        db.commit()
        db.refresh(s1); db.refresh(s2)

        db.add_all([
            LeadSnapshotItem(snapshot_id=int(s1.id), event_id=int(e1.id), event_hash=e1.hash, rank=1, score=10, score_details={"scoring_version": "v2", "pair_bonus": 2}),
            LeadSnapshotItem(snapshot_id=int(s1.id), event_id=int(e2.id), event_hash=e2.hash, rank=2, score=5,  score_details={"scoring_version": "v2"}),
            LeadSnapshotItem(snapshot_id=int(s2.id), event_id=int(e1.id), event_hash=e1.hash, rank=1, score=11, score_details={"scoring_version": "v2", "pair_bonus": 3}),
            LeadSnapshotItem(snapshot_id=int(s2.id), event_id=int(e3.id), event_hash=e3.hash, rank=2, score=6,  score_details={"scoring_version": "v2"}),
        ])
        db.commit()

    out_dir = tmp_path / "out"
    result = export_lead_deltas(from_snapshot_id=int(s1.id), to_snapshot_id=int(s2.id), database_url=db_url, output=out_dir)

    assert result["csv"].exists()
    assert result["json"].exists()
    assert result["count"] == 3

    payload = json.loads(result["json"].read_text(encoding="utf-8"))
    assert payload["counts"]["new"] == 1
    assert payload["counts"]["removed"] == 1
    assert payload["counts"]["changed"] == 1
    assert payload["from_snapshot"]["scoring_version"] == "v2"
    assert payload["to_snapshot"]["scoring_version"] == "v2"

    header = result["csv"].read_text(encoding="utf-8").splitlines()[0]
    assert "to_contributing_correlations_json" in header
    assert "from_matched_ontology_rules_json" in header
