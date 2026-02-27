import json

from backend.db.models import Correlation, CorrelationLink, Event, LeadSnapshot, LeadSnapshotItem, ensure_schema, get_session_factory
from backend.services.export_leads import export_lead_snapshot


def test_export_lead_snapshot_includes_explainability(tmp_path):
    db_path = tmp_path / "explain.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        e1 = Event(category="award", source="USAspending", hash="ev_x_1", snippet="s1", place_text="p1", doc_id="d1", source_url="http://x/1", raw_json={}, keywords=["k1"], clauses=[])
        db.add(e1)
        db.commit()
        db.refresh(e1)

        c = Correlation(
            correlation_key="kw_pair|USAspending|30|pair:aaaaaaaaaaaaaaaa",
            score="3",
            window_days=30,
            radius_km=0.0,
            lanes_hit={"lane": "kw_pair", "keyword_1": "alpha", "keyword_2": "beta", "event_count": 3},
        )
        db.add(c)
        db.commit()
        db.refresh(c)

        db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(e1.id)))
        db.commit()

        snap = LeadSnapshot(source="USAspending", min_score=1, scoring_version="v2")
        db.add(snap)
        db.commit()
        db.refresh(snap)

        db.add(
            LeadSnapshotItem(
                snapshot_id=int(snap.id),
                event_id=int(e1.id),
                event_hash=e1.hash,
                rank=1,
                score=12,
                score_details={"scoring_version": "v2", "clause_score": 0, "keyword_score": 3, "entity_bonus": 0, "pair_bonus": 6, "pair_count": 1, "pair_strength": 0.5774},
            )
        )
        db.commit()

    out_dir = tmp_path / "out"
    res = export_lead_snapshot(snapshot_id=int(snap.id), database_url=db_url, output=out_dir)
    payload = json.loads(res["json"].read_text(encoding="utf-8"))
    assert payload["count"] == 1
    item = payload["items"][0]
    assert "why_summary" in item
    assert "top_kw_pairs_json" in item
    pairs = json.loads(item["top_kw_pairs_json"])
    assert pairs and pairs[0]["keyword_1"] == "alpha"
