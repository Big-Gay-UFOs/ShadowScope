import json
from datetime import datetime, timezone

from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.export_leads import export_scoring_comparison


def test_export_scoring_comparison_emits_side_by_side_v2_v3_artifact(tmp_path):
    db_path = tmp_path / "comparison.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="cmp-rich",
                    created_at=now,
                    doc_id="CMP-001",
                    source_url="https://sam.gov/opp/cmp-1",
                    snippet="Structurally rich SAM notice",
                    raw_json={
                        "noticeType": "Sources Sought",
                        "solicitationNumber": "CMP-001",
                        "naicsCode": "541330",
                        "typeOfSetAside": "SBA",
                        "responseDeadLine": "2026-03-20",
                        "fullParentPathCode": "DOE.HQ",
                        "Recipient Name": "Acme Federal",
                    },
                    keywords=["sam_procurement_starter:notice_type_sources_sought"],
                    clauses=[],
                ),
                Event(
                    category="opportunity",
                    source="SAM.gov",
                    hash="cmp-thin",
                    created_at=now,
                    doc_id="CMP-002",
                    source_url="https://sam.gov/opp/cmp-2",
                    snippet="Thin SAM notice",
                    raw_json={},
                    keywords=["sam_procurement_starter:notice_type_sources_sought"],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    res = export_scoring_comparison(
        versions=["v2", "v3"],
        source="SAM.gov",
        min_score=0,
        limit=10,
        scan_limit=50,
        database_url=db_url,
        output=tmp_path / "out",
    )

    assert res["csv"].exists()
    assert res["json"].exists()
    assert res["versions"] == ["v2", "v3"]
    assert res["count"] == 2

    payload = json.loads(res["json"].read_text(encoding="utf-8"))
    assert payload["versions"] == ["v2", "v3"]
    assert payload["baseline_version"] == "v2"
    assert payload["target_version"] == "v3"
    assert payload["count"] == 2
    assert any(item.get("comparison_state") == "shared" for item in payload["items"])
    assert any("structural" in str(item.get("explanation_delta")) for item in payload["items"])

    first = payload["items"][0]
    assert "v2_rank" in first
    assert "v2_score" in first
    assert "v3_rank" in first
    assert "v3_score" in first
    assert "delta_rank" in first
    assert "delta_score" in first
    assert "lead_family" in first

    header = res["csv"].read_text(encoding="utf-8").splitlines()[0]
    assert "v2_rank" in header
    assert "v3_rank" in header
    assert "lead_family" in header
