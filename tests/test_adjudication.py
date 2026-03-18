import csv
import json
from pathlib import Path

from backend.db.models import Event, LeadSnapshot, LeadSnapshotItem, ensure_schema, get_session_factory
from backend.services.adjudication import evaluate_lead_adjudications, export_lead_adjudication_template


def _rewrite_adjudications(path: Path, updates: dict[int, dict[str, str]]) -> None:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    for row in rows:
        rank = int(row["rank"])
        row.update(updates.get(rank, {}))

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_adjudication_template_round_trip_and_metrics(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'adjudication.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        events = [
            Event(category="notice", source="SAM.gov", hash="adj-1", doc_id="ADJ-1", raw_json={}, keywords=[], clauses=[]),
            Event(category="notice", source="SAM.gov", hash="adj-2", doc_id="ADJ-2", raw_json={}, keywords=[], clauses=[]),
            Event(category="notice", source="SAM.gov", hash="adj-3", doc_id="ADJ-3", raw_json={}, keywords=[], clauses=[]),
            Event(category="notice", source="SAM.gov", hash="adj-4", doc_id="ADJ-4", raw_json={}, keywords=[], clauses=[]),
            Event(category="notice", source="SAM.gov", hash="adj-5", doc_id="ADJ-5", raw_json={}, keywords=[], clauses=[]),
        ]
        db.add_all(events)
        db.commit()
        for event in events:
            db.refresh(event)

        snap_v2 = LeadSnapshot(source="SAM.gov", min_score=1, scoring_version="v2")
        snap_v3 = LeadSnapshot(source="SAM.gov", min_score=1, scoring_version="v3")
        db.add_all([snap_v2, snap_v3])
        db.commit()
        db.refresh(snap_v2)
        db.refresh(snap_v3)

        db.add_all(
            [
                LeadSnapshotItem(snapshot_id=int(snap_v2.id), event_id=int(events[0].id), event_hash=events[0].hash, rank=1, score=12, score_details={"scoring_version": "v2"}),
                LeadSnapshotItem(snapshot_id=int(snap_v2.id), event_id=int(events[1].id), event_hash=events[1].hash, rank=2, score=10, score_details={"scoring_version": "v2"}),
                LeadSnapshotItem(snapshot_id=int(snap_v2.id), event_id=int(events[2].id), event_hash=events[2].hash, rank=3, score=8, score_details={"scoring_version": "v2"}),
                LeadSnapshotItem(snapshot_id=int(snap_v3.id), event_id=int(events[3].id), event_hash=events[3].hash, rank=1, score=14, score_details={"scoring_version": "v3"}),
                LeadSnapshotItem(snapshot_id=int(snap_v3.id), event_id=int(events[4].id), event_hash=events[4].hash, rank=2, score=11, score_details={"scoring_version": "v3"}),
            ]
        )
        db.commit()

    review_one = export_lead_adjudication_template(
        snapshot_id=int(snap_v2.id),
        database_url=db_url,
        output=tmp_path / "review_v2.csv",
    )
    review_two = export_lead_adjudication_template(
        snapshot_id=int(snap_v3.id),
        database_url=db_url,
        output=tmp_path / "review_v3.csv",
    )

    _rewrite_adjudications(
        Path(review_one["csv"]),
        {
            1: {"lead_family": "alpha_family", "decision": "keep", "foia_ready": "yes"},
            2: {"lead_family": "beta_family", "decision": "reject", "reason_code": "low_signal", "foia_ready": "no"},
            3: {"lead_family": "alpha_family", "decision": "unclear"},
        },
    )
    _rewrite_adjudications(
        Path(review_two["csv"]),
        {
            1: {"lead_family": "alpha_family", "decision": "reject", "reason_code": "duplicate_context", "foia_ready": "no"},
            2: {"lead_family": "beta_family", "lead_family_override": "gamma_override", "decision": "keep", "foia_ready": "yes"},
        },
    )

    result = evaluate_lead_adjudications(
        adjudications=[Path(review_one["csv"]), Path(review_two["csv"])],
        precision_at_k=[1, 2, 3],
        output=tmp_path / "metrics_out",
    )

    summary = result["summary"]
    assert summary["row_count"] == 5
    assert summary["reviewed_count"] == 5
    assert summary["decisive_count"] == 4
    assert summary["keep_count"] == 2
    assert summary["reject_count"] == 2
    assert summary["unclear_count"] == 1
    assert summary["acceptance_rate"] == 0.5
    assert summary["precision_at_k"]["1"]["precision"] == 0.5
    assert summary["precision_at_k"]["2"]["precision"] == 0.5
    assert summary["precision_at_k"]["3"]["precision"] == 0.5

    by_reason = {entry["reason_code"]: entry for entry in result["rejection_reasons"]}
    assert by_reason["duplicate_context"]["count"] == 1
    assert by_reason["low_signal"]["count"] == 1

    by_family = {entry["lead_family"]: entry for entry in result["by_lead_family"]}
    assert by_family["alpha_family"]["row_count"] == 3
    assert by_family["alpha_family"]["keep_count"] == 1
    assert by_family["alpha_family"]["reject_count"] == 1
    assert by_family["gamma_override"]["keep_count"] == 1
    assert by_family["gamma_override"]["acceptance_rate"] == 1.0

    by_version = {entry["scoring_version"]: entry for entry in result["by_scoring_version"]}
    assert by_version["v2"]["precision_at_k"]["1"]["precision"] == 1.0
    assert by_version["v3"]["precision_at_k"]["1"]["precision"] == 0.0

    artifacts = result["artifacts"]
    metrics_json = Path(artifacts["metrics_json"])
    normalized_csv = Path(artifacts["normalized_adjudications_csv"])
    assert metrics_json.exists()
    assert normalized_csv.exists()

    payload = json.loads(metrics_json.read_text(encoding="utf-8"))
    assert payload["summary"]["keep_count"] == 2
    normalized_rows = list(csv.DictReader(normalized_csv.open("r", encoding="utf-8-sig", newline="")))
    assert len(normalized_rows) == 5
    assert normalized_rows[0]["scoring_version"] == "v2"
    assert normalized_rows[-1]["lead_family_override"] == "gamma_override"
