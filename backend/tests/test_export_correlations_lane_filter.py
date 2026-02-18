from datetime import datetime, timezone
from pathlib import Path
import json

from backend.db.models import Correlation, ensure_schema, get_session_factory
from backend.services.export_correlations import export_correlations


def test_export_correlations_lane_filter(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'exp_lane.db'}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    db = SessionFactory()
    now = datetime.now(timezone.utc)

    db.add_all(
        [
            Correlation(
                correlation_key="same_entity|USAspending|30|entity:1",
                score="2",
                window_days=30,
                radius_km=0.0,
                lanes_hit={"lane": "same_entity"},
                summary="e",
                rationale="r",
                created_at=now,
            ),
            Correlation(
                correlation_key="same_uei|USAspending|30|uei:UEI123",
                score="3",
                window_days=30,
                radius_km=0.0,
                lanes_hit={"lane": "same_uei"},
                summary="u",
                rationale="r",
                created_at=now,
            ),
        ]
    )
    db.commit()
    db.close()

    out = tmp_path / "out.json"
    res = export_correlations(out_path=str(out), source=None, lane="same_uei", database_url=db_url)
    assert res["count"] == 1
    data = json.load(open(out, "r", encoding="utf-8"))
    assert data["count"] == 1