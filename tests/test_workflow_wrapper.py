from datetime import datetime, timezone
from pathlib import Path

from backend.db.models import Event, ensure_schema, get_session_factory
from backend.services.workflow import run_usaspending_workflow


def test_workflow_wrapper_runs_offline_parts(tmp_path: Path):
    db_path = tmp_path / "wf.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        db.add_all(
            [
                Event(
                    category="award",
                    source="USAspending",
                    hash="wf1",
                    created_at=now,
                    raw_json={"Recipient Name": "Acme Corp", "UEI": "UEI123"},
                    keywords=["alpha", "beta"],
                    clauses=[],
                ),
                Event(
                    category="award",
                    source="USAspending",
                    hash="wf2",
                    created_at=now,
                    raw_json={"Recipient Name": "Acme Corp", "UEI": "UEI123"},
                    keywords=["alpha", "beta"],
                    clauses=[],
                ),
                Event(
                    category="award",
                    source="USAspending",
                    hash="wf3",
                    created_at=now,
                    raw_json={"Recipient Name": "Other Corp", "UEI": "UEI999"},
                    keywords=["alpha", "gamma"],
                    clauses=[],
                ),
            ]
        )
        db.commit()

    out_dir = tmp_path / "out"
    res = run_usaspending_workflow(
        database_url=db_url,
        output=out_dir,
        skip_ingest=True,
        skip_ontology=True,
        # run the rest
        window_days=30,
        min_events_entity=2,
        min_events_keywords=2,  # make alpha+beta eligible
        max_events_keywords=200,
        max_keywords_per_event=10,
        export_events_flag=False,
    )

    assert res.get("entities_link") is not None
    assert res.get("correlations") is not None
    assert res.get("snapshot") is not None
    assert res.get("exports") is not None

    ex = res["exports"]
    assert Path(ex["lead_snapshot"]["csv"]).exists()
    assert Path(ex["kw_pairs"]["csv"]).exists()
    assert Path(ex["entities"]["entities_csv"]).exists()
    assert ex["kw_pairs"]["count"] >= 1
