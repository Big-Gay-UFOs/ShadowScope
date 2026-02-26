import json

from backend.db.models import Correlation, ensure_schema, get_session_factory
from backend.services.export_correlations import export_kw_pairs


def test_export_kw_pairs_writes_files(tmp_path):
    db_path = tmp_path / "kwpairs.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)

    with SessionFactory() as db:
        db.add_all([
            Correlation(
                correlation_key="kw_pair|*|30|pair:aaaaaaaaaaaaaaaa",
                score="3",
                window_days=30,
                radius_km=0.0,
                lanes_hit={"lane": "kw_pair", "keyword_1": "a", "keyword_2": "b", "event_count": 3},
            ),
            Correlation(
                correlation_key="kw_pair|*|30|pair:bbbbbbbbbbbbbbbb",
                score="1",
                window_days=30,
                radius_km=0.0,
                lanes_hit={"lane": "kw_pair", "keyword_1": "c", "keyword_2": "d", "event_count": 1},
            ),
        ])
        db.commit()

    out_dir = tmp_path / "out"
    res = export_kw_pairs(database_url=db_url, output=out_dir, limit=50, min_event_count=2)

    assert res["csv"].exists()
    assert res["json"].exists()
    payload = json.loads(res["json"].read_text(encoding="utf-8"))
    assert payload["count"] == 1
