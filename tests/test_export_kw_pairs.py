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
                score="0.625000",
                window_days=30,
                radius_km=0.0,
                lanes_hit={
                    "lane": "kw_pair",
                    "keyword_1": "a",
                    "keyword_2": "b",
                    "event_count": 3,
                    "c12": 3,
                    "keyword_1_df": 3,
                    "keyword_2_df": 3,
                    "total_events": 8,
                    "score_signal": 0.625,
                    "score_kind": "npmi",
                    "score_secondary": 1.875,
                    "score_secondary_kind": "log_odds",
                },
            ),
            Correlation(
                correlation_key="kw_pair|*|30|pair:cccccccccccccccc",
                score="0.750000",
                window_days=30,
                radius_km=0.0,
                lanes_hit={
                    "kw_pair": {
                        "keyword_1": "e",
                        "keyword_2": "f",
                        "event_count": 4,
                        "c12": 4,
                        "keyword_1_df": 4,
                        "keyword_2_df": 4,
                        "total_events": 10,
                        "score_signal": 0.75,
                        "score_kind": "npmi",
                        "score_secondary": 2.125,
                        "score_secondary_kind": "log_odds",
                    }
                },
            ),
            Correlation(
                correlation_key="kw_pair|*|30|pair:bbbbbbbbbbbbbbbb",
                score="0.100000",
                window_days=30,
                radius_km=0.0,
                lanes_hit={
                    "lane": "kw_pair",
                    "keyword_1": "c",
                    "keyword_2": "d",
                    "event_count": 1,
                    "c12": 1,
                    "keyword_1_df": 1,
                    "keyword_2_df": 1,
                    "total_events": 8,
                    "score_signal": 0.1,
                    "score_kind": "npmi",
                    "score_secondary": 0.5,
                    "score_secondary_kind": "log_odds",
                },
            ),
        ])
        db.commit()

    out_dir = tmp_path / "out"
    res = export_kw_pairs(database_url=db_url, output=out_dir, limit=50, min_event_count=2)

    assert res["csv"].exists()
    assert res["json"].exists()
    payload = json.loads(res["json"].read_text(encoding="utf-8"))
    assert payload["count"] == 2
    items = {item["correlation_key"]: item for item in payload["items"]}
    assert items["kw_pair|*|30|pair:aaaaaaaaaaaaaaaa"]["score_signal"] == 0.625
    assert items["kw_pair|*|30|pair:aaaaaaaaaaaaaaaa"]["event_count"] == 3
    assert items["kw_pair|*|30|pair:aaaaaaaaaaaaaaaa"]["c12"] == 3
    assert items["kw_pair|*|30|pair:aaaaaaaaaaaaaaaa"]["score_secondary"] == 1.875
    assert items["kw_pair|*|30|pair:cccccccccccccccc"]["score_signal"] == 0.75
    assert items["kw_pair|*|30|pair:cccccccccccccccc"]["event_count"] == 4
    assert items["kw_pair|*|30|pair:cccccccccccccccc"]["score_secondary"] == 2.125
