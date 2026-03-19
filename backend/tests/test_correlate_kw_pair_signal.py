from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.correlate import correlate
from backend.db.models import Correlation, Event, ensure_schema, get_session_factory



def _seed_event(db, *, source: str, hash_value: str, keywords: list[str], created_at: datetime) -> None:
    db.add(
        Event(
            category="procurement",
            source=source,
            hash=hash_value,
            keywords=keywords,
            clauses=[],
            raw_json={},
            created_at=created_at,
        )
    )



def _kw_pair_rows(db) -> list[Correlation]:
    return list(db.query(Correlation).filter(Correlation.correlation_key.like("kw_pair|%|%|pair:%")).order_by(Correlation.id.asc()).all())



def test_rebuild_keyword_pair_correlations_stores_signal_and_metadata(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'kw_pair_signal.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_event(db, source="USAspending", hash_value="sig_1", keywords=["pack:alpha", "pack:beta"], created_at=now - timedelta(days=1))
        _seed_event(db, source="USAspending", hash_value="sig_2", keywords=["pack:alpha", "pack:beta"], created_at=now - timedelta(days=2))
        _seed_event(db, source="USAspending", hash_value="sig_3", keywords=["pack:alpha", "pack:beta"], created_at=now - timedelta(days=3))
        _seed_event(db, source="USAspending", hash_value="sig_4", keywords=["pack:solo"], created_at=now - timedelta(days=4))
        db.commit()

    res = correlate.rebuild_keyword_pair_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        min_pair_count=2,
        max_events=200,
        max_keywords_per_event=10,
        max_keyword_df_ratio=1.0,
        max_keyword_df_floor=0,
        dry_run=False,
        database_url=db_url,
    )

    assert res["eligible_pairs"] == 1

    with SessionFactory() as db:
        rows = _kw_pair_rows(db)
        assert len(rows) == 1
        corr = rows[0]
        lanes = corr.lanes_hit or {}

    assert abs(float(corr.score) - float(lanes["score_signal"])) < 1e-6
    assert lanes["event_count"] == 3
    assert lanes["c12"] == 3
    assert lanes["keyword_1_df"] == 3
    assert lanes["keyword_2_df"] == 3
    assert lanes["score_kind"] == "npmi"
    assert lanes["score_secondary_kind"] == "log_odds"
    assert lanes["score_secondary"] > 0
    assert "npmi=" in (corr.summary or "")



def test_rebuild_keyword_pair_correlations_filters_extremely_common_terms(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'kw_pair_common.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        for idx in range(3):
            _seed_event(db, source="USAspending", hash_value=f"common_ab_{idx}", keywords=["pack:alpha", "pack:beta"], created_at=now - timedelta(days=idx + 1))
        for idx in range(3):
            _seed_event(db, source="USAspending", hash_value=f"common_cg_{idx}", keywords=["pack:common", "pack:gamma"], created_at=now - timedelta(days=idx + 4))
        for idx in range(3):
            _seed_event(db, source="USAspending", hash_value=f"common_cd_{idx}", keywords=["pack:common", "pack:delta"], created_at=now - timedelta(days=idx + 7))
        _seed_event(db, source="USAspending", hash_value="common_tail", keywords=["pack:loner"], created_at=now - timedelta(days=10))
        db.commit()

    res = correlate.rebuild_keyword_pair_correlations(
        window_days=30,
        source="USAspending",
        min_events=3,
        min_pair_count=3,
        max_events=200,
        max_keywords_per_event=10,
        max_keyword_df_ratio=0.4,
        max_keyword_df_floor=0,
        dry_run=False,
        database_url=db_url,
    )

    assert res["keywords_excluded_df"] >= 1
    assert res["eligible_pairs"] == 1

    with SessionFactory() as db:
        rows = _kw_pair_rows(db)
        assert len(rows) == 1
        lanes = rows[0].lanes_hit or {}

    assert {lanes["keyword_1"], lanes["keyword_2"]} == {"pack:alpha", "pack:beta"}
    assert "pack:common" not in {lanes["keyword_1"], lanes["keyword_2"]}



def test_rebuild_keyword_pair_correlations_controls_rare_pair_jackpot(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'kw_pair_rare.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        _seed_event(db, source="USAspending", hash_value="rare_once", keywords=["pack:x", "pack:y"], created_at=now - timedelta(days=1))
        _seed_event(db, source="USAspending", hash_value="rare_keep_1", keywords=["pack:alpha", "pack:beta"], created_at=now - timedelta(days=2))
        _seed_event(db, source="USAspending", hash_value="rare_keep_2", keywords=["pack:alpha", "pack:beta"], created_at=now - timedelta(days=3))
        _seed_event(db, source="USAspending", hash_value="rare_tail", keywords=["pack:solo"], created_at=now - timedelta(days=4))
        db.commit()

    res = correlate.rebuild_keyword_pair_correlations(
        window_days=30,
        source="USAspending",
        min_events=2,
        min_pair_count=2,
        max_events=200,
        max_keywords_per_event=10,
        max_keyword_df_ratio=1.0,
        max_keyword_df_floor=0,
        dry_run=False,
        database_url=db_url,
    )

    assert res["pairs_below_min_pair_count"] >= 1
    assert res["eligible_pairs"] == 1

    with SessionFactory() as db:
        rows = _kw_pair_rows(db)
        assert len(rows) == 1
        lanes = rows[0].lanes_hit or {}

    assert {lanes["keyword_1"], lanes["keyword_2"]} == {"pack:alpha", "pack:beta"}



def test_rebuild_keyword_pair_correlations_suppresses_noise_terms(tmp_path: Path):
    db_url = f"sqlite:///{(tmp_path / 'kw_pair_noise.db').as_posix()}"
    ensure_schema(db_url)
    SessionFactory = get_session_factory(db_url)
    now = datetime.now(timezone.utc)

    with SessionFactory() as db:
        for idx in range(3):
            _seed_event(
                db,
                source="SAM.gov",
                hash_value=f"noise_drop_{idx}",
                keywords=["pack:alpha", "operational_noise_terms:nsn_line_item_commodity_noise"],
                created_at=now - timedelta(days=idx + 1),
            )
        for idx in range(3):
            _seed_event(
                db,
                source="SAM.gov",
                hash_value=f"noise_keep_{idx}",
                keywords=["pack:alpha", "pack:beta"],
                created_at=now - timedelta(days=idx + 4),
            )
        db.commit()

    res = correlate.rebuild_keyword_pair_correlations(
        window_days=30,
        source="SAM.gov",
        min_events=3,
        min_pair_count=3,
        max_events=200,
        max_keywords_per_event=10,
        max_keyword_df_ratio=1.0,
        max_keyword_df_floor=0,
        dry_run=False,
        database_url=db_url,
    )

    assert res["keywords_excluded_noise"] >= 3
    assert res["eligible_pairs"] == 1

    with SessionFactory() as db:
        rows = _kw_pair_rows(db)
        assert len(rows) == 1
        lanes = rows[0].lanes_hit or {}

    assert {lanes["keyword_1"], lanes["keyword_2"]} == {"pack:alpha", "pack:beta"}
