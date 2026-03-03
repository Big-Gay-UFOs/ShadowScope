from __future__ import annotations

from pathlib import Path
import inspect

import pytest

from backend.connectors import samgov
from backend.db.models import IngestRun, get_session_factory
from backend.services import ingest as ingest_service


def _ensure_schema(db_url: str) -> None:
    """
    Create tables for the sqlite test DB.
    Tries project helper(s) first; falls back to Base.metadata.create_all().
    """
    # Project helper (if exists)
    try:
        from backend.db.schema import ensure_schema as ensure_schema_fn  # type: ignore
        ensure_schema_fn(db_url)
        return
    except Exception:
        pass

    # Fallback: SQLAlchemy Base.metadata.create_all
    try:
        from sqlalchemy import create_engine
        from backend.db.models import Base  # type: ignore

        engine = create_engine(db_url)
        Base.metadata.create_all(engine)
        return
    except Exception as e:
        raise RuntimeError("Could not initialize test database schema") from e


def _call_ingest_sam_opportunities(db_url: str, tmp_path: Path) -> None:
    """
    Call ingest_sam_opportunities with a signature-flexible kwargs set,
    so the test doesn't break if param names change slightly.
    """
    fn = ingest_service.ingest_sam_opportunities
    sig = inspect.signature(fn)
    kwargs: dict[str, object] = {}

    # Always provide a DB URL, even if the function doesn't accept it as a param
    # (some code paths read DATABASE_URL from env/.env)
    # The caller sets env DATABASE_URL already.
    for name in ("database_url", "db_url"):
        if name in sig.parameters:
            kwargs[name] = db_url
            break

    # API key param name can vary; provide if present
    for name in ("api_key", "sam_api_key"):
        if name in sig.parameters:
            kwargs[name] = "dummy"
            break

    if "days" in sig.parameters:
        kwargs["days"] = 7
    if "pages" in sig.parameters:
        kwargs["pages"] = 1

    # page size/limit param name varies
    for name in ("page_size", "limit", "page_limit"):
        if name in sig.parameters:
            kwargs[name] = 1
            break

    for name in ("start_page", "start"):
        if name in sig.parameters:
            kwargs[name] = 1
            break

    # raw dir override if supported
    for name in ("raw_dir", "raw_root", "raw_base"):
        if name in sig.parameters:
            kwargs[name] = tmp_path / "raw_sam_abort"
            break

    fn(**kwargs)  # type: ignore[arg-type]


def test_ingest_samgov_keyboardinterrupt_marks_run_aborted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Use sqlite DB for isolation
    db_file = tmp_path / "sam_abort.db"
    db_url = f"sqlite:///{db_file.as_posix()}"

    # Make sure ingest reads our DB if it relies on env
    monkeypatch.setenv("DATABASE_URL", db_url)

    _ensure_schema(db_url)

    # Avoid writing raw snapshots into repo folders
    if hasattr(ingest_service, "RAW_SOURCES") and isinstance(getattr(ingest_service, "RAW_SOURCES"), dict):
        monkeypatch.setitem(ingest_service.RAW_SOURCES, "sam", tmp_path / "raw_sam_abort")

    def fake_fetch(*args, **kwargs):
        raise KeyboardInterrupt()

    # Patch the connector call.
    monkeypatch.setattr(samgov, "fetch_opportunities_page", fake_fetch)

    # If ingest.py imported the function directly, patch that too (harmless if absent).
    if hasattr(ingest_service, "fetch_opportunities_page"):
        monkeypatch.setattr(ingest_service, "fetch_opportunities_page", fake_fetch)

    with pytest.raises(KeyboardInterrupt):
        _call_ingest_sam_opportunities(db_url, tmp_path)

    SessionFactory = get_session_factory(db_url)
    db = SessionFactory()
    try:
        run = (
            db.query(IngestRun)
            .filter(IngestRun.source == "SAM.gov")
            .order_by(IngestRun.id.desc())
            .first()
        )
        assert run is not None
        assert run.status == "aborted"
        assert run.ended_at is not None
    finally:
        db.close()
