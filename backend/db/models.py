from __future__ import annotations
import os
from contextlib import contextmanager
from typing import Iterator, Optional

from dotenv import load_dotenv

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from sqlalchemy.exc import OperationalError

load_dotenv()

DEFAULT_DATABASE_URL = "sqlite:///./dev.db"
Base = declarative_base()


class Entity(Base):
    __tablename__ = "entities"
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    uei = Column(String)
    cage = Column(String)
    parent = Column(String)
    type = Column(String)
    sponsor = Column(String)
    sites_json = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    events = relationship("Event", back_populates="entity")


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (UniqueConstraint("hash", name="uq_events_hash"),)
    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey("entities.id"))
    category = Column(String, nullable=False)
    occurred_at = Column(DateTime(timezone=True))
    lat = Column(Float)
    lon = Column(Float)
    source = Column(String, nullable=False)
    source_url = Column(Text)
    doc_id = Column(String)
    keywords = Column(JSON)
    clauses = Column(JSON)
    place_text = Column(Text)
    snippet = Column(Text)
    raw_json = Column(JSON)
    hash = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    entity = relationship("Entity", back_populates="events")


class Correlation(Base):
    __tablename__ = "correlations"
    id = Column(Integer, primary_key=True)
    score = Column(String, nullable=False)
    window_days = Column(Integer, nullable=False)
    radius_km = Column(Float, nullable=False)
    lanes_hit = Column(JSON, nullable=False)
    summary = Column(Text)
    rationale = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class CorrelationLink(Base):
    __tablename__ = "correlation_links"
    id = Column(Integer, primary_key=True)
    correlation_id = Column(Integer, ForeignKey("correlations.id", ondelete="CASCADE"))
    event_id = Column(Integer, ForeignKey("events.id", ondelete="CASCADE"))


_engines: dict[str, Engine] = {}
_session_factories: dict[str, sessionmaker] = {}


def _resolve_url(database_url: Optional[str] = None) -> str:
    return database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def get_engine(database_url: Optional[str] = None):
    url = _resolve_url(database_url)
    engine = _engines.get(url)
    if engine is None:
        engine = create_engine(url, future=True)
        _engines[url] = engine
    return engine


def configure_session(database_url: Optional[str] = None) -> sessionmaker:
    url = _resolve_url(database_url)
    factory = _session_factories.get(url)
    if factory is None:
        engine = get_engine(url)
        factory = sessionmaker(bind=engine, expire_on_commit=False, class_=Session)
        _session_factories[url] = factory
    return factory


def get_session_factory(database_url: Optional[str] = None) -> sessionmaker:
    url = _resolve_url(database_url)
    factory = _session_factories.get(url)
    if factory is None:
        factory = configure_session(url)
    return factory


@contextmanager
def session_scope(database_url: Optional[str]=None) -> Iterator[Session]:
    session_cls = get_session_factory(database_url)
    session = session_cls()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ensure_schema(database_url: Optional[str]=None) -> None:
    engine = get_engine(database_url)
    try:
        Base.metadata.create_all(engine)
    except OperationalError as exc:
        raise RuntimeError("Unable to initialize database schema") from exc

class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id = Column(Integer, primary_key=True)
    source = Column(String(32), nullable=False)
    status = Column(String(16), nullable=False, default="running")
    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    ended_at = Column(DateTime(timezone=True), nullable=True)

    days = Column(Integer)
    start_page = Column(Integer)
    pages = Column(Integer)
    page_size = Column(Integer)
    max_records = Column(Integer)

    fetched = Column(Integer, nullable=False, default=0)
    normalized = Column(Integer, nullable=False, default=0)
    inserted = Column(Integer, nullable=False, default=0)

    snapshot_dir = Column(Text)
    error = Column(Text)

class AnalysisRun(Base):
    __tablename__ = "analysis_runs"

    id = Column(Integer, primary_key=True)
    analysis_type = Column(String(32), nullable=False)
    status = Column(String(16), nullable=False, default="running")
    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    ended_at = Column(DateTime(timezone=True), nullable=True)

    source = Column(String(32))
    days = Column(Integer)

    ontology_version = Column(String(32))
    ontology_hash = Column(String(64))
    dry_run = Column(Boolean, nullable=False, default=False)
    scanned = Column(Integer, nullable=False, default=0)
    updated = Column(Integer, nullable=False, default=0)
    unchanged = Column(Integer, nullable=False, default=0)

    error = Column(Text)

class LeadSnapshot(Base):
    __tablename__ = "lead_snapshots"

    id = Column(Integer, primary_key=True)
    analysis_run_id = Column(Integer, ForeignKey("analysis_runs.id"), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    source = Column(String(32))
    min_score = Column(Integer, nullable=False, default=1)
    limit = Column(Integer, nullable=False, default=200)
    scoring_version = Column(String(32), nullable=False, default="v1")
    notes = Column(Text)

    items = relationship("LeadSnapshotItem", back_populates="snapshot", cascade="all, delete-orphan")


class LeadSnapshotItem(Base):
    __tablename__ = "lead_snapshot_items"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("lead_snapshots.id"), nullable=False)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)

    event_hash = Column(String(64), nullable=False)

    rank = Column(Integer, nullable=False)
    score = Column(Integer, nullable=False)
    score_details = Column(JSON)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    snapshot = relationship("LeadSnapshot", back_populates="items")
