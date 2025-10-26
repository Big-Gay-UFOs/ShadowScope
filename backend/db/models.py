from __future__ import annotations
import os
from contextlib import contextmanager
from typing import Iterator, Optional

from sqlalchemy import JSON, Column, DateTime, Float, ForeignKey, Integer, String, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from sqlalchemy.exc import OperationalError

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
    created_at = Column(DateTime(timezone=True))
    events = relationship("Event", back_populates="entity")


class Event(Base):
    __tablename__ = "events"
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
    clauses  = Column(JSON)
    place_text = Column(Text)
    snippet = Column(Text)
    raw_json = Column(JSON)
    hash = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True))
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
    created_at = Column(DateTime(timezone=True))


class CorrelationLink(Base):
    __tablename__ = "correlation_links"
    id = Column(Integer, primary_key=True)
    correlation_id = Column(Integer, ForeignKey("correlations.id", ondelete="CASCADE"))
    event_id = Column(Integer, ForeignKey("events.id", ondelete="CASCADE"))


_engine: Optional[object] = None
_SessionFactory: Optional[sessionmaker] = None


def get_engine(database_url: Optional[str]=None):
    url = database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    global _engine
    if _engine is None:
        _engine = create_engine(url, future=True)
    return _engine


def configure_session(database_url: Optional[str]=None) -> sessionmaker:
    global _SessionFactory
    engine = get_engine(database_url)
    _SessionFactory = sessionmaker(bind=engine, expire_on_commit=False, class_=Session)
    return _SessionFactory


def get_session_factory() -> sessionmaker:
    return _SessionFactory or configure_session()


@contextmanager
def session_scope(database_url: Optional[str]=None) -> Iterator[Session]:
    session_cls = configure_session(database_url)
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
