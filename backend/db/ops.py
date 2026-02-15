"""Database lifecycle helpers for CLI and bootstrap workflows."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import psycopg
from psycopg import sql
from psycopg.errors import InvalidCatalogName

from alembic import command
from alembic.config import Config

from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url

from backend.db import models

LOGGER = logging.getLogger(__name__)

# One global lock id for "alembic upgrade" to avoid concurrent upgrades (backend startup + CLI).
_ADVISORY_LOCK_ID = 947261  # arbitrary constant; stable across runs


def _resolve_database_url(database_url: Optional[str] = None) -> str:
    return database_url or os.getenv("DATABASE_URL") or models.DEFAULT_DATABASE_URL


def _make_alembic_config(database_url: Optional[str] = None) -> Config:
    base_dir = Path(__file__).resolve().parent.parent.parent
    alembic_ini = base_dir / "alembic.ini"
    cfg = Config(str(alembic_ini))

    migrations_path = Path(__file__).resolve().parent / "migrations"
    cfg.set_main_option("script_location", str(migrations_path))

    url = _resolve_database_url(database_url)
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _postgres_driverless_dsn(database_url: str) -> str:
    url = make_url(database_url)
    driverless = url.set(drivername="postgresql")
    return driverless.render_as_string(hide_password=False)


def _acquire_migration_lock(database_url: str):
    """Acquire a Postgres advisory lock; released when connection closes."""
    dsn = _postgres_driverless_dsn(database_url)
    conn = psycopg.connect(dsn, autocommit=True)
    conn.execute("SELECT pg_advisory_lock(%s)", (_ADVISORY_LOCK_ID,))
    return conn


def ensure_database(database_url: Optional[str] = None) -> None:
    url = make_url(_resolve_database_url(database_url))
    backend = url.get_backend_name()

    if backend.startswith("postgresql"):
        driverless = url.set(drivername="postgresql")
        dsn = driverless.render_as_string(hide_password=False)

        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("SELECT 1")
                return

        except InvalidCatalogName:
            target_db = driverless.database
            if not target_db:
                raise RuntimeError("DATABASE_URL must include a database name for Postgres")

            admin_url = driverless.set(database="postgres")
            LOGGER.info("Database %s missing; creating via %s", target_db, admin_url)

            admin_dsn = admin_url.render_as_string(hide_password=False)
            with psycopg.connect(admin_dsn, autocommit=True) as conn:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
            return

        except psycopg.OperationalError as exc:  # type: ignore[attr-defined]
            raise RuntimeError(f"Unable to connect to Postgres server: {exc}") from exc

    # For SQLite or other backends, letting SQLAlchemy create the file is sufficient.
    models.get_engine(str(url))


def sync_database(database_url: Optional[str] = None) -> str:
    """Ensure the schema matches the latest Alembic migrations."""
    url = _resolve_database_url(database_url)
    ensure_database(url)

    engine = models.get_engine(url)
    inspector = inspect(engine)

    has_version = inspector.has_table("alembic_version")
    core_tables = ("entities", "events", "correlations", "correlation_links", "ingest_runs")
    has_core_tables = any(inspector.has_table(name) for name in core_tables)

    cfg = _make_alembic_config(url)

    backend = make_url(url).get_backend_name()
    lock_conn = None
    try:
        if backend.startswith("postgresql"):
            lock_conn = _acquire_migration_lock(url)
            LOGGER.info("Acquired Alembic advisory lock (%s)", _ADVISORY_LOCK_ID)

        if not has_version and has_core_tables:
            LOGGER.info("Alembic version table missing; stamping head")
            command.stamp(cfg, "head")
            return "stamped"

        LOGGER.info("Running Alembic upgrade to head")
        command.upgrade(cfg, "head")
        return "upgraded"

    finally:
        if lock_conn is not None:
            try:
                lock_conn.close()
                LOGGER.info("Released Alembic advisory lock (%s)", _ADVISORY_LOCK_ID)
            except Exception:
                pass


def stamp_head(database_url: Optional[str] = None) -> None:
    url = _resolve_database_url(database_url)
    cfg = _make_alembic_config(url)
    command.stamp(cfg, "head")


def reset_schema(database_url: Optional[str] = None) -> None:
    url = _resolve_database_url(database_url)
    engine = models.get_engine(url)

    backend = make_url(url).get_backend_name()
    if backend.startswith("postgresql"):
        LOGGER.warning("Dropping and recreating public schema for %s", url)
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            connection.execute(text("CREATE SCHEMA public"))
    else:
        LOGGER.warning("Dropping all tables for %s", url)
        models.Base.metadata.drop_all(engine)

    sync_database(url)


__all__ = ["ensure_database", "sync_database", "stamp_head", "reset_schema"]