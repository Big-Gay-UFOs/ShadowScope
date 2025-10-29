"""Central logging configuration for ShadowScope."""
from __future__ import annotations

import logging
from logging.config import dictConfig
from pathlib import Path
from typing import Dict

from backend.runtime import ensure_runtime_directories, LOGS_DIR


def configure_logging(level: int = logging.INFO) -> Dict[str, Path]:
    """Configure application-wide logging with rotating file handlers."""
    paths = ensure_runtime_directories()
    log_dir = LOGS_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                    "level": level,
                },
                "app_file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "standard",
                    "level": level,
                    "filename": str(log_dir / "app.log"),
                    "maxBytes": 5 * 1024 * 1024,
                    "backupCount": 3,
                    "encoding": "utf-8",
                },
                "ingest_file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "standard",
                    "level": level,
                    "filename": str(log_dir / "ingest.log"),
                    "maxBytes": 5 * 1024 * 1024,
                    "backupCount": 3,
                    "encoding": "utf-8",
                },
            },
            "root": {
                "handlers": ["console", "app_file"],
                "level": level,
            },
            "loggers": {
                "backend.connectors": {
                    "handlers": ["ingest_file", "console"],
                    "level": level,
                    "propagate": False,
                },
                "shadowscope.ingest": {
                    "handlers": ["ingest_file", "console"],
                    "level": level,
                    "propagate": False,
                },
            },
        }
    )
    return paths


__all__ = ["configure_logging"]
