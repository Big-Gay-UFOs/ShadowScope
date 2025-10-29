"""Runtime utilities for ensuring directories and shared paths."""
from __future__ import annotations

from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PARSED_DIR = DATA_DIR / "parsed"
EXPORTS_DIR = DATA_DIR / "exports"
LOGS_DIR = PROJECT_ROOT / "logs"

RAW_SOURCES = {
    "usaspending": RAW_DIR / "usaspending",
    "sam": RAW_DIR / "sam",
}

PARSED_SOURCES = {
    "sam": PARSED_DIR / "sam",
}

DIRECTORIES = [
    DATA_DIR,
    RAW_DIR,
    PARSED_DIR,
    EXPORTS_DIR,
    LOGS_DIR,
    *RAW_SOURCES.values(),
    *PARSED_SOURCES.values(),
]


def ensure_runtime_directories() -> Dict[str, Path]:
    """Ensure the standard directory structure exists and return useful paths."""
    for path in DIRECTORIES:
        path.mkdir(parents=True, exist_ok=True)
    return {
        "data": DATA_DIR,
        "raw": RAW_DIR,
        "parsed": PARSED_DIR,
        "exports": EXPORTS_DIR,
        "logs": LOGS_DIR,
        **{f"raw_{name}": path for name, path in RAW_SOURCES.items()},
        **{f"parsed_{name}": path for name, path in PARSED_SOURCES.items()},
    }


__all__ = [
    "PROJECT_ROOT",
    "DATA_DIR",
    "RAW_DIR",
    "PARSED_DIR",
    "EXPORTS_DIR",
    "LOGS_DIR",
    "RAW_SOURCES",
    "PARSED_SOURCES",
    "ensure_runtime_directories",
]
