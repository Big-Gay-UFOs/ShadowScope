
from __future__ import annotations

import importlib

import backend.connectors.samgov as samgov

DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 8
DEFAULT_BACKOFF_BASE = 0.75


def _reload():
    importlib.reload(samgov)
    return samgov


def test_samgov_retry_env_overrides(monkeypatch):
    monkeypatch.setenv("SAM_API_TIMEOUT_SECONDS", "90")
    monkeypatch.setenv("SAM_API_MAX_RETRIES", "11")
    monkeypatch.setenv("SAM_API_BACKOFF_BASE", "1.25")
    _reload()

    assert samgov.DEFAULT_TIMEOUT == 90
    assert samgov.DEFAULT_MAX_RETRIES == 11
    assert samgov.DEFAULT_BACKOFF_BASE == 1.25


def test_samgov_retry_env_invalid_values_fall_back(monkeypatch):
    monkeypatch.setenv("SAM_API_TIMEOUT_SECONDS", "0")
    monkeypatch.setenv("SAM_API_MAX_RETRIES", "nope")
    monkeypatch.setenv("SAM_API_BACKOFF_BASE", "-1")
    _reload()

    assert samgov.DEFAULT_TIMEOUT == DEFAULT_TIMEOUT
    assert samgov.DEFAULT_MAX_RETRIES == DEFAULT_MAX_RETRIES
    assert samgov.DEFAULT_BACKOFF_BASE == DEFAULT_BACKOFF_BASE
