from __future__ import annotations

import importlib

import backend.connectors.samgov as samgov

DEFAULT = "https://api.sam.gov/prod/opportunities/v2/search"


def _reload():
    importlib.reload(samgov)
    return samgov


def test_samgov_base_url_defaults_to_prod(monkeypatch):
    monkeypatch.delenv("SAM_API_BASE_URL", raising=False)
    _reload()
    assert samgov.BASE_URL == DEFAULT


def test_samgov_base_url_env_override(monkeypatch):
    monkeypatch.setenv("SAM_API_BASE_URL", "https://example.invalid/prod/opportunities/v2/search")
    _reload()
    assert samgov.BASE_URL == "https://example.invalid/prod/opportunities/v2/search"

    # cleanup
    monkeypatch.delenv("SAM_API_BASE_URL", raising=False)
    _reload()


def test_samgov_base_url_blank_env_falls_back(monkeypatch):
    monkeypatch.setenv("SAM_API_BASE_URL", "")
    _reload()
    assert samgov.BASE_URL == DEFAULT

    monkeypatch.setenv("SAM_API_BASE_URL", "   ")
    _reload()
    assert samgov.BASE_URL == DEFAULT

    # cleanup
    monkeypatch.delenv("SAM_API_BASE_URL", raising=False)
    _reload()
