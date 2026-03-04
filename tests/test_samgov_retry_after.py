from __future__ import annotations

import requests
import pytest

import backend.connectors.samgov as samgov


class FakeSession:
    def __init__(self, responses: list[requests.Response]) -> None:
        self._responses = list(responses)
        self.calls = 0

    def get(self, url: str, params=None, timeout: int = 60) -> requests.Response:
        self.calls += 1
        if not self._responses:
            raise RuntimeError("FakeSession ran out of responses")
        return self._responses.pop(0)


def _resp(status: int, *, headers: dict[str, str] | None = None) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r.url = "https://api.sam.gov/prod/opportunities/v2/search"
    r._content = b"{}"
    if headers:
        for k, v in headers.items():
            r.headers[k] = v
    return r


def test_get_with_retries_honors_retry_after_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(samgov.time, "sleep", lambda s: sleeps.append(float(s)))
    # Make jitter deterministic if it ever triggers
    monkeypatch.setattr(samgov.random, "random", lambda: 0.0)

    session = FakeSession([_resp(429, headers={"Retry-After": "2"}), _resp(200)])

    resp = samgov._get_with_retries(session, samgov.BASE_URL, params={}, max_retries=2, backoff_base=0.1)
    assert resp.status_code == 200
    assert session.calls == 2
    assert sleeps == [2.0]


def test_get_with_retries_falls_back_to_exponential_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(samgov.time, "sleep", lambda s: sleeps.append(float(s)))
    monkeypatch.setattr(samgov.random, "random", lambda: 0.0)

    session = FakeSession([_resp(429), _resp(200)])

    resp = samgov._get_with_retries(session, samgov.BASE_URL, params={}, max_retries=2, backoff_base=0.5)
    assert resp.status_code == 200
    assert session.calls == 2
    # attempt=0 => min(60, 0.5*(2**0)) + 0 = 0.5
    assert sleeps == [0.5]
