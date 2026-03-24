from __future__ import annotations

import pytest

from crawler.http import HttpClient
from crawler.rules import APIEndpoint, ThrottleRule


class _DummyResponse:
    def __init__(self, text: str = "not-json") -> None:
        self.status_code = 200
        self.text = text

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:  # pragma: no cover - response is invalid by design
        raise ValueError("invalid JSON")


class _DummySession:
    def __init__(self, response: _DummyResponse) -> None:
        self.response = response
        self.calls = 0

    def get(self, url: str, params: dict[str, object], headers: dict[str, str] | None, timeout: int) -> _DummyResponse:
        self.calls += 1
        return self.response

    def close(self) -> None:  # pragma: no cover - not used in this test
        return None


def test_fetch_json_retries_and_raises_on_decode_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    response = _DummyResponse(text="<html>oops</html>")
    session = _DummySession(response)
    monkeypatch.setattr("crawler.http.requests.Session", lambda: session)
    monkeypatch.setattr("crawler.http.HttpClient._sleep_once", lambda self: None)
    monkeypatch.setattr("crawler.http.time.sleep", lambda *args, **kwargs: None)
    throttle = ThrottleRule(min_seconds=0, max_seconds=0, max_retries=2, retry_backoff=1.0, timeout=1)
    endpoint = APIEndpoint(url="https://example.com/api", default_params={})
    client = HttpClient(throttle)

    with pytest.raises(RuntimeError) as excinfo:
        client.fetch_json(endpoint, {})

    assert "Failed to parse JSON" in str(excinfo.value)
    assert "oops" in str(excinfo.value)
    assert session.calls == throttle.max_retries
