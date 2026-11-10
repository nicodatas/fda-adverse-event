"""Bronze ingestion tests: pagination end conditions, retry policy, and
API-key security.

Runs without Spark: requests.get is faked via monkeypatch.
"""
import json

import pytest
import requests

URL = "https://api.fda.gov/drug/event.json"


class FakeResponse:
    """Minimal requests.Response stand-in usable as a context manager."""

    def __init__(self, status_code=200, payload=None, body=None):
        self.status_code = status_code
        self._body = body if body is not None else json.dumps(payload or {}).encode()
        self._url = ""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size=65536):
        view = memoryview(self._body)
        for i in range(0, len(view), chunk_size):
            yield view[i : i + chunk_size].tobytes()

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"{self.status_code} Error for url: {self._url}", response=self
            )


def page_of(count):
    return FakeResponse(200, {"results": [{"safetyreportid": "X"}] * count})


def install_fake_get(monkeypatch, responses, calls):
    """Patch requests.get to serve `responses` in order; record every call.

    Exception instances are raised instead of returned. The full URL is
    embedded in responses and exceptions the way real requests does — that is
    what makes the api_key leak through error messages if it is not redacted.
    """

    def fake_get(url, params=None, headers=None, timeout=None, stream=False):
        query = "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))
        full_url = f"{url}?{query}"
        calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        item = responses.pop(0) if responses else FakeResponse(200, {"results": []})
        if isinstance(item, Exception):
            item.url = full_url
            raise item
        if isinstance(item, FakeResponse):
            item._url = full_url
        return item

    monkeypatch.setattr(requests, "get", fake_get)


# -- Redaction helpers -------------------------------------------------------
def test_redact_secrets_replaces_every_occurrence(bronze):
    redact = bronze["redact_secrets"]
    assert (
        redact("https://x?api_key=K1&limit=1", ["K1"])
        == "https://x?api_key=***REDACTED***&limit=1"
    )
    assert redact("K1K1", ["K1"]) == "***REDACTED******REDACTED***"


def test_redact_secrets_ignores_empty_and_none(bronze):
    assert bronze["redact_secrets"]("unchanged", ["", None]) == "unchanged"


def test_active_secrets_reflect_key_config(bronze, bronze_with_key):
    assert bronze["active_secrets"]() == []
    assert bronze_with_key["active_secrets"]() == ["SECRET123"]


# -- Pagination end conditions -----------------------------------------------
def test_pagination_stops_on_short_page(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(monkeypatch, [page_of(500), page_of(120)], calls)
    total = bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)
    assert total == 620
    assert [c["params"]["skip"] for c in calls] == [0, 500]
    files = sorted(p.name for p in tmp_path.iterdir())
    assert files == ["page_000.json", "page_001.json"]


def test_pagination_stops_on_404_past_first_page(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(monkeypatch, [page_of(500), FakeResponse(404)], calls)
    total = bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)
    assert total == 500
    assert len(calls) == 2
    assert [p.name for p in tmp_path.iterdir()] == ["page_000.json"]


def test_404_on_first_page_raises_without_retry(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(monkeypatch, [FakeResponse(404)], calls)
    with pytest.raises(RuntimeError, match="404 on the first page"):
        bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)
    assert len(calls) == 1  # configuration error: never retried


def test_empty_results_page_ends_pagination(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(monkeypatch, [FakeResponse(200, {"results": []})], calls)
    total = bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)
    assert total == 0
    assert len(calls) == 1


# -- Retry policy ------------------------------------------------------------
def test_retries_5xx_then_succeeds(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(
        monkeypatch,
        [FakeResponse(500), FakeResponse(502), page_of(300)],
        calls,
    )
    total = bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)
    # two failed attempts then success on the third — all on the SAME page
    assert total == 300
    assert len(calls) == 3
    assert [c["params"]["skip"] for c in calls] == [0, 0, 0]


def test_exponential_backoff_between_retries(bronze, monkeypatch, tmp_path):
    sleeps = []
    monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))
    calls = []
    install_fake_get(monkeypatch, [FakeResponse(500)] * 3, calls)
    with pytest.raises(RuntimeError):
        bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)
    # RETRY_BACKOFF * 2**attempt, sleeping only BETWEEN attempts
    assert sleeps == [2, 4]


def test_non_retryable_401_fails_fast(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(monkeypatch, [FakeResponse(401)], calls)
    with pytest.raises(RuntimeError, match="Non-retryable API error \\(HTTP 401\\)"):
        bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)
    assert len(calls) == 1


# -- Response structure ------------------------------------------------------
def test_unexpected_results_structure_raises(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(monkeypatch, [FakeResponse(200, payload={"results": "oops"})], calls)
    with pytest.raises(RuntimeError, match="unexpected structure"):
        bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)


def test_malformed_json_raises_sanitized_runtime_error(bronze, monkeypatch, tmp_path):
    calls = []
    install_fake_get(monkeypatch, [FakeResponse(200, body=b"<html>not json</html>")], calls)
    with pytest.raises(RuntimeError, match="not valid JSON"):
        bronze["fetch_and_save_raw_data"](URL, {"limit": 500}, {}, tmp_path)


# -- API-key security --------------------------------------------------------
def _key_leaking_error():
    """A requests exception whose message embeds the URL incl. the api_key.

    Real requests exceptions do exactly this — that is the leak vector.
    """
    return requests.exceptions.ConnectionError(
        "HTTPSConnectionPool(host='api.fda.gov', port=443): Max retries exceeded "
        "with url: /drug/event.json?api_key=SECRET123&limit=500&skip=0 "
        "(Caused by NewConnectionError(...))"
    )


def test_api_key_never_leaks_in_logs_or_exception(
    bronze_with_key, monkeypatch, tmp_path, caplog
):
    calls = []
    install_fake_get(
        monkeypatch,
        [_key_leaking_error(), _key_leaking_error(), _key_leaking_error()],
        calls,
    )
    with caplog.at_level("WARNING"):
        with pytest.raises(RuntimeError) as excinfo:
            bronze_with_key["fetch_and_save_raw_data"](
                "https://api.fda.gov/drug/event.json",
                {"limit": 500, "api_key": "SECRET123"},
                {},
                tmp_path,
            )
    # the key must not appear in logs or in the raised message
    assert "SECRET123" not in caplog.text
    assert "SECRET123" not in str(excinfo.value)
    # the redaction marker replaces it
    assert "***REDACTED***" in caplog.text


def test_raised_error_hides_original_exception(bronze_with_key, monkeypatch, tmp_path):
    calls = []
    # three failures exhaust RETRY_ATTEMPTS, so fetch_page must raise
    install_fake_get(monkeypatch, [_key_leaking_error() for _ in range(3)], calls)
    with pytest.raises(RuntimeError) as excinfo:
        bronze_with_key["fetch_page"](
            "https://api.fda.gov/drug/event.json",
            {"limit": 500, "api_key": "SECRET123", "skip": 0},
            {},
            str(tmp_path / "page_000.json"),
            0,
        )
    # from None on every raise: the original requests exception (with the key
    # in its message) must not be chained into the displayed traceback
    assert excinfo.value.__suppress_context__ is True
