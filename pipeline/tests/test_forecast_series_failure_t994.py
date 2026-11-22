"""(T994 #17) Only a 404 is "not in season": every other failure of a forecast series fetch is counted and said.

`forecast.series` caught every error into None (the comment said "404 = not in season"), so a timeout, a 5xx or a DNS
failure looked like a station with no forecast, and an RFC outage like a province with none. Mutation: put the
`except Exception: return None` back -> the raise and the FAILED-line tests fail.
"""
from __future__ import annotations

import io
import urllib.error
from datetime import datetime, timezone

import pytest

from pipeline.gauges.feed import forecast as F

NOW = datetime(2026, 10, 22, 22, 0, tzinfo=timezone.utc)


def _urlopen_raising(exc):
    def opener(req, timeout):
        raise exc
    return opener


def _http(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://rfc.invalid/x.csv", code, "x", {}, io.BytesIO(b""))


def test_a_404_is_not_in_season(monkeypatch):
    monkeypatch.setattr(F.urllib.request, "urlopen", _urlopen_raising(_http(404)))
    assert F.series("08MH090", "CLEVER") is None


@pytest.mark.parametrize("exc", [_http(503), urllib.error.URLError("dns"), TimeoutError("read timed out")])
def test_any_other_failure_raises(monkeypatch, exc):
    monkeypatch.setattr(F.urllib.request, "urlopen", _urlopen_raising(exc))
    with pytest.raises(type(exc)):
        F.series("08MH090", "CLEVER")


def test_with_series_counts_and_says_the_failures(monkeypatch, capsys):
    def series(st, m, timeout=25.0):
        if st == "08B":
            raise urllib.error.URLError("the Centre is down")
        return None                                            # 08A: a 404, not in season
    monkeypatch.setattr(F, "series", series)
    summary = {"08A": {"CLEVER": {"issuedAt": "2026-10-22T09:43:00"}},
               "08B": {"CLEVER": {"issuedAt": "2026-10-22T09:43:00"}}}
    F.with_series(summary, stations={"08A", "08B"}, now=NOW)
    out = capsys.readouterr()
    assert "2 requests, 0 fetched" in out.out
    assert "1 of 2 requests FAILED" in out.err and "08B CLEVER: URLError" in out.err and "the Centre is down" in out.err
