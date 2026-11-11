"""T550 item 1: the Python publisher (the spec the worker is held to) gates a reading's rank by its age, as the worker does.

The worker's side is app/workers/feeds/test/gauges.age.test.ts; the parity test runs both on one recording. The hours are
one constant, emitted to the app (`READING_MAX_AGE_HOURS`, `pipeline.tools.emit.gauge_policy`).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.gauges.feed.publish import READING_MAX_AGE_HOURS, age_gate, reading_current

AT = "2026-11-08T04:40:00-08:00"
T = datetime.fromisoformat(AT)


def test_a_reading_on_the_line_is_current_and_one_past_it_is_not():
    assert reading_current(AT, T + timedelta(hours=READING_MAX_AGE_HOURS))
    assert not reading_current(AT, T + timedelta(hours=READING_MAX_AGE_HOURS, seconds=1))


def test_an_old_reading_loses_every_rank_and_keeps_its_time_in_key_order():
    e = {"percentile": 0.4, "parameter": "discharge", "discharge": 0.4, "level": 0.6, "observedAt": AT, "forecast": False}
    got = age_gate(e, T + timedelta(hours=37))
    assert got == {"percentile": None, "parameter": "discharge", "observedAt": AT, "forecast": False}
    assert list(got) == ["percentile", "parameter", "observedAt", "forecast"]
    assert age_gate(e, T + timedelta(hours=1)) is e


def test_a_naive_or_missing_stamp_is_not_judged():
    now = datetime(2026, 11, 20, tzinfo=timezone.utc)
    assert reading_current("2026-11-08T04:40:00", now)
    assert reading_current(None, now)
    assert reading_current("not a time", now)
