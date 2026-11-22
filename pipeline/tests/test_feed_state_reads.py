"""T994 #5 / #26: STORED FEED STATE THAT DOES NOT READ FAILS, NAMING THE FILE — never an empty state written back.

The gauge publisher read its water-temperature history (the only temperature record there is), each station's
year-to-date daily file and the previous index's maintenance notices in a bare `except`: one torn file and the run
wrote a near-empty one over it. The owner-reminder dedupe (`notify.send_due`) read its sent keys the same way, so a torn
file re-sent every reminder. Absent is still a first run.

THE CASES ARE SHARED (AGENTS 40): `app/workers/feeds/test/fixtures/state-reads.json`, read here and by the worker's twin
`app/workers/feeds/test/state-reads.test.ts`, so the two languages cannot each hold a copy that drifts.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.gauges.feed import notify
from pipeline.gauges.feed import publish as P

FIXTURE = Path(__file__).resolve().parents[2] / "app" / "workers" / "feeds" / "test" / "fixtures" / "state-reads.json"
F = json.loads(FIXTURE.read_text(encoding="utf-8"))


def _store(path: Path, stored: str | None) -> None:
    if stored is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(stored, encoding="utf-8")


@pytest.mark.parametrize("case", F["tempHistory"]["cases"], ids=lambda c: c["name"])
def test_the_temperature_history_reads_or_fails(tmp_path, case):
    _store(tmp_path / P.TEMP_HISTORY, case["stored"])
    temps = {s: ("t", c) for s, c in F["tempHistory"]["temps"].items()}
    if "error" in case:
        with pytest.raises(ValueError) as e:
            P.accumulate_temperatures(tmp_path, temps, pent=F["tempHistory"]["pent"])
        assert case["error"] in str(e.value)
        assert (tmp_path / P.TEMP_HISTORY).read_text(encoding="utf-8") == case["stored"]     # nothing written over it
    else:
        assert P.accumulate_temperatures(tmp_path, temps, pent=F["tempHistory"]["pent"]) == case["result"]


@pytest.mark.parametrize("case", F["stationFile"]["cases"], ids=lambda c: c["name"])
def test_a_station_file_reads_or_fails(tmp_path, case):
    st = F["stationFile"]["station"]
    _store(tmp_path / f"{st}.json", case["stored"])
    if "error" in case:
        with pytest.raises(ValueError) as e:
            P.previous_station(tmp_path, st)
        assert case["error"] in str(e.value)
    else:
        # the worker answers null for no file; the Python's caller starts from {}
        assert P.previous_station(tmp_path, st) == (case["result"] or {})


@pytest.mark.parametrize("case", F["notified"]["cases"], ids=lambda c: c["name"])
def test_the_reminder_dedupe_sends_nothing_on_a_state_that_does_not_read(tmp_path, capsys, case):
    out = tmp_path / "feeds"
    path = notify.state_path(out)
    _store(path, case["stored"])
    sent: list[str] = []
    got = notify.send_due(F["notified"]["notices"], out, url="https://ntfy.sh/x", sender=lambda u, t: sent.append(t))
    assert (got["sent"], got["failed"]) == (case["sent"], case["failed"])
    assert len(sent) == len(case["sent"])
    if "error" in case:
        assert case["error"] in got["error"]
        assert case["error"] in capsys.readouterr().err
        assert path.read_text(encoding="utf-8") == case["stored"]
    else:
        assert "error" not in got
        assert json.loads(path.read_text(encoding="utf-8"))["sent"] == case["recorded"]


def test_the_previous_index_absent_is_a_first_run_torn_fails(tmp_path):
    """The maintenance notices' `since` is carried from the last index; a torn one no longer resets them all (the
    worker reads it through `readStored`)."""
    assert P.previous_maintenance(tmp_path) is None
    (tmp_path / "index.json").write_text(json.dumps({"maintenance": [{"kind": "hydat", "since": "x"}]}), encoding="utf-8")
    assert P.previous_maintenance(tmp_path) == [{"kind": "hydat", "since": "x"}]
    (tmp_path / "index.json").write_text("[]", encoding="utf-8")
    assert P.previous_maintenance(tmp_path) is None
    (tmp_path / "index.json").write_text('{"maintenance": [', encoding="utf-8")
    with pytest.raises(ValueError, match="index.json is stored but does not parse"):
        P.previous_maintenance(tmp_path)


def test_the_sent_keys_are_written_atomically(tmp_path, monkeypatch):
    """A crash mid-write leaves the old file whole (temp file + os.replace), never a torn one."""
    out = tmp_path / "feeds"
    calls: list[Path] = []
    real = notify.atomic_write
    monkeypatch.setattr(notify, "atomic_write", lambda p, t: (calls.append(Path(p)), real(p, t)))
    notify.send_due(F["notified"]["notices"][:1], out, url="u", sender=lambda u, t: None)
    assert calls == [notify.state_path(out)]


@pytest.mark.parametrize("case", F["notified"]["cases"], ids=lambda c: c["name"])
def test_the_checked_form_fails_the_run_on_a_state_that_does_not_read(tmp_path, capsys, case):
    """(review) An `error` nobody reads stopped the reminders for good with one printed line: every feed calls
    `send_due_checked`, which raises naming the file (the feed is already written); the worker's `sendDueChecked`."""
    out = tmp_path / "feeds"
    _store(notify.state_path(out), case["stored"])
    go = lambda: notify.send_due_checked(F["notified"]["notices"], out, url="u", sender=lambda u, t: None)   # noqa: E731
    if "error" in case:
        with pytest.raises(notify.NotifyStateError) as e:
            go()
        assert case["error"] in str(e.value) and "The feed was written" in str(e.value)
    else:
        assert go()["sent"] == case["sent"]


def test_every_feed_calls_the_checked_form():
    root = Path(__file__).resolve().parents[2]
    for f in ("pipeline/gauges/feed/publish.py", "pipeline/regs/dfo_salmon/live.py"):
        src = (root / f).read_text(encoding="utf-8")
        assert "send_due_checked(" in src, f
        assert ".send_due(" not in src, f
