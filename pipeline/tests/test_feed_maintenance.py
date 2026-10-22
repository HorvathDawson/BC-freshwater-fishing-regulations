"""Owner-run upstream updates, said in the feed's own index (owner 2026-10-22 late: "the feed should check for hydat
releases then have a way for a notification to be sent that the hydat needs to be updated").

Every feed run already asks HYDAT's listing (one GET) and ECCC's real-time station list. What it learns is written
into `index.json` as `maintenance`: one notice per owner task that is due, `{kind, since, current, latest, action}`.
`since` is the first feed run that saw the notice and is carried forward from the previous index; the notice clears
itself on the first run where nothing is due (HYDAT: the envelope's release IS the newest listed).

The app re-reads the index while in service (like a data release), so a notice reaches readers with no app change.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline.gauges.feed import publish as pub

NOW = datetime(2026, 10, 22, 21, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 10, 22, 21, 30, tzinfo=timezone.utc)
CLIM = {"release": "20260717", "stations": {}}
NEWER = "20261020"


@pytest.fixture
def net(monkeypatch):
    # no station answers: the notices do not depend on readings, and nothing reaches the network
    monkeypatch.setattr(pub, "_get", lambda url: None)
    monkeypatch.setattr(pub, "_sleep", lambda s: None)


def _index(out: Path) -> dict:
    return json.loads((out / "index.json").read_text())


HYDAT_DUE = {"kind": "hydat", "since": "2026-10-22T21:00:00Z", "current": "20260717", "latest": NEWER,
             "action": "scripts/update-hydat.sh"}


def test_a_newer_hydat_writes_a_notice(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=NOW)
    idx = _index(tmp_path)
    assert idx["maintenance"] == [HYDAT_DUE]
    # the old block stays for any reader of it
    assert idx["hydat"] == {"release": "20260717", "latest": NEWER, "stale": True}


def test_since_is_the_first_run_that_saw_it(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=NOW)
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=LATER)
    assert _index(tmp_path)["maintenance"] == [HYDAT_DUE]


def test_a_later_release_is_a_new_notice(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=NOW)
    pub.publish(tmp_path, [], clim=CLIM, latest_release="20270115", now=LATER)
    assert _index(tmp_path)["maintenance"] == [dict(HYDAT_DUE, latest="20270115", since="2026-10-22T21:30:00Z")]


def test_the_notice_clears_when_the_envelope_catches_up(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=NOW)
    pub.publish(tmp_path, [], clim={"release": NEWER, "stations": {}}, latest_release=NEWER, now=LATER)
    idx = _index(tmp_path)
    assert idx["maintenance"] == []
    assert idx["hydat"] == {"release": NEWER, "latest": NEWER, "stale": False}


def test_nothing_due_is_an_empty_list(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release="20260717", now=NOW)
    assert _index(tmp_path)["maintenance"] == []


def test_a_failed_listing_keeps_a_due_notice(tmp_path, net):
    # One failed GET must not make a due update look done (the app would flip to "Up to date" and back).
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=NOW)
    pub.publish(tmp_path, [], clim=CLIM, latest_release=None, now=LATER)
    assert _index(tmp_path)["maintenance"] == [HYDAT_DUE]


def test_a_failed_listing_does_not_keep_a_notice_the_envelope_answered(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=NOW)
    pub.publish(tmp_path, [], clim={"release": NEWER, "stations": {}}, latest_release=None, now=LATER)
    assert _index(tmp_path)["maintenance"] == []


def test_no_envelope_no_hydat_notice(tmp_path, net):
    # Without clim.json there is no release in use to compare: nothing is claimed.
    pub.publish(tmp_path, [], clim=None, latest_release=NEWER, now=NOW)
    assert _index(tmp_path)["maintenance"] == []


DRIFT = {"joined": ["08NM139", "10CD005"], "left": ["08EG012"]}
ROSTER_DUE = {"kind": "roster", "since": "2026-10-22T21:00:00Z", "current": "data/source/bc_hydrometric_stations.json",
              "latest": "+08NM139 +10CD005 -08EG012", "action": pub.ROSTER_ACTION}


def test_roster_drift_writes_a_notice(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release="20260717", now=NOW, drift=DRIFT)
    assert _index(tmp_path)["maintenance"] == [ROSTER_DUE]


def test_roster_and_hydat_notices_together_in_kind_order(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=NOW, drift=DRIFT)
    pub.publish(tmp_path, [], clim=CLIM, latest_release=NEWER, now=LATER, drift=DRIFT)
    assert _index(tmp_path)["maintenance"] == [HYDAT_DUE, ROSTER_DUE]


def test_no_drift_clears_the_roster_notice(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release="20260717", now=NOW, drift=DRIFT)
    pub.publish(tmp_path, [], clim=CLIM, latest_release="20260717", now=LATER, drift={"joined": [], "left": []})
    assert _index(tmp_path)["maintenance"] == []


def test_an_unreadable_station_list_keeps_the_roster_notice(tmp_path, net):
    pub.publish(tmp_path, [], clim=CLIM, latest_release="20260717", now=NOW, drift=DRIFT)
    pub.publish(tmp_path, [], clim=CLIM, latest_release="20260717", now=LATER, drift=None)
    assert _index(tmp_path)["maintenance"] == [ROSTER_DUE]


def test_the_roster_action_is_the_refresh_command():
    assert pub.ROSTER_ACTION == (
        "rm data/source/bc_hydrometric_stations.json && "
        "(cd data && PYTHONPATH=.. ../.venv/bin/python fetch_data.py --layers hydrometric_stations) && "
        "scripts/heavy.sh build 40 -- .venv/bin/python -m pipeline build")


def test_the_hydat_action_is_a_script_that_exists():
    assert pub.HYDAT_ACTION == "scripts/update-hydat.sh"
    root = Path(__file__).resolve().parents[2]
    script = root / pub.HYDAT_ACTION
    assert script.exists()
    text = script.read_text()
    assert text.startswith("#!/usr/bin/env bash")
    assert "set -euo pipefail" in text
    # the order is the point: fetch, envelope, republish with it, then the bundle through heavy.sh
    steps = ["fetch_data.py --layers hydat", "pipeline.gauges.feed.climatology",
             "pipeline.gauges.feed.publish", "scripts/heavy.sh"]
    body = text[text.index("set -euo pipefail"):]             # the commands, not the header's description of them
    at = [body.index(s) for s in steps]
    assert at == sorted(at)
    assert '--clim "$FEED/clim.json"' in body
    assert "scripts/heavy.sh build 40 -- $PY -m pipeline build" in body
