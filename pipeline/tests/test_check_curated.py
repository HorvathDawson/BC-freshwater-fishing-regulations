"""The staleness gate over generated-but-committed artifacts.

WHAT IT GUARDS. `gauge_match.json` costs a completed build and 2.8 GB of pickled geometry to
produce, so it is made rarely, reviewed by hand and committed. That is correct — BC
commissions a handful of stations a year — but it fails silently: the roster gains a station,
nothing regenerates, and that river is simply ungauged forever. No error, no empty table.

These tests pin the three verdicts apart, because collapsing them is the whole bug. `absent`
must never fail CI (the stocking and bathymetry artifacts do not exist yet), `STALE` always
must, and `dropped` is reported but is not on its own a failure.
"""

from __future__ import annotations

import json

import pytest

from pipeline.tools import check_curated as C


def _art(tmp_path, roster_rows, frozen_blob, *, frozen_ids="stations.station"):
    r = tmp_path / "roster.json"
    r.write_text(json.dumps(roster_rows))
    f = tmp_path / "frozen.json"
    if frozen_blob is not None:
        f.write_text(json.dumps(frozen_blob))
    return C.Artifact(name="t", roster=r, frozen=f, regenerate="run the thing",
                      roster_ids="station", frozen_ids=frozen_ids)


class TestVerdicts:
    def test_every_id_decided_is_fresh(self, tmp_path):
        a = _art(tmp_path, [{"station": "A"}, {"station": "B"}],
                 {"stations": [{"station": "A"}, {"station": "B"}]})
        assert C.check(a)[0] == "fresh"

    def test_a_new_station_in_the_roster_is_stale(self, tmp_path):
        # The failure this file exists for: ECCC commissions a gauge, nobody re-runs the
        # match, and the river is ungauged with nothing anywhere saying so.
        a = _art(tmp_path, [{"station": "A"}, {"station": "NEW"}],
                 {"stations": [{"station": "A"}]})
        verdict, detail = C.check(a)
        assert verdict == "STALE"
        assert "NEW" in detail

    def test_a_dropped_station_is_reported_but_still_fresh(self, tmp_path):
        # Decommissioning happens and is not an error. It IS how a curated decision quietly
        # stops applying to anything, so it has to appear in the line.
        a = _art(tmp_path, [{"station": "A"}],
                 {"stations": [{"station": "A"}, {"station": "RETIRED"}]})
        verdict, detail = C.check(a)
        assert verdict == "fresh"
        assert "1 dropped" in detail

    def test_an_unbuilt_artifact_is_absent_not_stale(self, tmp_path):
        # `stock_match.json` and `chart_match.json` are declared before they exist so the
        # gate is waiting for them. Reporting those as STALE would make CI red forever and
        # teach everyone to ignore it.
        a = _art(tmp_path, [{"station": "A"}], None)
        assert C.check(a)[0] == "absent"

    def test_a_missing_roster_is_absent_too(self, tmp_path):
        a = C.Artifact(name="t", roster=tmp_path / "nope.json", frozen=tmp_path / "f.json",
                       regenerate="x", roster_ids="station", frozen_ids="stations.station")
        assert C.check(a)[0] == "absent"


class TestIdExtraction:
    """The shape is declared, never guessed — see `Artifact.ids`."""

    def test_a_top_level_list(self, tmp_path):
        a = _art(tmp_path, [{"station": "A"}], {"stations": []})
        assert a.ids(a.roster, "station") == {"A"}

    def test_a_named_list(self, tmp_path):
        a = _art(tmp_path, [], {"stations": [{"station": "A"}, {"station": "B"}]})
        assert a.ids(a.frozen, "stations.station") == {"A", "B"}

    def test_a_named_map_ignores_sibling_keys(self, tmp_path):
        # The bug this replaces: guessing the shape counted `_about` as a station id, so a
        # perfectly fresh file reported two phantom drops.
        a = _art(tmp_path, [], {"_about": "prose", "stations": {"A": "River", "B": None}},
                 frozen_ids="stations.*")
        assert a.ids(a.frozen, "stations.*") == {"A", "B"}

    def test_a_wrong_spec_raises_rather_than_guessing(self, tmp_path):
        a = _art(tmp_path, [], {"stations": {"A": "River"}})
        with pytest.raises(TypeError):
            a.ids(a.frozen, "stations.station")     # a map declared as a list
        with pytest.raises(KeyError):
            a.ids(a.frozen, "nope.field")


class TestTheRealDeclarations:
    def test_every_artifact_names_a_command_to_regenerate_it(self):
        # A gate that says "this is stale" without saying what to run is a gate people
        # learn to skip.
        for art in C.ARTIFACTS:
            assert art.regenerate.strip(), art.name

    def test_the_live_ones_parse(self):
        # Catches an Artifact spec that does not match the file it points at — which would
        # otherwise surface as a traceback in CI rather than as a verdict.
        for art in C.ARTIFACTS:
            C.check(art)

    def test_the_gauge_match_is_in_sync_with_the_roster(self):
        gauge = next(a for a in C.ARTIFACTS if a.name == "gauge match")
        if gauge.roster.exists() and gauge.frozen.exists():
            assert C.check(gauge)[0] == "fresh", (
                "the gauge match is stale — run "
                "`python -m pipeline.gauges.generate.match --build <a completed build>`")


def test_only_active_stations_need_a_decision(tmp_path):
    """User, 2026-10-09: an inactive gauge publishes no reading, so an unmatched one is reported,
    never STALE; an unmatched ACTIVE gauge still fails."""
    import dataclasses
    rows = [{"station": "A", "active": True}, {"station": "B", "active": False}]
    a = dataclasses.replace(_art(tmp_path, rows, {"stations": [{"station": "A"}]}),
                            roster_where="active")
    verdict, detail = C.check(a)
    assert verdict == "fresh" and "1 without a decision, none active" in detail
    a = dataclasses.replace(_art(tmp_path, rows, {"stations": [{"station": "B"}]}),
                            roster_where="active")
    assert C.check(a)[0] == "STALE"


def test_the_gauge_queue_is_committed_curated_data():
    """CI has no data/generated: the review queue lives beside the matches, in curated."""
    gauge = next(a for a in C.ARTIFACTS if a.name == "gauge match")
    assert gauge.candidates is not None and "curated" in gauge.candidates.parts
    assert gauge.roster_where == "active"
