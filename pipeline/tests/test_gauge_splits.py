"""Gauges as split definitions — a pure function over the frozen match."""

from __future__ import annotations

from pipeline.gauges.generate.match import StationMatch
from pipeline.gauges.consume.cuts import split_defs
from pipeline.common.models import AnchorType
from pipeline.common.models.splits import SplitDef


def _m(station="08AA001", **kw):
    base = dict(status="matched", resolved_by="name+radius", distance_m=12.0,
                lon=-123.0, lat=49.0, wsc="100-1", name="Test River", blk="B1")
    base.update(kw)
    return StationMatch(station, **base)


def _s(station="08AA001", realtime=True, name="TEST RIVER AT NOWHERE", active=True):
    return {"station": station, "name": name, "lon": -123.0, "lat": 49.0,
            "realtime": realtime, "active": active}


class TestWhatIsEmitted:
    def test_a_gauge_anchor_at_the_stations_own_coordinate(self):
        # The published coordinate, untouched. Everything after it — projection, the
        # braid sweep (`braid_sweep`), offsets, proximity pickup — is the resolver's
        # job, which is why this emits a definition and not a finished cut.
        rows = split_defs([_m()], [_s()])
        assert len(rows) == 1
        assert rows[0]["anchor"] == {"type": "gauge", "coord": [-123.0, 49.0],
                                     "is_lonlat": True}

    def test_scoped_by_watershed_so_a_braid_is_cut_across(self):
        # A gauge on a braided reach measures the whole channel, not the strand its
        # coordinate happens to land on.
        assert split_defs([_m()], [_s()])[0]["wsc"] == "100-1"

    def test_the_station_is_the_id(self):
        # It becomes the graph boundary `split:gauge__08AA001` and travels into the
        # registry, so a boundary carries the name of the thing that made it. Also its own
        # field, so nothing downstream parses an id string.
        r = split_defs([_m()], [_s()])[0]
        assert r["id"] == "gauge__08AA001"
        assert r["station"] == "08AA001"

    def test_every_row_is_a_valid_split_def(self):
        # THE CONTRACT. These go through the same loader as splits.json, so a row it
        # rejects takes the whole build down — after eighteen minutes of work.
        d = SplitDef.from_dict(split_defs([_m()], [_s()])[0])
        assert d.anchor.type is AnchorType.gauge
        assert d.anchor.coord == (-123.0, 49.0)
        assert d.wsc == "100-1"


class TestWhatIsRefused:
    def test_a_lake_station_is_not_a_cut(self):
        # It reports a level for a body of water; there is no "above it" and "below it"
        # along a channel to separate. The match records it with a wbk and no blk.
        assert split_defs([_m(wsc="", wbk="99")], [_s()]) == []

    def test_a_discontinued_station_is_matched_but_not_cut_at(self):
        # It still sits somewhere — the bundle wants it — but cutting the river there buys
        # a boundary no reading will ever appear on.
        assert split_defs([_m()], [_s(realtime=False, active=False)]) == []
        assert len(split_defs([_m()], [_s(realtime=False, active=False)], live_only=False)) == 1

    def test_a_silent_station_is_cut_like_a_transmitting_one(self):
        # T691, owner 2026-11-12: "silent doesn't mean retired so we should add cuts Incase they start
        # transmitting again?" — ECCC Active, off today's real-time list (a seasonal gauge): cut, so its
        # section is there the day it reports. MUTATION: reading `speaks_now` again drops it.
        assert [r["id"] for r in split_defs([_m()], [_s(realtime=False, active=True)])] == ["gauge__08AA001"]
        # listed on the snapshot but discontinued by ECCC's own status: not cut
        assert split_defs([_m()], [_s(realtime=True, active=False)]) == []

    def test_an_unmatched_station_contributes_nothing(self):
        assert split_defs([_m(status="unresolved", wsc="", lon=None, lat=None)], [_s()]) == []

    def test_a_station_missing_from_the_roster_contributes_nothing(self):
        assert split_defs([_m("08AA001")], [_s("08ZZ999")]) == []


class TestDeterminism:
    def test_sorted_by_station_whatever_order_the_match_came_in(self):
        # AGENTS rule 19. Split ids are part of the section-id ABI; a reordering would move
        # boundaries between builds for no reason at all.
        ms = [_m("08ZZ009"), _m("08AA001")]
        st = [_s("08ZZ009"), _s("08AA001")]
        first = split_defs(ms, st)
        second = split_defs(list(reversed(ms)), list(reversed(st)))
        assert first == second
        assert [r["id"] for r in first] == ["gauge__08AA001", "gauge__08ZZ009"]


def test_t679_the_atlas_cut_is_the_gauges_placement():
    """T679 #3 (owner 2026-11-12, "One rule, accept the moves"): a station the atlas cut a river at speaks for the
    section BEGINNING at its cut; a cut where no section begins (a lake outlet) is held, and listed. Mutation pin:
    `place.CUT_STARTS_M = -1` places nothing."""
    from pipeline.common.models import NodeKind, StreamGraph, StreamNode
    from pipeline.gauges.consume.place import nodes_at_cuts
    g = StreamGraph()
    for nid, lo, hi in (("R:0", 0, 4451.9), ("R:4451", 4451.9, 9000)):
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk="R", down_m=lo, up_m=hi, length_m=hi - lo)
    rows = [{"split_id": "gauge__08XX001", "blk": "R", "route_measure": 4451.91},
            {"split_id": "gauge__08XX002", "blk": "R", "route_measure": 9000.0},
            {"split_id": "puntledge__signs", "blk": "R", "route_measure": 4451.91}]
    rep: dict = {}
    assert nodes_at_cuts(rows, g, report=rep) == {"08XX001": "R:4451"}
    assert [h["station"] for h in rep["held"]] == ["08XX002"]


def test_t682_the_owners_line_places_a_gauge_the_atlas_cut_on_several_channels(monkeypatch):
    """T682 (owner 2026-11-12, RULINGS P49): the Fraser at Steveston's coordinate lands on the Cannery Channel stub (C);
    the atlas cuts the stub AND, by the braid sweep, the Fraser beside it (F). A row whose `blk` names F is placed at
    F's cut; a row whose `blk` is the projection's own line, or names a line with no cut of this gauge, is not moved
    (the nine the owner left on today's line). Mutation pin: the policy off moves none."""
    from types import SimpleNamespace
    from pipeline.common.models import NodeKind, StreamGraph, StreamNode
    from pipeline.gauges.consume import place as P
    g = StreamGraph()
    for nid, blk, lo, hi in (("C:0", "C", 0, 656.5), ("C:656", "C", 656.5, 3886), ("F:0", "F", 0, 545.4),
                             ("F:545", "F", 545.4, 9069)):
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, down_m=lo, up_m=hi, length_m=hi - lo)
    rows = [{"split_id": "gauge__08MH028", "blk": "C", "route_measure": 656.48},
            {"split_id": "gauge__08MH028", "blk": "F", "route_measure": 545.44},
            {"split_id": "gauge__08XX009", "blk": "C", "route_measure": 656.48},
            {"split_id": "gauge__08XX009", "blk": "F", "route_measure": 545.44}]
    by_line: dict = {}
    assert P.nodes_at_cuts(rows, g, by_line=by_line) == {"08MH028": "F:545", "08XX009": "F:545"}     # the last row, as before
    assert by_line == {"08MH028": {"C": "C:656", "F": "F:545"}, "08XX009": {"C": "C:656", "F": "F:545"}}
    projected = {"08MH028": "C:656", "08XX009": "C:656", "08XX010": "C:656"}
    ms = [SimpleNamespace(station="08MH028", blk="F"),           # the owner named the Fraser
          SimpleNamespace(station="08XX009", blk="C"),           # today's line: not moved
          SimpleNamespace(station="08XX010", blk="F")]           # names a line the atlas did not cut at this gauge
    assert P.owner_line(ms, by_line, projected, g) == {"08MH028": "F:545"}
    monkeypatch.setattr(P, "THE_OWNERS_LINE_PLACES_A_GAUGE_CUT_ON_SEVERAL_LINES", False)
    assert P.owner_line(ms, by_line, projected, g) == {}


def test_t682_the_owners_line_is_a_field_of_his_call_and_promote_writes_it():
    """the call's `line` (a blue line key) survives the model; a call without one has none"""
    from pipeline.gauges.review import Decision
    d = Decision(verdict="confirmed", matched="Cannery Channel", wsc="100", line="356364114")
    assert d.line == "356364114" and Decision(verdict="confirmed").line == ""
