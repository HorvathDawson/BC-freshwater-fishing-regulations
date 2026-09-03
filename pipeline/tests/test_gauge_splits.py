"""Gauges as split definitions — a pure function over the frozen match."""

from __future__ import annotations

from pipeline.gauges.generate.match import StationMatch
from pipeline.gauges.consume.cuts import split_defs
from pipeline.models import AnchorType
from pipeline.models.splits import SplitDef


def _m(station="08AA001", **kw):
    base = dict(status="matched", resolved_by="name+radius", distance_m=12.0,
                lon=-123.0, lat=49.0, wsc="100-1", name="Test River", blk="B1")
    base.update(kw)
    return StationMatch(station, **base)


def _s(station="08AA001", realtime=True, name="TEST RIVER AT NOWHERE"):
    return {"station": station, "name": name, "lon": -123.0, "lat": 49.0,
            "realtime": realtime}


class TestWhatIsEmitted:
    def test_a_gauge_anchor_at_the_stations_own_coordinate(self):
        # The published coordinate, untouched. Everything after it — projection, the
        # perpendicular sweep across a braid, offsets, proximity pickup — is the resolver's
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
        assert split_defs([_m()], [_s(realtime=False)]) == []
        assert len(split_defs([_m()], [_s(realtime=False)], live_only=False)) == 1

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
