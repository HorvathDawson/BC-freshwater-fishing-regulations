"""Hydrometric stations as split definitions — what gets emitted, and what does not."""

from __future__ import annotations

from types import SimpleNamespace

from pipeline.hydro import splits as GS
from pipeline.models import AnchorType
from pipeline.models.splits import SplitDef


def _graph(nodes: dict):
    return SimpleNamespace(nodes=nodes)


def _node(blk="B1", wsc="100-123456", kind="NodeKind.stream", name="Test River"):
    return SimpleNamespace(blk=blk, wsc=wsc, kind=kind, display_name=name)


def _station(sid="08AA001", name="TEST RIVER AT NOWHERE", lon=-123.0, lat=49.0):
    return {"station": sid, "name": name, "lon": lon, "lat": lat, "realtime": True}


class TestWhatIsEmitted:
    def test_a_gauge_anchor_at_the_stations_own_coordinate(self):
        # The published coordinate, untouched. Everything downstream of it — projection,
        # the perpendicular sweep across a braid, offsets, proximity pickup — is the
        # resolver's job, which is the whole point of emitting a definition and not a cut.
        rows = GS.split_defs(_graph({"n1": _node()}), [_station()], {"08AA001": "n1"})
        assert len(rows) == 1
        assert rows[0]["anchor"] == {"type": "gauge", "coord": [-123.0, 49.0],
                                     "is_lonlat": True}
        assert rows[0]["id"] == "gauge__08AA001"

    def test_scoped_by_wsc_so_a_braid_is_cut_across(self):
        # A gauge on a braided reach measures the whole channel, not the strand its
        # coordinate happens to land on. WSC is the river AND its side channels.
        rows = GS.split_defs(_graph({"n1": _node(wsc="100-123456")}), [_station()],
                             {"08AA001": "n1"})
        assert rows[0]["wsc"] == "100-123456"
        assert "blk" not in rows[0]

    def test_falls_back_to_the_blue_line_when_there_is_no_watershed_code(self):
        rows = GS.split_defs(_graph({"n1": _node(wsc="", blk="BLK9")}), [_station()],
                             {"08AA001": "n1"})
        assert rows[0]["blk"] == "BLK9"
        assert "wsc" not in rows[0]

    def test_every_row_is_a_valid_split_def(self):
        # THE CONTRACT. These are loaded by the same loader as splits.json, so a row the
        # loader rejects takes the whole build down — after 18 minutes of work.
        rows = GS.split_defs(_graph({"n1": _node()}), [_station()], {"08AA001": "n1"})
        d = SplitDef.from_dict(rows[0])
        assert d.anchor.type is AnchorType.gauge
        assert d.anchor.coord == (-123.0, 49.0)
        assert d.wsc == "100-123456"

    def test_records_which_water_the_match_chose(self):
        # The one judgement in this file worth a human's eye: everything else is mechanical.
        rows = GS.split_defs(_graph({"n1": _node(name="Chilliwack River")}),
                             [_station(name="CHILLIWACK RIVER AT VEDDER CROSSING")],
                             {"08AA001": "n1"})
        assert rows[0]["_name"] == "Chilliwack River"
        assert rows[0]["_node"] == "n1"


class TestWhatIsRefused:
    def test_a_lake_station_is_not_a_cut(self):
        # A lake reports a level for a body of water. There is no "above it" and "below it"
        # along a channel to separate, so there is nothing to cut — it is linked to the lake
        # instead, by pipeline.hydro.shed.lake_gauge_links.
        rows = GS.split_defs(_graph({"lk": _node(kind="NodeKind.lake")}), [_station()],
                             {"08AA001": "lk"})
        assert rows == []

    def test_a_node_with_no_identifier_at_all_is_skipped(self):
        # A bare coordinate is not a split: a point anchor with no target has no mainstem to
        # cut across, and the loader refuses it. Better to emit nothing than a bad row.
        rows = GS.split_defs(_graph({"n1": _node(wsc="", blk="")}), [_station()],
                             {"08AA001": "n1"})
        assert rows == []

    def test_a_station_with_no_coordinate_is_skipped(self):
        st = _station()
        st["lon"] = None
        rows = GS.split_defs(_graph({"n1": _node()}), [st], {"08AA001": "n1"})
        assert rows == []

    def test_an_unmatched_station_contributes_nothing(self):
        rows = GS.split_defs(_graph({"n1": _node()}), [_station()], {})
        assert rows == []


class TestDeterminism:
    def test_sorted_by_station_whatever_order_the_match_came_in(self):
        # AGENTS rule 19. Split ids are part of the section-id ABI; a reordering would move
        # boundaries between builds for no reason at all.
        g = _graph({"a": _node(blk="B1"), "b": _node(blk="B2", wsc="100-999")})
        st = [_station("08ZZ009"), _station("08AA001")]
        first = GS.split_defs(g, st, {"08ZZ009": "b", "08AA001": "a"})
        second = GS.split_defs(g, list(reversed(st)), {"08AA001": "a", "08ZZ009": "b"})
        assert first == second
        assert [r["id"] for r in first] == ["gauge__08AA001", "gauge__08ZZ009"]
