"""FIDQ waterbody -> registry item.

The failure that matters is not a missing lake. It is the WRONG lake: BC has many lakes
sharing a name, and a stocking record attached to the wrong one tells somebody there are
fish where there are none. Every test here is that question asked a different way.
"""

from __future__ import annotations

import pytest
from shapely.geometry import Point

from pipeline.common.models.enums import NodeKind
from pipeline.common.models.graph import StreamGraph, StreamNode
from pipeline.stocking.generate.match import RADIUS_M, StockMatch, match_waterbodies

pytest.importorskip("geopandas")


def _world(named: dict[str, tuple[float, float]]):
    """Nodes at given LON/LAT, so a test can place a lake beside a point."""
    import geopandas as gpd

    g = StreamGraph()
    geoms = {}
    ids, pts = [], []
    for name, (lon, lat) in named.items():
        nid = f"lake:{name}"
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.lake, wbk=name,
                                  display_name=name, stream_magnitude=10)
        ids.append(nid)
        pts.append(Point(lon, lat))
    proj = gpd.GeoSeries(pts, crs=4326).to_crs(3005)
    for nid, geom in zip(ids, proj):
        geoms[nid] = geom
    return g, geoms


def _water(wid: str, name: str, lon: float, lat: float) -> dict:
    return {"waterbody_id": wid, "name": name, "lon": lon, "lat": lat}


class TestMatching:
    def test_a_lake_at_the_point_matches(self):
        g, geoms = _world({"Hatheume Lake": (-120.10, 49.90)})
        [m] = match_waterbodies([_water("W1", "Hatheume Lake", -120.10, 49.90)], geoms, g)
        assert (m.status, m.resolved_by) == ("matched", "name+radius")
        assert m.node_id == "lake:Hatheume Lake"

    def test_a_differently_named_lake_at_the_point_does_not_match(self):
        # Location alone is never enough. This is the whole reason the name is checked.
        g, geoms = _world({"Pennask Lake": (-120.10, 49.90)})
        [m] = match_waterbodies([_water("W1", "Hatheume Lake", -120.10, 49.90)], geoms, g)
        assert m.status == "unresolved"

    def test_a_correctly_named_lake_far_away_does_not_match(self):
        # v1's prototype matched a Maple Ridge "Cedar Creek" to a Similkameen one 250 km
        # off. The radius is a hard cap, never a preference.
        g, geoms = _world({"Hatheume Lake": (-123.50, 49.90)})
        [m] = match_waterbodies([_water("W1", "Hatheume Lake", -120.10, 49.90)], geoms, g)
        assert m.status == "unresolved"
        assert f"{RADIUS_M:.0f}" in (m.reason or "")

    def test_two_differently_named_matches_are_refused_not_guessed(self):
        # THE ONE THAT MATTERS. Where the gauge matcher may take the closest of several
        # correctly-named candidates, this must not: a coin toss here puts fish in the
        # wrong lake, and the app then says so with total confidence.
        g, geoms = _world({"Twin Lake": (-120.100, 49.900),
                           "Twin Lake Upper": (-120.101, 49.901)})
        [m] = match_waterbodies(
            [_water("W1", "Twin Lake Upper", -120.100, 49.900)], geoms, g)
        assert m.status == "ambiguous"
        assert m.node_id is None
        assert len(m.candidates) == 2

    def test_an_ambiguous_row_names_its_candidates_so_it_can_be_curated(self):
        g, geoms = _world({"Twin Lake": (-120.100, 49.900),
                           "Twin Lake Upper": (-120.101, 49.901)})
        [m] = match_waterbodies(
            [_water("W1", "Twin Lake Upper", -120.100, 49.900)], geoms, g)
        assert set(m.candidates) == {"twin lake", "twin lake upper"}

    def test_several_pieces_of_one_water_are_not_several_candidates(self):
        # A river run is many nodes with one name. Counting pieces rather than waters would
        # make every river ambiguous.
        g, geoms = _world({"Dragon Lake": (-120.100, 49.900)})
        g.nodes["lake:Dragon Lake 2"] = StreamNode(
            node_id="lake:Dragon Lake 2", kind=NodeKind.lake, wbk="d2",
            display_name="Dragon Lake", stream_magnitude=10)
        geoms["lake:Dragon Lake 2"] = geoms["lake:Dragon Lake"]
        [m] = match_waterbodies([_water("W1", "Dragon Lake", -120.100, 49.900)], geoms, g)
        assert m.status == "matched"

    def test_an_override_binds_to_a_node_not_to_a_name(self):
        # Same discipline as the gauge overrides: a name-keyed override is a second guess
        # at the same ambiguous question, and follows the wrong water on a rename.
        g, geoms = _world({"Blakeny Creek": (-120.10, 49.90)})
        [m] = match_waterbodies([_water("W1", "Cedar Creek", -120.10, 49.90)], geoms, g,
                                aliases={"W1": "lake:Blakeny Creek"})
        assert (m.status, m.resolved_by) == ("matched", "override")
        assert m.node_id == "lake:Blakeny Creek"


class TestIdentifierFirst:
    """FIDQ ships the FWA's own group code. Use it before reaching for a name."""

    def test_the_identifier_matches_without_any_name_agreeing(self):
        g, geoms = _world({"Surveyor Name Lake": (-120.10, 49.90)})
        [m] = match_waterbodies(
            [{**_water("W1", "Angler Name Lake", -120.10, 49.90), "identifier": "02322SAJR"}],
            geoms, g, by_identifier={"02322SAJR": ["lake:Surveyor Name Lake"]})
        assert m.status == "matched"
        assert m.node_id == "lake:Surveyor Name Lake"

    def test_an_identifier_no_name_corroborates_is_flagged_not_hidden(self):
        # v1's `_confirmed_by_name`. An exact key that agrees with no name is more likely a
        # stale identifier than a surprise, and silently trusting it is worse than saying so.
        g, geoms = _world({"Surveyor Name Lake": (-120.10, 49.90)})
        [m] = match_waterbodies(
            [{**_water("W1", "Angler Name Lake", -120.10, 49.90), "identifier": "02322SAJR"}],
            geoms, g, by_identifier={"02322SAJR": ["lake:Surveyor Name Lake"]})
        assert m.unconfirmed is True
        assert m.resolved_by == "identifier"

    def test_an_identifier_the_name_agrees_with_is_not_flagged(self):
        g, geoms = _world({"Dragon Lake": (-120.10, 49.90)})
        [m] = match_waterbodies(
            [{**_water("W1", "Dragon Lake", -120.10, 49.90), "identifier": "02322SAJR"}],
            geoms, g, by_identifier={"02322SAJR": ["lake:Dragon Lake"]})
        assert m.unconfirmed is False
        assert m.resolved_by == "identifier+name"

    def test_a_group_code_covering_two_waters_takes_the_nearer(self):
        # FWA's own multi-part grouping, or a collision. Never arbitrary.
        g, geoms = _world({"Near Part": (-120.100, 49.900),
                           "Far Part": (-120.400, 49.900)})
        [m] = match_waterbodies(
            [{**_water("W1", "Near Part", -120.100, 49.900), "identifier": "02322SAJR"}],
            geoms, g, by_identifier={"02322SAJR": ["lake:Far Part", "lake:Near Part"]})
        assert m.node_id == "lake:Near Part"

    def test_an_unknown_identifier_falls_through_to_the_name_search(self):
        g, geoms = _world({"Hatheume Lake": (-120.10, 49.90)})
        [m] = match_waterbodies(
            [{**_water("W1", "Hatheume Lake", -120.10, 49.90), "identifier": "NOSUCH"}],
            geoms, g, by_identifier={"02322SAJR": ["lake:Hatheume Lake"]})
        assert (m.status, m.resolved_by) == ("matched", "name+radius")


class TestEveryInputIsAccountedFor:
    def test_a_water_that_matches_nothing_still_gets_a_row(self):
        # A silent drop is how a stocked lake disappears from the app without anybody
        # noticing it was ever expected to be there.
        g, geoms = _world({"Somewhere Else": (-123.0, 50.0)})
        out = match_waterbodies([_water("W1", "Hatheume Lake", -120.1, 49.9)], geoms, g)
        assert len(out) == 1 and out[0].waterbody_id == "W1"

    def test_a_water_with_no_name_is_unresolved_not_skipped(self):
        g, geoms = _world({"Hatheume Lake": (-120.10, 49.90)})
        out = match_waterbodies([_water("W1", "", -120.10, 49.90)], geoms, g)
        assert [m.status for m in out] == ["unresolved"]

    def test_output_is_sorted_so_the_bundle_bytes_are_stable(self):
        g, geoms = _world({"A Lake": (-120.1, 49.9), "B Lake": (-120.2, 49.9)})
        out = match_waterbodies([_water("W9", "B Lake", -120.2, 49.9),
                                 _water("W1", "A Lake", -120.1, 49.9)], geoms, g)
        assert [m.waterbody_id for m in out] == ["W1", "W9"]


def test_the_alias_file_is_never_the_shared_name_variants_file():
    # v1's bathymetry matching "fixed" bad matches by editing shared name variants, which
    # corrupted display names elsewhere. No matcher may write into that file.
    from pathlib import Path

    import pipeline.stocking.generate.match as m

    src = Path(m.__file__).read_text(encoding="utf-8")
    assert "name_variants.json`" in src          # mentioned only in the warning
    assert src.count("name_variants") == 1


class TestIdentifierIndex:
    """The FWA 50K group code -> lake node index that makes tier 1 an exact join.

    Validated against independent government data: 2,676 of 2,697 identifiers in
    `data/wsa_bathymetry_maps.csv` (99.2%) resolve through this index, and the names agree
    — 02322SAJR is 103 Mile Lake on both sides. That is a different BC dataset using the
    same identifier family, which is about as strong a check as this can get without FIDQ.
    """

    def _graph_with(self, wbk_by_node):
        g = StreamGraph()
        for nid, wbk in wbk_by_node.items():
            g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.lake, wbk=wbk,
                                      display_name=nid, stream_magnitude=5)
        return g

    def test_a_missing_gpkg_is_an_empty_index_not_a_crash(self):
        from pathlib import Path

        from pipeline.stocking.generate.identifiers import build_identifier_index
        assert build_identifier_index(Path("does/not/exist.gpkg"),
                                      self._graph_with({})) == {}

    def test_a_code_covering_two_waters_keeps_both(self):
        # FWA's own multi-part grouping — 4.2% of codes on the real atlas. Collapsing to
        # one here would hide the ambiguity at the only place it can still be resolved
        # honestly, which is against FIDQ's own anchor point.
        from pipeline.stocking.generate.match import match_waterbodies
        g, geoms = _world({"A": (-120.10, 49.90), "B": (-120.30, 49.90)})
        [m] = match_waterbodies(
            [{**_water("W1", "A", -120.10, 49.90), "identifier": "00001ADMS"}], geoms, g,
            by_identifier={"00001ADMS": ["lake:A", "lake:B"]})
        assert m.node_id == "lake:A"          # the nearer of the two

    def test_a_node_the_graph_does_not_have_is_ignored(self):
        # The index is built from the source layer, which may be newer than the graph.
        from pipeline.stocking.generate.match import match_waterbodies
        g, geoms = _world({"A": (-120.10, 49.90)})
        [m] = match_waterbodies(
            [{**_water("W1", "A", -120.10, 49.90), "identifier": "00001ADMS"}], geoms, g,
            by_identifier={"00001ADMS": ["lake:GONE"]})
        assert m.resolved_by == "name+radius"   # fell through, did not crash
