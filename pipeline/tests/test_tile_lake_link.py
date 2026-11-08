"""(T493) The creek through a hidden lake, and (T489) the standing water's gentler pass at the low zooms.

T493 (owner 2026-11-07, Foley Lake on Foley Creek): zoomed out, a small lake leaves the map before its creek, and the
creek broke at the lake. Below the lake's first zoom its route is drawn in the `stream` tile layer under the LAKE's
section id (`layers.LAKE_LINK`), so the stretch wears the lake's status and a tap opens the lake.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import orjson

from pipeline.common.models.graph import FlowEdge
from pipeline.deliver.tiles import layers as L
from pipeline.deliver.tiles import tippe
from pipeline.deliver.tiles.export import lake_link_zooms


def _graph(*flows: tuple[str, str]):
    """A graph of only its edges: `a` flows into `b` for each (a, b)."""
    edges = [FlowEdge(a, b, 0.0) for a, b in flows]
    up, down = {}, {}
    for i, e in enumerate(edges):
        up.setdefault(e.to_node, []).append(i)
        down.setdefault(e.from_node, []).append(i)
    return SimpleNamespace(edges=edges, up_adj=up, down_adj=down)


def test_a_lake_hidden_between_two_drawn_creeks_is_linked_until_its_own_zoom():
    # Foley: the creek z8 above and below, the lake from z10
    g = _graph(("up", "lake"), ("lake", "down"))
    assert lake_link_zooms(g, {"lake": 10}, {"up": 8, "down": 8}) == {"lake": 8}
    # drawn from the zoom BOTH sides are: the creek below from z9 leaves z8 with nothing to join
    assert lake_link_zooms(g, {"lake": 10}, {"up": 8, "down": 9}) == {"lake": 9}


def test_no_link_where_there_is_no_gap():
    g = _graph(("up", "lake"), ("lake", "down"))
    # the lake is drawn before (or with) its creek
    assert lake_link_zooms(g, {"lake": 8}, {"up": 8, "down": 8}) == {}
    assert lake_link_zooms(g, {"lake": 6}, {"up": 8, "down": 8}) == {}
    # a headwater lake: water on one side only
    assert lake_link_zooms(_graph(("lake", "down")), {"lake": 10}, {"down": 8}) == {}
    # an inflow that is not drawn (no feature: a wetland, an undrawn piece) is no water
    assert lake_link_zooms(g, {"lake": 10}, {"down": 8}) == {}


def test_a_chain_of_hidden_lakes_is_linked_through():
    # creek z8 -> lake a (z11) -> lake b (z12) -> creek z9: both lakes linked from z9 (the creek below comes in at 9)
    g = _graph(("top", "a"), ("a", "b"), ("b", "bottom"))
    assert lake_link_zooms(g, {"a": 11, "b": 12}, {"top": 8, "bottom": 9}) == {"a": 9, "b": 9}
    # a drawn lake in the chain is water too: b drawn from z9 itself
    assert lake_link_zooms(g, {"a": 11, "b": 9}, {"top": 8, "bottom": 12}) == {"a": 9}


def test_a_cycle_through_lakes_ends():
    g = _graph(("top", "a"), ("a", "b"), ("b", "a"), ("b", "bottom"))
    assert lake_link_zooms(g, {"a": 11, "b": 12}, {"top": 8, "bottom": 8}) == {"a": 8, "b": 8}


def test_the_link_file_is_no_layer_of_its_own_and_not_in_the_contract():
    assert L.DRAWN_INTO == {L.LAKE_LINK: "stream"}
    assert L.LAKE_LINK not in L.BY_NAME and L.LAKE_LINK not in L.contract()["layers"]
    assert L.LAKE_LINK in tippe.WATER_PASS, "the link's ends must stay shared nodes with the creek's"


def _feature(layer: str, minzoom: int, maxzoom: int = 13, **props) -> bytes:
    return orjson.dumps({
        "type": "Feature", "geometry": {"type": "Point", "coordinates": [-123.0, 49.0]},
        "properties": props, "tippecanoe": {"layer": layer, "minzoom": minzoom, "maxzoom": maxzoom},
    }) + b"\n"


def _runs(tmp_path: Path, monkeypatch) -> dict[str, tuple[list[str], list[dict]]]:
    d = tmp_path / "layers"
    d.mkdir()
    (d / "stream.geojsonl").write_bytes(_feature("stream", 8, section_id=11, ord=3))
    (d / f"{L.LAKE_LINK}.geojsonl").write_bytes(_feature("stream", 8, 9, section_id=21, ord=3))
    (d / "lake.geojsonl").write_bytes(_feature("lake", 6, section_id=31, water="lake", area_m2=9))
    (d / "park.geojsonl").write_bytes(_feature("park", 6, area_id="X PARK", name="X", kind="provincial"))
    runs = {}

    def fake_run(cmd):
        feats = [orjson.loads(ln) for a in cmd if a.endswith(".geojsonl")
                 for ln in Path(a).read_bytes().splitlines() if ln.strip()]
        runs[Path(cmd[cmd.index("-o") + 1]).name] = (cmd, feats)
        Path(cmd[cmd.index("-o") + 1]).write_bytes(b"")

    monkeypatch.setattr(tippe, "_run", fake_run)
    monkeypatch.setattr(tippe, "check", lambda: None)
    tippe.build(d, tmp_path / "atlas.pmtiles")
    return runs


def test_the_link_is_built_into_the_stream_layer_with_the_lakes_id(tmp_path, monkeypatch):
    runs = _runs(tmp_path, monkeypatch)
    cmd, feats = runs["z8-water.mbtiles"]
    by_id = {f["id"]: f for f in feats}
    assert set(by_id) == {11, 21}, "the link rides the water pass with the creek"
    link = by_id[21]
    assert link["tippecanoe"] == {"layer": "stream", "minzoom": 8, "maxzoom": 9}
    assert link["properties"] == {"ord": 3}, "cut to the stream layer's tile properties, the id once"
    # past its last zoom the link is in NO pass's input: left in, its vertices would pin the same route in under_lake
    for name in ("z10-water.mbtiles", "z12-water.mbtiles", "high.mbtiles"):
        assert 21 not in {f.get("id") for f in runs[name][1]}, name


def test_the_lakes_take_a_gentler_pass_up_to_z10_and_ride_the_water_pass_above(tmp_path, monkeypatch):
    runs = _runs(tmp_path, monkeypatch)
    for z in range(6, 13):
        gentle = z <= tippe.STILL_WATER_GENTLE_MAX_Z
        name = f"z{z}-still.mbtiles" if gentle else f"z{z}-water.mbtiles"
        cmd, feats = runs[name]
        assert [f["id"] for f in feats if f["tippecanoe"]["layer"] == "lake"] == [31], name
        tol = tippe.STILL_WATER_SIMPLIFICATION if gentle else tippe.WATER_SIMPLIFICATION
        assert f"--simplification={tol}" in cmd and f"--simplification-at-maximum-zoom={tol}" in cmd
        if gentle and f"z{z}-water.mbtiles" in runs:
            assert not [f for f in runs[f"z{z}-water.mbtiles"][1] if f["tippecanoe"]["layer"] == "lake"]
    assert tippe.STILL_WATER_SIMPLIFICATION < tippe.WATER_SIMPLIFICATION
