"""T152 — NOT FISHING WATER: sewage settling basins, mine tailing ponds and dumps are stamped once by
the atlas from their FWA FEATURE_CODE (`StreamNode.not_fishing`); no rule binds them, and the tiles
draw them in no water layer."""

from __future__ import annotations

import json
import pickle

import orjson
import pytest
from shapely.geometry import box

from pipeline.atlas.build import stamp_not_fishing
from pipeline.atlas.reach import classify as C
from pipeline.atlas.reach.build import build_reach
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.common.fishing_water import NOT_FISHING_WATER, is_not_fishing, not_fishing_sections
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem


def test_the_codes_are_pinned():
    assert NOT_FISHING_WATER == {"EA26700110": "sewage settling basin",
                                 "AP90300100": "mine tailing pond", "AP09200000": "dump"}
    assert is_not_fishing(" ea26700110 ") and not is_not_fishing("GB24300000")   # a reservoir is water


def _lake(wbk, **kw):
    return StreamNode(node_id=f"lake:{wbk}", kind=NodeKind.lake, wbk=str(wbk), **kw)


def _graph(*nodes):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    return g


def test_the_atlas_stamps_the_code_and_the_reach_reads_it():
    g = _graph(_lake(1), _lake(2), StreamNode(node_id="9:0", kind=NodeKind.stream, blk="9"))
    assert stamp_not_fishing(g, {"2": "EA26700110", "77": "AP09200000"}) == {"EA26700110": 1}
    assert g.nodes["lake:2"].not_fishing == "EA26700110" and not g.nodes["lake:1"].not_fishing
    assert not_fishing_sections(g) == {"lake:2"}


def _reg():
    def item(iid, secs, kind="area"):
        return RegistryItem(id=iid, name=iid, kind=kind, section_ids=tuple(secs))
    return {"area:region:4": item("area:region:4", ["lake:1", "lake:2", "s"]),
            "area:watershed:liard": item("area:watershed:liard", ["lake:1", "lake:2"])}


def _g():
    return _graph(_lake(1), _lake(2, not_fishing="EA26700110"),
                  StreamNode(node_id="s", kind=NodeKind.stream, blk="1", down_m=0, up_m=1, length_m=1))


def test_no_zone_or_area_rule_binds_a_sewage_basin(monkeypatch):
    """The 12 Fort Nelson sewage basins under the "Liard River watershed" area rule."""
    reg, g = _reg(), _g()
    zone = {"rule_id": "r1", "type": "closure", "extents": [{"op": "within", "area_id": "area:region:4"}]}
    b, diags = build_reach({"entry_id": "z4:x", "matched": [], "rules": [zone]}, zone, reg, g)
    assert b.outcome is Outcome.bound and set(b.sections) == {"lake:1", "s"}
    assert any(d.kind == "not_fishing_water" and d.payload["removed"] == 1 for d in diags)
    # mutation: switch the limit off and the basin binds again
    monkeypatch.setattr(C, "NOT_FISHING_WATER_SUBTRACTED", False)
    b, _ = build_reach({"entry_id": "z4:x", "matched": [], "rules": [zone]}, zone, reg, g)
    assert "lake:2" in b.sections


def test_a_rule_left_with_only_not_fishing_water_is_unresolved_not_empty():
    reg = dict(_reg(), **{"area:x": RegistryItem(id="area:x", name="x", kind="area",
                                                 section_ids=("lake:2",))})
    rule = {"rule_id": "r1", "type": "closure", "extents": [{"op": "within", "area_id": "area:x"}]}
    b, _ = build_reach({"entry_id": "z4:x", "matched": [], "rules": [rule]}, rule, reg, _g())
    assert b.outcome is Outcome.unresolved and b.reason is Reason.no_sections
    assert "not fishing water" in b.detail


def test_the_tiles_draw_it_in_no_water_layer(tmp_path):
    from pipeline.deliver.tiles.export import export_waterbodies
    g = _graph(_lake(1), _lake(2, not_fishing="AP90300100"))
    (tmp_path / "graph.pkl").write_bytes(pickle.dumps(g))
    (tmp_path / "waterbody_polys.pkl").write_bytes(pickle.dumps(
        {"lake:1": box(1_000_000, 500_000, 1_000_500, 500_500),
         "lake:2": box(1_002_000, 500_000, 1_002_500, 500_500)}))
    (tmp_path / "registry.json").write_text(json.dumps({"items": []}))
    (tmp_path / "section_handles.txt").write_text("lake:1\nlake:2\n")
    out = tmp_path / "layers"
    out.mkdir()
    counts = export_waterbodies(tmp_path, "", out)
    assert counts == {"lake": 1, "not_fishing_water": 1}
    nf = [orjson.loads(x) for x in (out / "not_fishing_water.geojsonl").read_bytes().splitlines()]
    assert nf[0]["properties"] == {}                     # decorative: no id, nothing to tap
    lakes = [orjson.loads(x) for x in (out / "lake.geojsonl").read_bytes().splitlines()]
    assert [f["properties"]["section_id"] for f in lakes] == [1]
