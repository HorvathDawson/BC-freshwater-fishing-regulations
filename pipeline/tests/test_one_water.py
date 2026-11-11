"""T664: a water the curator named across several codes is ONE registry item (`registry.one_water`).

Owner 2026-11-11: "the oxbows should be combined as one item". Synthetic registries; the last test reads the committed
`name_variants.json` for the flag."""
from __future__ import annotations

import pytest

from pipeline.atlas.registry import one_water as OW
from pipeline.atlas.registry.flowing import absorbed_refs, canonical, canonical_ids
from pipeline.common.models import RegistryBoundary, RegistryItem

NAME = "Okanagan River Oxbows"


def _it(iid, name, kind, secs, mus=("8-1",), boundaries=()):
    return RegistryItem(id=iid, name=name, kind=kind, variants=(name,), mus=tuple(mus), section_ids=tuple(secs),
                        ref_ids=(iid,), boundaries=tuple(boundaries))


def _reg():
    return {
        "wsc:300-1-200": _it("wsc:300-1-200", NAME, "stream", ["b2:0", "lake:7"], boundaries=[
            RegistryBoundary("ox__cut", "cut", "split", ref="split:ox__cut"),
            RegistryBoundary("ox__lake_7", "lake 7", "lake", ref="lake:7", wbk="7")]),
        "wsc:300-1-100": _it("wsc:300-1-100", NAME, "stream", ["b1:0", "b1:50"]),
        "wbk:9": _it("wbk:9", "OKANAGAN RIVER OXBOWS", "stream", ["lake:9"]),
        "wbk:8": _it("wbk:8", "OKANAGAN RIVER OXBOWS", "wetland", ["lake:8"]),
        "gnis:5": _it("gnis:5", "Park Rill", "stream", ["p:0"]),
    }


def test_the_named_stream_items_become_one_and_the_absorbed_ids_are_aliases():
    """Mutation: skip the pop -> 5 items; keep the first id instead of the line rank -> survivor wbk:9."""
    rep: dict = {}
    out = OW.merge_one_waters(_reg(), [NAME], rep)
    assert set(out) == {"wsc:300-1-100", "wbk:8", "gnis:5"}
    one = out["wsc:300-1-100"]
    assert set(one.section_ids) == {"b1:0", "b1:50", "b2:0", "lake:7", "lake:9"}
    assert set(one.aliases) == {"wsc:300-1-200", "wbk:9"}
    assert {"wsc:300-1-200", "wbk:9", "wsc:300-1-100"} <= set(one.ref_ids)
    assert one.kind == "stream" and one.name == NAME
    assert rep[NAME] == {"survivor": "wsc:300-1-100", "absorbed": ["wbk:9", "wsc:300-1-200"], "kept_apart": ["wbk:8"],
                         "sections": 5}
    # rule 55's read points map an absorbed id; everything else refuses it
    assert canonical("wbk:9", out) == "wsc:300-1-100"
    entry = {"entry_id": "r8:x", "matched": ["wsc:300-1-200", "wbk:9", "wbk:8"]}
    assert canonical_ids(entry, out)["matched"] == ["wsc:300-1-100", "wbk:8"]
    assert len(absorbed_refs([entry], out)) == 2


def test_a_cut_stays_and_a_lake_edge_onto_its_own_polygon_goes():
    out = OW.merge_one_waters(_reg(), [NAME])
    ids = {b.id for b in out["wsc:300-1-100"].boundaries}
    assert "ox__cut" in ids and "ox__lake_7" not in ids


def test_a_cut_aliased_onto_its_own_polygon_edge_is_refused():
    reg = _reg()
    b = RegistryBoundary("ox__lake_7", "lake 7", "lake", ref="lake:7", wbk="7", aliases=("split:a_cut",))
    reg["wsc:300-1-200"] = _it("wsc:300-1-200", NAME, "stream", ["b2:0", "lake:7"], boundaries=[b])
    with pytest.raises(ValueError, match="cut alias"):
        OW.merge_one_waters(reg, [NAME])


def test_still_water_of_the_name_stays_apart_unless_the_owner_switches_it(monkeypatch):
    """Default: no kind moves, so no zone rule moves. The switch pins the other branch."""
    out = OW.merge_one_waters(_reg(), [NAME])
    assert out["wbk:8"].kind == "wetland"
    monkeypatch.setattr(OW, "ONE_WATER_TAKES_STILL_WATER", True)
    out2 = OW.merge_one_waters(_reg(), [NAME])
    assert "wbk:8" not in out2 and "wbk:8" in out2["wsc:300-1-100"].aliases


def test_no_flag_changes_nothing_and_other_names_are_untouched():
    reg = _reg()
    assert OW.merge_one_waters(reg, []) is reg
    out = OW.merge_one_waters(reg, [NAME])
    assert out["gnis:5"] is reg["gnis:5"]


def test_one_name_in_two_regions_is_refused_never_merged():
    reg = _reg()
    reg["wsc:900-1"] = _it("wsc:900-1", NAME, "stream", ["z:0"], mus=("2-4",))
    with pytest.raises(ValueError, match="two waters share the name"):
        OW.merge_one_waters(reg, [NAME])


def test_the_flag_is_read_from_a_name_record():
    nv = [{"target": {"wscs": ["1"]}, "names": [{"name": NAME, "one_water": True}]},
          {"target": {"blks": ["2"]}, "names": [{"name": NAME.upper(), "one_water": True}]},
          {"target": {"blks": ["3"]}, "names": [{"name": "Other Creek"}]}]
    assert OW.one_water_names(nv) == [NAME]


def test_the_committed_file_flags_the_oxbows_and_only_them():
    from pipeline.atlas.graph.names import load_name_variants
    from pipeline.tests import under_test
    assert OW.one_water_names(load_name_variants(under_test.curated().waters.name_variants)) == [NAME]
