"""T230 (owner 2026-10-29): the in-season pick-points on the built atlas — lake inlets / outlets, named confluences,
region / MU crossings, the mouth — and what an extent on them covers (`pipeline.regs.inseason.points`)."""
from __future__ import annotations

import pytest

from pipeline.tests import under_test
from pipeline.tests.conftest import need, ATLAS_HINT

PITT, KASLO, KEEN, FRASER, GRANBY = "gnis:7551", "gnis:11345", "gnis:11410", "gnis:39325", "gnis:18775"


@pytest.fixture(scope="module")
def atlas(request):
    need(request, "atlas", under_test.atlas(request) / "graph.pkl", ATLAS_HINT)
    from pipeline.atlas.reach import position
    from pipeline.atlas.registry import load_registry, regions
    from pipeline.common.io.serialize import read_artifact
    b = under_test.atlas()
    reg = load_registry(str(b / "registry.json"))
    g = read_artifact(str(b / "graph.pkl"))
    regions.attach_from(b, g, reg)
    position.attach(g, b)
    return reg, g


def _ids(atlas, item):
    from pipeline.regs.inseason import points as P
    return {p.id: p for p in P.points(*atlas, item)}


@pytest.mark.needs_atlas
def test_the_picker_offers_lakes_confluences_crossings_and_the_mouth(atlas):
    pitt, kaslo, fraser = _ids(atlas, PITT), _ids(atlas, KASLO), _ids(atlas, FRASER)
    assert pitt["lake:329291806:inlet"].label == "Pitt Lake inlet"
    assert pitt["lake:329291806:outlet"].m < pitt["lake:329291806:inlet"].m
    assert kaslo[f"trib:{KEEN}"].label == "Keen Creek confluence"
    assert kaslo["mouth"].label.startswith("Mouth (into Kootenay Lake")
    assert fraser["region:2|3"].m < fraser["region:3|5"].m


@pytest.mark.needs_atlas
def test_extents_on_derived_points_select_by_measure_and_walk_tributaries(atlas):
    from pipeline.regs.inseason import points as P
    reg, g = atlas
    up = set(P.resolve(reg, g, {"op": "upstream_of", "item_id": PITT, "splits": ["lake:329291806:inlet"]})["sections"])
    down = set(P.resolve(reg, g, {"op": "downstream_of", "item_id": PITT, "splits": ["lake:329291806:outlet"]})["sections"])
    assert up and down and not up & down
    whole = set(reg[PITT].section_ids)
    assert up | down <= whole and up | down != whole           # the lake between them is neither
    below_keen = P.resolve(reg, g, {"op": "downstream_of", "item_id": KASLO, "splits": [f"trib:{KEEN}"]})
    assert 0 < below_keen["n"] < len(reg[KASLO].section_ids)
    only = P.resolve(reg, g, {"op": "whole", "item_id": GRANBY, "tributaries": "only"})
    assert only["n"] == only["tributaries"] > 0 and not set(only["sections"]) & set(reg[GRANBY].section_ids)
    inc = P.resolve(reg, g, {"op": "whole", "item_id": GRANBY, "tributaries": "include"})
    assert set(inc["sections"]) == set(only["sections"]) | set(reg[GRANBY].section_ids)
    with pytest.raises(P.Unresolved):
        P.resolve(reg, g, {"op": "upstream_of", "item_id": PITT, "splits": ["trib:gnis:11410"]})   # not on the Pitt


def test_place_ids_validate_by_shape():
    from pipeline.regs.inseason import matches as M
    good = ["mouth", "lake:329291806:inlet", "trib:gnis:11410", "region:2|3", "mu:2-4|2-8", "area:5", "length:1:2"]
    assert all(M.place_id_shape(x) for x in good)
    assert not any(M.place_id_shape(x) for x in ["lake:1:middle", "trib:nope", "region:2", "no_such_split"])
    doc = M.parse('{"format": "inseason-matches/2", "matches": {"is-000000000001": {"extents": '
                  '[{"op": "upstream_of", "item_id": "gnis:7551", "splits": ["lake:1:inlet"], "tributaries": "only"}]}}}')
    assert M.problems(doc, split_ids=set()) == []
    bad = doc.model_copy(update={"matches": {"is-000000000001": doc.matches["is-000000000001"].model_copy(
        update={"extents": [M.InSeasonExtent(op="upstream_of", item_id="gnis:7551", splits=["nope"])]})}})
    assert M.problems(bad, split_ids=set()) == ["is-000000000001: unknown split 'nope'"]
    with pytest.raises(ValueError):
        M.InSeasonExtent(op="whole", item_id="gnis:1", tributaries="some")
