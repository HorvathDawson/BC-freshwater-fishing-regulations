"""Region 6 section E is COMPUTED as the region minus B, C, D and F (owner 2026-11-09), never a hand list.

`entries.residual_minus` writes every E default's `outside_areas` from its siblings' own defaults on load and on save.
Mutation-pinned: moving a sibling's area moves E, a hand-edited E list is overwritten, a sibling that is not a whole
area is refused by name.
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.regs.dfo_salmon import entries as E

RAW = json.loads(E.path_for("6").read_text(encoding="utf-8"))
E_DEFAULTS = ("section_default", "area_default")


def _e_lists(ef: E.EntryFile) -> list[list[str]]:
    return [list(x.outside_areas) for l in ef.locations if l.kind in E_DEFAULTS and l.section == "E"
            for x in l.binding.extents if x.op == "within"]


def _sibling(raw: dict, location_id: str) -> dict:
    return next(l for l in raw["locations"] if l["location_id"] == location_id)


def test_e_is_the_region_minus_its_siblings_areas():
    ef = E._from_dict(copy.deepcopy(RAW))
    want = ["area:basin:400-", "area:basin:500-", "area:mu_group:management_units_6_12_and_6_13", "area:basin:100-"]
    lists = _e_lists(ef)
    assert lists and all(x == want for x in lists)        # B, C, D, F in Scope.minus order


def test_a_hand_edited_e_list_is_overwritten():
    raw = copy.deepcopy(RAW)
    for l in raw["locations"]:
        if l["kind"] in E_DEFAULTS and l["section"] == "E":
            for ex in l["binding"]["extents"]:
                ex["outside_areas"] = ["area:basin:400-"]        # forgot C, D and F
    assert all(len(x) == 4 for x in _e_lists(E._from_dict(raw)))


def test_moving_a_siblings_area_moves_e():
    raw = copy.deepcopy(RAW)
    _sibling(raw, "6:c:section")["binding"]["extents"] = [{"op": "within", "area_id": "area:basin:999-"}]
    lists = _e_lists(E._from_dict(raw))
    assert all("area:basin:999-" in x and "area:basin:500-" not in x for x in lists)


def test_a_sibling_that_is_not_a_whole_area_is_refused():
    raw = copy.deepcopy(RAW)
    _sibling(raw, "6:b:section")["binding"]["extents"] = [
        {"op": "upstream_of", "splits": ["skeena_river__cnr_railway_bridge_terrace"], "item_id": "gnis:2936"}]
    with pytest.raises(ValueError, match=r"residual E subtracts B.*not a whole area"):
        E._from_dict(raw)


def test_the_file_reads_back_as_itself():
    ef = E._from_dict(copy.deepcopy(RAW))
    d = E.residual_minus(ef).to_dict()
    assert E._from_dict(json.loads(json.dumps(d))).to_dict() == d
