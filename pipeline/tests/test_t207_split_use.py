"""T207 item 2 (owner 2026-10-26): a curated cut is USED when anything names it — by id, by the area
an extent names (Pitt River's "within Garibaldi Park" binds the park boundary), by a merged place
(an alias, or another name at that place), or by a DFO salmon entry. `split_use.used_split_ids` is
the one answer; the review app's "curated cut(s) no rule uses" and the unused-splits audit read it."""
from __future__ import annotations

import glob
import json

from pipeline.common.curated import CURATED
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.regs.review.split_use import used_split_ids

PARK = {"id": "x__park_boundary", "label": "Some Park boundary", "kind": "area_boundary",
        "anchor": {"type": "area_boundary", "area_layer": "parks_bc", "area_name": "SOME PARK",
                   "area_name_field": "PROTECTED_LANDS_NAME"}}
FALLS = {"id": "x__falls", "label": "the falls", "anchor": {"type": "point", "coord": [0, 0]}}
SIGNS = {"id": "x__signs", "label": "the signs", "anchor": {"type": "point", "coord": [0, 0]}}
SPLITS = {"waterbodies": [{"name": "X", "applies_to": {"gnis_id": "1"}, "splits": [PARK, FALLS, SIGNS]}]}


def _entry(*extents):
    return {"entry_id": "r1:x@1-1", "matched": ["gnis:1"],
            "rules": [{"rule_id": "x.r1", "extents": list(extents)}]}


def test_an_area_extent_uses_that_area_s_boundary_cut():
    within = _entry({"op": "whole", "within_area": "area:park:some_park"})
    assert used_split_ids([within], SPLITS) == {"x__park_boundary"}
    # the mutation: another park uses nothing here
    assert used_split_ids([_entry({"op": "whole", "within_area": "area:park:other_park"})], SPLITS) == set()
    for key in ("area_id", "outside_area"):
        assert used_split_ids([_entry({"op": "within", key: "area:park:some_park"})], SPLITS) == {"x__park_boundary"}


def test_an_id_and_a_dfo_entry_use_a_cut():
    assert used_split_ids([_entry({"op": "upstream_of", "splits": ["x__falls"]})], SPLITS) == {"x__falls"}
    dfo = {"scopes": [{"note": "bound at x__signs", "bound": "x__signs"}]}
    assert used_split_ids([], SPLITS, dfo=[dfo]) == {"x__signs"}


def test_a_merged_place_is_used_through_any_id_or_name_it_answers_to():
    by_alias = RegistryBoundary(id="x__falls", label="the falls", kind="split", ref="split:x__falls",
                                aliases=("split:x__signs",))
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="X", kind="stream", boundaries=(by_alias,),
                                  ref_ids=("gnis:1",))}
    rule = _entry({"op": "upstream_of", "splits": ["x__signs"]})
    assert used_split_ids([rule], SPLITS, registry=reg) == {"x__falls", "x__signs"}
    by_name = RegistryBoundary(id="x__falls", label="the falls", kind="split", ref="split:x__falls",
                               names=("the signs",))
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="X", kind="stream", boundaries=(by_name,),
                                  ref_ids=("gnis:1",))}
    falls = _entry({"op": "upstream_of", "splits": ["x__falls"]})
    assert used_split_ids([falls], SPLITS, registry=reg) == {"x__falls", "x__signs"}
    assert used_split_ids([falls], SPLITS) == {"x__falls"}


def test_the_pitt_and_wood_park_boundaries_are_used_on_the_corpus():
    """The review app's false positive: Pitt River's park cut (and Wood River's Hamber Park cut) are
    bound by an area extent, not by id."""
    splits = json.loads(CURATED.waters.splits.read_text(encoding="utf-8"))
    entries = [e for p in sorted(glob.glob(str(CURATED.regulations.entries.catalogue / "region-*.json")))
               for e in json.loads(open(p, encoding="utf-8").read())["entries"]]
    used = used_split_ids(entries, splits)
    assert {"pitt_river__garibaldi_pitt", "wood_river__hamber_prov_park_boundary"} <= used
    # T207's new cuts are bound
    assert {"white_river__100_m_below_salmon_viewing_platform", "white_river__100_m_above_salmon_viewing_platform",
            "fulton_river__spawning_channels_downstream_end", "fulton_river__spawning_channels_upstream_end"} <= used
