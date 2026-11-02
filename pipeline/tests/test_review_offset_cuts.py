"""Owner 2026-11-02: an offset cut is REFERENCE + the split's own offset attribute; owner overrides on a
PROPOSED split (move, re-anchor on a park boundary, one cut per offset); a DFO "rest" names its siblings."""
from __future__ import annotations

from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P

WEIR = dict(ref="w", anchor="dam", name="weir", on_item="gnis:1", coord=(-127.4855, 50.6895),
            blk="9", measure=1434.7)


class _Src:
    def item(self, _i):
        return None

    splits_path = "x"

    def doc(self, _p):
        return {"waterbodies": [{"splits": [{"id": "s1", "anchor": {"type": "point", "coord": [-127.0, 50.0],
                                                                     "is_lonlat": True}}]}]}


def test_two_offsets_off_one_reference_are_two_cuts():
    a, b = M.NewSplit(**WEIR, offset_m=100), M.NewSplit(**WEIR, offset_m=-100)
    assert not M.same_anchor(a, b) and M.same_anchor(a, M.NewSplit(**WEIR, offset_m=100))


def test_an_offset_override_makes_one_cut_per_offset_and_the_split_carries_it():
    o = M.ConfidenceOverride(match=M.NewSplit(**WEIR), offsets=[100, -100], by="t", date="d")
    g = {"id": "w", "split": M.NewSplit(**WEIR), "members": [M.NewSplit(**WEIR)], "marks": []}
    rows = M.with_confidence([g], [o])
    assert [r["id"] for r in rows] == ["w-u100", "w-d100"]
    rec, _ = P.split_record(rows[1]["split"], _Src(), sid="x", provenance="p")
    assert rec["anchor"]["coord"] == [-127.4855, 50.6895]               # the reference, not baked
    assert rec["anchor"]["offset_m"] == 100.0 and rec["anchor"]["offset_dir"] == "downstream"


def test_a_set_override_moves_or_reanchors_the_proposed_cut():
    mv = M.ConfidenceOverride(match=M.NewSplit(**WEIR), set={"measure": 1500.0}, by="t", date="d")
    assert M.with_confidence([{"id": "w", "split": M.NewSplit(**WEIR), "members": [M.NewSplit(**WEIR)],
                               "marks": []}], [mv])[0]["split"].measure == 1500.0
    park = M.NewSplit(**{**WEIR, "anchor": "area_boundary", "area": "X PARK", "area_end": "downstream"})
    rec, _ = P.split_record(park, _Src(), sid="x", provenance="p")
    assert rec["anchor"] == {"type": "area_boundary", "area_layer": "parks_bc", "area_name": "X PARK",
                             "area_name_field": "PROTECTED_LANDS_NAME", "side": "downstream"}
    off = M.NewSplit(ref="s", anchor="split", split_id="s1", offset_m=-50)
    rec, _ = P.split_record(off, _Src(), sid="x", provenance="p")
    assert rec["anchor"]["coord"] == [-127.0, 50.0] and rec["anchor"]["offset_dir"] == "downstream"


def test_a_dfo_rest_names_its_siblings_and_rides_them_into_the_builder():
    fix = M.ExtentFix(scope="entry", op="rest", items=["gnis:1"])
    assert P.extent_of(fix, lambda *_: None)[1]                          # catalogue: a hand edit
    ex, probs = P.extent_of(fix, lambda *_: None, ["1:a:b", "1:a:c"])
    assert not probs and ex["siblings"] == ["1:a:b", "1:a:c"]
    from pipeline.regs.dfo_salmon import entries as E
    loc = lambda lid, ext: E.EntryLocation.from_dict({                   # noqa: E731
        "location_id": lid, "region": "1", "region_number": 1, "kind": "water", "precedence": 3,
        "water": "A", "water_id": "1:a", "binding": {"extents": ext}})
    me = loc("1:a:rest", [{"op": "rest", "item_id": "gnis:1", "siblings": ["1:a:b"]}])
    sib = loc("1:a:b", [{"op": "whole", "item_id": "gnis:1"}])
    entry, rules = E.to_reach_input(me, [{}], None, siblings={"1:a:b": sib})
    assert [r["rule_id"] for r in entry["rules"]] == ["1:a:rest#0", "1:a:b"]
