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


def _dfo(lid, ext, water="1:a"):
    from pipeline.regs.dfo_salmon import entries as E
    return E.EntryLocation.from_dict({"location_id": lid, "region": "1", "region_number": 1, "kind": "water",
                                      "precedence": 3, "water": "A", "water_id": water,
                                      "binding": {"extents": ext}})


def test_a_dfo_rest_is_saved_unfrozen_and_resolved_against_every_placed_place_at_build():
    """Owner 2026-11-02: "the rest-of op should be after all extents are placed; it will be the remaining
    parts without any other extent applying"."""
    from pipeline.regs.dfo_salmon import entries as E
    fix = M.ExtentFix(scope="entry", op="rest", items=["gnis:1"])
    assert P.extent_of(fix, lambda *_: None)[1]                          # catalogue: a hand edit
    ex, probs = P.extent_of(fix, lambda *_: None, rest_at_build=True)
    assert not probs and "siblings" not in ex                            # nothing frozen
    me = _dfo("1:a:rest", [ex])
    placed = {x.location_id: x for x in (
        me, _dfo("1:a:b", [{"op": "whole", "item_id": "gnis:1"}]),
        _dfo("1:a:unplaced", []),                                          # not subtracted
        _dfo("1:z:other", [{"op": "whole", "item_id": "gnis:9"}], water="1:z"))}
    entry, rules = E.to_reach_input(me, [{}], None, siblings=placed)
    assert rules[0]["extents"][0]["siblings"] == ["1:a:b"]
    assert [r["rule_id"] for r in entry["rules"]] == ["1:a:rest#0", "1:a:b"]
    later = {**placed, "1:a:c": _dfo("1:a:c", [{"op": "whole", "item_id": "gnis:1"}])}
    _e, rules = E.to_reach_input(me, [{}], None, siblings=later)        # placed later: out, no re-mark
    assert rules[0]["extents"][0]["siblings"] == ["1:a:b", "1:a:c"]


def test_the_rest_is_the_water_minus_the_union_of_the_other_placed_extents():
    """The builder's own complement over a toy water: 3 sections, a placed sibling on 1, a second on
    another; the unplaced one takes nothing."""
    from pipeline.atlas.reach import build as B
    from pipeline.regs.dfo_salmon import entries as E

    class _B:
        def __init__(self, secs):
            self.outcome, self.sections, self.via_tributary = B.Outcome.bound, tuple(secs), ()
            self.tributaries_pending, self.reason, self.detail = False, None, ""
    picks = {"1:a:b": ["s1"], "1:a:c": ["s2"]}

    def fake(entry, rule, *_a, **_k):
        if rule["rule_id"] in picks:
            return _B(picks[rule["rule_id"]]), []
        return _B(["s1", "s2", "s3"]), []                                 # the whole water
    me = _dfo("1:a:rest", [{"op": "rest"}])
    sibs = {"1:a:b": _dfo("1:a:b", [{"op": "whole"}]), "1:a:c": _dfo("1:a:c", [{"op": "whole"}]),
            "1:a:u": _dfo("1:a:u", [])}
    import unittest.mock as um
    with um.patch.object(B, "build_reach", side_effect=fake):
        entry, rules = E.to_reach_input(me, [{}], None, siblings=sibs)
        got, _d = B._build_rest(entry, rules[0], B._rest_of(rules[0]), None, None, covered=None, clip=None,
                                outside=None, shared=None, regional=False)
        assert set(got.sections) == {"s3"}
        entry, rules = E.to_reach_input(me, [{}], None, siblings={k: v for k, v in sibs.items() if k != "1:a:c"})
        got, _d = B._build_rest(entry, rules[0], B._rest_of(rules[0]), None, None, covered=None, clip=None,
                                outside=None, shared=None, regional=False)
        assert set(got.sections) == {"s2", "s3"}                         # fewer placed: a bigger rest
