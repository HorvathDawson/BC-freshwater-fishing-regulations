"""THE FRASER'S DFO SUGGESTIONS (Fraser dossier 2026-11-14, T743): three suggester rules the Fraser's wordings
broke, pinned on the Fraser's own words and measures (atlas/full, 2026-11-14) with no atlas.

  * two reaches back to back ("from Mission upstream to Hope and from Hope to the Alexandra Bridge", FN0841) are
    ONE reach, Mission to Alexandra, and never the same place as "Mission to Hope";
  * a runner-up that IS the winning cut (the Tidal tab's proposal anchored on the curated CPR Bridge at Mission)
    no longer drags the match to "low";
  * a DFO page speaks for its own region (T668): an open end stops at the region's line when a cut stands there,
    a "whole water" that runs through other regions is not suggested whole, and an end past the line is named.
"""
from __future__ import annotations

from pipeline.regs.dfo_salmon import coverage as C
from pipeline.regs.dfo_salmon import suggest as S

FRASER = "gnis:39325"
BLK = "356364114"
MISSION, SPUZZUM = "fraser_river__the_cpr_bridge_at_mission", "fraser_river__spuzzum_creek_into_fraser_river"
DOUBLE = ("from the downstream side of the CPR Bridge at Mission upstream to the Highway 1 Bridge at Hope and from "
          "the Highway 1 Bridge at Hope to the downstream side of the Alexandra Bridge")
SINGLE = "from the downstream side of the CPR Bridge at Mission upstream to the Highway 1 Bridge at Hope"


def water() -> S.Water:
    return S.Water([FRASER], {FRASER: "Fraser River"}, {FRASER: ["100"]}, {FRASER: "stream"})


def cuts() -> list[S.Cut]:
    return [S.Cut(MISSION, "the CPR Bridge at Mission", "split", FRASER, ("08MH024 · Fraser River At Mission",)),
            S.Cut(SPUZZUM, "Spuzzum Creek → Fraser River", "confluence", FRASER),
            S.Cut("tidal-356364114", "Fraser River: the CPR Bridge at Mission", "proposed", FRASER,
                  ("Fraser River: the CPR Bridge at Mission",),
                  proposal={"anchor": "split", "split_id": MISSION, "name": "Fraser River: the CPR Bridge at Mission"})]


# ------------------------------------------------------------------ two reaches back to back

def test_two_reaches_back_to_back_are_one_reach_a_to_c():
    ph = S.endpoints(DOUBLE)
    assert [p.role for p in ph] == ["from", "to"]
    assert "Mission" in ph[0].text and "Alexandra" in ph[1].text and "Hope" not in ph[1].text
    assert S.double_reach(DOUBLE) and not S.double_reach(SINGLE)


def test_a_reach_whose_two_halves_do_not_share_an_end_is_not_folded():
    text = "from the Hwy 7 bridge to Siddle Creek and from the Fraser River to Dewdney"
    assert not S.double_reach(text)


def test_the_double_wording_is_never_clustered_with_one_of_its_halves():
    """"Mission to Hope AND Hope to Alexandra" reads 80%+ like "Mission to Hope"; the placed half must not hand
    the double wording its (shorter) extent."""
    placed = {"location_id": "2:fr:single", "water_id": "2:fraser-river", "duplicate_of": None,
              "source_text": {"specific_area": SINGLE},
              "binding": {"extents": [{"op": "between", "splits": [MISSION, SPUZZUM], "item_id": FRASER}]}}
    double = {"location_id": "2:fr:double", "water_id": "2:fraser-river", "duplicate_of": None,
              "source_text": {"specific_area": DOUBLE}, "binding": {"extents": []}}
    assert C.similarity(SINGLE, DOUBLE) >= 0.6          # alike enough that the old cluster joined them
    sugg = {"op": "between", "points": [{"kind": "split", "split_id": MISSION},
                                        {"kind": "new", "split_id": "fraser_alexandra_bridge"}],
            "items": [], "tributaries": None, "new_splits": [{"ref": "fraser_alexandra_bridge", "name": "Alexandra"}]}
    row = {"location_id": "2:fr:double", "suggestion": dict(sugg), "confidence": "medium", "matched": "new",
           "reason": "split hunt", "items": [FRASER]}
    locs = {"2:fr:single": ("2", placed, None), "2:fr:double": ("2", double, None)}
    C.cluster([row], locs)
    assert row["suggestion"]["points"][1]["split_id"] == "fraser_alexandra_bridge"
    assert "cluster" not in row


# ------------------------------------------------------------------ the same cut twice is no rival

def test_a_proposal_on_the_winning_cut_is_not_its_rival():
    p = S.resolve("the downstream side of the CPR Bridge at Mission", water(), cuts(), S.NoLookup(), ref="sg1")
    assert p.fix == {"kind": "split", "split_id": MISSION}
    assert p.confidence != "low" and "as close" not in p.reason


def test_same_cut_is_symmetric_and_only_for_a_split_anchor():
    c = cuts()
    assert S.same_cut(c[0], c[2]) and S.same_cut(c[2], c[0])
    assert not S.same_cut(c[0], c[1])
    other = S.Cut("x", "the CPR Bridge at Mission", "proposed", FRASER, proposal={"anchor": "bridge"})
    assert not S.same_cut(c[0], other)


# ------------------------------------------------------------------ a page speaks for its own region

#: the Fraser's main line, coarse: Region 2 to Spuzzum (km 197.0), Region 3 to km 418.4, 3+5 to km 452.7,
#: Region 5 to km 674.5, 5+7 to km 703.7, Region 7 to the top (atlas/full parts, 2026-11-14)
LINE = [(0, {2}), (76506, {2}), (158751, {2}), (196966, {3}), (418405, {3, 5}), (452701, {5}),
        (674465, {5, 7}), (703745, {7})]
SECTIONS = [f"{BLK}:{m}" for m, _r in LINE]
MEMBER = {f"{BLK}:{m}": frozenset(r) for m, r in LINE}
CUTS_AT = [(MISSION, BLK, 76506.3, "the CPR Bridge at Mission"), (SPUZZUM, BLK, 196966.56, "Spuzzum Creek → Fraser River")]
MEASURES = {MISSION: {BLK: 76506.3}, SPUZZUM: {BLK: 196966.56}, "hope": {BLK: 158751.0},
            "alexandra": {BLK: 201241.5}}


def test_region_span_reads_the_regions_stretch_and_its_lines():
    two = S.region_span(2, SECTIONS, MEMBER, CUTS_AT)
    assert (two.lo, two.hi, two.down) == (0.0, 196966.0, None)
    assert two.up.cut == SPUZZUM and two.regions == (2, 3, 5, 7)
    three = S.region_span(3, SECTIONS, MEMBER, CUTS_AT)
    assert (three.lo, three.hi) == (196966.0, 452701.0)
    assert three.down.cut == SPUZZUM and three.up.cut is None          # the 3/5 line is an area boundary
    assert three.shared == ((418405.0, 452701.0, (3, 5)),)


def test_region_span_refuses_a_region_in_two_runs():
    member = dict(MEMBER, **{f"{BLK}:703745": frozenset({3})})
    assert S.region_span(3, SECTIONS, member, CUTS_AT) is None


def _row(op, pts, conf="high", matched="new"):
    return {"location_id": "x", "confidence": conf, "matched": matched, "reason": "r",
            "suggestion": {"op": op, "points": [{"kind": "split", "split_id": p} for p in pts], "items": [],
                           "tributaries": None, "new_splits": [], "words": "w"}}


def test_an_open_end_stops_at_the_region_line_cut():
    span = S.region_span(2, SECTIONS, MEMBER, CUTS_AT)
    out = S.cap_to_region(_row("upstream_of", ["hope"]), span, MEASURES)
    assert out["suggestion"]["op"] == "between"
    assert [p["split_id"] for p in out["suggestion"]["points"]] == ["hope", SPUZZUM]
    assert out["confidence"] == "medium" and "Region 2's line" in out["region_note"]
    assert out["before"]["suggestion"]["op"] == "upstream_of"


def test_an_open_end_with_no_cut_at_the_line_is_flagged_low_not_capped():
    span = S.region_span(3, SECTIONS, MEMBER, CUTS_AT)
    out = S.cap_to_region(_row("upstream_of", [SPUZZUM]), span, MEASURES)
    assert out["suggestion"]["op"] == "upstream_of" and out["confidence"] == "low"
    assert "area boundary" in out["region_note"]


def test_a_whole_water_across_regions_is_not_suggested_whole():
    span = S.region_span(3, SECTIONS, MEMBER, CUTS_AT)
    out = S.cap_to_region(_row("whole", [], conf="medium", matched="whole"), span, MEASURES)
    assert out["suggestion"] is None and out["matched"] == "none" and "undrawn" in out["region_note"]
    two = S.region_span(2, SECTIONS, MEMBER, CUTS_AT)                  # Region 2: the mouth and a cut
    out2 = S.cap_to_region(_row("whole", [], conf="medium", matched="whole"), two, MEASURES)
    assert out2["suggestion"]["op"] == "downstream_of"
    assert out2["suggestion"]["points"] == [{"kind": "split", "split_id": SPUZZUM}]


def test_an_end_past_the_line_is_named_and_kept():
    span = S.region_span(2, SECTIONS, MEMBER, CUTS_AT)
    row = _row("between", [MISSION, "alexandra"], conf="medium")
    out = S.cap_to_region(row, span, MEASURES)
    assert out["suggestion"] == row["suggestion"]
    assert "4.3 km past Region 2's line" in out["region_note"]


def test_a_reach_into_the_shared_stretch_says_it_is_the_owners_call():
    span = S.region_span(3, SECTIONS, MEMBER, CUTS_AT)
    m = dict(MEASURES, a={BLK: 400000.0}, b={BLK: 430000.0}, c={BLK: 300000.0})
    out = S.cap_to_region(_row("between", ["a", "b"]), span, m)
    assert "Region 3 and Region 5 at once" in out["region_note"]
    inside = S.cap_to_region(_row("between", [SPUZZUM, "c"]), span, m)
    assert "region_note" not in inside and inside["suggestion"] == _row("between", [SPUZZUM, "c"])["suggestion"]


def test_a_water_in_one_region_is_left_alone():
    one = {s: frozenset({2}) for s in SECTIONS}
    span = S.region_span(2, SECTIONS, one, CUTS_AT)
    row = _row("upstream_of", [MISSION])
    assert S.cap_to_region(row, span, MEASURES) == row
