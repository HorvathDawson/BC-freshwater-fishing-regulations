"""T801 item 5(1) (owner 2026-11-16, the Skeena licensing check): the "Mainstem only" place shows on every part the
Skeena's "(mainstem only)" row speaks on. Its RULES bind only the two weekend zones, so the place (`part_place`) said
it on those parts alone (11 and 14-18 of 22); its two Class II designations (Skeena River 2, Section 4) cover the rest
of their main stem. A row whose place is its printed scope now speaks wherever its licensing covers a part's main
stem, once one of its rules speaks on the water. A region place is where a part IS and is read from the rules alone
(the Stellako). A part of side channels alone takes nothing from a designation (the Reserve side channel; item 5(2),
its Skeena River 2 unit, the owner dropped 2026-11-20).

Province-wide on the live export (r-6258ffded676, 19,766 waters): three Skeena part groups gain "Mainstem only",
nothing else moves (lane L2 2026-11-20)."""
from __future__ import annotations

from pipeline.deliver.answers.display.builders import _by_grp
from pipeline.deliver.answers.display.part import Water, part_place
from pipeline.deliver.export.read import group_runs

MAIN = "r6:skeena_river@6-10"
STEM = "r6:skeena_river_mainstem_only@6-10"


def _doc(*, zone_rule_on: int = 1, region_row: bool = False) -> dict:
    """Four parts mouth upward: 0 no unit, 1 a weekend zone (the stem row's rule) + Section 4, 2 Section 4 only, 3 a
    side channel alone carrying Section 4. Every part has the main row's rule (rank 0)."""
    rules = {f"{MAIN}::r1": {"entry_id": MAIN, "provenance": {"rank": 0}, "label": "Bait ban"},
             f"{STEM}::r5": {"entry_id": STEM, "provenance": {"rank": 0}, "label": "Weekend zone"}}
    entries = {MAIN: {"kind": "water", "full_name": "SKEENA RIVER", "scope_note": None},
               STEM: {"kind": "water", "full_name": "SKEENA RIVER (mainstem only)", "scope_note": None}}
    if region_row:                     # the stem row as a plain row of another region: no scope words
        entries[STEM] = {"kind": "water", "full_name": "SKEENA RIVER", "scope_note": None}
    sets = {"0": {"reach": [f"{MAIN}::r1"]}, "1": {"reach": [f"{MAIN}::r1", f"{STEM}::r5"]}}
    lic = {f"{STEM}#s4": {"entry_id": STEM, "kind": "designation"},
           "zp:steelhead#t": {"entry_id": "zp:steelhead", "kind": "requirement"}}
    lsets = {"0": {"reach": ["zp:steelhead#t"]}, "1": {"reach": [f"{STEM}#s4", "zp:steelhead#t"]}}
    stem = [{"from": "a", "to": "b", "km_from": 1.0, "km_to": 0.0}]
    side = [{"from": "a", "to": "b", "km_from": None, "km_to": None, "branch": True}]
    parts = [{"ruleset": "0", "licensing_set": "0", "sections": 9, "runs": stem, "grp": 0},
             {"ruleset": "1" if zone_rule_on == 1 else "0", "licensing_set": "1", "sections": 8, "runs": stem,
              "grp": 1},
             {"ruleset": "0", "licensing_set": "1", "sections": 7, "runs": stem, "grp": 2},
             {"ruleset": "0", "licensing_set": "1", "sections": 6, "runs": side, "grp": 3}]
    return {"entries": entries, "rules": rules, "rulesets": sets, "licensing": lic, "licensing_sets": lsets,
            "waters": {"gnis:2936": {"parts": parts}}}


def _places(doc: dict) -> list[str]:
    """Each part's place, in the export's part order (the page sorts by sections: here the same order)."""
    W = Water("gnis:2936", doc["waters"]["gnis:2936"], doc)
    return [part_place(W, i) for i in range(len(W.parts))]


def test_the_stem_rows_place_follows_its_designation_on_the_main_stem():
    assert _places(_doc()) == ["", "Mainstem only", "Mainstem only", ""]


def test_a_side_channel_alone_takes_no_place_from_a_designation():
    # mutation: the side channel given a main-stem run takes the place
    doc = _doc()
    doc["waters"]["gnis:2936"]["parts"][3]["runs"] = [{"from": "a", "to": "b", "km_from": 2.0, "km_to": 1.0}]
    assert _places(doc)[3] == "Mainstem only"
    assert _places(_doc())[3] == ""


def test_a_row_that_speaks_by_no_rule_on_the_water_places_nothing():
    # the stem row's rule removed: no water gains a place from licensing alone
    assert _places(_doc(zone_rule_on=0)) == ["", "", "", ""]


def test_a_region_place_is_read_from_the_rules_alone():
    # a row with no scope words would say "In Region 6": its designation never moves a part's region (the Stellako)
    assert _places(_doc(region_row=True)) == ["", "", "", ""]


def test_a_rule_set_group_takes_the_place_only_where_every_main_stem_piece_is_covered():
    doc = _doc()
    ps = doc["waters"]["gnis:2936"]["parts"]
    ps[0]["grp"], ps[2]["grp"], ps[3]["grp"] = 0, 0, 2       # part 2 joins the unit-less part 0's group
    doc["waters"]["gnis:2936"]["grp_runs"] = group_runs(ps)  # (T679 #17) the export joins a group's runs once
    Wg, _ = _by_grp("gnis:2936", doc["waters"]["gnis:2936"], doc)
    got = [part_place(Wg, k) for k in range(len(Wg.parts))]
    assert got == ["", "Mainstem only", ""]                  # group 0 = parts 0 + 2: part 0 has no Section 4
    ps[0]["licensing_set"] = "1"                              # mutation: part 0 covered too -> the group takes it
    Wg, _ = _by_grp("gnis:2936", doc["waters"]["gnis:2936"], doc)
    assert part_place(Wg, 0) == "Mainstem only"
