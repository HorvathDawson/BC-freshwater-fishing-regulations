"""T1199 PR 2 (RULINGS E36; owner 2026-11-28, shown the Beaverfoot River's two open stretches beside Yoho that read
"within Yoho National Park" since the cut rule: "do the recommended the app should auto join them later i think assuming
same rules"; then "nvm it wont but that is okay"). A stretch between two crossings of one area's edge is called
"Within <area>" only when the part carries the area's rule (the cut puts it in the area: the bundle's `part_area_held`);
otherwise "Outside <area> (between its boundaries)" (`part.OUTSIDE_BETWEEN`, his recommended words). Every area kind the
end words name; the answers step refuses a part whose words disagree with its rule, both ways, by name."""
import sqlite3

import pytest

from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.display import part as P
from pipeline.deliver.answers.display.builders import held_facts, within_disagreements
from pipeline.deliver.bundle import side_channels as sc

YOHO = "Yoho National Park of Canada"
DOC = {"waters": {}, "splits": {}}


def _run(lo_km, hi_km, frm=f"area:{YOHO}", to=f"area:{YOHO}"):
    return {"from": frm, "to": to, "km_from": hi_km, "km_to": lo_km}


# the Beaverfoot River after the cut rule: the open group (p1 12.1-12.3, p3 13.3-14.3, p5 16.7 up to the source)
OPEN = [_run(12.1, 12.3), _run(13.3, 14.3), _run(16.7, 34.8, frm="source")]
# the closed group (p0 the mouth up to 12.1, p2 12.3-13.3, p4 14.3-16.7)
CLOSED = [_run(0.0, 12.1, to="mouth"), _run(12.3, 13.3), _run(14.3, 16.7)]


def test_a_stretch_between_two_crossings_is_within_only_when_it_carries_the_areas_rule():
    r = _run(12.1, 12.3)
    assert P._run_txt(DOC, r, [YOHO]) == f"Within {YOHO}"
    assert P._run_txt(DOC, r, []) == f"Outside {YOHO} (between its boundaries)"
    assert P._run_txt(DOC, r, ["SOME OTHER PARK"]) == f"Outside {YOHO} (between its boundaries)"


def test_no_facts_no_words():
    with pytest.raises(AnswersError, match="whether it lies inside is not given"):
        P._run_txt(DOC, _run(12.1, 12.3))


def test_the_beaverfoots_two_groups_read_as_he_picked():
    assert P.runs_label(DOC, {"runs": [dict(r) for r in OPEN], P.AREA_HELD: []}) == \
        f"3 stretches, including outside {YOHO} (between its boundaries)"
    # the closed group's own stretches between crossings stay "within"; its label leads with its lowest stretch
    assert P.runs_label(DOC, {"runs": [dict(r) for r in CLOSED], P.AREA_HELD: [YOHO]}) == \
        f"3 stretches, including from the mouth up to {YOHO}"
    assert P._run_txt(DOC, CLOSED[1], [YOHO]) == f"Within {YOHO}"


def test_the_words_are_one_constant():
    assert P.OUTSIDE_BETWEEN == "Outside {area} (between its boundaries)"


def test_the_answers_step_refuses_words_that_disagree_with_the_rule_both_ways():
    # the mutation pins: the pre-PR words on an open part, and "outside" on a part that carries the rule
    open_part = {"runs": [_run(12.1, 12.3)], P.AREA_HELD: []}
    closed_part = {"runs": [_run(12.3, 13.3)], P.AREA_HELD: [YOHO]}
    assert within_disagreements("gnis:11132", [open_part], [f"3 stretches, including within {YOHO}"], DOC) == \
        [f"gnis:11132 part 0: says within '{YOHO}' but does not carry its rule"]
    assert within_disagreements("gnis:11132", [closed_part],
                                [f"Outside {YOHO} (between its boundaries)"], DOC) == \
        [f"gnis:11132 part 0: says outside '{YOHO}' but carries its rule"]
    assert within_disagreements("gnis:11132", [open_part, closed_part],
                                [P.runs_label(DOC, open_part), P.runs_label(DOC, closed_part)], DOC) == []


def test_held_facts_set_every_part():
    w = {"parts": [{"runs": []}, {"runs": []}, None]}
    got = held_facts("gnis:1", w, {("gnis:1", "held", 1): {YOHO}})
    assert [p and p[P.AREA_HELD] for p in got["parts"]] == [[], [YOHO], None]


def test_the_bundle_writes_the_areas_each_part_carries():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE part (ord INTEGER, part_ix INTEGER, key_ix INTEGER);
        CREATE TABLE part_section (ord INTEGER, sid INTEGER, part_ix INTEGER);
        CREATE TABLE part_area_held (ord INTEGER, part_ix INTEGER, area TEXT);
        INSERT INTO part VALUES (1, 0, 1), (1, 1, 2), (1, 2, NULL);
        INSERT INTO part_section VALUES (1, 10, 0), (1, 11, 0), (1, 12, 1), (1, 13, 2);
    """)
    holds = {YOHO: frozenset({10, 11, 13}), "PARTLY": frozenset({10})}
    # part 0 carries Yoho (every section held), never the area holding one of its two; part 1 nothing; part 2 is no
    # listed part (no rules)
    assert sc.area_held_rows(db, holds) == [(1, 0, YOHO)]


def test_cut_area_holds_reads_every_cut_area_but_regions(tmp_path):
    from pipeline.tests.test_t1140_inside_area import _catalog
    from shapely.geometry import box
    _catalog(tmp_path, [(YOHO, "national_parks", box(0, 0, 1, 1)), ("Sign Zone", "sign_zone", box(0, 0, 1, 1)),
                        ("Region 4", "region", box(0, 0, 1, 1)), ("A PARK", "park", box(0, 0, 1, 1), False)])
    items = [{"id": "a0", "section_ids": ["1:0"]}, {"id": "a1", "section_ids": ["2:0"]}, {"id": "a2", "section_ids": ["3:0"]}]
    got = sc.cut_area_holds(items, {"1:0": 1, "2:0": 2, "3:0": 3}, tmp_path)
    assert got == {YOHO: frozenset({1}), "Sign Zone": frozenset({2})}


def test_on_the_mini_chain_the_stretch_within_restricted_land_carries_it(tmp_path_factory):
    """On the built mini world: Mock Range is restricted land the atlas cuts at; the creek's middle part between its two
    crossings carries it (part_area_held) and its words say "Within", the parts below and above do not carry it."""
    import json
    from pipeline.deliver.release.manifest import latest
    from pipeline.tests import mini_chain
    from pipeline.tools.audit import answers_diff as AD
    b = mini_chain.built(tmp_path_factory)
    db = sqlite3.connect(str(b / "deliver" / "bundle.sqlite"))
    held = {(iid, p, a) for iid, p, a in db.execute(
        "SELECT i.item_id, h.part_ix, h.area FROM part_area_held h JOIN item i ON i.ord = h.ord")}
    db.close()
    mock = {x for x in held if x[0] == "gnis:990004"}
    assert {(p, a.split(" [")[0]) for _i, p, a in mock} == {(1, "Mock Range")}
    rel = AD.Release(latest(b / "release"))
    parts = rel.wire["sections"]["display"]["waters"]["gnis:990004"]["parts"]
    assert parts[1]["label"].startswith("Within Mock Range")
    assert all("Outside" not in (p or {}).get("label", "") for p in parts), json.dumps(parts)[:300]
