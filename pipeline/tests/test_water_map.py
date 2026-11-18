"""T853: the water rules map — water resolution, the system and the part / cascade table, label placement.

The no-data tests read the committed MINI release (`under_test.mini()`: the invented world's whole release, salmon
file included — Fixture River with a tidal mouth, Mock Creek closed above its falls, Stub Creek and the Mockup Creek
that drains into it); the Nass test reads the live release (`needs_bundle`).
"""
from __future__ import annotations

from datetime import date

import pytest

from pipeline.tests import under_test
from pipeline.tools.water_map import model as M
from pipeline.tools.water_map.layout import Placer, overlaps
from pipeline.tools.water_map.release import Release

DAY = date(2026, 11, 18)


@pytest.fixture(scope="module")
def rel():
    r = Release(under_test.mini())
    yield r
    r.close()


@pytest.fixture(scope="module")
def fixture_river(rel):
    w = M.resolve(rel, "Fixture River")
    return w, M.system(rel, w), M.Reader(rel, DAY)


def test_a_water_resolves_by_item_id_name_and_folded_name(rel):
    assert M.resolve(rel, "gnis:990001").name == "Fixture River"
    assert M.resolve(rel, "fixture river").item_id == "gnis:990001"
    assert M.resolve(rel, "  MOCK   creek ").item_id == "gnis:990002"


def test_a_name_matching_several_waters_lists_them(rel):
    with pytest.raises(M.Ambiguous) as e:
        M.resolve(rel, "Lake")
    names = {w.name for w in e.value.choices}
    assert {"Fixture Lake", "Sample Lake", "Twofold Lake"} <= names
    assert "give an item id" in str(e.value.code)


def test_an_unknown_name_is_refused(rel):
    with pytest.raises(M.NotFound):
        M.resolve(rel, "No Such Creek Anywhere")


def test_the_system_is_the_water_and_its_rule_carrying_tributaries(fixture_river):
    w, s, _ = fixture_river
    assert [x.name for x in s.main] == ["Fixture River"]
    tribs = {p.water.name: p.depth for p in s.parts if p.role == "trib"}
    # every tributary by water_exit with a row of its own, at any depth (Mockup Creek drains into Stub Creek)
    assert tribs == {"Mock Creek": 1, "Fixture Lake": 1, "Twofold Lake": 1, "Stub Creek": 1, "Mockup Creek": 2}
    assert not s.left_out
    capped = M.system(rel_of(fixture_river), w, max_tribs=2)
    assert len({p.water.name for p in capped.parts if p.role == "trib"}) == 2 and len(capped.left_out) == 3
    assert any("3 more rule-carrying tributaries" in n for n in capped.notes)


def rel_of(fr):
    return fr[2].rel


def test_freshwater_units_carry_the_book_rows_and_the_apps_answer(fixture_river):
    _, s, r = fixture_river
    units, base = M.units(r, s, "freshwater")
    assert [u.number for u in units] == list(range(1, len(units) + 1))
    mouth = units[0]
    assert mouth.today.state == "tidal" and [t for t, _ in mouth.rows] == ["TODAY"]
    mock = [u for u in units if u.title == "Mock Creek"]
    assert [u.where for u in mock] == ["From the mouth up to the Mock Creek Falls",
                                       "From the Mock Creek Falls up to the top of the river"]
    below, above = mock
    assert below.today.state == "open" and below.today.marks == ("bait_ban",)
    assert below.rows[0] == ("BC", "Own row p.91: Bait ban.")
    assert below.today.words == "Open · bait ban · Keep 4 trout and char today."
    assert above.today.state == "closed_all" and above.today.words == "CLOSED to all fishing (No fishing)."
    stub = next(u for u in units if u.title == "Stub Creek")
    assert stub.today.words == "Open · Release every trout and char today."
    # the region tables, said once, with their pages
    assert [t for t, _ in base if t.startswith("BC")] == ["BC R1", "BC R2"]
    assert any("Region 2 table p.92" in t and "No fishing in streams, Jul 15–Aug 31" in t for _, t in base)


def test_salmon_units_read_the_salmon_file(fixture_river):
    _, s, r = fixture_river
    units, _ = M.units(r, s, "salmon")
    assert units[0].today.state == "tidal"
    # the mini world's DFO feed is empty: every listed water is closed to salmon by the file's own UNLISTED chain
    rest = [u for u in units[1:]]
    assert all(u.today.state in ("closed", "closed_all") for u in rest)
    assert all("closed: chinook, coho, sockeye, pink, chum" in u.today.words for u in rest)
    # a book closure closes all fishing on the salmon map too
    assert {u.title for u in rest if u.today.state == "closed_all"} == {"Mock Creek", "Mockup Creek"}
    # a mode joins a water's parts that read the same: Fixture River's two freshwater parts share one salmon answer
    fr = [u for u in units if u.title == "Fixture River" and u.today.state != "tidal"]
    assert len(fr) == 1 and fr[0].span == "km 1.0–25.0 and km 27.0–46.0"


def test_breaks_are_named_with_their_km(fixture_river):
    _, s, r = fixture_river
    units, _ = M.units(r, s, "freshwater")
    got = {(b.water.name, b.words, b.km) for b in M.breaks(r, s, units)}
    assert ("Mock Creek", "Mock Creek Falls", 6.0) in got
    assert ("Fixture River", "Shore", 1.0) in got


def test_end_words_fold_the_confidence_clause():
    ends = {"t": ["Tidal boundary (DFO Subarea 3-12), approximate location (medium confidence)"],
            "gauge__08DB001": ["Canyon City bridge", "08DB001 · Nass River Above Shumal Creek"]}
    assert M.end_words(ends, "t") == "≈ Tidal boundary (DFO Subarea 3-12)"
    assert M.end_words(ends, "gauge__08DB001") == "Canyon City bridge (gauge 08DB001)"
    assert M.end_words(ends, None) is None


def test_placed_labels_never_overlap_and_stay_in_the_frame():
    pl = Placer((0, 0, 400, 300))
    pl.add_line([(0, 150), (400, 150)])
    got = [b for b in (pl.place(200, 150, 60, 20) for _ in range(12)) if b is not None]
    assert len(got) >= 8
    for i, a in enumerate(got):
        assert 0 <= a[0] and a[2] <= 400 and 0 <= a[1] and a[3] <= 300
        for b in got[i + 1:]:
            assert not overlaps(a, b)
    assert pl.line_cost(got[0]) == 0          # a label avoids sitting on the line when it can


def test_a_leader_may_leave_its_own_marker():
    pl = Placer((0, 0, 400, 300))
    pl.block((195, 145, 205, 155))            # the break's dot the label points at
    assert pl.place(200, 150, 80, 24) is not None


@pytest.mark.needs_bundle
def test_the_nass_reads_as_the_app_holds_it(request):
    with Release(under_test.release(request)) as live:
        w = M.resolve(live, "Nass River")
        s = M.system(live, w)
        r = M.Reader(live, DAY)
        units, base = M.units(r, s, "salmon")
        nass = [u for u in units if u.parts[0].water.item_id == "gnis:3206"]
        assert nass[0].today.state == "tidal"
        assert any(u.today.state == "open" and "Coho" in u.today.words for u in nass)
        names = {u.parts[0].water.name for u in units}
        assert {"Ksi Sii Aks", "Ksi Gingolx", "Meziadin River", "Cranberry River"} <= names
        assert any(tag == "DFO" for tag, _ in base)
