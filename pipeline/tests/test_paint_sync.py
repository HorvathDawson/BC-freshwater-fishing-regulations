"""T517: the parts and the paint can never diverge (`pipeline.deliver.release.paint_sync`). The check's core over
hand-built rows, each MUTATED once to prove the check sees it, and over a fixture bundle's OWN parts (the build's
outputs) with the build's run join switched off."""
from __future__ import annotations

import pytest

from pipeline.deliver.release.paint_sync import no_access_sids, problems, status_profiles

#: one water (ord 1): part 0 = sections 1, 2; part 1 = 3; part 2 = 4, 5; sections touch end to end
PS = [(1, 1, 0), (1, 2, 0), (1, 3, 1), (1, 4, 2), (1, 5, 2)]
PARTS = {(1, 0): 10, (1, 1): 11, (1, 2): 10}
TOUCH = [(1, 2), (2, 3), (3, 4), (4, 5)]
NAMED = [(1, s) for s in (1, 2, 3, 4, 5)]
PROFILE = {1: 3, 2: 3, 3: 4, 4: 3, 5: 3}
CHAIN = {1: 7, 2: 7, 3: 7, 4: 7, 5: 7}


def test_a_release_whose_parts_paint_as_one_run_each_passes():
    assert problems(PS, PARTS, TOUCH, PROFILE, [], CHAIN, NAMED) == []


def test_a_section_moved_into_another_part_is_caught():
    """THE MUTATION: section 3 (status profile 4) filed under part 0 — part 0 now paints two ways."""
    ps = [(o, s, 0 if s == 3 else ix) for o, s, ix in PS]
    got = problems(ps, PARTS, TOUCH, PROFILE, [], CHAIN, NAMED)
    assert any("part (1, 0) paints 2 ways" in p for p in got)


def test_a_salmon_chain_or_no_access_change_inside_a_part_is_caught():
    assert any("paints 2 ways" in p for p in problems(PS, PARTS, TOUCH, PROFILE, [], {**CHAIN, 2: 8}, NAMED))
    assert any("paints 2 ways" in p for p in problems(PS, PARTS, TOUCH, PROFILE, [5], CHAIN, NAMED))


def test_a_section_in_two_parts_and_a_part_with_none_are_caught():
    got = problems(PS + [(1, 3, 2)], PARTS, TOUCH, PROFILE, [], CHAIN, NAMED)
    assert any("section 3 is in part" in p for p in got)
    assert any("has no section" in p for p in problems(PS, {**PARTS, (1, 3): 12}, TOUCH, PROFILE, [], CHAIN, NAMED))


def test_two_meeting_parts_with_one_key_and_paint_are_not_maximal():
    """Part 1 given part 0's key and paint: their sections touch, so they are one run the build failed to join."""
    got = problems(PS, {**PARTS, (1, 1): 10}, TOUCH, {**PROFILE, 3: 3}, [], CHAIN, NAMED)
    assert any("not one maximal run" in p for p in got)
    # the same pair split by the salmon chain is two runs (T517: the chain is in the key)
    assert problems(PS, {**PARTS, (1, 1): 10}, TOUCH, {**PROFILE, 3: 3}, [], {**CHAIN, 3: 9}, NAMED) == []


def test_a_named_section_in_no_part_or_in_a_part_of_another_water_is_caught():
    got = problems(PS, PARTS, TOUCH, PROFILE, [], CHAIN, NAMED + [(1, 6)])
    assert any("section 6 of water ord 1 is in no part" in p for p in got)
    got = problems(PS, PARTS, TOUCH, PROFILE, [], CHAIN, [x for x in NAMED if x != (1, 5)])
    assert any("does not own it" in p for p in got)


def _built(db):
    """The check's inputs from a fixture bundle's own parts: the akey stands for the rule key, the status profile for
    the rule set (one profile per set: what the status index would paint)."""
    ps = db.execute("SELECT ord, sid, part_ix FROM part_section").fetchall()
    parts = {(o, p): k for o, p, k in db.execute("SELECT ord, part_ix, key_ix FROM part")}
    prof = dict(db.execute("SELECT sid, set_id FROM section_ruleset"))
    touch = db.execute("SELECT a, b FROM section_touch").fetchall()
    named = db.execute("SELECT ord, sid FROM item_section").fetchall()
    return ps, parts, touch, prof, [], {}, named


def test_the_build_s_own_parts_pass_and_without_the_run_join_they_fail(monkeypatch):
    """THE MUTATION THROUGH THE BUILD: the braid fixture's parts as `derived` writes them pass; with the build's join
    of touching stretches switched off (`join_touching` -> identity, the side-build defect) the same check names the
    two parts of set 7 that touch — though the build wrote no mark between them, so a check across marks passed."""
    from pipeline.deliver.bundle import derived
    from pipeline.tests.bundle_fixture import finish
    from pipeline.tests.test_derived_tables import _braid
    db = _braid()
    finish(db)
    assert problems(*_built(db)) == []
    monkeypatch.setattr(derived, "join_touching", lambda pieces, touch: pieces)
    db = _braid()
    finish(db)
    assert db.execute("SELECT COUNT(*) FROM part WHERE ord = 1 AND set_id = 7").fetchone() == (2,)
    got = problems(*_built(db))
    assert len(got) == 1 and "touch (sections 3, 5)" in got[0] and "not one maximal run" in got[0], got
    # and a section the build dropped from every part is caught (coverage, not only duplicates)
    db.execute("DELETE FROM part_section WHERE sid = 2")
    assert any("section 2 of water ord 1 is in no part" in p for p in problems(*_built(db)))


def test_the_status_runs_are_read_back_per_section():
    from pipeline.deliver.binary import varint
    b = bytearray(b"BCSI") + bytes([2]) + bytes(16)
    b += varint(2)                                   # two profiles of one run each (366 days)
    for code in (1, 2):
        b += varint(1) + varint(366 << 3 | code)
    b += varint(2) + varint(1) + varint(2) + varint(2) + varint(1) + varint(0) + varint(1)
    assert status_profiles(bytes(b)) == {1: 0, 2: 0, 5: 1}


def test_the_no_access_colour_is_read_by_the_tier_s_name_and_summed():
    """The access half names its tier ("no_access"); a section is the colour over 500 permille, summed over areas
    (Coquitlam: two no-access areas of 300 each) — caught on the T517 side build, where every part read 'not
    no-access' and the check called the T226 breaks non-maximal."""
    acc = {"areas": [("no_access", "A"), ("no_access", "B"), ("permit", "C")],
           "rows": [(1, 0, 1000), (2, 0, 300), (2, 1, 300), (3, 0, 500), (4, 2, 1000)]}
    assert no_access_sids(acc) == {1, 2}


def test_a_lake_on_the_course_named_or_not_is_adjacency():
    """T517 (owner 2026-11-08): river pieces either side of a lake — named or not — with the same rules are one entry,
    so two parts of one key and paint there are not maximal."""
    from pipeline.deliver.release.paint_sync import lake_gaps
    stem = [(1, 1, 0, 100, "mouth", "lake_outlet"), (1, 2, 150, 300, "lake_inlet", "lake_outlet:wbk:9"),
            (1, 3, 400, 500, "lake_inlet:wbk:9", "source"), (1, 4, 500, 600, "length:1", "source")]
    assert lake_gaps(stem) == [(1, 2), (2, 3)]          # 3 -> 4 touch end to end: no lake between
    ps = [(1, 1, 0), (1, 2, 1), (1, 3, 1)]
    parts = {(1, 0): 10, (1, 1): 10}
    named = [(1, 1), (1, 2), (1, 3)]
    got = problems(ps, parts, lake_gaps(stem[:3]), {}, [], {}, named)
    assert len(got) == 1 and "parts (1, 0) and (1, 1) touch" in got[0]
    # MUTATION: different paint either side (the salmon chain) is two parts, as it should be
    assert problems(ps, parts, lake_gaps(stem[:3]), {}, [], {1: 5}, named) == []


@pytest.mark.needs_bundle
def test_a_built_release_passes_both_checks_on_its_shipped_files(request):
    """Review finding 9: `check` (and `name_check.check_release`) on a BUILT release's shipped files — the side set's
    or the live one (UI_EXPORT_DIR / the release root), against its own bundle."""
    from pipeline.deliver.answers.display.name_check import check_release
    from pipeline.deliver.release.paint_sync import check
    from pipeline.tests import under_test
    bundle = under_test.bundle(request)
    rel = under_test.release(request)                 # the candidate during a release (RELEASE_DIR), else LATEST
    check(rel, bundle)
    check_release(rel)
