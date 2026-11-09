"""T517: the parts and the paint can never diverge (`pipeline.deliver.release.paint_sync`). Pure: the check's core over
hand-built rows, each MUTATED once to prove the check sees it."""
from __future__ import annotations

from pipeline.deliver.release.paint_sync import no_access_sids, problems, status_profiles

#: one water (ord 1): part 0 = sections 1, 2; part 1 = 3; part 2 = 4, 5; marks 0|1 and 1|2
PS = [(1, 1, 0), (1, 2, 0), (1, 3, 1), (1, 4, 2), (1, 5, 2)]
PARTS = {(1, 0): 10, (1, 1): 11, (1, 2): 10}
MARKS = [(1, 0, [1]), (1, 1, [2])]
PROFILE = {1: 3, 2: 3, 3: 4, 4: 3, 5: 3}
CHAIN = {1: 7, 2: 7, 3: 7, 4: 7, 5: 7}


def test_a_release_whose_parts_paint_as_one_run_each_passes():
    assert problems(PS, PARTS, MARKS, PROFILE, [], CHAIN) == []


def test_a_section_moved_into_another_part_is_caught():
    """THE MUTATION: section 3 (status profile 4) filed under part 0 — part 0 now paints two ways."""
    ps = [(o, s, 0 if s == 3 else ix) for o, s, ix in PS]
    got = problems(ps, PARTS, MARKS, PROFILE, [], CHAIN)
    assert any("part (1, 0) paints 2 ways" in p for p in got)


def test_a_salmon_chain_or_no_access_change_inside_a_part_is_caught():
    assert any("paints 2 ways" in p for p in problems(PS, PARTS, MARKS, PROFILE, [], {**CHAIN, 2: 8}))
    assert any("paints 2 ways" in p for p in problems(PS, PARTS, MARKS, PROFILE, [5], CHAIN))


def test_a_section_in_two_parts_and_a_part_with_none_are_caught():
    got = problems(PS + [(1, 3, 2)], PARTS, MARKS, PROFILE, [], CHAIN)
    assert any("section 3 is in part" in p for p in got)
    assert any("has no section" in p for p in problems(PS, {**PARTS, (1, 3): 12}, MARKS, PROFILE, [], CHAIN))


def test_two_meeting_parts_with_one_key_and_paint_are_not_maximal():
    """Part 1 given part 0's key and paint: they meet at a mark, so they are one run the build failed to join."""
    got = problems(PS, {**PARTS, (1, 1): 10}, MARKS, {**PROFILE, 3: 3}, [], CHAIN)
    assert any("not one maximal run" in p for p in got)
    # the same pair split by the salmon chain is two runs (T517: the chain is in the key)
    assert problems(PS, {**PARTS, (1, 1): 10}, MARKS, {**PROFILE, 3: 3}, [], {**CHAIN, 3: 9}) == []


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
