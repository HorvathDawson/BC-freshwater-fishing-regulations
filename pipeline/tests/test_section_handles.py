"""The handle table's order, which is load-bearing.

A section handle is an index into `section_handles.txt`. Two properties have to hold or the
bundle is quietly wrong, and neither is enforced by anything else:

  * it must be DETERMINISTIC over the set of ids, or the tile and the bundle disagree and a
    lookup lands on the wrong section;
  * it must be in the WATER'S OWN ORDER, because `ORDER BY sid` is now how a river's sections
    are put in sequence. That query used to split and cast the id in SQL, and before THAT it
    ordered the raw text and drew rivers with their stretches interleaved.
"""

from __future__ import annotations

from pathlib import Path

from pipeline.common.section_handles import (assign, digest, id_at, read, sort_key,
                                             write)


def test_measure_sorts_as_a_number_not_as_text():
    """The exact regression: `...:122095` must not come before `...:9942`."""
    got = assign(["354087681:122095", "354087681:9942", "354087681:2"])
    assert got == ["354087681:2", "354087681:9942", "354087681:122095"]


def test_sections_of_one_blue_line_are_contiguous():
    """A run of sections must never be assembled across two waters, so a blue line's
    sections have to be adjacent in handle space."""
    got = assign(["2:0", "1:5", "2:1", "1:0", "2:2", "1:1"])
    lines = [s.split(":")[0] for s in got]
    assert lines == ["1", "1", "1", "2", "2", "2"], got


def test_the_lake_namespace_does_not_collide_with_a_blue_line():
    got = assign(["lake:-5", "100:0", "lake:-10"])
    assert got.index("100:0") < got.index("lake:-10") < got.index("lake:-5")


def test_the_order_depends_only_on_the_set():
    a = assign(["b:1", "a:2", "a:1"])
    b = assign(["a:1", "a:2", "b:1"])
    c = assign(["a:2", "a:1", "b:1", "a:1"])     # duplicates collapse
    assert a == b == c


def test_a_handle_survives_nothing_and_that_is_the_point():
    """Adding ONE section renumbers everything after it. This is asserted, not lamented:
    it is why the digest exists and why a handle may never be persisted (AGENTS rule 5)."""
    before = assign(["1:0", "1:10"])
    after = assign(["1:0", "1:5", "1:10"])
    assert before.index("1:10") == 1 and after.index("1:10") == 2


def test_write_read_round_trips_and_digests_the_content(tmp_path: Path):
    ids = ["1:0", "1:10", "lake:-1"]
    d = write(ids, tmp_path)
    back, by_id = read(tmp_path)
    assert list(back) == assign(ids)
    assert by_id[back[0]] == 1 and by_id[back[-1]] == len(back)   # 1-based; 0 is reserved
    assert d == digest(tmp_path / "section_handles.txt")
    # A different set must produce a different digest, or the vintage check is decoration.
    d2 = write(ids + ["1:5"], tmp_path)
    assert d2 != d


def test_sort_key_tolerates_an_id_it_cannot_parse():
    """Stability beats cleverness: an unexpected shape must not raise mid-build."""
    assert assign(["weird", "1:0"]) == ["1:0", "weird"] or "weird" in assign(["weird", "1:0"])
    sort_key("no-colon-at-all")


def test_zero_is_reserved_so_a_falsy_check_cannot_lose_a_section(tmp_path: Path):
    """The app asks `if (!section)` in ten places. A zero handle is falsy in JavaScript, so a
    zero-based table would make the first section in the province read as "no section" —
    everywhere, silently, with the types all correct. 0 is therefore not a handle."""
    write(["1:0", "1:10", "lake:-1"], tmp_path)
    _, by_id = read(tmp_path)
    assert min(by_id.values()) == 1, "0 must never be issued as a handle"
    assert sorted(by_id.values()) == [1, 2, 3]


def test_a_handle_is_the_line_number(tmp_path: Path):
    """So that `sed -n "${h}p" section_handles.txt` answers "which section is this"."""
    write(["1:0", "1:10", "lake:-1"], tmp_path)
    ids, by_id = read(tmp_path)
    lines = (tmp_path / "section_handles.txt").read_text().splitlines()
    for s, h in by_id.items():
        assert lines[h - 1] == s
        assert id_at(ids, h) == s
