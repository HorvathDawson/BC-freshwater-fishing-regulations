"""T765: a golden part that is closed all year has no label, so taps.py holds it by its END TOKENS (CLOSED_ENDS), and one
run lists every stale declaration (not the first). No data read: the module is loaded by path, its pure functions run on
made-up stores."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TAPS = REPO / "app/tools/regs-recut/taps.py"


@pytest.fixture(scope="module")
def taps():
    spec = importlib.util.spec_from_file_location("regs_recut_taps", TAPS)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_every_closed_golden_state_is_declared_by_its_ends(taps):
    states = json.loads((REPO / "app/fixtures/golden/states.json").read_text())["states"]
    closed = {sid for sid, st in states.items() if taps.closed_state(st)}
    held = closed - set(taps.PART_GONE) - set(taps.PART_TIDAL)
    assert held and held == set(taps.CLOSED_ENDS)


def test_a_moved_index_is_named_with_the_declared_ends_and_where_they_are(taps):
    ends = {7: ("a", "b"), 8: ("b", "c")}
    declared = {"s": ("a", "b")}
    assert taps.ends_problem("s", 7, ends, declared) is None
    msg = taps.ends_problem("s", 8, ends, declared)          # the index moved by one: the NEXT closed part
    assert "s" in msg and "part 8" in msg and "('b', 'c')" in msg and "store part [7]" in msg
    assert "no store part" in taps.ends_problem("s", 8, {8: ("x", "y")}, declared)
    assert "CLOSED_ENDS" in taps.ends_problem("other", 7, ends, declared)


def test_one_run_lists_every_stale_declaration(taps):
    states = {sid: {"sub": "stream · 1 section · closed all year", "part": "0"} for sid in ("x15p9_1010", "x16p7_1010", "x17p5_1010")}
    ends = {sid: {0: taps.CLOSED_ENDS[sid]} for sid in states}
    ends["x15p9_1010"] = {0: ("wrong", "ends")}
    ends["x17p5_1010"] = {0: ("wrong", "ends")}
    done, problems = taps.resolve_all(states, lambda sid, st: (0, None), lambda sid, st: ends[sid])
    assert sorted(done) == ["x16p7_1010"]
    assert len(problems) == 2 and "x15p9_1010" in problems[0] and "x17p5_1010" in problems[1]


def test_a_raising_resolve_does_not_stop_the_others(taps):
    states = {"a": {"sub": "stream · 1 section · From x up to y", "part": "0"}, "b": {"sub": "stream · 1 section · From x up to y", "part": "0"}}

    def one(sid, st):
        raise SystemExit(f"taps: {sid} stale")
    done, problems = taps.resolve_all(states, one, lambda sid, st: None)
    assert problems == ["taps: a stale", "taps: b stale"] and not done


def test_no_comment_cites_the_removed_exclusions_file():
    for p in ("app/tools/regs-recut/taps.py", "app/packages/core/src/regs/testing/golden.ts", "app/packages/core/src/regs/tree.ts"):
        assert "exclusions" not in (REPO / p).read_text(), p
