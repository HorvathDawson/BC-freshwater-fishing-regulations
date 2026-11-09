"""T517 (owner 2026-11-09: fail fast): the part-name check (`answers.display.name_check`) — a clean water passes, and
each rule's bad case PLANTED once is caught."""
from __future__ import annotations

import pytest

from pipeline.deliver.answers.display.name_check import NameCheckError, check, problems

ENDS = {"mouth": ["Mouth"], "source": ["Source", "to:the source"], "x__falls": ["Falls"],
        "bc_border@near:place:Osoyoos": ["B.C. border near Osoyoos"], "bc_border@km72.1": ["B.C. border (km 72.1)"],
        "gauge__08FC003": ["08FC003 · Dean River Below Tanswanket Creek"],
        "area:X": ["X Ecological Reserve boundary"], "falls_2": ["Falls (2)"], "within": ["Within X"]}
GOOD = [("gnis:1", "One River", [{"lo": "mouth", "hi": "x__falls"}, {"lo": "x__falls", "hi": "source"}, None,
                                 {"lo": None, "hi": None}]),       # its one part with no main stem: the water's name
        ("gnis:2", "Two Creek", [{"lo": "bc_border@near:place:Osoyoos", "hi": "bc_border@km72.1"}])]


def _plant(i: int, j: int, **f) -> list:
    out = [(w, n, [dict(p) if p else p for p in ps]) for w, n, ps in GOOD]
    out[i][2][j].update(f)
    return out


def test_a_clean_release_passes():
    assert problems(GOOD, ENDS) == []
    check(GOOD, ENDS)


@pytest.mark.parametrize("plant, rule", [
    (dict(lo=None), "missing"),                                   # one end token only
    (dict(lo="nowhere"), "no words"),                             # a token display.ends lacks
    (dict(lo="x__falls", hi="source"), "twice"),                  # part 0 says part 1's name
    (dict(lo=None, hi=None), "twice"),                            # a second part saying the water's name
    (dict(hi="falls_2"), "numbered"),
    (dict(hi="gauge__08FC003"), "gauge"),
    (dict(hi="within"), "within"),
])
def test_each_bad_name_planted_is_caught(plant, rule):
    bad = problems(_plant(0, 0, **plant), ENDS)
    assert any(b.startswith(rule + ":") for b in bad), bad
    with pytest.raises(NameCheckError):
        check(_plant(0, 0, **plant), ENDS)


def test_the_lower_end_says_its_to_words():
    """`source` as a LOWER end reads "the source" (its `to:` words), as the app's `spanName` says it."""
    w = [("gnis:3", "Three River", [{"lo": "source", "hi": "mouth"}])]
    assert problems(w, ENDS) == []
    from pipeline.deliver.answers.display.name_check import words
    assert words(ENDS, "source", "lo") == "the source" and words(ENDS, "source", "hi") == "Source"


def test_the_python_composer_says_what_the_app_says():
    """T517 review finding 8: ONE fixture (app/packages/core/src/ends.parity.json) — the app's `spanName`
    (ends.parity.test.ts) and this module's composer must give every case the same name."""
    import json
    from pipeline.common.curated import REPO_ROOT
    from pipeline.deliver.answers.display.name_check import words
    fx = json.loads((REPO_ROOT / "app/packages/core/src/ends.parity.json").read_text())
    for c in fx["cases"]:
        hi = c.get("last_hi") or c["hi"]
        if c["lo"] is None:
            got = c["water"]
        elif c["lo"] == hi and c["lo"].startswith("near:"):
            w = words(fx["ends"], hi, "hi")
            got = f"{c['water']} {w[:1].lower() + w[1:]}"
        else:
            got = f"{words(fx['ends'], c['lo'], 'lo')} → {words(fx['ends'], hi, 'hi')}"
        assert got == c["name"], c


def test_the_run_rules_catch_a_run_not_joined_by_marks_and_one_with_no_end():
    """Review findings 10 and 11: the pieces of one (grp, run) must be joined by marks, and the run's first piece has a
    lower end and its last an upper end (its joined name)."""
    from pipeline.deliver.answers.display.name_check import run_problems
    parts = [(1, 0, 0, 0, "mouth", "x"), (1, 1, 0, 0, "x", "y"), (1, 2, 0, 0, "y", "source"), (1, 3, 1, 0, None, None)]
    marks = [(1, 0, [1]), (1, 1, [2])]
    assert run_problems(parts, marks) == []
    assert any(b.startswith("run gap") for b in run_problems(parts, [(1, 0, [1])]))          # MUTATION: a mark lost
    bad = [p if p[1] != 2 else (1, 2, 0, 0, "y", None) for p in parts]                    # the last piece: no top
    assert any(b.startswith("run ends") for b in run_problems(bad, marks))


def test_a_mark_ships_the_end_token_its_parts_meet_at():
    """Review finding 2: a mark ships an END TOKEN (named by the app through `display.ends`), never stored words."""
    from pipeline.deliver.release.build import mark_token
    assert mark_token("x__falls", "x__falls") == "x__falls"                     # the place both parts end at
    assert mark_token("bc_border", None, up_keyed=False) == "bc_border"        # an unkeyed (outside B.C.) side
    assert mark_token(None, "x__tidal", down_keyed=False) == "x__tidal"        # an unkeyed (tidal) side
    assert mark_token("lake_outlet:wbk:1", "lake_inlet:wbk:1") == "lake_inlet:wbk:1"  # across a lake: the lake
    assert mark_token("x__signs", "x__fence") == "x__signs"                    # two cuts at one place: the lower top
    assert mark_token("x__falls", "source", up_side=True) is None              # a side channel meeting its river
