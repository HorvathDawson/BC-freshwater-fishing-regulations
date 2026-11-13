"""T669 item 1: ONE TABLE OF STRADDLER CASES that both "strictest of two regions" readers run.

Freshwater (RULINGS Z4, `read.effective_rules` step 6): a region's rule is displaced when the other region holds a rule
`ladder.stricter` than it; everything else binds beside it. Salmon (RULINGS Z4a, `deliver.salmon.strictest`): per fish
and day, the stricter verdict, and of two openings the stricter limit (`stricter_opening`), else both regions' words.

Each case states its expected outcome ONCE — "a" (region A's answer alone), "b", or "both" (both regions' rules / words
stand) — and both readers must reach it. Where the two differ ON PURPOSE (T668 audit, "Freshwater (Z4 / T183) vs salmon
straddlers"), the case carries a per-side outcome and the reason, so a change to either side shows here.
No production code is exercised beyond the two readers; nothing here reads data.
"""
from __future__ import annotations

import json

import pytest

from pipeline.deliver import salmon as S
from pipeline.deliver.bundle.read.ladder import stricter

CO = ["CO"]


def q(take, **kw):
    """A freshwater retention rule for coho (`read.rules` shape)."""
    return {"type": "retention_limit", "take": take, "species": CO, **kw}


def opening(n, t, *, over=(), tier="count", kind=None):
    """A DFO opening verdict: a daily count `n`, size tiers `over` = [(cm, k)] (k = 0: none over cm)."""
    kids = [{"node": "tier", "tier": "size", "n": k, "over_cm": cm} for cm, k in over]
    if kind:                                   # a hatchery / wild limit is a `kind` node (`dfo_salmon.quota`)
        kids.append({"node": "kind", "species": CO, "name": kind, "n": n, "says": t, "detail": ""})
    tree = {"node": "tier", "tier": tier, "n": n, "children": kids, "species": CO}
    return {"s": "open", "t": t, "tree": tree}


def fresh(A, B) -> str:
    """Step 6 over two regions' rule sets: a rule stays unless the other region holds one stricter than it; two
    regions' identical statements show once (RU-8). "a" / "b": what binds is exactly that region's set."""
    keep = [x for x in A if not any(stricter(y, x) for y in B)] + [y for y in B if not any(stricter(x, y) for x in A)]
    got = {json.dumps(x, sort_keys=True) for x in keep}
    as_set = lambda xs: {json.dumps(x, sort_keys=True) for x in xs}       # noqa: E731
    if got == as_set(A):
        return "a"
    if got == as_set(B):
        return "b"
    return "both"


def salmon(a, b) -> str:
    got = S.strictest([a, b])
    if got is a or got == a and got != b:
        return "a"
    if got is b or got == b:
        return "b"
    return "both"


#: (case, freshwater A rules, freshwater B rules, salmon A verdict, salmon B verdict, expected | {"fresh", "salmon", "why"})
CASES = [
    ("closure beats an opening",
     [q(0, may_target=False)], [q(2)], {"s": "closed", "t": "no fishing"}, opening(2, "2 a day"), "a"),
    ("an outright release beats an opening",
     [q(0)], [q(2)], {"s": "release", "t": "catch and release"}, opening(2, "2 a day"), "a"),
    ("of two counts, the lower",
     [q(4)], [q(2)], opening(4, "4 a day"), opening(2, "2 a day"), "b"),
    ("of two caps on the same size bound, the lower",
     [q(4), q(2, lengths=[{"min_cm": 50}])], [q(4), q(1, lengths=[{"min_cm": 50}])],
     opening(4, "4, only 2 over 50", over=[(50, 2)]), opening(4, "4, only 1 over 50", over=[(50, 1)]), "b"),
    ("a lower count against a size ceiling: both bind",
     [q(2)], [q(4), q(0, lengths=[{"min_cm": 50, "take": 0}])],
     opening(2, "2 a day"), opening(4, "4 a day, none over 50 cm", over=[(50, 0)]), "both"),
    ("caps on two different size bounds: both bind",
     [q(4), q(1, lengths=[{"min_cm": 50}])], [q(4), q(0, lengths=[{"min_cm": 60, "take": 0}])],
     opening(4, "4, only 1 over 50", over=[(50, 1)]), opening(4, "4, none over 60", over=[(60, 0)]), "both"),
    ("hatchery quota against a wild release: both (neither holds the other's fish)",
     [q(2, origin="hatchery")], [q(0, origin="wild")],
     opening(2, "2 hatchery a day", kind="hatchery"), opening(0, "wild: release", kind="wild"), "both"),
    ("a pooled count against a plain count",
     [q(4, whose="aggregate")], [q(2)],
     opening(4, "4 a day, all salmon combined", tier="aggregate"), opening(2, "2 a day"),
     {"fresh": "b", "salmon": "both",
      "why": "salmon turns no pooled tree into caps (T669 item 3): both regions' words, no number"}),
    ("gear: never ranked, both apply",
     [{"type": "gear_restriction", "gear": "barbless"}], [{"type": "gear_restriction", "gear": "single hook"}],
     None, None, "both"),
    ("hours: a tighter window",
     [{"type": "time_restriction", "when": "sunrise-sunset"}], [{"type": "time_restriction", "when": "1h after sunrise"}],
     None, None,
     {"fresh": "both", "salmon": "b",
      "why": "only the salmon header has hours to compare (`tighter_hours`); freshwater hours ride each rule's when"}),
    ("a region with nothing to say",
     [q(2)], [], opening(2, "2 a day"), None,
     {"fresh": "a", "salmon": "b",
      "why": "owner 2026-10-27: a DFO region that opens nothing is closed (`NOT_LISTED`)"}),
]


def _salmon_side(case):
    name, _A, _B, va, vb, _want = case
    if name.startswith("gear"):
        lines = S.both_gear([["barbless"], ["single hook"]])
        return "both" if lines == ("barbless", "single hook") else "a"
    if name.startswith("hours"):
        sun = lambda off: {"solar": "sunrise", "offset_min": off}        # noqa: E731
        set_ = {"solar": "sunset", "offset_min": 0}
        got = S.tighter_hours({"A": {"hours": {"start": sun(0), "end": set_}}, "B": {"hours": {"start": sun(60), "end": set_}}})
        return {("A",): "a", ("B",): "b"}.get(tuple(got), "both")
    if vb is None:
        got = S.strictest([va, None])
        return "a" if got is va else "b" if got == S.NOT_LISTED else "both"
    return salmon(va, vb)


@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_both_readers_reach_the_cases_stated_outcome(case):
    name, A, B, _va, _vb, want = case
    fw = want["fresh"] if isinstance(want, dict) else want
    sw = want["salmon"] if isinstance(want, dict) else want
    assert fresh(A, B) == fw, f"freshwater: {name}"
    assert _salmon_side(case) == sw, f"salmon: {name}"


def test_only_the_stated_cases_differ_on_purpose():
    """The differences are the T668 audit's, each with its reason; a new one must be stated, never slip in."""
    differ = {c[0]: c[5]["why"] for c in CASES if isinstance(c[5], dict)}
    assert sorted(differ) == ["a pooled count against a plain count", "a region with nothing to say",
                              "hours: a tighter window"]
    assert all(differ.values())


def test_mutation_a_reader_that_never_displaces_fails_the_table(monkeypatch):
    """MUTATION: with `stricter` answering no for every pair, the freshwater side reads "both" where the table says one
    region's rule speaks (closure, release, the lower count) — the table catches it."""
    import sys
    monkeypatch.setattr(sys.modules[__name__], "stricter", lambda a, b: False)
    wrong = [c[0] for c in CASES if not isinstance(c[5], dict) and c[5] != "both" and fresh(c[1], c[2]) != c[5]]
    assert len(wrong) >= 4
