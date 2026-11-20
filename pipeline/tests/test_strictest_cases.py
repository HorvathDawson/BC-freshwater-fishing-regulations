"""T669 item 1: ONE TABLE OF STRADDLER CASES that both "strictest of two regions" readers run.

Freshwater (RULINGS Z4, `read.effective_rules` step 6): a region's rule is displaced when the other region holds a rule
`ladder.stricter` than it; everything else binds beside it. Salmon (RULINGS Z4a, `deliver.salmon.strictest`): per fish
and day, the stricter verdict, and of two openings the stricter limit (`stricter_opening`), else (T669 item 3) one quota
tree of both regions' caps that bind — pooled, hatchery-marked and unmarked shares included — and both regions' words
only where a number cannot be read. The openings' trees are built by the release's builder (`quota._one`).

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
from pipeline.regs.dfo_salmon import quota as Q

CO = ["CO"]


def q(take, **kw):
    """A freshwater retention rule for coho (`read.rules` shape)."""
    return {"type": "retention_limit", "take": take, "species": CO, **kw}


#: Region 5B's pool (`quota.pool_of`): "in the aggregate (combined total), in Management Units 5-6 to 5-11"
POOL = "area:mu_group:management_units_5_6_to_5_11"


def opening(n, t, *, over=(), ceil=None, hatchery=False, wild=None, pooled=None, monthly=()):
    """A DFO opening verdict for coho, its quota tree built by the release's own builder (`quota._one`): a daily count
    `n`, "only k over cm" for `over` = [(cm, k)], "none over `ceil` cm", hatchery-marked only, "`wild` of which can be
    unmarked", a count pooled across an area (`pooled`), a monthly cap [(cm, k)]."""
    rule = {"type": "retention_limit", "take": n, "species": CO, **({"origin": "hatchery"} if hatchery else {}),
            **({"pooled_across": [pooled]} if pooled else {})}
    subs = [{"take": k, "lengths": [{"min_cm": cm}]} for cm, k in over]
    subs += [{"take": k, "lengths": [{"min_cm": cm}], "period": "monthly"} for cm, k in monthly]
    if ceil is not None:
        subs.append({"take": 0, "lengths": [{"min_cm": ceil, "take": 0}]})
    if wild is not None:
        subs.append({"origin": "wild", "take": wild})
    return {"s": "open", "t": t, **Q._one(rule, subs)}


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
     opening(2, "2 a day"), opening(4, "4 a day, none over 50 cm", ceil=50), "both"),
    ("caps on two different size bounds: both bind",
     [q(4), q(1, lengths=[{"min_cm": 50}])], [q(4), q(0, lengths=[{"min_cm": 60, "take": 0}])],
     opening(4, "4, only 1 over 50", over=[(50, 1)]), opening(4, "4, none over 60", ceil=60), "both"),
    ("hatchery-marked only against a lower count: both bind (neither holds the other's fish)",
     [q(2, origin="hatchery")], [q(1)],
     opening(2, "2 hatchery marked a day", hatchery=True), opening(1, "1 a day"), "both"),
    ("an unmarked share against hatchery-marked only: no unmarked fish",
     [q(2), q(1, origin="wild")], [q(2), q(0, origin="wild")],
     opening(2, "2 a day, 1 of which can be unmarked", wild=1), opening(2, "2 hatchery marked a day", hatchery=True),
     "b"),
    ("a pool at or under a plain count: the pool (it holds every fish the count holds)",
     [q(2, whose="aggregate")], [q(4)],
     opening(2, "2 a day in the aggregate", pooled=POOL), opening(4, "4 a day"), "a"),
    ("a pooled count over a lower plain count",
     [q(4, whose="aggregate")], [q(2)],
     opening(4, "4 a day in the aggregate", pooled=POOL), opening(2, "2 a day"),
     {"fresh": "b", "salmon": "both",
      "why": "a DFO pool counts every water of its set (`whose`), a plain count this water alone: a narrower cap never "
             "displaces a wider (`ladder.A_NARROWER_CAP_NEVER_DISPLACES_A_WIDER`, over waters), so both bind — the "
             "pool over the count (T669 item 3); no freshwater quota is pooled over waters (`statement` reads no "
             "`whose`)"}),
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
    assert sorted(differ) == ["a pooled count over a lower plain count", "a region with nothing to say",
                              "hours: a tighter window"]
    assert all(differ.values())


#: (T669 item 3) where both regions' caps bind, ONE quota tree of them: the card's line (`quota.words`, the app's
#: `quotaLine` reads the same tree), never both regions' words with no number
MIXED = {
    "a lower count against a size ceiling: both bind": "2 a day · none over 50 cm",
    "caps on two different size bounds: both bind": "4 a day · only 1 over 50 cm · none over 60 cm",
    "hatchery-marked only against a lower count: both bind (neither holds the other's fish)":
        "1 a day, hatchery-marked only",
    "a pooled count over a lower plain count":
        "Up to 4 coho a day in total, all waters of Management Units 5-6 to 5-11 combined · 2 a day",
}


def test_t669_item3_a_mix_of_both_regions_caps_shows_numbers():
    """Every salmon "both" case of the table (bar gear) is one tree of the caps that bind — numbers, with both regions'
    words kept in `t` — never `u`. Either fold order reads the same numbers."""
    by = {c[0]: c for c in CASES}
    assert sorted(MIXED) == sorted(n for n, *_, w in CASES if (w["salmon"] if isinstance(w, dict) else w) == "both"
                                   and by[n][3] is not None)
    for name, want in MIXED.items():
        _n, _A, _B, va, vb, _w = by[name]
        for got in (S.strictest([va, vb]), S.strictest([vb, va])):
            assert "u" not in got and got["s"] == "open", name
            assert Q.words(got["tree"]) == want, name
        assert S.strictest([va, vb])["t"] == f"{va['t']}; {vb['t']}"


def test_t669_item3_every_single_region_tree_reads_back_to_itself():
    """`_caps` -> `_tree_of` is the identity on every tree shape the release builds (`quota._one`): a merged tree is
    drawn exactly as a DFO row with those numbers would be. (On r-6258ffded676 all 111 opening trees read back.)"""
    shapes = [opening(4, "x"), opening(2, "x", over=[(77, 1)]), opening(2, "x", hatchery=True),
              opening(1, "x", ceil=35), opening(2, "x", wild=1), opening(4, "x", hatchery=True, over=[(35, 2)]),
              opening(4, "x", pooled=POOL, over=[(50, 1)]), opening(4, "x", monthly=[(65, 1)]),
              opening(2, "x", wild=1, ceil=50, over=[(40, 1)])]
    for v in shapes:
        assert S._tree_of(S._moot(S._caps(v))) == v["tree"], Q.words(v["tree"])


def test_t669_item3_what_is_not_one_tree_keeps_both_regions_words():
    """An unread number, or two pools over different waters, is not one count: both regions' words, no number."""
    two = opening(2, "2 a day")
    assert S.strictest([two, {"s": "open", "t": "see notice", "u": 1}]) == {"s": "open", "t": "2 a day; see notice",
                                                                            "u": 1}
    other = opening(3, "3 in the aggregate", pooled="area:mu_group:management_units_3_1_to_3_5")
    assert S.strictest([opening(4, "4 in the aggregate", pooled=POOL), other])["u"] == 1


def test_t669_item3_the_live_5b_6_straddler():
    """r-6258ffded676 chain 300 (Region 5B's region section beside Region 6 E/A): Region 5B pools 4 a day over MUs
    5-6 to 5-11, Region 6 opens 4 a day. Chinook "only 1 over 50 cm" (5B) against "only 1 over 65 cm" (6): 5B's own
    opening (1 over 50 holds 1 over 65). Coho "only 2 over 50 cm" (5B) against "only 1 over 50 cm" (6): 5B's pool with
    Region 6's size cap — before item 3 both regions' words with no number."""
    pool = lambda sp, k: {"s": "open", "t": f"4 per day, only {k} over 50 cm (Apr 1–Mar 31)",      # noqa: E731
                          **Q._one({"take": 4, "species": [sp], "pooled_across": [POOL]},
                                   [{"take": k, "lengths": [{"min_cm": 50}]}])}
    four = lambda sp, k, cm: {"s": "open", "t": f"4 per day, only {k} over {cm} cm (Apr 1–Mar 31)",  # noqa: E731
                              **Q._one({"take": 4, "species": [sp]}, [{"take": k, "lengths": [{"min_cm": cm}]}])}
    ch = pool("CH", 1)
    assert S.strictest([ch, four("CH", 1, 65)]) is ch
    got = S.strictest([pool("CO", 2), four("CO", 1, 50)])
    assert "u" not in got and Q.words(got["tree"]) == \
        "Up to 4 coho a day in total, all waters of Management Units 5-6 to 5-11 combined · only 1 over 50 cm"


def test_mutation_without_the_merged_tree_a_mix_is_words_again(monkeypatch):
    """MUTATION: `_tree_of` answering None puts every mixed case back to both regions' words with no number."""
    monkeypatch.setattr(S, "_tree_of", lambda caps: None)
    by = {c[0]: c for c in CASES}
    assert all(S.strictest([by[n][3], by[n][4]]).get("u") == 1 for n in MIXED)


def test_mutation_a_reader_that_never_displaces_fails_the_table(monkeypatch):
    """MUTATION: with `stricter` answering no for every pair, the freshwater side reads "both" where the table says one
    region's rule speaks (closure, release, the lower count) — the table catches it."""
    import sys
    monkeypatch.setattr(sys.modules[__name__], "stricter", lambda a, b: False)
    wrong = [c[0] for c in CASES if not isinstance(c[5], dict) and c[5] != "both" and fresh(c[1], c[2]) != c[5]]
    assert len(wrong) >= 4


def test_t669_name_the_region_on_a_mixed_line():
    """(owner 2026-11-20 "Name the region") a MIXED line — numbers from both regions — says each number's region; a line
    taken whole from one region stays DFO's words. Qualcho Creek (5B pool + Region 6's size cap). A third region folds
    in with the labels read back. MUTATION: `_name_regions` a no-op — the coho line names no region and this fails."""
    co = opening(4, "4 per day, only 2 over 50 cm", pooled=POOL, over=[(50, 2)])
    six = opening(4, "4 per day, only 1 over 50 cm", over=[(50, 1)])
    regions = ["Region 5B", "Region 6"]
    got = S.strictest([co, six], regions)
    assert Q.words(got["tree"]) == ("Up to 4 coho a day in total, all waters of Management Units 5-6 to 5-11 combined "
                                    "(Region 5B) · only 1 over 50 cm (Region 6)")
    own = opening(4, "4 per day, only 1 over 50 cm", pooled=POOL, over=[(50, 1)])
    assert S.strictest([own, opening(4, "4, only 1 over 65", over=[(65, 1)])], regions) is own
    assert "Region" not in Q.words(S.strictest([co, six])["tree"])          # no regions given: no names
    three = S.strictest([co, six, opening(3, "3 a day")], [*regions, "Region 7"])
    assert Q.words(three["tree"]) == ("Up to 4 coho a day in total, all waters of Management Units 5-6 to 5-11 combined "
                                      "(Region 5B) · 3 a day (Region 7) · only 1 over 50 cm (Region 6)")
    hatch = S.strictest([opening(2, "2 hatchery marked", hatchery=True), opening(1, "1 a day")], regions)
    assert Q.words(hatch["tree"]) == "1 a day, hatchery-marked only (Region 5B, Region 6)"
    assert S.region_label(["5b:region:section", "book:z5:x::y"]) == "Region 5B"


def test_t669_the_release_and_the_live_feed_name_the_same_regions(monkeypatch):
    """`chain_calendar` (the release) and `live.compose` (the feed, the pair table's reference) hand `strictest` the same
    group regions (`region_label` of each group's places). MUTATION: either side without its labels fails here."""
    from pipeline.regs.dfo_salmon import live as L
    from pipeline.tests.test_t668_salmon_regions import _build, _chain_of, _d3_world
    locs, rules, placed, reg = _d3_world()
    payload, secs = _build(locs, rules, placed, reg)
    c = _chain_of(payload, secs, 67)
    seen = []
    real = S.strictest
    monkeypatch.setattr(S, "strictest", lambda answers, regions=None: (seen.append(tuple(regions or ())),
                                                                       real(answers, regions))[1])
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    cals = {l: L.location_calendar(rules[l], v, g) for l in c["l"]}
    L.compose(c["l"], cals, v.items, groups=c["s"], intern=v, gears=g.items, gear_intern=g)
    fed = set(seen)
    seen.clear()
    _build(locs, rules, placed, reg)
    assert fed == {("Region 6", "Region 7")} and ("Region 6", "Region 7") in set(seen)
