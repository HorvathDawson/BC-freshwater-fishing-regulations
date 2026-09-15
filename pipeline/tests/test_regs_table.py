"""The laws the quota table rests on, and the defects that proved they were not held.

`comply` proves nothing is LOST. It cannot prove anything is RIGHT — every defect below passed
it. These are the properties that do, written against the real corpus where the corpus is what
broke them.
"""
from __future__ import annotations

import collections
import pytest

from pipeline.regs.table.subject import Subject, Origin, expand
from pipeline.regs.table.outcome import Outcome, outcome_of, CLOSED, RELEASE
from pipeline.regs.table.size import size_of


# --------------------------------------------------------------------------- #
# `covers` must be a partial order. It has been broken three times.
# --------------------------------------------------------------------------- #
def _subjects():
    return [Subject(frozenset({"ALL_GAME_FISH"})), Subject(frozenset({"TROUT_CHAR"})),
            Subject(frozenset({"ALL_FIN_FISH"})), Subject(frozenset({"NON_GAME_FISH"})),
            Subject(frozenset({"PROTECTED_SPECIES"})), Subject(frozenset({"KO"})),
            Subject(frozenset({"ST"}), Origin.wild), Subject(frozenset({"ST"}), Origin.hatchery),
            Subject(frozenset({"ST"})), Subject(frozenset()),
            Subject(frozenset({"ALL_GAME_FISH"}), excepts=frozenset({"BB"})),
            Subject(frozenset({"ALL_FIN_FISH"}), excepts=frozenset({"CRA"}))]


def test_covers_is_reflexive():
    """"All game fish OTHER THAN BURBOT" did not cover itself, because burbot is inside the
    group it names — so the rule excluded its own subject and vanished from the table."""
    for s in _subjects():
        assert s.covers(s), f"{sorted(s.fish)} except {sorted(s.excepts)} does not cover itself"


def test_covers_is_transitive():
    for a in _subjects():
        for b in _subjects():
            if not a.covers(b):
                continue
            for c in _subjects():
                if b.covers(c):
                    assert a.covers(c), f"{sorted(a.fish)} > {sorted(b.fish)} > {sorted(c.fish)}"


def test_covers_is_antisymmetric():
    for a in _subjects():
        for b in _subjects():
            if a != b and a.covers(b) and b.covers(a):
                pytest.fail(f"{sorted(a.fish)}/{sorted(a.excepts)} and "
                            f"{sorted(b.fish)}/{sorted(b.excepts)} cover each other")


def test_an_open_group_does_not_invert_the_order():
    """NON_GAME_FISH is a COMPLEMENT, not a universe. Read as "no members, therefore no fish"
    it became the lattice bottom and every rule covered it; read as "everything" it became the
    top and covered the universe. Both break the order; it is neither."""
    ng = Subject(frozenset({"NON_GAME_FISH"}))
    allf = Subject(frozenset({"ALL_FIN_FISH"}))
    assert not ng.covers(allf)
    assert allf.covers(ng)
    assert not ng.covers(Subject(frozenset({"KO"}))), "kokanee is a game fish"


def test_an_exception_narrows_and_does_not_widen():
    a = Subject(frozenset({"ALL_FIN_FISH"}), excepts=frozenset({"CRA"}))
    assert not a.covers(Subject(frozenset())), "carving CRA out cannot cover everything"
    assert Subject(frozenset()).covers(a)


def test_a_subject_that_cancels_itself_is_covered_by_nothing():
    empty = Subject(frozenset({"KO"}), excepts=frozenset({"KO"}))
    assert not Subject(frozenset({"BB"})).covers(empty)


# --------------------------------------------------------------------------- #
# `Outcome` must be a total order on what it actually means
# --------------------------------------------------------------------------- #
def test_stricter_is_an_order_and_closed_wins():
    got = sorted([Outcome("unlimited"), Outcome("quota", 15), Outcome("quota", 2),
                  RELEASE, CLOSED], key=lambda o: o.rank)
    assert [o.word() for o in got] == ["0", "release", "2", "15", "∞"]


def test_a_shut_water_beats_a_release_with_no_test_written():
    assert CLOSED.stricter(RELEASE) is CLOSED


def test_pooled_is_stricter_than_per_species_at_the_same_number():
    """"6 in the aggregate" and "6 of each" ranked equal, so the winner was list order — and
    the page printed "6 of each" for a rule that allows 6 between them."""
    assert Outcome("quota", 6, pooled=True).rank < Outcome("quota", 6).rank


def test_a_quota_of_zero_cannot_be_constructed():
    """Left representable it ranked WEAKER than release, which is the order upside down."""
    with pytest.raises(ValueError):
        Outcome("quota", 0)
    with pytest.raises(ValueError):
        Outcome("quota", None)


def test_may_target_is_an_int_and_still_means_closed():
    """Declared Optional[bool], shipped as 0/1. `is False` missed every closure."""
    assert outcome_of(0, 0, False, "daily") is CLOSED
    assert outcome_of(0, 1, False, "daily") is RELEASE
    assert outcome_of(None, None, False, "daily") is None


# --------------------------------------------------------------------------- #
# Size polarity — inverted on 112 rules
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kw,expect", [
    (dict(over_cm=None, under_cm=60, take=1), "none under 60 cm"),      # r2:cultus_lake
    (dict(over_cm=50, under_cm=None, take=0), "none over 50 cm"),
    (dict(over_cm=50, under_cm=None, take=1, within="p"), "counting those over 50 cm"),
    (dict(over_cm=None, under_cm=30, take=2, within="p"), "none under 30 cm"),
    (dict(over_cm=50, under_cm=None, take=5, period="annual"), "counting those over 50 cm"),
    (dict(over_cm=100, under_cm=70, take=1, band=True), "none between 70 cm and 100 cm"),
])
def test_size_polarity(kw, expect):
    """`under_cm` is always a FLOOR and never a ceiling. Read as a ceiling it put "keep 1 ·
    under 60 cm" on the Shuswap, where the one char you keep must be OVER 60."""
    assert size_of(**kw).words() == expect


# --------------------------------------------------------------------------- #
# The fold, against the live corpus
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def tables():
    """Built the way the pipeline builds them — WITH the section's regions and label.

    Without those, a region-scoped rule cannot be placed and an extent the atlas already cut
    reads as undrawable, so the fixture was testing a table no reader will ever see.
    """
    from pipeline.regs.table.build import (section_rules, section_regions, section_label,
                                           build, D, WATERS)
    out = []
    for w in WATERS:
        kind = "lake" if (D[w].get("kind") == "lake") else "stream"
        for run in range(len(D[w].get("runs") or [])):
            rs = section_rules(w, run)
            if rs:
                out.append((w, run, kind, rs,
                            build(rs, kind, section_regions(w, run), section_label(w, run))))
    return out


def test_no_row_is_printed_twice(tables):
    """`join` manufactures a subject equal to one already present; the guard against mutual
    cover then refused to absorb either. 684 identical rows, sorted adjacent."""
    for w, run, _, _, t in tables:
        seen = collections.Counter((r.subject, r.outcome) for r in t)
        dupe = [k for k, n in seen.items() if n > 1]
        assert not dupe, f"{w} stretch {run + 1} prints {len(dupe)} row(s) twice"


def test_only_one_rung_in_a_chain_governs(tables):
    for w, run, _, _, t in tables:
        for r in t:
            n = sum(1 for c in r.chain if c.status == "governs")
            assert n <= 1, f"{w} stretch {run + 1}: {n} rungs claim to govern one row"


def test_a_species_closure_does_not_shut_a_water_that_is_open(tables):
    """Region 4's "Bass: 0 quota" is a quota of zero, not a closure of the Columbia. It took
    the row anyway and the page read "you may not fish for it" over the water's own rule
    saying bass are unlimited — 1,652 rows of that shape.

    A genuine "No Fishing" is a different statement and DOES outrank a local quota; the Atnarko
    above Tweedsmuir is shut and its trout rule does not reopen it. So the law is about
    species-level closures only."""
    from pipeline.regs.table.build import name
    from pipeline.regs.table.resolve import _shuts_the_water
    for w, run, _, _, t in tables:
        for r in t:
            if r.outcome.kind != "closed":
                continue
            if _shuts_the_water(r.subject):
                continue
            gov = next((c for c in r.chain if c.status == "governs"), None)
            if gov is None:
                continue
            local = [c for c in r.chain if c.rank < gov.rank
                     and c.outcome.kind in ("quota", "unlimited")]
            assert not local, (
                f"{w} stretch {run + 1}: {r.subject.words(name)[0]} reads closed by "
                f"{gov.authority}, over a more local rule saying {local[0].outcome.word()}")


# --------------------------------------------------------------------------- #
# Authority settles one subject; it does not settle two.
# --------------------------------------------------------------------------- #
def test_a_broad_local_quota_does_not_override_a_narrow_wider_protection(tables):
    """"All wild steelhead must be released" is provincial and has no exception anywhere in the
    book. Region 4's "Trout/char: 5" says how many trout and char you may keep — it does not say
    a wild steelhead may be among them. Ranked against each other the 5 won on 27 sections."""
    from pipeline.regs.table.build import name
    for w, run, _, _, t in tables:
        for r in t:
            who, q = r.subject.words(name, is_release=(r.outcome.kind == "release"))
            if "Steelhead" in who and "wild" in q:
                assert r.outcome.kind != "quota", (
                    f"{w} stretch {run + 1} offers wild steelhead to keep: {r.outcome.word()}")


def test_a_water_may_replace_a_regional_number_upward(tables):
    """The mirror case, which rules out simply preferring the stricter or the narrower rule: a
    water writing "Bass daily quota = unlimited" over Region 4's "Bass: 0 quota" really does
    replace it, and the reader is allowed to be told so."""
    from pipeline.regs.table.build import name
    upward = []
    for w, run, _, _, t in tables:
        for r in t:
            who, _ = r.subject.words(name)
            if who != "Bass" or r.outcome.kind != "unlimited":
                continue
            gov = next((c for c in r.chain if c.status == "governs"), None)
            beaten = [c for c in r.chain
                      if c is not gov and c.rank > gov.rank and c.outcome.rank < gov.outcome.rank]
            if beaten:
                upward.append((w, run, gov.authority, beaten[0].authority))
    assert upward, ("no row anywhere replaces a wider, STRICTER rule — the ordering has "
                    "collapsed into 'strictest wins' and authority no longer counts")


def test_a_superior_authority_is_not_outranked(tables):
    """Fishing in a National Park is prohibited unless the National Parks regulations open it.
    A regional quota does not open it — and the park closure was being demoted under one."""
    from pipeline.regs.table.build import name
    for w, run, _, _, t in tables:
        # Only a closure on the WATER. The federal protected-species closure is a superior
        # authority about twelve fish, and says nothing about the rest of the river.
        from pipeline.regs.table.resolve import _shuts_the_water
        sup = [c for r in t for c in r.chain
               if c.authority == "Federal or Parks" and c.outcome.kind == "closed"
               and c.status == "governs" and _shuts_the_water(c.subject)]
        if not sup:
            continue
        for r in t:
            assert r.outcome.kind == "closed", (
                f"{w} stretch {run + 1}: {r.subject.words(name)[0]} is "
                f"{r.outcome.word()} where a federal or parks closure governs")


def test_a_rule_cannot_lift_itself(tables):
    """Region 6's steelhead stream closure names its OWN entry as the default it exempts, so it
    lifted itself on every water in the region — and the Babine, which the exemption's note does
    not name, lost a closure the book keeps."""
    from pipeline.regs.table.lifts import lifts_here
    from pipeline.regs.table.corpus import rid
    for w, run, _, rules, _ in tables:
        narrow, drop, _u = lifts_here(rules, frozenset())
        for x in rules:
            assert rid(x) not in drop or not any(
                ex.get("default_id") and rid(x).split("::")[-1].startswith(ex["default_id"])
                for ex in (x.get("exempts") or [])), f"{rid(x)} lifts itself"


def test_nothing_rides_on_a_row_that_permits_nothing(tables):
    """A possession multiple is a multiplier on a daily limit, and an annual ceiling is a
    ceiling on a number. A row that says release or closed has neither."""
    from pipeline.regs.table.build import name
    for w, run, _, _, t in tables:
        for r in t:
            if r.outcome.kind in ("quota", "unlimited"):
                continue
            assert not (r.ceilings or []), (
                f"{w} stretch {run + 1}: {r.subject.words(name)[0]} is {r.outcome.word()} "
                f"and carries a ceiling")
            assert not [q for q in (r.quals or []) if q.kind == "possession"], (
                f"{w} stretch {run + 1}: {r.subject.words(name)[0]} is {r.outcome.word()} "
                f"and carries a possession multiple")


def test_no_new_self_lifting_rules_appear():
    """A rule whose own exemption names it lifts itself everywhere. `lifts_here` refuses to let
    that happen, but the entry is still wrong, and a workaround that leaves no trace is how a
    corpus defect becomes permanent. One is known; a second would be a new one."""
    from pipeline.regs.table.corpus import rules
    from pipeline.regs.table.lifts import self_lifting
    got = {x["rule"] for x in self_lifting(rules())}
    known = {"z6:steelhead_stream_closure::steelhead_stream_closure.r1"}
    assert got <= known, f"new self-lifting rule(s) in the corpus: {sorted(got - known)}"
