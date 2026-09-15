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
from pipeline.regs.table.build import section_regions


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


# --------------------------------------------------------------------------- #
# The order: authority wins, except a take of zero. Four real cases.
# --------------------------------------------------------------------------- #
def _rung(rule_id, rank, fish, outcome, origin=Origin.both):
    from pipeline.regs.table.resolve import Rung
    who = {3: "Provincial", 2: "Region 4", 0: "this water"}[rank]
    return Rung(rule_id, who, rank, Subject(frozenset(fish), origin), outcome, rule_id)


def _answer(rungs, fish, origin=Origin.both):
    from pipeline.regs.table.resolve import resolve
    row = resolve(rungs, Subject(frozenset(fish), origin))
    return row.outcome, next(c for c in row.chain if c.status == "governs").rule_id


def test_a_take_of_zero_stands_over_a_closer_number_for_a_broader_group():
    """(a) "All wild steelhead must be released" is provincial; Region 4's "Trout/char: 5" is
    closer and says nothing about wild steelhead. The release stands — a take of zero from any
    authority stands unless something lifts it."""
    rungs = [_rung("zp:steelhead.r2", 3, {"ST"}, RELEASE, Origin.wild),
             _rung("z4:trout_char.r1", 2, {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))]
    out, gov = _answer(rungs, {"ST"}, Origin.wild)
    assert out == RELEASE and gov == "zp:steelhead.r2"


def test_b_the_closest_authority_wins_across_subjects_when_nothing_is_closed():
    """(b) Kootenay Lake's own "rainbow trout daily quota = 10" over Region 4's "Trout/char: 5".
    Strictest-group-winner said 5; the water is the closer authority and the answer is 10."""
    rungs = [_rung("r4:kootenay_lake_main_body.r4", 0, {"RB"}, Outcome("quota", 10)),
             _rung("z4:trout_char.r1", 2, {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))]
    out, gov = _answer(rungs, {"RB"})
    assert out.n == 10 and gov == "r4:kootenay_lake_main_body.r4"
    # ...and the trout-and-char row itself is still the regional 5: the 10 is about one fish.
    out, gov = _answer(rungs, {"TROUT_CHAR"})
    assert out.n == 5


def test_c_at_equal_authority_the_narrower_subject_speaks_first():
    """(c) Region 8 writes "Trout/char: 4 from streams" and "20 brook trout from streams" in the
    same table. One authority, two statements; the one about brook trout is the one about
    brook trout, and the reader was told 4."""
    rungs = [_rung("z8:trout_char.r3", 2, {"TROUT_CHAR"}, Outcome("quota", 4, pooled=True)),
             _rung("z8:trout_char.r5", 2, {"EB"}, Outcome("quota", 20))]
    out, gov = _answer(rungs, {"EB"})
    assert out.n == 20 and gov == "z8:trout_char.r5"


def test_d_a_water_replaces_a_regional_number_upward():
    """(d) A water's "trout and char: 10" over a regional 5 is the same fish, and the closer
    authority replaces it — upward. Neither strictness nor narrowness gets a say."""
    rungs = [_rung("r4:some_lake.r1", 0, {"TROUT_CHAR"}, Outcome("quota", 10, pooled=True)),
             _rung("z4:trout_char.r1", 2, {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))]
    out, gov = _answer(rungs, {"TROUT_CHAR"})
    assert out.n == 10 and gov == "r4:some_lake.r1"


def test_the_mirror_case_still_holds():
    """A WATER's "trout and char: 2" over the province's "rainbow trout: 5": the water is the
    closer authority and 2 is the answer for a rainbow, even though the province named it."""
    rungs = [_rung("r:water.r1", 0, {"TROUT_CHAR"}, Outcome("quota", 2, pooled=True)),
             _rung("zp:rb.r1", 3, {"RB"}, Outcome("quota", 5))]
    out, gov = _answer(rungs, {"RB"})
    assert out.n == 2 and gov == "r:water.r1"


def test_closed_stands_over_release_among_takes_of_zero():
    rungs = [_rung("r:water.r1", 0, {"RB"}, RELEASE),
             _rung("zp:x.r1", 3, {"TROUT_CHAR"}, CLOSED)]
    out, _ = _answer(rungs, {"RB"})
    assert out == CLOSED


def test_brook_trout_on_the_okanagan_is_twenty(tables):
    """The corpus form of (c): Region 8's "20 brook trout from streams" was absorbed into
    "Trout and char | 4" as "not the strictest here", and no brook trout row existed."""
    from pipeline.regs.table.build import name
    seen = 0
    for w, run, _, _, t in tables:
        if w != "Okanagan River":
            continue
        eb = [r for r in t if r.subject.words(name)[0] == "Brook trout"]
        assert eb and eb[0].outcome.n == 20, f"{w} stretch {run + 1}: {[r.outcome.word() for r in eb]}"
        seen += 1
    assert seen == 3


def test_a_stream_clause_does_not_retire_its_parent_on_a_lake(tables):
    """Region 4 writes "Trout/char: 5" and, inside it, "2 from streams". On a stream the 2 is
    the answer and the 5 retires. On a LAKE the 2 is about somewhere else — and the 5 retired
    anyway, so eight of nine lake sections had no trout and char row at all. Kootenay Lake's
    cutthroat and lake trout had no quota."""
    from pipeline.regs.table.corpus import rid
    for w, run, kind, rules, t in tables:
        by = {rid(x): x for x in rules}
        # The parents a clause FOR THIS KIND OF WATER may retire, and no others.
        may_retire = {f"{c.get('entry')}::{c.get('within')}" for c in rules
                      if c.get("within") and c.get("water") == kind
                      and c.get("take") is not None}
        for r in t:
            for c in r.chain:
                if "its own clause for this kind of water" in c.status:
                    assert c.rule_id in may_retire, (
                        f"{w} stretch {run + 1} ({kind}): {c.rule_id} retired by a clause "
                        f"about the other kind of water")


def test_kootenay_lake_main_body_is_the_corpus_form_of_case_b(tables):
    """The water's "rainbow trout daily quota = 10" and Region 4's "Trout/char: 5" are both
    printed, each about its own fish: 10 for a rainbow, 5 for the rest."""
    from pipeline.regs.table.build import name
    (t,) = [t for w, run, _, _, t in tables if w == "Kootenay Lake" and run == 0]
    by = {r.subject.words(name)[0]: r for r in t}
    assert by["Rainbow trout"].outcome.n == 10
    assert by["Trout and char"].outcome.n == 5
    gov = next(c for c in by["Rainbow trout"].chain if c.status == "governs")
    assert gov.authority == "this water"


# --------------------------------------------------------------------------- #
# A named-water list qualified by a region is not a region scope.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("text,kind,regions", [
    ("Regions 3, 5, 6, 7 and 8", "regions", {"3", "5", "6", "7", "8"}),
    ("Regions 1, 2 and 4", "regions", {"1", "2", "4"}),
    ("Fraser, Lower Pitt and Lower Harrison Rivers, Region 2", "undrawable", set()),
    ("Hells Gate upstream to the Region 3 boundary", "undrawable", set()),
    ("Fraser watershed within Region 6", "undrawable", set()),
    ("non-tidal portion of the Fraser River in Region 2", "undrawable", set()),
    ("Regions 3-8", "undrawable", set()),
])
def test_a_region_mention_scopes_only_when_it_is_the_whole_extent(text, kind, regions):
    """"(a) when sport fishing for sturgeon in Region 2 only on the Fraser River, Lower Pitt
    River, Lower Harrison River" names three rivers. Read for its "Region 2" alone, the
    exemption lifted the province-wide fin-fish bait ban on the Chilliwack, the Coquihalla and
    the Harrison — eleven sections that the book keeps under the ban."""
    from pipeline.regs.table.where import parse_where
    w = parse_where(text)
    assert (w.kind, set(w.regions)) == (kind, regions)


def test_the_fraser_bait_exemption_does_not_lift_the_ban_on_the_chilliwack(tables):
    from pipeline.regs.table.method_build import rungs_for
    from pipeline.regs.table.method import resolve_method
    from pipeline.regs.table.build import section_regions
    for w, run, kind, rules, _ in tables:
        if w not in ("Chilliwack River", "Coquihalla River"):
            continue
        here = section_regions(w, run)
        row = resolve_method("angling", rungs_for("angling", rules, here, kind), here)
        ids = {c.rule_id for c in row.constraints}
        assert "zp:bait::bait.r1" in ids, f"{w} stretch {run + 1}: the fin-fish ban is lifted"


# --------------------------------------------------------------------------- #
# White sturgeon: a closure the book scopes by population, and a table that cannot.
# --------------------------------------------------------------------------- #
def test_the_contradicted_closures_are_the_sturgeon_ones_and_no_others():
    """The book's protected-species entry lists "White Sturgeon (Nechako, Upper Fraser,
    Kootenay and Columbia populations)"; the catalogue's group holds bare WSG, so the federal
    closure covers the Fraser fishery the regional tables open. Nothing lifts it, the closure
    stands, and the reader on the Fraser is told there is no sturgeon fishery. The fix is a
    lift in the catalogue (see `contradicted_closures`); this pins the report so the defect
    cannot quietly grow or quietly vanish."""
    from pipeline.regs.table.corpus import rules
    from pipeline.regs.table.lifts import contradicted_closures
    got = {(x["closure"], x["opened_by"]) for x in contradicted_closures(rules())}
    closures = {"zp:protected_species::protected_species.r1", "z7a:sara_sturgeon::sara_sturgeon.r1"}
    openers = {"z1:species_quotas::species_quotas.r5", "z2:species_quotas::species_quotas.r7",
               "z3:species_quotas::species_quotas.r7", "z5:white_sturgeon::white_sturgeon.r2",
               "zp:white_sturgeon_licence::white_sturgeon_licence.r2"}
    assert got == {(c, o) for c in closures for o in openers}, sorted(got)


def test_a_superior_closure_stands_and_says_why(tables):
    """On the Fraser in Region 2 the protected-species row governs white sturgeon, and the
    regional "CATCH AND RELEASE ONLY" rides beneath it — not as "set wider, replaced by one
    closer to this water", which a federal closure is not, but as the thing it is."""
    from pipeline.regs.table.build import name
    seen = 0
    for w, run, _, _, t in tables:
        if w != "Fraser River":
            continue
        for r in t:
            if r.subject.words(name)[0] != "Protected species":
                continue
            assert r.outcome == CLOSED
            for c in r.chain:
                if c.rule_id == "z2:species_quotas::species_quotas.r7":
                    assert c.status == "does not open what a superior authority closed", c.status
                    seen += 1
    assert seen, "the Region 2 sturgeon release reached no Fraser table"


def test_the_answer_is_the_first_rung_of_every_chain(tables):
    """The chain was sorted by (authority, strictness) and the answer found by a second
    algorithm, so `chain[0]` was not the governing rung on 370 of 846 rows — and
    `Row.governs`, which returns `chain[0]`, was wrong on every one of them."""
    for w, run, _, _, t in tables:
        for r in t:
            assert r.chain and r.chain[0].status == "governs" and r.governs is r.chain[0], (
                f"{w} stretch {run + 1}: chain does not start with its answer")


def test_a_take_of_zero_beats_a_closer_number_only_where_the_book_means_it(tables):
    """The "except closures" clause is GENERAL, and its blast radius is not.

    Authority wins: a regional table overrides the province, and a water overrides the region.
    The one exception is a take of zero, which stands unless something lifts it — and that
    exception exists for one sentence in the book, "All wild steelhead must be released", which a
    regional trout-and-char quota does not mention and does not lift.

    Measured across all 102 sections, the exception changes the answer on 27 rows and every one
    of them is wild steelhead. That is the rule behaving as written rather than as hoped, and it
    is worth pinning: a general rule that happens to fire narrowly today will fire wider the
    moment the corpus moves, and the next reader of that row deserves someone to have looked.
    """
    from pipeline.regs.table.build import name
    wider = []
    for w, run, _, _, t in tables:
        for r in t:
            gov = next((c for c in r.chain if c.status == "governs"), None)
            if gov is None or gov.outcome.kind not in ("closed", "release"):
                continue
            if not any(c is not gov and c.rank < gov.rank
                       and c.outcome.kind in ("quota", "unlimited") for c in r.chain):
                continue
            who, _ = r.subject.words(name, is_release=(r.outcome.kind == "release"))
            if "Steelhead" not in who:
                wider.append((w, run + 1, who, gov.verbatim[:60]))
    assert not wider, (
        "a take of zero now beats a closer authority's number for something other than wild "
        f"steelhead — look at whether the book means it: {wider[:4]}")


def test_a_stream_rule_never_reaches_a_lake_table_by_any_route(tables):
    """`table` drops rules about the other kind of water before it resolves anything, and the
    orphan sweep put them back — Region 4's "Trout and char — 2 per day, FROM STREAMS" sat in
    the chain on Kootenay LAKE, whose answer is 5. It was inert until something re-weighed the
    chain, and the date-aware pass does exactly that, so the lake read 2."""
    from pipeline.regs.table.corpus import rid
    for w, run, kind, rules, t in tables:
        other = "lake" if kind == "stream" else "stream"
        # COMPOSITE IDS. A bare rule id is shared by up to nine rules — `trout_char_quota.r7`
        # is one per region — so comparing on it flags a stream rule because some OTHER
        # region's rule of the same name is about lakes. The test that guards the collision
        # must not fall for it.
        bad = {rid(x) for x in rules if x.get("water") == other}
        for r in t:
            for c in r.chain + (r.caveats or []) + (r.ceilings or []):
                assert c.rule_id not in bad, (
                    f"{w} stretch {run + 1} is a {kind}, and {c.rule_id} is about {other}s")


# --------------------------------------------------------------------------- #
# A size bound is a gate, not a competitor.
# --------------------------------------------------------------------------- #
def _row(t, fish, qual=""):
    from pipeline.regs.table.build import name
    for r in t:
        who, q = r.subject.words(name)
        if who == fish and q == qual:
            return r
    raise AssertionError(f"no row {fish!r} · {qual!r} in {[r.subject.words(name) for r in t]}")


def test_meets_is_symmetric_and_reflexive_where_a_subject_names_a_fish():
    """`covers` is containment and a gate needs intersection: "bull trout, Dolly Varden and
    lake trout" and "trout and char · hatchery only" cover neither each other nor nothing —
    a hatchery bull trout is inside both."""
    hatch = Subject(frozenset({"TROUT_CHAR"}), Origin.hatchery)
    char = Subject(frozenset({"BT", "DV", "LT"}))
    assert not hatch.covers(char) and not char.covers(hatch)
    assert hatch.meets(char) and char.meets(hatch)
    for a in _subjects():
        for b in _subjects():
            assert a.meets(b) == b.meets(a)
        if a.effective() or a.is_everything:
            assert a.meets(a)
    assert not Subject(frozenset({"KO"})).meets(Subject(frozenset({"BB"})))
    assert not Subject(frozenset({"ST"}), Origin.wild).meets(Subject(frozenset({"ST"}), Origin.hatchery))


def test_a_size_bound_reaches_the_keep_row_it_narrows_across_origin(tables):
    """Region 2's "none under 60 cm" is about bull trout, Dolly Varden and lake trout of either
    origin; the Fraser's only keep row is "Trout and char · hatchery only". Neither subject
    covers the other, so the bound reached no row on seven Fraser stretches and the check
    filed it under "no trigger" — a bucket for duties. Same sentence in Region 3, filed as a
    qualifier, printed."""
    for w, run, _, _, t in tables:
        if w != "Fraser River" or run > 2:
            continue
        keep = _row(t, "Trout and char", "hatchery only")
        ids = {g.rule_id for g in keep.gates if g.binds}
        assert "z2:trout_char_quota::trout_char_quota.r5b" in ids, (run + 1, ids)
        assert "z2:trout_char_quota::trout_char_quota.r8" in ids, (run + 1, ids)


def test_a_take_of_zero_on_a_size_class_is_the_gate_and_not_a_row(tables):
    """"Hatchery trout/char under 30 cm from streams: 0" is not a release of hatchery trout —
    it is the floor on the two you may keep. As a rung it was a release for a subject nobody
    else wrote about, took a row of its own, and the keep row beside it never showed 30 cm."""
    from pipeline.regs.table.build import name
    for w, run, kind, rules, t in tables:
        for r in t:
            for c in r.chain:
                assert not c.subject.size.is_gate, (
                    f"{w} stretch {run + 1}: {c.rule_id} competes with a size bound on its subject")
            assert not r.subject.size.is_gate, (w, run + 1, r.subject.words(name))
            assert r.governs.rule_id.split("::")[-1] != "trout_char_quota.r8" or "z2" not in r.governs.rule_id


def test_every_size_bound_handed_in_rides_on_a_row(tables):
    """Every spelling of a size bound — a parenthesis on a number, a bare "No trout under 25
    cm", a take of zero on the class, a clause inside an allowance — is one kind of value and
    reaches the table by one route. None may vanish, and none may be filed as "no trigger"."""
    from pipeline.regs.table.corpus import rid
    from pipeline.regs.table.build import _water_of
    for w, run, kind, rules, t in tables:
        by = {rid(x): x for x in rules}
        landed = {g.rule_id for r in t for g in (r.gates or [])}
        for x in rules:
            if str(x.get("type") or "") != "retention_limit" or x.get("method"):
                continue
            size = size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                           within=x.get("within"), band=bool(x.get("band")),
                           period=x.get("period") or "daily")
            wk = _water_of(x, by)
            if not size.is_gate or (wk and wk != kind):
                continue
            assert rid(x) in landed, f"{w} stretch {run + 1}: {rid(x)} “{x.get('verbatim')}”"


def test_a_licence_rule_with_a_length_in_it_is_not_a_size_bound(tables):
    """"Conservation Surcharge Stamp required to catch and keep rainbow trout over 50 cm"
    carries `over_cm` too. Read as a bound it told the Shuswap "none over 50 cm" on the water
    where the stamp is exactly what lets you keep one."""
    (t,) = [t for w, run, _, _, t in tables if w == "Shuswap Lake"]
    for r in t:
        for g in (r.gates or []):
            assert "shuswap_lake.r13" not in g.rule_id and "shuswap_lake.r14" not in g.rule_id


def test_the_shuswap_rainbow_row_carries_its_floor_as_a_gate(tables):
    """"Rainbow trout daily quota = 1 (none under 50 cm)" is one rule saying two things. With
    the bound on its subject it was a different subject from "rainbow trout", so the row was
    headed "Rainbow trout · none under 50 cm" and competed with nothing."""
    (t,) = [t for w, run, _, _, t in tables if w == "Shuswap Lake"]
    rb = _row(t, "Rainbow trout")
    assert rb.outcome.n == 1
    assert [g.size.words() for g in rb.gates if g.binds] == ["none under 50 cm"]
    assert rb.gates[0].rule_id == rb.governs.rule_id


def test_a_stream_clause_is_not_a_condition_on_a_lake(tables):
    """Region 8's "only 2 over 30 cm" sits inside "4 from streams"; flattened onto "Trout/char:
    5" it rode on Okanagan Lake as a condition on the five, wearing "in streams" as a label."""
    for w, run, kind, _, t in tables:
        if kind != "lake":
            continue
        for r in t:
            for l in (r.limits or []) + (r.dormant or []):
                assert "z8:trout_char_quota::trout_char_quota.r4" != l.rule_id, (w, run + 1)


def test_dormant_clauses_are_the_losing_rules_clauses():
    """`Row` was built positionally and `dormant` landed in `exemptions`, which `build` then
    overwrote — so the clauses of a beaten rule computed in `resolve` were thrown away every
    time, and only a sweep in `build` found them again on whichever row it tried first."""
    from pipeline.regs.table.resolve import resolve
    from pipeline.regs.table.clauses import SubLimit
    rungs = [_rung("r:water.r1", 0, {"TROUT_CHAR"}, Outcome("quota", 2, pooled=True)),
             _rung("z4:trout_char.r1", 2, {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))]
    clause = SubLimit(Subject(frozenset({"BT"})), 1, False, "1 bull trout", "z4:trout_char.r2")
    row = resolve(rungs, Subject(frozenset({"TROUT_CHAR"})), kids={"z4:trout_char.r1": [clause]})
    assert row.dormant == [clause] and row.exemptions is None and row.limits == []
