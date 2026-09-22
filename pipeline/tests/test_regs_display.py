"""THE TABLE A READER SEES — checked against the ledger, never against itself.

Every test here compares `display` to a number derived INDEPENDENTLY from `Ledger`. None asks
whether the display agrees with itself, because that is the failure this file was written after:
the first display had seven defects and a green suite, and three of the tests that were supposed
to catch them restated the implementation instead of an outcome.

The one failure mode that matters: a counter the reader is never shown always reads as MORE fish
than the book allows. So the sweeps below are asymmetric on purpose — a table that says fewer is
a shared budget biting, a table that says more is a person over their limit.
"""
import pytest

from pipeline.regs.table import display as D
from pipeline.regs.table import state as ST
from pipeline.regs.table.subject import Origin

#: enough of the year to reach every seasonal clause in the corpus, plus the standing answer.
DATES = [None, (1, 15), (3, 1), (5, 1), (6, 15), (7, 20), (8, 20), (9, 15), (10, 20),
         (11, 1), (12, 1)]


def _ledgers():
    for region in ST.REGIONS:
        for kind in ST.KINDS:
            rules, here, label = ST.rules_for(region, kind)
            t = ST.build(rules, kind, here, label, province=region in ("province", "p"))
            yield region, kind, label, t.ledger


def _tables():
    for region, kind, label, L in _ledgers():
        for on in DATES:
            yield region, kind, on, L, D.table(L, kind, on, label)


def _origins(view_name):
    if view_name == "both":
        return (Origin.wild, Origin.hatchery)
    return (Origin.wild if view_name == "wild" else Origin.hatchery,)


def _slots(tab, L, on):
    """(species, origin, leaf, view) for every fish the table draws."""
    for vn, v in tab["views"].items():
        for lf in v["leaves"]:
            for sp in lf["fish"]:
                for o in _origins(vn):
                    yield sp, o, lf, v


# ----------------------------------------------------------------------------------------
def test_no_counter_that_binds_is_left_off_the_table():
    """THE PARTITION. Three of the first version's defects were this one wearing three hats: a
    pooled cap narrowed by the season to one fish, a non-pooled sized cap, and a clause each
    fell through the gap between "a headline" and "a shared number" and were simply dropped.
    Region 3's lakes printed 4 where the book allows 1.

    `display._check` raises when a counter lands nowhere. This walks every table on every date
    so that it gets the chance to."""
    n = 0
    for region, kind, on, L, tab in _tables():
        n += 1
    assert n == len(ST.REGIONS) * len(ST.KINDS) * len(DATES), n


def test_the_table_never_offers_more_fish_than_the_ledger_allows():
    """The whole point. `most` is compared against a number this test derives from the ledger
    itself — the strictest daily counter binding that fish, where "binding" includes a sized
    counter that the fish's own definition puts entirely inside (every steelhead is over 50 cm,
    so "1 over 50 cm" is simply 1).

    ASYMMETRIC ON PURPOSE. Fewer than the strictest single counter is a shared budget biting and
    is correct; more is a reader over their limit."""
    over, checked = [], 0
    for region, kind, on, L, tab in _tables():
        for sp, o, lf, v in _slots(tab, L, on):
            ns = [0 if a.is_zero else a.n for a in D._daily(L, sp, o, on)
                  if a.scope.size.is_any or D._whole_fish(a, (sp,))]
            truth = min([x for x in ns if x is not None], default=None)
            checked += 1
            if truth is not None and lf["most"] is not None and lf["most"] > truth:
                over.append((region, kind, on, o.value, sp, lf["most"], truth))
    assert checked > 15000, checked
    assert not over, over[:10]


def test_a_fish_that_must_go_back_is_in_no_live_budget_and_no_sharer_list():
    """A released fish spends nothing. Naming it inside a live shared number tells the reader
    the number is smaller than it is, and tells the reader holding THAT fish that they may keep
    it. `Ledger.keepable` is the independent witness."""
    bad = []
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for p in v["pools"]:
                for sp in p["spends"]:
                    for o in _origins(vn):
                        if not L.keepable(sp, o, on):
                            bad.append((region, kind, on, vn, sp, p["n"]))
    assert not bad, bad[:10]


def test_nothing_kept_means_no_size_and_no_number():
    """"must be at least 60 cm" beside "put it back" reads as permission."""
    bad = []
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for lf in v["leaves"]:
                if lf["zero"] and (lf["sizes"] or lf["most"]):
                    bad.append((region, kind, on, vn, lf["fish"], lf["sizes"], lf["most"]))
    assert not bad, bad[:10]


def test_a_size_has_two_ends():
    """Reading only `none_under` lost every ceiling and every slot. Region 7A's lakes let a bull
    trout be kept at 30 to 50 cm, and the table showed no size at all. Both ends, or neither."""
    rules, here, label = ST.rules_for("7a", "lake")
    L = ST.build(rules, "lake", here, label).ledger
    slots = [a for a in L.allowances if a.kind == "gate" and a.scope.size.kind == "slot"]
    assert slots, "the fixture this test is about is gone from the corpus"
    for a in slots:
        for on in [(11, 1), (6, 1)]:
            for sp in sorted(a.scope.effective()):
                if not L.binds(a, sp, Origin.wild, None, on):
                    continue
                bounds = {s["bound"] for s in D.sizes(L, sp, Origin.wild, on)}
                assert {"floor", "ceiling"} <= bounds, (sp, on, bounds)


def test_a_shared_cap_narrowed_to_one_fish_is_still_a_cap():
    """Region 3's streams share one bull trout, Dolly Varden OR lake trout. Bull trout and Dolly
    Varden go back in the autumn, so on 20 August the only fish left spending that 1 is the lake
    trout — and a budget needs two spenders to be a budget. The first version dropped it and
    printed 4. "The cap is only for the remaining fish", not "the cap is gone"."""
    rules, here, label = ST.rules_for("3", "stream")
    L = ST.build(rules, "stream", here, label).ledger
    tab = D.table(L, "stream", (8, 20), label)
    lt = [lf for v in tab["views"].values() for lf in v["leaves"] if lf["fish"] == ["LT"]]
    assert lt, "Region 3's lake trout no longer stands alone on 20 August"
    for lf in lt:
        assert lf["most"] == 1, (lf["most"], lf["answer"], lf["own"])
        assert any("1" in c["words"] for c in lf["own"]), lf["own"]


def test_the_definition_reaches_the_number_and_not_only_the_words():
    """A steelhead is a rainbow over 50 cm. The table printed "must be at least 50 cm" from that
    definition and then printed 5 beside it — two halves of one fact, and the half that reached
    the number was missing. Region 6's hatchery steelhead read 5 where the book allows 1."""
    seen = 0
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for lf in v["leaves"]:
                if lf["fish"] != ["ST"] or lf["zero"] or lf["unwritten"]:
                    continue
                caps = [c["n"] for c in lf["own"] if c.get("by_definition")]
                caps += [v["by_id"][p]["n"] for p in lf["spends"]
                         if v["by_id"][p].get("by_definition")]
                if caps:
                    seen += 1
                    assert lf["most"] <= min(caps), (region, kind, on, vn, lf["most"], caps)
    assert seen > 50, seen


def test_a_seasonal_counter_is_drawn_only_on_its_days():
    """The year view printed "1 trout · July 1 – Oct 31" beside the trout answering 5."""
    bad = []
    for region, kind, label, L in _ledgers():
        for vn, v in D.table(L, kind, None, label)["views"].items():
            for p in v["pools"]:
                if p["source"]["season"]:
                    bad.append((region, kind, vn, p["n"], p["source"]["season"]))
    assert not bad, bad[:10]


def test_every_clause_hangs_off_the_budget_it_is_a_clause_of():
    """`Allowance.within` holds the answer and the first version never read it, so 52 clause
    pairs floated unparented — and the commonest clause of all ("1 over 50 cm" inside "4 trout
    and char") reaches exactly the same fish, so containment alone can never see it."""
    orphans, cycles, parented = [], [], 0
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for p in v["pools"]:
                if p["clause_of"]:
                    (parented, orphans[0:0]) if p["parent"] else orphans.append(
                        (region, kind, on, p["n"], p["clause_of"]))
                    parented += bool(p["parent"])
                seen, cur = set(), p
                while cur["parent"]:
                    if cur["parent"] in seen:
                        cycles.append((region, kind, on, p["id"]))
                        break
                    seen.add(cur["parent"])
                    cur = v["by_id"][cur["parent"]]
    assert not orphans, orphans[:10]
    assert not cycles, cycles[:10]
    assert parented > 150, parented


#: everything on a leaf a reader can see. `_shape` decides whether the two origins are one
#: table from a list like this one, and the first version's list left out the annual limit and
#: the fish's own caps — so Regions 3 and 5, whose ONLY difference between wild and hatchery is
#: steelhead's ten a year, collapsed into one table and the ten vanished off the page. This list
#: is written out here, independently, so that a field dropped from `_shape` fails a test
#: instead of quietly merging two tables.
VISIBLE = ("answer", "most", "sizes", "spends", "own", "own_number", "capped_to", "annual",
           "zero", "unwritten", "members")


def test_the_two_origins_are_one_table_only_when_a_reader_could_not_tell_them_apart():
    """A table drawn once says the wild and the hatchery fish are governed alike. Whenever that
    is false the reader must see it, and every visible field counts — a limit of ten a year is
    not a detail, it is the only thing the province says about a hatchery steelhead."""
    for region, kind, label, L in _ledgers():
        tab = D.table(L, kind, None, label)
        w, h = D.view(L, Origin.wild, None), D.view(L, Origin.hatchery, None)
        wl = {tuple(lf["fish"]): lf for lf in w["leaves"]}
        hl = {tuple(lf["fish"]): lf for lf in h["leaves"]}
        alike = set(wl) == set(hl) and all(
            all(wl[k][f] == hl[k][f] for f in VISIBLE) for k in wl)
        alike = alike and ([(p["spends"], p["n"], p["size"], p["parent"]) for p in w["pools"]]
                           == [(p["spends"], p["n"], p["size"], p["parent"]) for p in h["pools"]])
        assert (tab["origin_mode"] == "none") == alike, (
            region, kind, tab["origin_mode"], alike,
            [(k, f) for k in wl if k in hl for f in VISIBLE if wl[k][f] != hl[k][f]][:4])


def test_two_origins_are_hoisted_apart_only_when_a_shared_number_itself_differs():
    """Keyed on the SPENDER SET this came out backwards — 16 tables hoisted and 2 split, where
    the truth is the other way round — because a wild release empties a pool of its spenders
    without changing the number the book prints beside it. Hoisting means two whole tables and a
    choice the reader has to make; it has to be earned."""
    for region, kind, label, L in _ledgers():
        tab = D.table(L, kind, None, label)
        if tab["origin_mode"] != "hoist":
            continue
        w = {p["id"]: p["n"] for p in tab["views"]["wild"]["pools"]}
        h = {p["id"]: p["n"] for p in tab["views"]["hatchery"]["pools"]}
        assert any(w[k] != h[k] for k in set(w) & set(h)), (region, kind)


def test_no_answer_is_ever_blank():
    """A row with no rule says so in words. A blank reads as "no limit" — the most permissive
    failure a regulations table has."""
    bad = []
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for lf in v["leaves"]:
                if lf["answer"] is None and not lf["unwritten"]:
                    bad.append((region, kind, on, vn, lf["fish"]))
                if not lf["members"] or not lf["handle"]:
                    bad.append((region, kind, on, vn, lf["fish"], "unnamed"))
    assert not bad, bad[:10]


def test_every_fish_the_ledger_knows_reaches_the_reader_by_name():
    """"9 kinds of trout and char" tells a reader holding a bull trout nothing. Every species in
    the ledger's universe is drawn, and drawn under a name."""
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            drawn = {sp for lf in v["leaves"] for sp in lf["fish"]}
            assert drawn == set(L.universe()), (region, kind, on, vn,
                                                sorted(set(L.universe()) - drawn))


def test_a_sized_budget_caps_only_the_fish_its_size_class_swallows_whole():
    """"1 over 50 cm" shared between a steelhead and nine other fish is a 1 for the steelhead,
    which is over 50 cm by definition, and no limit at all on the Arctic char, which may be
    any size. Asking that question of the BUDGET rather than of each fish under it put the
    steelhead's 1 onto every fish sharing the number — Region 6's Arctic char read 1 where the
    book allows 5. It is an under-statement, so no sweep for over-statements can find it.

    Independent witness: a fish whose own counters never fall below N may not be shown less
    than N unless an UNSIZED budget says so."""
    bad = []
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for lf in v["leaves"]:
                if lf["most"] is None or lf["zero"]:
                    continue
                for o in _origins(vn):
                    for sp in lf["fish"]:
                        if D._whole_fish_any(L, sp, o, on):
                            continue
                        floor_ = [a.n for a in D._daily(L, sp, o, on)
                                  if a.scope.size.is_any and a.n is not None]
                        plain = [v["by_id"][p]["n"] for p in lf["spends"]
                                 if not v["by_id"][p]["sized"]
                                 and v["by_id"][p]["n"] is not None]
                        allowed = min(floor_ + plain, default=None)
                        if allowed is not None and lf["most"] < allowed:
                            bad.append((region, kind, on, vn, sp, lf["most"], allowed))
    assert not bad, bad[:10]


def test_a_fish_can_only_be_named_one_way():
    """`leaf["fish"]` is sorted by species code and `leaf["members"]` by name, so zipping the
    two — the obvious thing for a caller to do, and what the page did — hands back the wrong
    name for nearly every fish. Region 6's shared five listed a Dolly Varden among the fish that
    spend it, and the Dolly Varden is released. The table carries a map instead."""
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for lf in v["leaves"]:
                assert sorted(tab["names"][c] for c in lf["fish"]) == lf["members"], \
                    (region, kind, on, vn, lf["fish"], lf["members"])
            for p in v["pools"]:
                for c in p["spends"]:
                    assert c in tab["names"], (region, kind, on, vn, c)


def test_what_you_may_have_follows_what_you_may_keep_today():
    """A possession counter is N times a DAILY one, and the daily one it multiplies is the
    family's, not this fish's. Region 6's hatchery steelhead may keep ONE a day, and the row
    offered ten in the cooler while explaining itself as "2 x the daily 5"."""
    bad = []
    for region, kind, on, L, tab in _tables():
        for vn, v in tab["views"].items():
            for lf in v["leaves"]:
                p, n = lf["possession"], lf["most"]
                if p is None or p["n"] is None or n is None or not p["times"]:
                    continue
                if lf["may_have"] is None or lf["may_have"] > p["times"] * n:
                    bad.append((region, kind, on, vn, lf["fish"], lf["may_have"],
                                p["times"], n))
    assert not bad, bad[:10]


# ----------------------------------------------------------------------------------------
# ONE TABLE — both origins on it, and the complex fish apart from the simple ones
# ----------------------------------------------------------------------------------------
def test_every_fish_appears_once_on_the_one_table():
    """The merged table is a partition of the universe: each fish on exactly one line. Grouping
    on a signature is how a fish goes missing — two keys that should be one, or one that should
    be two — so this counts rather than trusts."""
    for region, kind, on, L, tab in _tables():
        m = tab["merged"]
        seen = [f for lf in m["leaves"] for f in lf["fish"]]
        assert sorted(seen) == sorted(L.universe()), (region, kind, on)
        assert len(seen) == len(set(seen)), (region, kind, on)


def test_a_line_is_drawn_once_only_when_both_origins_say_the_same_thing():
    """Wild and hatchery were drawn as two whole tables when in every region the difference is
    one fact — the wild trout go back. Now the origin is on the line, and `same` decides whether
    there is one line or two. It has to mean what it says in both directions: one line where a
    reader could not tell the origins apart, two wherever they could."""
    for region, kind, on, L, tab in _tables():
        for lf in tab["merged"]["leaves"]:
            if lf["same"]:
                assert lf["both"] is not None and lf["sides"] is None, (region, kind, lf["fish"])
            else:
                w, h = lf["sides"]["wild"], lf["sides"]["hatchery"]
                assert w != h, (region, kind, on, lf["fish"])
                assert lf["both"] is None, (region, kind, lf["fish"])


def test_the_two_origins_on_a_line_are_the_two_origins_views():
    """The merged line is not a third derivation. Whatever it says about a fish, `view` says
    about that fish for that origin — otherwise there are two answers in the codebase and the
    page is reading the one nothing checks."""
    for region, kind, on, L, tab in _tables():
        per = {o: {sp: lf for lf in D.view(L, o, on)["leaves"] for sp in lf["fish"]}
               for o in D.ORIGINS}
        for lf in tab["merged"]["leaves"]:
            rep = min(lf["fish"])
            for o in D.ORIGINS:
                side = lf["both"] if lf["same"] else lf["sides"][o.value]
                for k in D.SEEN:
                    assert side[k] == per[o][rep][k], (region, kind, on, rep, o.value, k)


def test_the_complex_fish_are_the_ones_with_a_structure():
    """A trout shares a budget that has a clause that has a size class, and half the trout go
    back; a burbot is a fish and a number. Drawn in one list the burbot looks complicated and
    the structure is buried. The split is by the book's own families, not by a guess at which
    rows look busy."""
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
    want = frozenset(SPECIES_GROUPS["TROUT_CHAR"]) | frozenset(SPECIES_GROUPS["SALMON"])
    for region, kind, on, L, tab in _tables():
        for lf in tab["merged"]["leaves"]:
            inside = set(lf["fish"]) & want
            # a line is wholly in one table or wholly in the other, never across the join —
            # on the province's tables every fish shares one answer, and grouping on the answer
            # alone put the burbot and the crayfish under "Trout, char and salmon"
            assert not inside or inside == set(lf["fish"]), (region, kind, lf["fish"])
            assert lf["complex"] == bool(inside), (region, kind, lf["fish"])


def test_a_budget_says_which_origins_can_spend_it():
    """Region 1's streams share two between the hatchery trout, and the wild ones are all going
    back — there is nothing for them to share. A budget drawn with no origin on it there reads
    as a number a wild fish counts against."""
    for region, kind, on, L, tab in _tables():
        for p in tab["merged"]["pools"]:
            assert p["origins"], (region, kind, p["id"])
            assert (p["only"] is None) == (len(p["origins"]) == 2), (region, kind, p["id"])
            for o in p["origins"]:
                assert p["spends_by"][o], (region, kind, p["id"], o)
            live = {o.value: D.view(L, o, on)["by_id"] for o in D.ORIGINS}
            for o in p["origins"]:
                assert live[o][p["id"]]["n"] == p["n"], (region, kind, p["id"], o)


def test_a_line_is_never_named_by_a_count():
    """"9 kinds of all game fish" fails twice over: a count is not a name, so a reader holding a
    burbot cannot tell whether they are in it, and the umbrella is one the set does not fill —
    nine of twenty-eight game fish are not "all game fish"."""
    import re
    for region, kind, on, L, tab in _tables():
        for lf in tab["merged"]["leaves"]:
            assert not re.match(r"^\d+ kinds of", lf["handle"]), (region, kind, lf["handle"])
            # a handle is the group's name, or every member of it
            if len(lf["fish"]) > 1 and not all(m in lf["handle"] for m in lf["members"]):
                from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
                from pipeline.regs.table.subject import expand
                assert any(expand(frozenset({g})) == frozenset(lf["fish"])
                           for g in SPECIES_GROUPS), (region, kind, lf["handle"], lf["fish"])


def test_a_line_cites_one_rule_because_its_fish_share_one():
    """Kokanee, white sturgeon and the char all read "Put it back" in Region 1's streams, and
    three different rules put them there. Grouped on the words alone they collapse into one
    line — drawn under one of their names, citing one of their rules, with the other two
    nowhere. A line carries a single `source`, so the fish on it have to have a single source.

    This is the one thing the merge must keep apart that a reader cannot see: two identical
    answers from two authorities look the same on the page and are not the same fact."""
    for region, kind, on, L, tab in _tables():
        per = {o: {sp: lf for lf in D.view(L, o, on)["leaves"] for sp in lf["fish"]}
               for o in D.ORIGINS}
        for lf in tab["merged"]["leaves"]:
            for o in D.ORIGINS:
                cited = {(per[o][sp]["source"] or {}).get("rule") for sp in lf["fish"]}
                assert len(cited) == 1, (region, kind, on, o.value, lf["fish"], sorted(cited))
