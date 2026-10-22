"""THE PORT AGAINST THE CONSUMER PAGE v35, with no harness (audit F12, owner 2026-10-22).

The page v35 harness is archived (`archive/pipeline/answers-reference-v35/`); its pinned check lives on as a
golden fixture (`fixtures/answers_v35_port.json.gz`, made by `python -m pipeline.tests.answers_v35_port`):
`rows.py` on the page's own ladder reproduces the page's own card except where a documented page bug shows.

Moved here from test_answers_reference.py with their assertions unchanged: the classifier tests (each class
explains only its own difference, with its MUTATION). Not moved, because what they tested is archived and no
live code runs it: the page script's sha256 pin (page_v35.js), the converter's `caught` -> `while` hand-off
(convert.py, export -> page blocks), compare.py's `diff_values` / `compare` (the `cli page-view` diff), and the
golden cache's stamp / make-once / no-half-cache tests (regenerate.sh's cache).
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.tests import answers_v35_port as H


def test_port_classes_explain_only_their_own_difference():
    """The port's classifier: a documented page bug explains a difference
    only when its own feature is on the record; anything else is `unexplained`."""
    from pipeline.tests.answers_v35_port import classify_answer, classify_row
    page = {"status": "keep", "daily": 1, "winner": "w", "narrow": None,
            "lines": [{"t": "annual", "r": "p"}], "roles": {"w": ["governs", None], "p": ["season", None]}}
    ours = {**page, "lines": [{"t": "possession_cap", "r": "p"}],
            "roles": {"w": ["governs", None], "p": ["possession_cap", None]}}
    assert classify_answer(page, page) == set()
    assert classify_answer(page, ours) == {"F6"}
    assert classify_answer(page, {**ours, "daily": 2}) == {"unexplained"}
    prow = {"kind": "keep", "pool": "p", "win": None, "members": ["CT", "RB"],
            "all_members": ["CT", "RB"], "daily": 4, "real_daily": None,
            "everyone": [["cap", "c", ["CT", "RB"]]], "groups": []}
    orow = {**prow, "groups": [[["CT", "RB"], [["origin2", "x"]]]]}
    full = {"members": ["CT", "RB"], "daily": 4, "groups": [], "conds": [], "items": []}
    assert classify_row(prow, orow, full) == {"F2"}
    assert classify_row(prow, {**orow, "daily": 3}, full) == {"F2", "unexplained"}
    # a real daily limit that moved with no F1/F3/F4/F5 feature on the row is not explained
    assert classify_row(prow, {**prow, "real_daily": [4, True, 2, 2, False]}, full) == {"unexplained"}


def test_port_explains_a_restated_rule_only_where_ours_agrees_with_the_winner():
    """ROUND15 3 (rows.py, owner): a rule of the winner's own row saying the very same thing is the
    same statement printed twice: it AGREES with the winner (no line), where the page says `also`
    (Columbia River's two "bass, no limit" rules, r5 and r9). Anything else stays unexplained."""
    from pipeline.tests.answers_v35_port import classify_answer, classify_row, restated
    page = {"status": "nolimit", "daily": "Infinity", "winner": "w", "narrow": None,
            "lines": [{"t": "also", "r": "p"}], "roles": {"w": ["governs", None], "p": ["also", None]}}
    ours = {**page, "lines": [], "roles": {"w": ["governs", None], "p": ["agrees", "w"]}}
    assert restated(page, ours) == {"p"}
    assert classify_answer(page, ours) == {"R15.3"}
    # MUTATION: agreeing with another rule than the winner, or a number that moved, is not it
    assert classify_answer(page, {**ours, "roles": {"w": ["governs", None], "p": ["agrees", "x"]}}) == {"unexplained"}
    assert classify_answer(page, {**ours, "daily": 3}) == {"unexplained"}
    assert restated(page, {**ours, "roles": {"w": ["governs", None], "p": ["agrees", "x"]}}) == set()
    prow = {"kind": "nolimit", "pool": "w", "win": None, "members": ["LMB", "SMB"],
            "all_members": ["LMB", "SMB"], "daily": "Infinity", "real_daily": None,
            "everyone": [["also", "p", ["LMB", "SMB"]]], "groups": []}
    orow = {**prow, "everyone": []}
    full = {"members": ["LMB", "SMB"], "daily": None, "groups": [], "conds": [], "items": []}
    assert classify_row(prow, orow, full, {"p"}) == {"R15.3"}
    assert classify_row(prow, orow, full) == {"unexplained"}          # p not restated on the card
    # the restated line accounted for, a different line left over: still unexplained
    assert classify_row(prow, {**orow, "everyone": [["also", "q", ["LMB"]]]}, full, {"p"}) == \
        {"R15.3", "unexplained"}


def test_port_explains_a_shared_count_only_with_its_feature():
    """F11 (rows.DECISIONS): a count limit shared by several kinds ("3 Dolly Varden and lake trout
    combined", Region 6) gives them that many TOGETHER, so the real daily limit moves; the page
    summed each kind's own. Explained only where our row names the shared count."""
    from pipeline.tests.answers_v35_port import classify_row
    prow = {"kind": "keep", "pool": "z6", "win": None, "members": ["CT", "DV", "RB"],
            "all_members": ["CT", "DV", "RB"], "daily": 5, "real_daily": [5, False, None, 6, False],
            "everyone": [], "groups": []}
    orow = {**prow, "real_daily": [5, False, None, 3, False]}
    full = {"members": ["CT", "DV", "RB"], "daily": 5, "groups": [], "conds": [], "items": [],
            "real_daily": {"shared_count": [{"take": 3, "by": "z6.r3"}]}}
    assert classify_row(prow, orow, full) == {"F11"}
    assert classify_row(prow, orow, {**full, "real_daily": {}}) == {"unexplained"}


def _kitimat_row(rest_bands):
    """A Kitimat River-like keep row (Region 6 trout/char, Jul 1-Oct 31): steelhead by
    cross-reference, lake trout under a sub-limit (a condition naming it), rainbow in a fact group;
    brook, brown and cutthroat trout named by nothing, so the page lumps them into its `rest` item.
    `rest_bands` gives our items for those three: {members: bands}."""
    items = [{"members": ["ST"], "bands": None, "xref": True},
             {"members": ["LT"], "bands": [[30, None, 3]]},
             {"members": ["RB"], "bands": [[30, 50, 5], [50, None, 1]]}]
    items += [{"members": list(ms), "bands": b} for ms, b in rest_bands.items()]
    return {"members": ["CT", "EB", "GB", "LT", "RB", "ST"],
            "all_members": ["RB", "ST", "LT", "EB", "GB", "CT"], "daily": 5,
            "groups": [{"members": ["ST"], "facts": [{"t": "xref", "r": "kitimat.r5", "members": ["ST"]}]},
                       {"members": ["RB"], "facts": [{"t": "steel", "r": "z6.r9", "members": ["RB"]}]}],
            "conds": [{"c": "group", "who": ["LT"], "sub": 3}],
            "everyone": [], "items": items,
            "real_daily": {"n": 5, "all": False, "sum": None, "capped_sum": 6, "rb": False,
                           "capped": ["ST", "LT", "GB", "CT"], "open": ["RB", "EB"]}}


def test_port_explains_the_pages_rest_item_as_f1_only_where_its_fish_differ():
    """F1 (rows.DECISIONS): page_v35.js `speciesItems` puts every keepable fish no group or
    condition names into ONE trailing `rest` item, and `quotaLines` gives it the stream share only
    when the share covers EVERY member. Kitimat River, Jul 1-Oct 31: rest = brook, brown and
    cutthroat trout; Region 6's '1 trout from streams' (except char) covers brown and cutthroat but
    not brook trout, so the page kept 5 for all three (real daily 5, ours 6: brown/cutthroat 1)."""
    from pipeline.tests.answers_v35_port import classify_row
    prow = {"kind": "keep", "pool": "z6.r1", "win": None, "members": ["CT", "EB", "GB", "LT", "RB", "ST"],
            "all_members": ["RB", "ST", "LT", "EB", "GB", "CT"], "daily": 5,
            "real_daily": [5, False, None, 5, False], "everyone": [], "groups": []}
    orow = {**prow, "real_daily": [5, False, None, 6, False]}
    split = _kitimat_row({("GB", "CT"): [[30, None, 1]], ("EB",): [[30, 50, 5], [50, None, 1]]})
    assert classify_row(prow, orow, split) == {"F1"}
    # MUTATION, outside Jul 1-Oct 31: no stream share, the rest fish have the same range (one item)
    same = _kitimat_row({("EB", "GB", "CT"): [[30, 50, 5], [50, None, 1]]})
    assert classify_row(prow, orow, same) == {"unexplained"}
    # MUTATION, the share covers every rest fish (brook trout named by a group, so not in rest):
    # what is left in the page's rest item has one range, so the rest item is not the cause
    covered = _kitimat_row({("GB", "CT"): [[30, None, 1]], ("EB",): [[30, 50, 5], [50, None, 1]]})
    covered["groups"].append({"members": ["EB"], "facts": [{"t": "tnote", "r": "z6.r7", "members": ["EB"]}]})
    assert classify_row(prow, orow, covered) == {"unexplained"}
    # MUTATION: fish with different ranges that a group or condition names are not the rest item
    named = _kitimat_row({("GB", "CT"): [[30, None, 1]], ("EB",): [[30, 50, 5], [50, None, 1]]})
    named["conds"].append({"c": "group", "who": ["GB", "CT"], "sub": 1})
    assert classify_row(prow, orow, named) == {"unexplained"}


def test_port_explains_a_record_duty_as_b94_only_on_a_record_rule():
    """B9.4 (rows.stored_role, rows 31, owner 2026-10-21): a deciding record-it rule (Shuswap Lake's
    rainbow trout stamp record, `record_retention`) is stored as `duty`, as page v36 lists it under
    "Other rules"; page v35 shows its deciding role (`floor`). Only that explains a moved role."""
    from pipeline.tests.answers_v35_port import PAGE_BUGS, classify_answer, recorded
    assert "B9.4" in PAGE_BUGS
    rec = {"zp:shuswap_rainbow_stamp::shuswap_rainbow_stamp.r1"}
    s = "zp:shuswap_rainbow_stamp::shuswap_rainbow_stamp.r1"
    page = {"status": "keep", "daily": 2, "winner": "w", "narrow": None, "lines": [],
            "roles": {"w": ["governs", None], s: ["floor", None]}}
    ours = {**page, "roles": {"w": ["governs", None], s: ["duty", None]}}
    assert recorded(page, ours, rec) == {s}
    assert classify_answer(page, ours, rec) == {"B9.4"}
    # with another documented class on the same answer: both named
    p6 = {**page, "lines": [{"t": "annual", "r": "p"}], "roles": {**page["roles"], "p": ["season", None]}}
    o6 = {**ours, "lines": [{"t": "possession_cap", "r": "p"}],
          "roles": {**ours["roles"], "p": ["possession_cap", None]}}
    assert classify_answer(p6, o6, rec) == {"B9.4", "F6"}
    # MUTATION: no record rules known; a rule without record_retention moved to duty; the page's
    # role not a deciding one; held by another rule; something else differing too
    assert classify_answer(page, ours) == {"unexplained"}
    assert classify_answer(page, ours, {"zp:other::other.r1"}) == {"unexplained"}
    assert classify_answer({**page, "roles": {**page["roles"], s: ["agrees", "w"]}}, ours, rec) == {"unexplained"}
    assert classify_answer(page, {**ours, "roles": {**ours["roles"], s: ["duty", "w"]}}, rec) == {"unexplained"}
    assert classify_answer(page, {**ours, "daily": 3}, rec) == {"unexplained"}
    assert classify_answer(page, {**ours, "roles": {"w": ["governs", None], s: ["floor", None]}}, rec) == set()


def test_port_explains_an_unread_orphan_count_clause_as_f16_only_with_the_orphan():
    """F16 (rows.DECISIONS, RULINGS Q2): an ORPHAN count clause (another table's, still speaking)
    caps its fish's bands like a sub-cap; page_v35.js `quotaLines` caps only from outercap/subcap
    and never reads `orphan`. Gotchen Lake, 'Trout/char daily quota = 2' under Region 5's
    '1 Dolly Varden/bull trout': ours caps DV at 1 (real daily [2, F, null, 1, F]), the page none."""
    from pipeline.tests.answers_v35_port import PAGE_BUGS, classify_row
    assert PAGE_BUGS["F16"] == "an orphan count clause (another table's, still speaking) not read as " \
                               "a cap on its fish (rows.DECISIONS F16, RULINGS Q2)"
    prow = {"kind": "keep", "pool": "gotchen.r1", "win": None, "members": ["DV", "EB", "RB"],
            "all_members": ["DV", "EB", "RB"], "daily": 2, "real_daily": None,
            "everyone": [["orphan", "z5.r4", ["DV"]]], "groups": []}
    orow = {**prow, "real_daily": [2, False, None, 1, False]}
    orphan = {"t": "orphan", "r": "z5.r4", "members": ["DV"], "rules": ["z5.r4"], "take": 1,
              "binds": ["DV"]}
    # a fact naming some of the row's fish sits in their fact group (rows.build_model)
    full = {"members": ["DV", "EB", "RB"], "all_members": ["DV", "EB", "RB"], "daily": 2,
            "groups": [{"members": ["DV"], "facts": [orphan]}], "conds": [], "everyone": [],
            "items": [{"members": ["DV"], "bands": [[0, None, 1]]},
                      {"members": ["EB", "RB"], "bands": [[0, None, 2]]}],
            "real_daily": {"n": 2, "all": False, "sum": None, "capped_sum": 1, "rb": False,
                           "capped": ["DV"], "open": ["EB", "RB"]}}
    assert classify_row(prow, orow, full) == {"F16"}
    # one naming every fish of the row is said to everyone: the same
    said = {**full, "everyone": [{**orphan, "members": ["DV", "EB", "RB"]}],
            "groups": [{"members": ["DV"], "facts": [{"t": "tnote", "r": "z5.r8", "members": ["DV"]}]}]}
    assert classify_row(prow, orow, said) == {"F16"}

    def with_fact(l):
        return {**full, "groups": [{"members": ["DV"], "facts": [l]}]}
    # MUTATION: no orphan fact; an orphan with no take; an orphan on a fish the limit does not cap
    assert classify_row(prow, orow, with_fact({"t": "tnote", "r": "z5.r8", "members": ["DV"]})) \
        == {"unexplained"}
    assert classify_row(prow, orow, with_fact({**orphan, "take": None})) == {"unexplained"}
    assert classify_row(prow, orow, {**full, "groups": [
        {"members": ["DV"], "facts": [{"t": "tnote", "r": "z5.r8", "members": ["DV"]}]},
        {"members": ["EB"], "facts": [{**orphan, "members": ["EB"], "binds": ["EB"]}]}]}) \
        == {"unexplained"}
    # MUTATION: the orphan is not what caps the fish — its take is above the fish's range (z5's
    # '3 char' over a DV capped at 1 by something else), or its own gate (water, dates, species)
    # does not hold for the fish, so rows.fish_bands never capped it with the orphan
    assert classify_row(prow, orow, with_fact({**orphan, "take": 3})) == {"unexplained"}
    assert classify_row(prow, orow, with_fact({**orphan, "binds": []})) == {"unexplained"}
    assert classify_row(prow, orow, with_fact({**orphan, "binds": ["EB"]})) == {"unexplained"}


def test_port_attributes_polley_lakes_orphan_caps_to_f16_alone():
    """Polley Lake (the gate fixture that pins DECISIONS F16 on our side,
    test_answers_28_gate_classes): Region 5's '1 Dolly Varden' and '2 lake trout' under the lake's
    'trout 8'. The page, not reading orphans, shows no real daily limit; the port puts that on F16
    and on nothing else (its rest fish share one range, so not F1)."""
    from pipeline.tests.answers_v35_port import classify_row, orphan_takes, row_projection
    from pipeline.tests.test_answers_28_gate_classes import JUL15, card

    class Ids(dict):
        def __missing__(self, k):
            return k
    out, P = card("polley", JUL15)
    out = orphan_takes(json.loads(json.dumps(out, default=str)), P)
    row = next(r for r in out["rows"] if r.get("pool") == "r5:polley_lake@5-2::polley_lake.r1")
    ours = row_projection(row, Ids())
    assert ours["real_daily"] == [8, False, None, 3, False]
    assert classify_row({**ours, "real_daily": None}, ours, row) == {"F16"}
    orphans = [l for g in row["groups"] for l in g["facts"]] + row["everyone"]
    assert sorted((l["take"], l["binds"]) for l in orphans if l["t"] == "orphan") \
        == [(1, ["DV"]), (2, ["LT"])]
    for l in orphans:
        if l["t"] == "orphan":
            l["take"] += 1                                          # MUTATION: above the fish's cap
    assert classify_row({**ours, "real_daily": None}, ours, row) == {"unexplained"}
    for l in orphans:
        if l["t"] == "orphan":
            l["take"] = None                                       # MUTATION: no clause number
    assert classify_row({**ours, "real_daily": None}, ours, row) == {"unexplained"}


def test_port_gives_orphan_facts_their_clauses_take():
    """Our rows ship an orphan fact without a number (the rule holds it): the port reads the take
    off the rule, and the fish it BINDS — rows.fish_bands' own orphan gate (water None or the
    part's kind, its dates hold, it covers the fish) over the fact's fish (else the row's) — so F16
    can require that the orphan is what caps the fish. Other facts are left as they are."""
    from types import SimpleNamespace as NS
    from pipeline.tests.answers_v35_port import orphan_takes

    def rule(take=None, water=None, species=("DV",), except_=(), dated=True):
        return NS(f={"take": take, "water": water, "dated": dated}, species=list(species),
                  except_=list(except_))
    P = NS(kind="lake", in_dates=lambda r: r.f["dated"], by_key={
        "z5::r4": rule(1), "z5::r9": rule(), "z5::r5": rule(2, "lake", ("DV", "LT")),
        "z5::r6": rule(2, "stream"), "z5::r7": rule(2, dated=False),
        "z5::r8": rule(2, species=("DV", "LT"), except_=("LT",))})
    out = {"rows": [{"members": ["DV", "LT"],
                     "everyone": [{"t": "orphan", "r": "z5::r4"}, {"t": "cap", "r": "z5::r4"},
                                  {"t": "orphan", "r": "z5::r5"}],
                     "groups": [{"members": ["DV"], "facts": [
                         {"t": "orphan", "r": "z5::r9", "members": ["DV"]},
                         {"t": "orphan", "r": "z5::r6", "members": ["DV"]},
                         {"t": "orphan", "r": "z5::r7", "members": ["DV"]},
                         {"t": "orphan", "r": "z5::r8", "members": ["DV", "LT"]}]}]},
                    {"members": [], "everyone": [], "groups": []}]}
    orphan_takes(out, P)
    row = out["rows"][0]
    assert row["everyone"] == [{"t": "orphan", "r": "z5::r4", "take": 1, "binds": ["DV"]},
                               {"t": "cap", "r": "z5::r4"},
                               {"t": "orphan", "r": "z5::r5", "take": 2, "binds": ["DV", "LT"]}]
    assert [(l["take"], l["binds"]) for l in row["groups"][0]["facts"]] == [
        (None, ["DV"]),     # no take: F16 refuses it on the take
        (2, []),            # another water's clause
        (2, []),            # out of its dates
        (2, ["DV"])]        # LT is excepted
    assert out["rows"][1] == {"members": [], "everyone": [], "groups": []}


def test_port_keys_our_rows_in_the_pages_word():
    """The page keys a row by ITS kind word ("nolimit"); ours says "no_limit" (row_projection
    maps the value, the archived cli page-view the key). Keyed raw, every card with a no-limit row read
    as a row order/set difference."""
    from pipeline.tests.answers_v35_port import row_key
    ids = ["a::r1", "b::r2"]
    assert row_key({"kind": "no_limit", "pool": None, "win": 1}, ids) == ("nolimit", "b::r2")
    assert row_key({"kind": "keep", "pool": 0, "win": 1}, ids) == ("keep", "a::r1")
    assert row_key({"kind": "release", "pool": None, "win": 0}, ids) == ("release", "a::r1")


# ------------------------------------------------------------------------------------------------ the replay
def _page_card_of(out: dict, rule_ids: list) -> dict:
    """A golden card in the PAGE's record shape that says exactly what `out` says (the inverse of
    `page_answer` / `page_rows`): the replay's identity case."""
    def ans(d):
        if d is None:
            return None
        return {"status": d["status"], "daily": d["daily"], "win": d["winner"], "narrow": d["narrow"],
                "lines": d["lines"], "roles": [[k, {"role": v[0], "by": v[1]}] for k, v in d["roles"].items()]}
    rows = []
    for r in out["rows"]:
        o = H.row_projection(r, rule_ids)
        rd = o["real_daily"]
        rows.append({"kind": o["kind"], "pool": o["pool"], "win": o["win"], "members": o["members"],
                     "allMembers": o["all_members"], "daily": o["daily"],
                     "real_daily": None if rd is None else dict(zip(("n", "all", "sum", "cappedSum", "rb"), rd)),
                     "everyone": [{"t": t, "r": rr, "members": ms} for t, rr, ms in o["everyone"]],
                     "groups": [{"members": ms, "facts": [{"t": t, "r": rr} for t, rr in fs]} for ms, fs in o["groups"]]})
    return {"spp": out["spp"], "rows": rows,
            "answers": {S: {o: ans(H.decided_projection(v[o], rule_ids)) for o in ("hatchery", "wild")}
                        for S, v in out["fish"].items()}}


def test_a_frozen_card_replays_to_the_rows_it_was_cut_from():
    """NO DATA. The fixture's mechanism on the gate fixture's Polley Lake (Jul 15): the card's inputs, cut
    to JSON as `card_inputs` cuts them and replayed by `run_card` on a stub bundle, give byte for byte what
    `rows.produce` gives on the live Part; a page card saying the same is clean, and a page that shows
    another daily number is `unexplained` (MUTATION)."""
    from pipeline.deliver.answers import rows as RW
    from pipeline.tests.test_answers_28_gate_classes import JUL15, PATH, _bound, _table, part
    P = part("polley", JUL15)
    rule_ids = sorted(f"{e}::{r}" for e, r in _table())
    ri = {r: i for i, r in enumerate(rule_ids)}
    want = RW.refs(H.orphan_takes(json.loads(json.dumps(RW.produce(P))), P), ri.__getitem__)
    inp = json.loads(json.dumps({"set": [list(t) for t in _bound("polley")], "sw": P.steelhead_water,
                                 "sr": P.steelhead_rules, "kind": P.kind, "presence": P.presence,
                                 "ladder": P.ladder, "md": list(P.md), "at": None, "open": {}}))
    fx = {"rules": {f"{e}::{r}": x for (e, r), x in _table().items()}, "nearer": []}
    got = H.run_card(H.stub(json.loads(json.dumps(fx))), inp, ri)
    assert json.dumps(got, sort_keys=True) == json.dumps(want, sort_keys=True)
    page = _page_card_of(got, rule_ids)
    classes, st = H.classify_card(page, got, rule_ids, set())
    assert not classes and st["cards"] == 1 and st["answers"] > 0 and st["rows"] > 0, (classes, st)
    bad = copy.deepcopy(page)
    S = next(S for S, v in bad["answers"].items() if v["wild"] and v["wild"]["status"] == "keep")
    bad["answers"][S]["wild"]["daily"] = (bad["answers"][S]["wild"]["daily"] or 0) + 1
    assert H.classify_card(bad, got, rule_ids, set())[0] == {"answer unexplained": 1}


# ------------------------------------------------------------------------------------------------ the golden
@pytest.fixture(scope="module")
def frozen():
    if not H.FIXTURE.exists():
        pytest.fail(f"missing {H.FIXTURE} — make it with: python -m pipeline.tests.answers_v35_port BUNDLE "
                    "EXPORT_DIR GOLDEN_DIR pipeline/tests/fixtures/answers_v35_port.json.gz (module docstring)",
                    pytrace=False)
    return H.load()


def test_port_on_the_pages_own_ladder(frozen):
    """THE PORT, ISOLATED (was test_answers_reference's slow data test): rows.py on the page's own golden
    ladder reproduces the page's card on every frozen card, answer and row, except where a documented
    page bug shows (PAGE_BUGS) — pinned at ZERO unexplained differences."""
    import collections
    B = H.stub(frozen)
    rule_ids, rec_rules = frozen["rule_ids"], set(frozen["record_rules"])
    ri = {r: i for i, r in enumerate(rule_ids)}
    all_rules = set(frozen["all_rules"])
    classes, st = collections.Counter(), collections.Counter()
    for c in frozen["cards"]:
        out = H.run_card(B, c, ri, all_rules)
        k, s = H.classify_card(c["page"], out, rule_ids, rec_rules)
        classes.update(k)
        st.update(s)
    assert st["cards"] == len(frozen["cards"]) > 0, st
    assert st["answers"] > 0 and st["rows"] > 0, st
    assert not st.get("spp_differ"), st
    bad = {k: v for k, v in classes.items() if "unexplained" in k}
    assert not bad, bad
    assert {k.split()[1] for k in classes} <= set(H.PAGE_BUGS), classes
    # every documented class the live port saw over EVERY golden card is still exercised here
    live = {k.split()[1] for k in frozen["about"]["live_port"]["classes"]}
    assert {k.split()[1] for k in classes} == live, (classes, live)
