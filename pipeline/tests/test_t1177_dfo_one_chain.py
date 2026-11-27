"""T1177: DFO places walk as the book's rows do (`reach.build.ONE_TRIBUTARY_LOGIC_FOR_DFO_PLACES`), and the salmon chain
the bundle builds from them — no data needed.

  * A SIDE CHANNEL BESIDE A PLACE IS IN IT (`salmon.SIDE_CHANNEL_BESIDE_A_PLACE_IS_IN_IT`): a lake place's walk up a river
    binds the river's stem, never its braids; a braid wholly beside the place's stem takes the place, one reaching stem
    the place does not cover stays out (no creeping past a cut). Without it the stem and its braids carried two chains
    and the bundle refused gnis:3942 Kitsumkalum River ("a salmon-chain run straddles two runs of its rule set").
  * THE WATER'S OWN PLACE SPEAKS FIRST (`salmon.THE_WATERS_OWN_PLACE_SPEAKS_FIRST`): on the river above a lake, the
    river's own place answers before the lake's "including tributaries" walk, however many more sections it covers; the
    lake's place fills only what the river's own places leave unsaid; on a creek that is neither's own water the
    nearer (narrower) place answers first, as before.
"""
from __future__ import annotations

from types import SimpleNamespace

from pipeline.deliver import salmon as S
from pipeline.deliver.bundle import derived as D

ALL_YEAR = {"dates": [{"from_month": 4, "from_day": 1, "to_month": 3, "to_day": 31}]}
SEP_OCT = {"dates": [{"from_month": 9, "from_day": 1, "to_month": 10, "to_day": 31}]}
SEP_20 = 920


def _rule(rid, sp, take, when=ALL_YEAR, may_target=None):
    r = {"rule_id": rid, "type": "retention_limit", "verbatim": rid, "species": sp, "when": when, "take": take}
    if may_target is not None:
        r["may_target"] = may_target
    return r


# ---------------------------------------------------------------------------------------------- side channels

def _braids():
    """Water 10's stem 1-2-6 (a place covers 1 and 2 only: its cut at 2/6), braid A = 3-4 wholly beside 2, braid B = 7
    leaving 2 and back into 6 (across the cut), 5 another water's channel touching 1."""
    sid_of = {f"h{i}": i for i in range(1, 8)}
    ord_of = {1: 10, 2: 10, 3: 10, 4: 10, 5: 20, 6: 10, 7: 10}
    touch = {1: {2, 5, 6}, 2: {1, 3, 7}, 3: {2, 4}, 4: {3}, 5: {1}, 6: {1, 7}, 7: {2, 6}}
    return sid_of, ord_of, [3, 4, 5, 7], touch


def test_a_braid_beside_a_place_is_carried_and_one_across_its_cut_is_not():
    sid_of, ord_of, off, touch = _braids()
    part = [{"location_id": "lake-walk", "mapped": True, "sections": ["h1", "h2"]}]
    [got] = S.carry_side_channels(part, set(), sid_of, ord_of, off, touch)
    assert got["sections"] == ["h1", "h2", "h3", "h4"]          # braid A in; braid B (into 6) and 5 (another water) out


def test_mutation_pin_side_channels_only_on_a_water_covered_whole(monkeypatch):
    """MUTATION: with the policy off a place covering part of a water carries no side channel (T228 as built)."""
    monkeypatch.setattr(S, "SIDE_CHANNEL_BESIDE_A_PLACE_IS_IN_IT", False)
    sid_of, ord_of, off, touch = _braids()
    part = [{"location_id": "lake-walk", "mapped": True, "sections": ["h1", "h2"]}]
    assert S.carry_side_channels(part, set(), sid_of, ord_of, off, touch)[0] is part[0]


def _span_db(chain_of):
    """A one-river bundle shaped as the Kitsumkalum's: stem 1 (0-10 km, rule set 1), 2 (10-20 km, rule set 2: what
    splits rule set 1 into two runs), 3 (20-30 km, rule set 1, the lake walk's), braid 4 beside 3 (no measure); the
    salmon chain per section from `chain_of`."""
    import sqlite3
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER, item_id TEXT, kind TEXT);
        CREATE TABLE item_section (ord INTEGER, sid INTEGER);
        CREATE TABLE section_ruleset (sid INTEGER, set_id INTEGER, key_ix INTEGER);
        CREATE TABLE section_licensing (sid INTEGER, set_id INTEGER);
        CREATE TABLE province_except (area_kind TEXT, sid INTEGER);
        CREATE TABLE steelhead_water (sid INTEGER);
        CREATE TABLE section_steelhead (sid INTEGER, code INTEGER);
        CREATE TABLE section_steelhead_rules (sid INTEGER);
        CREATE TABLE section_home (sid INTEGER, region TEXT);
        CREATE TABLE tidal (sid INTEGER);
        CREATE TABLE outside_bc (sid INTEGER);
        CREATE TABLE salmon_section (sid INTEGER PRIMARY KEY, chain INTEGER NOT NULL);
        CREATE TABLE span_end (eid INTEGER, token TEXT);
        CREATE TABLE section_span (sid INTEGER, lo_m INTEGER, hi_m INTEGER, lo INTEGER, hi INTEGER, off_stem INTEGER,
                                   shape INTEGER);
        CREATE TABLE section_touch (a INTEGER, b INTEGER);
    """)
    db.execute("INSERT INTO item VALUES (0, 'gnis:3942', 'stream')")
    for sid, rs, lo, hi, off in ((1, 1, 0, 10_000, 0), (2, 2, 10_000, 20_000, 0), (3, 1, 20_000, 30_000, 0),
                                 (4, 1, None, None, 1)):
        db.execute("INSERT INTO item_section VALUES (0, ?)", (sid,))
        db.execute("INSERT INTO section_ruleset VALUES (?, ?, 0)", (sid, rs))
        db.execute("INSERT INTO section_span VALUES (?, ?, ?, 1, 1, ?, 0)", (sid, lo, hi, off))
        db.execute("INSERT INTO salmon_section VALUES (?, ?)", (sid, chain_of[sid]))
    db.execute("INSERT INTO span_end VALUES (1, 'x')")
    db.executemany("INSERT INTO section_touch VALUES (?, ?)", [(1, 2), (2, 3), (3, 4)])
    return db


def test_the_part_builder_refuses_a_braid_left_on_another_chain_and_takes_it_carried(monkeypatch):
    """The Kitsumkalum refusal, on data built here: the lake walk binds stem 3 (chain 2) and not its braid 4, left on
    the river's own chain 1 with the stem below the rule-set break — a km-less run filed with stem 1, across stem 2.
    The braid carried (chain 2 as its stem) makes the parts."""
    from pipeline.deliver.bundle import access
    monkeypatch.setattr(access, "mostly_inside", lambda db: set())
    import pytest
    db = _span_db({1: 1, 2: 1, 3: 2, 4: 1})
    with pytest.raises(D.DerivedError, match="straddles two runs of its rule set"):
        D.part_rows(db)
    db = _span_db({1: 1, 2: 1, 3: 2, 4: 2})
    parts = D.part_rows(db)
    assert sorted(sorted(p[14]) for p in parts) == [[1], [2], [3, 4]]


def _chains_through_carry():
    """The Kitsumkalum shape through the real `carry_side_channels`: the river's place covers its whole stem 1-2-3 (so
    T228 carries braid 4 to it), the lake place's walk binds stem 3 only; each section's chain is its set of places."""
    sid_of = {f"h{i}": i for i in (1, 2, 3, 4)}
    ord_of = {1: 0, 2: 0, 3: 0, 4: 0}
    touch = {1: {2}, 2: {1, 3}, 3: {2, 4}, 4: {3}}
    placed = [{"location_id": "river", "mapped": True, "sections": ["h1", "h2", "h3"]},
              {"location_id": "lake-walk", "mapped": True, "sections": ["h3"]}]
    at = {s: set() for s in sid_of.values()}
    for r in S.carry_side_channels(placed, set(), sid_of, ord_of, [4], touch):
        for h in r["sections"]:
            at[sid_of[h]].add(r["location_id"])
    ids: dict = {}
    return {s: ids.setdefault(frozenset(v), len(ids) + 1) for s, v in at.items()}


def test_the_carry_rule_is_what_lets_the_part_builder_through(monkeypatch):
    """L5: through the real function. With the rule the braid takes the lake walk with its stem and the parts build;
    MUTATION: with it off the braid keeps the river's chain alone and the bundle refuses, as it refused gnis:3942."""
    import pytest
    from pipeline.deliver.bundle import access
    monkeypatch.setattr(access, "mostly_inside", lambda db: set())
    assert sorted(sorted(p[14]) for p in D.part_rows(_span_db(_chains_through_carry()))) == [[1], [2], [3, 4]]
    monkeypatch.setattr(S, "SIDE_CHANNEL_BESIDE_A_PLACE_IS_IN_IT", False)
    with pytest.raises(D.DerivedError, match="straddles two runs of its rule set"):
        D.part_rows(_span_db(_chains_through_carry()))


def test_a_braid_touching_no_main_channel_is_not_carried_on_a_partly_covered_water():
    """The rule's words: a braid is carried when every stretch of main channel it touches is the place's. One touching
    NO main channel (only a side channel the place already holds) has nothing to be beside: not carried on that ground
    (it was in the measured build: 1 section of the Blanchard River, no answer moved). On a water covered whole T228
    still carries it."""
    sid_of = {f"h{i}": i for i in range(1, 6)}
    ord_of = {1: 10, 2: 10, 3: 10, 4: 10, 5: 10}
    touch = {1: {2, 3}, 2: {1}, 3: {1, 4}, 4: {3}, 5: set()}
    off = [3, 4]                                            # 3 a placed channel off stem 1, 4 a channel off 3 only
    part = [{"location_id": "p", "mapped": True, "sections": ["h1", "h3"]}]     # stem 1 of 1, 2, 5: part of the water
    assert S.carry_side_channels(part, set(), sid_of, ord_of, off, touch)[0] is part[0]
    whole = [{"location_id": "w", "mapped": True, "sections": ["h1", "h2", "h3", "h5"]}]
    assert S.carry_side_channels(whole, set(), sid_of, ord_of, off, touch)[0]["sections"][-1] == "h4"


# ---------------------------------------------------------------------------------------------- the chain order

def _loc(lid, prec, fps, water_id=None):
    return SimpleNamespace(location_id=lid, precedence=prec, fingerprints=fps, water="", water_id=water_id,
                           source_text={"specific_area": ""}, tidal_water=False, region_number=6)


def _fixture():
    """River R (item gnis:1, sections 10 below the lake and 11 above it; its own place covers it and its tributaries:
    10, 11, 12, 13 — 4 sections) and Lake L on it (item wbk:1, section 20; "including tributaries": 20, the river
    above it 11 and creek 12 — 3 sections). R opens coho Sep-Oct and says nothing of chinook; L closes coho and
    chinook all year; the region opens both."""
    locs = [_loc("6:a", 0, ["fa"]), _loc("6:river", 3, ["fr"], "6:r"), _loc("6:lake", 3, ["fl"], "6:l")]
    waters = {"6:r": SimpleNamespace(water_id="6:r", item_ids=["gnis:1"]),
              "6:l": SimpleNamespace(water_id="6:l", item_ids=["wbk:1"])}
    ef = SimpleNamespace(region_name="Skeena", locations=locs, water=lambda wid: waters.get(wid))
    feed = {"date_modified": "2026-08-28", "locators": [
        {"fingerprint": "fa", "rules": [_rule("a.co", ["CO"], 4), _rule("a.ch", ["CH"], 4)]},
        {"fingerprint": "fr", "rules": [_rule("r.co", ["CO"], 4, SEP_OCT)]},
        {"fingerprint": "fl", "rules": [_rule("l.co", ["CO"], 0, may_target=False),
                                        _rule("l.ch", ["CH"], 0, may_target=False)]}]}
    placed = [{"location_id": "6:a", "mapped": True, "sections": ["s10", "s11", "s12", "s13", "s20"]},
              {"location_id": "6:river", "mapped": True, "sections": ["s10", "s11", "s12", "s13"]},
              {"location_id": "6:lake", "mapped": True, "sections": ["s20", "s11", "s12"]}]
    sid_of = {f"s{i}": i for i in (10, 11, 12, 13, 20)}
    return {"6": ef}, {"6": feed}, placed, sid_of, {"gnis:1": [10, 11], "wbk:1": [20]}


def _answer(payload, sections, sid, sp):
    ch = payload["chains"][dict(sections)[sid]]
    runs = ch["sp"][S.SPECIES.index(sp)]
    day = list(S.DAYS).index(SEP_20)
    v = [runs[k + 1] for k in range(0, len(runs), 2) if runs[k] <= day][-1]
    return ch["l"], payload["verdicts"][v]["s"]


def test_the_rivers_own_place_answers_above_the_lake_and_the_lake_fills_the_rest():
    payload, sections = S.build(*_fixture(), section_regions=None)
    chain, coho = _answer(payload, sections, 11, "CO")
    assert chain == ["6:river", "6:lake", "6:a"] and coho == "open"        # the river's own coho opening
    assert _answer(payload, sections, 11, "CH")[1] == "closed"              # the lake's chinook closure fills it
    # creek 12 is neither's own water: the nearer (narrower) lake place first, as before
    assert _answer(payload, sections, 12, "CO") == (["6:lake", "6:river", "6:a"], "closed")
    assert _answer(payload, sections, 20, "CO")[0] == ["6:lake", "6:river", "6:a"][::2]


def test_mutation_pin_size_alone_lets_the_lake_answer_for_the_river(monkeypatch):
    """MUTATION: with the policy off the lake's walk (3 sections) sorts before the river's own place (4) on the river
    itself and closes the river's coho."""
    monkeypatch.setattr(S, "THE_WATERS_OWN_PLACE_SPEAKS_FIRST", False)
    payload, sections = S.build(*_fixture(), section_regions=None)
    assert _answer(payload, sections, 11, "CO") == (["6:lake", "6:river", "6:a"], "closed")


def test_own_first_keeps_the_tier_and_puts_the_flag_after_a_region_part():
    key = S.chain_order({"big": 3, "small": 3, "sec": 1}, {"big": 9, "small": 2, "sec": 1})
    own = {"big"}.__contains__
    assert sorted(["sec", "small", "big"], key=S.own_first(key, own)) == ["big", "small", "sec"]
    assert sorted(["sec", "small", "big"], key=key) == ["small", "big", "sec"]
    k2 = S.own_first(lambda l: (0, {"big": 1, "small": 0}[l], l), own, at=2)
    assert sorted(["big", "small"], key=k2) == ["small", "big"]            # the region part still decides first
