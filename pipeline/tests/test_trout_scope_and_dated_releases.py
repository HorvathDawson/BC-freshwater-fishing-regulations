"""THE USER'S RULINGS OF 2026-09-28, each pinned on the book's own words.

A. "Trout" includes char UNLESS CHAR ARE MENTIONED. When the same water row, or the same zone
   table, names a char apart (char, Dolly Varden/bull trout, lake trout, brook trout), that row's
   bare "trout" lines exclude char — `TROUT_CHAR` with `species_except: [CHAR]`. Otherwise trout =
   trout + char. Region 6's box (p.49), Region 1's (p.13); a lake's "Trout daily quota = 2".
B. A DATED zone release or closure is not silenced by a water's quota unless the water says the
   exact same thing on the same dates (or prints a lift, or the closure sends the reader to the
   tables): Shuswap Lake's "Char daily quota = 1" beside Region 3's "Lake trout from Oct 15-Jan 31"
   release (p.28).
C. Kitimat River's "No Fishing on the west half of river …" (p.51) holds on ONE HALF of the
   channel: placed on the reach, shown beside the rules the other half answers to (`side`).

Mutation (run by hand, 2026-09-28; see the round's report): each guard below was broken and the
tests naming it failed — `trout_scope_problems` returning [], `mentions_char_apart` ignoring the
group word, `statement` keeping the CHAR exception, `beats` without the dated-release clause,
`sends_to_tables` answering True, and the `side` -> "part" state in `effective_rules`.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.deliver.bundle import rules as RM
from pipeline.regs.parsing import catalogue as C
from pipeline.tests.conftest import need, predates, BUNDLE_HINT

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))

R6_BOX = ("Trout/char: 5, but not more than • 1 over 50 cm (quota includes hatchery steelhead) • 3 "
          "Dolly Varden/bull trout and/or lake trout combined • 1 trout from streams July 1-Oct 31 "
          "And you must release: • all Dolly Varden/bull trout from streams all year • Trout under "
          "30 cm from any stream • Trout of any size from streams, Nov 1-June 30")
R1_BOX = ("Trout: 4, but not more than • 1 over 50 cm • 2 from streams (must be hatchery) And you "
          "must release: • All wild steelhead • All wild trout from streams • All char (includes "
          "Dolly Varden)")
DODD = "Wild trout/char daily quota = 2 (no wild trout over 40 cm); single barbless hook"


# =============================================================================== A — the word
def test_a_row_mentions_char_apart_only_by_naming_a_char_on_its_own():
    assert C.mentions_char_apart(R6_BOX) and C.mentions_char_apart(R1_BOX)
    assert not C.mentions_char_apart("Trout daily quota = 2")                  # Amor Lake
    # the group word names char IN; it is no mention apart (user: "none over 40 cm like trout")
    assert not C.mentions_char_apart(DODD)
    assert not C.mentions_char_apart("Kokanee, trout and char catch and release")
    assert C.mentions_char_apart("Rainbow trout and char catch and release")   # the trout is a rainbow
    assert C.mentions_char_apart("Trout/char daily quota combined = 2, no trout over 40cm, no more "
                                 "than 1 char (none under 60 cm)")             # Alta Lake
    assert C.mentions_char_apart("No trout under 25 cm; bull trout catch and release")
    assert C.mentions_char_apart("trout daily quota = 1 (none under 40 cm), brook trout daily "
                                 "quota = 5")


def test_the_trout_word_a_line_prints():
    assert C.trout_word("Trout/char: 5") == "trout/char"
    assert C.trout_word("Kokanee, trout and char catch and release") == "trout/char"
    assert C.trout_word("1 trout from streams July 1-Oct 31") == "trout"
    assert C.trout_word("no wild trout over 40 cm") == "trout"
    assert C.trout_word("1 over 50 cm") is None
    assert C.trout_word("Rainbow trout daily quota = 2") is None
    assert C.trout_word("1 bull trout (Dolly Varden) or lake trout") is None


def _entry(text: str, rules: list[dict]) -> dict:
    return {"entry_id": "r9:x@9-1", "name": "X", "regs_verbatim": text,
            "rules": [dict({"type": "retention_limit", "extents": [{"op": "whole"}]}, **r)
                      for r in rules]}


def test_the_row_decides_the_scope_both_ways():
    """A 'trout' line of a row naming char apart must exclude char; of a row naming none, must
    not; a 'trout/char' line never does; one char alone is never the exclusion."""
    trout = {"rule_id": "x.r1", "verbatim": "1 trout from streams", "species": ["TROUT_CHAR"],
             "take": 1}
    ok = _entry("1 trout from streams; bull trout catch and release",
                [dict(trout, species_except=["CHAR"]),
                 {"rule_id": "x.r2", "verbatim": "bull trout catch and release",
                  "species": ["DV"], "take": 0, "may_target": True}])
    C.CatalogueEntry.model_validate(ok)
    for bad, why in (
            (_entry("1 trout from streams; bull trout catch and release",
                    [trout, ok["rules"][1]]), "its trout exclude char"),
            (_entry("1 trout from streams", [dict(trout, species_except=["CHAR"])]),
             "no char rule of its own"),
            (_entry("Trout/char: 5; char catch and release",
                    [dict(trout, verbatim="Trout/char: 5", take=5, species_except=["CHAR"])]),
             "names char in"),
            (_entry("1 trout from streams; bull trout catch and release",
                    [dict(trout, species_except=["DV"]), ok["rules"][1]]), "never one char")):
        with pytest.raises(ValueError, match=why):
            C.CatalogueEntry.model_validate(bad)
    # a clause under a "trout" quota is a trout line too (Region 1's "1 over 50 cm")
    clause = _entry("Trout: 4 • 1 over 50 cm • All char",
                    [dict(trout, verbatim="Trout: 4", take=4, species_except=["CHAR"]),
                     {"rule_id": "x.r3", "verbatim": "1 over 50 cm", "species": ["TROUT_CHAR"],
                      "take": 1, "within": "x.r1", "lengths": [{"min_cm": 50}]},
                     {"rule_id": "x.r4", "verbatim": "All char", "species": ["CHAR"], "take": 0,
                      "may_target": True}])
    with pytest.raises(ValueError, match="x.r3: prints 'trout'"):
        C.CatalogueEntry.model_validate(clause)


def _corpus() -> dict:
    from pipeline.common.curated import CURATED
    out = {}
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        for e in C.CatalogueFile.model_validate(json.loads(p.read_text())).entries:
            out[e.entry_id] = e
    return out


@pytest.fixture(scope="module")
def corpus():
    return _corpus()


def _rule(corpus, eid, rid):
    return next(r for r in corpus[eid].rules if r.rule_id == rid)


def test_region_6_and_region_1_trout_lines_are_trout_only(corpus):
    """R6 (p.49) and R1 (p.13) give char RELATED lines of their own (user 2026-10-07, TROUT/CHAR
    CLARIFIED), so their bare 'trout' count/release lines exclude it; their 'Trout/char' lines keep
    it, and Region 6's size line 'Trout under 30 cm from any stream' (no char size line beside it)
    covers char. The label says 'Trout'."""
    assert "CHAR" not in _rule(corpus, "z6:trout_char_quota", "trout_char_quota.r6").species_except
    for eid, rid in (("z6:trout_char_quota", "trout_char_quota.r4"),
                     ("z6:trout_char_quota", "trout_char_quota.r7"),
                     ("z1:trout_quota", "trout_quota.r1"), ("z1:trout_quota", "trout_quota.r2"),
                     ("z1:trout_quota", "trout_quota.r4"), ("z1:trout_quota", "trout_quota.r6")):
        r = _rule(corpus, eid, rid)
        assert "CHAR" in r.species_except and r.species == ["TROUT_CHAR"], (eid, rid)
        lab = C.label(r)
        assert lab.startswith(("Trout —", "Trout (", "Trout other than")), lab
        assert "char" not in lab.lower(), lab
    for eid, rid in (("z6:trout_char_quota", "trout_char_quota.r1"),
                     ("z6:trout_char_quota", "trout_char_quota.r2"),
                     ("z1:hg_quota", "hg_quota.r1")):
        assert "CHAR" not in _rule(corpus, eid, rid).species_except, (eid, rid)


def test_a_row_naming_no_char_apart_keeps_char_in_its_trout(corpus):
    """Amor Lake's 'Trout daily quota = 2'; the seven Region 2 lakes' 'no wild trout over 40 cm'
    under 'Wild trout/char daily quota = 2' (the group word is no mention apart)."""
    assert not _rule(corpus, "r1:amor_lake@1-10", "amor_lake.r1").species_except
    for lake in ("dodd", "horseshoe", "ireland", "nanton", "windsor", "lois", "khartoum"):
        r = _rule(corpus, f"r2:{lake}_lake@2-12", f"{lake}_lake.r2")
        assert r.species == ["TROUT_CHAR"] and "CHAR" not in r.species_except, lake


def test_the_region_2_bull_trout_rows_exclude_char_as_a_group(corpus):
    """The six 'No wild trout over 50 cm, 1 bull trout over 60 cm' lakes now follow the general
    rule (CHAR), not a DV-only exception."""
    for lake, mu in (("chehalis", "2-19"), ("chilliwack", "2-4"), ("cultus", "2-3"),
                     ("harrison", "2-18"), ("lillooet", "2-10"), ("pitt", "2-8")):
        r = _rule(corpus, f"r2:{lake}_lake@{mu}", f"{lake}_lake.r1")
        assert r.species_except == ["CHAR"], lake


def test_the_printed_word_trout_is_one_statement():
    """Amor Lake's 'Trout 2' (trout and char) and Region 1's 'Trout: 4' (trout only) are the same
    statement — the CHAR exception is the word's scope. MUTATION: keeping CHAR in `statement`
    fails this and the Amor case below."""
    zone = {"type": "retention_limit", "species": ["TROUT_CHAR"], "species_except": ["CHAR"],
            "take": 4}
    lake = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 2}
    assert RM.same_statement(zone, lake)
    assert not RM.same_statement(dict(zone, species_except=["CHAR", "ST"]), lake)
    assert not RM.same_statement({"type": "retention_limit", "species": ["ST"], "take": 1}, lake)


def _tiny(tmp_path, rules: list) -> str:
    path = str(tmp_path / f"tiny{len(list(tmp_path.iterdir()))}.sqlite")
    con = sqlite3.connect(path)
    con.execute("create table section_ruleset (sid integer, set_id integer)")
    con.execute("create table ruleset (set_id integer, entry_id text, rule_id text, via text)")
    con.execute("insert into section_ruleset values (1, 0)")
    con.executemany("insert into ruleset values (0, ?, ?, 'reach')",
                    [(x["entry"], x["rule"]) for x in rules])
    con.commit()
    con.close()
    R._RULES_BY_PATH[path] = {(x["entry"], x["rule"]): dict(
        {"type": "retention_limit", "dimension": "daily", "family": "retention"}, **x)
        for x in rules}
    return path


def _states(path, fish, on) -> dict:
    return {x["rule"]: x["state"] for x in R.effective_rules(1, on, fish, path)}


def test_amor_lakes_2_replaces_region_1s_4_and_the_char_release_still_binds(tmp_path):
    path = _tiny(tmp_path, [
        {"entry": "z1:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "species_except": ["CHAR"],
         "take": 4, "_rank": 3},
        {"entry": "z1:q", "rule": "q.r7", "species": ["CHAR"], "take": 0, "may_target": 1,
         "_rank": 3},
        {"entry": "r1:amor", "rule": "amor.r1", "species": ["TROUT_CHAR"], "take": 2, "_rank": 0}])
    assert _states(path, "RB", (7, 1)) == {"amor.r1": "speaks"}
    for char in ("DV", "LT", "EB"):
        assert _states(path, char, (7, 1)) == {"q.r7": "speaks"}, char


def test_region_6s_winter_stream_release_no_longer_takes_char(tmp_path):
    """'Trout of any size from streams, Nov 1-June 30' is trout only: a brook trout in a stream in
    December answers to the box's 'Trout/char: 5' (and its char lines), a rainbow is released."""
    path = _tiny(tmp_path, [
        {"entry": "z6:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
        {"entry": "z6:q", "rule": "q.r7", "species": ["TROUT_CHAR"], "species_except": ["CHAR"],
         "take": 0, "may_target": 1, "water": "stream", "_rank": 3,
         "dimension": "daily@water=stream",
         "when": {"dates": [{"from_month": 11, "from_day": 1, "to_month": 6, "to_day": 30}]}}])
    assert _states(path, "EB", (12, 1)) == {"q.r1": "speaks"}
    assert _states(path, "RB", (12, 1)) == {"q.r1": "speaks", "q.r7": "speaks"}


@pytest.mark.needs_bundle
def test_the_book_answers_on_real_sections():
    """On the built bundle: Amor Lake answers a rainbow with its own 2 and a Dolly Varden with
    Region 1's char release; Region 6's winter stream release speaks for a rainbow, not a brook
    trout."""
    need(None, "bundle", BUNDLE, BUNDLE_HINT)
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    sp = db.execute("select species_except from rule where entry_id='z1:trout_quota' and "
                    "rule_id='trout_quota.r1'").fetchone()
    if not sp or "CHAR" not in (sp[0] or ""):
        predates(f"{BUNDLE} predates the 2026-09-28 rulings — point UI_EXPORT_BUNDLE at one")
    sid = lambda eid, rid: db.execute(  # noqa: E731
        "select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "where r.entry_id = ? and r.rule_id = ? and r.via = 'reach'", (eid, rid)).fetchone()[0]
    speaks = lambda s, f, on: {f"{x['entry']}::{x['rule']}"  # noqa: E731
                               for x in R.effective_rules(s, on, f, BUNDLE)
                               if x["state"] == "speaks" and x["type"] == "retention_limit"}
    amor = sid("r1:amor_lake@1-10", "amor_lake.r1")
    assert "r1:amor_lake@1-10::amor_lake.r1" in speaks(amor, "RB", (7, 1))
    assert "z1:trout_quota::trout_quota.r1" not in speaks(amor, "RB", (7, 1))
    assert "z1:trout_quota::trout_quota.r7" in speaks(amor, "DV", (7, 1))
    assert "r1:amor_lake@1-10::amor_lake.r1" not in speaks(amor, "DV", (7, 1))
    stream = sid("z6:trout_char_quota", "trout_char_quota.r7")
    assert "z6:trout_char_quota::trout_char_quota.r7" in speaks(stream, "RB", (12, 1))
    assert "z6:trout_char_quota::trout_char_quota.r7" not in speaks(stream, "EB", (12, 1))


# ================================================================ B — dated zone releases stand
def _shuswap(tmp_path, water: dict) -> str:
    return _tiny(tmp_path, [
        {"entry": "z3:q", "rule": "q.r7", "species": ["LT"], "take": 0, "may_target": 1,
         "_rank": 3,
         "when": {"dates": [{"from_month": 10, "from_day": 15, "to_month": 1, "to_day": 31}]}},
        dict({"entry": "r3:shuswap", "rule": "shuswap.r9", "_rank": 0}, **water)])


def test_a_water_quota_never_silences_a_dated_zone_release(tmp_path):
    """Shuswap Lake's 'Char daily quota = 1 (none under 60 cm)' names lake trout and outranks
    Region 3 by place — yet on Oct 15-Jan 31 the region's lake trout release still speaks beside
    it (user ruling 2026-09-28). Outside those dates only the lake's quota speaks. MUTATION:
    removing the dated-release clause from `beats` silences the release again."""
    path = _shuswap(tmp_path, {"species": ["CHAR"], "take": 1, "lengths": [{"min_cm": 60}]})
    assert _states(path, "LT", (11, 1)) == {"q.r7": "speaks", "shuswap.r9": "speaks"}
    assert _states(path, "LT", (7, 1)) == {"shuswap.r9": "speaks"}
    # an undated water quota naming the fish is no exact statement of the dated release either
    path = _shuswap(tmp_path, {"species": ["LT"], "take": 2})
    assert _states(path, "LT", (11, 1)) == {"q.r7": "speaks", "shuswap.r9": "speaks"}


def test_the_exact_same_statement_on_the_same_dates_replaces_it(tmp_path):
    path = _shuswap(tmp_path, {"species": ["LT"], "take": 1, "when": {"dates": [
        {"from_month": 10, "from_day": 15, "to_month": 1, "to_day": 31}]}})
    assert _states(path, "LT", (11, 1)) == {"shuswap.r9": "speaks"}


def test_a_printed_lift_still_removes_a_dated_zone_release(tmp_path):
    path = _shuswap(tmp_path, {"species": ["LT"], "take": 1,
                               "exempts": [{"entry_id": "z3:q", "rule_id": "q.r7"}]})
    assert _states(path, "LT", (11, 1)) == {"shuswap.r9": "speaks"}


def test_a_blanket_spring_closure_still_closes_a_river_with_a_trout_quota(tmp_path):
    path = _tiny(tmp_path, [
        {"entry": "z3:s", "rule": "s.r1", "species": ["ALL_GAME_FISH"], "take": 0,
         "may_target": 0, "_rank": 3,
         "when": {"dates": [{"from_month": 1, "from_day": 1, "to_month": 6, "to_day": 30}]}},
        {"entry": "r3:river", "rule": "river.r1", "species": ["TROUT_CHAR"], "take": 2,
         "_rank": 0}])
    assert _states(path, "RB", (5, 1)) == {"s.r1": "speaks"}
    assert _states(path, "RB", (7, 1)) == {"river.r1": "speaks"}


@pytest.mark.needs_bundle
def test_shuswap_lake_trout_are_released_oct_15_to_jan_31():
    need(None, "bundle", BUNDLE, BUNDLE_HINT)
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    eid = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26"
    sid = db.execute("select min(sr.sid) from ruleset r join section_ruleset sr on "
                     "sr.set_id = r.set_id where r.entry_id = ? and r.rule_id = ? and "
                     "r.via = 'reach'", (eid, "shuswap_lake.r9")).fetchone()[0]
    got = {f"{x['entry']}::{x['rule']}": x["state"]
           for x in R.effective_rules(sid, (11, 1), "LT", BUNDLE)}
    assert got.get("z3:trout_char_quota::trout_char_quota.r7") == "speaks"
    assert got.get(f"{eid}::shuswap_lake.r9") == "speaks"


def test_only_a_closure_sending_the_reader_to_the_tables_takes_a_derived_lift():
    """Region 8's 'Bass: 0 quota, CLOSED TO FISHING' sits in a box headed '(See tables for
    exceptions)' (p.68): a row naming bass lifts it. The same closure in a table that does not
    send the reader to the tables takes no derived lift. MUTATION: `sends_to_tables` answering
    True gives the second a lift."""
    bass = C.CatalogueRule.model_validate(
        {"rule_id": "s.r1", "type": "retention_limit", "verbatim": "Bass: 0 quota, CLOSED TO "
         "FISHING", "species": ["BASS"], "take": 0, "may_target": False,
         "extents": [{"op": "whole"}]})
    E = lambda eid, text: type("E", (), {"entry_id": eid, "rules": [bass],  # noqa: E731
                                         "regs_verbatim": text})
    row = C.CatalogueRule.model_validate(
        {"rule_id": "w.r1", "type": "retention_limit", "verbatim": "bass daily quota = 8",
         "species": ["BASS"], "take": 8, "extents": [{"op": "whole"}]})
    sent = RM.zone_closures([E("z8:s", "Region 8 Daily Quotas (See tables for exceptions) "
                                       "Bass: 0 quota, CLOSED TO FISHING")])
    silent = RM.zone_closures([E("z3:s", "Bass: 0 quota, CLOSED TO FISHING")])
    assert RM._named_lifts("r8:w", row, sent) and not silent
    assert RM._named_lifts("r3:w", row, silent) == []


def test_every_derived_lift_is_of_a_closure_that_sends_the_reader_to_the_tables(corpus):
    docs = list(corpus.values())
    got = {(e, r.rule_id) for rows in RM.zone_closures(docs).values() for e, r in rows}
    assert got == {("z8:species_quotas", "species_quotas.r1"),
                   ("z8:species_quotas", "species_quotas.r9")}


# ================================================================ C — one half of the channel
KITIMAT = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3"


def test_a_half_of_the_channel_is_said_by_side_both_ways():
    base = {"rule_id": "k.r1", "type": "retention_limit", "species": ["ALL_GAME_FISH"],
            "take": 0, "may_target": False, "extents": [{"op": "whole"}],
            "verbatim": "No Fishing on the west half of river between fishing boundary signs"}
    with pytest.raises(ValueError, match="set side: west"):
        C.CatalogueRule.model_validate(base)
    r = C.CatalogueRule.model_validate(dict(base, side="west"))
    assert C.label_parts(r)["side"] == ("west half of the channel only — on the east half, this "
                                        "water's other regulations apply")
    with pytest.raises(ValueError, match="prints no 'east half"):
        C.CatalogueRule.model_validate(dict(base, side="east",
                                            verbatim="No Fishing between the signs"))
    with pytest.raises(ValueError, match="needs extents"):
        C.CatalogueRule.model_validate(dict(base, side="west", extents=[],
                                            extent_text="between signs"))


def test_kitimats_west_half_closure_is_placed_and_says_so(corpus):
    r = _rule(corpus, KITIMAT, "kitimat_river.r1")
    assert r.side is C.ChannelSide.west and r.extents
    assert "west half" not in r.extent_text
    assert C.label(r) == ("No fishing — between fishing boundary signs near Kitimat Hatchery "
                          "outfall — west half of the channel only — on the east half, this "
                          "water's other regulations apply")


def test_a_one_side_closure_stands_beside_the_other_halfs_rules(tmp_path):
    """MUTATION: dropping the `side` -> part state lets the closure silence the quota."""
    path = _tiny(tmp_path, [
        {"entry": "r6:k", "rule": "k.r1", "species": ["ALL_GAME_FISH"], "take": 0,
         "may_target": 0, "_rank": 0, "side": "west"},
        {"entry": "r6:k", "rule": "k.r4", "species": ["RB"], "take": 5, "origin": "hatchery",
         "_rank": 0, "dimension": "daily@origin=hatchery"}])
    assert _states(path, "RB", (7, 1)) == {"k.r1": "beside", "k.r4": "speaks"}


def test_the_only_half_of_channel_rule_in_the_book_is_kitimats(corpus):
    got = [(e.entry_id, r.rule_id) for e in corpus.values() for r in e.rules
           if C.HALF_OF_CHANNEL.search(r.verbatim or "") or r.side is not None]
    assert got == [(KITIMAT, "kitimat_river.r1")]


def test_a_trout_and_char_lift_of_a_trout_only_line_adds_no_for():
    """Seeley Creek's "no minimum size for trout" (trout and char) lifts Region 6's "Trout under
    30 cm from any stream" (trout only): "… lifted for trout and char" would claim the lift reaches
    char the lifted rule never bound. The `lifts` part and a lift-only line read alike (reviewer,
    2026-09-28)."""
    cover = C._lift_covers_fish
    assert cover("trout and char", ["“Trout (none under 30 cm), from streams”"])
    assert cover("trout and char", ["“Trout and char — release all”"])
    # a slug or another fish's line still says whom the lift is for
    assert not cover("trout and char", ["Trout char winter release"])
    assert not cover("burbot", ["“No spear fishing”"])
