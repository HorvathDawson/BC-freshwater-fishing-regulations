"""`pipeline.deliver.bundle.rules` — the row it writes, and the summary it prints about it.

The summary matters more than a printed line usually would: it is what a person reads to decide
whether a province-wide rebuild is healthy, and there is no second place to check.
"""

import json

from pathlib import Path

import pipeline.deliver.bundle.rules as rules_mod
from pipeline.deliver.bundle.rules import _rule_row


RULE = {
    "rule_id": "x.r1",
    "type": "retention_limit",
    "species": ["DV"],
    "take": 2,
    "verbatim": "Trout daily quota 2",
}


def _fields():
    """The row, by position — the thing that drifts."""
    return _rule_row("r1:x@1-1", dict(RULE), uncertain=True)


def test_the_uncertain_flag_is_the_only_field_that_says_uncertain():
    """**The bug this file exists for.** The summary counted `r[8]`, which is
    `json.dumps(species)` — a string that is never empty ("[]" at minimum) and so always
    truthy. Every build reported every rule as uncertain: 3,422 of 3,422. The column itself
    was always right, so nothing downstream broke and nothing failed; only the number a person
    reads to judge a build was meaningless, and it was wrong in the safe-looking direction.

    Pinned by VALUE rather than by index, so moving a field cannot quietly restore it.
    """
    on = _rule_row("r1:x@1-1", dict(RULE), uncertain=True)
    off = _rule_row("r1:x@1-1", dict(RULE), uncertain=False)
    differ = [i for i, (a, b) in enumerate(zip(on, off)) if a != b]
    assert len(differ) == 1, f"uncertain changed {len(differ)} fields: {differ}"
    assert on[differ[0]] == 1 and off[differ[0]] == 0


def test_an_empty_json_collection_is_truthy_and_so_can_never_stand_in_for_a_flag():
    """Why the old index was silently wrong rather than loudly wrong. Several fields are JSON
    strings, and a JSON string for an EMPTY collection is "[]" — two characters, and truthy.
    A `if r[i]` test on any of them is true for every rule in the corpus.

    This rule names no `species_except` and no windows, so those fields are as empty as the
    row ever gets; they are still truthy."""
    row = _rule_row("r1:x@1-1", dict(RULE), uncertain=False)
    blanks = [f for f in row if isinstance(f, str) and f in ("[]", "{}")]
    assert blanks, "expected an empty-collection JSON string in the row"
    assert all(bool(b) for b in blanks), "'[]' is truthy — that is the trap"


def test_the_row_is_the_length_the_insert_expects():
    """The INSERT names its columns because a positional list once wrote a 42 MB bundle with
    zero entries in it. This holds the row to the same count from the other side."""
    assert len(_fields()) == len(_cols()) == 22


def test_species_survive_as_json_not_python_repr():
    # By NAME — an index here moved when `when_`/`while_` became columns.
    assert json.loads(_named(dict(RULE))["species"]) == ["DV"]


def _cols():
    """The INSERT's own column list, so a test can name a field instead of an index."""
    import re
    src = Path(rules_mod.__file__).read_text()
    m = re.search(r'INSERT INTO rule \((.*?)\)\s*"\s*"VALUES', src, re.S)
    return [c.strip() for c in re.sub(r'"\s*"', "", m.group(1)).split(",")]


def _named(raw, **kw):
    return dict(zip(_cols(), _rule_row("r1:x@1-1", raw, uncertain=False, **kw)))


def test_the_season_ships_from_when_and_only_from_when():
    """EVERY SEASON WAS LOST. The column was filled from the rule's `windows`/`dates` — prose-era
    fields a catalogue rule does not have — so all 3,269 rules shipped `[]`, which the app reads
    as ALL YEAR: every seasonal closure in force every day. The season is `when`."""
    got = _named({**RULE, "when": {"dates": [
        {"from_month": 6, "from_day": 1, "to_month": 6, "to_day": 30}],
        "weekdays": ["Saturday"]}})
    when = json.loads(got["when_"])
    assert when["dates"] == [{"from_month": 6, "from_day": 1, "to_month": 6, "to_day": 30}]
    assert when["weekdays"] == ["Saturday"]
    # one home: not also in `conditions`
    assert "when" not in json.loads(got["conditions"] or "{}")
    # no season = NULL = all year, and a prose `dates` key is refused by the model, never read
    assert _named(dict(RULE))["when_"] is None
    import pytest
    with pytest.raises(Exception):
        _rule_row("r1:x@1-1", {**RULE, "dates": ["Jun 1-30"]}, uncertain=False)


def test_hours_and_unparsed_seasons_ship_too():
    """A night closure's hours and a season nobody could read both reach the client — an
    unparsed season dropped here would make the rule all year."""
    got = _named({**RULE, "when": {"hours": {"start": {"at": "21:00"}, "end": {"at": "05:00"}},
                                   "unparsed": ["To be determined"]}})
    when = json.loads(got["when_"])
    assert when["hours"]["start"]["at"] == "21:00" and when["unparsed"] == ["To be determined"]


def test_while_is_a_column_because_it_decides_an_outcome():
    """"Only non-game fish may be speared" is take 0 on every game fish WHILE spear fishing.
    The app looked for `conditions.method` — retired — and every river in B.C. read CLOSED."""
    got = _named({**RULE, "take": 0, "may_target": False, "species": ["ALL_GAME_FISH"],
                  "while": ["spear_fishing"], "verbatim": "only non-game fish may be speared"})
    assert json.loads(got["while_"]) == ["spear_fishing"]
    assert "while" not in json.loads(got["conditions"] or "{}")
    assert _named(dict(RULE))["while_"] is None


def test_standing_is_a_column_because_it_decides_an_outcome():
    """"No fishing within 23 m downstream of any fishway" holds everywhere at places no dataset
    draws. Read as the closure it literally is, it painted every section of B.C. CLOSED."""
    got = _named({**RULE, "take": 0, "may_target": False, "species": ["ALL_GAME_FISH"],
                  "standing": True, "review_reason": "no dataset of fishways",
                  "verbatim": "Within 23 m downstream of the lower entrance to any fishway"})
    assert got["standing"] == 1 and "standing" not in json.loads(got["conditions"] or "{}")
    assert _named(dict(RULE))["standing"] == 0


def test_a_rule_ships_its_own_extents_and_never_its_entrys():
    """139 rules the reach builder left UNBOUND ("on parts", a place it could not draw) shipped
    `extents: [{op: whole}]` borrowed from their entry, beside `uncertain = 1`: the bundle claiming
    the whole water for a rule placement refused to widen (AGENTS 13)."""
    import inspect
    assert "entry_extents" not in inspect.signature(_rule_row).parameters
    bare = _named({**RULE, "extent_text": "on parts"})
    assert "extents" not in json.loads(bare["conditions"] or "{}")
    own = _named({**RULE, "extents": [{"op": "upstream_of", "splits": ["x"]}]})
    assert json.loads(own["conditions"])["extents"] == [{"op": "upstream_of", "splits": ["x"]}]


def test_when_open_is_gone_and_refused():
    """"Where open" said nothing: a rule only binds while the water is open at all."""
    import pytest
    assert "when_open" not in json.loads(_named(dict(RULE))["conditions"] or "{}")
    with pytest.raises(Exception):
        _rule_row("r1:x@1-1", {**RULE, "when_open": True}, uncertain=False)


ZONES = {"spring_stream_closure": ["z3:spring_stream_closure", "z4:spring_stream_closure",
                                   "z7a:spring_stream_closure"],
         "steelhead_stream_closure": ["z6:steelhead_stream_closure"],
         "trout_char_winter_release": ["z4:trout_char_winter_release"]}


def _cr(rid, **kw):
    from pipeline.regs.parsing.catalogue import CatalogueRule
    return CatalogueRule.model_validate({"rule_id": rid, "type": "retention_limit",
                                         "verbatim": "x", "species": ["ALL_GAME_FISH"],
                                         "take": 0, "may_target": False, **kw})


def _bait(rid, **kw):
    from pipeline.regs.parsing.catalogue import CatalogueRule
    return CatalogueRule.model_validate({"rule_id": rid, "type": "bait_restriction",
                                         "verbatim": "x", "gear": [
                                             {"slot": "bait", "ban": ["fin_fish"]}], **kw})


RULES_OF = {
    "z3:spring_stream_closure": {"spring_stream_closure.r1": _cr("spring_stream_closure.r1")},
    "z4:spring_stream_closure": {"spring_stream_closure.r1": _cr("spring_stream_closure.r1")},
    "z7a:spring_stream_closure": {"spring_stream_closure.r1": _cr("spring_stream_closure.r1"),
                                  "spring_stream_closure.r2": _cr("spring_stream_closure.r2")},
    "z6:steelhead_stream_closure": {
        "steelhead_stream_closure.r1": _cr("steelhead_stream_closure.r1")},
    "z4:trout_char_winter_release": {"trout_char_winter_release.r1": _cr(
        "trout_char_winter_release.r1", species=["TROUT_CHAR"], may_target=True)},
    "z4:species_quotas": {"species_quotas.r5": _cr("species_quotas.r5", species=["KO"])},
    "zp:bait": {"bait.r1": _bait("bait.r1")},
    "r1:x@1-1": {"x.r1": _cr("x.r1"), "x.r2": _cr("x.r2")},
}


def _lifts(entry_id, exempts, **kw):
    raw = {**RULE, "rule_id": "x.r1", "exempts": exempts, **kw}
    row = dict(zip(_cols(), _rule_row(entry_id, raw, uncertain=False, zones=ZONES,
                                      rules_of=RULES_OF)))
    assert "exempts" not in json.loads(row["conditions"] or "{}"), "one home: its own column"
    return json.loads(row["exempts"]) if row["exempts"] else None


def test_exempts_ship_resolved_to_each_rule_they_lift():
    """EXEMPTIONS WERE APPLIED NOWHERE — shipped inside `conditions`, which no client reads. Now
    each item names ONE rule, never a whole entry: the whole-entry item is what let a bull-trout
    exemption lift a trout/char release wholesale."""
    all_ = {"species": ["ALL_GAME_FISH"]}
    # a zone default by slug: each rule of the rule's OWN region's zone entry
    assert _lifts("r3:north_thompson@3-27", [{"default_id": "spring_stream_closure"}],
                  **all_) == [{"entry_id": "z3:spring_stream_closure",
                               "rule_id": "spring_stream_closure.r1"}]
    # Region 7's rows reach both of its zones, and every rule of each
    assert _lifts("r7:x@7-1", [{"default_id": "spring_stream_closure"}], **all_) == [
        {"entry_id": "z7a:spring_stream_closure", "rule_id": "spring_stream_closure.r1"},
        {"entry_id": "z7a:spring_stream_closure", "rule_id": "spring_stream_closure.r2"}]
    # a target in another entry, named — KO lifts KO outright
    assert _lifts("r4:upper_arrow@4-31", [{"target": "species_quotas.r5",
                                           "entry_id": "z4:species_quotas"}],
                  species=["KO"]) == [{"entry_id": "z4:species_quotas",
                                       "rule_id": "species_quotas.r5"}]
    # a target in this entry
    assert _lifts("r1:x@1-1", [{"target": "x.r2"}], **all_) == [
        {"entry_id": "r1:x@1-1", "rule_id": "x.r2"}]


def test_a_lift_is_never_wider_than_its_lifter():
    """Duncan River (bull trout) lifted the whole trout/char release; the sturgeon bait lift lifted
    the fin-fish ban for every angler. Each lift now carries what it holds for."""
    duncan = _lifts("r4:duncan_river@4-19", [{"default_id": "trout_char_winter_release"}],
                    species=["DV"])
    assert duncan == [{"entry_id": "z4:trout_char_winter_release",
                       "rule_id": "trout_char_winter_release.r1", "species": ["DV"]}]
    whole = _lifts("r4:columbia@4-15", [{"default_id": "trout_char_winter_release"}],
                   species=["TROUT_CHAR"])
    assert whole == [{"entry_id": "z4:trout_char_winter_release",
                      "rule_id": "trout_char_winter_release.r1"}]
    # fish the lifted rule does not speak about: nothing is lifted, and the build says so
    import pytest
    with pytest.raises(SystemExit, match="lifts no rule"):
        _lifts("r4:x@4-1", [{"default_id": "trout_char_winter_release"}], species=["KO"])
    # a target: lifted only when fishing FOR sturgeon; only while set lining
    from pipeline.deliver.bundle.rules import _exempts
    sturgeon = _bait("bait.r3", when_targeting=["WSG"], exempts=[{"target": "bait.r1"}],
                     gear=[{"slot": "bait", "allow": ["dead_fin_fish"]}],
                     extents=[{"op": "whole", "item_id": "gnis:15333"}])
    assert json.loads(_exempts("zp:bait", sturgeon, ZONES, RULES_OF)) == [
        {"entry_id": "zp:bait", "rule_id": "bait.r1", "when_targeting": ["WSG"]}]
    setline = _bait("bait.r2", exempts=[{"target": "bait.r1"}], **{"while": ["set_lining"]},
                    gear=[{"slot": "bait", "allow": ["dead_fin_fish"]}],
                    water="lake", extents=[{"op": "within", "area_id": "area:region:6",
                                            "feature_types": ["lake"]}])
    assert json.loads(_exempts("zp:bait", setline, ZONES, RULES_OF)) == [
        {"entry_id": "zp:bait", "rule_id": "bait.r1", "while": ["set_lining"]}]


def test_a_lift_is_in_force_only_while_its_lifter_is():
    """A LIFT CARRIED NO TIME, and an item with no qualifier lifts its rule outright — so a lifter
    printed for some days lifted on every day. Chilliwack/Vedder's hatchery rainbow quota (Jul 1-Apr
    30) lifted Region 2's "2 from streams" in May and June too. The lift now carries the lifter's
    `when` wherever the lifter is not in force every day the lifted rule is, and lifts NOTHING when
    the two never share a day (Adams River's bull trout quota, Jul 1-30 and Nov 1-Dec 31, against
    Region 3's Aug 1-Oct 31 stream release: they speak on different days)."""
    import pytest
    from pipeline.deliver.bundle.rules import _exempts
    from pipeline.regs.parsing.catalogue import CatalogueRule

    def rule(rid, when=None, **kw):
        return CatalogueRule.model_validate({
            "rule_id": rid, "type": "retention_limit", "verbatim": "x", "species": ["DV"],
            "take": 1, **({"when": {"dates": when}} if when else {}), **kw})

    d = lambda fm, fd, tm, td: {"from_month": fm, "from_day": fd, "to_month": tm, "to_day": td}
    release = rule("z.r6", [d(8, 1, 10, 31)], take=0, may_target=True)
    allyear = rule("z.r1", take=0, may_target=True)
    rules_of = {"z3:q": {"z.r6": release, "z.r1": allyear}}

    def lifts(target, when):
        r = rule("a.r4", when, exempts=[{"target": target, "entry_id": "z3:q"}])
        got = _exempts("r3:a@3-1", r, {}, rules_of)
        return json.loads(got) if got else None

    # a dated lifter against an all-year rule: in force only on the lifter's days
    assert lifts("z.r1", [d(7, 1, 4, 30)]) == [
        {"entry_id": "z3:q", "rule_id": "z.r1",
         "when": {"dates": [d(7, 1, 4, 30)]}}]
    # the lifter covers every day the lifted rule binds: lifted outright, no qualifier
    assert lifts("z.r6", [d(7, 1, 11, 30)]) == [{"entry_id": "z3:q", "rule_id": "z.r6"}]
    assert lifts("z.r6", None) == [{"entry_id": "z3:q", "rule_id": "z.r6"}]
    # they overlap in part: the lift names the lifter's days
    assert lifts("z.r6", [d(10, 1, 12, 31)]) == [
        {"entry_id": "z3:q", "rule_id": "z.r6", "when": {"dates": [d(10, 1, 12, 31)]}}]
    # ADAMS: never on the same day — nothing is lifted, and the build refuses the empty lift
    with pytest.raises(SystemExit, match="lifts no rule"):
        lifts("z.r6", [d(7, 1, 7, 30), d(11, 1, 12, 31)])
    # hours never cover a whole day, so a lifter with hours always carries its `when`
    r = CatalogueRule.model_validate({
        "rule_id": "a.r5", "type": "retention_limit", "verbatim": "x", "species": ["DV"],
        "take": 1, "exempts": [{"target": "z.r1", "entry_id": "z3:q"}],
        "when": {"hours": {"start": {"at": "06:00"}, "end": {"at": "18:00"}}}})
    from pipeline.deliver.bundle.rules import _when
    # the lifter's `when` exactly as its own `when_` column ships it
    assert json.loads(_exempts("r3:a@3-1", r, {}, rules_of))[0]["when"] == json.loads(_when(r))


def test_a_lifter_whose_water_its_placement_does_not_enforce_stops_the_build():
    """`water: lake` on a lifter is enforced where it is PLACED (`feature_types`), because the
    client has no water kind to check it by. A lake-only lift placed on every water is refused."""
    import pytest
    from pipeline.deliver.bundle.rules import _exempts
    loose = _bait("bait.r2", exempts=[{"target": "bait.r1"}], water="lake",
                  gear=[{"slot": "bait", "allow": ["dead_fin_fish"]}],
                  extents=[{"op": "within", "area_id": "area:region:6"}])
    with pytest.raises(SystemExit, match="lifts only on lakes"):
        _exempts("zp:bait", loose, ZONES, RULES_OF)


def test_an_exemption_that_names_nothing_stops_the_build_unless_it_says_why():
    import pytest
    with pytest.raises(SystemExit, match="lifts no rule"):
        _lifts("r1:x@1-1", [{"target": "x.r9"}])
    with pytest.raises(SystemExit, match="lifts no rule"):
        _lifts("r2:x@2-1", [{"default_id": "spring_stream_closure"}])     # no Region 2 zone
    # THE SELF-LIFT: z6's steelhead closure names its own slug. It resolves to nothing, and only
    # its review_reason lets it through — it can never lift itself.
    assert _lifts("z6:steelhead_stream_closure", [{"default_id": "steelhead_stream_closure"}],
                  review_reason="self-lift, known") is None
    with pytest.raises(SystemExit):
        _lifts("r1:x@1-1", [{"target": "x.r1"}])                          # itself, by id


def test_every_exemption_in_the_corpus_lifts_a_real_rule_or_says_why():
    """Against the curated corpus: each `exempts` resolves to an entry that exists — the bundle
    would stop otherwise — and the only ones that resolve to nothing carry a review_reason."""
    from pipeline.deliver.bundle.rules import _exempts
    from pipeline.regs.parsing import io
    from pipeline.regs.parsing.catalogue import CatalogueEntry

    ces = [CatalogueEntry.model_validate(e) for e in io.read_entries_dir().values()]
    zones, rules_of = {}, {}
    for ce in ces:
        rules_of[ce.entry_id] = {r.rule_id: r for r in ce.rules}
        if ce.entry_id.startswith("z"):
            zones.setdefault(ce.entry_id.split(":", 1)[1], []).append(ce.entry_id)
    lifted, silent, partial = 0, [], []
    for ce in ces:
        for r in ce.rules:
            if not r.exempts:
                continue
            got = _exempts(ce.entry_id, r, zones, rules_of)
            if got:
                lifted += 1
                for x in json.loads(got):
                    assert set(x) <= set(rules_mod.LIFT_KEYS), x
                    assert x["rule_id"] in rules_of[x["entry_id"]]
                    assert (x["entry_id"], x["rule_id"]) != (ce.entry_id, r.rule_id)
                    if {"species", "when_targeting", "while", "when", "origin"} & set(x):
                        partial.append((ce.entry_id, r.rule_id, x["rule_id"]))
            else:
                silent.append((ce.entry_id, r.rule_id))
    # THE ONES THAT LIFT IN PART, and only those. Region 4's reopened bass/perch/pike/walleye
    # (decision 6, 2026-09-24) each lift the four-species invasive notice for their own fish
    # only — 52 rules on 29 waters; the rest are named.
    notice = [p for p in partial if p[2] == "invasive_species_notice.r1"]
    assert len(notice) == 52 and all(e.startswith("r4:") for e, _, _ in notice)
    # A water's quota that must still replace a CONDITIONED zone line ("2 from streams", "hatchery
    # under 30 cm from streams", "1 over 50 cm") lifts it for the water's own fish (review
    # 2026-09-24: conditions in the key stopped these competing, and the angler got two limits).
    chw = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4"
    kit = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3"
    # RULING 1 (2026-09-26): a water row printing a LARGER number for a fish lifts the zone's
    # trout/char quota (and the clauses of it the row outnumbers) for THAT fish only. The larger
    # number overrides the zone's "1 over 50 cm" too, "(any size)" or not (user ruling
    # 2026-09-26, reversing R8), and that lift carries a caution; a lake printing its OWN lake
    # trout sizes lifts only that clause. The Region 5 "Trout daily
    # quota = 8" lakes are TROUT_CHAR (p.86) and replace the 5 as the same statement (no lift);
    # Lois/Khartoum's aggregate 6 lifts the 4 for rainbow + steelhead (its "2 hatchery steelhead"
    # clause is lifted whole, so it is not listed here).
    larger = [(e, f"{r}.r{n}", f"trout_char_quota.r{t}") for e, r, n, ts in (
        ("r2:ross_lake_boundary_between_ross_lake_and_skagit_river_is_mar@2-2", "ross_lake", 4,
         (1, 2)),
        ("r2:lois_lake@2-12", "lois_lake", 4, (1, 2)),
        ("r2:khartoum_lake@2-12", "khartoum_lake", 5, (1, 2)),
        ("r3:tranquille_lake@3-29", "tranquille_lake", 1, (1, 3)),
        ("r4:beaver_creek@4-8", "beaver_creek", 1, (1,)),
        ("r4:big_fish_dunbar_lake@4-34", "big_fish_dunbar_lake", 1, (1,)),
        ("r4:botts_lake@4-34", "botts_lake", 1, (1,)),
        ("r4:box_lake@4-15", "box_lake", 1, (1,)),
        ("r4:lavington_creek@4-26", "lavington_creek", 1, (1,)),
        ("r4:north_star_lake@4-22", "north_star_lake", 1, (1,)),
        ("r4:perry_creek@4-20", "perry_creek", 3, (1,)),
        ("r4:premier_lake@4-21", "premier_lake", 3, (1,)),
        ("r4:twin_lakes@4-34", "twin_lakes", 1, (1,)),
        ("r4:whitetail_lake@4-26", "whitetail_lake", 3, (1,)),
        ("r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19",
         "kootenay_lake_main_body", 4, (1, 2)),
        ("r4:duncan_river@4-19", "duncan_river", 4, (2,)),
        ("r4:lardeau_river@4-29+4-30", "lardeau_river", 4, (2,)),
        ("r5:quesnel_lake@5-15", "quesnel_lake", 3, (2,)),
        ("r6:atlin_lake@6-25+6-27", "atlin_lake", 1, (2,)),
        ("r6:laidlaw_lake@6-25+6-27", "laidlaw_lake", 1, (2,)),
        ("r6:bennett_lake@6-28", "bennett_lake", 1, (2,)),
        ("r6:morley_lake@6-25", "morley_lake", 1, (2,)),
        ("r6:tagish_lake@6-27", "tagish_lake", 1, (2,)),
        ("r7:bow_lake@7-15", "bow_lake", 1, (1, 2)),
        ("r7:butterfly_lake@7-15", "butterfly_lake", 1, (1, 2)),
        ("r7:camp_lake@7-15", "camp_lake", 1, (1, 2)),
        ("r7:kathie_lake@7-15", "kathie_lake", 1, (1, 2)),
        ("r7:unnamed_lake_kinglet_lake_located_100_m_west_of_butterfly_la@7-15", "kinglet_lake",
         1, (1, 2)),
        ("r7:unnamed_lake_redstart_lake_located_approx_200_m_southwest_of@7-15", "redstart_lake",
         1, (1, 2)),
        ("r7:gwillim_lake@7-21", "gwillim_lake", 1, (2,)),
        ("r7:muncho_lake@7-51", "muncho_lake", 1, (2,)),
        ("r7:williston_lake_in_zone_b@7-31+7-36", "williston_lake_zone_b", 5, (4,)),
        ("r8:jewel_lake@8-14", "jewel_lake", 1, (1, 2))) for t in ts]
    assert sorted(p for p in partial if p[2] != "invasive_species_notice.r1") == sorted(larger + [
        (chw, "chilliwack_vedder_rivers.r9", "trout_char_quota.r4"),
        (chw, "chilliwack_vedder_rivers.r9", "trout_char_quota.r8"),
        ("r2:coquitlam_river@2-8", "coquitlam_river.r3", "trout_char_quota.r8"),
        ("r4:beaver_creek@4-8", "beaver_creek.r1", "trout_char_quota.r3"),
        # DUCK LAKE: two dated rules (Jun 16-May 14, May 15-Jun 15) each lift the bass closure
        # only on their own days — together, the year.
        ("r4:duck_lake_permit_required_see_note_on_page_34@4-6", "duck_lake.r1",
         "species_quotas.r1"),
        ("r4:duck_lake_permit_required_see_note_on_page_34@4-6", "duck_lake.r3",
         "species_quotas.r1"),
        ("r4:duncan_river@4-19", "duncan_river.r2", "trout_char_winter_release.r1"),
        ("r4:duncan_river@4-19", "duncan_river.r4", "trout_char_quota.r3"),
        ("r4:duncan_river@4-19", "duncan_river.r5", "trout_char_quota.r3"),
        ("r4:lardeau_river@4-29+4-30", "lardeau_river.r4", "trout_char_quota.r3"),
        ("r4:lardeau_river@4-29+4-30", "lardeau_river.r5", "trout_char_quota.r3"),
        ("r4:lavington_creek@4-26", "lavington_creek.r1", "trout_char_quota.r3"),
        ("r4:perry_creek@4-20", "perry_creek.r3", "trout_char_quota.r3"),
        ("r4:whiteswan_lake_s_inlet_outlet_streams@4-24",
         "whiteswan_lakes_inlet_outlet_streams.r3", "trout_char_quota.r3"),
        (kit, "kitimat_river.r4", "trout_char_quota.r4"),
        (kit, "kitimat_river.r4", "trout_char_quota.r6"),
        (kit, "kitimat_river.r4", "trout_char_quota.r7"),
        (kit, "kitimat_river.r5", "trout_char_quota.r2"),
        (kit, "kitimat_river.r5", "trout_char_quota.r4"),
        (kit, "kitimat_river.r5", "trout_char_quota.r7"),
        # CLEARWATER LAKE's "Catch and release, May 1-Oct 31" lifts Zone B's species quotas only
        # on its own days (the naming ruling: the zone's "Burbot: 5" names the fish, so without
        # the lift it would outrank the lake's release; the other days the lake is closed).
        # Its grayling quota too, since "tributaries" became streams and the lake left the
        # Williston "release all … and its tributaries" rule (2026-09-25).
        ("r7:clearwater_lake@7-31", "clearwater_lake.r2", "arctic_grayling.r1"),
        ("r7:clearwater_lake@7-31", "clearwater_lake.r2", "arctic_grayling.r3"),
        *[("r7:clearwater_lake@7-31", "clearwater_lake.r2", f"species_quotas.r{k}")
          for k in (1, 12, 2, 3, 4, 6, 7, 8, 9)],
        ("r7:peace_river_from_hwy_29_bridge_to_the_site_c_dam@7-31", "peace_river.r4",
         "trout_char_quota.r3"),
        # REGION 8's "And you may retain: 20 brook trout from streams" (p.68) is counted APART
        # from the trout/char quota: it lifts the quota and its clauses for brook trout only
        # (user ruling 2026-09-25).
        *[("z8:trout_char_quota", "trout_char_quota.r5", f"trout_char_quota.r{k}")
          for k in (1, 2, 3, 4)],
        ("zp:bait", "bait.r2", "bait.r1"), ("zp:bait", "bait.r3", "bait.r1"),
        ("zp:spear_fishing", "spear_fishing.r2", "spear_fishing.r1"),
        # EXEMPTIONS OF 2026-09-29, each in force only on its own days (`when`): Region 1's
        # dated bait-ban rows lift the region's all-year ban outside their dates; Fulton's "Open
        # June 16-Apr 30" lifts the Skeena winter closure; Thompson's "Additional opening, May
        # 1-31" lifts its own closure and Region 3's spring closure; Bella Coola's Apr 1-May 31
        # EXCEPT lifts the row's quota and its cutthroat release.
        ("r1:quatse_river@1-13", "quatse_river.r4x", "bait_ban_streams.r1"),
        ("r1:somass_river@1-7", "somass_river.r2x", "bait_ban_streams.r1"),
        ("r1:sproat_river@1-7", "sproat_river.r2x", "bait_ban_streams.r1"),
        ("r1:stamp_river@1-7", "stamp_river.r4x", "bait_ban_streams.r1"),
        ("r6:fulton_river@6-8", "fulton_river.r1b", "skeena_nass_winter_closure.r1"),
        ("r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t@3-13+3-14+3-18",
         "thompson_river_downstream_of_kamloops_lake.r3o",
         "thompson_river_downstream_of_kamloops_lake.r1"),
        ("r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t@3-13+3-14+3-18",
         "thompson_river_downstream_of_kamloops_lake.r3o", "spring_stream_closure.r1"),
        ("r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8",
         "atnarko_bella_coola_rivers.r5", "atnarko_bella_coola_rivers.r3"),
        ("r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8",
         "atnarko_bella_coola_rivers.r5", "atnarko_bella_coola_rivers.r4")])
    assert lifted >= 80
    # nothing lifts itself: the z6 steelhead exemption is its own rule, placed on the five
    # mainstems (steelhead_stream_closure.r2), so no lift is dropped
    assert silent == []


def test_all_fish_lifts_every_fish_the_lifted_rule_names_but_crayfish():
    """`ALL_FIN_FISH` does not expand (it is an open complement), so as a set it met no fish and
    Pine River's "Catch and release all fish" lifted nothing. It covers every fish the lifted rule
    names — wholly — except crayfish, which the book keeps apart from fin fish."""
    from pipeline.regs.parsing.catalogue import CatalogueRule
    all_fish = CatalogueRule(rule_id="p.r1", type="retention_limit", species=["ALL_FIN_FISH"],
                             take=0, may_target=True, verbatim="Release all fin fish",
                             extents=[{"op": "whole"}])
    burbot = CatalogueRule(rule_id="s.r1", type="retention_limit", species=["BB"], take=5,
                           verbatim="Burbot: 5", extents=[{"op": "whole"}])
    crayfish = CatalogueRule(rule_id="s.r3", type="retention_limit", species=["CRA"], take=25,
                             verbatim="Crayfish: 25", extents=[{"op": "whole"}])
    assert rules_mod._lift_terms(all_fish, burbot) == {}
    assert rules_mod._lift_terms(all_fish, crayfish) is None
