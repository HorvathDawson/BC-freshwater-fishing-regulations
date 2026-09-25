"""A rule that holds in a PART of a water nothing draws: `undrawn_part`.

"No fishing in Salmon Arm Bay, west of the line between Engineer's Point and Sunnybrae Point" has
no polygon to bind to. Bound as the bare whole lake it closed all of Shuswap Lake; unbound it
vanished from every screen, and on a closure silence reads as permission. `undrawn_part` is the
sanctioned third shape: bound to the water it is in, marked in the key as holding only in the
named part, so a reader shows it as a note and never colours the water by it.

Also here, because they are the same kind of fix — a field saying something on a rule it does not
apply to: `period` belongs to the two counting types, `obligation: must` is the default.
"""
from __future__ import annotations

import json

import pytest

from pipeline.deliver.bundle.rules import _rule_row
from pipeline.regs.parsing.catalogue import CatalogueRule, Period, label, label_parts

BAY = "Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point"
CLOSED = {"rule_id": "shuswap_lake.r5", "type": "retention_limit",
          "verbatim": "No fishing in Salmon Arm Bay, Sept 1-Dec 31",
          "species": ["ALL_GAME_FISH"], "take": 0, "may_target": False,
          "when": {"dates": [{"from_month": 9, "from_day": 1, "to_month": 12, "to_day": 31}]}}


def rule(**kw):
    return CatalogueRule.model_validate({**CLOSED, **kw})


# ---------------------------------------------------------------------------------------
# The model: the sanctioned shape, and the ambiguous ones refused
# ---------------------------------------------------------------------------------------
def test_the_whole_water_with_its_undrawn_part_is_the_sanctioned_shape():
    r = rule(extents=[{"op": "whole", "item_id": "wbk:329518145"}], undrawn_part=BAY)
    assert r.undrawn_part == BAY


def test_the_bare_whole_beside_a_place_in_words_is_still_refused_and_names_the_field():
    with pytest.raises(ValueError, match="undrawn_part"):
        rule(extents=[{"op": "whole"}], extent_text=BAY)


def test_a_part_of_nothing_is_refused():
    """With no extents there is no water for it to be a part of: that rule keeps its words in
    `extent_text` and stays unbound."""
    with pytest.raises(ValueError, match="undrawn_part needs extents"):
        rule(undrawn_part=BAY)


def test_one_place_phrase_not_two():
    with pytest.raises(ValueError, match="both the rule's place phrase"):
        rule(extents=[{"op": "whole", "item_id": "wbk:329518145"}], undrawn_part=BAY,
             extent_text="Shuswap Lake")


def test_a_standing_rule_is_never_also_a_part():
    with pytest.raises(ValueError, match="standing"):
        rule(extents=[{"op": "whole"}], undrawn_part=BAY, standing=True, review_reason="x")


def test_a_blank_part_is_refused():
    with pytest.raises(ValueError, match="blank"):
        rule(extents=[{"op": "whole"}], undrawn_part="  ")


def test_the_label_names_the_part_as_a_part():
    r = rule(extents=[{"op": "whole"}], undrawn_part=BAY)
    assert label(r) == f"No fishing, Sep 1-Dec 31 — in part: {BAY}"
    # the water it is in is its `where`; the part is its own part, never merged into the place
    parts = label_parts(r, place_of=lambda ext: "Shuswap Lake")
    assert (parts["where"], parts["in_part"]) == ("Shuswap Lake", BAY)


def test_an_undrawn_part_that_is_a_list_item_is_refused():
    with pytest.raises(ValueError, match="list marker"):
        rule(extents=[{"op": "whole"}], undrawn_part="(b) Salmon Arm Bay")


# ---------------------------------------------------------------------------------------
# period: only on the types that count fish
# ---------------------------------------------------------------------------------------
BAIT = {"rule_id": "x.r2", "type": "bait_restriction", "verbatim": "Bait ban",
        "gear": [{"slot": "bait", "ban": ["any_bait"]}], "extents": [{"op": "whole"}]}


def test_a_bait_ban_carries_no_clock():
    r = CatalogueRule.model_validate(BAIT)
    assert r.period is None
    with pytest.raises(ValueError, match="period belongs to"):
        CatalogueRule.model_validate({**BAIT, "period": "daily"})


def test_an_advisory_carries_no_clock():
    with pytest.raises(ValueError, match="period belongs to"):
        CatalogueRule.model_validate({"rule_id": "a.r1", "type": "advisory", "verbatim": "Quotas",
                                      "period": "possession", "extents": [{"op": "whole"}]})


def test_a_quota_with_no_period_counts_daily_and_says_so_only_through_clock():
    r = CatalogueRule.model_validate({"rule_id": "q.r1", "type": "retention_limit",
                                      "verbatim": "Trout daily quota = 2", "species": ["RB"],
                                      "take": 2, "extents": [{"op": "whole"}]})
    assert r.period is None and r.clock is Period.daily
    assert r.dimension == "daily"
    assert label(r) == "Rainbow trout — 2 per day"


def test_stop_fishing_after_quota_may_state_its_clock():
    CatalogueRule.model_validate({"rule_id": "s.r1", "type": "stop_fishing_after_quota",
                                  "verbatim": "stop fishing after your daily quota",
                                  "species": ["ST"], "period": "daily",
                                  "extents": [{"op": "whole"}]})


# ---------------------------------------------------------------------------------------
# Labels: one line, no markdown, and two rules never read as one
# ---------------------------------------------------------------------------------------
def test_the_book_s_emphasis_never_reaches_a_label():
    r = CatalogueRule.model_validate({"rule_id": "h.r1", "type": "hazard",
                                      "verbatim": "**WARNING! Dangerous thin ice due to aeration!**",
                                      "extents": [{"op": "whole"}]})
    assert label(r) == "WARNING! Dangerous thin ice due to aeration!"


def test_a_label_is_one_line():
    r = CatalogueRule.model_validate({"rule_id": "h.r1", "type": "advisory",
                                      "verbatim": "When you have\ncaught your quota",
                                      "extents": [{"op": "whole"}]})
    assert label(r) == "When you have caught your quota"


def test_a_clause_counts_on_its_parent_s_clock():
    """Bennett Lake: "Lake trout daily and possession quotas = 2 (only 1 over 90 cm, none between
    60 cm and 90 cm)" is a clause under EACH quota; without the clock they read identically."""
    lengths = [{"min_cm": 60, "max_cm": 90, "take": 0}, {"min_cm": 90}]
    base = {"type": "retention_limit", "species": ["LT"], "extents": [{"op": "whole"}]}
    daily = CatalogueRule.model_validate({**base, "rule_id": "b.r1", "take": 2,
                                          "verbatim": "Lake trout daily quota = 2"})
    poss = CatalogueRule.model_validate({**base, "rule_id": "b.r3", "take": 2,
                                         "period": "possession",
                                         "verbatim": "Lake trout possession quota = 2"})
    c1 = CatalogueRule.model_validate({**base, "rule_id": "b.r2", "take": 1, "within": "b.r1",
                                       "lengths": lengths, "verbatim": "only 1 over 90 cm"})
    c2 = CatalogueRule.model_validate({**base, "rule_id": "b.r4", "take": 1, "within": "b.r3",
                                       "lengths": lengths, "verbatim": "only 1 over 90 cm"})
    sib = {x.rule_id: x for x in (daily, poss, c1, c2)}
    assert label(c1, sib) != label(c2, sib)
    assert label_parts(c2, sib)["conditions"] == "in possession"
    assert "conditions" not in label_parts(c1, sib)


def test_one_sentence_lifting_two_defaults_reads_as_two_rules():
    said = "EXEMPT from Apr 1-June 14 closure AND from Nov 1-Mar 31 trout/char catch and release"
    a = CatalogueRule.model_validate({"rule_id": "k.r2", "type": "retention_limit",
                                      "verbatim": said, "species": ["ALL_GAME_FISH"],
                                      "exempts": [{"default_id": "spring_stream_closure"}],
                                      "extents": [{"op": "whole"}]})
    b = CatalogueRule.model_validate({"rule_id": "k.r3", "type": "retention_limit",
                                      "verbatim": said, "species": ["TROUT_CHAR"],
                                      "exempts": [{"default_id": "trout_char_winter_release"}],
                                      "extents": [{"op": "whole"}]})
    sib = {"k.r2": a, "k.r3": b}
    assert label(a, sib) != label(b, sib)
    # A RULE THAT ONLY LIFTS says so in generated words (decision 2, 2026-09-24): its line was the
    # book's sentence plus "— lifts …". No part carries the sentence, and `lifts` is not repeated.
    assert label_parts(a) == {"what": "Spring stream closure lifted"}
    assert label(a, sib) == "Spring stream closure lifted"
    assert label(b, sib) == "Trout char winter release lifted for trout and char"


def test_a_lift_names_what_it_lifts_in_words_never_by_slug():
    """"Columbia lake s tributaries lifted" was the lifted ENTRY's slug. With the corpus to hand a
    lift names a zone default by its zone entry's name (not its place), another water's rule by
    that water and the rule's kind, and a zone rule by its own generated line."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry

    def entry(eid, name, display, rules):
        return CatalogueEntry.model_validate({
            "entry_id": eid, "name": name, "display_name": display, "region": eid[1],
            "regs_verbatim": " ".join(r["verbatim"] for r in rules), "rules": rules})
    zone = entry("z4:trout_char_winter_release", "Trout and char winter release",
                 "Every stream in Region 4", [
                     {"rule_id": "trout_char_winter_release.r1", "type": "retention_limit",
                      "verbatim": "release", "species": ["TROUT_CHAR"], "take": 0,
                      "may_target": True, "extents": [{"op": "whole"}]}])
    quota = entry("z4:species_quotas", "Other daily quotas", "Region 4", [
        {"rule_id": "species_quotas.r1", "type": "retention_limit", "verbatim": "Bass: 0",
         "species": ["BASS"], "take": 0, "may_target": False, "extents": [{"op": "whole"}]}])
    lake = entry("r4:columbia_lake_s_tributaries@4-25", "COLUMBIA LAKE'S TRIBUTARIES",
                 "Columbia Lake", [
                     {"rule_id": "columbia_lake_tributaries.r1", "type": "retention_limit",
                      "verbatim": "No fishing", "species": ["ALL_GAME_FISH"], "take": 0,
                      "may_target": False, "tributaries_only": True,
                      "extents": [{"op": "whole"}]}])
    entries = {e.entry_id: e for e in (zone, quota, lake)}

    def lifter(exempts, species=("ALL_GAME_FISH",), take=None):
        return CatalogueRule.model_validate({
            "rule_id": "w.r1", "type": "retention_limit", "verbatim": "EXEMPT", "take": take,
            "species": list(species), "exempts": exempts, "extents": [{"op": "whole"}]})
    dutch = lifter([{"target": "columbia_lake_tributaries.r1",
                     "entry_id": "r4:columbia_lake_s_tributaries@4-25"}])
    assert label(dutch, entries=entries) == "Columbia Lake's tributaries closure lifted"
    assert label(dutch) == "Columbia lake s tributaries lifted"      # the slug, with nothing
    duncan = lifter([{"default_id": "trout_char_winter_release"}], species=["BT"])
    assert label(duncan, entries=entries) == "Trout and char winter release lifted for bull trout"
    bass = lifter([{"target": "species_quotas.r1", "entry_id": "z4:species_quotas"}],
                  species=["BASS"], take=5)
    assert label_parts(bass, entries=entries)["lifts"] == "lifts “No fishing for bass”"


# ---------------------------------------------------------------------------------------
# The bundle row
# ---------------------------------------------------------------------------------------
def _row(raw):
    from pipeline.tests.test_bundle_rule_rows import _cols
    return dict(zip(_cols(), _rule_row("r3:x@3-1", raw, uncertain=False)))


def test_the_part_ships_as_its_own_column_never_in_conditions():
    row = _row({**CLOSED, "extents": [{"op": "whole"}], "undrawn_part": BAY})
    assert row["undrawn_part"] == BAY
    assert "undrawn_part" not in json.loads(row["conditions"])
    assert row["label"].endswith(f"in part: {BAY}")
    assert _row({**CLOSED, "extents": [{"op": "whole"}]})["undrawn_part"] is None


def test_a_counting_rule_ships_its_clock_and_no_other_rule_ships_one():
    assert json.loads(_row({**CLOSED, "extents": [{"op": "whole"}]})["conditions"])[
        "period"] == "daily"
    assert "period" not in json.loads(_row(BAIT)["conditions"])


def test_law_is_the_default_and_only_advice_is_said():
    assert "obligation" not in json.loads(_row(BAIT)["conditions"])
    got = json.loads(_row({"rule_id": "a.r1", "type": "advisory", "verbatim": "please",
                           "obligation": "should", "extents": [{"op": "whole"}]})["conditions"])
    assert got["obligation"] == "should"


# ---------------------------------------------------------------------------------------
# Parts, over the whole corpus: deterministic, in order, never the verbatim's list marker
# ---------------------------------------------------------------------------------------
def test_every_rule_s_parts_are_deterministic_and_in_order():
    from pipeline.regs.parsing.catalogue import (LABEL_PARTS, LICENSING_PARTS, CatalogueEntry,
                                                 licensing_parts)
    from pipeline.regs.parsing.io import read_all_entries
    ents = [CatalogueEntry.model_validate(e) for e in read_all_entries().values()]
    assert ents
    for ce in ents:
        sib = {r.rule_id: r for r in ce.rules}
        for r in ce.rules:
            a, b = label_parts(r, sib), label_parts(r, sib)
            assert a == b and list(a) == [k for k in LABEL_PARTS if k in a], r.rule_id
        for x in ce.licensing:
            a = licensing_parts(x, sib)
            assert a == licensing_parts(x, sib)
            assert list(a) == [k for k in LICENSING_PARTS if k in a], x.id


def test_a_species_closure_by_one_way_of_fishing_names_both():
    """zp:spear_fishing.r1 "Only non-game fish may be speared" is take 0 on every game fish WHILE
    spear fishing. "No fishing by spear fishing" dropped the species: a total spear ban."""
    r = CatalogueRule.model_validate({
        "rule_id": "spear_fishing.r1", "type": "retention_limit",
        "verbatim": "Only non-game fish may be speared", "species": ["ALL_GAME_FISH"], "take": 0,
        "may_target": False, "while": ["spear_fishing"],
        "extents": [{"op": "within", "area_kind": "region"}]})
    assert label_parts(r) == {"what": "No spear fishing for game fish"}


def test_a_lift_names_the_fish_it_lifts_for():
    """A lift-only rule about some fish says which: the Duncan's "exempt from the regional bull
    trout catch and release" lifts the winter release for bull trout, not for all trout/char."""
    r = CatalogueRule.model_validate({
        "rule_id": "duncan_river.r3", "type": "retention_limit",
        "verbatim": "exempt from regional Nov 1-Mar 31 bull trout catch and release",
        "species": ["BT"], "exempts": [{"default_id": "trout_char_winter_release"}],
        "extents": [{"op": "whole"}]})
    assert label_parts(r) == {"what": "Trout char winter release lifted for bull trout"}


def test_spear_fishing_is_a_method_and_its_parts_read_naturally():
    """Decision 3 (2026-09-24): spear fishing is a METHOD, not a quota. "Only non-game fish may be
    speared" is a ban on spearing WHEN FISHING FOR game fish; "except burbot, which may also be
    speared in Regions 3, 5, 6, 7 and 8" is the same method allowed for burbot, lifting that ban."""
    ban = CatalogueRule.model_validate({
        "rule_id": "spear_fishing.r1", "type": "method_rule",
        "verbatim": "Only non-game fish (such as carp) may be speared",
        "gear": [{"slot": "method", "ban": ["spear_fishing"],
                  "when": {"targeting": ["ALL_GAME_FISH"]}}],
        "extents": [{"op": "within", "area_kind": "region"}]})
    burbot = CatalogueRule.model_validate({
        "rule_id": "spear_fishing.r2", "type": "method_rule",
        "verbatim": "except burbot, which may also be speared",
        "gear": [{"slot": "method", "allow": ["spear_fishing"], "when": {"targeting": ["BB"]}}],
        "exempts": [{"target": "spear_fishing.r1"}],
        "extents": [{"op": "within", "area_id": "area:region:3"}]})
    assert label_parts(ban) == {"what": "No spear fishing for game fish"}
    assert label_parts(burbot)["what"] == "Spear fishing for burbot allowed"
    # neither displaces the province's unconditional allow, nor each other
    assert ban.dimension == "method:spear_fishing@targeting=ALL_GAME_FISH"
    assert burbot.dimension == "method:spear_fishing@targeting=BB"
