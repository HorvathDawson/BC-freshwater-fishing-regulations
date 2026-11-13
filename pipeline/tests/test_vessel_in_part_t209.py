"""T209 follow-up (owner 2026-11-12): "It should be in the vessel restriction section but it should say the locator text
there".

A vessel rule held on a part of its water no map draws ("various locations (as buoyed and signed)", "in swimming areas")
reaches the app in the day's gear frame as `in_part` (`answers.gear.gear_answer`: reader state `not_yet_mapped`), never in
`vessel` and never widened to the whole water. The app draws `in_part` vessel rules in its Vessel restrictions section,
each chip ending with the rule's `undrawn_part` words (`app/packages/core/src/regs/boats.ts` `inPartVessels`). This pins
the pipeline half of that contract: every such rule is `not_yet_mapped`, carries its locator in the book's words, and has a
kind the app words (aspect speed / towing, or propulsion at a known level) — a rule the app could not word would stay a
caveat note instead (`caveats.gearInPart` keeps what `inPartVessels` does not take).
"""
from __future__ import annotations

from pipeline.deliver.answers import gear as G
from pipeline.deliver.bundle.read.facts import not_yet_mapped
from pipeline.regs.parsing.io import read_all_entries

#: the ten "close wording" rules of curation-review/on-parts.md, with the words the app's chip ends with
TEN = {
    ("r2:alouette_lake@2-8", "alouette_lake.r2"): "in swimming areas, as buoyed and signed",
    ("r2:harrison_river_from_the_fraser_river_upstream_to_harrison_la@2-18", "harrison_river.r3"):
        "in small bays along the river as signed",
    ("r4:columbia_river@4-15+4-26+4-34+4-38+4-8", "columbia_river.r12"): "in wetlands",
    ("r8:kalamalka_lake@8-22", "kalamalka_lake.r3"): "various locations (as buoyed and signed)",
    ("r8:kalamalka_lake@8-22", "kalamalka_lake.r4"): "various locations (as buoyed and signed)",
    ("r8:okanagan_lake@8-10", "okanagan_lake.r4"): "various locations (as buoyed and signed)",
    ("r8:okanagan_lake@8-10", "okanagan_lake.r5"): "various locations (as buoyed and signed)",
    ("r8:osoyoos_lake@8-1", "osoyoos_lake.r7"): "in 5 signed swimming areas",
    ("r8:skaha_lake@8-9", "skaha_lake.r5"): "various locations (as buoyed and signed)",
    ("r8:skaha_lake@8-9", "skaha_lake.r6"): "various locations (as buoyed and signed)",
}
#: what `boats.boatKind` words (app/packages/core/src/regs/boats.ts)
APP_LEVELS = {"none", "unpowered", "electric_only", "power_capped"}


def _vessel_in_part():
    raw = read_all_entries()
    return {(eid, r["rule_id"]): r for eid, e in raw.items() for r in e.get("rules") or []
            if r.get("type") == "vessel_rule" and str(r.get("undrawn_part") or "").strip()}


def test_the_ten_are_vessel_rules_of_an_undrawn_part_with_the_books_locator():
    got = _vessel_in_part()
    for k, words in TEN.items():
        assert k in got, k
        assert got[k]["undrawn_part"] == words, k
        assert not_yet_mapped(got[k]), k


def test_every_vessel_rule_of_an_undrawn_part_has_a_kind_the_app_words():
    bad = [k for k, r in _vessel_in_part().items()
           if not (r.get("aspect") in ("speed", "towing") or (r.get("aspect") == "propulsion" and r.get("level") in APP_LEVELS))]
    assert bad == []


def test_the_gear_answer_lists_an_undrawn_vessel_rule_in_part_never_in_vessel():
    """The frame the app reads: `in_part` holds the rule, `vessel` does not (the app moves it to its vessel section)."""
    rule = G.Rule("k", 0, {"family": "vessel", "type": "vessel_rule", "aspect": "speed",
                                 "undrawn_part": "various locations (as buoyed and signed)"}, ())
    ans = G.resolve([], "lake", (), in_part=[rule])
    assert ans["in_part"] == ["k"]
    assert ans["vessel"] == {"active": [], "timed": []}
