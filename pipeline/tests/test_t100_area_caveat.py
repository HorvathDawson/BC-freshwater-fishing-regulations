"""T100 — LAKES DIVIDED INTO CLOSURE AREAS (RULINGS K5), owner Q43 default 2026-10-23: a rule naming a
part of its water that nothing draws ("widened": bound to the WHOLE water, `undrawn_part` naming the
part) keeps binding the whole water, and the page carries an AREA CAVEAT saying where it holds — and,
where the book prints a map of the part, citing it ("see map A on p.28 of the regulations").

ONE DECISION, ONE FUNCTION. Which book map a part is drawn on is `export_ui_rules.book_map`; the
caveat sentence is the export's `not_yet_mapped.says` (`export_ui_rules.not_yet_mapped`); the
answers READ it — `display.applies_here` ends a widened rule's `applies` with it and
`display.book_pages` adds the map's page to its `book`. The app prints `applies` and `book` as
stored (rule sheet, sources, decided lines, the Keep in-part block's book link); it words nothing.

Mutations, each failing the named test:
  * `book_map` returns None always                 -> test_the_book_maps_of_the_widened_rules,
                                                      test_a_mapped_part_cites_its_map_whole_string
  * `_SEE_MAP_ON_PAGE` misses the row heading      -> test_the_book_maps_of_the_widened_rules (Mahood, Canim)
  * the "(map A)" tag kept beside the cite         -> test_a_mapped_part_cites_its_map_whole_string
  * `applies_here` drops the caveat                 -> test_applies_ends_with_the_area_caveat
  * `book_pages` ignores the map                    -> test_book_cites_the_map_page_after_the_row
  * `undrawn_map_problems` returns []              -> test_a_named_map_with_no_page_is_refused
  * a new rule with an `undrawn_part` in the corpus -> test_the_widened_rules_are_counted
"""
from __future__ import annotations

import json
from collections import Counter

import pytest

from pipeline.common.curated import CURATED
from pipeline.deliver.answers.common import BOOK_TITLE
from pipeline.deliver.answers.display import applies_here, book_pages
from pipeline.tools.export_ui_rules import book_map, not_yet_mapped, undrawn_map_problems

TAIL = " It does not apply to the rest of the water."


def _corpus():
    """(entry, its book facts as the bundle ships them, rule) for every rule of the corpus. The
    bundle's `entry.full_name` is the row's `name`, `entry.verbatim` (the export's `printed`) its
    `regs_verbatim`, `entry.pages` its `source_pages` (`bundle/rules.py` entry rows)."""
    for f in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        for e in json.loads(f.read_text(encoding="utf-8"))["entries"]:
            book = {"full_name": e.get("name"), "printed": e.get("regs_verbatim"),
                    "pages": e.get("source_pages")}
            for r in e.get("rules") or []:
                yield e, book, r


# ---------------------------------------------------------------------------------------------
# The worklist: every widened rule, pinned. A new one fails here until its caveat is looked at.
# ---------------------------------------------------------------------------------------------

#: entry -> how many of its rules are widened (bound to the whole water, holding only in an
#: undrawn part). 135 rules on 87 rows, 2026-10-23. Lakes are most of them (100 rules on lakes, 9 on
#: Hatzic Lake and Slough / Shuswap's lake-and-river row, the rest streams and one zone line).
#: Kootenay, Williston (Zones A/B) and Shannon are SPLIT into parts (`sub_lake_areas.json`) and are
#: not here; Ross Lake / Skagit River are not either — each row binds its whole water by the book.
WIDENED = {
    "r1:beaver_lake@1-1": 1, "r1:brannen_lake@1-5": 1, "r1:courtenay_river@1-6": 1,
    "r1:cowichan_lake_including_bear_lake@1-4": 2, "r1:cowichan_river_see_map_below@1-4": 2,
    "r1:deena_creek@6-12": 1, "r1:elk_lake@1-1": 4, "r1:great_central_lake@1-7": 1,
    "r1:long_lake_nanaimo@1-5": 1, "r1:mamin_river@6-13": 1, "r1:nanaimo_river@1-5": 1,
    "r1:prospect_lake@1-2": 2, "r1:quennell_lake@1-5": 1, "r1:shawnigan_lake@1-2": 2,
    "r1:stamp_river@1-7": 1, "r1:wahpeeto_creek@1-14": 1, "r1:white_river@1-10": 1,
    "r2:alouette_lake@2-8": 2, "r2:chapman_creek@2-5": 1, "r2:coquitlam_river@2-8": 1,
    "r2:cultus_lake@2-3": 2, "r2:green_lake@2-9": 2, "r2:harrison_lake@2-18": 1,
    "r2:harrison_river_from_the_fraser_river_upstream_to_harrison_la@2-18": 1,
    "r2:hatzic_lake_and_slough@2-8": 2, "r2:nicomekl_river@2-4": 1, "r2:nicomen_slough@2-8": 1,
    "r2:pitt_lake@2-8": 2, "r2:pitt_river@2-8": 1, "r2:powell_lake@2-12": 1, "r2:ruby_lake@2-5": 1,
    "r2:sakinaw_lake@2-5": 2, "r2:stave_river@2-8": 2, "r2:strawberry_slough@2-8": 1,
    "r3:adams_lake@3-37": 1, "r3:heffley_lake_parts_of@3-27": 2, "r3:leighton_lake@3-18": 2,
    "r3:loon_lake@3-30": 2, "r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46": 8,
    "r3:mcarthur_island_slough@3-28": 1, "r3:roche_lake@3-20": 1, "r3:shumway_lake@3-20": 1,
    "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26": 7,
    "r3:white_lake@3-26": 1, "r4:columbia_lake@4-25": 1,
    "r4:columbia_river@4-15+4-26+4-34+4-38+4-8": 2, "r4:kinbasket_mcnaughton_lake@4-36": 1,
    "r4:lake_revelstoke@4-38+4-39": 1, "r4:premier_lake@4-21": 3, "r4:trout_lake@4-30": 1,
    "r4:upper_arrow_lake_drawdown_area@4-31+4-32": 3, "r5:canim_lake_see_map_on_page_42@5-1": 1,
    "r5:chilko_lake@5-4": 1, "r5:chilko_river@5-5": 1, "r5:dragon_lake@5-2": 1,
    "r5:green_lake@5-1": 1, "r5:mitchell_lake@5-15": 1, "r5:quesnel_lake@5-15": 2,
    "r5:williams_lake@5-2": 1, "r6:babine_lake@6-6": 2, "r6:buckinghorse_lake@6-20": 1,
    "r6:cranberry_river@6-15": 1, "r6:fran_ois_lake@6-4": 7, "r6:fulton_river@6-8": 1,
    "r6:helene_lake@6-6": 1, "r6:klahowya_lake@6-20": 1, "r6:silverthorne_erickson_lake@6-9": 1,
    "r6:takysie_lake@6-4": 1, "r7:davis_bay_in_finlay_reach_of_williston_lake@7-37": 1,
    "r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31": 1,
    "r7:halfway_river@7-34": 1, "r7:letain_lake@7-52": 1, "r7:nation_arm_williston_lake@7-30": 2,
    "r7:nulki_lake@7-12": 1, "r7:rainbow_lakes@7-52": 1,
    "r7:williston_lake_in_zone_a_includes_waters_500_m_east_upstream@7-30+7-37+7-38": 1,
    "r7:wolverine_lake@7-52": 1, "r8:christina_lake@8-15": 1, "r8:kalamalka_lake@8-22": 2,
    "r8:mabel_lake@8-24": 1, "r8:mara_lake@8-26": 1, "r8:okanagan_lake@8-10": 2,
    "r8:osoyoos_lake@8-1": 2, "r8:rosemond_lake@8-26": 1, "r8:skaha_lake@8-9": 2,
    "r8:wood_lake@8-22": 1, "z7b:peace_river_navigation": 1,
}

#: The widened rules whose part the book draws on a map, and the map each cites (printed pages;
#: verified against the synopsis: PDF 30 = p.28 holds Shuswap's maps A/B/C, Salmon Arm Bay and
#: Mahood Lake; PDF 48 = p.42 Canim Lake; PDF 57 = p.51 "MAP 1 FRANÇOIS LAKE - Outlet").
SHUSWAP = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26::shuswap_lake"
MAHOOD = "r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46::mahood_lake"
MAPS = {
    **{f"{MAHOOD}.r{i}": "the map on p.28" for i in range(1, 9)},
    f"{SHUSWAP}.r1": "map A on p.28", f"{SHUSWAP}.r2": "map A on p.28",
    f"{SHUSWAP}.r3": "map B on p.28", f"{SHUSWAP}.r4": "map C on p.28",
    f"{SHUSWAP}.r5": "the map on p.28",                       # Salmon Arm Bay, "(see map on page 28)"
    f"{SHUSWAP}.r5x": "the maps on p.28", f"{SHUSWAP}.r11x": "the maps on p.28",   # the pier: the row's maps
    "r5:canim_lake_see_map_on_page_42@5-1::canim_lake.r1": "the map on p.42",
    **{f"r6:fran_ois_lake@6-4::francois_lake.r{i}": "map 1 on p.51" for i in range(1, 8)},
}


def test_the_widened_rules_are_counted():
    """Every rule that binds its whole water but holds only in an undrawn part, by row — a new one
    (a parse, a curation) fails here until its area caveat (and any map the book prints) is looked
    at. The one licensing record in a part (the Creston Valley WMA permit) is counted beside."""
    per = Counter(e["entry_id"] for e, _, r in _corpus() if str(r.get("undrawn_part") or "").strip())
    assert dict(sorted(per.items())) == WIDENED
    assert (sum(per.values()), len(per)) == (135, 87)
    lic = sorted(e["entry_id"] for f in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json"))
                 for e in json.loads(f.read_text(encoding="utf-8"))["entries"]
                 for x in e.get("licensing") or [] if str(x.get("undrawn_part") or "").strip())
    assert lic == ["z4:creston_valley_permit"]


def test_the_book_maps_of_the_widened_rules():
    """`book_map` over the corpus as the bundle ships it: exactly these 24 rules cite a map. An
    unidentified "on parts" never does, even on a row that prints one (the Cowichan River)."""
    got = {f"{e['entry_id']}::{r['rule_id']}": mp["cite"] for e, book, r in _corpus()
           if str(r.get("undrawn_part") or "").strip()
           and (mp := book_map(r["undrawn_part"], r.get("verbatim") or "", book))}
    assert got == MAPS


# ---------------------------------------------------------------------------------------------
# The caveat sentence (whole strings, AGENTS 33)
# ---------------------------------------------------------------------------------------------

SHUSWAP_ROW = {"full_name": "SHUSWAP LAKE (see maps on page 28) (includes Little Shuswap Lake, …)",
               "printed": "…", "pages": [32]}


def test_a_mapped_part_cites_its_map_whole_string():
    part = ("waters lying west of a line between signs at Henstridge Road and Wharf Road to a line "
            "between signs on the south and north shores of Little Shuswap Lake (map A)")
    assert not_yet_mapped({"undrawn_part": part}, "No Fishing Mar 15-May 14", SHUSWAP_ROW) == {
        "display": "prominent", "part": part, "identified": True,
        "map": {"name": "map A", "page": 28, "cite": "map A on p.28"},
        "says": "Not yet mapped: this applies only in one part of this water — waters lying west of "
                "a line between signs at Henstridge Road and Wharf Road to a line between signs on "
                "the south and north shores of Little Shuswap Lake — which is not drawn on the map "
                "yet; see map A on p.28 of the regulations." + TAIL}
    # the rule's own line names the page (Salmon Arm Bay); the row prints the map below it (François)
    bay = not_yet_mapped({"undrawn_part": "Salmon Arm Bay, west of line between Engineer's Point and "
                                          "Sunnybrae Point"},
                         "No fishing in Salmon Arm Bay (see map on page 28), Sept 1-Dec 31",
                         {"full_name": "SHUSWAP LAKE", "pages": [32]})
    assert bay["says"] == ("Not yet mapped: this applies only in one part of this water — Salmon Arm "
                           "Bay, west of line between Engineer's Point and Sunnybrae Point — which is "
                           "not drawn on the map yet; see the map on p.28 of the regulations." + TAIL)
    assert book_map("the area at the outlet of François Lake described on map 1", "bait ban",
                    {"full_name": "FRANÇOIS LAKE", "printed": "For the area at the outlet of François "
                     "Lake described on map 1, (see below): No Fishing", "pages": [51]}) == \
        {"name": "map 1", "page": 51, "cite": "map 1 on p.51"}


def test_no_map_no_cite():
    """A part the book draws on no map reads as before; an unidentified part cites no map even on
    a row that prints one ("see map below": the Cowichan River's vessel rules "on parts")."""
    assert not_yet_mapped({"undrawn_part": "south half"}, "no powered boats", {"full_name": "PREMIER LAKE",
                                                                              "pages": [39]}) == {
        "display": "prominent", "part": "south half", "identified": True,
        "says": "Not yet mapped: this applies only in one part of this water — south half — which is "
                "not drawn on the map yet." + TAIL}
    assert book_map("on parts", "Speed restriction (8 km/h) on parts",
                    {"full_name": "COWICHAN RIVER (see map below)", "pages": [15]}) is None
    # a "below" with no single page of its own cites nothing rather than guess
    assert book_map("the bay (map B)", "x", {"full_name": "X LAKE (see map below)", "pages": [15, 16]}) is None


# ---------------------------------------------------------------------------------------------
# The answers read it: `applies` and `book`
# ---------------------------------------------------------------------------------------------

def _doc(nym: dict | None) -> tuple[dict, dict]:
    x = {"id": f"{SHUSWAP}.r1", "entry_id": SHUSWAP.split("::")[0],
         "rule_id": "shuswap_lake.r1", "provenance": {"rank": 0, "entry_name": "Shuswap Lake"},
         "fields": {}, **({"not_yet_mapped": nym} if nym else {})}
    doc = {"entries": {x["entry_id"]: {"name": "Shuswap Lake", "kind": "water", "pages": [32],
                                       "chapter": "r3"}},
           "rules": {x["id"]: x}, "waters": {}, "guide": {"pdf_pages": {"28": 30, "32": 34}}}
    return x, doc


NYM = not_yet_mapped({"undrawn_part": "entire area north of Albas (map B)"}, "No Fishing", SHUSWAP_ROW)
OWN_ROW = f"This water’s own row in the {BOOK_TITLE}: Shuswap Lake."


def test_applies_ends_with_the_area_caveat():
    x, doc = _doc(NYM)
    assert applies_here(x, doc) == (
        OWN_ROW + " Not yet mapped: this applies only in one part of this water — entire area north of "
                  "Albas — which is not drawn on the map yet; see map B on p.28 of the regulations." + TAIL,
        "Inherited from Shuswap Lake: its row covers its tributaries.")
    # a rule of the whole water says only its scope
    x, doc = _doc(None)
    assert applies_here(x, doc)[0] == OWN_ROW


def test_book_cites_the_map_page_after_the_row():
    x, doc = _doc(NYM)
    assert book_pages(x, doc) == [[32, 34], [28, 30]]
    x, doc = _doc(None)
    assert book_pages(x, doc) == [[32, 34]]


# ---------------------------------------------------------------------------------------------
# The export refuses a map it cannot cite
# ---------------------------------------------------------------------------------------------

def test_a_named_map_with_no_page_is_refused():
    no_page = not_yet_mapped({"undrawn_part": "the bay (map B)"}, "No Fishing", {"full_name": "X LAKE",
                                                                                 "pages": [40]})
    assert "map" not in no_page
    off_book = not_yet_mapped({"undrawn_part": "the bay (map B)"}, "No Fishing",
                              {"full_name": "X LAKE (see maps on page 99)", "pages": [40]})
    doc = {"guide": {"pdf_pages": {"28": 30, "40": 42}},
           "rules": {"e::a": {"not_yet_mapped": no_page}, "e::b": {"not_yet_mapped": off_book},
                     "e::c": {"not_yet_mapped": NYM}},
           "licensing": {}}
    assert undrawn_map_problems(doc) == [
        "e::a: its part names a map ('the bay (map B)') but no page is found for it in its line or "
        "its row (`book_map`)",
        "e::b: cites map B on p.99, which guide.pdf_pages does not map"]


@pytest.fixture(scope="module")
def doc(request) -> dict:
    import os
    from pathlib import Path
    from pipeline.tests.conftest import BUNDLE_HINT, need
    from pipeline.tools import export_ui_rules as X
    bundle = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)
    need(request, "bundle", bundle, BUNDLE_HINT)
    return X.build(bundle)


@pytest.mark.needs_bundle
def test_the_export_carries_the_caveats(doc):
    """On the export as built: every widened rule carries the caveat; the mapped ones carry their
    map; and the export's own check passes."""
    nym = {k: r["not_yet_mapped"] for k, r in doc["rules"].items() if r.get("not_yet_mapped")}
    assert len(nym) == sum(WIDENED.values())
    assert {k: n["map"]["cite"] for k, n in nym.items() if n.get("map")} == MAPS
    assert undrawn_map_problems(doc) == []
