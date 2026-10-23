"""'SEE X' GROUP ENTRIES, MEMBER LAKES UNDER THEIR OWN NAMES, AND ONE RULE FROM TWO ROWS (owner 2026-10-22,
handoff/PLAN/app-integration/decisions.md "Main Lake / Mine Lake, and 'See X' group entries").

  1. A row that only says "See …" (TEE PEE LAKES "See Friday, Saturday, and Sunday Lakes"; CAMERON LAKES "See North
     and South Cameron lakes"; GARBUTT LAKE "See Norbury Lake") is a POINTER: it binds nothing and is never drawn as a
     regulation line on the lakes it names. The group name leads the reader to them: the bundle's `see_pointer`
     (`rules.see_pointer_rows`), copied to the release's `core` for search.
  2. The member lakes show their regulations under THEIR OWN names: a water row is shown under its book heading unless
     the registry's name for its water names the same water (`place_names.row_name`) — Haynes Lake, never
     "McCulloch Reservoir"; South Cameron Lake, never "Cameron Lakes".
  3. An identical rule reaching one water from two rows reads ONCE (`gear.once`): McCulloch Reservoir's and Minnow
     Lake's engine limit on the reservoir, Norbury's and Peckhams' "No powered boats"; the other is still in force and
     cited (`repeats`).

Every check is pinned by breaking its input and watching it fail (mutation). The bundle tests read the bundle the
merger's `python -m pipeline.deliver` writes: one made before `see_pointer` existed FAILS (`predates`).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.answers import gear as G
from pipeline.deliver.bundle import rules as RULES
from pipeline.deliver.bundle.place_names import row_name
from pipeline.tests.conftest import BUNDLE_HINT, need, predates


@pytest.fixture(scope="module")
def corpus():
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    from pipeline.regs.parsing.io import read_all_entries
    return {k: CatalogueEntry.model_validate(v) for k, v in read_all_entries().items()}


# --------------------------------------------------------------------------- 2. the name a row is shown under
@pytest.mark.parametrize("heading,registry,shown", [
    # the registry's name is the GROUP's: the book's heading is the row's name
    ("HAYNES LAKE", "McCulloch Reservoir", "Haynes Lake"),
    ("HYDRAULIC LAKE", "McCulloch Reservoir", "Hydraulic Lake"),
    ("SOUTH CAMERON LAKE", "Cameron Lakes", "South Cameron Lake"),
    ("PECKHAMS LAKE", "Norbury Lakes", "Peckhams Lake"),
    ("NORBURY (Garbutt) LAKE", "Norbury Lakes", "Norbury Lake"),          # the name, its note in `full_name`
    ("CHUCHI LAKE", "Nation Lakes", "Chuchi Lake"),
    ("TEE PEE LAKES", "Tepee Lakes", "Tee Pee Lakes"),
    ('LONZO ("Marshall") CREEK', "Marshall Creek", "Lonzo Creek"),
    ("SLEWISKIN (Macdonald) CREEK", "McDonald Creek", "Slewiskin Creek"),
    # the same water, tidier: the registry's name is kept
    ("FRIDAY LAKE", "Friday Lake", "Friday Lake"),
    ("MAIN LAKE (Quadra Island)", "Main Lake", "Main Lake"),
    ("Big Lake (approx. 10 km west of 100 Mile House)", "Big Lake", "Big Lake"),
    ("Hautete Lake", "Hautête Lake", "Hautête Lake"),
    # ...or a shorter name of the heading's own water (a row printed for two waters, named by its main one)
    ("CHILLIWACK / Vedder Rivers (does not include Sumas River) (see map on page 24)", "Chilliwack River",
     "Chilliwack River"),
    ("ATNARKO/Bella Coola Rivers [Includes Tributaries] EXCEPT: Burnt Bridge Creek upstream of Sitkatapa Creek",
     "Bella Coola River", "Bella Coola River"),
    ("Buttle Lake's Tributaries", "Buttle Lake", "Buttle Lake"),
    # ...or a lake part's authored name
    ("Kootenay Lake, All Parts (Main Body, Upper West Arm and Lower West Arm)", "Kootenay Lake — Main Body",
     "Kootenay Lake — Main Body"),
    # ...and one that adds only words of the heading's own note
    ("Williston Lake (in Zone A) (includes waters 500 m east/upstream of the Causeway Road)",
     "Williston Lake — Zone A", "Williston Lake — Zone A"),
    ("Wigwam River (downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road)",
     "Wigwam River", "Wigwam River"),
    # no registry name: the heading as it came (the export re-cases it)
    ("GARNET LAKE", "", "GARNET LAKE"),
    # T80 (T56 review): the book's quotes round an unofficial name are not part of the name the row is shown under —
    # as `see_name` and search already read it ("Diana Creek", never '"Diana" Creek')
    ('"DIANA" CREEK', "Gold Creek", "Diana Creek"),
    ('"LOWER BEAVERPOND" LAKE', "", "LOWER BEAVERPOND LAKE"),
    ('"Diana" Creek', "", "Diana Creek"),
])
def test_a_row_is_shown_under_its_own_name(heading, registry, shown):
    assert row_name(heading, registry) == shown


def test_the_corpus_names_member_lakes_by_their_own_names(corpus):
    got = {e: RULES.entry_name(corpus[e]) for e in (
        "r8:haynes_lake@8-10", "r8:hydraulic_lake@8-10", "r8:minnow_lake@8-10", "r8:mcculloch_reservoir@8-10",
        "r7:south_cameron_lake@7-31", "r7:north_cameron_lake@7-31", "r7:cameron_lakes@7-31",
        "r4:peckhams_lake@4-22", "r4:norbury_garbutt_lake@4-22", "r4:garbutt_lake@4-22",
        "r8:tee_pee_lakes@8-6", "r8:friday_lake@8-6", "r1:main_lake_quadra_island@1-15", "r1:mine_lake@1-15",
        "z1:bait_ban_streams")}
    assert got == {
        "r8:haynes_lake@8-10": "Haynes Lake", "r8:hydraulic_lake@8-10": "Hydraulic Lake",
        "r8:minnow_lake@8-10": "Minnow Lake", "r8:mcculloch_reservoir@8-10": "McCulloch Reservoir",
        "r7:south_cameron_lake@7-31": "South Cameron Lake", "r7:north_cameron_lake@7-31": "North Cameron Lake",
        "r7:cameron_lakes@7-31": "Cameron Lakes",
        "r4:peckhams_lake@4-22": "Peckhams Lake", "r4:norbury_garbutt_lake@4-22": "Norbury Lake",
        "r4:garbutt_lake@4-22": "Garbutt Lake",
        "r8:tee_pee_lakes@8-6": "Tee Pee Lakes", "r8:friday_lake@8-6": "Friday Lake",
        "r1:main_lake_quadra_island@1-15": "Main Lake", "r1:mine_lake@1-15": "Mine Lake",
        # a zone table keeps its authored scope: the rule is for water rows only
        "z1:bait_ban_streams": "Every stream in Region 1"}
    # how many water rows the rule renames (the "applies" line of each moves; diff the answers before promoting)
    moved = sorted(e for e, ce in corpus.items() if e.startswith("r") and RULES.entry_name(ce) != (ce.display_name or ce.name))
    assert len(moved) == 77, moved[:5]
    assert all(RULES.entry_name(ce) == (ce.display_name or ce.name) for e, ce in corpus.items() if not e.startswith("r"))


def test_mutation_the_registry_name_alone_brings_the_group_name_back(corpus, monkeypatch):
    import pipeline.deliver.bundle.place_names as PN
    monkeypatch.setattr(PN, "row_name", lambda name, display_name="": display_name or name)
    assert RULES.entry_name(corpus["r8:haynes_lake@8-10"]) == "McCulloch Reservoir"      # what the page said before


# --------------------------------------------------------------------------- 1. the group name points
def _rows(corpus, eid):
    return [r for r in RULES.see_pointer_rows(corpus) if r[0] == eid]


def test_a_group_name_leads_to_the_waters_it_names(corpus):
    assert _rows(corpus, "r8:tee_pee_lakes@8-6") == [
        ("r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 0, "r8:friday_lake@8-6", "Friday Lake", "wbk:329520049"),
        ("r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 1, "r8:saturday_lake@8-6", "Saturday Lake", "wbk:329520046"),
        ("r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 2, "r8:sunday_lake@8-6", "Sunday Lake", "wbk:329520017")]
    assert _rows(corpus, "r7:cameron_lakes@7-31") == [
        ("r7:cameron_lakes@7-31", "Cameron Lakes", 0, "r7:north_cameron_lake@7-31", "North Cameron Lake",
         "wbk:329657333"),
        ("r7:cameron_lakes@7-31", "Cameron Lakes", 1, "r7:south_cameron_lake@7-31", "South Cameron Lake",
         "wbk:329657328")]
    assert _rows(corpus, "r4:garbutt_lake@4-22") == [
        ("r4:garbutt_lake@4-22", "Garbutt Lake", 0, "r4:norbury_garbutt_lake@4-22", "Norbury Lake", "wbk:329524023")]
    assert _rows(corpus, "r2:marshall_creek@2-4") == [
        ("r2:marshall_creek@2-4", "Marshall Creek", 0, "r2:lonzo_marshall_creek@2-4", "Lonzo Creek", "gnis:1860")]


def test_only_a_row_that_only_points_is_a_group_and_a_twin_is_none(corpus):
    rows = RULES.see_pointer_rows(corpus)
    eids = {r[0] for r in rows}
    assert all(corpus[e].pointer_only for e in eids)
    assert "r8:mara_lake@8-26" not in eids                  # a row with rules of its own AND a "See Shuswap Lake"
    assert "r5:squirrel_lake@6-1" not in eids               # the same row printed in Region 5's table: a twin
    # a zone table's note is no water's name ("Main body of Kootenay Lake" would answer a search for "Main")
    assert "z4:kootenay_annual" not in eids and all(e.startswith("r") for e in eids)
    assert (len(eids), len(rows)) == (44, 50)


def test_one_pointer_printed_in_two_regions_is_one_way_in(corpus):
    """T80 (T56 review): Regions 5 and 7 both print "Blackwater River: see West Road River" — one name, one water;
    search showed it twice. The first row (by entry id) carries it; no two rows say the same name for the same water."""
    rows = RULES.see_pointer_rows(corpus)
    assert [r for r in rows if r[1] == "Blackwater River"] == [
        ("r5:blackwater_river@5-13", "Blackwater River", 0, "r5:west_road_blackwater_river@5-12+5-13",
         "West Road River", "gnis:26104")]
    assert len({(r[1], r[4], r[5]) for r in rows}) == len(rows)


def test_mutation_without_the_cross_row_check_blackwater_is_said_twice(corpus, monkeypatch):
    monkeypatch.setattr(RULES, "ONE_WAY_IN_ACROSS_ROWS", False)
    assert [r[0] for r in RULES.see_pointer_rows(corpus) if r[1] == "Blackwater River"] == [
        "r5:blackwater_river@5-13", "r7:blackwater_river@7-10"]


def _line(corpus, eid):
    rows = _rows(corpus, eid)
    return (rows[0][1], [(r[4], r[5]) for r in rows])


def test_every_pointer_is_said_by_the_names_the_book_prints(corpus):
    """REVIEW 2026-10-22: the pointer's name is what search matches (`SEARCH_POINTERS`: the name or its START), so it
    must be the book's heading. The shown name (`entry_name`) is the registry's where it has fewer words — "LITTLE
    CAMPBELL RIVER" read "Campbell River: see Campbell River", "BIG QUALICUM" "Qualicum River: see Qualicum River" —
    and kept the book's quotes ('"Jones" Lake' is never found by "Jones Lake")."""
    got = {e: _line(corpus, e) for e in (
        "r1:big_qualicum_river@1-6", "r2:little_campbell_river@2-4", "r2:south_alouette_river@2-8",
        "r1:weston_lake@1-1", "r2:jones_lake@2-3", "r7:chinaman_lake@7-35", "r4:revelstoke_lake_s_tributaries@4-38",
        "r3:coldwater_river_s_tributaries@3-13", "r4:arrow_lakes_tributaries", "r4:bighorn_ram_creek@4-2",
        "r5:bella_coola_river@5-8", "r2:vedder_river@2-4", "r7:nation_river@7-30", "r4:waneta_reservoir@4-8")}
    assert got == {
        "r1:big_qualicum_river@1-6": ("Big Qualicum River", [("Qualicum River", "gnis:27804")]),
        "r2:little_campbell_river@2-4": ("Little Campbell River", [("Campbell River", "gnis:7250")]),
        "r2:south_alouette_river@2-8": ("South Alouette River", [("Alouette River", "gnis:9630")]),
        "r1:weston_lake@1-1": ("Weston Lake", [("Lake Weston", "wbk:328996530")]),
        "r2:jones_lake@2-3": ("Jones Lake", [("Wahleach Lake", "wbk:329177886")]),
        "r7:chinaman_lake@7-35": ("Chinaman Lake", [("Chunamun Lake", "wbk:328995585")]),
        "r4:revelstoke_lake_s_tributaries@4-38": ("Revelstoke Lake's Tributaries",
                                                  [("Lake Revelstoke's Tributaries", "wbk:329484653")]),
        "r3:coldwater_river_s_tributaries@3-13": ("Coldwater River's Tributaries", [
            ("Juliet Creek", "gnis:19162"), ("July Creek", "gnis:37260"), ("Mine Creek", "gnis:29546")]),
        "r4:arrow_lakes_tributaries": ("Arrow Lakes' Tributaries", [
            ("Lower Arrow Lake's Tributaries", "wbk:328961720"), ("Upper Arrow Lake's Tributaries", "wbk:328961689")]),
        # the two Wigwam rows are one river to open: one way to it, not two identical buttons
        "r4:bighorn_ram_creek@4-2": ("Bighorn Creek", [("Wigwam River", "gnis:27703")]),
        "r5:bella_coola_river@5-8": ("Bella Coola River", [("Atnarko/Bella Coola Rivers", "gnis:11611")]),
        "r2:vedder_river@2-4": ("Vedder River", [("Chilliwack / Vedder Rivers", "gnis:8634")]),
        # a lake part keeps its authored name
        "r7:nation_river@7-30": ("Nation River", [("Williston Lake — Nation Arm", "wbk:-16")]),
        "r4:waneta_reservoir@4-8": ("Waneta Reservoir", [("Pend D'Oreille River", "gnis:4927")])}
    rows = RULES.see_pointer_rows(corpus)
    from pipeline.deliver.bundle.place_names import is_shouting
    assert [r for r in rows if '"' in r[1] + r[4] or is_shouting(r[1]) or is_shouting(r[4]) or r[1] == r[4]] == []
    assert len(set((r[0], r[4], r[5]) for r in rows)) == len(rows)          # no water named twice by one pointer


def test_mutation_the_shown_name_as_the_pointers_name(corpus, monkeypatch):
    monkeypatch.setattr(RULES, "see_name", RULES.entry_name)
    assert _line(corpus, "r2:little_campbell_river@2-4") == ("Campbell River", [("Campbell River", "gnis:7250")])


def test_mutation_a_twin_counted_as_a_group(corpus, monkeypatch):
    import pipeline.regs.parsing.catalogue as CAT
    monkeypatch.setattr(CAT, "see_relation", lambda ce, targets: "see")
    eids = {r[0] for r in RULES.see_pointer_rows(corpus)}
    assert "r5:squirrel_lake@6-1" in eids                   # the twin filter is what kept it out


# --------------------------------------------------------------------------- the bundle (after `pipeline.deliver`)
@pytest.fixture(scope="module")
def db(request):
    from pipeline.deliver.bundle import read as _R
    import os
    p = os.environ.get("UI_EXPORT_BUNDLE") or _R.BUNDLE
    need(request, "bundle", p, BUNDLE_HINT)
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name = 'see_pointer'").fetchone():
        predates(f"{p} predates `see_pointer` (run python -m pipeline.deliver)")
    yield con
    con.close()


@pytest.mark.needs_bundle
def test_the_bundle_carries_the_tee_pee_pointer(db):
    assert db.execute("SELECT * FROM see_pointer WHERE entry_id = 'r8:tee_pee_lakes@8-6' ORDER BY ix").fetchall() == [
        ("r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 0, "r8:friday_lake@8-6", "Friday Lake", "wbk:329520049"),
        ("r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 1, "r8:saturday_lake@8-6", "Saturday Lake", "wbk:329520046"),
        ("r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 2, "r8:sunday_lake@8-6", "Sunday Lake", "wbk:329520017")]


def _row_names_on(db, item_id):
    """The shown names (`entry.name`) of every water row bound on a water's sections, and its pointer rows' ids."""
    names = {r[0] for r in db.execute(
        "SELECT DISTINCT e.name FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_ruleset sr ON sr.sid = s.sid JOIN ruleset rs ON rs.set_id = sr.set_id "
        "JOIN entry e ON e.entry_id = rs.entry_id WHERE i.item_id = ? AND rs.entry_id LIKE 'r%'", (item_id,))}
    labels = {r[0] for r in db.execute(
        "SELECT DISTINCT r.label FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_ruleset sr ON sr.sid = s.sid JOIN ruleset rs ON rs.set_id = sr.set_id "
        "JOIN rule r ON r.entry_id = rs.entry_id AND r.rule_id = rs.rule_id "
        "WHERE i.item_id = ? AND rs.entry_id LIKE 'r%'", (item_id,))}
    return names, labels


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id,names,labels", [
    ("wbk:329520049", {"Friday Lake"}, {"Electric motor only (max 7.5 kW)"}),                 # Friday Lake 8-6
    ("wbk:329657333", {"North Cameron Lake"}, {"No powered boats"}),                         # North Cameron 7-31
    ("wbk:329657328", {"South Cameron Lake"}, {"No powered boats"}),                         # South Cameron 7-31
    ("wbk:329524023", {"Norbury Lake", "Peckhams Lake"}, {"No powered boats"}),              # Norbury 4-22
])
def test_member_lakes_carry_their_own_rows_under_their_own_names_and_no_see_line(db, item_id, names, labels):
    got_names, got_labels = _row_names_on(db, item_id)
    assert (got_names, got_labels) == (names, labels)
    assert not any(l.lower().startswith("see ") for l in got_labels)
    # no pointer row binds anything there
    pointers = {e for (e,) in db.execute("SELECT DISTINCT entry_id FROM see_pointer")}
    bound = {e for (e,) in db.execute(
        "SELECT DISTINCT rs.entry_id FROM item i JOIN item_section s ON s.ord = i.ord JOIN section_ruleset sr "
        "ON sr.sid = s.sid JOIN ruleset rs ON rs.set_id = sr.set_id WHERE i.item_id = ?", (item_id,))}
    assert not pointers & bound


@pytest.mark.needs_bundle
def test_the_applies_line_names_the_member_lake(db):
    """The rule's "applies" line (`display.applies_here`) reads the shown name: Peckhams' row on Norbury is
    "Peckhams Lake", never the group's "Norbury Lakes"."""
    from pipeline.deliver.answers import display as X
    from pipeline.deliver.answers.common import BOOK_TITLE
    name = db.execute("SELECT name FROM entry WHERE entry_id = 'r4:peckhams_lake@4-22'").fetchone()[0]
    doc = {"entries": {"r4:peckhams_lake@4-22": {"name": name, "kind": "water"}}}
    x = {"id": "r4:peckhams_lake@4-22::peckhams_lake.r1", "entry_id": "r4:peckhams_lake@4-22",
         "provenance": {"rank": 1, "entry_name": name}}
    assert X.applies_here(x, doc) == (f"This water’s own row in the {BOOK_TITLE}: Peckhams Lake.",
                                      "Inherited from Peckhams Lake: its row covers its tributaries.")


# --------------------------------------------------------------------------- 3. one rule from two rows reads once
def _vessel(key, rank, **f):
    x = {"type": "vessel_rule", "family": "vessel", "dimension": "propulsion", "aspect": "propulsion",
         "species": [], "species_except": [], "take": None, "may_target": None, "uncertain": 0,
         "undrawn_part": None, "entry_id": key.split("::")[0], "entry_name": key.split("::")[0], **f}
    x.setdefault("label", "Engine power restriction 7.5 kW (10 hp)")
    x.setdefault("parts", json.dumps({"what": x["label"]}))
    return G.Rule(key, rank, x)


def test_an_identical_vessel_rule_from_two_rows_reads_once():
    a = _vessel("r8:mcculloch_reservoir@8-10::mcculloch_reservoir.r1", 0, level="power_capped", max_power_kw=7.5)
    b = _vessel("r8:minnow_lake@8-10::minnow_lake.r1", 0, level="power_capped", max_power_kw=7.5)
    c = _vessel("r8:other@8-10::other.r1", 0, level="power_capped", max_power_kw=10.0,
                label="Engine power restriction 10 kW")
    got = G.resolve([a, b, c], "lake", [])
    assert (got["vessel"], [k for k in got["repeats"] if "vessel" in k or k in (a.key, b.key, c.key)]) == (
        {"active": [a.key, c.key], "timed": []}, [b.key])
    # the nearer row is the one shown: a reach rule (rank 0) over the same words walked in (rank 1)
    near = G.resolve([_vessel("r8:z@8-10::z.r1", 1, level="power_capped", max_power_kw=7.5), b], "lake", [])
    assert near["vessel"]["active"] == [b.key] and near["repeats"] == ["r8:z@8-10::z.r1"]


def test_two_boat_rules_alike_but_where_they_reach_both_show():
    """REVIEW 2026-10-22: the statement is WHERE as well as what (`extents`, `extent_text`, `unresolved`, `scope`) —
    those were left out of `vessel_says`, so a rule for one water of a row and a whole-row rule folded into one line."""
    a = _vessel("r2:a@2-19::a.r1", 0, level="power_capped", max_power_kw=7.5, extents=[{"op": "whole"}])
    b = _vessel("r2:b@2-19::b.r2", 0, level="power_capped", max_power_kw=7.5,
                extents=[{"op": "whole", "item_id": "wbk:329177889"}])
    c = _vessel("r2:c@2-19::c.r1", 0, level="power_capped", max_power_kw=7.5, extents=[{"op": "whole"}],
                extent_text="south of the CPR bridge")
    d = _vessel("r2:d@2-19::d.r1", 0, level="power_capped", max_power_kw=7.5, extents=[{"op": "whole"}],
                unresolved="the bridge")
    e = _vessel("r2:e@2-19::e.r1", 0, level="power_capped", max_power_kw=7.5, extents=[{"op": "whole"}], scope="mu")
    got = G.resolve([a, b, c, d, e], "lake", [])
    assert (got["vessel"]["active"], [k for k in got["repeats"] if k in (a.key, b.key, c.key, d.key, e.key)]) == (
        [a.key, b.key, c.key, d.key, e.key], [])


def test_mutation_the_statement_without_where(monkeypatch):
    monkeypatch.setattr(G, "_VESSEL_SAYS", tuple(k for k in G._VESSEL_SAYS
                                                 if k not in ("extents", "extent_text", "unresolved", "scope")))
    a = _vessel("r2:a@2-19::a.r1", 0, level="power_capped", max_power_kw=7.5, extents=[{"op": "whole"}])
    c = _vessel("r2:c@2-19::c.r1", 0, level="power_capped", max_power_kw=7.5, extents=[{"op": "whole"}],
                extent_text="south of the CPR bridge")
    assert G.resolve([a, c], "lake", [])["vessel"]["active"] == [a.key]          # the fold the pin above refuses


def test_mutation_the_row_in_the_statement_shows_both(monkeypatch):
    real = G.vessel_says
    monkeypatch.setattr(G, "vessel_says", lambda x: real(x) + x["entry_id"])
    a = _vessel("r4:norbury_garbutt_lake@4-22::norbury_garbutt_lake.r1", 0, level="unpowered", label="No powered boats")
    b = _vessel("r4:peckhams_lake@4-22::peckhams_lake.r1", 0, level="unpowered", label="No powered boats")
    assert G.resolve([a, b], "lake", [])["vessel"]["active"] == [a.key, b.key]      # the page's "twice"


@pytest.fixture(scope="module")
def bundle(request):
    from pipeline.deliver.answers import common
    p = common.bundle_path()
    need(request, "bundle", p, BUNDLE_HINT)
    return common.load(p)


def _boats(B, item_id):
    """The boat lines a water's first rule key shows on day 200 (`vessel.active`, each its label), and its repeats."""
    from pipeline.tests.test_answers_gear import _answer, _key_of
    from pipeline.deliver.calendar import month_day
    a = _answer(B, _key_of(B, item_id), month_day(200), G.province_methods(B.rules.values()))
    label = {G.rule_id(k): B.rules[k]["label"] for k in B.rules}
    return [label[k] for k in a["vessel"]["active"]], [k for k in a["repeats"] if B.rules[tuple(k.split("::", 1))]
                                                         .get("family") == "vessel"]


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id,lines,again", [
    # frame 462, McCulloch's main polygon: McCulloch's and Minnow Lake's identical engine limit
    ("wbk:328985739", ["Engine power restriction 7.5 kW (10 hp)"], ["r8:minnow_lake@8-10::minnow_lake.r1"]),
    # frame 946, Norbury: Norbury's and Peckhams' "No powered boats"
    ("wbk:329524023", ["No powered boats"], ["r4:peckhams_lake@4-22::peckhams_lake.r1"]),
])
def test_the_page_shows_one_boat_line_where_two_rows_print_it(bundle, item_id, lines, again):
    assert _boats(bundle, item_id) == (lines, again)


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id", ["wbk:329459137", "wbk:329095512"])     # Haynes Lake; Mine Lake
def test_haynes_and_mine_show_the_engine_limit_once(bundle, item_id):
    """Once on either vintage: after the reach rerun McCulloch's row also binds Haynes (and Main Lake's binds Mine),
    each with the identical engine limit — one line, the other a repeat."""
    lines, _ = _boats(bundle, item_id)
    assert lines == ["Engine power restriction 7.5 kW (10 hp)"]


# --------------------------------------------------------------------------- the release's core (search reads it)
SCHEMA = Path(__file__).resolve().parents[1] / "deliver" / "bundle" / "schema.sql"
_CAMERON = [
    ("r7:cameron_lakes@7-31", "Cameron Lakes", 0, "r7:north_cameron_lake@7-31", "North Cameron Lake", "wbk:329657333"),
    ("r7:cameron_lakes@7-31", "Cameron Lakes", 1, "r7:south_cameron_lake@7-31", "South Cameron Lake", "wbk:329657328")]


def _tiny_bundle(p: Path) -> None:
    con = sqlite3.connect(p)
    con.executescript(SCHEMA.read_text())
    con.executemany("INSERT INTO item (ord, item_id, name, kind) VALUES (?,?,?,?)", [
        (1, "wbk:329657333", "North Cameron Lake", "lake"), (2, "wbk:329657984", "North Cameron Lake", "lake"),
        (3, "wbk:329657328", "Cameron Lakes", "lake"), (4, "wbk:9", "Nowhere Lake", "lake")])
    con.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", [(1, 10), (2, 11), (3, 12), (4, 13)])
    con.executemany("INSERT INTO entry (entry_id, item_id, name, full_name, verbatim, matched) VALUES (?,?,?,?,?,?)", [
        ("r7:north_cameron_lake@7-31", "wbk:329657333", "North Cameron Lake", "NORTH CAMERON LAKE",
         "No powered boats", '["wbk:329657333","wbk:329657984"]'),
        ("r7:south_cameron_lake@7-31", "wbk:329657328", "South Cameron Lake", "SOUTH CAMERON LAKE",
         "No powered boats", '["wbk:329657328"]')])
    con.executemany("INSERT INTO see_pointer VALUES (?,?,?,?,?,?)", _CAMERON)
    con.commit()
    con.close()


def test_the_release_core_carries_the_pointers_and_rows_every_matched_water(tmp_path):
    from pipeline.deliver.release import build as B
    b = tmp_path / "bundle.sqlite"
    _tiny_bundle(b)
    core = B.copy_file(tmp_path / "core.sqlite", {"b": b}, [("b", t, None) for t in B.CORE_TABLES],
                       {"format": B.FORMATS["core"]}, extra=B._core_flags)
    c = sqlite3.connect(core)
    assert c.execute("SELECT * FROM see_pointer ORDER BY entry_id, ix").fetchall() == _CAMERON
    # every water a row matched is rowed — North Cameron's SECOND polygon too (it was `entry.item_id` only)
    assert dict(c.execute("SELECT item_id, has_rows FROM item")) == {
        "wbk:329657333": 1, "wbk:329657984": 1, "wbk:329657328": 1, "wbk:9": 0}
    # MUTATION: the old reading (the first match only) misses the second polygon — the pin above tells them apart
    c.execute("ATTACH ? AS b", (str(b),))
    assert c.execute("SELECT EXISTS (SELECT 1 FROM b.entry e WHERE e.item_id = 'wbk:329657984')").fetchone() == (0,)
    # search reads the pointer by name, case-insensitively, through its index
    plan = " ".join(r[3] for r in c.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM main.see_pointer WHERE name LIKE 'cameron%'"))
    assert "see_pointer_nc" in plan, plan
