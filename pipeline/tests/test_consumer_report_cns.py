"""THE UI CONSUMER'S REPORT OF 2026-10-03 — seven items, each pinned against the book.

  1. SHUSWAP ANNUAL QUOTAS are one limit each, read twice: the row (p.32) and Region 3 (p.28) say
     the same thing, so the row says exactly what the zone says and the water's number replaces it.
  2. THE PROTECTED FISH BY NAME (p.9; Region 2's four, p.21) — named members of PROTECTED_SPECIES.
  3. THE PAPER LICENCE names the fish you must record, DERIVED from the record-duty rules.
  4. BAIT: what a bait ban covers, and worms (p.8).
  5. LICENCE CLASSES (p.5): who may buy each, how long it runs, the printed fee per residency.
  6. THE CRESTON VALLEY WMA PERMIT holds on Kootenay Lake's Main Body only as a part not yet mapped.
  7. CUT-POINT NAMES say "<what> at <place>", never "<place> — <what>".

Model checks run on the corpus; bundle and export checks read `UI_EXPORT_BUNDLE` (else the live
bundle). Each derived thing has a mutation twin.
"""
from __future__ import annotations

import copy
import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.deliver.bundle.place_names import _between, cut_name
from pipeline.regs.parsing import catalogue as C
from pipeline.regs.parsing.io import read_all_entries
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)
SHUSWAP = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26"
KOOTENAY_MAIN = "wbk:-20"
DUCK_LAKE = "wbk:329246292"
PERMIT = "z4:creston_valley_permit#creston_valley_wma_permit"
PERMIT_PART = "z4:creston_valley_permit#creston_valley_wma_permit_kootenay_lake"


@pytest.fixture(scope="module")
def corpus() -> dict:
    return {k: C.CatalogueEntry.model_validate(v) for k, v in read_all_entries().items()}


@pytest.fixture(scope="module")
def db():
    if not BUNDLE.exists():
        pytest.skip(f"no bundle at {BUNDLE}")
    c = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield c
    c.close()


@pytest.fixture(scope="module")
def doc():
    if not BUNDLE.exists():
        pytest.skip(f"no bundle at {BUNDLE}")
    return X.build(BUNDLE)


def _rule(corpus, eid, rid):
    return next(r for r in corpus[eid].rules if r.rule_id == rid)


# ---------------------------------------------------------------------------------------------
# 1. Shuswap
# ---------------------------------------------------------------------------------------------
@pytest.mark.parametrize("row,zone", [("shuswap_lake.r8", "shuswap_annual.r1"),
                                      ("shuswap_lake.r10", "shuswap_annual.r2")])
def test_the_shuswap_annual_quota_is_one_statement(corpus, row, zone):
    """The row's 'annual quota = 5' and Region 3's 'Annual catch quota for Shuswap Lake' are one
    limit: the same fish (char = lake trout and bull trout, p.7/p.28 — never brook trout), size,
    number and clock, so `same_statement` holds and the water's number replaces the zone's."""
    from pipeline.deliver.bundle.rules import same_statement
    w, z = _rule(corpus, SHUSWAP, row), _rule(corpus, "z3:shuswap_annual", zone)
    as_dict = lambda r: json.loads(json.dumps(r.model_dump(mode="json", by_alias=True,
                                                           exclude_none=True)))
    assert (w.species, w.take, w.period, w.lengths) == (z.species, z.take, z.period, z.lengths)
    assert same_statement(as_dict(w), as_dict(z))
    assert "EB" not in C.expand_species(list(w.species))


def test_the_shuswap_char_record_duty_still_links(doc):
    R_ = doc["rules"]
    for q in (f"{SHUSWAP}::shuswap_lake.r10", "z3:shuswap_annual::shuswap_annual.r2"):
        assert R_[q]["recorded_by"] == "zp:shuswap_char_stamp::shuswap_char_stamp.r1", q


def test_one_annual_char_quota_speaks_at_shuswap(db):
    """For a lake trout at Shuswap Lake the annual 5 is said once: the row's displaces the zone's."""
    sid = db.execute("SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
                     "WHERE i.item_id = 'wbk:329518145' LIMIT 1").fetchone()[0]
    annual = {(SHUSWAP, "shuswap_lake.r8"), (SHUSWAP, "shuswap_lake.r10"),
              ("z3:shuswap_annual", "shuswap_annual.r1"), ("z3:shuswap_annual", "shuswap_annual.r2")}
    for fish, want in (("LT", "shuswap_lake.r10"), ("DV", "shuswap_lake.r10"),
                       ("RB", "shuswap_lake.r8")):
        got = R.effective_rules(sid, (7, 1), fish, str(BUNDLE))
        speaking = [x["rule_id"] for x in got if x["state"] == "speaks"
                    and (x["entry_id"], x["rule_id"]) in annual]
        assert speaking == [want], (fish, speaking)
    # a brook trout is not a Shuswap char (p.7/p.28): no Shuswap annual quota speaks for it
    got = R.effective_rules(sid, (7, 1), "EB", str(BUNDLE))
    assert not [x for x in got if (x["entry_id"], x["rule_id"]) in annual]


# ---------------------------------------------------------------------------------------------
# 2. Protected species
# ---------------------------------------------------------------------------------------------
P9 = ["CULTUS_LAKE_SCULPIN", "ENOS_LAKE_STICKLEBACK", "MISTY_LAKE_STICKLEBACK", "NOOKSACK_DACE",
      "PAXTON_LAKE_STICKLEBACK", "ROCKY_MOUNTAIN_SCULPIN", "SHORTHEAD_SCULPIN", "SALISH_SUCKER",
      "VANANDA_CREEK_STICKLEBACK", "VANCOUVER_LAMPREY", "WESTERN_BROOK_LAMPREY_MORRISON_CREEK",
      "WHITE_STURGEON_PROTECTED_POPULATIONS"]


def test_the_protected_rules_name_the_fish_the_book_lists(corpus):
    assert list(_rule(corpus, "zp:protected_species", "protected_species.r1").species) == P9
    assert list(_rule(corpus, "z2:protected_species", "protected_species.r1").species) == [
        "NOOKSACK_DACE", "SALISH_SUCKER", "GREEN_STURGEON", "CULTUS_LAKE_SCULPIN"]
    assert not set(C.PROTECTED_FISH) & set(C.BOOK_SPECIES), "protected fish are not game fish"
    assert C.SPECIES_GROUPS["PROTECTED_SPECIES"] == (), "the group stays open"
    lab = C.label(_rule(corpus, "z2:protected_species", "protected_species.r1"))
    assert lab == ("No fishing for protected species: Nooksack dace, Salish sucker, Green "
                   "sturgeon and Cultus Lake sculpin")


def test_a_protected_rule_speaks_for_no_game_fish():
    rule = {"species": P9}
    assert not any(R.speaks_for(rule, f) for f in C.BOOK_SPECIES)
    assert "WSG" not in P9, "the protected white sturgeon is the four populations, not WSG"


def _protected_entry(**over):
    e = {"entry_id": "zx:t", "name": "t",
         "regs_verbatim": "illegal to fish for: Nooksack dace · Salish sucker",
         "rules": [{"rule_id": "t.r1", "type": "retention_limit", "verbatim": "illegal to fish for",
                    "species": ["NOOKSACK_DACE", "SALISH_SUCKER"], "take": 0,
                    "may_target": False, "extents": [{"op": "within", "area_kind": "region"}]}]}
    e["rules"][0].update(over)
    return e


def test_mutation_a_protected_list_must_be_the_rows_own():
    C.CatalogueEntry.model_validate(_protected_entry())
    with pytest.raises(ValueError, match="its row prints"):
        C.CatalogueEntry.model_validate(_protected_entry(species=["NOOKSACK_DACE"]))
    with pytest.raises(ValueError, match="its row prints"):
        C.CatalogueEntry.model_validate(_protected_entry(
            species=["NOOKSACK_DACE", "SALISH_SUCKER", "GREEN_STURGEON"]))
    with pytest.raises(ValueError, match="share the rule"):
        C.CatalogueEntry.model_validate(_protected_entry(
            species=["NOOKSACK_DACE", "SALISH_SUCKER", "RB"]))


def test_the_export_lists_the_protected_fish(doc):
    assert set(doc["species"]["protected"]) == set(C.PROTECTED_FISH)
    assert X.protected_problems(doc) == []
    bad = {"rules": copy.deepcopy(doc["rules"]), "species": doc["species"]}
    bad["rules"]["zp:protected_species::protected_species.r1"]["fields"]["species"] = [
        "PROTECTED_SPECIES"]
    assert any("names PROTECTED_SPECIES" in p for p in X.protected_problems(bad))


# ---------------------------------------------------------------------------------------------
# 3. Paper licence
# ---------------------------------------------------------------------------------------------
def test_the_paper_licence_says_which_fish(corpus):
    words = [w for w, _ in C.recorded_fish(corpus.values())]
    assert words == ["hatchery steelhead", "adult chinook",
                     "rainbow trout over 50 cm (Main body of Kootenay Lake)",
                     "lake trout and Dolly Varden/bull trout over 60 cm (Shuswap Lake)",
                     "rainbow trout over 50 cm (Shuswap system)"]
    x = next(r for r in corpus["zp:licence_administration"].licensing
             if r.id == "carry_paper_licence")
    parts = C.licensing_parts(x, recorded=words)
    assert C.compose_licensing(parts) == (
        "Anglers 16 and over: carry your paper licence when keeping a fish whose retention you "
        "must record on your licence: hatchery steelhead, adult chinook, rainbow trout over 50 cm "
        "(Main body of Kootenay Lake), lake trout and Dolly Varden/bull trout over 60 cm (Shuswap "
        "Lake) or rainbow trout over 50 cm (Shuswap system).")


def test_mutation_a_new_record_duty_reaches_the_paper_licence(corpus):
    """DERIVED, never typed: a record duty added to the corpus is on the line by itself."""
    e = copy.deepcopy(corpus["zp:salmon_stamp"].model_dump(mode="json", by_alias=True,
                                                           exclude_none=True))
    e["regs_verbatim"] += " You must record your retention of burbot."
    e["rules"].append({"rule_id": "salmon_stamp.r9", "type": "retention_limit",
                       "verbatim": "record your retention of burbot", "species": ["BB"],
                       "record_retention": True,
                       "extents": [{"op": "within", "area_kind": "region"}]})
    got = C.recorded_fish([C.CatalogueEntry.model_validate(e)])
    assert [w for w, _ in got] == ["adult chinook", "burbot"]


def test_the_export_paper_licence_names_every_record_rule(doc):
    assert X.paper_licence_problems(doc) == []
    x = doc["licensing"]["zp:licence_administration#carry_paper_licence"]
    assert x["records"] == X.recorded_rules(doc["rules"]) and len(x["records"]) == 6
    assert "hatchery steelhead" in x["parts"]["records"] and "hatchery steelhead" in x["label"]
    bad = {"rules": doc["rules"], "licensing": copy.deepcopy(doc["licensing"])}
    bad["licensing"]["zp:licence_administration#carry_paper_licence"]["records"].pop()
    assert X.paper_licence_problems(bad)


# ---------------------------------------------------------------------------------------------
# 4. Bait
# ---------------------------------------------------------------------------------------------
def test_the_guide_says_what_a_bait_ban_covers(doc):
    b = doc["guide"]["gear"]["bait"]
    assert "worms" in b["book"]["bait"]["text"] and b["book"]["bait"]["page"] == 8
    assert "worms included" in b["members"]["any_bait"]
    assert "bait ban" in b["worms"]["says"].lower()
    used = {m for x in doc["rules"].values() for c in x["fields"].get("gear") or []
            if c["slot"] == "bait" for k in ("allow", "only", "ban", "except", "of")
            for m in c.get(k) or []}
    assert used <= set(b["members"])


# ---------------------------------------------------------------------------------------------
# 5. Licence classes
# ---------------------------------------------------------------------------------------------
def test_the_fee_table_is_structured(corpus):
    terms = {x.id: x for x in corpus["zp:licence_fees"].licensing}
    assert len(terms) == 16
    a = terms["basic_annual"]
    assert (a.document.value, a.sold, a.fees_cad) == (
        "basic_licence", "per_licence_year",
        {"resident": 41.15, "non_resident": 62.87, "non_resident_alien": 91.44})
    assert terms["basic_eight_day"].valid_days == 8 and terms["basic_one_day"].valid_days == 1
    assert terms["basic_annual_65_plus"].who.status == ["aged_65_plus"]
    assert set(terms["classified_class_i_day"].fees_cad) == {"non_resident", "non_resident_alien"}
    assert terms["classified_annual"].who.residency == ["resident"]


@pytest.mark.parametrize("bad,why", [
    ({"fees_cad": {"resident": 1.0}}, "no fee for"),
    ({"who": {"residency": ["resident"]}, "fees_cad": {"non_resident": 2.0, "resident": 1.0}},
     "does not sell"),
    ({"sold": "per_licence_year", "valid_days": 8, "fees_cad": {
        "resident": 1.0, "non_resident": 1.0, "non_resident_alien": 1.0}}, "valid_days"),
])
def test_mutation_a_class_must_be_sold_to_whom_it_prices(bad, why):
    with pytest.raises(ValueError, match=why):
        C.LicenceTerms.model_validate({"id": "t", "document": "basic_licence", "name": "T",
                                       "verbatim": "T $1", **bad})


def test_the_export_lists_the_classes(doc):
    c = doc["guide"]["licensing"]["classes"]
    assert len(c["classes"]) == 16 and "change" in c["fees"]
    assert set(c["residency"]) == {"page", "resident", "non_resident", "non_resident_alien"}


# ---------------------------------------------------------------------------------------------
# 6. Creston Valley WMA permit
# ---------------------------------------------------------------------------------------------
def _sid(db, item):
    return db.execute("SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
                      "WHERE i.item_id = ? LIMIT 1", (item,)).fetchone()[0]


def test_the_permit_is_a_part_of_kootenay_lake_and_whole_on_duck_lake(db):
    main = R.requirements_in_force(db, _sid(db, KOOTENAY_MAIN), (7, 1))
    assert PERMIT not in main["holds"] and PERMIT_PART not in main["holds"]
    assert main["not_yet_mapped"] == {
        PERMIT_PART: "only the south end, within the Creston Valley Wildlife Management Area"}
    duck = R.requirements_in_force(db, _sid(db, DUCK_LAKE), (7, 1))
    assert PERMIT in duck["holds"] and PERMIT_PART not in duck["holds"]


def test_mutation_an_undrawn_part_needs_a_water():
    with pytest.raises(ValueError, match="undrawn_part"):
        C.Requirement.model_validate({"id": "t", "satisfied_by": [{"hold": ["basic_licence"]}],
                                      "doing": {"act": "fishing"}, "undrawn_part": "the south end",
                                      "verbatim": "a permit is required"})


def test_the_export_marks_the_permit_part(doc):
    x = doc["licensing"][PERMIT_PART]
    assert x["not_yet_mapped"]["part"].startswith("only the south end")
    assert x["parts"]["in_part"].startswith("only the south end")


# ---------------------------------------------------------------------------------------------
# 7. Cut-point names
# ---------------------------------------------------------------------------------------------
@pytest.mark.parametrize("raw,want", [
    ("Kitimat Hatchery outfall — d/s sign", "downstream sign at the Kitimat Hatchery outfall"),
    ("Kitimat Hatchery outfall — u/s sign", "upstream sign at the Kitimat Hatchery outfall"),
    ("Maple Street — sign", "sign at Maple Street"),
    ("signs at the IPP tail race", "signs at the IPP tail race"),
    ("Kootenay Lake — Main Body", "Kootenay Lake — Main Body"),       # a part's name, not a marker
])
def test_cut_name(raw, want):
    assert cut_name(raw) == want


def test_two_things_at_one_place_say_the_place_once():
    a, b = cut_name("Kitimat Hatchery outfall — d/s sign"), cut_name(
        "Kitimat Hatchery outfall — u/s sign")
    assert _between(b, a) == ("between the upstream sign and the downstream sign at the Kitimat "
                              "Hatchery outfall")


def test_no_split_name_is_two_parts(db):
    names = dict(db.execute("SELECT split_id, name FROM split"))
    # a "<place> — <marker>" label is said "<marker> at <place>"; only a lake part's own name
    # ("Kootenay Lake — Main Body", "Shannon Lake — proper") keeps its dash — on the part, and on
    # the part's EDGE (`kind: lake_edge`, named for the lake)
    edges = {s for (s,) in db.execute("SELECT split_id FROM split WHERE kind IN ('lake_edge', 'border')")}
    assert not [n for s, n in names.items() if s not in edges
                and " — " in n and n.split(" — ", 1)[1][:1].islower()]
    assert names["kitimat_river__kitimat_hatchery_outfall_d_s_sign"] == \
        "downstream sign at the Kitimat Hatchery outfall"
