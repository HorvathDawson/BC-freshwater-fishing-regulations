"""The names a page shows for the cuts (`splits`), the waters and the rows.

Three defects the UI builder reported in the first `splits` table (59c188ae), each pinned here on
hand-made input and on the corpus (`UI_EXPORT_BUNDLE`, default the shipped bundle):

  1. A NAME REPEATED ON ONE WATER — 186 names: two "CNR bridge" points on the Thompson, four
     "Halfway River confluence" points on the Peace (the curated offsets were dropped).
  2. A CONFLUENCE NAMED AFTER ITS OWN WATER — 1,057 length cuts the atlas made where a side channel
     of the same river flows back in, labelled with that river's own name ("Gold River" on the
     Gold River).
  3. ALL CAPITALS — area names from the parks layer ("CLAYHURST ECOLOGICAL RESERVE"), and book
     headings ("MITE LAKE", "ENDAKO RIVER").

Each corpus check has a mutation twin: `name_problems` must go red on a broken file.
"""
from __future__ import annotations

import copy
import os
from collections import defaultdict
from pathlib import Path

import pytest

from pipeline.deliver.bundle import spans as SP
from pipeline.deliver.bundle.place_names import display_case, is_shouting
from pipeline.tools import export_ui_rules as X
from pipeline.tests.conftest import need, BUNDLE_HINT

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)


# ---------------------------------------------------------------------------------------------
# Casing
# ---------------------------------------------------------------------------------------------
@pytest.mark.parametrize("raw,want", [
    ("CLAYHURST ECOLOGICAL RESERVE", "Clayhurst Ecological Reserve"),
    ("DEWDNEY AND GLIDE ISLANDS ECOLOGICAL RESERVE", "Dewdney and Glide Islands Ecological Reserve"),
    ("FIELD'S LEASE ECOLOGICAL RESERVE", "Field's Lease Ecological Reserve"),
    ("O'ROURKE LAKE", "O'Rourke Lake"),
    ("ARROW LAKES' TRIBUTARIES", "Arrow Lakes' Tributaries"),
    ("MOORE/MCKENNY/WHITMORE ISLANDS ECOLOGICAL RESERVE",
     "Moore/McKenny/Whitmore Islands Ecological Reserve"),
    ("TSI-EZISH ECOLOGICAL RESERVE", "Tsi-Ezish Ecological Reserve"),
    ("K'ÓMOKS", "K'ómoks"),
    ('"MOSS POTHOLE" LAKES', '"Moss Pothole" Lakes'),
    ("CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS",
     "Creston Valley Wildlife Management Area (CVWMA) Waters"),
    ("GREY HORSE LAKE #1", "Grey Horse Lake #1"),
    ("HART LAKE (Fort St. James)", "Hart Lake (Fort St. James)"),     # a heading + a note
    ("CPR", "CPR"),                                                    # a lone short word
    ("CNR bridge", "CNR bridge"),                                      # mixed case: as written
    ("Kitsumkalum River (KALUM)", "Kitsumkalum River (KALUM)"),
])
def test_display_case(raw, want):
    assert display_case(raw) == want
    assert not is_shouting(want)


@pytest.mark.parametrize("name,shouts", [
    ("MITE LAKE", True), ("CLAYHURST ECOLOGICAL RESERVE boundary", True), ("BLUEY 1", True),
    ("CNR bridge", False), ("CFB Comox [12332677]", False), ("BC Hydro CNR BCR crossing", False),
    ("Mite Lake", False), ("CPR", False),
])
def test_is_shouting(name, shouts):
    assert is_shouting(name) is shouts


# ---------------------------------------------------------------------------------------------
# Naming a cut, on hand-made facts
# ---------------------------------------------------------------------------------------------
def _facts(offsets=None):
    """River `R` (blk 100, item gnis:1 "Big River"), 0..50,000 m in two pieces. Flowing in:
    Eve Creek at 10,000 (gnis:2), a side channel of Big River itself at 20,000, an unnamed
    tributary at 30,000, Fox Creek at 40,000 (gnis:3), and Nine Lake draining straight in at 45,000
    (a lake is no landmark)."""
    item_name = {"gnis:1": "Big River", "gnis:2": "Eve Creek", "gnis:3": "Fox Creek"}
    node_items = {"100:0": {"gnis:1"}, "100:25000": {"gnis:1"}, "200:0": {"gnis:2"},
                  "300:0": {"gnis:1"}, "500:0": {"gnis:3"}}
    joins = {"100": [(10000.0, "200:0", "200", "Eve Creek"), (20000.0, "300:0", "300", "Big River"),
                     (30000.0, "400:0", "400", ""), (40000.0, "500:0", "500", "Fox Creek"),
                     (45000.0, "lake:9", None, "Nine Lake")]}
    pieces = {"100": [(0.0, 25000.0, "100:0"), (25000.0, 50000.0, "100:25000")]}
    return SP.NameFacts(item_name, node_items, joins, pieces, offsets or {})


STEMS = {"100": [("gnis:1", 1)]}


def _r(sid, m, label="", kind="point", blk="100"):
    return {"split_id": sid, "blk": blk, "route_measure": m, "label": label, "anchor_type": kind}


def _names(resolved, offsets=None):
    return {r[0]: r for r in SP.split_rows(resolved, STEMS, _facts(offsets))}


def test_a_length_cut_is_named_for_the_water_that_joins_there():
    got = _names([_r("length:100:10000", 10000, "Eve Creek", "confluence"),
                  _r("length:100:20000", 20000, "Big River", "confluence"),
                  _r("length:100:30000", 30000, "", "confluence"),
                  _r("length:100:45000", 45000, "Nine Lake", "confluence")])
    assert got["length:100:10000"][1] == "Eve Creek confluence"
    # the atlas labelled this one "Big River": a side channel of Big River flowing back in
    assert got["length:100:20000"][1] == "side channel above Eve Creek"
    assert got["length:100:30000"][1] == "unnamed tributary below Fox Creek"
    assert got["length:100:45000"][1] == "Nine Lake outlet"


def test_a_curated_confluence_names_the_other_water_and_keeps_its_offset():
    got = _names([_r("big__eve_into_big", 10000, "Eve Creek → Big River", "confluence"),
                  _r("big__eve_into_big_u5000m", 15000, "Eve Creek → Big River (5000 m upstream)",
                     "confluence"),
                  _r("big__eve_into_big_d500m", 9500, "Eve Creek → Big River (500 m downstream)",
                     "confluence")],
                 offsets={"big__eve_into_big_u5000m": (5000.0, "upstream"),
                          "big__eve_into_big_d500m": (500.0, "downstream")})
    assert got["big__eve_into_big"][1] == "Eve Creek confluence"
    assert got["big__eve_into_big_u5000m"][1] == "5 km upstream of the Eve Creek confluence"
    assert got["big__eve_into_big_d500m"][1] == "500 m downstream of the Eve Creek confluence"
    # on the TRIBUTARY the same label names the receiving river
    assert SP._confluence_name("Eve Creek → Big River", {"Eve Creek"}, None) == \
        "Big River confluence"


def test_a_label_repeated_on_one_water_is_placed_by_its_nearest_landmark():
    got = _names([_r("big__cnr_bridge", 8000, "CNR bridge"),
                  _r("big__cnr_bridge_2", 42000, "CNR bridge")])
    assert got["big__cnr_bridge"][1] == "CNR bridge below Eve Creek"
    assert got["big__cnr_bridge_2"][1] == "CNR bridge above Fox Creek"


def test_then_by_distance_then_upper_and_lower():
    got = _names([_r("big__signs", 8000, "signs"), _r("big__signs_2", 9000, "signs")])
    assert got["big__signs"][1] == "signs 2 km below Eve Creek"
    assert got["big__signs_2"][1] == "signs 1 km below Eve Creek"
    bare = SP.split_rows([_r("big__falls", 8000, "falls"), _r("big__falls_2", 9000, "falls")],
                         STEMS, SP.NameFacts({}, {}, {}, {}, {}))
    assert [r[1] for r in bare] == ["lower falls", "upper falls"]


def test_two_ids_at_one_place_share_a_name_and_say_so():
    got = _names([_r("big__eve_into_big", 10000, "Eve Creek → Big River", "confluence"),
                  _r("eve__eve_into_big", 10000, "Eve Creek → Big River", "confluence")])
    assert got["big__eve_into_big"][1] == got["eve__eve_into_big"][1] == "Eve Creek confluence"
    assert got["big__eve_into_big"][5] is None
    assert got["eve__eve_into_big"][5] == "big__eve_into_big"


def test_an_area_name_is_recased_and_its_official_spelling_kept():
    got = _names([_r("area:CLAYHURST ECOLOGICAL RESERVE", 0, "CLAYHURST ECOLOGICAL RESERVE",
                     "area_boundary"),
                  _r("area:Garibaldi Park", 0, "within Garibaldi Park", "area_boundary")])
    assert got["area:CLAYHURST ECOLOGICAL RESERVE"][1:] == (
        "Clayhurst Ecological Reserve boundary", "area", "[]", "CLAYHURST ECOLOGICAL RESERVE", None,
        None)
    assert got["area:Garibaldi Park"][1] == "Garibaldi Park boundary"
    assert got["area:Garibaldi Park"][4] is None


@pytest.mark.parametrize("m,words", [(30, "30 m"), (500, "500 m"), (1500, "1.5 km"),
                                     (5000, "5 km"), (14830, "14.8 km")])
def test_distance_words(m, words):
    assert SP.distance_words(m) == words


# ---------------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def doc(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    return X.build(BUNDLE)


@pytest.mark.needs_bundle
def test_the_corpus_names_pass(doc):
    assert X.name_problems(doc) == []


@pytest.mark.needs_bundle
def test_within_each_water_split_names_are_unique(doc):
    """Read from the output, independently of `name_problems`: a name stands at ONE place of a
    water (one cut may stand at two — Adams Lake's inlet and outlet); a second id at that place
    names the first in `same_place_as`."""
    seen = defaultdict(list)
    for sid, x in doc["splits"].items():
        if x["kind"] in ("lake_edge", "border"):
            continue          # named for the lake (or the border): at its inlet AND its outlet
        for w, km in X._split_waters(x):
            seen[(w, x["name"])].append((km, sid))
    for (w, name), at in seen.items():
        if len({sid for _, sid in at}) == 1:
            continue
        kms = sorted(km for km, _ in at)
        assert kms[-1] - kms[0] <= 0.01, (w, name, at)
        first = min(sid for _, sid in at)
        for _, sid in at:
            assert sid == first or doc["splits"][sid]["same_place_as"] == first, (w, name, sid)


@pytest.mark.needs_bundle
def test_no_confluence_is_named_after_its_own_water(doc):
    S, W = doc["splits"], doc["waters"]
    bad = [(sid, x["name"], w) for sid, x in S.items() if x["kind"] == "confluence"
           for w, _ in X._split_waters(x)
           if W[w]["name"].lower() in (x["name"].lower(),
                                       x["name"].lower().removesuffix(" confluence"),
                                       x["name"].lower().split(" of the ")[-1]
                                       .removesuffix(" confluence"))]
    assert bad == []
    side = [x for x in S.values() if x["name"].startswith("side channel")]
    assert len(side) >= 1_000         # the 1,057 self-named length cuts, and their kin off-stem


def official_name_problems(splits: dict) -> list[str]:
    """An AREA boundary's `name` is its source name as a reader sees it (`area_display`) +
    " boundary", and `official_name` is the source spelling exactly when the two differ."""
    from pipeline.deliver.bundle.place_names import area_boundary_name
    out = []
    for k, x in splits.items():
        if x.get("kind") != "area" or not k.startswith("area:"):
            if x.get("official_name"):
                out.append(f"{k}: official_name on a {x.get('kind')} cut")
            continue
        src = k.split(":", 1)[1]
        shown, _ = area_boundary_name(src)
        if x["name"] != f"{shown} boundary":
            out.append(f"{k}: name {x['name']!r}, want {shown + ' boundary'!r}")
        if x.get("official_name") != (src if shown != src else None):
            out.append(f"{k}: official_name {x.get('official_name')!r}")
    return out


@pytest.mark.needs_bundle
def test_official_name_check_sees_each_mistake(doc):
    """Mutations of the shipped splits, each refused: an OSM-id area losing its source spelling, a
    re-cased area whose official_name is the shown name, an area name still carrying its id, and an
    official_name on an area whose name was not changed."""
    import copy
    S = doc["splits"]
    assert official_name_problems(S) == []
    osm = next(k for k in S if k.startswith("area:") and "[" in k)
    caps = next(k for k in S if k.startswith("area:") and k.split(":", 1)[1].isupper())
    plain = next(k for k in S if k.startswith("area:") and not S[k].get("official_name"))
    for k, edit in ((osm, lambda x: x.pop("official_name")),
                    (caps, lambda x: x.update(official_name=x["name"].removesuffix(" boundary"))),
                    (osm, lambda x: x.update(name=x["official_name"] + " boundary")),
                    (plain, lambda x: x.update(official_name="X"))):
        m = copy.deepcopy(S)
        edit(m[k])
        assert official_name_problems(m), k


@pytest.mark.needs_bundle
def test_no_displayed_name_is_all_capitals(doc):
    shown = ([w["name"] for w in doc["waters"].values()]
             + [e["name"] for e in doc["entries"].values() if e["name"]]
             + [x["name"] for x in doc["splits"].values()])
    assert [n for n in shown if is_shouting(n)] == []
    # `official_name` is the SOURCE spelling wherever the shown name differs from it — re-cased,
    # an OpenStreetMap id dropped, a cut-off word restored — not necessarily capitals
    assert official_name_problems(doc["splits"]) == []
    official = [x for x in doc["splits"].values() if x.get("official_name")]
    # the all-capitals source shown in title case is still the common case
    caps = [x for x in official if x["official_name"] == x["official_name"].upper()]
    assert len(caps) >= 100 and all(not is_shouting(x["name"]) for x in caps)
    assert any(x["name"] == "Clayhurst Ecological Reserve boundary"
               and x["official_name"] == "CLAYHURST ECOLOGICAL RESERVE" for x in caps)


# ---- the four the UI builder named ----------------------------------------------------------
@pytest.mark.needs_bundle
def test_thompson_cnr_bridges(doc):
    S = doc["splits"]
    assert S["thompson_river__cnr_bridge"]["name"] == "CNR bridge below Deadman River"
    assert S["thompson_river__cnr_bridge_2"]["name"] == "CNR bridge above Bonaparte River"


@pytest.mark.needs_bundle
def test_peace_halfway_keeps_its_offsets(doc):
    S = doc["splits"]
    got = {sid: (S[sid]["name"], S[sid]["km"], S[sid].get("same_place_as")) for sid in (
        "peace_river__halfway_river_into_peace_river_d5000m",
        "halfway_river__halfway_river_into_peace_river",
        "halfway_river__halfway_river_into_peace_river_u5000m",
        "peace_river__halfway_river_into_peace_river_u5000m")}
    assert {k: v[0] for k, v in got.items()} == {
        "peace_river__halfway_river_into_peace_river_d5000m":
            "5 km downstream of the Halfway River confluence",
        "halfway_river__halfway_river_into_peace_river": "Halfway River confluence",
        "halfway_river__halfway_river_into_peace_river_u5000m":
            "5 km upstream of the Halfway River confluence",
        "peace_river__halfway_river_into_peace_river_u5000m":
            "5 km upstream of the Halfway River confluence"}
    # the two upstream ids are one curated cut authored twice: one place, one name
    assert got["peace_river__halfway_river_into_peace_river_u5000m"][2] == \
        "halfway_river__halfway_river_into_peace_river_u5000m"
    kms = [v[1] for v in got.values()]
    assert kms[1] - kms[0] == pytest.approx(5) and kms[2] - kms[1] == pytest.approx(5)
    assert kms[3] == kms[2]


@pytest.mark.needs_bundle
def test_gold_river_cut_is_a_side_channel_not_the_gold(doc):
    x = doc["splits"]["length:354154308:48965"]
    assert x["water_id"] == "gnis:17593" and doc["waters"]["gnis:17593"]["name"] == "Gold River"
    assert x["name"].startswith("side channel ") and "Gold River" not in x["name"]


@pytest.mark.needs_bundle
def test_clayhurst_is_title_case_with_its_official_name(doc):
    x = doc["splits"]["area:CLAYHURST ECOLOGICAL RESERVE"]
    assert x["name"] == "Clayhurst Ecological Reserve boundary"
    assert x["official_name"] == "CLAYHURST ECOLOGICAL RESERVE"


# ---- mutation: the checks go red -------------------------------------------------------------
def _mutated(doc, fn):
    bad = dict(doc, splits=copy.deepcopy(doc["splits"]), waters=dict(doc["waters"]),
               entries=dict(doc["entries"]))
    fn(bad)
    return X.name_problems(bad)


def _rename(sid, name):
    return lambda d: d["splits"][sid].update(name=name)


@pytest.mark.needs_bundle
@pytest.mark.parametrize("label,mutate,expect", [
    ("repeat a name on the Thompson",
     _rename("thompson_river__cnr_bridge_2", "CNR bridge below Deadman River"), "2 places"),
    ("drop the Halfway offset",
     _rename("peace_river__halfway_river_into_peace_river_d5000m", "Halfway River confluence"),
     "2 places"),
    ("a co-located id that does not say so",
     lambda d: d["splits"]["peace_river__halfway_river_into_peace_river_u5000m"].pop(
         "same_place_as"), "says no `same_place_as`"),
    ("a same_place_as pointing elsewhere",
     lambda d: d["splits"]["peace_river__halfway_river_into_peace_river_u5000m"].update(
         same_place_as="thompson_river__cnr_bridge"), "is no cut at its place"),
    ("the Gold cut named after the Gold", _rename("length:354154308:48965", "Gold River confluence"),
     "named after that water"),
    ("a bare self-name", _rename("length:354154308:48965", "Gold River"), "named after that water"),
    ("an area in capitals", _rename("area:CLAYHURST ECOLOGICAL RESERVE",
                                    "CLAYHURST ECOLOGICAL RESERVE boundary"), "all capitals"),
    ("a water in capitals",
     lambda d: d["waters"].update({"gnis:17593": dict(d["waters"]["gnis:17593"],
                                                      name="GOLD RIVER")}), "all capitals"),
])
def test_name_problems_catch_a_broken_name(doc, label, mutate, expect):
    out = _mutated(doc, mutate)
    assert any(expect in p for p in out), (label, out[:5])


def test_area_display_drops_the_osm_id_and_restores_cut_off_words():
    """An OpenStreetMap id is never part of a name, and the source layer's 50-character cut is
    restored."""
    from pipeline.deliver.bundle.place_names import area_display
    assert area_display("CFB Comox [12332677]") == "CFB Comox"
    assert area_display("Blaney Bog Regional Park Reserve [1066227155]") == \
        "Blaney Bog Regional Park Reserve"
    assert area_display("VLADIMIR J. KRAJINA (PORT CHANAL) ECOLOGICAL RESER") == \
        "Vladimir J. Krajina (Port Chanal) Ecological Reserve"
    assert area_display("MACKAY CREEK") == "Mackay Creek"


@pytest.mark.needs_bundle
def test_no_split_name_shows_an_osm_id_or_a_cut_off_word(doc):
    import re
    bad = [(k, v["name"]) for k, v in doc["splits"].items()
           if re.search(r"\[\d+\]", v["name"] or "") or re.search(r"\bReser\b", v["name"] or "")]
    assert not bad, bad[:5]
