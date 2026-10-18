"""THE CARD GATE'S CLASSES (round 9, 2026-10-13) — each one the live gate caught, pinned with no data.

The live gate (`python -m pipeline.deliver.answers.card_check`) reported 1,907 problems on the first
round-9 build. Every class was traced to the side that contradicts the book or a ruling and fixed there;
one test per class, on real rule records cut from the bundle (`fixtures/answers28_gate_waters.json`,
beside Region 2's table in `answers28_region2.json`), the REAL reader and the REAL rows:

  A ENGINE  a water's own number limits what is kept THERE; fish kept elsewhere count toward it (rows
            F9), past it the region's rules govern other waters (Deer Lake, Burnaby: trout/char 2, and
            Region 2's 4 still lets 2 more be kept on another lake). The gate held it across waters.
  B CARD    a kind's number is its OWN row's (Ross Lake: trout 1 none under 40 cm — the card said the
            region's 4, from the cross-reference on the region's row).
  C ROWS    a count clause of another table that still speaks caps the fish (RULINGS Q2: Region 5's
            "1 Dolly Varden" and "2 lake trout" under Polley Lake's "trout 8" — the card said 8).
  D ROWS    a fish whose every length the speaking rules release together is released (decision F16:
            Marshall (Lonzo) Creek's trout/char 20-30 cm beside Region 2's char "none under 60 cm"; the
            Coquitlam's hatchery trout 20-30 cm on a steelhead water — the card said "1", "2").
  E CARD    a water's own pool under the region's total is the cap its kinds draw from (P4) and the
            headline is what THIS water allows (P1): Abbott Lake "1 a day", the Dean "1 a day from this
            stream" — the card said the region's 5 and 2.
  F CARD    kinds share a kind row only under one limit: Bridge Lake's lake trout 1 (its row) and
            Region 5's Dolly Varden 1 are two rows (the card said "1" for both together; 2 may be kept).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read
from pipeline.tests import answers28_fixture as F

WATERS_FILE = Path(__file__).parent / "fixtures" / "answers28_gate_waters.json"
PATH = "fixture:answers28_gate"
FISH = ("RB", "CT", "EB", "GB", "DV", "LT", "ST")
JAN15, JUL15, OCT10 = (1, 15), (7, 15), (10, 10)

#: water -> (kind, steelhead water, its region's table entry prefix, its own entry)
WATERS = {
    "deer": ("lake", False, "z2:", "r2:deer_lake_burnaby@2-8"),
    "ross": ("lake", False, "z2:", "r2:ross_lake_boundary_between_ross_lake_and_skagit_river_is_mar@2-2"),
    "lonzo": ("stream", False, "z2:", "r2:lonzo_marshall_creek@2-4"),
    "coquitlam": ("stream", True, "z2:", "r2:coquitlam_river@2-8"),
    "polley": ("lake", False, "z5:", "r5:polley_lake@5-2"),
    "abbott": ("lake", False, "z5:", "r5:abbott_lake@5-2"),
    "dean": ("stream", False, "z5:", "r5:dean_river@5-9"),
    "bridge": ("lake", False, "z5:", "r5:bridge_lake@5-2"),
}


def _table() -> Dict[Tuple[str, str], dict]:
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        recs = json.loads(F.FIXTURE.read_text(encoding="utf-8"))["rules"] + \
            json.loads(WATERS_FILE.read_text(encoding="utf-8"))["rules"]
        got = {(x["entry"], x["rule"]): x for x in recs}
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    return got


class _B:
    def __init__(self, rules, sets):
        self.rules, self.sets = rules, sets


def _bound(water: str) -> List[Tuple[str, str, str]]:
    kind, _sw, zone, own = WATERS[water]
    rules = _table()
    lift = tuple(F.LIFT.split("::"))
    return [(e, r, "reach") for (e, r) in sorted(rules)
            if (e, r) != lift and (e.startswith(zone) or e == own or e.startswith("zp:"))
            and (kind == "stream" or rules[(e, r)].get("water") != "stream")]


def part(water: str, md) -> RW.Part:
    kind, sw, _zone, _own = WATERS[water]
    bound = _bound(water)
    B = _B(_table(), {1: bound})
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, sw, md, f, PATH, steelhead_rules_here=True, origin=None if o == "none" else o,
        trace=True)) for o in ("none", "hatchery", "wild")} for f in FISH}
    return RW.Part(B, 1, sw, True, kind, "known" if sw else None, lad, md, {})


def card(water: str, md):
    P = part(water, md)
    return RW.produce(P), P


def group(out: dict, total: str) -> dict:
    got = [r["group"] for r in out["rows"] if r.get("group") and r["group"]["pool"] == total]
    assert len(got) == 1, [r["group"]["pool"] for r in out["rows"] if r.get("group")]
    return got[0]


def kind_rows(g: dict) -> Dict[str, Tuple[dict, dict]]:
    return {S: (b, k) for b in g["buckets"] for k in b["kinds"] for S in k["species"]}


Z2 = "z2:trout_char_quota::trout_char_quota.r1"
Z5 = "z5:trout_char_quota::trout_char_quota.r1"


def test_every_class_water_passes_the_gate():
    for water in WATERS:
        for md in (JAN15, JUL15, OCT10):
            out, P = card(water, md)
            assert CC.check_card(P, out) == [], (water, md)


# A ---------------------------------------------------------------------------------------------
def test_a_a_waters_own_total_limits_this_water_and_counts_fish_kept_elsewhere():
    out, P = card("deer", JUL15)
    g = group(out, "r2:deer_lake_burnaby@2-8::deer_lake_burnaby.r1")
    assert g["total"] == 2 and g["headline"]["n"] == 2
    E = CC.Engine(P, g["pool"])
    here = E.fish(g["kinds"])
    assert E.most(here, cap=5) == 2
    # Region 2's 4 still lets 2 more be kept on another lake after this lake's 2 (the old gate's
    # "the total says 2, 3 may be kept here and elsewhere" read the lake's 2 as a regional total)
    assert E.most(E.fish(g["kinds"], CC.PLACES), cap=5) == 4
    away = next(f for f in E.fish(g["kinds"], ("lake",)) if f[1] == "hatchery")
    assert E.most(here, (away,), cap=5) == 1                     # a fish kept elsewhere counts here
    assert not [p for p in CC.check_group(P, g) if "total" in p]


def test_a_the_gate_still_fails_a_waters_total_that_overstates_it():
    out, P = card("deer", JUL15)
    g = json.loads(json.dumps(group(out, "r2:deer_lake_burnaby@2-8::deer_lake_burnaby.r1")))
    g["total"] = 3
    assert any("the total says 3" in p for p in CC.check_group(P, g))


# B ---------------------------------------------------------------------------------------------
def test_b_a_kinds_number_is_its_own_rows_not_the_cross_reference():
    out, P = card("ross", JUL15)
    rows = kind_rows(group(out, Z2))
    for S in ("RB", "CT", "GB"):
        assert rows[S][1]["n"] == 1, (S, rows[S][1])           # Ross Lake: trout 1 (it said 4)


# C ---------------------------------------------------------------------------------------------
def test_c_an_orphan_count_clause_caps_its_fish_under_a_larger_water_pool():
    out, P = card("polley", JUL15)
    g = group(out, "r5:polley_lake@5-2::polley_lake.r1")
    rows = kind_rows(g)
    assert rows["DV"][1]["n"] == 1 and rows["LT"][1]["n"] == 2      # Region 5's, Q2 (it said 8)
    assert rows["RB"][1]["n"] == 8


# D ---------------------------------------------------------------------------------------------
def test_d_a_fish_every_length_of_which_is_released_is_released():
    out, P = card("lonzo", JUL15)
    for S in ("DV", "LT"):
        assert out["fish"][S]["hatchery"]["status"] == "release", out["fish"][S]
    assert "DV" not in kind_rows(group(out, Z2))
    out, P = card("coquitlam", JUL15)
    assert out["fish"]["ST"]["hatchery"]["status"] == "release"   # 20-30 cm only: no steelhead
    assert "ST" not in kind_rows(group(out, Z2))
    # a fish with a length left keeps its answer
    assert out["fish"]["RB"]["hatchery"]["status"] == "keep"


# E ---------------------------------------------------------------------------------------------
def test_e_a_waters_own_pool_under_the_total_is_its_bucket_and_the_headline():
    out, _P = card("abbott", JUL15)
    g = group(out, Z5)
    assert g["headline"]["says"] == "1 a day"
    assert [(b["title"], b["cap"]) for b in g["buckets"]] == [("This lake’s rule", 1)]
    assert g["total_line"] == "No more than 5 trout and char a day per angler across Region 5, streams included."
    out, _P = card("dean", JUL15)
    g = group(out, Z5)
    assert g["headline"]["says"] == "1 a day from this stream"
    assert [b["cap"] for b in g["buckets"]] == [1]


# F ---------------------------------------------------------------------------------------------
def test_f_kinds_under_two_limits_are_two_rows():
    out, _P = card("bridge", JUL15)
    rows = kind_rows(group(out, Z5))
    assert rows["DV"][1] is not rows["LT"][1]
    assert rows["DV"][1]["n"] == 1 and rows["LT"][1]["n"] == 1
