"""ROUND 16 1 (owner review of v17, 2026-10-16) — the card group as a TABLE and an exact CAP SYSTEM. No data.

The owner: "how many 30 cm bull trout can I keep?" is unreadable on the Bella Coola, May 1. Each group now
carries `table` (per species the size bands from the rules' own cut points, each cell the most of that fish
alone here today, why when none) and `caps` over `cap_kinds` (species, band[, origin]) — a basket kept here
and on other streams / lakes today is legal iff every cap holds. The card gate proves every cell against the
engine's maxima and that the caps accept exactly the baskets the engine accepts (`card_check.check_caps`),
on the fixture and on random rule systems (fake rules and verdicts, a fixed seed).
"""
from __future__ import annotations

import copy
import random

from pipeline.deliver.answers import caps as CP
from pipeline.deliver.answers import card_check as CC
from pipeline.tests import answers28_fixture as F
from pipeline.tests.test_answers_28_card_solver import SPECIES, _system

MAY1, OCT10 = (5, 1), (10, 10)


def _group(water, md):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and r["group"]["pool"] == F.TOTAL]
    return g, out, P


def _cells(g, S):
    return [(b["says"], c["n"], c["why"] or c["origin"]) for b, c in zip(g["table"]["bands"][S],
                                                                        g["table"]["cells"][S])]


def test_a_group_carries_the_table_from_the_rules_own_cut_points():
    g, _o, _P = _group(F.BELLA_MAY_ST, MAY1)
    # the Bella Coola's own rows on a Region 2 stream: none under 25 (and Region 2's 30), cutthroat none over 33
    assert _cells(g, "CT") == [("under 30 cm", 0, "too small"), ("30–33 cm", 1, "hatchery"),
                               ("over 33 cm", 0, "too big")]
    assert _cells(g, "DV") == [("under 60 cm", 0, "too small"), ("over 60 cm", 1, "hatchery")]
    # a rainbow over 50 cm on a steelhead water is a steelhead: closed here
    rb = g["table"]["cells"]["RB"][-1]
    assert (g["table"]["bands"]["RB"][-1]["says"], rb["n"], rb["as"], rb["why"]) == ("over 50 cm", 0, "ST", "closed")
    assert g["table"]["steelhead_cm"] == 50
    assert "ST" in g["table"]["bands"]                       # a fish nobody may keep has its row too


def test_the_caps_are_exact_on_the_bella_coola_shape():
    g, _o, _P = _group(F.BELLA_MAY_ST, MAY1)
    caps = g["caps"]
    own = next(c for c in caps if c["by"] == F.BELLA_R5 and c["take"] == 2)
    assert own["if_kept_here"] and own["kinds"]["stream"]     # the river's 2 counts fish kept elsewhere
    assert own["says"] == "Up to 2 trout and char over 25 cm here, counting fish kept elsewhere today."
    ct, rb = "CT.2.h", "RB.2.h"
    assert {ct, rb} <= set(next(c for c in caps if c["by"] == F.BELLA_R6)["kinds"]["here"])
    assert CP.accepts(caps, {"here": {ct: 1}}) and not CP.accepts(caps, {"here": {ct: 1, rb: 1}})
    # two kept on another Region 2 stream: nothing more here (the river's 2), one more from a lake is fine
    assert not CP.accepts(caps, {"stream": {"EB.2.h": 2}, "here": {"EB.2.h": 1}})
    assert CP.accepts(caps, {"stream": {"EB.2.h": 2}, "lake": {"EB.2.w": 1}})


def test_the_gate_proves_every_fixture_card_with_its_cap_system():
    n = 0
    for w in F.WATERS:
        for md in ((1, 15), MAY1, (5, 15), (6, 15), (7, 15), OCT10):
            out, P = F.card(w, md)
            gs = [r["group"] for r in out["rows"] if r.get("group")]
            assert all("caps" in g and "table" in g and "cap_kinds" in g for g in gs), w
            n += len(gs)
            assert CC.check_card(P, out) == [], (w, md)
    assert n > 60


def test_the_gate_refuses_a_wrong_cell_a_wrong_take_and_a_missing_cap():
    g, _o, P = _group(F.BELLA_MAY_ST, MAY1)
    assert CC.check_caps(P, g) == []
    bad = copy.deepcopy(g)
    bad["table"]["cells"]["CT"][1]["n"] = 2
    assert any("CT 30–33 cm says 2" in p for p in CC.check_caps(P, bad))
    bad = copy.deepcopy(g)
    next(c for c in bad["caps"] if c["by"] == F.BELLA_R6)["take"] = 2
    assert any("the caps accept" in p for p in CC.check_caps(P, bad))
    bad = copy.deepcopy(g)
    bad["caps"] = [c for c in bad["caps"] if not (c["by"] == F.BELLA_R5 and c["take"] == 2)]
    assert any("the caps accept" in p for p in CC.check_caps(P, bad))
    bad = copy.deepcopy(g)
    next(c for c in bad["caps"] if c["by"] == F.BELLA_R5 and c["take"] == 2)["if_kept_here"] = False
    assert any("the caps refuse" in p for p in CC.check_caps(P, bad))


def test_accepts_equals_legal_on_random_rule_systems():
    rng = random.Random(20261016)
    for _ in range(300):
        P = _system(rng)
        E = CC.Engine(P, P.cands[0].key)
        pool = E.fish(SPECIES, CC.PLACES) + [(S, o, L, p) for S in SPECIES for o in ("hatchery", "wild")
                                            for L in (10, 45, 70) for p in CC.PLACES]
        for _k in range(8):
            seq = [rng.choice(pool) for _j in range(rng.randint(0, 6))]
            assert E.accepts(seq) == E.legal(seq), seq


def test_the_cap_system_is_exact_on_random_rule_systems():
    rng = random.Random(20261017)
    caps_seen = conditional = 0
    for _ in range(300):
        P = _system(rng)
        sysm = CP.system(P, P.cands[0].key, list(SPECIES))
        g = {"pool": P.cands[0].key, "table": sysm["table"], "cap_kinds": sysm["kinds"], "caps": sysm["caps"]}
        assert CC.check_caps(P, g, baskets=40) == [], ([(r.k, r.f, r.species, r.entry_id) for r in P.cands],
                                                       P.ladder, P.kind)
        caps_seen += len(sysm["caps"])
        conditional += sum(c["if_kept_here"] for c in sysm["caps"])
    assert caps_seen > 300 and conditional > 10, (caps_seen, conditional)
