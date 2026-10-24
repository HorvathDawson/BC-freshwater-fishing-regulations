"""TIDAL WATER IS A DIFFERENT REGULATION SYSTEM (user ruling 2026-10-06, FIX D12).

No provincial rule, quota, closure, gear rule, licence or stamp holds on water the book calls tidal
(Nitinat Lake, p.17). Its answers are a DOCUMENTED STATE — "tidal water: see the DFO tidal
regulations / the Fishing BC app" — never an empty or a computed one: before the fix the gear frame
said "angling: not allowed here" for every method and every licence profile said "none needed".

`UI_EXPORT_BUNDLE` and `ANSWERS_EXPORT_DIR` point the suite at a side bundle; it skips without one.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.bundle import read as R
from pipeline.deliver.export import guide_words as export_guide_words
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR")
                  or Path(R.BUNDLE).parent.parent / "regs")
NITINAT, CHILLIWACK = "wbk:329504244", "gnis:8634"


def _built(**patch):
    need(None, "bundle", BUNDLE, BUNDLE_HINT)
    need(None, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, _guide = C.load_export(EXPORT_DIR)
    if NITINAT not in data["waters"]:
        pytest.fail(f"pinned water Nitinat Lake ({NITINAT}) is not in this export")
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=(NITINAT, CHILLIWACK), log=lambda *_: None)
    return data, json.loads(json.dumps(E.encode(model, data)))


@pytest.fixture(scope="module")
def built():
    return _built()


def test_the_export_and_the_answers_say_the_same_words():
    assert export_guide_words.TIDAL_GUIDE.startswith(C.TIDAL_NOTE)
    for words in ("different regulation system", "DFO", "Fishing BC app",
                  "Tidal Waters Sport Fishing Licence"):
        assert words in C.TIDAL_NOTE


@pytest.mark.needs_bundle
@pytest.mark.parametrize("on", [(1, 15), (7, 15), (12, 31)])
def test_nitinat_lake_is_a_documented_tidal_state_all_year(built, on):
    data, wire = built
    assert data["waters"][NITINAT]["tidal"]["guide"] == export_guide_words.TIDAL_GUIDE
    k = wire["parts"][NITINAT][0]
    assert wire["keys"][k][C.PART_KEY_FIELDS.index("tidal")] == 1
    t = E.tap(wire, data, NITINAT, 0, *on)
    assert t["display"] == {"status": "tidal", **C.TIDAL_STATE}
    assert t["gear"] == C.TIDAL_STATE, "never 'angling: not allowed here'"
    assert t["licence"]["holds"] == {"tidal": C.TIDAL_STATE}, "the documented state only"
    assert t["licence"]["profiles"] and all(p == {"tidal": True} for p in t["licence"]["profiles"]), \
        "never 'no licence needed', no computed provincial field"
    # the ladder lists every fish the verdicts asked (DATAFLOW P6, bucket a): on tidal water each
    # holds only the tidal row's own note, SHOWN — no provincial rule speaks, nothing is decided
    assert t["ladder"] and all(
        v == {"r1:nitinat_lake@1-3::nitinat_lake.r1": ["shown", None, None, None]}
        for by_o in t["ladder"].values() for v in by_o.values())
    assert t["rows"]["rows"] == []


@pytest.mark.needs_bundle
def test_a_freshwater_part_is_never_tidal(built):
    data, wire = built
    for i, _p in enumerate(wire["parts"][CHILLIWACK]):
        t = E.tap(wire, data, CHILLIWACK, i, 7, 15)
        if t is None:
            continue
        assert t["display"]["status"] in ("base", "own", "closed") and "tidal" not in t["gear"]


@pytest.mark.needs_bundle
def test_mutation_without_the_tidal_scope_the_old_empty_answers_return(monkeypatch):
    """With `is_tidal` read as False everywhere the gear frame is the computed one again —
    every method 'not allowed here'."""
    monkeypatch.setattr(C, "is_tidal", lambda key: False)
    data, wire = _built()
    t = E.tap(wire, data, NITINAT, 0, 7, 15)
    assert t["display"]["status"] != "tidal" and "tidal" not in t["gear"]
    assert {w["why"] for w in t["gear"]["ways"]} >= {"not_allowed_here"}


@pytest.mark.needs_bundle
def test_mutation_the_licence_half_returns_to_no_licence_needed(monkeypatch):
    """With `licence.TIDAL_IS_DOCUMENTED` off the licence frame is the computed one again: every
    profile "none needed" and the provincial fields beside it — the defect the documented state
    replaces."""
    from pipeline.deliver.answers import licence as L
    monkeypatch.setattr(L, "TIDAL_IS_DOCUMENTED", False)
    data, wire = _built()
    t = E.tap(wire, data, NITINAT, 0, 7, 15)
    assert "tidal" not in t["licence"]["holds"] and "holds" in t["licence"]["holds"]
    assert all(p.get("none_needed") is True for p in t["licence"]["profiles"])


def test_the_tidal_sections_carry_their_versions():
    # answers 2.3 (Q38 gear `caught`, G2 licence, Z12/G3 display): display 4, gear 4, licence 4;
    # answers 2.6: display 5 (the strip code, the rules' book pages and `applies`), gear 5
    # (`count_nouns`); answers 2.7: display 6 (part `short`, static `licensing` book pages);
    # answers 2.8 (d78cda88, D2/P9): display 7 (`rules[].where`, "Where you can’t fish")
    # ROUND10 W4: gear 6 (`counts[slot].cites`, the Lines row's `says`)
    # ROUND11 A2/A3: gear 7 (`summary`), display 8 (frame `inherited`)
    want = {"display": 18, "gear": 12, "licence": 7}   # ROUND13: gear 9; ROUND14: display 10; ROUND15: 11, licence 5; ROUND16: 12, 6; ROUND19: 13; UR1: 14, gear 10; UR3: 15; ROUND20g: 16; UR6: 17; closers: 18; licence 7: the steelhead stamp, youth alternatives
    assert {s.name: s.version for s in A.SECTIONS if s.name in want} == want
    from pipeline.deliver.answers.encode import SPEC
    for name in want:
        sec = SPEC["sections"][name]
        assert sec["version"] == str(want[name]) and "tidal" in json.dumps(sec).lower(), name
