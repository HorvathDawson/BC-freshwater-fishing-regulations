"""THE GUIDE'S PROSE EXAMPLES ARE CHECKED (CLEAN round, 2026-10-05; `pipeline/tools/guide_examples.py`).

Every water, date and fish the ladder and gotcha prose argue by is declared as data and re-asked
here of the reference reader; the export refuses a pair where one disagrees
(`export_ui_rules.guide_example_problems`, inside `problems`). Three were wrong in prose when they
were first declared — the Thompson's 2 "in June", Kakwa Lake's 2 "outranking" the zone's 5, and
Denetiah Creek's "own bull trout rule" — and each is pinned here by MUTATION: the old claim, declared,
is refused. `UI_EXPORT_BUNDLE` points the suite at a side bundle.
"""
from __future__ import annotations

import copy
import os
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as RD
from pipeline.tools import export_ui_rules as X
from pipeline.tools import guide_examples as G

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)


@pytest.fixture(scope="module")
def doc() -> dict:
    return X.build(BUNDLE)


@pytest.fixture(scope="module")
def shipped(doc) -> dict:
    return doc["guide"]["examples"]


def test_every_declared_example_is_shipped_and_agrees_with_the_reader(doc, shipped):
    assert set(shipped) == {e.id for e in G.EXAMPLES}
    assert len(shipped) == len(G.EXAMPLES) >= 57
    assert G.example_problems(shipped) == []
    assert X.guide_example_problems(doc) == []


@pytest.mark.parametrize("ex", G.EXAMPLES, ids=lambda e: e.id)
def test_each_example_reasked(ex):
    """Independent of the export: the reader, asked on the example's section, says what the
    example says."""
    import sqlite3
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        sid = G._section(db, ex)
    finally:
        db.close()
    assert sid is not None, f"no section of {ex.water} carries {ex.speaks + ex.part}"
    m, d = (int(x) for x in ex.date.split("-"))
    got = {f"{x['entry']}::{x['rule']}": x["state"]
           for x in RD.effective_rules(sid, (m, d), ex.fish, str(BUNDLE))}
    for r in ex.speaks:
        assert got.get(r) == "speaks", (r, got.get(r))
    for r in ex.silent:
        assert r not in got, (r, got[r])
    for r, s in ex.states.items():
        assert got.get(r) == s, (r, got.get(r))


def test_each_example_is_cited_where_the_prose_names_it(doc):
    """No stale example: the text it cites names its water (or the name the prose uses)."""
    texts = dict(G._texts(doc["guide"]))
    names = {i: w["name"] for i, w in doc["waters"].items()}
    for ex in G.EXAMPLES:
        word = (ex.named_as or names.get(ex.water) or "").split()[0]
        for k in ex.cited_in:
            assert k in texts, (ex.id, k)
            assert word and word in texts[k], (ex.id, k, word)


# ---- mutation: the three claims that were wrong in prose are refused when declared ------------
def _declared(shipped, i, **change) -> dict:
    s = copy.deepcopy(shipped)
    s[i] = {**s[i], **change}
    return s


def test_mutation_the_thompsons_2_in_june_is_refused(shipped):
    """Prose said 'in June the 2' speaks; June is inside Region 3's spring stream closure."""
    x = shipped["thompson_cnr_june_spring_closure"]
    two = G.THOMPSON + ".r2"
    bad = _declared(shipped, "thompson_cnr_june_spring_closure", speaks=[two], silent=[])
    assert any(two in p for p in G.example_problems(bad))
    assert two not in {a["id"] for a in x["expect"]}


def test_mutation_kakwas_2_outranking_the_zones_5_is_refused(shipped):
    """Prose said the lake's 2 outranks the zone's 'Trout/char: 5' by place; both speak."""
    five = G.Z7B + ".r1"
    bad = _declared(shipped, "kakwa_other_trout_lake_2_beside_5",
                    speaks=["r7:kakwa_lake@7-19::kakwa_lake.r2"], silent=[five])
    assert any(five in p and "not silent" in p for p in G.example_problems(bad))


def test_mutation_denetiahs_own_bull_trout_rule_does_not_exist():
    """Prose said Denetiah Creek's own bull trout rule beats the Liard watershed row; the creek's
    row prints only 'No Fishing' (Jul 1-15), and outside it the Liard row's quota speaks."""
    rules = RD._rules_of(str(BUNDLE))
    own = [k for k, x in rules.items() if k[0].startswith("r7:denetiah_creek")
           and "DV" in (x.get("species") or [])]
    assert own == [], own


def test_mutation_prose_naming_an_unchecked_water_is_refused(doc):
    g = copy.deepcopy(doc["guide"])
    g["ladder"]["who_speaks"] += " Bridge Lake's 'Lake trout daily quota = 1' speaks."
    names = {i: w["name"] for i, w in doc["waters"].items()}
    gaps = G.prose_gaps(g, set(names.values()), doc["guide"]["examples"], names)
    assert gaps == ["guide prose ladder.who_speaks names 'Bridge Lake' but no declared example "
                    "checks it (pipeline/tools/guide_examples.py)"]
    assert G.prose_gaps(doc["guide"], set(names.values()), doc["guide"]["examples"], names) == []
