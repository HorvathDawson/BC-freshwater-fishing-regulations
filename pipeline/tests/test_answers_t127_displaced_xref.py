"""T127: a pool the reader DISPLACED for a fish yields no "counts toward this one" cross-reference.

On Perry Creek (gnis:14741, a St. Mary River tributary) the St. Mary's "trout/char 1 on all tributaries" row
(st_mary_river.r2) listed brook trout as "has its own limit, counts toward this one (1)", though the reader DISPLACED that
row for brook trout (reason own_row: Perry Creek's own "brook trout 20" governs, 20 may be kept). `rows.build_model` made
a cross-reference from every role `replaced` or `contains`; `group_of` and `total_of` counted only `contains`. One
predicate now decides all three — `rows.counts_toward`, the reader's own "who governs this kind here" answer (the pool
governs the fish, or holds it as the total around its winner) — so the card gate's `_kind_n`, which reads every item
holding a kind, is no longer bounded by such a cross-reference and needs no special case.
"""
from __future__ import annotations

import inspect
import os
import re
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
PERRY = "gnis:14741"
ST_MARY_TRIBS = "r4:st_mary_river@4-20::st_mary_river.r2"


def _build(items):
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=list(items), sections=["rows", "ladder"], log=lambda *_: None)


@pytest.fixture(scope="module")
def perry(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, _ = C.load_export(EXPORT_DIR)
    return data, _build([PERRY])


def _st_mary(data, model, md=(6, 15)):
    k = model.parts[PERRY][0]
    segs = model.segments[k]
    card = model.sections["rows"][k][segs.index(segs[E.segment_index(segs, CAL.day_of(md))])]
    return next(r for r in card["rows"] if r["pool"] is not None and data["rule_ids"][r["pool"]] == ST_MARY_TRIBS)


@pytest.mark.needs_bundle
def test_the_st_mary_row_on_perry_creek_lists_no_brook_trout(perry):
    data, model = perry
    for md in ((6, 15), (7, 15), (10, 10)):
        row = _st_mary(data, model, md)
        assert (row["members"], row["items"]) == (["GB", "LT", "RB"], [
            {"members": ["CT", "DV"], "bands": None, "back": True, "xref": False, "sub": None, "conds": [1]},
            {"members": ["GB", "LT", "RB"], "bands": [[30, None, 1]], "back": False, "xref": False, "sub": None,
             "conds": []}]), md
        assert row["group"]["kinds"] == ["GB", "LT", "RB"]


@pytest.mark.needs_bundle
def test_the_card_gate_proves_perry_creek(perry):
    from pipeline.deliver.answers import card_check as CC
    data, model = perry
    assert CC.check_model(model, BUNDLE, data, log=lambda *_: None)["problems"] == []


@pytest.mark.needs_bundle
def test_mutation_the_displaced_pool_cross_references_the_brook_trout_again(perry, monkeypatch):
    """MUTATION: without the ruling the St. Mary's row says Perry Creek's brook trout count toward its 1 (`against` 1),
    which the reader displaced for them — the words T127 removed."""
    data, _ = perry
    monkeypatch.setattr(RW, "A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE", False)
    row = _st_mary(data, _build([PERRY]))
    xs = [it for it in row["items"] if it["xref"]]
    assert xs == [{"members": ["EB"], "bands": None, "back": False, "xref": True, "sub": None, "conds": [],
                   "against": 1, "origins": [{"o": "hatchery", "n": 20, "lo": 0, "hi": None},
                                             {"o": "wild", "n": 20, "lo": 0, "hi": None}]}]


def test_one_predicate_decides_what_a_fish_counts_toward():
    """No second copy: the `contains` role is read in one place (`counts_toward`), and the three readers — the
    cross-reference, the group's kinds and the outer total — all call it."""
    src = Path(RW.__file__).read_text()
    reads = [ln for ln in src.splitlines() if re.search(r"(==|\bin)\s*\(?[^)\n]*\"contains\"", ln)]
    assert len(reads) == 1 and "return m[\"win\"].key == key" in reads[0], reads
    for fn in (RW.build_model, RW.group_of, RW.total_of):
        assert "counts_toward(" in inspect.getsource(fn), fn.__name__
