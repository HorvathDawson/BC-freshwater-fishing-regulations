"""T779 ON BUILT DATA: the save-time check of a DFO place (`reuse.dfo_binding_change`, what `apply_proposals` lands a
mark on) binds what the reach run binds, and refuses a place the run's gate would stop on.

- The two Middle Shuswap places (T778): with T778's fix switched off, the piece `355998688:691` straddles the cut again —
  the check lists it and `apply` refuses the mark (the run stopped 14 minutes in on exactly this); with it on, nothing is
  withheld and the mark lands.
- Parity (item 4): the save-time binding equals the reach run's row (`dfo_salmon.jsonl.gz`) for a tidal place and two
  places at the Region 2 / 3 line on the Fraser at Hope. Bound with the old arguments (`regional=True`, no tidal water
  out) the tidal place keeps sections the run takes out (on the 2026-11-23 atlas: 27 of 328 places differed so, all by
  tidal water; the region limit moved none).
"""
from __future__ import annotations

import gzip
import json
import sys

import pytest

from pipeline.tests import under_test

pytestmark = [pytest.mark.slow, pytest.mark.needs_atlas, pytest.mark.needs_reach_run]

SHUSWAP = ("8:shuswap-river:middle-between-shuswap-falls-and-m", "8:shuswap-river:middle-between-shuswap-falls-and-m~2")
PIECE = ("355998688:691", "straddles_the_cut")
TIDAL = "1:chemainus-river:whole"
REGION_LINE = ("2:fraser-river:upstream-from-the-highway-1-bridge", "3:fraser-river:in-that-portion-of-the-fraser-rive")


@pytest.fixture(scope="module")
def backend():
    sys.path.insert(0, "curation-review/backend")
    import dfo
    return dfo, dfo.reuse


@pytest.fixture(scope="module")
def run_rows(request):
    p = under_test.reach_run(request) / "dfo_salmon.jsonl.gz"
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return {r["location_id"]: r for r in map(json.loads, f)}


@pytest.mark.parametrize("lid", SHUSWAP)
def test_a_place_that_withholds_a_straddling_piece_is_refused_and_lands_once_it_is_placed(backend, monkeypatch, lid):
    from pipeline.atlas.reach import extent as X
    from pipeline.regs.review import apply_proposals as AP
    _dfo, reuse = backend
    monkeypatch.setattr(X, "A_PIECE_INTO_THE_CUT_LAKE_IS_JUST_ABOVE_ITS_CUT", False)     # before T778's fix
    got = reuse.dfo_binding_change(lid, [])
    assert {(x["piece"], x["reason"]) for x in got["unaccepted"]} == {PIECE}
    why = AP.withheld_refusal({"id": "x", "scope": "dfo", "kind": "extent", "check": got})
    assert why and f"{PIECE[0]} ({PIECE[1]})" in why
    monkeypatch.setattr(X, "A_PIECE_INTO_THE_CUT_LAKE_IS_JUST_ABOVE_ITS_CUT", True)      # T778's fix: placed
    got = reuse.dfo_binding_change(lid, [])
    assert got["withheld"] == [] and got["stale_acceptances"] == []
    assert AP.withheld_refusal({"id": "x", "scope": "dfo", "kind": "extent", "check": got}) is None


@pytest.mark.parametrize("lid", (TIDAL, *REGION_LINE))
def test_the_save_time_binding_is_the_runs(backend, run_rows, lid):
    dfo, reuse = backend
    from pipeline.atlas.reach.build import build_reach
    _slug, loc, water = dfo._locations()[lid]
    entry, rule = dfo._reach_input(loc, water)
    b, _d = build_reach(entry, rule, reuse._registry(), reuse._graph(), **dfo.as_the_run_binds(lid))
    assert set(b.sections) == set(run_rows[lid]["sections"])
    old, _ = build_reach(entry, rule, reuse._registry(), reuse._graph())             # the arguments it used to pass
    if lid == TIDAL:
        assert set(old.sections) > set(run_rows[lid]["sections"])                     # tidal water the run takes out
