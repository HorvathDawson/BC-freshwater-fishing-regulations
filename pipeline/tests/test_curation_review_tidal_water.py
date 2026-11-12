"""T166 (left after phase 1): the review app takes out ALL the tidal water the reach run does — the rows marked tidal, the
piece below every tidal split, every whole tidal water — with its owners, so a rule reviewed there binds what ships.
It used to take the rows alone (`outside.tidal_sections`), and its frozenset made a tidal row keep nothing special."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"


@pytest.fixture()
def reuse(monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    sys.modules.pop("reuse", None)
    import reuse as R
    yield R
    R._tidal_pieces_for.cache_clear()
    sys.modules.pop("reuse", None)


class _Item:
    def __init__(self, secs):
        self.section_ids = tuple(secs)


def test_the_app_takes_out_the_rows_the_cut_pieces_and_the_whole_waters(reuse, monkeypatch):
    import pipeline.atlas.reach.outside as O
    reg = {"lake:nitinat": _Item(["n1"]), "gnis:fraser": _Item(["f1", "f2"]), "gnis:annacis": _Item(["a1"])}
    monkeypatch.setattr(reuse, "_registry", lambda: reg)
    monkeypatch.setattr(reuse, "_graph", lambda: object())
    monkeypatch.setattr(reuse, "_all_entries", lambda: [("2", {"entry_id": "r1:nitinat", "tidal": True,
                                                               "matched": ["lake:nitinat"]})])
    monkeypatch.setattr(O, "tidal_splits", lambda _b: ("fraser__cpr",))
    monkeypatch.setattr(O, "tidal_cut_owner", lambda _r, _g, ids, strict: {"f2": f"split:{ids[0]}"})
    monkeypatch.setattr(O, "tidal_waters", lambda _b: ("gnis:annacis",))
    monkeypatch.setattr(O, "inside_lines", lambda _b: ())      # this test's atlas lists no line tidal whole
    from pipeline.atlas.splits import coast as C
    monkeypatch.setattr(C, "built_by_coast_rule", lambda _b: False)     # an atlas cut before T678
    reuse._tidal_pieces_for.cache_clear()
    got = reuse._tidal()
    assert got == {"n1": "r1:nitinat", "f2": "split:fraser__cpr", "a1": "water:gnis:annacis"}
    # T678: an atlas cut by the coast rule: its one tidal record is read, nothing recomputed (the resolvers are not asked)
    monkeypatch.setattr(C, "built_by_coast_rule", lambda _b: True)
    monkeypatch.setattr(C, "read_record", lambda _b: {"f2": "split:fraser__cpr", "k1": "crossing:k1"})
    monkeypatch.setattr(O, "tidal_cut_owner", lambda *a, **k: (_ for _ in ()).throw(AssertionError("recomputed")))
    reuse._tidal_pieces_for.cache_clear()
    assert reuse._tidal() == {"n1": "r1:nitinat", "f2": "split:fraser__cpr", "k1": "crossing:k1"}
    monkeypatch.setattr(C, "built_by_coast_rule", lambda _b: False)
    monkeypatch.setattr(O, "tidal_cut_owner", lambda _r, _g, ids, strict: {"f2": f"split:{ids[0]}"})
    reuse._tidal_pieces_for.cache_clear()
    # and the owners do their work: the tidal row keeps its own lake, every other row loses all three
    from pipeline.atlas.reach.build import tidal_limit
    assert set(tidal_limit({"entry_id": "r1:nitinat", "tidal": True}, got)) == {"f2", "a1"}
    assert set(tidal_limit({"entry_id": "r2:fraser"}, got)) == {"n1", "f2", "a1"}
