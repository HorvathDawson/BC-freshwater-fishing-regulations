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


def test_the_app_takes_out_the_rows_and_the_atlas_record(reuse, monkeypatch):
    """T678: the atlas's tidal water is read as the reach CLI reads it (`coast.read_tidal`), nothing recomputed; an
    atlas cut before the coast rule is refused (the pre-T678 resolvers are retired)."""
    reg = {"lake:nitinat": _Item(["n1"]), "gnis:fraser": _Item(["f1", "f2"]), "gnis:annacis": _Item(["a1"])}
    monkeypatch.setattr(reuse, "_registry", lambda: reg)
    monkeypatch.setattr(reuse, "_graph", lambda: object())
    monkeypatch.setattr(reuse, "_all_entries", lambda: [("2", {"entry_id": "r1:nitinat", "tidal": True,
                                                               "matched": ["lake:nitinat"]})])
    from pipeline.atlas.splits import coast as C
    monkeypatch.setattr(C, "built_by_coast_rule", lambda _b: True)
    monkeypatch.setattr(C, "record_on_nodes", lambda _b: False)
    monkeypatch.setattr(C, "read_record", lambda _b: {"f2": "split:fraser__cpr", "a1": "water:gnis:annacis",
                                                      "k1": "crossing:k1"})
    reuse._tidal_pieces_for.cache_clear()
    got = reuse._tidal()
    assert got == {"n1": "r1:nitinat", "f2": "split:fraser__cpr", "a1": "water:gnis:annacis", "k1": "crossing:k1"}
    # and the owners do their work: the tidal row keeps its own lake, every other row loses all of it
    from pipeline.atlas.reach.build import tidal_limit
    assert set(tidal_limit({"entry_id": "r1:nitinat", "tidal": True}, got)) == {"f2", "a1", "k1"}
    assert set(tidal_limit({"entry_id": "r2:fraser"}, got)) == {"n1", "f2", "a1", "k1"}
    monkeypatch.setattr(C, "built_by_coast_rule", lambda _b: False)          # an atlas cut before T678: refused
    reuse._tidal_pieces_for.cache_clear()
    with pytest.raises(SystemExit, match="not cut by the coast rule"):
        reuse._tidal()
