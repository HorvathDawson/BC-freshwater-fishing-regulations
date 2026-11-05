"""T337: the DFO water bindings are the owner's, kept as written; the build checks each still names a registry item and
stops naming the water when one does not (never re-matched by name)."""
from types import SimpleNamespace

import pytest

from pipeline.atlas.reach import dfo_salmon as D


def _ef(*waters):
    return SimpleNamespace(waters=[SimpleNamespace(region=r, name=n, water_id=w, item_ids=ids) for r, n, w, ids in waters])


def test_every_id_in_the_registry_is_clean():
    ef = _ef(("6", "Cranberry River", "6:cranberry-river", ["gnis:4674"]))
    assert D.stale_bindings(ef, {"gnis:4674": object()}) == []


def test_an_id_the_registry_lost_is_named():
    ef = _ef(("6", "Cranberry River", "6:cranberry-river", ["gnis:4674"]),
             ("1", "Stamp River", "1:stamp-river", ["gnis:1", "blk:9"]))
    got = D.stale_bindings(ef, {"gnis:4674": object(), "gnis:1": object()})
    assert got == ["r1 Stamp River (1:stamp-river): blk:9"]


def test_resolve_all_stops_on_a_stale_binding(monkeypatch):
    from pipeline.regs.dfo_salmon import entries as E
    ef = SimpleNamespace(locations=[], **vars(_ef(("2", "Fraser River", "2:fraser-river", ["gnis:gone"]))))
    monkeypatch.setattr(D, "_slugs", lambda: ["2"])
    monkeypatch.setattr(E, "load", lambda *a, **k: ef)
    with pytest.raises(ValueError, match=r"r2 Fraser River \(2:fraser-river\): gnis:gone"):
        D.resolve_all({}, None, outside=object())
