"""(T994 #14, #15) The review backend's broad catches that turned a failure into an answer.

  #14 `reuse._fwa_lake_poly`   a missing geopackage or a bad blob was None (cached), and the map drew the river
                               through the lake; only "no row for the key" is None now, and the connection closes
  #15 `awaiting_water`         a failed geometry read was "no position" (`_mu_centre` cached it)
      `reports`                no handle table / no bundle was "another vintage" / "no water"
      `dfo.walks_now`          any error was "unknown"; only the reach input's refusal (ValueError) is
      `dfo.rule_labels`        any error fell back to the verbatim words unseen; a refusal now says so
      `inseason._derived_label` an unloaded atlas was the raw pick id; only a water gone from the registry is
Each test fails on the old catch (mutation: put `except Exception: return None` back).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

from pipeline.tests.conftest import curated_root

BACKEND = Path(__file__).resolve().parents[2] / "tools" / "curation-review" / "backend"
_MODS = ("app", "reuse", "writes", "review", "write_through", "awaiting_water", "reports", "dfo", "inseason")


@pytest.fixture()
def be(tmp_path, monkeypatch):
    curated_root(monkeypatch, tmp_path / "curated")
    monkeypatch.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import reuse                                                           # noqa: E402
    yield reuse
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _missing(*_a, **_k):
    raise FileNotFoundError("no registry at /x (build the atlas)")


# ---------------------------------------------------------------------------------------------------- #14

def _gpkg(path: Path, rows: list[tuple[int, bytes]]) -> None:
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE lakes (WATERBODY_KEY INTEGER, geom BLOB)")
        db.executemany("INSERT INTO lakes VALUES (?, ?)", rows)


def _blob(geom) -> bytes:
    # GeoPackage binary: "GP", version 0, flags 0 (no envelope), srs_id, then the WKB
    return b"GP\x00\x00" + (3005).to_bytes(4, "little") + geom.wkb


def test_14_a_missing_geopackage_is_said_not_drawn_as_the_river(be, tmp_path, monkeypatch):
    monkeypatch.setattr(be, "SOURCE", tmp_path)
    be._fwa_lake_poly.cache_clear()
    with pytest.raises(FileNotFoundError, match="bc_fisheries_data.gpkg is missing"):
        be._fwa_lake_poly("123")


def test_14_only_no_row_is_none_and_a_bad_blob_raises_every_time(be, tmp_path, monkeypatch):
    from shapely.geometry import box
    _gpkg(tmp_path / "bc_fisheries_data.gpkg", [(123, _blob(box(0, 0, 1, 1))), (7, b"GP\x00\x00\x00\x00\x00\x00junk")])
    monkeypatch.setattr(be, "SOURCE", tmp_path)
    be._fwa_lake_poly.cache_clear()
    assert be._fwa_lake_poly("123").equals(box(0, 0, 1, 1))
    assert be._fwa_lake_poly("999") is None                                  # not a lake: the caller's through-line
    for _ in range(2):                                                       # raised, so never cached as None
        with pytest.raises(Exception) as got:
            be._fwa_lake_poly("7")
        assert not isinstance(got.value, FileNotFoundError)


# ---------------------------------------------------------------------------------------------------- #15

def test_15_awaiting_water_positions_raise_on_a_failed_read(be, monkeypatch):
    import awaiting_water as A
    monkeypatch.setattr(be, "_registry", _missing)
    with pytest.raises(FileNotFoundError, match="no registry"):
        A._centre("wbk:123")
    lake = type("I", (), {"mus": ("3-15",), "kind": "lake"})()
    monkeypatch.setattr(be, "_registry", lambda: {"wbk:5": lake})
    monkeypatch.setattr(be, "_features", _missing)
    A._mu_centre.cache_clear()
    with pytest.raises(FileNotFoundError):
        A._mu_centre("3-15")


def test_15_reports_say_a_missing_build_or_bundle(be, tmp_path, monkeypatch):
    import reports as R
    from pipeline.deliver.bundle import read as BR
    monkeypatch.setattr(be, "_BUILD", tmp_path / "no-build")
    R._vintage.cache_clear()
    assert R._same_vintage(None) is False                       # a report naming no tiles never asks the build
    with pytest.raises(FileNotFoundError):
        R._same_vintage("abc.def")
    with pytest.raises(FileNotFoundError, match="section_handles"):
        R._node_of(1)
    monkeypatch.setattr(BR, "BUNDLE", str(tmp_path / "none.sqlite"))
    with pytest.raises(FileNotFoundError, match="no bundle at"):
        R._bundle_items(1)
    R._vintage.cache_clear()


def test_15_walks_now_is_unknown_only_when_the_reach_input_refuses(be, monkeypatch):
    import dfo as D

    def refuses(*_a, **_k):
        raise ValueError("a place in tidal water binds nothing")

    def bug(*_a, **_k):
        raise RuntimeError("a bug")
    monkeypatch.setattr(D, "_reach_input", refuses)
    assert D.walks_now({}, None) is None
    monkeypatch.setattr(D, "_reach_input", bug)
    with pytest.raises(RuntimeError):
        D.walks_now({}, None)


def test_15_a_refused_rule_label_says_so_and_a_bug_is_not_a_label(be, monkeypatch):
    import dfo as D
    from pipeline.regs.dfo_salmon import typed as T
    monkeypatch.setattr(T, "as_catalogue", lambda r: {"no_such_field": 1})        # the model refuses it
    got = D.rule_labels([{"rule_id": "r1", "verbatim": "Closed to fishing"}])
    assert got[0]["label"] == "Closed to fishing" and got[0]["refused"]
    monkeypatch.setattr(T, "as_catalogue", _missing)
    with pytest.raises(FileNotFoundError):
        D.rule_labels([{"rule_id": "r1", "verbatim": "Closed to fishing"}])


def test_t788_7_region_rows_and_a_border_label_need_the_build(be, tmp_path, monkeypatch):
    """(T788 item 7) `_region_rows` was `{}` with no splits.resolved.json ("crosses no region line"), and `cut_label`
    caught a missing atlas into the bare atlas label."""
    from types import SimpleNamespace as NS
    monkeypatch.setattr(be, "SPLITS_RESOLVED_PATH", tmp_path / "splits.resolved.json")
    with pytest.raises(FileNotFoundError, match="splits.resolved.json"):
        be._region_rows()
    monkeypatch.setattr(be, "border_end", _missing)
    with pytest.raises(FileNotFoundError):
        be.cut_label(NS(id="border:356566436:0", label="BC boundary", kind="confluence"))


def test_15_a_pick_label_needs_the_atlas(be, monkeypatch):
    import inseason as I
    monkeypatch.setattr(I, "_points", _missing)
    with pytest.raises(FileNotFoundError):
        I._derived_label("gnis:1", "split:x")

    def gone(_item):
        raise KeyError("gnis:1")
    monkeypatch.setattr(I, "_points", gone)
    assert I._derived_label("gnis:1", "split:x") == "split:x"
