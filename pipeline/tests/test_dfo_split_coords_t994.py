"""(T994 #16) `splitwork.split_coords` says why it has no coordinates: a missing input raises, naming the file.

One `except Exception: return {}` covered the resolved splits, the registry, the gpkg and geopandas, so every failure
read as "no coordinates" for every split (AGENTS 37 / W46). Mutation: put the catch back -> both tests fail.
"""
from __future__ import annotations

import json

import pytest

from pipeline.regs.dfo_salmon import splitwork as SW


def _build_at(monkeypatch, d):
    monkeypatch.setattr(SW, "GENERATED", type("G", (), {"build": staticmethod(lambda: d)})())
    monkeypatch.setattr(SW, "_COORD_CACHE", {})


def test_no_resolved_splits_names_the_file(tmp_path, monkeypatch):
    _build_at(monkeypatch, tmp_path)
    with pytest.raises(FileNotFoundError, match="splits.resolved.json"):
        SW.split_coords(["gnis:1"])


def test_no_registry_names_the_file(tmp_path, monkeypatch):
    (tmp_path / "splits.resolved.json").write_text(json.dumps([{"split_id": "s", "blk": 1, "route_measure": 0}]))
    _build_at(monkeypatch, tmp_path)
    with pytest.raises((FileNotFoundError, SystemExit), match="registry"):
        SW.split_coords(["gnis:1"])
    assert SW._COORD_CACHE == {}                     # a failure is never cached as "no coordinates"
