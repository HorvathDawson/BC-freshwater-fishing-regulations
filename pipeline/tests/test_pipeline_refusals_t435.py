"""T435 (audit 2026-11-05): no quiet default where a missing thing should stop.

  get_path      a config key that names no path raises (it returned `Path()`, the cwd)
  dfo save      a region file is validated (reads back as itself) and written atomically
  page notes    a missing slough page-calls file raises (it read as "no notes")
"""
from __future__ import annotations

import json

import pytest

from pipeline.regs.dfo_salmon import entries as DE
from pipeline.regs.review import slough as S
from project_config import ProjectConfig


def test_get_path_raises_on_a_missing_key():
    cfg = ProjectConfig()
    with pytest.raises(KeyError, match="no.such.key"):
        cfg.get_path("no", "such", "key")
    assert str(cfg.get_path("no", "such", default="x/y")) == "x/y"


def test_dfo_save_round_trips_and_leaves_no_temp(tmp_path):
    ef = DE.load("1")
    p = DE.save(ef, tmp_path)
    assert DE.load("1", tmp_path).to_dict() == ef.to_dict()
    assert [x.name for x in tmp_path.iterdir()] == [p.name]


def test_dfo_save_refuses_what_does_not_read_back(tmp_path, monkeypatch):
    ef = DE.load("1")
    real = DE._from_dict
    monkeypatch.setattr(DE, "_from_dict", lambda d: real({**d, "region_name": "other"}))
    with pytest.raises(ValueError, match="does not read back"):
        DE.save(ef, tmp_path)
    assert not list(tmp_path.iterdir())


def test_slough_page_notes_missing_file_raises(tmp_path):
    S._page_notes.cache_clear()
    with pytest.raises(OSError):
        S._page_notes(str(tmp_path / "gone.json"), 0.0)
    (tmp_path / "p.json").write_text(json.dumps({"verdicts": {"1": {"note": " hi "}, "2": {"note": "complex:cx-1"}}}))
    assert S._page_notes(str(tmp_path / "p.json"), 1.0) == {"1": "hi"}
