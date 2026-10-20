"""P07: `deliver all` IS ATOMIC. Every step writes into a staging directory beside the destination. Only after the
last step AND `vintage.report_all` pass is each file moved into place (`os.replace`). A step that fails leaves every
live file as it was (same bytes, same mtime) and removes the staging directory, so a failed run never leaves a mixed
vintage.

No data: the steps are replaced by fakes that write their files, and one of them is made to fail.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from pipeline.deliver import __main__ as D

#: what each step writes, by the names `deliver all` gives them
WRITES = {"bundle": ["bundle.sqlite"], "verdicts": ["verdicts.sqlite"], "status_index": ["status_index.bin"],
          "export": ["ui-rules-export.json", "ui-rules-guide.json"],
          "answers": ["ui-rules-answers.json", "ui-rules-answers.json.gz"], "card_check": ["card-check.json"],
          "store": ["regs.sqlite"], "release": []}


def _fake_steps(monkeypatch, fail_at=None, vintage_ok=True):
    ran = []

    def step(args):
        name = args[0]
        ran.append(name)
        if name == fail_at:
            raise subprocess.CalledProcessError(1, args)
        if name == "release":                       # a folder, under --out-root
            r = Path(args[args.index("--out-root") + 1]) / "r-test"
            r.mkdir(parents=True)
            (r / "manifest.json").write_text("new manifest")
            return
        out = Path(args[args.index("--out") + 1]).parent if "--out" in args else Path(args[args.index("--export-dir") + 1])
        for f in WRITES[name]:
            (out / f).write_text(f"new {f}")
    monkeypatch.setattr(D, "_step", step)
    monkeypatch.setattr("pipeline.common.vintage.report_all",
                        lambda *a, **k: (vintage_ok, "vintage: ok" if vintage_ok else "VINTAGE MISMATCH"))
    return ran


def _live(dest: Path) -> dict:
    dest.mkdir(parents=True, exist_ok=True)
    for files in WRITES.values():
        for f in files:
            (dest / f).write_text(f"old {f}")
            os.utime(dest / f, (1_000_000_000, 1_000_000_000))
    return {f.name: (f.read_text(), f.stat().st_mtime) for f in dest.iterdir() if f.is_file()}


def _state(dest: Path) -> dict:
    return {f.name: (f.read_text(), f.stat().st_mtime) for f in dest.iterdir() if f.is_file()}


@pytest.mark.parametrize("fail_at", ["bundle", "export", "answers", "card_check", "store", "release"])
def test_a_failing_step_leaves_every_live_file_unchanged(monkeypatch, tmp_path, fail_at):
    dest = tmp_path / "out"
    before = _live(dest)
    _fake_steps(monkeypatch, fail_at=fail_at)
    with pytest.raises((subprocess.CalledProcessError, SystemExit)):
        D.main(["all", "--out-dir", str(dest)])
    assert _state(dest) == before
    assert [p.name for p in tmp_path.iterdir()] == ["out"]           # no staging directory left behind


def test_a_vintage_mismatch_promotes_nothing(monkeypatch, tmp_path):
    dest = tmp_path / "out"
    before = _live(dest)
    _fake_steps(monkeypatch, vintage_ok=False)
    assert D.main(["all", "--out-dir", str(dest)]) == 1
    assert _state(dest) == before


def test_a_passing_run_promotes_every_file_and_leaves_no_staging(monkeypatch, tmp_path):
    dest = tmp_path / "out"
    _live(dest)
    ran = _fake_steps(monkeypatch)
    assert D.main(["all", "--out-dir", str(dest)]) == 0
    assert ran == ["bundle", "verdicts", "status_index", "export", "answers", "card_check", "store", "release"]
    assert {n: t for n, (t, _m) in _state(dest).items() if n != "release"} == \
        {f: f"new {f}" for fs in WRITES.values() for f in fs}
    assert (dest / "release" / "r-test" / "manifest.json").read_text() == "new manifest"
    assert [p.name for p in tmp_path.iterdir()] == ["out"]


def test_the_canonical_run_promotes_each_file_to_its_own_home():
    """Without --out-dir the files go to three homes; every file a step writes has one (none is dropped)."""
    homes = D.canonical_homes()
    assert set(homes) == {f for fs in WRITES.values() for f in fs}
    assert homes["regs.sqlite"].name == "regs.sqlite" and homes["card-check.json"].parent == homes["ui-rules-answers.json"].parent
