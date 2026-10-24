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
          "answers": ["ui-rules-answers.json"], "card_check": ["card-check.json"],
          "store": ["regs.sqlite"], "release": []}
#: what a run keeps: everything but the regs store, which is the release's input only (P20, T61 2.4)
KEPT = [f for fs in WRITES.values() for f in fs if f != "regs.sqlite"]


def _fake_steps(monkeypatch, fail_at=None, vintage_ok=True):
    ran = []

    def step(args):
        name = args[0]
        ran.append(name)
        if name == fail_at:
            raise subprocess.CalledProcessError(1, args)
        if name == "release":                       # a folder, under --out-root
            # owner 2026-10-21 (lake modes prepared, not fed): no lake values ride in the chain, so the release ships none
            assert "--lake-facts" not in args, args
            r = Path(args[args.index("--out-root") + 1]) / "r-test"
            r.mkdir(parents=True)
            (r / "manifest.json").write_text("new manifest")
            (r.parent / "LATEST").write_text("r-test\n")    # the release step names it (T72)
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
    for f in KEPT:
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
    assert {n: t for n, (t, _m) in _state(dest).items() if n != "release"} == {f: f"new {f}" for f in KEPT}
    assert not (dest / "regs.sqlite").exists()                      # P20: the store is not kept
    assert (dest / "release" / "r-test" / "manifest.json").read_text() == "new manifest"
    assert [p.name for p in tmp_path.iterdir()] == ["out"]


def test_debug_json_keeps_the_regs_store_the_release_was_cut_from(monkeypatch, tmp_path):
    dest = tmp_path / "out"
    _live(dest)
    _fake_steps(monkeypatch)
    assert D.main(["all", "--out-dir", str(dest), "--debug-json"]) == 0
    assert (dest / "regs.sqlite").read_text() == "new regs.sqlite"


def test_the_canonical_run_promotes_each_file_to_its_own_home():
    """Without --out-dir the files go to their homes; every file a run keeps has one, and the regs store has none
    unless --debug-json asks for it (then the debug directory, never beside the bundle)."""
    from pipeline.deliver.store import DEBUG_OUT
    homes = D.canonical_homes()
    assert set(homes) == set(KEPT) and homes["card-check.json"].parent == homes["ui-rules-answers.json"].parent
    debug = D.canonical_homes(debug=True)
    assert set(debug) == set(KEPT) | {"regs.sqlite"} and debug["regs.sqlite"] == DEBUG_OUT
    assert DEBUG_OUT.parent != homes["bundle.sqlite"].parent


def test_the_answers_step_writes_one_file_and_no_gzip_copy(tmp_path):
    """F19 (audit 2026-10-22): `ui-rules-answers.json.gz` was written and promoted, and nothing read it (the release
    gzips its own copies). The answers step writes the JSON alone; a second file would also be refused by `deliver
    all`, which promotes only files that have a canonical home."""
    from pipeline.deliver.answers.cli import _write
    out = tmp_path / "ui-rules-answers.json"
    sizes = _write(b'{"a":1}\n', out)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["ui-rules-answers.json"]
    assert out.read_bytes() == b'{"a":1}\n' and sizes["raw"] == 8
    assert not any(n.endswith(".gz") for n in D.canonical_homes())


def test_a_live_latest_pointer_is_replaced_not_treated_as_a_release(monkeypatch, tmp_path):
    """T72 (2026-10-24 build): the promote moved the staged LATEST onto the live one with rmtree and crashed after
    the files were promoted. Only r-* folders are promoted; LATEST is re-written once the folder is in place."""
    dest = tmp_path / "out"
    _live(dest)
    (dest / "release").mkdir()
    (dest / "release" / "LATEST").write_text("r-old\n")
    _fake_steps(monkeypatch)
    assert D.main(["all", "--out-dir", str(dest)]) == 0
    assert (dest / "release" / "LATEST").read_text().strip() == "r-test"
    assert sorted(p.name for p in (dest / "release").iterdir()) == ["LATEST", "r-test"]
