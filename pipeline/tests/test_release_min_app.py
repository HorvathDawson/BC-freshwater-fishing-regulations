"""T993 lane G2: what the release knows BEFORE a build — the minimum app derived from the code's formats (item 2), the
size headroom (item 9) and the pieces gate's worker (T788 #2). The orchestration is pinned in test_release_command.py
(`world`); these pin the pieces it stands in for."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline import release as R
from pipeline.common.curated import REPO_ROOT
from pipeline.deliver.release import build as RB
from pipeline.deliver.release import packed as PACKED

MINI = Path(REPO_ROOT) / "app" / "packages" / "data" / "dev" / "mini-release"


def _mini_manifest() -> dict:
    rid = (MINI / "LATEST").read_text().strip()
    return json.loads((MINI / rid / "manifest.json").read_text(encoding="utf-8"))


def test_the_codes_formats_are_a_built_releases_files():
    """`build.built_formats` is what the build writes: the committed mini release (built by the real pipeline from the
    invented world, `mini_release --check` in CI) carries exactly those files in exactly those formats. A new file a
    switch holds back, or a format bumped in one place, fails here — the pre-build min app would be wrong otherwise.
    The one data-dependent difference: a salmon region shard with no runs is not written (the mini world has no
    section outside every region: no `salmon-x`); its format is every other shard's, so the derivation is the same."""
    built = {k: v["format"] for k, v in _mini_manifest()["files"].items()}
    code = RB.built_formats()
    assert {k: code[k] for k in built} == built
    extra = set(code) - set(built)
    assert extra <= {k for k in code if k.startswith("salmon-")} and {code[k] for k in extra} <= set(built.values())


def test_mutation_the_contract_alone_is_not_what_the_build_writes(monkeypatch):
    """The mutation: the contract's FORMATS as they stand (with `lakes`, which no producer makes) is not a built release;
    and the `flow-own` switch moves the set."""
    assert set(RB.FORMATS) - set(RB.built_formats()) == {"lakes"}
    monkeypatch.setattr(PACKED, "SHIP_OWN_GAUGE_FLAG", False)
    assert "flow-own" not in RB.built_formats()


def test_the_derived_min_app_is_what_a_built_release_needs():
    """The code's derivation answers what the app gate would answer on the built release (no gap, T993 item 2)."""
    from pipeline.deliver.release.app_gate import oldest_reader
    assert R.derived_min_app() == oldest_reader(_mini_manifest()) is not None


def test_the_default_is_the_stamp_raised_never_lowered(monkeypatch):
    monkeypatch.setattr(R, "derived_min_app", lambda: "0.8.4")
    for stamp, want in (("0.8.0", "0.8.4"), ("0.8.5", "0.8.5"), (None, "0.8.4")):
        monkeypatch.setattr(R, "manifest_of", lambda rid, s=stamp: {"minAppVersion": s} if s else {})
        assert R.default_min_app("r-live") == want
    monkeypatch.setattr(R, "derived_min_app", lambda: None)
    monkeypatch.setattr(R, "manifest_of", lambda rid: {"minAppVersion": "0.8.0"})
    assert R.default_min_app("r-live") == "0.8.0"


def test_a_rise_names_the_apps_it_leaves_behind():
    apps = ["0.7.0", "0.8.0", "0.8.3", "0.8.4", "0.8.5", "0.8.10"]
    assert R.left_behind("0.8.0", "0.8.4", apps) == ["0.8.0", "0.8.3"]
    assert R.left_behind("0.8.4", "0.8.4", apps) == [] and R.left_behind("0.8.5", "0.8.4", apps) == []
    assert R.left_behind(None, "0.8.4", apps) == ["0.7.0", "0.8.0", "0.8.3"]


def test_the_pieces_gate_asks_the_worker_the_upload_asks():
    """T788 #2: one staging worker. deploy/staging/upload.sh names it (G3 may move it into the script's own variable:
    the URL must still be there)."""
    assert R.STAGING_FEEDS_URL in (Path(REPO_ROOT) / "deploy" / "staging" / "upload.sh").read_text(encoding="utf-8")


def test_a_pieces_gate_that_cannot_read_is_a_stop(monkeypatch):
    from pipeline.deliver import salmon as S

    def boom(rid):
        raise S.SalmonError("salmon: no manifest")
    monkeypatch.setattr(R, "pieces_problems", boom)
    monkeypatch.setattr(R, "read_pointer", lambda name: None)
    f = R.check_pieces(R.Ctx(live="r-live"))
    assert not f.ok and "could not run on r-live" in f.said and "SalmonError" in f.said


def _release(tmp: Path, files: dict) -> Path:
    d = tmp / "r-x"
    d.mkdir()
    (d / "manifest.json").write_text(json.dumps({"files": {k: {"bytes": v * 3, "gz": v, "url": f"r-x/{k}"}
                                                           for k, v in files.items()}}))
    return d


def test_headroom_is_tightest_first_with_every_gate(tmp_path):
    """T993 item 9 (0.8.5 stop 2: `water` was 2.1 % under its gate on LATEST; a column took it over after the build)."""
    rows = RB.headroom(_release(tmp_path, {"water": 2_750_000, "core": 1_000_000, "paint": 900_000, "regs-6": 400_000}))
    assert rows[0][0] == "water" and rows[0][1:3] == (2_750_000, RB.SIZE_GATES["water"])
    assert rows[0][3] == pytest.approx((RB.SIZE_GATES["water"] - 2_750_000) / RB.SIZE_GATES["water"])
    names = [r[0] for r in rows]
    assert {"core", "paint", "offline total"} <= set(names) and all(f"web {k}" in names for k in RB.C.WEB_BUDGET)
    assert [r[3] for r in rows] == sorted(r[3] for r in rows)


def test_a_file_with_no_gate_shows_as_over(tmp_path):
    rows = RB.headroom(_release(tmp_path, {"no-such-file": 10, "regs-6": 10}))
    assert rows[0][:3] == ("no-such-file", 10, 0) and rows[0][3] < 0


def test_the_rehearsal_warns_under_five_percent():
    rows = [("water", 2_750_000, 2_850_000, 0.035), ("salmon-6", 415_307, 460_000, 0.097),
            ("core", 1_000_000, 1_650_000, 0.39)]
    said = "\n".join(R.headroom_said("r-x", rows, "  ", show=2))
    assert "water" in said and "3.5%  WARN" in said and "salmon-6" in said and "core" not in said
    assert "1 under 5%" in said and "9.7%  WARN" not in said
