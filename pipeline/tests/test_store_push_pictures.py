"""T1127: the two store push scripts take their pictures from app/tools/store-shots.json, never from a glob.

A leftover picture in a size folder, or a listed one that is missing, stops the run by name in plan mode (no network),
which is where every mode begins. The scripts are run as the owner runs them (their file names have hyphens).
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "deploy/staging"
SET = ROOT / "handoff/PLAN/app-integration/store/graphics/store/light"
sys.path.insert(0, str(STAGING))
import store_shots_list  # noqa: E402

PLAY, ASC = STAGING / "play-listing-push.py", STAGING / "appstore-screenshots-push.py"
TARGETS = ["play-phone", "play-tablet7", "play-tablet10", "appstore-6.9", "appstore-6.5"]


def _run(script: Path, *args: str) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(script), *args]
    if script == PLAY:
        cmd.append("--plan")
    return subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)   # plan mode: no network, no key read


def _set_copy(tmp_path: Path) -> Path:
    dst = tmp_path / "light"
    dst.mkdir()
    for t in TARGETS:
        shutil.copytree(SET / t, dst / t)
    return dst


def _args(script: Path, root: Path) -> list[str]:
    if script == ASC:
        return ["--shots", str(root)]
    return ["--shots", str(root / "play-phone"), "--tablet7", str(root / "play-tablet7"), "--tablet10", str(root / "play-tablet10")]


def test_helper_list_matches_what_the_json_implies():
    """Written out again here from the JSON (store-shots.mjs: listFor + 1-based two-digit index), for every target."""
    src = json.loads((ROOT / "app/tools/store-shots.json").read_text(encoding="utf-8"))
    assert set(TARGETS) == set(src["targets"])
    for t in src["targets"]:
        want = [f"{i + 1:02d}-{s['scene']}.png" for i, s in enumerate(x for x in src["shots"] if not x.get("only") or t in x["only"])]
        assert store_shots_list.expected(t) == want and len(want) == 7


def test_helper_list_is_what_is_committed():
    for t in TARGETS:
        assert sorted(p.name for p in (SET / t).glob("*.png")) == store_shots_list.expected(t), t


@pytest.mark.parametrize("script", [PLAY, ASC], ids=["play", "appstore"])
def test_the_real_set_passes_and_prints_counts(script):
    r = _run(script)
    assert r.returncode == 0, r.stdout + r.stderr
    assert r.stdout.count("01-01-map.png") >= (3 if script == PLAY else 2)
    assert "7 screenshots" in r.stdout or ": 7 " in r.stdout


@pytest.mark.parametrize("script,folder", [(PLAY, "play-phone"), (PLAY, "play-tablet7"), (PLAY, "play-tablet10"),
                                           (ASC, "appstore-6.9"), (ASC, "appstore-6.5")])
def test_a_stray_picture_stops_the_run_by_name(script, folder, tmp_path):
    root = _set_copy(tmp_path)
    shutil.copy(root / folder / "01-01-map.png", root / folder / "02-06-search-OLD.png")
    r = _run(script, *_args(script, root))
    assert r.returncode != 0
    assert "02-06-search-OLD.png" in r.stdout + r.stderr


@pytest.mark.parametrize("script", [PLAY, ASC], ids=["play", "appstore"])
def test_a_missing_picture_stops_the_run_by_name(script, tmp_path):
    root = _set_copy(tmp_path)
    folder = "appstore-6.5" if script == ASC else "play-tablet10"
    (root / folder / "03-21-rivers.png").unlink()
    r = _run(script, *_args(script, root))
    assert r.returncode != 0
    assert "missing" in r.stderr and "03-21-rivers.png" in r.stderr


def test_the_appstore_script_ignores_folders_it_has_no_slot_for(tmp_path):
    root = _set_copy(tmp_path)
    for old in ("appstore-5.5", "appstore-6.7", "appstore-ipad13"):
        (root / old).mkdir()
        shutil.copy(root / "appstore-6.9" / "01-01-map.png", root / old / "09-old.png")
    r = _run(ASC, "--shots", str(root))
    assert r.returncode == 0, r.stderr
    assert "09-old.png" not in r.stdout
