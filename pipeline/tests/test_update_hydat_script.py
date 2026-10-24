"""`scripts/update-hydat.sh` RUN, against stubs (owner 2026-10-22 late: HYDAT first).

The script is copied into a throwaway tree whose `.venv/bin/python` and `scripts/heavy.sh` are stubs that only log
what they were asked, from where. Nothing is downloaded, built or sent. What is pinned is what the owner's one command
actually does: every HYDAT consumer in order, on the SERVED feed directory (the one `pipeline build` packs clim.json
from), and it stops before rebuilding anything when the fetch did not land the newest HYDAT (fetch_data.py reports a
failed listing or download on stdout and still exits 0).
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVED = "data/generated/gauges/feeds"

PYTHON_STUB = """#!/usr/bin/env bash
here=$(pwd); echo "${here#$TREE}|$*" >> "$TREE/log"
case "$*" in
  *"fetch_data.py --layers hydat"*) [ -n "$LANDS" ] && printf %s "$LANDS" > source/hydat.release ;;
  *hydat_latest_release*) printf '%s\\n' "$LISTED" ;;
  # (T156) step 5 asks THE release on disk: answered by the real `manifest.latest` over the sandbox's data/release
  *"pipeline.deliver.release.manifest import latest"*) PYTHONPATH="$REPO" exec "$REAL_PY" "$@" ;;
esac
exit 0
"""
HEAVY_STUB = """#!/usr/bin/env bash
echo "heavy|$*" >> "$TREE/log"
"""


def _exe(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "scripts").mkdir()
    shutil.copy(ROOT / "scripts/update-hydat.sh", tmp_path / "scripts/update-hydat.sh")
    _exe(tmp_path / ".venv/bin/python", PYTHON_STUB)
    _exe(tmp_path / "scripts/heavy.sh", HEAVY_STUB)
    (tmp_path / "data/source").mkdir(parents=True)
    (tmp_path / "data/source/hydat.release").write_text("20260717")
    # (T68) step 5 uploads the NEWEST release by time: r-old is made second but its manifest is dated older.
    # (T156) a release is a folder holding a manifest (`manifest.latest`); without one the script says "no release"
    for rel in ("r-new", "r-old"):
        (tmp_path / f"data/release/{rel}").mkdir(parents=True)
        (tmp_path / f"data/release/{rel}/manifest.json").write_text("{}")
    os.utime(tmp_path / "data/release/r-old/manifest.json", (1_000_000, 1_000_000))
    return tmp_path


R2 = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")


def _run(tree: Path, lands: str, listed: str, *args: str, r2: bool = False, **env) -> subprocess.CompletedProcess:
    # the owner's own R2 credentials never reach a test run; `r2` sets dummies (the stubs send nothing)
    e = {**{k: v for k, v in os.environ.items() if k not in R2}, "TREE": str(tree), "LANDS": lands, "LISTED": listed,
         "REPO": str(ROOT), "REAL_PY": sys.executable,
         **({k: "x" for k in R2} if r2 else {}), **env}
    return subprocess.run(["bash", str(tree / "scripts/update-hydat.sh"), *args], env=e, capture_output=True,
                          text=True)


def _log(tree: Path) -> list[str]:
    lines = (tree / "log").read_text().splitlines()
    # the listing check is a python -c one-liner: pin that it asks hydat_latest_release, not its exact text
    # and so is the release lookup: pin that it asks `manifest.latest`
    return ["|-c <latest>" if "|-c " in ln and "hydat_latest_release" in ln
            else "|-c <release>" if "|-c " in ln and "manifest import latest" in ln else ln for ln in lines]


def test_a_new_hydat_reruns_every_consumer_in_order_on_the_served_feed(tree):
    r = _run(tree, lands="20261020", listed="20261020")
    assert r.returncode == 0, r.stderr
    assert _log(tree) == [
        "/data|fetch_data.py --layers hydat",
        "|-c <latest>",
        f"|-m pipeline.gauges.feed.climatology --hydat data/source/hydat.sqlite3 --out {SERVED}/clim.json",
        f"|-m pipeline.gauges.feed.publish --out {SERVED} --clim {SERVED}/clim.json",
        "heavy|build 40 -- .venv/bin/python -m pipeline build",
        "|-c <release>",
        # (T68) step 5 without --upload: the publisher's PLAN of the newest release; nothing is sent
        "|-m pipeline.deliver.release.publish --release data/release/r-new",
    ]
    assert "  .venv/bin/python -m pipeline.deliver.release.publish --release data/release/r-new --yes" in r.stdout


def test_another_feed_dir_is_not_honoured(tree):
    # `pipeline build` packs the bundle from the SERVED clim.json only; an envelope written elsewhere would leave the
    # bundle on the old HYDAT while the script reported it rebuilt. The committed dev fixture is never touched.
    r = _run(tree, lands="20261020", listed="20261020", FEED="app/packages/data/dev/feeds")
    assert r.returncode == 0, r.stderr
    assert not any("app/packages/data/dev/feeds" in ln for ln in _log(tree))
    assert f"|-m pipeline.gauges.feed.publish --out {SERVED} --clim {SERVED}/clim.json" in _log(tree)


def test_a_fetch_that_did_not_land_the_newest_stops_before_any_rebuild(tree):
    # the download failed (fetch_data prints and exits 0): the marker is still the old release
    r = _run(tree, lands="", listed="20261020")
    assert r.returncode != 0
    assert _log(tree) == ["/data|fetch_data.py --layers hydat", "|-c <latest>"]
    assert "HYDAT 20261020 is listed but data/source holds 20260717" in r.stderr


def test_an_unreadable_listing_stops_before_any_rebuild(tree):
    r = _run(tree, lands="", listed="")
    assert r.returncode != 0
    assert _log(tree) == ["/data|fetch_data.py --layers hydat", "|-c <latest>"]
    assert "could not read the HYDAT listing" in r.stderr


# T68 review: the R2 upload step, run.
PLAN = "|-m pipeline.deliver.release.publish --release data/release/r-new"


def test_upload_sends_the_planned_release_once_after_the_build(tree):
    r = _run(tree, "20261020", "20261020", "--upload", r2=True)
    assert r.returncode == 0, r.stderr
    log = _log(tree)
    # boto3 is checked up front AND again right before the send (the credentials are checked both times too)
    assert log[0] == "|-c import boto3"
    assert log[-5:] == ["heavy|build 40 -- .venv/bin/python -m pipeline build", "|-c <release>", PLAN, "|-c import boto3",
                        PLAN + " --yes"]
    assert "release uploaded to R2 (data/release/r-new)" in r.stdout


def test_upload_without_credentials_stops_before_any_work(tree):
    r = _run(tree, "20261020", "20261020", "--upload")
    assert r.returncode == 1
    assert not (tree / "log").exists()
    assert "--upload: R2_ACCOUNT_ID is not set; nothing uploaded." in r.stderr


def test_an_unknown_flag_is_usage_and_runs_nothing(tree):
    r = _run(tree, "20261020", "20261020", "--yes")
    assert r.returncode == 2
    assert not (tree / "log").exists()
    assert "usage: scripts/update-hydat.sh [--upload]" in r.stderr


def test_no_release_after_the_build_says_so(tree):
    # the build wrote no release: the script must SAY it (under `set -euo pipefail` an empty `ls` glob used to end
    # the script silently, exit 1 and no word, right after the half-hour build)
    shutil.rmtree(tree / "data/release")
    r = _run(tree, "20261020", "20261020")
    assert r.returncode == 1
    assert "no release under data/release; nothing to upload." in r.stderr
    assert not any("pipeline.deliver.release.publish" in ln for ln in _log(tree))
