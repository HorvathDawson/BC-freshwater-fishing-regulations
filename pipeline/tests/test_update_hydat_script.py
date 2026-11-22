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
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVED = "data/generated/gauges/feeds"

PYTHON_STUB = """#!/usr/bin/env bash
here=$(pwd); echo "${here#$TREE}|$*" >> "$TREE/log"
case "$*" in
  *"fetch_data.py --layers hydat"*) [ -n "$LANDS" ] && printf %s "$LANDS" > source/hydat.release ;;
  *hydat_latest_release*) printf '%s\\n' "$LISTED" ;;
esac
exit 0
"""
HEAVY_STUB = """#!/usr/bin/env bash
echo "heavy|$*" >> "$TREE/log"
# the build with --hold-latest names its release in CANDIDATE (LATEST untouched); BUILT empty = it named none
if [ -n "${BUILT:-}" ]; then printf '%s\\n' "$BUILT" > data/release/CANDIDATE; fi
exit 0
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
    # the live release (LATEST) and the one the build makes (named in CANDIDATE by the heavy stub)
    for rel in ("r-new", "r-old"):
        (tmp_path / f"data/release/{rel}").mkdir(parents=True)
        (tmp_path / f"data/release/{rel}/manifest.json").write_text("{}")
    (tmp_path / "data/release/LATEST").write_text("r-old\n")
    return tmp_path


R2 = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")


def _run(tree: Path, lands: str, listed: str, *args: str, r2: bool = False, **env) -> subprocess.CompletedProcess:
    # the owner's own R2 credentials never reach a test run; `r2` sets dummies (the stubs send nothing)
    e = {**{k: v for k, v in os.environ.items() if k not in R2}, "TREE": str(tree), "LANDS": lands, "LISTED": listed,
         "BUILT": "r-new", **({k: "x" for k in R2} if r2 else {}), **env}
    return subprocess.run(["bash", str(tree / "scripts/update-hydat.sh"), *args], env=e, capture_output=True,
                          text=True)


def _log(tree: Path) -> list[str]:
    lines = (tree / "log").read_text().splitlines()
    # the listing check is a python -c one-liner: pin that it asks hydat_latest_release, not its exact text
    return ["|-c <latest>" if "|-c " in ln and "hydat_latest_release" in ln else ln for ln in lines]


def test_a_new_hydat_reruns_every_consumer_in_order_on_the_served_feed(tree):
    r = _run(tree, lands="20261020", listed="20261020")
    assert r.returncode == 0, r.stderr
    assert _log(tree) == [
        "/data|fetch_data.py --layers hydat",
        "|-c <latest>",
        f"|-m pipeline.gauges.feed.climatology --hydat data/source/hydat.sqlite3 --out {SERVED}/clim.json",
        # (T994 #1) the release waits in CANDIDATE for the release command's checks; nothing after the build runs
        "heavy|build 40 -- .venv/bin/python -m pipeline build --hold-latest",
    ]
    assert "release r-new waits in data/release/CANDIDATE, NOT uploaded" in r.stdout
    assert "       .venv/bin/python -m pipeline release --staging" in r.stdout
    assert (tree / "data/release/LATEST").read_text() == "r-old\n"


def test_another_feed_dir_is_not_honoured(tree):
    # `pipeline build` packs the bundle from the SERVED clim.json only; an envelope written elsewhere would leave the
    # bundle on the old HYDAT while the script reported it rebuilt. The committed dev fixture is never touched.
    r = _run(tree, lands="20261020", listed="20261020", FEED="app/packages/data/dev/feeds")
    assert r.returncode == 0, r.stderr
    assert not any("app/packages/data/dev/feeds" in ln for ln in _log(tree))
    assert f"|-m pipeline.gauges.feed.climatology --hydat data/source/hydat.sqlite3 --out {SERVED}/clim.json" in _log(tree)


def test_no_second_gauge_publisher_runs(tree):
    # (T557) the feeds worker is the one publisher: the owner's HYDAT command rebuilds the envelope, never the feed
    r = _run(tree, lands="20261020", listed="20261020")
    assert r.returncode == 0, r.stderr
    assert not any("pipeline.gauges.feed.publish" in ln for ln in _log(tree))


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


# T994 #1: the script uploads nothing. Its old `--upload` sent the release to whatever R2_BUCKET named, past every
# deploy gate (`pipeline.deliver.release.publish --yes`). Mutation: put the publish call back -> both tests fail.
def test_upload_is_refused_and_runs_nothing(tree):
    r = _run(tree, "20261020", "20261020", "--upload", r2=True)
    assert r.returncode == 2
    assert not (tree / "log").exists()
    assert "--upload is gone (T994 #1)" in r.stderr and "pipeline release --staging" in r.stderr


def test_no_publisher_runs_even_with_r2_credentials_set(tree):
    r = _run(tree, "20261020", "20261020", r2=True)
    assert r.returncode == 0, r.stderr
    assert not any("pipeline.deliver.release.publish" in ln or "boto3" in ln for ln in _log(tree))
    assert "--yes" not in r.stdout


def test_an_unknown_flag_is_usage_and_runs_nothing(tree):
    r = _run(tree, "20261020", "20261020", "--yes")
    assert r.returncode == 2
    assert not (tree / "log").exists()
    assert "usage: scripts/update-hydat.sh" in r.stderr


def test_no_release_after_the_build_says_so(tree):
    # the build named no release: the script must SAY it (never end silently right after the half-hour build)
    r = _run(tree, "20261020", "20261020", BUILT="")
    assert r.returncode == 1
    assert "the build named no release in data/release/CANDIDATE; nothing to hand over." in r.stderr


def test_a_candidate_left_by_an_earlier_build_is_not_handed_over(tree):
    # (T994 review) a release that stopped at its checks left CANDIDATE; a build that made nothing new must not hand
    # that one over as this HYDAT's. Mutation: drop the before/after compare -> exit 0 and r-old handed over.
    (tree / "data/release/CANDIDATE").write_text("r-old\n")
    r = _run(tree, "20261020", "20261020", BUILT="")
    assert r.returncode == 1
    assert "the build made no new release: data/release/CANDIDATE still names r-old, left by an earlier build" in r.stderr
    assert "release --staging" not in r.stdout
    # a build that names a new release over the old candidate hands the new one over
    r = _run(tree, "20261020", "20261020")
    assert r.returncode == 0, r.stderr
    assert "release r-new waits in data/release/CANDIDATE" in r.stdout
