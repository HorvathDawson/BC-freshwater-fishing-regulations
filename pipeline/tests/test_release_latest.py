"""T72: the release LATEST names is current, whatever a later touch does to an older folder's mtime; the prune keeps
LATEST, PUBLISHED, one previous and --keep, and removes only the rest."""
import json
import os
import time

import pytest

from pipeline.deliver.release.manifest import latest, write_latest
from pipeline.tools import prune_releases as P


def _rel(root, rid, at):
    d = root / rid
    d.mkdir(parents=True)
    (d / "manifest.json").write_text(json.dumps({"data_release": rid}))
    os.utime(d / "manifest.json", (at, at))
    return d


@pytest.fixture
def root(tmp_path):
    now = time.time()
    for i, rid in enumerate(["r-old", "r-mid", "r-new", "r-pub"]):
        _rel(tmp_path, rid, now - 1000 + i * 100)
    return tmp_path


def test_a_touched_older_release_is_never_picked(root):
    write_latest(root, "r-mid")
    os.utime(root / "r-old" / "manifest.json")          # touched after the release step: newest by mtime
    assert latest(root).name == "r-mid"


def test_without_a_pointer_the_newest_manifest_stands_in(root):
    assert latest(root).name == "r-pub"


def test_a_pointer_to_a_missing_release_is_refused(root):
    write_latest(root, "r-gone")
    with pytest.raises(SystemExit):
        latest(root)


def test_the_prune_keeps_latest_published_previous_and_keep(root):
    _rel(root, "r-extra", time.time() - 5000)
    write_latest(root, "r-mid")
    (root / "PUBLISHED").write_text("r-old\n")
    kept, gone = P.plan(root, keep=["r-extra"])
    assert {d.name for d in kept} == {"r-mid", "r-old", "r-pub", "r-extra"}   # r-pub: the newest other one
    assert [d.name for d in gone] == ["r-new"]


def test_the_dry_run_removes_nothing(root, capsys):
    write_latest(root, "r-new")
    P.main(["--root", str(root)])
    assert sorted(p.name for p in root.glob("r-*")) == ["r-mid", "r-new", "r-old", "r-pub"]
    assert "would remove r-old" in capsys.readouterr().out
    P.main(["--root", str(root), "--yes"])
    assert sorted(p.name for p in root.glob("r-*")) == ["r-new", "r-pub"]
