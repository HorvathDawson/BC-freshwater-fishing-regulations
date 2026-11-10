"""T547 / T599 step 1: the atlas records the curated inputs it was built from (`pipeline.atlas.inputs`), and no cheap
build step writes the curated tree.

Pinned: every input key digests; a byte change of any input names that file; a cut's label is not a cut but its anchor
is; a skipped input (`--no-splits`) is "behind"; an empty record is refused with the backfill command; the backfill
writes only what it proved; every `CURATED.` key the atlas build's modules name is an input or a listed non-input;
`write_artifact` writes the same pickle bytes with or without a record (the record moves only the meta file).
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import pickle
import re
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.atlas import inputs as I
from pipeline.common.curated import CURATED, make_root
from pipeline.common.io.serialize import write_artifact

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def gpkg(tmp_path):
    p = tmp_path / "fwa.gpkg"
    p.write_bytes(b"not a real gpkg")
    return p


def _atlas(tmp_path: Path, hashes: dict) -> Path:
    d = tmp_path / "atlas"
    d.mkdir(exist_ok=True)
    write_artifact({"g": 1}, str(d / "graph.pkl"), input_hashes=hashes)
    return d


def test_every_input_digests_and_a_current_atlas_is_not_behind(tmp_path, gpkg):
    now = I.digests({"source:fwa_gpkg": gpkg})
    assert list(now) == list(I.ATLAS_INPUTS)
    assert now["curated:waters.cuts"].startswith("anchors:")
    assert now["source:fwa_gpkg"].startswith("stamp:")
    assert all(v.startswith("sha256:") for k, v in now.items() if k not in ("curated:waters.cuts", "source:fwa_gpkg"))
    assert I.behind(_atlas(tmp_path, now), now) == []


@pytest.mark.parametrize("key", [k for k in I.ATLAS_INPUTS if k.startswith("curated:")])
def test_a_changed_input_is_named(tmp_path, gpkg, key):
    now = I.digests({"source:fwa_gpkg": gpkg})
    old = dict(now, **{key: now[key][:-4] + "0000"})
    got = I.behind(_atlas(tmp_path, old), now)
    assert len(got) == 1
    assert got[0].startswith(f"{I.curated_path(key).name} ({key}): changed since the atlas was built — rebuild: ")
    assert I.REBUILD in got[0]


def test_an_empty_record_is_refused_with_the_backfill_command(tmp_path, gpkg):
    got = I.behind(_atlas(tmp_path, {}), I.digests({"source:fwa_gpkg": gpkg}))
    assert len(got) == 1 and "records no inputs" in got[0] and "--backfill" in got[0]


def test_a_skipped_or_unrecorded_input_is_behind(tmp_path, gpkg):
    now = I.digests({"source:fwa_gpkg": gpkg})
    rec = dict(now, **{"curated:waters.cuts": I.not_read("--no-splits")})
    del rec["curated:waters.slough_calls"]
    got = I.behind(_atlas(tmp_path, rec), now)
    assert [g.split(" (")[0] for g in got] == [I.curated_path("curated:waters.cuts").name,
                                                I.curated_path("curated:waters.slough_calls").name]
    assert "built without it, not read (--no-splits)" in got[0]
    assert "records no digest" in got[1]


def test_build_paths_follow_the_build_flags(gpkg):
    import argparse
    a = argparse.Namespace(gpkg=str(gpkg), no_splits=True, splits=None, name_variants=None, no_added_lakes=False,
                           added_lakes="/x/lakes.geojson", no_added_streams=True, added_streams=None)
    got = I.build_paths(a)
    assert got["curated:waters.cuts"] == got["curated:waters.tidal"] == "not read (--no-splits)"
    assert got["curated:waters.added_streams"] == "not read (--no-added-streams)"
    assert got["curated:waters.added_lakes"] == Path("/x/lakes.geojson")
    assert "curated:waters.name_variants" not in got     # the default file


def test_a_cut_label_is_not_a_cut_but_its_anchor_is(tmp_path):
    doc = json.loads(Path(CURATED.waters.cuts).read_text(encoding="utf-8"))
    wb = next(w for w in doc["waterbodies"] if w.get("splits"))
    base = tmp_path / "base.json"
    base.write_text(json.dumps(doc))
    relabel = copy.deepcopy(doc)
    next(w for w in relabel["waterbodies"] if w.get("splits"))["splits"][0]["label"] = "renamed by a test"
    moved = copy.deepcopy(doc)
    sp = next(w for w in moved["waterbodies"] if w.get("splits"))["splits"][0]
    sp["anchor"] = dict(sp["anchor"], _test_moved=1, offset_m=float(sp["anchor"].get("offset_m") or 0) + 7)
    k = "curated:waters.cuts"
    for name, d in (("relabel", relabel), ("moved", moved)):
        (tmp_path / f"{name}.json").write_text(json.dumps(d))
    assert wb["splits"]
    assert I.digest(k, tmp_path / "relabel.json") == I.digest(k, base)
    assert I.digest(k, tmp_path / "moved.json") != I.digest(k, base)


def test_backfill_writes_only_what_it_proved(tmp_path, gpkg, monkeypatch):
    d = _atlas(tmp_path, {})
    monkeypatch.setattr(I, "default_path", lambda key: gpkg if key == "source:fwa_gpkg" else I.curated_path(key))
    # the cuts are proven by git AND by split_anchors.json equal to the anchors now
    from pipeline.atlas.splits.splits import anchor_digests
    (d / "split_anchors.json").write_text(json.dumps(anchor_digests(json.loads(
        Path(CURATED.waters.cuts).read_text(encoding="utf-8")))))
    unproven_key = "curated:waters.found_names"
    monkeypatch.setattr(I, "git_proof", lambda p, ts: None if Path(p) == I.curated_path(unproven_key) else "git: test")
    os.utime(gpkg, (0, 0))                      # before any build_ts
    got, unproven = I.backfill(d, log=lambda *_: None)
    assert unproven == [f"{I.curated_path(unproven_key).name} ({unproven_key})"]
    assert set(got) == set(I.ATLAS_INPUTS) - {unproven_key}
    meta = I.read_meta(d)
    assert meta["input_hashes"] == got and set(meta["input_hashes_backfilled"]["proof"]) == set(got)
    left = I.behind(d, I.digests())
    assert len(left) == 1 and f"({unproven_key}): the atlas records no digest" in left[0]
    with pytest.raises(SystemExit, match="already records"):
        I.backfill(d, log=lambda *_: None)


def test_backfill_refuses_cuts_whose_anchor_record_differs(tmp_path, gpkg, monkeypatch):
    d = _atlas(tmp_path, {})
    (d / "split_anchors.json").write_text(json.dumps({"some_cut": "0" * 16}))
    monkeypatch.setattr(I, "default_path", lambda key: gpkg if key == "source:fwa_gpkg" else I.curated_path(key))
    monkeypatch.setattr(I, "git_proof", lambda p, ts: "git: test")
    _got, unproven = I.backfill(d, log=lambda *_: None)
    assert unproven == [f"{I.curated_path('curated:waters.cuts').name} (curated:waters.cuts)"]


def test_the_record_moves_only_the_meta_file(tmp_path):
    a, b = tmp_path / "a" / "graph.pkl", tmp_path / "b" / "graph.pkl"
    obj = {"nodes": list(range(50)), "name": "x"}
    write_artifact(obj, str(a))
    write_artifact(obj, str(b), input_hashes={"curated:waters.cuts": "anchors:00"})
    assert a.read_bytes() == b.read_bytes() == pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    assert json.loads((b.parent / "graph.pkl.meta.json").read_text())["input_hashes"] == {
        "curated:waters.cuts": "anchors:00"}


#: modules the atlas build imports from outside pipeline/atlas to read a curated file
INDIRECT = ("pipeline/tools/slough_calls.py", "pipeline/gauges/matches.py", "pipeline/gauges/review.py")


def test_every_curated_key_the_atlas_build_names_is_an_input_or_a_listed_non_input():
    files = [p for p in (REPO / "pipeline/atlas").rglob("*.py")
             if not {"reach", "tests", "__pycache__"} & set(p.relative_to(REPO / "pipeline/atlas").parts)]
    files += [REPO / f for f in INDIRECT]
    named = set()
    for p in files:
        named |= {f"curated:{m}" for m in re.findall(r"CURATED\.(\w+\.\w+)", p.read_text(encoding="utf-8"))}
    named |= {"curated:waters.added_areas"}     # by name, from an areas.json def's `file` (`_resolve_area_file`)
    missing = sorted(named - set(I.ATLAS_INPUTS) - set(I.NOT_ATLAS_INPUTS))
    assert not missing, f"the atlas build reads curated key(s) it does not record: {missing} — add each to " \
                        "atlas.inputs.ATLAS_INPUTS, or to NOT_ATLAS_INPUTS with the reason"
    assert not set(I.ATLAS_INPUTS) & set(I.NOT_ATLAS_INPUTS)


# --------------------------------------------------------------------------- T599 §4.4: no cheap step writes curated

def _tree_digest(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        h.update(str(p.relative_to(root)).encode())
        if p.is_file():
            h.update(p.read_bytes())
    return h.hexdigest()


#: (command, the return codes that are an answer, not a failure). The cheap steps a build / release starts with.
CHEAP_STEPS = (
    (["pipeline.tools.audit.check_curated", "--waters"], {0}),
    (["pipeline.atlas.inputs"], {0, 1}),                       # 1 = the atlas is behind (an answer)
    (["pipeline", "build", "--dry-run"], {0}),
    (["pipeline.regs.review.apply", "--check"], {0, 1}),       # 1 = marks not landed (an answer)
)


@pytest.mark.needs_atlas
@pytest.mark.needs_source
def test_no_cheap_build_step_writes_the_curated_tree(tmp_path):
    root = make_root(tmp_path / "curated")
    for p in [root, *root.rglob("*")]:
        p.chmod(p.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
    before = _tree_digest(root)
    env = {**os.environ, "CURATED_ROOT": str(root)}
    try:
        for cmd, ok in CHEAP_STEPS:
            got = subprocess.run([sys.executable, "-m", *cmd], cwd=REPO, env=env, capture_output=True, text=True)
            said = (got.stdout + got.stderr)[-1500:]
            assert got.returncode in ok, f"{' '.join(cmd)} rc={got.returncode} under a read-only curated tree:\n{said}"
            assert "PermissionError" not in said and "Read-only" not in said, f"{' '.join(cmd)} tried to write:\n{said}"
        assert _tree_digest(root) == before
    finally:
        for p in [root, *root.rglob("*")]:
            p.chmod(p.stat().st_mode | stat.S_IWUSR)
