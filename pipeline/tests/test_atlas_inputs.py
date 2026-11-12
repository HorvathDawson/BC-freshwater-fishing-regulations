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


#: the two source stamps (no FWA file in a test: the stand-in file is stamped for both)
SOURCES = ("source:fwa_gpkg", "source:fwa_coast")


def _src(gpkg) -> dict:
    return {k: gpkg for k in SOURCES}


def _atlas(tmp_path: Path, hashes: dict) -> Path:
    d = tmp_path / "atlas"
    d.mkdir(exist_ok=True)
    write_artifact({"g": 1}, str(d / "graph.pkl"), input_hashes=hashes)
    return d


def test_every_input_digests_and_a_current_atlas_is_not_behind(tmp_path, gpkg):
    now = I.digests(_src(gpkg))
    assert list(now) == list(I.ATLAS_INPUTS)
    assert now["curated:waters.cuts"].startswith("anchors:")
    assert all(now[k].startswith("stamp:") for k in SOURCES)
    assert all(v.startswith("sha256:") for k, v in now.items() if k != "curated:waters.cuts" and k not in SOURCES)
    assert I.behind(_atlas(tmp_path, now), now) == []


@pytest.mark.parametrize("key", [k for k in I.ATLAS_INPUTS if k.startswith("curated:")])
def test_a_changed_input_is_named(tmp_path, gpkg, key):
    now = I.digests(_src(gpkg))
    old = dict(now, **{key: now[key][:-4] + "0000"})
    got = I.behind(_atlas(tmp_path, old), now)
    assert len(got) == 1
    assert got[0].startswith(f"{I.curated_path(key).name} ({key}): changed since the atlas was built — rebuild: ")
    assert I.REBUILD in got[0]


def test_an_empty_record_is_refused_with_the_backfill_command(tmp_path, gpkg):
    got = I.behind(_atlas(tmp_path, {}), I.digests(_src(gpkg)))
    assert len(got) == 1 and "records no inputs" in got[0] and "--backfill" in got[0]


def test_a_skipped_or_unrecorded_input_is_behind(tmp_path, gpkg):
    now = I.digests(_src(gpkg))
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
    monkeypatch.setattr(I, "default_path", lambda key: gpkg if key in SOURCES else I.curated_path(key))
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
    # a second run proves only what is still missing, and keeps every recorded digest as it was
    monkeypatch.setattr(I, "git_proof", lambda p, ts: "git: test")
    again, unproven = I.backfill(d, log=lambda *_: None)
    assert set(again) == {unproven_key} and unproven == []
    assert I.read_meta(d)["input_hashes"] == {k: {**got, **again}[k] for k in I.ATLAS_INPUTS}
    with pytest.raises(SystemExit, match="already records"):
        I.backfill(d, log=lambda *_: None)


def test_backfill_adds_only_the_inputs_a_record_lacks(tmp_path, gpkg, monkeypatch):
    """T678: the first coast-cut atlas records the older inputs as built; its coast inputs are added by proof and
    a recorded digest is never rewritten, even one the tree has moved past (the release must still see it behind)."""
    monkeypatch.setattr(I, "default_path", lambda key: gpkg if key in SOURCES else I.curated_path(key))
    monkeypatch.setattr(I, "git_proof", lambda p, ts: "git: test")
    os.utime(gpkg, (0, 0))
    now = I.digests()
    added = ("curated:waters.tidal_split_authority", "curated:waters.shore_held", "source:fwa_coast")
    rec = {k: v for k, v in now.items() if k not in added}
    rec["curated:waters.tidal"] = "sha256:" + "0" * 64          # an input the tree moved past
    d = _atlas(tmp_path, rec)
    got, unproven = I.backfill(d, log=lambda *_: None)
    assert set(got) == set(added) and unproven == []
    meta = I.read_meta(d)
    assert meta["input_hashes"]["curated:waters.tidal"] == "sha256:" + "0" * 64
    assert list(meta["input_hashes"]) == list(I.ATLAS_INPUTS)
    assert [b.split(" (")[1].split(")")[0] for b in I.behind(d, now)] == ["curated:waters.tidal"]


def test_backfill_refuses_cuts_whose_anchor_record_differs(tmp_path, gpkg, monkeypatch):
    d = _atlas(tmp_path, {})
    (d / "split_anchors.json").write_text(json.dumps({"some_cut": "0" * 16}))
    monkeypatch.setattr(I, "default_path", lambda key: gpkg if key in SOURCES else I.curated_path(key))
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


#: modules under pipeline/atlas that are not the build: promote reads the corpus to check the rules' boundary ids
NOT_THE_BUILD = ("promote.py",)
#: modules the atlas build imports from outside pipeline/atlas to read a curated file
INDIRECT = ("pipeline/tools/slough_calls.py", "pipeline/gauges/matches.py", "pipeline/gauges/review.py")


def test_every_curated_key_the_atlas_build_names_is_an_input_or_a_listed_non_input():
    files = [p for p in (REPO / "pipeline/atlas").rglob("*.py")
             if not {"reach", "tests", "__pycache__"} & set(p.relative_to(REPO / "pipeline/atlas").parts)
             and p.name not in NOT_THE_BUILD]
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


# --------------------------------------------------------------- T547 §3: a rule's boundary id must keep its place

def _reg(d: Path, bounds: dict[str, str]) -> Path:
    from pipeline.common.section_handles import FILENAME
    d.mkdir(parents=True)
    (d / FILENAME).write_text(d.name)
    (d / "registry.json").write_text(json.dumps({"items": [
        {"id": "gnis:1", "boundaries": [{"id": i, "ref": r} for i, r in bounds.items()]}]}))
    I.write_complete(d, {})
    return d


def test_promote_refuses_a_rule_boundary_that_moved_to_another_lake(tmp_path, monkeypatch):
    from pipeline.atlas import promote as PR
    named = "nanaimo_river__nanaimo_lakes_2"
    target = _reg(tmp_path / "full", {"nanaimo_river__nanaimo_lakes": "lake:329101304", named: "lake:329101307",
                                      "x__gone": "lake:1", "y__unnamed": "lake:5"})
    cand = _reg(tmp_path / "full_next", {"nanaimo_river__nanaimo_lakes": "lake:329101307", named: "lake:329101304",
                                         "y__unnamed": "lake:6", "z__new": "lake:7"})   # no net loss
    monkeypatch.setattr(PR, "curated_boundary_ids", lambda: {named, "x__gone"})
    kw = dict(target=target, tiles=tmp_path / "tiles", bundle=tmp_path / "b.sqlite", log=lambda *_: None)
    assert PR.promote(cand, dry_run=True, **kw)["moved"] == [f"{named}: lake:329101307 -> lake:329101304"]
    with pytest.raises(SystemExit, match="1 boundary id"):
        PR.promote(cand, **kw)
    assert (target / "registry.json").exists() and cand.exists()          # nothing renamed
    got = PR.promote(cand, accept_moved=True, **kw)
    assert got["moved"] and json.loads((target / "registry.json").read_text())["items"][0]["boundaries"][1]["ref"] \
        == "lake:329101304"


def test_the_curated_rules_name_the_suffixed_lake_boundary():
    from pipeline.atlas import promote as PR
    assert "nanaimo_river__nanaimo_lakes_2" in PR.curated_boundary_ids()


# ------------------------------------------------------- T547 §4: only a finished build is promoted, and safely

def test_promote_refuses_an_unfinished_candidate(tmp_path):
    from pipeline.atlas import promote as PR
    target = _reg(tmp_path / "full", {"a__lake": "lake:1"})
    cand = _reg(tmp_path / "full_next", {"a__lake": "lake:1"})
    kw = dict(target=target, tiles=tmp_path / "tiles", bundle=tmp_path / "b.sqlite", log=lambda *_: None)
    I.clear_complete(cand)                                  # a build that started and died
    with pytest.raises(SystemExit, match="not a finished atlas: no COMPLETE.json"):
        PR.promote(cand, **kw)
    I.write_complete(cand, {})
    (cand / "registry.json").rename(cand / "registry.json.gone")   # listed, then missing: refused by the handles check
    with pytest.raises(SystemExit, match="not a finished atlas"):
        PR.promote(cand, **kw)
    (cand / "registry.json.gone").rename(cand / "registry.json")
    (cand / "extra.json").write_text("{}")
    I.write_complete(cand, {})
    (cand / "extra.json").unlink()
    with pytest.raises(SystemExit, match="extra.json is listed in COMPLETE.json and missing"):
        PR.promote(cand, **kw)
    I.clear_complete(cand)
    assert PR.promote(cand, no_marker="a sidecars --out farm", **kw)["to"] == str(target.resolve())


def test_a_net_boundary_loss_is_refused_unless_accepted(tmp_path, monkeypatch):
    from pipeline.atlas import promote as PR
    assert PR.boundary_loss("items  ...\nboundaries  before 27,197   after 27,190   delta -7\n") == 7
    assert PR.boundary_loss("boundaries  before 10   after 12   delta +2") == 0
    target = _reg(tmp_path / "full", {"a__lake": "lake:1"})
    cand = _reg(tmp_path / "full_next", {"a__lake": "lake:1"})

    class Done:
        stdout, stderr = "boundaries  before 2   after 1   delta -1\n", ""
    monkeypatch.setattr(PR.subprocess, "run", lambda *a, **k: Done)
    kw = dict(target=target, tiles=tmp_path / "tiles", bundle=tmp_path / "b.sqlite", log=lambda *_: None)
    with pytest.raises(SystemExit, match="1 fewer boundaries"):
        PR.promote(cand, **kw)
    assert cand.exists()
    PR.promote(cand, accept_loss=True, **kw)
    assert not cand.exists()


def test_a_failed_second_rename_restores_the_served_atlas(tmp_path, monkeypatch):
    from pipeline.atlas import promote as PR
    target = _reg(tmp_path / "full", {"a__lake": "lake:1"})
    cand = _reg(tmp_path / "full_next", {"a__lake": "lake:1"})
    real = Path.rename

    def rename(self, dst):
        if self.name == "full_next":
            raise OSError("disk said no")
        return real(self, dst)
    monkeypatch.setattr(Path, "rename", rename)
    with pytest.raises(OSError, match="disk said no"):
        PR.promote(cand, target=target, tiles=tmp_path / "tiles", bundle=tmp_path / "b.sqlite", log=lambda *_: None)
    assert (target / "registry.json").exists() and not (tmp_path / "full.prev").exists() and cand.exists()
