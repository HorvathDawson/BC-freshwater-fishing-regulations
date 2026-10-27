"""T185: review marks -> proposed edits -> the owner's decision (a mark) -> one commit each.

  build       a mark with an extent fix / tributary call is ONE proposal (one patch); a new split is
              ONE proposal per distinct cut across every mark (T210), and a mark naming it depends
              on it; a lake-split-later call is listed, with no edit
  read only   building and writing the proposals leaves every curated byte as it was
  decision    a `proposal` mark (P y accept / P n reject) on the proposal's digest; never a ticket;
              stale once the proposal changes
  apply       the owner's CLI lands each ACCEPTED, still-holding proposal as its own commit, new
              splits first, and refuses a stale decision (a temp git repo, never this one)
  the app     GET /api/proposals reads; the decision goes through the one write, POST /api/marks

Reads the checked-in catalogue / DFO / splits.json (copied into temp dirs) and no generated data.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.review import apply_proposals as A
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P
from pipeline.regs.review.tickets import ticketed

SHA = "0" * 64
HATZIC = "r2:hatzic_lake_and_slough@2-8"
CAMPBELL_A = "1:campbell-river:downstream-of-the-confluence-with"
CAMPBELL_B = "1:campbell-river:downstream-of-the-confluence-with~2"
COWICHAN = "1:cowichan-river:from-66-mile-trestle-downstream-to"
DRAGON = "r5:dragon_lake@5-2"
QUINSAM = {"anchor": "confluence", "name": "Quinsam River", "item_id": "gnis:27883",
           "on_item": "gnis:39532", "coord": [-125.294097, 50.037899], "blk": "354154635",
           "measure": 4199.5}


def _entry_prints(rules=()):
    return {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]
            + [{"key": f"rule:{r}", "meaning": "m", "binding": "b"} for r in rules]}


def _mark(**kw) -> M.Mark:
    base = {"scope": "entry", "verdict": "unsure", "answers": {"T": "y", "R": "y", "P": "?", "A": "y"},
            "reviewer": "dawson", "date": "2026-10-26", "pdf_sha256": SHA,
            "prints": _entry_prints()}
    if kw.get("scope") == "dfo":
        base["answers"] = {"T": "y", "R": "y", "P": "?"}
    return M.Mark.model_validate({**base, **kw})


def owner_marks() -> list[M.Mark]:
    """The owner's marks as of 2026-10-27, in shape: a tributary call, a lake split later, two DFO
    locations proposing the same Quinsam confluence (n2, n3), one proposing a sign by map point."""
    return [
        _mark(target=HATZIC, verdict="verified", answers={"T": "y", "R": "y", "P": "y", "A": "y"},
              prints=_entry_prints(["hatzic_lake_and_slough.r2", "hatzic_lake_and_slough.r3"]),
              tributaries={"hatzic_lake_and_slough.r2": "include",
                           "hatzic_lake_and_slough.r3": "include"}),
        _mark(target=DRAGON, verdict="verified", answers={"T": "y", "R": "y", "P": "y", "A": "y"},
              lake_split_later=True),
        _mark(scope="dfo", target=CAMPBELL_B, extent_fix=[{
            "scope": "entry", "op": "between", "items": ["gnis:39532"],
            "points": [{"kind": "new", "split_id": "n2"},
                       {"kind": "split", "split_id": "campbell_river__maple_street_boundary_sign"}]}],
              new_splits=[{"ref": "n2", **QUINSAM}]),
        _mark(scope="dfo", target=CAMPBELL_A, extent_fix=[{
            "scope": "entry", "op": "downstream_of", "items": ["gnis:39532"],
            "points": [{"kind": "new", "split_id": "n3"}]}],
              new_splits=[{"ref": "n3", **QUINSAM}]),
        _mark(scope="dfo", target=COWICHAN, extent_fix=[{
            "scope": "entry", "op": "between", "items": ["gnis:12227"],
            "points": [{"kind": "split", "split_id": "cowichan_river__cnr_trestle"},
                       {"kind": "new", "split_id": "n1"}]}],
              new_splits=[{"ref": "n1", "anchor": "sign", "name": "white triangle sign",
                           "on_item": "gnis:12227", "coord": [-123.835989, 48.757157]}]),
    ]


@pytest.fixture()
def src(tmp_path) -> P.Sources:
    """Temp copies of the files a proposal edits, and a build whose registry names the Quinsam."""
    cat, dfo = tmp_path / "data/curated/catalogue", tmp_path / "data/curated/dfo_salmon"
    cat.mkdir(parents=True)
    dfo.mkdir(parents=True)
    for name in ("region-2.json", "region-5.json"):
        shutil.copy2(CURATED.regulations.entries.catalogue / name, cat / name)
    shutil.copy2(Path(CURATED.regulations.entries.dfo_salmon) / "region-1.json", dfo / "region-1.json")
    shutil.copy2(CURATED.waters.splits, tmp_path / "data/curated/splits.json")
    build = tmp_path / "build"
    build.mkdir()
    (build / "registry.json").write_text(json.dumps({"items": [
        {"id": "gnis:27883", "name": "Quinsam River", "kind": "stream",
         "ref_ids": ["blk:354155091", "gnis:27883", "wsc:920-627155-045026"]}]}))
    return P.Sources(entries_dir=cat, dfo_dir=dfo, splits_path=tmp_path / "data/curated/splits.json",
                     build=build)


def _by_id(props):
    return {p["id"]: p for p in props}


# --------------------------------------------------------------------------- build

def test_one_proposal_per_mark_and_one_per_distinct_cut(src):
    props = P.build(owner_marks(), src)
    kinds = [(p["kind"], p["target"]) for p in props]
    assert kinds == [("new_split", "gnis:39532"), ("new_split", "gnis:12227"),
                     ("extent", HATZIC), ("extent", CAMPBELL_B), ("extent", CAMPBELL_A),
                     ("extent", COWICHAN), ("lake_split_later", DRAGON)]
    by = _by_id(props)
    quinsam = by["split-n2"]                       # n2 and n3 are ONE cut (T210)
    assert {m["target"] for m in quinsam["marks"]} == {CAMPBELL_A, CAMPBELL_B}
    (ed,) = quinsam["edits"]
    assert ed["split"]["anchor"] == {"type": "confluence", "tributary_wsc": "920-627155-045026",
                                     "_name": "Quinsam River"}
    assert ed["waterbody"]["applies_to"] == {"gnis_id": "39532"}
    sid = quinsam["split_id"]
    for t in (CAMPBELL_A, CAMPBELL_B):             # both DFO marks name it, and wait on it
        p = next(q for q in props if q["target"] == t)
        assert p["depends_on"] == ["split-n2"] and sid in p["edits"][0]["after"][0]["splits"]
    sign = by["split-n1"]["edits"][0]["split"]
    assert sign["anchor"] == {"type": "point", "coord": [-123.835989, 48.757157], "is_lonlat": True}
    hz = next(p for p in props if p["target"] == HATZIC)
    assert [(e["scope"], e["field"], e["before"], e["after"]) for e in hz["edits"]] == [
        ("hatzic_lake_and_slough.r2", "includes_tributaries", False, True),
        ("hatzic_lake_and_slough.r3", "includes_tributaries", False, True)]
    lake = props[-1]
    assert lake["edits"] == [] and not lake["applicable"]
    assert all(p["applicable"] for p in props[:-1])


def test_the_patch_changes_only_what_the_mark_says(src):
    props = P.build(owner_marks(), src)
    hz = next(p for p in props if p["target"] == HATZIC)
    (path, text), = P.materialize([hz], src).items()
    diff = P.unified("region-2.json", src.text(path), text)
    changed = [ln for ln in diff.splitlines() if ln[:1] in "+-" and ln[:3] not in ("+++", "---")]
    assert changed == ['-     "includes_tributaries": false,', '+     "includes_tributaries": true,'] * 2
    e = next(x for x in json.loads(text)["entries"] if x["entry_id"] == HATZIC)
    assert [r.get("includes_tributaries") for r in e["rules"]] == [None, True, True]
    # splits.json keeps its hand-kept one-line anchors: only the new split's lines are added
    split = _by_id(props)["split-n2"]
    (sp, stext), = P.materialize([split], src).items()
    d = P.unified("splits.json", src.text(sp), stext).splitlines()
    assert not [ln for ln in d if ln.startswith("-") and not ln.startswith("---")]


def test_a_mark_whose_edit_is_already_in_the_file_proposes_nothing(src, tmp_path):
    props = P.build(owner_marks(), src)
    hz = next(p for p in props if p["target"] == HATZIC)
    for path, text in P.materialize([hz], src).items():
        path.write_text(text)
    again = P.build(owner_marks(), P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, src.build))
    assert HATZIC not in {p["target"] for p in again if p["kind"] == "extent"}


def test_building_and_writing_never_touches_curated_data(tmp_path):
    root = Path(CURATED.waters.splits).parents[1]
    before = {p: p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}
    src = P.Sources.live()
    props = P.build(owner_marks(), src)
    P.write(props, tmp_path / "out", src, marks_path=tmp_path / "marks.jsonl")
    assert {p: p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()} == before
    assert (tmp_path / "out" / P.INDEX).exists()
    assert sorted(x.name for x in (tmp_path / "out").glob("*.patch"))
    with pytest.raises(ValueError, match="curated"):
        P.write_side(props[0], props, src, root / "side")


# --------------------------------------------------------------------------- decisions

def _decision(pid: str, digest: str, accept: bool = True) -> M.Mark:
    return M.Mark.model_validate({
        "scope": "proposal", "target": pid, "verdict": "verified" if accept else "wrong",
        "answers": {"P": "y" if accept else "n"}, "reviewer": "dawson", "date": "2026-10-27",
        "pdf_sha256": SHA, "prints": {"text": digest, "records": [
            {"key": f"proposal:{pid}", "meaning": digest, "binding": digest}]}})


def test_a_decision_is_a_mark_and_never_a_ticket():
    acc, rej = _decision("split-n2", "d1"), _decision("split-n2", "d1", accept=False)
    assert not ticketed(acc) and not ticketed(rej)
    assert M.decisions([acc])["split-n2"]["decision"] == "accept"
    assert M.decisions([acc, rej])["split-n2"]["decision"] == "reject"        # the latest holds
    assert M.line(acc) == M.line(M.Mark.model_validate_json(M.line(acc)))   # round trip
    bad = acc.model_dump() | {"tributaries": {"entry": "include"}}
    with pytest.raises(ValueError, match="proposes nothing"):
        M.Mark.model_validate(bad)
    with pytest.raises(ValueError):
        M.Mark.model_validate(acc.model_dump() | {"verdict": "unsure", "answers": {"P": "?"}})


def test_a_stale_or_waiting_decision_is_not_applied(src):
    props = P.build(owner_marks(), src)
    by = _by_id(props)
    camp = next(p for p in props if p["target"] == CAMPBELL_A)
    decided = {camp["id"]: M.decisions([_decision(camp["id"], camp["digest"])])[camp["id"]],
               "split-n1": M.decisions([_decision("split-n1", "an-older-digest")])["split-n1"]}
    nxt, blocked = A.plan(props, decided, set())
    assert nxt is None
    assert dict(blocked) == {"split-n1": dict(blocked)["split-n1"],
                             camp["id"]: "waits on new split(s) not applied: split-n2"}
    assert "STALE" in dict(blocked)["split-n1"]
    decided["split-n2"] = M.decisions([_decision("split-n2", by["split-n2"]["digest"])])["split-n2"]
    assert A.plan(props, decided, set())[0]["id"] == "split-n2"               # new splits first


# --------------------------------------------------------------------------- apply

def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True,
                          check=True).stdout


def test_apply_lands_each_accepted_proposal_as_its_own_commit(src, tmp_path, monkeypatch):
    repo = tmp_path
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    _git(repo, "add", "data")
    _git(repo, "commit", "-qm", "base")
    props = P.build(owner_marks(), src)
    camp = next(p for p in props if p["target"] == CAMPBELL_B)
    hz = next(p for p in props if p["target"] == HATZIC)
    cow = next(p for p in props if p["target"] == COWICHAN)
    store = tmp_path / "marks.jsonl"
    store.write_text("".join(M.line(m) for m in owner_marks() + [
        _decision(camp["id"], camp["digest"]), _decision(hz["id"], hz["digest"]),
        _decision("split-n2", _by_id(props)["split-n2"]["digest"]),
        _decision(cow["id"], cow["digest"], accept=False)]))
    monkeypatch.setattr(P, "REPO", repo)
    monkeypatch.setattr(P.Sources, "live", classmethod(
        lambda cls, build=None: P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, src.build)))
    assert A.main(["--marks", str(store)]) == 0
    log = _git(repo, "log", "--format=%s").splitlines()
    assert log == [f"T185 apply {camp['id']}: {camp['title']}"[:200],
                   f"T185 apply {hz['id']}: {hz['title']}"[:200],
                   f"T185 apply split-n2: {_by_id(props)['split-n2']['title']}"[:200], "base"]
    assert _git(repo, "show", "--name-only", "--format=", "HEAD~1").split() == [
        "data/curated/catalogue/region-2.json"]
    splits = json.loads(src.splits_path.read_text())
    assert any(sp["id"] == camp["edits"][0]["after"][0]["splits"][0]
               for wb in splits["waterbodies"] for sp in wb["splits"])
    assert _git(repo, "status", "--porcelain", "--", "data") == ""
    # nothing more to land: the rejected one stays, the applied ones are gone from the proposals
    assert A.main(["--marks", str(store)]) == 0
    assert len(_git(repo, "log", "--format=%s").splitlines()) == 4


# --------------------------------------------------------------------------- the app

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "dfo", "review",
         "evidence")


def test_the_app_reads_proposals_and_records_a_decision_as_a_mark(src, tmp_path, monkeypatch):
    props = P.build(owner_marks(), src)
    out = tmp_path / "proposals"
    P.write(props, out, src, marks_path=tmp_path / "marks.jsonl")
    store = tmp_path / "marks.jsonl"
    store.write_text("")
    monkeypatch.setenv("CURATION_MARKS", str(store))
    monkeypatch.setenv("CURATION_PROPOSALS_DIR", str(out))
    monkeypatch.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    try:
        import app as app_mod
        from fastapi.testclient import TestClient
        c = TestClient(app_mod.app)
        got = c.get("/api/proposals").json()
        assert [p["id"] for p in got["proposals"]] == [p["id"] for p in props]
        assert all(p["decision"] is None for p in got["proposals"])
        r = c.post("/api/marks", json={"scope": "proposal", "target": "split-n2",
                                       "answers": {"P": "y"}, "reviewer": "dawson"})
        assert r.status_code == 200, r.text
        (m,) = M.load(store)
        assert (m.scope, m.verdict, m.prints.text) == ("proposal", "verified",
                                                        _by_id(props)["split-n2"]["digest"])
        d = next(p for p in c.get("/api/proposals").json()["proposals"] if p["id"] == "split-n2")
        assert d["decision"]["decision"] == "accept" and d["decision"]["holds"]
        lake = props[-1]["id"]                    # nothing to apply: refused, nothing written
        r = c.post("/api/marks", json={"scope": "proposal", "target": lake, "answers": {"P": "y"},
                                       "reviewer": "dawson"})
        assert r.status_code == 422 and len(M.load(store)) == 1
        assert c.post("/api/marks", json={"scope": "proposal", "target": "nope",
                                          "answers": {"P": "n"}, "reviewer": "d"}).status_code == 404
    finally:
        for mod in _MODS:
            sys.modules.pop(mod, None)
