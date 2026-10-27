"""T185: review marks -> proposed edits -> applied by Claude, one commit each.

The owner's MARKS ARE THE DECISIONS (2026-10-27): no accept step, no `proposal` mark.

  build       a mark with an extent fix / tributary call is ONE proposal (one patch); a new split is
              ONE proposal per distinct cut across every mark (T210), and a mark naming it depends
              on it; a lake-split-later call is listed, with no edit
  read only   building and writing the proposals leaves every curated byte as it was
  confidence  every proposed split carries one: set, an owner override, else the anchor default
  retired     a `proposal` line in the store is skipped; the model and the app refuse a new one
  apply       Claude's CLI lands every applicable proposal as its own commit after the binding
              check, new splits first, a cut change only once its side rebuild exists (a temp git
              repo, never this one)
  the app     GET /api/proposals reads (a read-only "waiting to apply" list); nothing to decide

Reads the checked-in catalogue / DFO / splits.json (copied into temp dirs) and no generated data.
"""
from __future__ import annotations

import json
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


#: the commit the owner's 2026-10-27 marks were made against: every one of them was APPLIED after it
#: (8459fa16..897b96f4, Hatzic held), so the live files no longer yield these proposals
MARKED_AT = "2bd29a26"


def _as_marked(path: Path, dst: Path) -> None:
    """`path` as it stood at MARKED_AT (the files the proposals were built from)."""
    rel = Path(path).resolve().relative_to(P.REPO.resolve()).as_posix()
    got = subprocess.run(["git", "show", f"{MARKED_AT}:{rel}"], cwd=P.REPO, capture_output=True,
                         check=True)
    dst.write_bytes(got.stdout)


@pytest.fixture()
def src(tmp_path) -> P.Sources:
    """Temp copies of the files a proposal edits, and a build whose registry names the Quinsam."""
    cat, dfo = tmp_path / "data/curated/catalogue", tmp_path / "data/curated/dfo_salmon"
    cat.mkdir(parents=True)
    dfo.mkdir(parents=True)
    for name in ("region-2.json", "region-5.json"):
        _as_marked(CURATED.regulations.entries.catalogue / name, cat / name)
    _as_marked(Path(CURATED.regulations.entries.dfo_salmon) / "region-1.json", dfo / "region-1.json")
    _as_marked(CURATED.waters.splits, tmp_path / "data/curated/splits.json")
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
    # owner 2026-10-27: a proposed split carries its confidence — a registry confluence is exact,
    # a hand-placed sign is medium (splits.json spells high by leaving it out)
    assert (quinsam["confidence"], by["split-n1"]["confidence"]) == ("high", "medium")
    assert "confidence" not in ed["split"] and sign["confidence"] == "medium"
    assert hz["confidence"] is None


def test_confidence_set_in_the_mark_or_by_an_owner_override(src):
    marks = owner_marks()
    marks[4] = _mark(**{**marks[4].model_dump(), "new_splits": [
        {**marks[4].new_splits[0].model_dump(), "confidence": "low"}]})
    assert _by_id(P.build(marks, src))["split-n1"]["edits"][0]["split"]["confidence"] == "low"
    # an override (the marks store is never edited) wins over the mark
    src.overrides = (M.ConfidenceOverride.model_validate({
        "match": {"ref": "o", "anchor": "coord", "on_item": "gnis:12227",
                  "coord": [-123.835990, 48.757158]},
        "confidence": "high", "by": "owner", "date": "2026-10-27"}),)
    got = _by_id(P.build(marks, src))["split-n1"]
    assert got["confidence"] == "high" and "confidence" not in got["edits"][0]["split"]
    assert {p["id"]: p["confidence"] for p in M.proposals(marks, src.overrides)} == {
        "n2": "high", "n1": "high"}


def test_sandy_pool_is_medium_by_the_owners_override():
    """decisions.md 2026-10-27: Sandy Pool = medium — recorded in the override file, since the
    marks store is never edited."""
    got = M.load_overrides(CURATED.regulations.review.proposal_overrides)
    sandy = [o for o in got if "Sandy Pool" in (o.match.name or "")]
    assert [o.confidence for o in sandy] == ["medium"]
    assert M.confidence_of([owner_marks()[4].new_splits[0]], got) == "medium"


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


# --------------------------------------------------------------------------- the retired accept step

RETIRED = {"scope": "proposal", "target": "split-n2", "verdict": "verified", "answers": {"P": "y"},
           "reviewer": "dawson", "date": "2026-10-27", "pdf_sha256": SHA,
           "prints": {"text": "d", "records": [{"key": "proposal:split-n2", "meaning": "d",
                                                "binding": "d"}]}}


def test_a_proposal_mark_is_retired_skipped_in_the_store_and_refused_new(tmp_path):
    store = tmp_path / "marks.jsonl"
    hz = owner_marks()[0]
    store.write_text(M.line(hz) + json.dumps(RETIRED) + "\n")
    assert M.load(store) == [hz]                       # the old decision line is history only
    with pytest.raises(ValueError):
        M.Mark.model_validate(RETIRED)
    assert not hasattr(M, "decisions")
    assert ticketed(owner_marks()[2])                  # a DFO mark with a new split: a ticket


def test_a_waiting_or_side_less_proposal_is_not_applied(src, monkeypatch):
    props = P.build(owner_marks(), src)
    monkeypatch.setattr(P, "side_ready", lambda p: False)
    nxt, blocked = A.plan(props, set())
    assert nxt["target"] == HATZIC                     # no cut changes: the binding check is enough
    assert A.plan(props, {nxt["id"]})[0] is None
    why = dict(A.plan(props, {nxt["id"]})[1])
    assert why["split-n2"].startswith("a cut changes: make the side rebuild")
    camp = next(p for p in props if p["target"] == CAMPBELL_A)
    assert why[camp["id"]] == "waits on new split(s) not applied: split-n2"
    assert P.side_dir(props[0]) in why["split-n2"]
    monkeypatch.setattr(P, "side_ready", lambda p: True)
    assert A.plan(props, set())[0]["id"] == "split-n2"                         # new splits first
    assert A.plan(props, set(), side_checked=["split-n2"])[0]["id"] == "split-n2"


# --------------------------------------------------------------------------- apply

def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True,
                          check=True).stdout


def test_apply_lands_each_proposal_from_the_marks_as_its_own_commit(src, tmp_path, monkeypatch):
    repo = tmp_path
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    _git(repo, "add", "data")
    _git(repo, "commit", "-qm", "base")
    props = P.build(owner_marks(), src)
    by = _by_id(props)
    camp_b = next(p for p in props if p["target"] == CAMPBELL_B)
    camp_a = next(p for p in props if p["target"] == CAMPBELL_A)
    hz = next(p for p in props if p["target"] == HATZIC)
    cow = next(p for p in props if p["target"] == COWICHAN)
    store = tmp_path / "marks.jsonl"
    store.write_text("".join(M.line(m) for m in owner_marks()))
    checked = []

    def fake_check(ps, _src):                         # the in-memory binding (needs an atlas)
        for p in ps:
            checked.append(p["id"])
            p["check"] = {"cheap": True, "why": None, "rules": {"r1": {
                "n_before": 4, "n_after": 92, "gained": 88, "lost": 0, "km_before": 8,
                "km_after": 124, "outcome_before": "open", "outcome_after": "open"}}}
    side = {"ready": False}
    monkeypatch.setattr(P, "REPO", repo)
    monkeypatch.setattr(P, "check", fake_check)
    monkeypatch.setattr(P, "side_ready", lambda p: side["ready"])
    monkeypatch.setattr(P.Sources, "live", classmethod(
        lambda cls, build=None: P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, src.build)))
    # no side rebuild yet: only the proposal that changes no cut lands, after its binding check
    assert A.main(["--marks", str(store)]) == 0
    assert _git(repo, "log", "--format=%s").splitlines() == [
        f"T185 apply {hz['id']}: {hz['title']}"[:200], "base"]
    assert checked == [hz["id"]]
    body = _git(repo, "log", "-1", "--format=%b")
    assert "the marks are the decision" in body and "r1: 4 -> 92 sections" in body
    # the side rebuilds made: the new splits land first, then the extents that name them
    side["ready"] = True
    assert A.main(["--marks", str(store)]) == 0
    log = _git(repo, "log", "--format=%s").splitlines()
    assert log == [f"T185 apply {q['id']}: {q['title']}"[:200]
                   for q in (cow, camp_a, camp_b, by["split-n1"], by["split-n2"], hz)] + ["base"]
    assert checked == [hz["id"]]                       # cut changes are bound on the side atlas
    assert _git(repo, "show", "--name-only", "--format=", "HEAD~4").split() == [
        "data/curated/splits.json"]
    splits = json.loads(src.splits_path.read_text())
    got = {sp["id"]: sp for wb in splits["waterbodies"] for sp in wb["splits"]}
    assert camp_b["edits"][0]["after"][0]["splits"][0] in got
    assert got[by["split-n1"]["split_id"]]["confidence"] == "medium"
    assert _git(repo, "status", "--porcelain", "--", "data") == ""
    # nothing more to land: every proposal is in the files now
    assert A.main(["--marks", str(store)]) == 0
    assert len(_git(repo, "log", "--format=%s").splitlines()) == 7


def test_a_dry_run_lands_nothing(src, tmp_path, monkeypatch, capsys):
    store = tmp_path / "marks.jsonl"
    store.write_text("".join(M.line(m) for m in owner_marks()))
    monkeypatch.setattr(P, "check", lambda ps, _s: [p.update(check={"cheap": True, "rules": {}})
                                                    for p in ps])
    monkeypatch.setattr(P, "side_ready", lambda p: False)
    monkeypatch.setattr(P.Sources, "live", classmethod(
        lambda cls, build=None: P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, src.build)))
    before = src.text(src.splits_path)
    assert A.main(["--marks", str(store), "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "== would apply" in out and "a cut changes" in out
    assert src.splits_path.read_text() == before


# --------------------------------------------------------------------------- the app

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "dfo", "review",
         "evidence")


def test_the_app_lists_proposals_read_only_and_refuses_a_decision(src, tmp_path, monkeypatch):
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
        assert all("decision" not in p for p in got["proposals"])
        assert {p["id"]: p["confidence"] for p in got["proposals"]
                if p["kind"] == "new_split"} == {"split-n2": "high", "split-n1": "medium"}
        r = c.post("/api/marks", json={"scope": "proposal", "target": "split-n2",
                                       "answers": {"P": "y"}, "reviewer": "dawson"})
        assert r.status_code == 422 and "the marks behind it are the decision" in r.text
        assert store.read_text() == ""
    finally:
        for mod in _MODS:
            sys.modules.pop(mod, None)
