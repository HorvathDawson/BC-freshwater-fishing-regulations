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
  the app     refuses a new proposal mark (the review UI for the list was removed, T290)

Reads the checked-in catalogue / DFO / splits.json (copied into temp dirs) and no generated data.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.tests.conftest import curated_root
from pipeline.regs.review import apply_proposals as A
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P


def _decided_args(store: Path) -> list[str]:
    """T408: apply_proposals computes the marks' decisions in memory from the store it is given."""
    return ["--marks", str(store)]
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
#: (T324) the curated paths as they were named at MARKED_AT
PRE_T324 = (("data/curated/regulations/synopsis/", "data/curated/regulations/entries/catalogue/"),
            ("data/curated/dfo_salmon/places/", "data/curated/regulations/entries/dfo_salmon/"),
            ("data/curated/waters/cuts.json", "data/curated/waters/splits.json"))


def _as_marked(path: Path, dst: Path) -> None:
    """`path` as it stood at MARKED_AT (the files the proposals were built from)."""
    rel = Path(path).resolve().relative_to(P.REPO.resolve()).as_posix()
    for now, then in PRE_T324:            # MARKED_AT predates the T324 naming pass: ask for the file by its old name
        if rel.startswith(now):
            rel = then + rel[len(now):]
            break
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
        _as_marked(CURATED.regulations.synopsis / name, cat / name)
    _as_marked(Path(CURATED.dfo_salmon.places) / "region-1.json", dfo / "region-1.json")
    _as_marked(CURATED.waters.cuts, tmp_path / "data/curated/splits.json")
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
    got = M.load_overrides(CURATED.review.proposal_overrides)
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
    root = Path(CURATED.waters.cuts).parents[1]
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
    assert A.main(_decided_args(store)) == 0
    assert _git(repo, "log", "--format=%s").splitlines() == [
        f"T185 apply {hz['id']}: {hz['title']}"[:200], "base"]
    assert checked == [hz["id"]]
    body = _git(repo, "log", "-1", "--format=%b")
    assert "the marks are the decision" in body and "r1: 4 -> 92 sections" in body
    # the side rebuilds made: the new splits land first, then the extents that name them
    side["ready"] = True
    assert A.main(_decided_args(store)) == 0
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
    assert A.main(_decided_args(store)) == 0
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
    assert A.main(_decided_args(store) + ["--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "== would apply" in out and "a cut changes" in out
    assert src.splits_path.read_text() == before


# --------------------------------------------------------------------------- the app

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "dfo", "review",
         "evidence")


def test_the_app_refuses_a_proposal_decision(src, tmp_path, monkeypatch):
    props = P.build(owner_marks(), src)
    out = tmp_path / "proposals"
    P.write(props, out, src, marks_path=tmp_path / "marks.jsonl")
    curated_root(monkeypatch, tmp_path / "curated")           # T422: the whole curated tree, its marks store empty
    store = Path(CURATED.review.marks)
    monkeypatch.setenv("CURATION_PROPOSALS_DIR", str(out))
    monkeypatch.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    try:
        import app as app_mod
        from fastapi.testclient import TestClient
        c = TestClient(app_mod.app)
        r = c.post("/api/marks", json={"scope": "proposal", "target": "split-n2",
                                       "answers": {"P": "y"}, "reviewer": "dawson"})
        assert r.status_code == 422 and "the marks behind it are the decision" in r.text
        assert store.read_text() == ""
    finally:
        for mod in _MODS:
            sys.modules.pop(mod, None)


# --------------------------------------------------------------------------- T524: a DFO place of several pieces

#: the owner's two-river place (region-2 location 56): the Chilliwack-Vedder below the Slesse line, INCLUDING the Sumas
#: River below Barrow Town Pump Station to its natural end at the Fraser. The pump station has NO cut today: the Sumas
#: "lake" (wbk 328997872, a 1 km river polygon) was absorbed into the Sumas River item, and its auto boundary with it, so
#: a lake-outlet anchor is refused (`check_anchor`: no such lake) — dry run 2026-11-09. The Sumas piece starts at a NEW
#: point cut 50 m below that polygon's outlet (`check_anchor`: Sumas River blk 356352298, measure 5579.5, snapped 0 m)
CHILLIWACK_SUMAS = "2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~3"
CHIL = {"scope": "entry", "op": "downstream_of", "items": ["gnis:8634", "gnis:3062"],
        "points": [{"kind": "split", "split_id": "chilliwack_vedder_rivers__boundary_signs_below_slesse_creek"}]}
SUMAS = {"scope": "entry", "op": "downstream_of", "items": ["gnis:26216"],
         "points": [{"kind": "new", "split_id": "barrow"}]}
BARROW = {"ref": "barrow", "anchor": "dam", "name": "Barrow Town Pump Station", "coord": [-122.112533, 49.114813],
          "on_item": "gnis:26216", "blk": "356352298", "measure": 5579.5}


#: the commit T524 landed at: its region-2 / region-6 places hold the Chilliwack place (~3) as ONE piece and the Forks
#: places as they were, so applying the owner's two-piece marks later never moves these tests
T524_AT = "355ef4cc8"


def _at_t524(path: Path, dst: Path) -> None:
    rel = Path(path).resolve().relative_to(P.REPO.resolve()).as_posix()
    dst.write_bytes(subprocess.run(["git", "show", f"{T524_AT}:{rel}"], cwd=P.REPO, capture_output=True,
                                   check=True).stdout)


@pytest.fixture()
def src_now(tmp_path) -> P.Sources:
    """FROZEN copies (at `T524_AT`) of the region-2 / region-6 places and the cuts: the one-piece places these tests
    turn into two pieces."""
    cat, dfo = tmp_path / "cat", tmp_path / "dfo"
    cat.mkdir()
    dfo.mkdir()
    for name in ("region-2.json", "region-6.json"):
        _at_t524(Path(CURATED.dfo_salmon.places) / name, dfo / name)
    _at_t524(CURATED.waters.cuts, tmp_path / "splits.json")
    (tmp_path / "build").mkdir()
    (tmp_path / "build" / "registry.json").write_text(json.dumps({"items": []}))
    return P.Sources(entries_dir=cat, dfo_dir=dfo, splits_path=tmp_path / "splits.json", build=tmp_path / "build")


def test_a_dfo_mark_holds_several_pieces_and_any_other_mark_one_fix_per_scope():
    m = _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[CHIL, SUMAS], new_splits=[BARROW])
    assert [f.items for f in m.extent_fix] == [["gnis:8634", "gnis:3062"], ["gnis:26216"]]
    with pytest.raises(ValueError, match="one extent fix per scope"):
        _mark(target=HATZIC, extent_fix=[{**CHIL}, {**SUMAS}])
    reversed_items = {**CHIL, "items": list(reversed(CHIL["items"]))}     # the same piece, its waters ticked the other way
    for bad, why in (([CHIL, CHIL], "each piece once"), ([CHIL, reversed_items], "each piece once"),
                     ([CHIL, {"scope": "entry", "op": "rest", "points": [], "items": []}], "only piece"),
                     ([{**CHIL, "tributaries": True}, {**SUMAS, "tributaries": False}], "one tributary call")):
        with pytest.raises(ValueError, match=why):
            _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=bad)


def test_two_pieces_land_as_the_places_extent_list_the_union(src_now):
    m = _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[CHIL, SUMAS], new_splits=[BARROW])
    props = P.build([m], src_now)
    [cut] = [p for p in props if p["kind"] == "new_split"]
    assert cut["edits"][0]["split"]["anchor"] == {"type": "point", "coord": [-122.112533, 49.114813], "is_lonlat": True}
    assert cut["confidence"] == "medium"                  # a hand-placed point: the owner checks it on the map
    [p] = [p for p in props if p["kind"] == "extent"]
    assert p["applicable"], p["problems"]
    assert p["depends_on"] == [cut["id"]]                 # the Sumas piece waits on its new cut
    [ed] = [e for e in p["edits"] if e["field"] == "extents"]
    assert ed["after"] == [
        {"op": "downstream_of", "splits": ["chilliwack_vedder_rivers__boundary_signs_below_slesse_creek"],
         "item_ids": ["gnis:8634", "gnis:3062"]},
        {"op": "downstream_of", "splits": [cut["split_id"]], "item_id": "gnis:26216"}]
    # the place as the build reads it: both pieces, in order
    [text] = [t for path, t in P.materialize([cut, p], src_now).items() if path.parent == src_now.dfo_dir]
    loc = next(x for x in json.loads(text)["locations"] if x["location_id"] == CHILLIWACK_SUMAS)
    from pipeline.regs.dfo_salmon import entries as E
    assert [e.op for e in E.EntryLocation.from_dict(loc).binding.extents] == ["downstream_of", "downstream_of"]


#: a one-piece DFO mark as the model wrote it BEFORE T524 (`marks.line` at ff28e777d) — frozen
ONE_PIECE_LINE = (
    '{"scope":"dfo","target":"2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~3","rule_id":null,'
    '"verdict":"unsure","answers":{"T":"y","R":"y","P":"?"},"problem":null,"note":"","reviewer":"dawson",'
    '"date":"2026-10-26","pdf_sha256":"0000000000000000000000000000000000000000000000000000000000000000","pages":[],'
    '"prints":{"text":"t","records":[{"key":"entry","meaning":"m","binding":"b","reach":null,"answer":null}]},'
    '"carried_by":null,"seconds":null,"extent_fix":[{"scope":"entry","op":"downstream_of","points":[{"kind":"split",'
    '"split_id":"chilliwack_vedder_rivers__boundary_signs_below_slesse_creek","name":null,"item_id":null,"coord":null}],'
    '"items":["gnis:8634","gnis:3062"],"tributaries":null,"area":null}]}\n')


def test_one_piece_is_todays_mark_unchanged(src_now):
    """Parity: one piece writes exactly the one-element extents list it always did — here the place's own curated
    extent, so the mark is already applied (no proposal)."""
    m = _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[CHIL])
    applied: set = set()
    assert [p for p in P.build([m], src_now, applied=applied) if p["kind"] == "extent"] == []
    assert m.key in applied
    # the line is byte for byte the one the pre-T524 model wrote (ff28e777d's `marks.line` on the same mark)
    assert M.line(_mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[CHIL])) == ONE_PIECE_LINE


def test_a_piece_that_binds_nothing_is_named(monkeypatch):
    """The build gate (`reach.dfo_salmon.dead_pieces`): each piece resolved alone; one that binds nothing is named,
    never lost in the union. One piece is not checked (its own outcome says it)."""
    from pipeline.atlas.reach import build as B
    from pipeline.atlas.reach import dfo_salmon as DS
    from pipeline.atlas.reach.models import Outcome, Reason, RuleBinding
    calls = []

    def fake(entry, rule, *a, **k):
        calls.append(rule)
        [ex] = rule["extents"]
        assert rule["includes_tributaries"] is False and entry["includes_tributaries"] is False   # no walk
        if ex["splits"] == ["x"]:                    # a cut that is not on this piece's water
            return RuleBinding(entry["entry_id"], rule["rule_id"], Outcome.unresolved, (), Reason.no_sections, ""), []
        return RuleBinding(entry["entry_id"], rule["rule_id"], Outcome.bound, ("1:0",)), []

    monkeypatch.setattr(B, "build_reach", fake)
    entry = {"entry_id": CHILLIWACK_SUMAS, "matched": [], "includes_tributaries": True}
    good = [{"op": "downstream_of", "splits": ["a"], "item_ids": ["gnis:8634"]},
            {"op": "downstream_of", "splits": ["b"], "item_id": "gnis:26216"}]
    assert DS.dead_pieces(entry, {"rule_id": "r", "extents": good}, None, None) == []
    assert DS.dead_pieces(entry, {"rule_id": "r", "extents": [good[0], {**good[1], "splits": ["x"]}]},
                          None, None) == [1]
    calls.clear()
    assert DS.dead_pieces(entry, {"rule_id": "r", "extents": good[:1]}, None, None) == [] and not calls


#: the Forks (region 6, owner 2026-11-09): "starts 100 meters upstream of the confluence and ends approximately 1 kilometre
#: downstream" — the 100 m is on the upper Bulkley arm, the 1 km on the water below the Forks; the Morice arm is not in it
FORKS = {"6:bulkley-river:bulkley-river-and-morice-river-wat", "6:morice-river:bulkley-river-and-morice-river-wat"}
FORK_CUT = "bulkley_river__morice_river_into_bulkley_river"
#: FWA (dry run 2026-11-09): below the Forks is the BULKLEY (blk 360873822, wsc 400-431358) — the Morice (blk 360885316,
#: wsc 400-431358-585806) ends at the confluence. A = 360873822:164908 (100 m, upper Bulkley), B = 360873822:163908
#: (1,000 m below). The Forks closure takes no tributaries: the Morice place's "(including tributaries)" would walk the
#: whole Morice and upper-Bulkley systems from these 1.1 km, so the pieces say exclude.
FORKS_A = {"scope": "entry", "op": "between", "items": ["gnis:2195"], "tributaries": False,
           "points": [{"kind": "split", "split_id": FORK_CUT + "_u100m"}, {"kind": "split", "split_id": FORK_CUT}]}
FORKS_B = {"scope": "entry", "op": "between", "items": ["gnis:2195"],
           "points": [{"kind": "split", "split_id": FORK_CUT}, {"kind": "split", "split_id": FORK_CUT + "_d1000m"}]}


def test_the_forks_is_two_pieces_on_both_places(src_now):
    """Both Forks places (the Bulkley's row and the Morice's, the same words) take the same two pieces: 100 m up the
    upper Bulkley to the confluence, and the confluence to ~1 km below. Existing cuts only, so Place y may carry it."""
    for target in sorted(FORKS):
        m = _mark(scope="dfo", target=target, extent_fix=[FORKS_A, FORKS_B], verdict="verified",
                  answers={"T": "y", "R": "y", "P": "y"})
        [p] = [p for p in P.build([m], src_now) if p["kind"] == "extent"]
        assert p["applicable"], p["problems"]
        [ed] = [e for e in p["edits"] if e["field"] == "extents"]
        tri = [e for e in p["edits"] if e["field"] == "tributaries"]
        assert [e["after"] for e in tri] == [False]          # 128 inherits none, 129 says include today
        assert ed["after"] == [{"op": "between", "splits": [FORK_CUT + "_u100m", FORK_CUT], "item_id": "gnis:2195"},
                               {"op": "between", "splits": [FORK_CUT, FORK_CUT + "_d1000m"], "item_id": "gnis:2195"}]


def test_the_screen_says_when_a_fix_replaces_the_extent(monkeypatch):
    """(owner 2026-11-09) "more obvious when fix extent overrides existing extent": `dfo.fix_vs_current` — a fix that
    differs from the curated extent REPLACES it; equal on a verified existing-cuts mark is the same; equal on a wrong /
    unsure mark has landed (applied). A different tributary call replaces too. No fix: nothing to say."""
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    try:
        import dfo
        curated = {"op": "downstream_of", "splits": ["chilliwack_vedder_rivers__boundary_signs_below_slesse_creek"],
                   "item_ids": ["gnis:3062", "gnis:8634"]}        # the order of the waters is no difference
        loc = {"location_id": CHILLIWACK_SUMAS, "binding": {"extents": [curated], "tributaries": None}}
        two = _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[CHIL, SUMAS], new_splits=[BARROW])
        got = dfo.fix_vs_current(two, loc, None)
        assert got["state"] == "replaces" and got["current"] == [curated] and len(got["fix"]) == 2
        one = _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[CHIL])
        assert dfo.fix_vs_current(one, loc, None)["state"] == "applied"
        same = _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[CHIL], verdict="verified",
                     answers={"T": "y", "R": "y", "P": "y"})
        assert dfo.fix_vs_current(same, loc, None)["state"] == "same"
        tribs = _mark(scope="dfo", target=CHILLIWACK_SUMAS, extent_fix=[{**CHIL, "tributaries": True}])
        assert dfo.fix_vs_current(tribs, loc, None)["state"] == "replaces"
        assert dfo.fix_vs_current(None, loc, None) is None
        assert dfo.fix_vs_current(_mark(scope="dfo", target=CHILLIWACK_SUMAS), loc, None) is None
        # T525 round 3 (owner: "a save doesn't visibly update"): what the last save did, for the one status line. The
        # Chilliwack + Sumas save waits for the rebuild to build the new Barrow Town cut; once built it has not landed
        # (refused or not run); a fix equal to the extent, or no fix, has landed; a defer decided nothing.
        st = dfo.saved_status(two, dfo.fix_vs_current(two, loc, None), built=set())
        assert st["state"] == "waits_cut" and len(st["cuts"]) == 1 and st["date"] == two.date
        assert dfo.saved_status(two, dfo.fix_vs_current(two, loc, None), built=set(st["cuts"]))["state"] == "not_landed"
        assert dfo.saved_status(one, dfo.fix_vs_current(one, loc, None), built=set())["state"] == "landed"
        keep = _mark(scope="dfo", target=CHILLIWACK_SUMAS, verdict="verified", answers={"T": "y", "R": "y", "P": "y"})
        assert dfo.saved_status(keep, None, built=set()) == {"state": "landed", "date": keep.date, "verdict": "verified"}
        later = _mark(scope="dfo", target=CHILLIWACK_SUMAS, verdict="deferred", answers={"T": "y"}, note="unclear")
        assert dfo.saved_status(later, None, built=set())["state"] == "deferred"
        assert dfo.saved_status(None, None, built=set()) is None
    finally:
        for mod in _MODS:
            sys.modules.pop(mod, None)


def test_a_dead_piece_is_reported_once_however_many_variants_ride_it(monkeypatch):
    """A variant with no extent of its own rides its target's (`resolve_all`): the target's dead piece is ONE refusal,
    named on the target, not one per wording."""
    from types import SimpleNamespace as NS
    from pipeline.atlas.reach import build as B
    from pipeline.atlas.reach import dfo_salmon as DS
    from pipeline.atlas.reach.models import Outcome, RuleBinding
    from pipeline.regs.dfo_salmon import entries as E

    def place(lid, resolved, dup=None):
        return NS(location_id=lid, binding=NS(resolved=resolved), is_variant=dup is not None, duplicate_of=dup,
                  water_id=None, tidal_water=False)
    target = place("2:x:target", True)
    ef = NS(locations=[place("2:x:target~2", False, target.location_id), target,
                       place("2:x:target~3", False, target.location_id)], waters=[], water=lambda _w: None)
    pieces = [{"op": "downstream_of", "splits": ["a"], "item_id": "gnis:1"},
              {"op": "downstream_of", "splits": ["x"], "item_id": "gnis:2"}]
    monkeypatch.setattr(DS, "_slugs", lambda: ["2"])
    monkeypatch.setattr(E, "load", lambda *a, **k: ef)
    monkeypatch.setattr(E, "to_reach_input", lambda loc, *a, **k: ({"entry_id": loc.location_id}, [
        {"rule_id": "r", "extents": pieces}]))
    monkeypatch.setattr(B, "build_reach", lambda entry, rule, *a, **k: (
        RuleBinding(entry["entry_id"], rule["rule_id"], Outcome.bound, ("1:0",)), []))
    monkeypatch.setattr(DS, "dead_pieces", lambda *a, **k: [1])
    with pytest.raises(ValueError) as got:
        DS.resolve_all(None, None, outside=frozenset())
    msg = str(got.value)
    assert "1 piece(s)" in msg and msg.count("piece 2 of 2") == 1
    assert "r2 2:x:target: piece 2 of 2 (downstream_of ['x'] on gnis:2)" in msg


def test_a_side_directory_never_merges_two_ids_alike_in_their_first_40_characters():
    """T562 item 5: the id part of the side-rebuild directory was a bare cut at 40 — two long split-proposal ids with the
    same digest prefix shared one directory, so one side rebuild vouched for the other"""
    long_a, long_b = "split-" + "x" * 60 + "-one", "split-" + "x" * 60 + "-two"
    a, b = P.side_dir({"id": long_a, "digest": "abcdef0123"}), P.side_dir({"id": long_b, "digest": "abcdef9999"})
    assert a != b
    assert P.side_dir({"id": "split-n2", "digest": "abcdef0123"}) == f"data/generated/side/t185-{P._slug('split-n2', 40)}-abcdef", \
        "an id that fits keeps the directory it had"
    assert P._slug(long_a, 40) == P._slug(long_b, 40), "the bare cut is the same for both: the hash is what tells them apart"
