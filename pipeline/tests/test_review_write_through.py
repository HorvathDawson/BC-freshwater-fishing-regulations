"""T325 (owner 2026-10-30): WRITE-THROUGH — a mark saved in the review app reaches its domain's final curated file by
itself; there is no lock step. Closed = a domain that feeds nothing (ponds, T329).

  per domain   a mark saved through the API updates the curated file: sloughs, names (in the request); dfo, entries
               (in the background, one commit per proposal, temp git repo); tidal (T326: a file domain — waters/tidal_limits.json
               in the request)
  refused      an apply that refuses keeps the mark, returns / holds the reason, and logs it
  batch        accept-all (one request, many marks) writes once
  atomic       a write that fails half way leaves the old file whole and no temp file behind
  closed       a closed domain is not exported, not applied, 423 in the app; removing the entry restores it

Every store, curated file and git repo is a temp copy (a closed list lives beside its marks store).
"""
from __future__ import annotations

import gzip
import json
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.review import apply as AP
from pipeline.regs.review import closed as C
from pipeline.regs.review import export as E
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P
from pipeline.tests.conftest import curated_root

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
SHA = "0" * 64
COWICHAN = "1:cowichan-river:from-66-mile-trestle-downstream-to"
HATZIC = "r2:hatzic_lake_and_slough@2-8"
MODS = ("app", "reuse", "ponds", "writes", "review", "sloughs", "variants", "dfo", "tidal", "write_through")


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True).stdout


def _pond(pid: str, verdict="verified", call="pool") -> M.Mark:
    return M.Mark.model_validate({
        "scope": "q154", "target": pid, "verdict": verdict, "call": call if verdict == "verified" else None,
        "answers": {"P": "y" if verdict == "verified" else "?"}, "reviewer": "dawson", "date": "2026-10-29",
        "pdf_sha256": SHA, "prints": {"text": None, "records": [{"key": f"q154:{pid}", "meaning": "m", "binding": "b"}]}})


def _store(tmp: Path, marks) -> Path:
    p = tmp / "marks.jsonl"
    p.write_text("".join(M.line(m) for m in marks))
    return p


# --------------------------------------------------------------------------- the app, write-through on

SLOUGHS = [{"id": i, "name": None, "stream": f"Creek {i}", "region": "3", "rule_class": "LIKELY_STREAM",
            "fwa": "Lake-Definite", "area_ha": 1.0, "width_m": 18.0, "img_veg": 0.5, "img_water": 0.1, "p_stream": 0.9,
            "impact": None, "complex": None, "lat": 50.0, "lon": -120.0, "decision": "auto", "verdict": None,
            "by": None, "why": "w", "confidence": "very_high"} for i in ("1", "2", "3")]
NAMES = [{"target": {"kind": "stream", "blk": "2"}, "current_names": [], "registry_items": [], "proposed": "Review Creek",
          "relation": "unnamed", "confidence": "medium", "auto_accept_eligible": False, "rival_names": [],
          "corroborated_by": [], "sources": [{"source": "osm", "source_label": "OpenStreetMap", "source_ref": "ref",
                                              "official": False, "confidence": "medium", "flags": [],
                                              "evidence": {"blk_overlap": 0.9, "source_overlap": 0.9,
                                                           "lonlat": [-122.2, 49.05]}}]}]


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    """The review app over a temp store with WRITE-THROUGH ON: temp slough / name final files, a
    temp git repo holding copies of the DFO / catalogue / splits files the proposal writer edits."""
    (tmp_path / "automark.json").write_text(json.dumps({"rows": SLOUGHS}))
    with gzip.open(tmp_path / "finder.json.gz", "wt") as f:
        json.dump({"candidates": [{"id": r["id"], "outline": [[-120, 50], [-120.001, 50.001], [-120, 50]]}
                                  for r in SLOUGHS]}, f)
    (tmp_path / "cands.json").write_text(json.dumps({"candidates": NAMES}))
    curated_root(monkeypatch, tmp_path / "curated")           # T422: the whole curated tree, its marks store empty
    store = Path(CURATED.review.marks)
    for k, v in {"CURATION_SLOUGH_AUTOMARK": "automark.json",
                 "CURATION_SLOUGH_FINDER": "finder.json.gz", "CURATION_NAME_CANDIDATES": "cands.json",
                 "CURATION_BACKUP_DIR": "backups",
                 "CURATION_WRITE_THROUGH_LOG": "write-through.jsonl",
                 "CURATION_REFUSED_LOG": "refused.jsonl"}.items():
        monkeypatch.setenv(k, str(tmp_path / v))
    monkeypatch.setenv("CURATION_WRITE_THROUGH", "1")
    monkeypatch.setenv("CURATION_WRITE_THROUGH_DEBOUNCE", "0.05")
    # the proposal writer's files: a temp git repo
    repo = tmp_path / "repo"
    dfo, cat = repo / "data/curated/dfo_salmon", repo / "data/curated/catalogue"
    dfo.mkdir(parents=True)
    cat.mkdir(parents=True)
    (dfo / "region-1.json").write_bytes((Path(CURATED.dfo_salmon.places) / "region-1.json").read_bytes())
    (cat / "region-2.json").write_bytes((Path(CURATED.regulations.synopsis) / "region-2.json").read_bytes())
    (repo / "data/curated/splits.json").write_bytes(Path(CURATED.waters.cuts).read_bytes())
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    _git(repo, "add", "data")
    _git(repo, "commit", "-qm", "base")
    monkeypatch.setattr(P, "REPO", repo)
    monkeypatch.setattr(P, "check", lambda ps, _s: [p.update(check={"cheap": True, "rules": {}}) for p in ps])
    monkeypatch.setattr(P, "side_ready", lambda p: False)
    monkeypatch.setattr(P.Sources, "live", classmethod(lambda cls, build=None: P.Sources(
        entries_dir=cat, dfo_dir=dfo, splits_path=repo / "data/curated/splits.json", build=tmp_path / "build")))
    # the file writers' final files: temp paths, the real writers
    calls = {"sloughs": 0}
    real_sloughs, real_names = AP.WRITERS["sloughs"], AP.WRITERS["names"]

    def sloughs(d):
        calls["sloughs"] += 1
        return {tmp_path / "slough_calls.json": t for t in real_sloughs(d).values()}
    monkeypatch.setitem(AP.WRITERS, "sloughs", sloughs)
    monkeypatch.setitem(AP.WRITERS, "names", lambda d: {tmp_path / "found_names.json": t
                                                       for t in real_names(d).values()})
    from pipeline.tools import tidal_limits as TL
    monkeypatch.setitem(AP.WRITERS, "tidal", lambda d: {tmp_path / "tidal.json": TL.document(TL.build(d, existing=[])[0])})
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in MODS:
        sys.modules.pop(mod, None)
    from pipeline.regs.review import names as N
    from pipeline.regs.review import slough as S
    S._load.cache_clear()
    N._load.cache_clear()
    import app as app_mod                                                  # noqa: E402
    import reuse
    from fastapi.testclient import TestClient

    real_post = reuse.post_mark

    def append(body):                       # a proposal-domain mark, appended as built (no live prints in a test)
        if not PENDING:
            return real_post(body)
        with open(store, "a") as f:
            f.write(M.line(PENDING.pop(0)))
        return {"n": 1}
    PENDING: list = []
    yield {"client": TestClient(app_mod.app), "store": store, "tmp": tmp_path, "repo": repo, "calls": calls,
           "WT": app_mod.WRITE_THROUGH, "pending": PENDING, "append": append, "reuse": reuse, "mp": monkeypatch}
    app_mod.WRITE_THROUGH.wait_idle(30)
    for mod in MODS:
        sys.modules.pop(mod, None)
    S._load.cache_clear()
    N._load.cache_clear()


SLOUGH = {"scope": "slough", "answers": {"P": "y"}, "reviewer": "test"}


def _waters(p: Path) -> list[str]:
    return [r["wbk"] for r in json.loads(p.read_text())["waters"]]


def test_a_slough_mark_writes_slough_calls_in_the_request(wt):
    c, tmp = wt["client"], wt["tmp"]
    r = c.post("/api/marks", json={**SLOUGH, "target": "1", "call": "stream"})
    assert r.status_code == 200, r.text
    got = r.json()["write_through"]
    assert got["domain"] == "sloughs" and got["state"] == "done", got
    assert _waters(tmp / "slough_calls.json") == ["1"]
    assert [m.target for m in E.load(wt["store"]).scope("slough")] == ["1"]
    # a clear takes it back out
    r = c.post("/api/marks", json={"scope": "slough", "verdict": "cleared", "answers": {}, "reviewer": "test",
                                   "target": "1", "note": "oops"})
    assert r.status_code == 200 and r.json()["write_through"]["state"] == "done"
    assert r.json()["write_through"]["dropped"] == ["1"]            # T545: the row a save takes out is named
    assert json.loads((tmp / "write-through.jsonl").read_text().splitlines()[-1])["dropped"] == ["1"]
    assert _waters(tmp / "slough_calls.json") == []


# --------------------------------------------------------------------------- T541: a file domain commits its file

@pytest.fixture()
def tracked(wt):
    """The sloughs file INSIDE the temp git repo, committed empty (as the live curated file is)."""
    from pipeline.tools import slough_calls as SC
    f = wt["repo"] / "data/curated/slough_calls.json"
    f.write_text(SC.document([]))
    _git(wt["repo"], "add", "--", str(f))
    _git(wt["repo"], "commit", "-qm", "sloughs", "--", str(f))
    real = AP.WRITERS["sloughs"]
    wt["mp"].setitem(AP.WRITERS, "sloughs", lambda d: {f: t for t in real(d).values()})
    return f


def test_a_file_domain_save_commits_its_file_and_says_the_commit(wt, tracked):
    r = wt["client"].post("/api/marks", json={**SLOUGH, "target": "1", "call": "stream"})
    got = r.json()["write_through"]
    assert got["state"] == "done" and _waters(tracked) == ["1"], got
    head = _git(wt["repo"], "rev-parse", "--short", "HEAD").strip()
    assert got["sha"] == head and got["outcomes"]["slough|1|"]["sha"] == head
    assert _git(wt["repo"], "log", "-1", "--format=%s") == "T325 apply sloughs: data/curated/slough_calls.json from the review marks\n"
    assert _git(wt["repo"], "show", "--stat", "--format=", "HEAD").split("|")[0].strip() == "data/curated/slough_calls.json"
    assert _git(wt["repo"], "status", "--porcelain", "--", str(tracked)) == ""


def test_a_file_domain_refuses_a_file_that_is_already_dirty(wt, tracked):
    """The save's commit would sweep the other edit in: nothing is written, the mark stays, the save says why."""
    tracked.write_text(tracked.read_text() + "\n")
    before, head = tracked.read_text(), _git(wt["repo"], "rev-parse", "HEAD")
    got = wt["client"].post("/api/marks", json={**SLOUGH, "target": "1", "call": "stream"}).json()["write_through"]
    assert got["state"] == "refused" and "uncommitted changes" in got["why"], got
    assert tracked.read_text() == before and _git(wt["repo"], "rev-parse", "HEAD") == head
    assert '"target": "1"' in wt["store"].read_text() or '"target":"1"' in wt["store"].read_text()


def test_a_file_domain_commit_that_fails_puts_the_file_back(wt, tracked):
    before = tracked.read_text()
    real = AP._git
    wt["mp"].setattr(AP, "_git", lambda *a: real("commit", "--no-such-flag") if a[0] == "commit" else real(*a))
    got = wt["client"].post("/api/marks", json={**SLOUGH, "target": "1", "call": "stream"}).json()["write_through"]
    assert got["state"] == "refused" and "git commit failed, the file is put back" in got["why"], got
    assert tracked.read_text() == before


def test_a_file_outside_the_repository_is_written_and_said_uncommitted(wt):
    got = AP.run(["sloughs"], marks=wt["store"])
    [e] = [e for e in got["events"] if e["status"] == "written"]
    assert "NOT committed (outside the repository)" in e["why"] and "sha" not in e


def test_accept_all_is_one_request_and_writes_once(wt):
    c, tmp = wt["client"], wt["tmp"]
    r = c.post("/api/marks", json={**SLOUGH, "targets": ["1", "2", "3"], "call": "stream"})
    assert r.status_code == 200 and r.json()["n"] == 3
    assert wt["calls"]["sloughs"] == 1
    assert _waters(tmp / "slough_calls.json") == ["1", "2", "3"]
    st = c.get("/api/review/write-through").json()
    assert {st["outcomes"][f"slough|{i}|"]["state"] for i in "123"} == {"saved"}


def test_a_name_mark_writes_mined_names_in_the_request(wt):
    c, tmp = wt["client"], wt["tmp"]
    r = c.post("/api/marks", json={"scope": "name", "answers": {"P": "y"}, "reviewer": "test",
                                   "target": "blk:2~review-creek", "call": "accept"})
    assert r.status_code == 200, r.text
    assert r.json()["write_through"]["state"] == "done"
    doc = json.loads((tmp / "found_names.json").read_text())
    assert [n["name"] for e in doc["entries"] if e["target"]["blks"] == ["2"] for n in e["names"]] == ["Review Creek"]


def test_a_name_mark_gone_stale_says_so_and_its_dropped_name_is_named(wt):
    """T545: the candidates were re-mined under an accepted name — the mark decides nothing now. It is said on the
    item (a refused event, as a stale slough is) and the name that leaves found_names.json is named."""
    c, tmp = wt["client"], wt["tmp"]
    assert c.post("/api/marks", json={"scope": "name", "answers": {"P": "y"}, "reviewer": "test",
                                      "target": "blk:2~review-creek", "call": "accept"}).status_code == 200
    from pipeline.regs.review import names as N
    moved = json.loads(json.dumps(NAMES))
    moved[0]["sources"][0]["source_ref"] = "another way"            # same id, another source: the prints moved
    (tmp / "cands.json").write_text(json.dumps({"candidates": moved}))
    N._load.cache_clear()
    got = AP.run(["names"], marks=wt["store"])
    refused = [e for e in got["events"] if e["status"] == "refused"]
    assert [e["marks"] for e in refused] == [[{"scope": "name", "target": "blk:2~review-creek", "rule_id": None}]]
    assert refused[0]["why"].startswith("stale: ")
    written = next(e for e in got["events"] if e["status"] == "written")
    assert written["dropped"] == ["Review Creek (blk:2~review-creek)"]
    assert json.loads((tmp / "found_names.json").read_text())["entries"] == []


def test_a_name_accepted_on_a_lake_is_refused_on_its_item_not_written_unapplied(wt):
    """T545 item 7: the atlas reads `target.blks` only — a `wbks` entry would sit in the file and never show."""
    c, tmp = wt["client"], wt["tmp"]
    from pipeline.regs.review import names as N
    lake = {**json.loads(json.dumps(NAMES[0])), "target": {"kind": "lake", "wbk": "77"}, "proposed": "Review Lake"}
    (tmp / "cands.json").write_text(json.dumps({"candidates": NAMES + [lake]}))
    N._load.cache_clear()
    [lid] = [i for i, x in N.load()["by_id"].items() if x["proposed"] == "Review Lake"]
    got = c.post("/api/marks", json={"scope": "name", "answers": {"P": "y"}, "reviewer": "test", "target": lid,
                                     "call": "accept"}).json()["write_through"]
    assert got["state"] == "refused" and "blue lines only" in got["why"] and "a lake polygon" in got["why"], got
    assert got["outcomes"][f"name|{lid}|"]["state"] == "refused"
    assert json.loads((tmp / "found_names.json").read_text())["entries"] == []


def _dfo_mark() -> M.Mark:
    fix = [{"scope": "entry", "op": "downstream_of", "items": ["gnis:12227"],
            "points": [{"kind": "split", "split_id": "cowichan_river__cnr_trestle"}]}]
    return M.Mark.model_validate({
        "scope": "dfo", "target": COWICHAN, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
        "reviewer": "dawson", "date": "2026-10-30", "pdf_sha256": SHA, "extent_fix": fix,
        "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}})


def _post_built(wt, mark: M.Mark, **extra):
    wt["pending"].append(mark)
    wt["mp"].setattr(wt["reuse"], "post_mark", wt["append"])
    body = {"scope": mark.scope, "target": mark.target, "answers": dict(mark.answers), "reviewer": "test", **extra}
    return wt["client"].post("/api/marks", json=body)


def test_a_dfo_mark_lands_in_its_dfo_file_in_the_background(wt):
    r = _post_built(wt, _dfo_mark())
    assert r.status_code == 200, r.text
    assert r.json()["write_through"] == {"domain": "dfo", "state": "saving"}
    assert wt["WT"].wait_idle(30)
    loc = next(x for x in json.loads((wt["repo"] / "data/curated/dfo_salmon/region-1.json").read_text())["locations"]
               if x["location_id"] == COWICHAN)
    assert loc["binding"]["extents"] == [{"op": "downstream_of", "splits": ["cowichan_river__cnr_trestle"],
                                          "item_id": "gnis:12227"}]
    assert _git(wt["repo"], "log", "-1", "--format=%s").startswith("T185 apply mark-dfo-")
    st = wt["client"].get("/api/review/write-through").json()
    assert st["outcomes"][f"dfo|{COWICHAN}|"]["state"] == "saved" and st["domains"]["dfo"]["state"] == "done"
    assert not st["saving"]


# T545: two DFO places whose ids are alike in their first 70 slugged characters (`…boun`, `…boun~2`)
LONG = "1:cowichan-river:" + "from-a-line-between-two-fishing-boundary-signs-downstream-to-the-mouth"[:53]
LONG2 = LONG + "~2"
_WANT = [{"op": "downstream_of", "splits": ["cowichan_river__cnr_trestle"], "item_id": "gnis:12227"}]


def _twins(wt) -> Path:
    """Two copies of the Cowichan place under ids that slug alike to 70 characters, committed in the temp repo."""
    assert P._slug(LONG, 70) == P._slug(LONG2, 70) and len(P._slug(LONG, 999)) == 70
    f = wt["repo"] / "data/curated/dfo_salmon/region-1.json"
    doc = json.loads(f.read_text())
    base = next(x for x in doc["locations"] if x["location_id"] == COWICHAN)
    doc["locations"] += [{**base, "location_id": i} for i in (LONG, LONG2)]
    f.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    _git(wt["repo"], "commit", "-qm", "twins", "--", str(f))
    return f


def _extents(f: Path) -> dict:
    return {x["location_id"]: x["binding"].get("extents") for x in json.loads(f.read_text())["locations"]
            if x["location_id"] in (LONG, LONG2)}


def _mark_twins(wt) -> dict:
    for t in (LONG, LONG2):
        assert _post_built(wt, _dfo_mark().model_copy(update={"target": t})).status_code == 200
    assert wt["WT"].wait_idle(30)
    return wt["client"].get("/api/review/write-through").json()


def test_two_dfo_marks_alike_in_their_first_70_characters_both_land(wt):
    f = _twins(wt)
    assert P.mark_pid("dfo", LONG) != P.mark_pid("dfo", LONG2)
    assert P.mark_pid("dfo", LONG) == "mark-dfo-" + P._slug(LONG, 70)          # an id that fitted is unchanged
    st = _mark_twins(wt)
    assert _extents(f) == {LONG: _WANT, LONG2: _WANT}
    for t in (LONG, LONG2):
        o = st["outcomes"][f"dfo|{t}|"]
        assert o["state"] == "saved" and o.get("sha"), o
    assert not st["attention"]
    assert len(_git(wt["repo"], "log", "--format=%s").splitlines()) == 4        # base, twins, one commit per mark


def _old_pid(scope, target, rule_id=None):
    return "mark-" + P._slug(scope, 6) + "-" + P._slug(target, 70) + ("-" + P._slug(rule_id, 40) if rule_id else "")


def test_proposals_under_one_id_are_all_refused_by_name(wt):
    """The guard behind the id: were two proposals to share an id again, neither lands and BOTH marks say why."""
    f = _twins(wt)
    before = _extents(f)
    wt["mp"].setattr(P, "mark_pid", _old_pid)
    st = _mark_twins(wt)
    assert _extents(f) == before
    for t in (LONG, LONG2):
        o = st["attention"][f"dfo|{t}|"]
        assert o["state"] == "refused" and "share the id" in o["why"], o
    assert st["domains"]["dfo"]["state"] == "refused"
    log = [json.loads(x) for x in (wt["tmp"] / "write-through.jsonl").read_text().splitlines()]
    assert "share the id" in log[-1]["refused"][0]["why"]


def test_a_proposal_left_without_an_outcome_is_said_not_saved(wt):
    """The last net: a proposal the apply never reached (here: the id clash, its guard off) reads NOT LANDED."""
    f = _twins(wt)
    wt["mp"].setattr(P, "mark_pid", _old_pid)
    wt["mp"].setattr(P, "duplicate_ids", lambda props: {})
    st = _mark_twins(wt)
    got = _extents(f)
    assert list(got.values()).count(_WANT) == 1, got            # one landed, its twin did not
    bad = [o for o in st["attention"].values() if o["state"] == "refused"]
    assert bad and all("still differs from the mark" in o["why"] for o in bad), st
    assert st["domains"]["dfo"]["state"] == "refused"


def test_a_proposal_the_apply_never_reached_is_said_not_saved(wt):
    from pipeline.regs.review import apply_proposals as A
    wt["mp"].setattr(A, "plan", lambda *a, **k: (None, []))
    assert _post_built(wt, _dfo_mark()).status_code == 200
    assert wt["WT"].wait_idle(30)
    o = wt["client"].get("/api/review/write-through").json()["attention"][f"dfo|{COWICHAN}|"]
    assert o["state"] == "refused" and "neither applied nor reported" in o["why"], o


# --------------------------------------------------------------------------- T545: the gate (`apply --check`)

def test_check_lists_every_mark_in_force_that_has_not_landed(wt, capsys):
    """A mark saved while nothing applied it (the server off, an apply that never ran): `check` names it, per mark
    for a proposal and per file for a file domain; an apply empties the list. It writes nothing."""
    store, f = wt["store"], wt["repo"] / "data/curated/dfo_salmon/region-1.json"
    assert AP.run(None, marks=store)["rc"] == 0                     # the temp file domains' first files
    assert AP.check(marks=store)["ok"]
    wt["WT"].enabled = False                                        # marks are saved, nothing lands
    assert _post_built(wt, _dfo_mark()).json()["write_through"]["state"] == "off"
    assert wt["client"].post("/api/marks", json={**SLOUGH, "target": "1", "call": "stream"}).status_code == 200
    before = f.read_text()
    got = AP.check(marks=store)
    assert not got["ok"] and f.read_text() == before and _waters(wt["tmp"] / "slough_calls.json") == []
    assert {(r["domain"], r["status"]) for r in got["not_landed"]} == {("dfo", "would"), ("sloughs", "stale")}
    assert [r["marks"] for r in got["not_landed"] if r["domain"] == "dfo"] == [[f"dfo|{COWICHAN}|"]]
    assert AP.print_check(got) == 1
    said = capsys.readouterr().out
    assert "2 NOT LANDED" in said and f"dfo|{COWICHAN}|" in said and "slough_calls.json" in said
    # ALLOWED TO WAIT: only by the mark's own key — and a stale file is never allowed
    ok = AP.check(marks=store, allow={f"dfo|{COWICHAN}|": {"date": "2026-11-09", "why": "test"}})
    assert [r["domain"] for r in ok["allowed"]] == ["dfo"] and [r["domain"] for r in ok["not_landed"]] == ["sloughs"]
    assert AP.check(marks=store, allow={"dfo|another|": {"date": "d", "why": "w"}})["unused_allow"] == ["dfo|another|"]
    # the apply lands both; the list is empty
    assert AP.run(None, marks=store)["rc"] == 0
    got = AP.check(marks=store)
    assert got["ok"] and not got["not_landed"] and AP.print_check(got) == 0


def test_check_lists_a_refused_mark(wt):
    store = wt["store"]
    assert AP.run(None, marks=store)["rc"] == 0
    with open(store, "a") as fh:                                    # a place that does not exist: not applicable
        fh.write(M.line(_dfo_mark().model_copy(update={"target": "1:no-such-river:nowhere"})))
    got = AP.check(marks=store)
    assert [(r["status"], r["marks"]) for r in got["not_landed"]] == [("refused", ["dfo|1:no-such-river:nowhere|"])]
    assert "no DFO location" in got["not_landed"][0]["why"]


def test_an_entry_mark_lands_in_its_catalogue_file_in_the_background(wt):
    mark = M.Mark.model_validate({
        "scope": "entry", "target": HATZIC, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y", "A": "y"},
        "reviewer": "dawson", "date": "2026-10-30", "pdf_sha256": SHA,
        "tributaries": {"hatzic_lake_and_slough.r2": "include", "hatzic_lake_and_slough.r3": "include"},
        "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"},
                                            {"key": "rule:hatzic_lake_and_slough.r2", "meaning": "m", "binding": "b"},
                                            {"key": "rule:hatzic_lake_and_slough.r3", "meaning": "m", "binding": "b"}]}})
    before = (wt["repo"] / "data/curated/catalogue/region-2.json").read_text()
    r = _post_built(wt, mark)
    assert r.json()["write_through"] == {"domain": "entries", "state": "saving"}
    assert wt["WT"].wait_idle(30)
    after = (wt["repo"] / "data/curated/catalogue/region-2.json").read_text()
    assert after != before
    assert _git(wt["repo"], "log", "-1", "--format=%s").startswith("T185 apply mark-entry-r2_hatzic")
    assert wt["client"].get("/api/review/write-through").json()["outcomes"][f"entry|{HATZIC}|"]["state"] == "saved"


def test_a_tidal_mark_writes_waters_tidal_json_in_the_request(wt, monkeypatch):
    """T326: tidal is a FILE domain — the mark's row lands in
    waters/tidal_limits.json in the same request (a new cut keeps its point; the atlas mints its id)."""
    cid = "tidal:354154938"
    cands = wt["tmp"] / "tidal-candidates.json"
    cands.write_text(json.dumps({"candidates": [{
        "id": cid, "water": "Nanaimo River", "blk": "354154938", "items": ["gnis:16505"],
        "proposed": {"measure": 2979.5, "coord": [-123.880448, 49.116249], "split_label": "tidal boundary",
                     "split_name": "Nanaimo River: tidal boundary"}, "polygon": {"crossing": "clean"}, "sources": [],
        "confidence": "high", "words_point": {"measure": 2979.5}}]}))
    monkeypatch.setenv("CURATION_TIDAL_CANDIDATES", str(cands))
    from pipeline.regs.review import tidal as T
    T._load.cache_clear()
    row = T.candidate(cid)
    mark = M.Mark.model_validate({
        "scope": "tidal", "target": cid, "verdict": "verified", "answers": {"P": "y"}, "reviewer": "dawson",
        "date": "2026-10-30", "pdf_sha256": SHA, "prints": T.prints(row),
        "new_splits": [{"ref": "t1", "on_item": "gnis:16505", "name": "Nanaimo River: tidal boundary",
                        "anchor": "coord", "coord": [-123.880448, 49.116249], "blk": "354154938",
                        "measure": 2979.5}]})
    r = _post_built(wt, mark)
    wt_ = r.json()["write_through"]
    assert wt_["domain"] == "tidal" and wt_["state"] == "done", wt_
    T._load.cache_clear()
    assert [m.target for m in E.load(wt["store"]).scope("tidal")] == [cid]
    doc = json.loads((wt["tmp"] / "tidal.json").read_text())
    assert doc["format"] == "tidal/1"
    [w] = doc["waters"]
    assert w["blk"] == "354154938" and w["limit"] == "cut" and w["at"]["coord"] == [-123.880448, 49.116249]
    assert "split" not in w and len(w["mark"]) == 12


def test_a_refused_apply_keeps_the_mark_and_says_why(wt):
    # background: the DFO file has uncommitted changes, so apply_proposals refuses (a commit would sweep them in)
    f = wt["repo"] / "data/curated/dfo_salmon/region-1.json"
    f.write_text(f.read_text() + "\n")
    r = _post_built(wt, _dfo_mark())
    assert r.status_code == 200
    assert wt["WT"].wait_idle(30)
    assert COWICHAN in wt["store"].read_text()                                           # the mark stays
    st = wt["client"].get("/api/review/write-through").json()
    o = st["attention"][f"dfo|{COWICHAN}|"]
    assert o["state"] == "refused" and "uncommitted changes" in o["why"]
    assert st["domains"]["dfo"]["state"] == "refused"
    log = [json.loads(x) for x in (wt["tmp"] / "write-through.jsonl").read_text().splitlines()]
    assert log[-1]["domains"] == ["dfo"] and "uncommitted" in log[-1]["refused"][0]["why"]
    # in the request: a writer that fails -> the response says why; the mark stays
    wt["mp"].setitem(AP.WRITERS, "sloughs", lambda d: (_ for _ in ()).throw(RuntimeError("disk full")))
    r = wt["client"].post("/api/marks", json={**SLOUGH, "target": "2", "call": "stream"})
    assert r.status_code == 200
    got = r.json()["write_through"]
    assert got["state"] == "refused" and "disk full" in got["why"]
    assert got["outcomes"]["slough|2|"]["state"] == "refused"
    assert '"target": "2"' in wt["store"].read_text() or '"target":"2"' in wt["store"].read_text()


def test_curated_writes_are_atomic(wt, monkeypatch):
    c, tmp = wt["client"], wt["tmp"]
    assert c.post("/api/marks", json={**SLOUGH, "target": "1", "call": "stream"}).json()["write_through"]["state"] == "done"
    before = (tmp / "slough_calls.json").read_bytes()
    import os
    from pipeline.common import curated_write as IO     # T600: the one temp+replace (parsing.io delegates to it)

    real = os.replace

    def boom(src, dst):                     # the power goes as the final file is renamed into place
        if str(dst).endswith("slough_calls.json"):
            raise OSError("power cut")
        return real(src, dst)
    monkeypatch.setattr(IO.os, "replace", boom)
    r = c.post("/api/marks", json={**SLOUGH, "target": "2", "call": "stream"})
    monkeypatch.setattr(IO.os, "replace", real)
    assert r.status_code == 200 and r.json()["write_through"]["state"] == "refused"
    assert (tmp / "slough_calls.json").read_bytes() == before                     # the old file, whole
    assert not list(tmp.glob("*.tmp"))
    # the next save lands both
    assert c.post("/api/marks", json={**SLOUGH, "target": "3", "call": "stream"}).json()["write_through"]["state"] == "done"
    assert _waters(tmp / "slough_calls.json") == ["1", "2", "3"]


def test_write_through_is_off_over_a_temp_store_unless_asked(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    sys.modules.pop("write_through", None)
    import write_through as W
    monkeypatch.delenv("CURATION_WRITE_THROUGH", raising=False)
    assert not W.from_env(tmp_path / "marks.jsonl", tmp_path).enabled
    assert W.from_env(Path(CURATED.review.marks), tmp_path).enabled
    curated_root(monkeypatch, tmp_path / "curated")           # T422: a sandbox's store is not the live one
    assert not W.from_env(Path(CURATED.review.marks), tmp_path).enabled
    monkeypatch.delenv("CURATED_ROOT")
    monkeypatch.setenv("CURATION_WRITE_THROUGH", "0")
    assert not W.from_env(Path(CURATED.review.marks), tmp_path).enabled
    monkeypatch.setenv("CURATION_WRITE_THROUGH", "1")
    assert W.from_env(tmp_path / "marks.jsonl", tmp_path).enabled
    sys.modules.pop("write_through", None)


# --------------------------------------------------------------------------- closed: only what feeds nothing

@pytest.fixture()
def app_env(tmp_path):
    """The review app over a temp marks store (its closed list beside it) and a 2-pond sample."""
    sample = tmp_path / "sample.json"
    row = {"set": "example", "n": 1, "stratum": "caught", "why": "w", "region": "7", "river": None, "order": 3,
           "ha": 0.2, "avg_width_m": 10.0, "length_m": 50, "elongation": 2.0, "river_width_m": 8.0, "ratio": 1.25,
           "man_made": False, "caught": True, "md_read": None, "lat": 55.0, "lon": -125.0,
           "rings": [[[-125, 55], [-125.001, 55.001], [-125, 55]]], "lines": []}
    sample.write_text(json.dumps({"cutoff": {"max_ha": 1.0}, "rows": [{**row, "id": "lake:1"}, {**row, "id": "lake:2"}]}))
    mp = pytest.MonkeyPatch()
    curated_root(mp, tmp_path / "curated")                    # T422: the whole curated tree, nothing closed
    store = Path(CURATED.review.marks)
    store.write_text("".join(M.line(m) for m in (_pond("lake:1"), _pond("lake:2"))))
    mp.setenv("CURATION_Q154_SAMPLE", str(sample))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    mp.syspath_prepend(str(BACKEND))
    for mod in MODS:
        sys.modules.pop(mod, None)
    from pipeline.regs.review import q154 as Q
    Q._load.cache_clear()
    import app as app_mod                                                  # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "store": store}
    mp.undo()
    for mod in MODS:
        sys.modules.pop(mod, None)


def test_a_closed_domain_is_skipped_and_removing_the_entry_restores_it(app_env):
    c, store = app_env["client"], app_env["store"]
    assert len(E.load(store).scope("q154")) == 2
    assert c.get("/api/q154/queue").status_code == 200 and c.get("/api/review/closed").json() == {"closed": {}}
    before = store.read_bytes()
    C.write({"ponds": {"domain": "ponds", "scopes": ["q154"], "date": "2026-10-30", "by": "owner",
                       "why": "feeds nothing", "final": None}}, C.path(store))
    # the decisions leave it out (the closed list beside the store); apply skips it
    assert not E.load(store).scope("q154")
    assert AP.main(["ponds", "--marks", str(store)]) == 0
    # the app: 423 on its queue / tally / pond, a mark refused, the closed list served
    for url in ("/api/q154/queue", "/api/q154/tally", "/api/q154/ponds/lake:1"):
        r = c.get(url)
        assert r.status_code == 423 and r.json()["locked"] == "ponds", url
    got = c.post("/api/marks", json={"scope": "q154", "target": "lake:1", "call": "real lake",
                                     "answers": {"P": "y"}, "reviewer": "test"})
    assert got.status_code == 423 and "closed.json" in json.dumps(got.json())
    assert store.read_bytes() == before
    # reopen: remove the entry -> served, decided and markable again
    C.write({}, C.path(store))
    assert c.get("/api/q154/queue").status_code == 200
    assert len(E.load(store).scope("q154")) == 2


def test_closing_one_domain_moves_no_other_decision(tmp_path):
    store = _store(tmp_path, [_pond("lake:1")])
    a, b = E.load(store, closed=()), E.load(store, closed={"ponds"})
    assert [m.scope for m in a.inforce] == ["q154"] and b.inforce == ()
    assert [m for m in a.inforce if m.scope != "q154"] == list(b.inforce) and a.groups == b.groups


def test_only_ponds_are_closed_in_the_curated_list():
    got = C.entries()
    assert set(got) == {"ponds", "proposals"} and got["ponds"]["scopes"] == ["q154"] and got["ponds"]["final"] is None
    assert got["proposals"]["scopes"] == ["proposal"] and got["proposals"]["final"] is None   # T331 item 6
    assert C.path() == Path(CURATED.review.marks).parent / "closed.json"


# --------------------------------------------------------------------------- T587: a grouping decision, there and back

def _grouping(target: str, **which) -> M.Mark:
    return M.Mark.model_validate({
        "scope": "dfo", "target": target, "verdict": "verified", "answers": {"T": "y"}, "reviewer": "dawson",
        "date": "2026-11-10", "pdf_sha256": SHA, **which,
        "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}})


def test_taking_a_wording_out_of_its_group_and_adding_it_back_lands_both_ways(wt):
    """T587 (T532 rounds 5–6): "not the same place" then "same place as…" through the write-through the owner's server
    runs — the background DFO apply, one commit per decision. The link leaves the places file, then returns, and the
    file ends byte-equal to where it started."""
    f = wt["repo"] / "data/curated/dfo_salmon/region-1.json"
    doc = json.loads(f.read_text())
    base = next(x for x in doc["locations"] if x["location_id"] == COWICHAN)
    other = COWICHAN + "~9"
    # a confirmed grouping carries its why (T610): the start state has its note, so there-and-back adds none
    doc["locations"].append({**base, "location_id": other, "duplicate_of": COWICHAN, "duplicate_confirmed": True,
                             "binding": {**base["binding"], "extents": [], "notes": ["DFO reworded it; owner confirmed"]}})
    f.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    _git(wt["repo"], "commit", "-qm", "an older wording, linked", "--", str(f))
    start, commits = f.read_bytes(), len(_git(wt["repo"], "log", "--format=%s").splitlines())
    link = lambda: {k: next(x for x in json.loads(f.read_text())["locations"] if x["location_id"] == other).get(k)  # noqa: E731
                    for k in ("duplicate_of", "duplicate_confirmed")}

    r = _post_built(wt, _grouping(other, not_variant_of=COWICHAN), not_variant_of=COWICHAN, verdict="verified")
    assert r.status_code == 200, r.text
    assert wt["WT"].wait_idle(30)
    st = wt["client"].get("/api/review/write-through").json()
    assert st["outcomes"][f"dfo|{other}|"]["state"] == "saved", st["outcomes"]
    assert link() == {"duplicate_of": None, "duplicate_confirmed": False}, "taken out: the link is gone from the file"
    assert len(_git(wt["repo"], "log", "--format=%s").splitlines()) == commits + 1
    assert f.read_bytes() != start

    r = _post_built(wt, _grouping(other, variant_of=COWICHAN), variant_of=COWICHAN, verdict="verified")
    assert r.status_code == 200, r.text
    assert wt["WT"].wait_idle(30)
    st = wt["client"].get("/api/review/write-through").json()
    assert st["outcomes"][f"dfo|{other}|"]["state"] == "saved", st["outcomes"]
    assert link() == {"duplicate_of": COWICHAN, "duplicate_confirmed": True}, "added back: the link returns"
    assert len(_git(wt["repo"], "log", "--format=%s").splitlines()) == commits + 2
    assert f.read_bytes() == start, "there and back leaves the file as it was"


def test_a_place_saved_as_all_the_other_places_lands_as_that_kind(wt):
    """T607: the new extent kind through the normal mark + write-through — the places file holds `other_places` (no
    cut, no item: the build names its siblings)"""
    mark = _dfo_mark().model_copy(update={"extent_fix": (M.ExtentFix.model_validate(
        {"scope": "entry", "op": "other_places", "points": [], "items": []}),)})
    r = _post_built(wt, mark)
    assert r.status_code == 200, r.text
    assert wt["WT"].wait_idle(30)
    st = wt["client"].get("/api/review/write-through").json()
    assert st["outcomes"][f"dfo|{COWICHAN}|"]["state"] == "saved", st["outcomes"]
    loc = next(x for x in json.loads((wt["repo"] / "data/curated/dfo_salmon/region-1.json").read_text())["locations"]
               if x["location_id"] == COWICHAN)
    assert loc["binding"]["extents"] == [{"op": "other_places"}]


def test_a_same_place_save_writes_why_on_the_place(wt):
    """T610: the grouping apply wrote the link and NO note, and `test_a_curator_confirmed_grouping_records_why_it_
    crossed_an_op` failed on the owner's Serpentine link. The note is his own (the mark's), else that he confirmed
    it, when, and on which mark; a place that already has a note keeps it."""
    from pipeline.regs.review import recheck as RC
    f = wt["repo"] / "data/curated/dfo_salmon/region-1.json"
    doc = json.loads(f.read_text())
    base = next(x for x in doc["locations"] if x["location_id"] == COWICHAN)
    blank = {**base["binding"], "extents": [], "notes": []}
    a, b = COWICHAN + "~7", COWICHAN + "~8"
    doc["locations"] += [{**base, "location_id": a, "binding": blank}, {**base, "location_id": b, "binding": blank}]
    f.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    _git(wt["repo"], "commit", "-qm", "two more wordings", "--", str(f))
    said = _grouping(a, variant_of=COWICHAN).model_copy(update={"note": "DFO dropped the park's name"})
    bare = _grouping(b, variant_of=COWICHAN)
    for m in (said, bare):
        r = _post_built(wt, m, variant_of=COWICHAN, verdict="verified")
        assert r.status_code == 200, r.text
        assert wt["WT"].wait_idle(30)
    got = {x["location_id"]: x for x in json.loads(f.read_text())["locations"]}
    assert got[a]["duplicate_confirmed"] is True and got[a]["binding"]["notes"] == [
        "DFO dropped the park's name — owner confirmed same place 2026-11-10"]
    assert got[b]["binding"]["notes"] == [f"owner confirmed same place, 2026-11-10, mark {RC.mark_digest(bare)}"]
    st = wt["client"].get("/api/review/write-through").json()
    assert all(st["outcomes"][f"dfo|{t}|"]["state"] == "saved" for t in (a, b)), st["outcomes"]
