"""T325 (owner 2026-10-30): WRITE-THROUGH — a mark saved in the review app reaches its domain's final curated file by
itself; there is no lock step. Closed = a domain that feeds nothing (ponds, T329).

  per domain   a mark saved through the API updates the curated file: sloughs, names (in the request); dfo, entries
               (in the background, one commit per proposal, temp git repo); tidal (T326: a file domain — waters/tidal.json
               in the request)
  refused      an apply that refuses keeps the mark, returns / holds the reason, and logs it
  batch        accept-all (one request, many marks) writes once
  atomic       a write that fails half way leaves the old file whole and no temp file behind
  closed       a closed domain is not exported, not applied, 423 in the app; removing the entry restores it

Every store, decided dir, curated file and git repo is a temp copy (a closed list lives beside its marks store).
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
    """The review app over a temp store with WRITE-THROUGH ON: temp decided dir, temp slough / name final files, a
    temp git repo holding copies of the DFO / catalogue / splits files the proposal writer edits."""
    (tmp_path / "automark.json").write_text(json.dumps({"rows": SLOUGHS}))
    with gzip.open(tmp_path / "finder.json.gz", "wt") as f:
        json.dump({"candidates": [{"id": r["id"], "outline": [[-120, 50], [-120.001, 50.001], [-120, 50]]}
                                  for r in SLOUGHS]}, f)
    (tmp_path / "page.json").write_text(json.dumps({"verdicts": {}}))
    (tmp_path / "cands.json").write_text(json.dumps({"candidates": NAMES}))
    store = tmp_path / "review" / "marks.jsonl"
    store.parent.mkdir()
    store.write_text("")
    for k, v in {"CURATION_SLOUGH_PAGE_MARKS": "page.json", "CURATION_SLOUGH_AUTOMARK": "automark.json",
                 "CURATION_SLOUGH_FINDER": "finder.json.gz", "CURATION_NAME_CANDIDATES": "cands.json",
                 "CURATION_MARKS": "review/marks.jsonl", "CURATION_BACKUP_DIR": "backups",
                 "CURATION_DECIDED": "review/decided", "CURATION_WRITE_THROUGH_LOG": "write-through.jsonl",
                 "CURATION_REFUSED_LOG": "refused.jsonl"}.items():
        monkeypatch.setenv(k, str(tmp_path / v))
    monkeypatch.setenv("CURATION_WRITE_THROUGH", "1")
    monkeypatch.setenv("CURATION_WRITE_THROUGH_DEBOUNCE", "0.05")
    # the proposal writer's files: a temp git repo
    repo = tmp_path / "repo"
    dfo, cat = repo / "data/curated/dfo_salmon", repo / "data/curated/catalogue"
    dfo.mkdir(parents=True)
    cat.mkdir(parents=True)
    (dfo / "region-1.json").write_bytes((Path(CURATED.regulations.entries.dfo_salmon) / "region-1.json").read_bytes())
    (cat / "region-2.json").write_bytes((Path(CURATED.regulations.entries.catalogue) / "region-2.json").read_bytes())
    (repo / "data/curated/splits.json").write_bytes(Path(CURATED.waters.splits).read_bytes())
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
    monkeypatch.setitem(AP.WRITERS, "names", lambda d: {tmp_path / "mined_names.json": t
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
    assert json.loads((tmp / "review/decided/sloughs.json").read_text())["decisions"][0]["key"][1] == "1"
    # a clear takes it back out
    r = c.post("/api/marks", json={"scope": "slough", "verdict": "cleared", "answers": {}, "reviewer": "test",
                                   "target": "1", "note": "oops"})
    assert r.status_code == 200 and r.json()["write_through"]["state"] == "done"
    assert _waters(tmp / "slough_calls.json") == []


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
    doc = json.loads((tmp / "mined_names.json").read_text())
    assert [n["name"] for e in doc["entries"] if e["target"]["blks"] == ["2"] for n in e["names"]] == ["Review Creek"]


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
    """T326: tidal is a FILE domain — the mark is exported to decided/tidal.json and its row lands in
    waters/tidal.json in the same request (a new cut keeps its point; the atlas mints its id)."""
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
    assert json.loads((wt["tmp"] / "review/decided/tidal.json").read_text())["decisions"][0]["key"][1] == cid
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
    from pipeline.regs.parsing import io as IO

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
    assert not list(tmp.glob("*.tmp")) and not list((tmp / "review/decided").glob("*.tmp"))
    # the next save lands both
    assert c.post("/api/marks", json={**SLOUGH, "target": "3", "call": "stream"}).json()["write_through"]["state"] == "done"
    assert _waters(tmp / "slough_calls.json") == ["1", "2", "3"]


def test_write_through_is_off_over_a_temp_store_unless_asked(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    sys.modules.pop("write_through", None)
    import write_through as W
    monkeypatch.delenv("CURATION_WRITE_THROUGH", raising=False)
    assert not W.from_env(tmp_path / "marks.jsonl", tmp_path).enabled
    assert W.from_env(Path(CURATED.regulations.review.marks), tmp_path).enabled
    monkeypatch.setenv("CURATION_WRITE_THROUGH", "0")
    assert not W.from_env(Path(CURATED.regulations.review.marks), tmp_path).enabled
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
    store = _store(tmp_path, [_pond("lake:1"), _pond("lake:2")])
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_Q154_SAMPLE", str(sample))
    mp.setenv("CURATION_MARKS", str(store))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    mp.syspath_prepend(str(BACKEND))
    for mod in MODS:
        sys.modules.pop(mod, None)
    from pipeline.regs.review import q154 as Q
    Q._load.cache_clear()
    import app as app_mod                                                  # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "store": store, "decided": tmp_path / "decided"}
    mp.undo()
    for mod in MODS:
        sys.modules.pop(mod, None)


def test_a_closed_domain_is_skipped_and_removing_the_entry_restores_it(app_env):
    c, store, decided = app_env["client"], app_env["store"], app_env["decided"]
    E.main(["--marks", str(store), "--out", str(decided)])
    assert (decided / "ponds.json").exists()
    assert c.get("/api/q154/queue").status_code == 200 and c.get("/api/review/closed").json() == {"closed": {}}
    before = store.read_bytes()
    C.write({"ponds": {"domain": "ponds", "scopes": ["q154"], "date": "2026-10-30", "by": "owner",
                       "why": "feeds nothing", "final": None}}, C.path(store))
    # the exporter skips it and removes its leftover file; apply skips it
    E.main(["--marks", str(store), "--out", str(decided)])
    assert not (decided / "ponds.json").exists() and E.stale(decided, store) == []
    assert not E.load(decided, closed={"ponds"}).scope("q154")
    assert AP.main(["ponds", "--marks", str(store), "--decided", str(decided)]) == 0
    # the app: 423 on its queue / tally / pond, a mark refused, the closed list served
    for url in ("/api/q154/queue", "/api/q154/tally", "/api/q154/ponds/lake:1"):
        r = c.get(url)
        assert r.status_code == 423 and r.json()["locked"] == "ponds", url
    got = c.post("/api/marks", json={"scope": "q154", "target": "lake:1", "call": "real lake",
                                     "answers": {"P": "y"}, "reviewer": "test"})
    assert got.status_code == 423 and "closed.json" in json.dumps(got.json())
    assert store.read_bytes() == before
    (decided / "ponds.json").write_text("{}")
    assert E.stale(decided, store) == ["ponds.json (closed: remove it)"]
    (decided / "ponds.json").unlink()
    # reopen: remove the entry -> served, exported and markable again
    C.write({}, C.path(store))
    assert c.get("/api/q154/queue").status_code == 200
    E.main(["--marks", str(store), "--out", str(decided)])
    assert len(json.loads((decided / "ponds.json").read_text())["decisions"]) == 2


def test_closing_one_domain_moves_no_byte_of_another(tmp_path):
    store = _store(tmp_path, [_pond("lake:1")])
    d = E.decide(M.load(store), tidal_decisions=None)
    a, b = E.documents(d), E.documents(d, {"ponds"})
    assert set(a) - set(b) == {"ponds.json"} and all(a[n] == b[n] for n in b)


def test_only_ponds_are_closed_in_the_curated_list():
    got = C.entries()
    assert set(got) == {"ponds", "proposals"} and got["ponds"]["scopes"] == ["q154"] and got["ponds"]["final"] is None
    assert got["proposals"]["scopes"] == ["proposal"] and got["proposals"]["final"] is None   # T331 item 6
    assert not (Path(CURATED.decided) / "ponds.json").exists()
    assert C.path() == Path(CURATED.regulations.review.marks).parent / "closed.json"
